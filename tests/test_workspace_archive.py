from __future__ import annotations

import hashlib
import os
import socket
import stat
import struct
import tempfile
from dataclasses import replace
from pathlib import Path

import pytest

from oracle_lab import workspace_archive
from oracle_lab.workspace_archive import (
    WORKSPACE_ARCHIVE_MAGIC,
    WorkspaceArchiveError,
    WorkspaceArchiveLimits,
    build_workspace_export,
    materialize_workspace_export,
    validate_workspace_archive,
)

_COUNT = struct.Struct(">Q")
_ENTRY = struct.Struct(">BIIQ")
_DIR = 1
_FILE = 2
_SYMLINK = 3


def _limits(
    *,
    raw: int = 1024 * 1024,
    entries: int = 100,
    payload: int = 512 * 1024,
) -> WorkspaceArchiveLimits:
    return WorkspaceArchiveLimits(
        max_raw_bytes=raw,
        max_entries=entries,
        max_regular_payload_bytes=payload,
    )


def _frame(items: list[tuple[int, int, bytes | str, bytes | str]]) -> bytes:
    data = bytearray(WORKSPACE_ARCHIVE_MAGIC)
    data.extend(_COUNT.pack(len(items)))
    for kind, mode, raw_path, raw_payload in items:
        path = raw_path.encode("utf-8") if isinstance(raw_path, str) else raw_path
        payload = raw_payload.encode("utf-8") if isinstance(raw_payload, str) else raw_payload
        data.extend(_ENTRY.pack(kind, mode, len(path), len(payload)))
        data.extend(path)
        data.extend(payload)
    return bytes(data)


def test_workspace_archive_roundtrip_is_deterministic_and_preserves_tree(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir(mode=0o700)
    git = source / ".git"
    git.mkdir()
    (git / "config").write_text("must not cross\n", encoding="utf-8")
    docs = source / "docs"
    docs.mkdir(mode=0o750)
    empty = docs / "empty"
    empty.mkdir(mode=0o710)
    guide = docs / "guide.txt"
    guide.write_bytes(b"guide\x00\xff\n")
    guide.chmod(0o640)
    script = source / "run.sh"
    script.write_bytes(b"#!/bin/sh\nexit 0\n")
    script.chmod(0o751)
    (docs / "latest").symlink_to("guide.txt")

    first = build_workspace_export(source, _limits())
    second = build_workspace_export(source, _limits())
    checked = validate_workspace_archive(first.data, _limits())

    assert first.data == second.data
    assert first.sha256 == second.sha256 == hashlib.sha256(first.data).hexdigest()
    assert first.entry_count == checked.entry_count == 5
    assert first.regular_payload_bytes == len(guide.read_bytes()) + len(script.read_bytes())
    assert first.size_bytes == len(first.data)
    assert b".git" not in first.data

    destination = materialize_workspace_export(checked, tmp_path / "restored")

    assert not (destination / ".git").exists()
    assert (destination / "docs/guide.txt").read_bytes() == b"guide\x00\xff\n"
    assert (destination / "run.sh").read_bytes() == b"#!/bin/sh\nexit 0\n"
    assert os.readlink(destination / "docs/latest") == "guide.txt"
    assert (destination / "docs/empty").is_dir()
    assert stat.S_IMODE((destination / "docs").stat().st_mode) == 0o750
    assert stat.S_IMODE((destination / "docs/empty").stat().st_mode) == 0o710
    assert stat.S_IMODE((destination / "docs/guide.txt").stat().st_mode) == 0o640
    assert stat.S_IMODE((destination / "run.sh").stat().st_mode) == 0o751


def test_build_excludes_only_root_git_and_rejects_nested_or_case_variant_git(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / ".git").mkdir()
    (source / ".git/config").write_text("private", encoding="utf-8")
    assert build_workspace_export(source, _limits()).entry_count == 0

    nested = source / "nested"
    nested.mkdir()
    (nested / ".git").mkdir()
    with pytest.raises(WorkspaceArchiveError, match="Git control"):
        build_workspace_export(source, _limits())

    (nested / ".git").rmdir()
    (source / ".git/config").unlink()
    (source / ".git").rmdir()
    (source / ".GIT").mkdir()
    with pytest.raises(WorkspaceArchiveError, match="Git control"):
        build_workspace_export(source, _limits())


def test_build_rejects_source_hardlinks(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    original = source / "one.txt"
    original.write_text("same inode", encoding="utf-8")
    os.link(original, source / "two.txt")

    with pytest.raises(WorkspaceArchiveError, match="hard-linked"):
        build_workspace_export(source, _limits())


@pytest.mark.skipif(os.name != "posix", reason="POSIX filesystem entry contract")
@pytest.mark.parametrize("special", ["fifo", "socket"])
def test_build_rejects_source_special_entries(tmp_path: Path, special: str) -> None:
    source = (
        Path(tempfile.mkdtemp(prefix="ws-special-", dir="/tmp"))
        if special == "socket"
        else tmp_path / "source"
    )
    source.mkdir(exist_ok=True)
    path = source / special
    bound_socket: socket.socket | None = None
    if special == "fifo":
        os.mkfifo(path)
    else:
        bound_socket = socket.socket(socket.AF_UNIX)
        bound_socket.bind(str(path))
    try:
        with pytest.raises(WorkspaceArchiveError, match="unsupported filesystem entry"):
            build_workspace_export(source, _limits())
    finally:
        if bound_socket is not None:
            bound_socket.close()
            path.unlink(missing_ok=True)
            source.rmdir()


@pytest.mark.parametrize(
    "path",
    [
        b"/absolute",
        b"../escape",
        b"a/../escape",
        b"a/./file",
        b"a//file",
        b"trailing/",
        b"back\\slash",
        b"drive:name",
        b"line\nfeed",
        b"invalid-\xff",
        b".git/config",
        b"nested/.git/config",
    ],
)
def test_validator_rejects_unsafe_or_noncanonical_paths(path: bytes) -> None:
    archive = _frame([(_FILE, 0o644, path, b"content")])

    with pytest.raises(WorkspaceArchiveError):
        validate_workspace_archive(archive, _limits())


@pytest.mark.parametrize(
    "target",
    [
        "/absolute",
        ".",
        "..",
        "a/../escape",
        "a/./target",
        "back\\slash",
        "drive:name",
        ".git/config",
        "line\nfeed",
    ],
)
def test_validator_rejects_unsafe_symlink_targets(target: str) -> None:
    archive = _frame([(_SYMLINK, 0o777, "link", target)])

    with pytest.raises(WorkspaceArchiveError):
        validate_workspace_archive(archive, _limits())


@pytest.mark.parametrize(
    "items",
    [
        [(_FILE, 0o644, "same", b"a"), (_FILE, 0o644, "same", b"b")],
        [(_FILE, 0o644, "A", b"a"), (_FILE, 0o644, "a", b"b")],
        [(_FILE, 0o644, "e\u0301", b"a"), (_FILE, 0o644, "é", b"b")],
        [(_FILE, 0o644, "node", b"a"), (_FILE, 0o644, "node/child", b"b")],
        [(_SYMLINK, 0o777, "node", "target"), (_FILE, 0o644, "node/child", b"b")],
        [(_FILE, 0o644, "missing/child", b"b")],
    ],
    ids=(
        "duplicate",
        "casefold",
        "unicode-normalization",
        "file-prefix",
        "symlink-prefix",
        "missing-directory-prefix",
    ),
)
def test_validator_rejects_collisions_and_non_directory_prefixes(
    items: list[tuple[int, int, bytes | str, bytes | str]],
) -> None:
    with pytest.raises(WorkspaceArchiveError):
        validate_workspace_archive(_frame(items), _limits())


def test_validator_requires_unique_byte_sorted_paths() -> None:
    archive = _frame(
        [
            (_FILE, 0o644, "z.txt", b"z"),
            (_FILE, 0o644, "a.txt", b"a"),
        ]
    )

    with pytest.raises(WorkspaceArchiveError, match="uniquely sorted"):
        validate_workspace_archive(archive, _limits())


@pytest.mark.parametrize(
    "items",
    [
        [(0, 0o644, "entry", b"")],
        [(4, 0o644, "entry", b"")],
        [(_DIR, 0o755, "directory", b"unexpected")],
        [(_FILE, 0o4755, "setuid", b"content")],
        [(_SYMLINK, 0o755, "link", "target")],
        [(_SYMLINK, 0o777, "link", b"")],
    ],
    ids=(
        "zero-kind",
        "unknown-kind",
        "directory-payload",
        "special-mode",
        "symlink-mode",
        "empty-symlink",
    ),
)
def test_validator_rejects_unrepresentable_entry_shapes(
    items: list[tuple[int, int, bytes | str, bytes | str]],
) -> None:
    with pytest.raises(WorkspaceArchiveError):
        validate_workspace_archive(_frame(items), _limits())


def test_validator_rejects_wrong_magic_truncation_impossible_count_and_trailing_bytes() -> None:
    valid = _frame([(_FILE, 0o644, "file", b"content")])
    impossible_count = (
        WORKSPACE_ARCHIVE_MAGIC
        + _COUNT.pack(2)
        + valid[len(WORKSPACE_ARCHIVE_MAGIC) + _COUNT.size :]
    )

    for damaged in (
        b"WRONG" + valid[5:],
        valid[:-1],
        impossible_count,
        valid + b"trailing",
    ):
        with pytest.raises(WorkspaceArchiveError):
            validate_workspace_archive(damaged, _limits())


def test_raw_entry_and_regular_payload_limits_are_independent_and_inclusive(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "a").write_bytes(b"123")
    (source / "b").write_bytes(b"45")
    built = build_workspace_export(source, _limits())

    exact = WorkspaceArchiveLimits(
        max_raw_bytes=built.size_bytes,
        max_entries=built.entry_count,
        max_regular_payload_bytes=built.regular_payload_bytes,
    )
    assert validate_workspace_archive(built.data, exact).entry_count == 2

    with pytest.raises(WorkspaceArchiveError, match="raw byte limit"):
        validate_workspace_archive(
            built.data,
            WorkspaceArchiveLimits(
                max_raw_bytes=built.size_bytes - 1,
                max_entries=2,
                max_regular_payload_bytes=5,
            ),
        )
    with pytest.raises(WorkspaceArchiveError, match="entry limit"):
        validate_workspace_archive(
            built.data,
            WorkspaceArchiveLimits(
                max_raw_bytes=built.size_bytes,
                max_entries=1,
                max_regular_payload_bytes=5,
            ),
        )
    with pytest.raises(WorkspaceArchiveError, match="regular payload"):
        validate_workspace_archive(
            built.data,
            WorkspaceArchiveLimits(
                max_raw_bytes=built.size_bytes,
                max_entries=2,
                max_regular_payload_bytes=4,
            ),
        )


def test_builder_stops_at_entry_and_payload_limits(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "a").write_bytes(b"123")
    (source / "b").write_bytes(b"456")

    with pytest.raises(WorkspaceArchiveError, match="entry limit"):
        build_workspace_export(source, _limits(entries=1))
    with pytest.raises(WorkspaceArchiveError, match="regular payload"):
        build_workspace_export(source, _limits(payload=5))


def test_builder_rejects_unsafe_source_path_and_symlink_target(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "colon:name").write_text("unsafe", encoding="utf-8")

    with pytest.raises(WorkspaceArchiveError, match="forbidden character"):
        build_workspace_export(source, _limits())

    (source / "colon:name").unlink()
    (source / "escape").symlink_to("../outside")
    with pytest.raises(WorkspaceArchiveError):
        build_workspace_export(source, _limits())


def test_materializer_requires_fresh_destination_and_preserves_existing_tree(
    tmp_path: Path,
) -> None:
    export = validate_workspace_archive(
        _frame([(_FILE, 0o644, "new.txt", b"new")]),
        _limits(),
    )
    destination = tmp_path / "existing"
    destination.mkdir()
    marker = destination / "marker"
    marker.write_text("preserve", encoding="utf-8")

    with pytest.raises(WorkspaceArchiveError, match="must be fresh"):
        materialize_workspace_export(export, destination)

    assert marker.read_text(encoding="utf-8") == "preserve"
    assert not (destination / "new.txt").exists()


def test_materializer_rejects_forged_validated_export(tmp_path: Path) -> None:
    export = validate_workspace_archive(
        _frame([(_FILE, 0o644, "file", b"content")]),
        _limits(),
    )
    forged = replace(export, _entries=())

    with pytest.raises(WorkspaceArchiveError, match="identity is inconsistent"):
        materialize_workspace_export(forged, tmp_path / "destination")

    assert not os.path.lexists(tmp_path / "destination")


def test_materializer_removes_private_partial_tree_on_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    export = validate_workspace_archive(
        _frame(
            [
                (_DIR, 0o755, "directory", b""),
                (_FILE, 0o644, "directory/file", b"content"),
            ]
        ),
        _limits(),
    )

    def fail_write(_descriptor: int, _content: object) -> int:
        raise OSError("injected write failure")

    monkeypatch.setattr(workspace_archive.os, "write", fail_write)
    destination = tmp_path / "destination"

    with pytest.raises(WorkspaceArchiveError, match="could not be materialized"):
        materialize_workspace_export(export, destination)

    assert not os.path.lexists(destination)
    assert list(tmp_path.glob(".destination.workspace-*")) == []


def test_materializer_rejects_symlink_destination_parent(tmp_path: Path) -> None:
    export = validate_workspace_archive(
        _frame([(_FILE, 0o644, "file", b"content")]),
        _limits(),
    )
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(WorkspaceArchiveError, match="parent must be a real directory"):
        materialize_workspace_export(export, linked_parent / "destination")

    assert list(real_parent.iterdir()) == []
