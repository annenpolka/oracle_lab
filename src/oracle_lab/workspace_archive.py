"""Bounded, deterministic workspace transfer across the worker trust boundary.

The format deliberately has no compression, hard-link, sparse-file, ownership,
timestamp, device, or extension syntax.  A workspace export is an opaque byte
string until :func:`validate_workspace_archive` has checked the complete frame.

Version 1 wire format (all integers are unsigned big-endian)::

    MAGIC
    entry_count: u64
    repeated entry_count times:
        kind: u8
        mode: u32
        path_length: u32
        payload_length: u64
        path: path_length bytes of canonical UTF-8 POSIX relative path
        payload: payload_length bytes

Directory payloads are empty.  Regular-file payloads are exact file bytes.
Symlink payloads are canonical UTF-8 relative targets.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import shutil
import stat
import struct
import tempfile
import unicodedata
from dataclasses import dataclass
from pathlib import Path


class WorkspaceArchiveError(RuntimeError):
    """Raised when a workspace cannot cross the untrusted archive boundary."""


WORKSPACE_ARCHIVE_MAGIC = b"ORACLELAB-WORKSPACE-V1\x00"

_COUNT = struct.Struct(">Q")
_ENTRY = struct.Struct(">BIIQ")
_KIND_DIRECTORY = 1
_KIND_REGULAR = 2
_KIND_SYMLINK = 3
_KNOWN_KINDS = frozenset({_KIND_DIRECTORY, _KIND_REGULAR, _KIND_SYMLINK})
_MAX_PATH_BYTES = 4096
_MAX_COMPONENT_BYTES = 255
_MAX_SYMLINK_TARGET_BYTES = 4096
_COPY_BLOCK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class WorkspaceArchiveLimits:
    """Independent hard limits for one workspace export."""

    max_raw_bytes: int
    max_entries: int
    max_regular_payload_bytes: int

    def __post_init__(self) -> None:
        for field_name in (
            "max_raw_bytes",
            "max_entries",
            "max_regular_payload_bytes",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise WorkspaceArchiveError(f"{field_name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class _ArchiveEntry:
    kind: int
    mode: int
    path: str
    path_bytes: bytes
    payload: bytes


@dataclass(frozen=True, slots=True)
class ValidatedWorkspaceExport:
    """A complete archive whose structure and aggregate limits were verified."""

    data: bytes
    sha256: str
    size_bytes: int
    entry_count: int
    regular_payload_bytes: int
    _entries: tuple[_ArchiveEntry, ...]


@dataclass(frozen=True, slots=True)
class _SourceEntry:
    path: Path
    relative: str
    path_bytes: bytes
    details: os.stat_result
    kind: int


def _forbidden_character(value: str) -> str | None:
    for character in value:
        if character in {"\\", ":"} or unicodedata.category(character).startswith("C"):
            return character
    return None


def _validate_component(component: str, *, label: str) -> None:
    if component in {"", ".", ".."}:
        raise WorkspaceArchiveError(f"{label} contains a non-canonical path component")
    forbidden = _forbidden_character(component)
    if forbidden is not None:
        raise WorkspaceArchiveError(f"{label} contains a forbidden character")
    try:
        encoded = component.encode("utf-8", "strict")
    except UnicodeEncodeError as error:
        raise WorkspaceArchiveError(f"{label} is not canonical UTF-8") from error
    if len(encoded) > _MAX_COMPONENT_BYTES:
        raise WorkspaceArchiveError(f"{label} contains an overlong path component")


def _validate_relative_path_bytes(raw: bytes, *, label: str) -> tuple[str, tuple[str, ...]]:
    if not raw or len(raw) > _MAX_PATH_BYTES:
        raise WorkspaceArchiveError(f"{label} length is invalid")
    try:
        value = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as error:
        raise WorkspaceArchiveError(f"{label} is not canonical UTF-8") from error
    if value.encode("utf-8") != raw or value.startswith("/") or value.endswith("/"):
        raise WorkspaceArchiveError(f"{label} is not a canonical relative POSIX path")
    components = tuple(value.split("/"))
    for component in components:
        _validate_component(component, label=label)
        if component.casefold() == ".git":
            raise WorkspaceArchiveError(f"{label} contains Git control data")
    if "/".join(components) != value:
        raise WorkspaceArchiveError(f"{label} is not a canonical relative POSIX path")
    return value, components


def _validate_symlink_target(raw: bytes) -> str:
    value, components = _validate_relative_path_bytes(raw, label="symlink target")
    if any(component in {".", ".."} for component in components):
        # Kept explicit even though the common component validator rejects both.
        raise WorkspaceArchiveError("symlink target may not traverse")
    return value


def _collision_key(components: tuple[str, ...]) -> str:
    return "/".join(unicodedata.normalize("NFD", component).casefold() for component in components)


def _validate_mode(kind: int, mode: int) -> None:
    if mode > 0o777:
        raise WorkspaceArchiveError("workspace entry mode contains special permission bits")
    if kind == _KIND_SYMLINK and mode != 0o777:
        raise WorkspaceArchiveError("workspace symlink mode must be 0777")


def _read_exact(data: bytes, cursor: int, length: int, *, label: str) -> tuple[bytes, int]:
    end = cursor + length
    if end > len(data):
        raise WorkspaceArchiveError(f"workspace archive is truncated in {label}")
    return data[cursor:end], end


def validate_workspace_archive(
    data: bytes,
    limits: WorkspaceArchiveLimits,
) -> ValidatedWorkspaceExport:
    """Validate opaque archive bytes and calculate every reported counter."""

    if not isinstance(data, bytes):
        raise WorkspaceArchiveError("workspace archive must be exact bytes")
    if not isinstance(limits, WorkspaceArchiveLimits):
        raise WorkspaceArchiveError("workspace archive limits are required")
    if len(data) > limits.max_raw_bytes:
        raise WorkspaceArchiveError("workspace archive exceeds the raw byte limit")

    minimum = len(WORKSPACE_ARCHIVE_MAGIC) + _COUNT.size
    if len(data) < minimum or not data.startswith(WORKSPACE_ARCHIVE_MAGIC):
        raise WorkspaceArchiveError("workspace archive magic or version is invalid")
    cursor = len(WORKSPACE_ARCHIVE_MAGIC)
    count_bytes, cursor = _read_exact(data, cursor, _COUNT.size, label="entry count")
    (entry_count,) = _COUNT.unpack(count_bytes)
    if entry_count > limits.max_entries:
        raise WorkspaceArchiveError("workspace archive exceeds the entry limit")
    if entry_count > (len(data) - cursor) // _ENTRY.size:
        raise WorkspaceArchiveError("workspace archive entry count is impossible")

    entries: list[_ArchiveEntry] = []
    exact_entries: dict[str, _ArchiveEntry] = {}
    collision_entries: dict[str, str] = {}
    previous_path_bytes: bytes | None = None
    regular_payload_bytes = 0

    for _ in range(entry_count):
        header, cursor = _read_exact(data, cursor, _ENTRY.size, label="entry header")
        kind, mode, path_length, payload_length = _ENTRY.unpack(header)
        if kind not in _KNOWN_KINDS:
            raise WorkspaceArchiveError("workspace archive contains an unsupported entry kind")
        _validate_mode(kind, mode)
        if path_length == 0 or path_length > _MAX_PATH_BYTES:
            raise WorkspaceArchiveError("workspace archive path length is invalid")
        if payload_length > len(data) - cursor - path_length:
            raise WorkspaceArchiveError("workspace archive payload length exceeds remaining bytes")
        if kind == _KIND_DIRECTORY and payload_length != 0:
            raise WorkspaceArchiveError("workspace directory payload must be empty")
        if kind == _KIND_REGULAR:
            regular_payload_bytes += payload_length
            if regular_payload_bytes > limits.max_regular_payload_bytes:
                raise WorkspaceArchiveError(
                    "workspace archive exceeds the regular payload byte limit"
                )
        if kind == _KIND_SYMLINK and (
            payload_length == 0 or payload_length > _MAX_SYMLINK_TARGET_BYTES
        ):
            raise WorkspaceArchiveError("workspace symlink target length is invalid")

        path_bytes, cursor = _read_exact(data, cursor, path_length, label="entry path")
        payload, cursor = _read_exact(data, cursor, payload_length, label="entry payload")
        path, components = _validate_relative_path_bytes(path_bytes, label="workspace path")
        if previous_path_bytes is not None and path_bytes <= previous_path_bytes:
            raise WorkspaceArchiveError("workspace archive paths are not uniquely sorted")
        previous_path_bytes = path_bytes
        if path in exact_entries:
            raise WorkspaceArchiveError("workspace archive contains a duplicate path")

        collision = _collision_key(components)
        colliding_path = collision_entries.get(collision)
        if colliding_path is not None:
            raise WorkspaceArchiveError(
                f"workspace archive paths collide after normalization: {colliding_path!r}"
            )

        for depth in range(1, len(components)):
            parent = "/".join(components[:depth])
            parent_entry = exact_entries.get(parent)
            if parent_entry is None or parent_entry.kind != _KIND_DIRECTORY:
                raise WorkspaceArchiveError(
                    "workspace archive has a missing or non-directory path prefix"
                )
            parent_collision = _collision_key(components[:depth])
            if collision_entries.get(parent_collision) != parent:
                raise WorkspaceArchiveError("workspace archive has a normalized prefix collision")

        if kind == _KIND_SYMLINK:
            _validate_symlink_target(payload)

        entry = _ArchiveEntry(
            kind=kind,
            mode=mode,
            path=path,
            path_bytes=path_bytes,
            payload=payload,
        )
        entries.append(entry)
        exact_entries[path] = entry
        collision_entries[collision] = path

    if cursor != len(data):
        raise WorkspaceArchiveError("workspace archive contains trailing bytes")

    return ValidatedWorkspaceExport(
        data=data,
        sha256=hashlib.sha256(data).hexdigest(),
        size_bytes=len(data),
        entry_count=len(entries),
        regular_payload_bytes=regular_payload_bytes,
        _entries=tuple(entries),
    )


def _stat_identity(details: os.stat_result) -> tuple[int, ...]:
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_nlink,
        details.st_size,
        details.st_mtime_ns,
        details.st_ctime_ns,
    )


def _entry_kind(details: os.stat_result) -> int:
    if stat.S_ISDIR(details.st_mode):
        return _KIND_DIRECTORY
    if stat.S_ISREG(details.st_mode):
        return _KIND_REGULAR
    if stat.S_ISLNK(details.st_mode):
        return _KIND_SYMLINK
    raise WorkspaceArchiveError("workspace contains an unsupported filesystem entry")


def _scan_source(
    source: Path,
    limits: WorkspaceArchiveLimits,
) -> list[_SourceEntry]:
    scanned: list[_SourceEntry] = []
    regular_payload_bytes = 0
    framed_bytes = len(WORKSPACE_ARCHIVE_MAGIC) + _COUNT.size

    def visit(directory: Path, parent_components: tuple[str, ...]) -> None:
        nonlocal framed_bytes, regular_payload_bytes
        try:
            children = tuple(os.scandir(directory))
        except OSError as error:
            raise WorkspaceArchiveError("workspace directory cannot be scanned") from error
        for child in children:
            if not parent_components and child.name == ".git":
                continue
            components = (*parent_components, child.name)
            try:
                path_bytes = "/".join(components).encode("utf-8", "strict")
            except UnicodeEncodeError as error:
                raise WorkspaceArchiveError("workspace path is not canonical UTF-8") from error
            relative, _ = _validate_relative_path_bytes(path_bytes, label="workspace path")
            try:
                details = child.stat(follow_symlinks=False)
            except OSError as error:
                raise WorkspaceArchiveError("workspace entry cannot be inspected") from error
            kind = _entry_kind(details)
            if kind != _KIND_DIRECTORY and details.st_nlink != 1:
                raise WorkspaceArchiveError("workspace contains a hard-linked entry")
            mode = stat.S_IMODE(details.st_mode)
            if mode > 0o777:
                raise WorkspaceArchiveError("workspace entry mode contains special permission bits")
            payload_size = 0 if kind == _KIND_DIRECTORY else details.st_size
            if payload_size < 0:
                raise WorkspaceArchiveError("workspace entry has an invalid size")
            if kind == _KIND_REGULAR:
                regular_payload_bytes += payload_size
                if regular_payload_bytes > limits.max_regular_payload_bytes:
                    raise WorkspaceArchiveError("workspace exceeds the regular payload byte limit")
            if kind == _KIND_SYMLINK and payload_size > _MAX_SYMLINK_TARGET_BYTES:
                raise WorkspaceArchiveError("workspace symlink target length is invalid")
            framed_bytes += _ENTRY.size + len(path_bytes) + payload_size
            if framed_bytes > limits.max_raw_bytes:
                raise WorkspaceArchiveError("workspace export exceeds the raw byte limit")
            scanned.append(
                _SourceEntry(
                    path=Path(child.path),
                    relative=relative,
                    path_bytes=path_bytes,
                    details=details,
                    kind=kind,
                )
            )
            if len(scanned) > limits.max_entries:
                raise WorkspaceArchiveError("workspace exceeds the entry limit")
            if kind == _KIND_DIRECTORY:
                visit(Path(child.path), components)

    visit(source, ())
    scanned.sort(key=lambda item: item.path_bytes)
    return scanned


def _verify_unchanged(entry: _SourceEntry) -> os.stat_result:
    try:
        current = entry.path.lstat()
    except OSError as error:
        raise WorkspaceArchiveError("workspace changed during export") from error
    if _stat_identity(current) != _stat_identity(entry.details):
        raise WorkspaceArchiveError("workspace changed during export")
    return current


def _read_regular(entry: _SourceEntry) -> bytes:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise WorkspaceArchiveError("workspace export requires O_NOFOLLOW support")
    try:
        descriptor = os.open(entry.path, os.O_RDONLY | nofollow)
    except OSError as error:
        raise WorkspaceArchiveError("workspace regular file cannot be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(entry.details) or not stat.S_ISREG(
            before.st_mode
        ):
            raise WorkspaceArchiveError("workspace changed during export")
        content = bytearray()
        while block := os.read(descriptor, _COPY_BLOCK_BYTES):
            content.extend(block)
            if len(content) > entry.details.st_size:
                raise WorkspaceArchiveError("workspace changed during export")
        after = os.fstat(descriptor)
        if _stat_identity(after) != _stat_identity(entry.details):
            raise WorkspaceArchiveError("workspace changed during export")
        if len(content) != entry.details.st_size:
            raise WorkspaceArchiveError("workspace changed during export")
        return bytes(content)
    finally:
        os.close(descriptor)


def _entry_payload(entry: _SourceEntry) -> tuple[int, bytes]:
    details = _verify_unchanged(entry)
    mode = stat.S_IMODE(details.st_mode)
    if entry.kind == _KIND_DIRECTORY:
        return mode, b""
    if details.st_nlink != 1:
        raise WorkspaceArchiveError("workspace contains a hard-linked entry")
    if entry.kind == _KIND_REGULAR:
        return mode, _read_regular(entry)
    try:
        target = os.readlink(entry.path)
    except OSError as error:
        raise WorkspaceArchiveError("workspace symlink cannot be read") from error
    _verify_unchanged(entry)
    try:
        target_bytes = target.encode("utf-8", "strict")
    except UnicodeEncodeError as error:
        raise WorkspaceArchiveError("symlink target is not canonical UTF-8") from error
    _validate_symlink_target(target_bytes)
    # Symlink permissions cannot be applied portably without following the link.
    return 0o777, target_bytes


def build_workspace_export(
    source: str | Path,
    limits: WorkspaceArchiveLimits,
) -> ValidatedWorkspaceExport:
    """Build and validate a deterministic complete workspace snapshot."""

    if not isinstance(limits, WorkspaceArchiveLimits):
        raise WorkspaceArchiveError("workspace archive limits are required")
    root = Path(source).expanduser()
    try:
        root_details = root.lstat()
    except OSError as error:
        raise WorkspaceArchiveError("workspace root is unavailable") from error
    if stat.S_ISLNK(root_details.st_mode) or not stat.S_ISDIR(root_details.st_mode):
        raise WorkspaceArchiveError("workspace root must be a real directory")

    scanned = _scan_source(root, limits)
    framed: list[tuple[_SourceEntry, int, bytes]] = []
    regular_payload_bytes = 0
    raw_size = len(WORKSPACE_ARCHIVE_MAGIC) + _COUNT.size
    for entry in scanned:
        mode, payload = _entry_payload(entry)
        if entry.kind == _KIND_REGULAR:
            regular_payload_bytes += len(payload)
            if regular_payload_bytes > limits.max_regular_payload_bytes:
                raise WorkspaceArchiveError("workspace exceeds the regular payload byte limit")
        raw_size += _ENTRY.size + len(entry.path_bytes) + len(payload)
        if raw_size > limits.max_raw_bytes:
            raise WorkspaceArchiveError("workspace export exceeds the raw byte limit")
        framed.append((entry, mode, payload))

    output = bytearray(WORKSPACE_ARCHIVE_MAGIC)
    output.extend(_COUNT.pack(len(framed)))
    for entry, mode, payload in framed:
        output.extend(_ENTRY.pack(entry.kind, mode, len(entry.path_bytes), len(payload)))
        output.extend(entry.path_bytes)
        output.extend(payload)
    return validate_workspace_archive(bytes(output), limits)


def _remove_private_tree(path: Path) -> None:
    if not os.path.lexists(path):
        return
    for raw_root, directory_names, _file_names in os.walk(
        path,
        topdown=True,
        followlinks=False,
    ):
        root = Path(raw_root)
        for name in directory_names:
            candidate = root / name
            if not candidate.is_symlink():
                with contextlib.suppress(OSError):
                    candidate.chmod(0o700)
        with contextlib.suppress(OSError):
            root.chmod(0o700)
    shutil.rmtree(path)


def materialize_workspace_export(
    export: ValidatedWorkspaceExport,
    destination: str | Path,
) -> Path:
    """Materialize into one fresh destination without exposing partial output."""

    if not isinstance(export, ValidatedWorkspaceExport):
        raise WorkspaceArchiveError("a validated workspace export is required")
    claimed_counters = (
        export.size_bytes,
        export.entry_count,
        export.regular_payload_bytes,
    )
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in claimed_counters
    ):
        raise WorkspaceArchiveError("validated workspace export identity is inconsistent")
    if not isinstance(export.sha256, str):
        raise WorkspaceArchiveError("validated workspace export identity is inconsistent")
    checked = validate_workspace_archive(
        export.data,
        WorkspaceArchiveLimits(
            max_raw_bytes=max(1, export.size_bytes),
            max_entries=max(1, export.entry_count),
            max_regular_payload_bytes=max(1, export.regular_payload_bytes),
        ),
    )
    if (
        export.sha256 != checked.sha256
        or export.size_bytes != checked.size_bytes
        or export.entry_count != checked.entry_count
        or export.regular_payload_bytes != checked.regular_payload_bytes
        or export._entries != checked._entries
    ):
        raise WorkspaceArchiveError("validated workspace export identity is inconsistent")
    target = Path(destination).expanduser().absolute()
    if os.path.lexists(target):
        raise WorkspaceArchiveError("workspace destination must be fresh")
    parent = target.parent
    try:
        parent_details = parent.lstat()
    except OSError as error:
        raise WorkspaceArchiveError("workspace destination parent is unavailable") from error
    if stat.S_ISLNK(parent_details.st_mode) or not stat.S_ISDIR(parent_details.st_mode):
        raise WorkspaceArchiveError("workspace destination parent must be a real directory")

    staging: Path | None = Path(tempfile.mkdtemp(prefix=f".{target.name}.workspace-", dir=parent))
    directory_modes: list[tuple[Path, int]] = []
    target_created = False
    installed = False
    try:
        nofollow = getattr(os, "O_NOFOLLOW", None)
        if nofollow is None:
            raise WorkspaceArchiveError("workspace import requires O_NOFOLLOW support")
        for entry in checked._entries:
            assert staging is not None
            path = staging.joinpath(*entry.path.split("/"))
            if entry.kind == _KIND_DIRECTORY:
                try:
                    path.mkdir(mode=0o700)
                except OSError as error:
                    raise WorkspaceArchiveError(
                        "workspace directory could not be created exclusively"
                    ) from error
                directory_modes.append((path, entry.mode))
                continue
            if entry.kind == _KIND_SYMLINK:
                try:
                    path.symlink_to(entry.payload.decode("utf-8", "strict"))
                except (OSError, UnicodeDecodeError) as error:
                    raise WorkspaceArchiveError(
                        "workspace symlink could not be created exclusively"
                    ) from error
                continue
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow
            try:
                descriptor = os.open(path, flags, 0o600)
            except OSError as error:
                raise WorkspaceArchiveError(
                    "workspace file could not be created exclusively"
                ) from error
            try:
                view = memoryview(entry.payload)
                written = 0
                while written < len(view):
                    count = os.write(descriptor, view[written:])
                    if count <= 0:
                        raise WorkspaceArchiveError("workspace file write made no progress")
                    written += count
                os.fchmod(descriptor, entry.mode)
            except OSError as error:
                raise WorkspaceArchiveError("workspace file could not be materialized") from error
            finally:
                os.close(descriptor)

        for path, mode in sorted(
            directory_modes,
            key=lambda value: len(value[0].parts),
            reverse=True,
        ):
            try:
                path.chmod(mode, follow_symlinks=False)
            except OSError as error:
                raise WorkspaceArchiveError(
                    "workspace directory mode could not be applied"
                ) from error

        if os.path.lexists(target):
            raise WorkspaceArchiveError("workspace destination appeared during import")
        try:
            target.mkdir(mode=0o700)
            target_created = True
            assert staging is not None
            for child in tuple(staging.iterdir()):
                child.rename(target / child.name)
            staging.rmdir()
        except OSError as error:
            raise WorkspaceArchiveError("workspace destination could not be installed") from error
        staging = None
        installed = True
        return target
    finally:
        if staging is not None and os.path.lexists(staging):
            _remove_private_tree(staging)
        if target_created and not installed and os.path.lexists(target):
            _remove_private_tree(target)


__all__ = [
    "WORKSPACE_ARCHIVE_MAGIC",
    "ValidatedWorkspaceExport",
    "WorkspaceArchiveError",
    "WorkspaceArchiveLimits",
    "build_workspace_export",
    "materialize_workspace_export",
    "validate_workspace_archive",
]
