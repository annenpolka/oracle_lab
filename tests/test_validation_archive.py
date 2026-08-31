from __future__ import annotations

import datetime as dt
import inspect
import json
import os
from dataclasses import FrozenInstanceError, fields
from pathlib import Path

import pytest

import oracle_lab.validation_archive as validation_archive_module
from oracle_lab.jsonutil import sha256_bytes
from oracle_lab.validation_archive import (
    SandboxValidationArchive,
    ValidationArchiveError,
    ValidationRunMetadata,
)

_ARCHIVED_AT = dt.datetime(2026, 9, 1, 0, 30, tzinfo=dt.timezone(dt.timedelta(hours=9)))


def _write_validation(
    archive: SandboxValidationArchive,
    *,
    run_id: str = "run_test",
    validation_id: str = "validation_01",
    stdout: bytes = b"ok\x00\xff\n",
    archived_at: dt.datetime = _ARCHIVED_AT,
):
    return archive.write(
        run_id=run_id,
        validation_id=validation_id,
        task={
            "patch_event_id": "evt_patch",
            "approval_event_id": "evt_approval",
            "application_event_id": "evt_application",
        },
        command=("pytest", "-q", "tests/test_example.py"),
        stdout=stdout,
        stderr=b"warning\x80\n",
        run_metadata=ValidationRunMetadata(
            started_at=dt.datetime(2026, 8, 31, 15, 29, tzinfo=dt.UTC),
            finished_at=dt.datetime(2026, 8, 31, 15, 30, tzinfo=dt.UTC),
            exit_code=0,
            timed_out=False,
            status="ok",
            error=None,
        ),
        archived_at=archived_at,
    )


def test_validation_archive_preserves_raw_bytes_and_integrity_under_utc_date(
    tmp_path: Path,
) -> None:
    archive = SandboxValidationArchive(tmp_path / "nested/archive/validations")

    record = _write_validation(archive)

    assert record.directory == (
        tmp_path / "nested/archive/validations/2026/08/31/run_test/validation_01"
    )
    assert record.archived_at == dt.datetime(2026, 8, 31, 15, 30, tzinfo=dt.UTC)
    assert [artifact.name for artifact in record.artifacts] == [
        "task.json",
        "command.json",
        "stdout.bin",
        "stderr.bin",
        "metadata.json",
    ]
    assert record.stdout.path.read_bytes() == b"ok\x00\xff\n"
    assert record.stderr.path.read_bytes() == b"warning\x80\n"
    assert json.loads(record.task.path.read_bytes()) == {
        "application_event_id": "evt_application",
        "approval_event_id": "evt_approval",
        "patch_event_id": "evt_patch",
    }
    assert json.loads(record.command.path.read_bytes()) == {
        "argv": ["pytest", "-q", "tests/test_example.py"]
    }
    for artifact in record.artifacts:
        content = artifact.path.read_bytes()
        assert artifact.sha256 == sha256_bytes(content)
        assert artifact.size_bytes == len(content)

    metadata_bytes = record.metadata.path.read_bytes()
    metadata = json.loads(metadata_bytes)
    assert metadata["truth_domain"] == "sandbox"
    assert metadata["artifact_origin"] == "tool_result"
    assert metadata["run_id"] == "run_test"
    assert metadata["validation_id"] == "validation_01"
    assert metadata["authoritative_raw_artifacts"] == ["stdout.bin", "stderr.bin"]
    assert metadata["execution"] == {
        "status": {"status": "known", "value": "ok"},
        "error": {"status": "known", "value": None},
        "exit_code": {"status": "known", "value": 0},
        "timed_out": {"status": "known", "value": False},
        "output_limited": {"status": "unknown", "value": None},
    }
    assert metadata["timestamps"]["archived_at"] == {
        "status": "known",
        "value": "2026-08-31T15:30:00+00:00",
    }
    assert set(metadata["artifacts"]) == {
        "task.json",
        "command.json",
        "stdout.bin",
        "stderr.bin",
    }
    for name, integrity in metadata["artifacts"].items():
        artifact = record.artifact(name)
        assert integrity == {
            "sha256": artifact.sha256,
            "size_bytes": artifact.size_bytes,
        }
    assert record.metadata.sha256 == sha256_bytes(metadata_bytes)
    assert record.reused_verified_orphan is False


def test_validation_archive_records_unknown_facts_without_host_interpretation(
    tmp_path: Path,
) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")

    record = archive.write(
        run_id="run_unknown",
        validation_id="validation_unknown",
        task={},
        command=(),
        stdout=b"",
        stderr=b"",
        run_metadata=ValidationRunMetadata(),
        archived_at=dt.datetime(2026, 8, 31, tzinfo=dt.UTC),
    )

    metadata = json.loads(record.metadata.path.read_bytes())
    assert all(value["status"] == "unknown" for value in metadata["execution"].values())
    assert metadata["timestamps"]["started_at"]["status"] == "unknown"
    assert metadata["timestamps"]["finished_at"]["status"] == "unknown"
    assert "environment" not in metadata
    assert "interpretation" not in metadata
    assert {field.name for field in fields(ValidationRunMetadata)} == {
        "started_at",
        "finished_at",
        "exit_code",
        "timed_out",
        "output_limited",
        "status",
        "error",
    }
    assert "environment" not in inspect.signature(archive.write).parameters


def test_validation_archive_records_are_immutable(tmp_path: Path) -> None:
    run_metadata = ValidationRunMetadata(exit_code=1, timed_out=False)
    record = _write_validation(SandboxValidationArchive(tmp_path / "validations"))

    with pytest.raises(FrozenInstanceError):
        run_metadata.exit_code = 0  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        record.validation_id = "changed"  # type: ignore[misc]


def test_validation_archive_uses_exclusive_creation_for_every_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")
    original_open = validation_archive_module.os.open
    flags_seen: list[int] = []

    def recording_open(path: Path, flags: int, mode: int = 0o777) -> int:
        flags_seen.append(flags)
        return original_open(path, flags, mode)

    monkeypatch.setattr(validation_archive_module.os, "open", recording_open)

    _write_validation(archive)

    assert len(flags_seen) == 5
    assert all(flags & os.O_EXCL for flags in flags_seen)


def test_validation_archive_reuses_only_complete_byte_identical_orphan(
    tmp_path: Path,
) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")
    first = _write_validation(archive)
    before = {artifact.name: artifact.path.stat().st_mtime_ns for artifact in first.artifacts}

    second = _write_validation(archive)

    assert second.reused_verified_orphan is True
    assert [(item.name, item.sha256, item.size_bytes) for item in second.artifacts] == [
        (item.name, item.sha256, item.size_bytes) for item in first.artifacts
    ]
    assert {
        artifact.name: artifact.path.stat().st_mtime_ns for artifact in second.artifacts
    } == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "different",
        "unexpected",
        "artifact_symlink",
        "validation_directory_symlink",
        "run_directory_symlink",
    ],
)
def test_validation_archive_rejects_damaged_or_symlink_orphans(
    tmp_path: Path,
    damage: str,
) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")
    directory = archive.directory_for("run_test", "validation_01", _ARCHIVED_AT)
    if damage == "validation_directory_symlink":
        directory.parent.mkdir(parents=True)
        target = tmp_path / "validation-target"
        target.mkdir()
        directory.symlink_to(target, target_is_directory=True)
    elif damage == "run_directory_symlink":
        directory.parent.parent.mkdir(parents=True)
        target = tmp_path / "run-target"
        target.mkdir()
        directory.parent.symlink_to(target, target_is_directory=True)
    else:
        record = _write_validation(archive)
        if damage == "missing":
            record.metadata.path.unlink()
        elif damage == "different":
            record.stdout.path.write_bytes(b"different")
        elif damage == "unexpected":
            (record.directory / "unexpected.txt").write_bytes(b"extra")
        else:
            target = tmp_path / "outside-stdout"
            target.write_bytes(record.stdout.path.read_bytes())
            record.stdout.path.unlink()
            record.stdout.path.symlink_to(target)

    with pytest.raises(ValidationArchiveError, match=r"orphan|different bytes|unsafe"):
        _write_validation(archive)


def test_validation_archive_rejects_symlink_archive_root(tmp_path: Path) -> None:
    target = tmp_path / "outside"
    target.mkdir()
    root = tmp_path / "validations"
    root.symlink_to(target, target_is_directory=True)

    with pytest.raises(ValidationArchiveError, match="unsafe validation archive directory"):
        _write_validation(SandboxValidationArchive(root))


def test_validation_archive_load_verifies_authoritative_result(tmp_path: Path) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")
    record = _write_validation(archive)

    snapshot = archive.load(
        run_id=record.run_id,
        validation_id=record.validation_id,
        archived_at=_ARCHIVED_AT,
    )

    assert snapshot.record.directory == record.directory
    assert snapshot.task["patch_event_id"] == "evt_patch"
    assert snapshot.command == ("pytest", "-q", "tests/test_example.py")
    assert snapshot.stdout == b"ok\x00\xff\n"
    assert snapshot.stderr == b"warning\x80\n"


def test_validation_archive_load_rejects_tampering(tmp_path: Path) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")
    record = _write_validation(archive)
    record.stderr.path.write_bytes(b"tampered")

    with pytest.raises(ValidationArchiveError, match="integrity mismatch"):
        archive.load(
            run_id=record.run_id,
            validation_id=record.validation_id,
            archived_at=_ARCHIVED_AT,
        )


def test_validation_archive_cleans_partial_write_and_allows_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")
    original_fsync = validation_archive_module.os.fsync
    calls = 0

    def fail_third_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("injected fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(validation_archive_module.os, "fsync", fail_third_fsync)
    with pytest.raises(OSError, match="injected fsync failure"):
        _write_validation(archive)
    directory = archive.directory_for("run_test", "validation_01", _ARCHIVED_AT)
    assert not directory.exists()

    monkeypatch.setattr(validation_archive_module.os, "fsync", original_fsync)
    assert _write_validation(archive).directory == directory


@pytest.mark.parametrize("field_name", ["run_id", "validation_id"])
@pytest.mark.parametrize("unsafe", ["../escape", "/absolute", "bad/name", "", ".hidden"])
def test_validation_archive_rejects_unsafe_identifiers(
    tmp_path: Path,
    field_name: str,
    unsafe: str,
) -> None:
    arguments = {"run_id": "run_test", "validation_id": "validation_01"}
    arguments[field_name] = unsafe

    with pytest.raises(ValidationArchiveError, match=field_name):
        _write_validation(SandboxValidationArchive(tmp_path / "validations"), **arguments)


def test_validation_archive_rejects_naive_archive_timestamp(tmp_path: Path) -> None:
    with pytest.raises(ValidationArchiveError, match="timezone-aware"):
        _write_validation(
            SandboxValidationArchive(tmp_path / "validations"),
            archived_at=dt.datetime(2026, 8, 31, 12, 30),
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"exit_code": True}, "exit_code"),
        ({"timed_out": 0}, "timed_out"),
        ({"output_limited": 0}, "output_limited"),
        ({"started_at": dt.datetime(2026, 8, 31)}, "started_at"),
        ({"finished_at": dt.datetime(2026, 8, 31)}, "finished_at"),
    ],
)
def test_validation_metadata_rejects_ambiguous_execution_facts(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationArchiveError, match=message):
        ValidationRunMetadata(**kwargs)  # type: ignore[arg-type]


def test_validation_archive_requires_exact_byte_streams(tmp_path: Path) -> None:
    archive = SandboxValidationArchive(tmp_path / "validations")

    with pytest.raises(ValidationArchiveError, match="stdout must be exact bytes"):
        archive.write(
            run_id="run_test",
            validation_id="validation_01",
            task={},
            command=("true",),
            stdout=bytearray(b"not-authoritative"),  # type: ignore[arg-type]
            stderr=b"",
            run_metadata=ValidationRunMetadata(),
            archived_at=_ARCHIVED_AT,
        )
