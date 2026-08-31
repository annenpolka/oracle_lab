from __future__ import annotations

import datetime as dt
import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import oracle_lab.worker_archive as worker_archive_module
from oracle_lab.jsonutil import sha256_bytes
from oracle_lab.worker_archive import (
    WorkerArchiveError,
    WorkerRunArchive,
    WorkerRunMetadata,
)

_ARCHIVED_AT = dt.datetime(2026, 8, 31, 12, 30, tzinfo=dt.UTC)


def _write_run(
    archive: WorkerRunArchive,
    *,
    run_id: str = "run_test",
    stdout: bytes = b"stdout\x00\xff\n",
    archived_at: dt.datetime = _ARCHIVED_AT,
):
    return archive.write(
        run_id=run_id,
        task={"source_event_ids": ["evt_source"], "goal": "Preserve exactly."},
        prompt="確認しろ。\n\n 末尾も保存する。  \n",
        command=("codex", "exec", "--model", "gpt-test"),
        stdout=stdout,
        stderr=b"stderr\x80\n",
        patch=b"diff --git a/a b/a\n@@ -1 +1 @@\n-old\n+new\n",
        run_metadata=WorkerRunMetadata(
            adapter="codex",
            adapter_version=None,
            model="gpt-test",
            base_commit="0123456789abcdef",
            started_at=dt.datetime(2026, 8, 31, 12, 29, tzinfo=dt.UTC),
            finished_at=None,
            status="failed",
            exit_code=7,
            timed_out=False,
            output_limited=True,
            environment_names=("PATH", "OPENAI_API_KEY"),
        ),
        archived_at=archived_at,
    )


def test_worker_archive_preserves_exact_artifacts_and_auditable_metadata(tmp_path: Path) -> None:
    archive = WorkerRunArchive(tmp_path / "nested/archive/workers")

    record = _write_run(archive)

    assert record.directory == tmp_path / "nested/archive/workers/2026/08/31/run_test"
    assert [artifact.name for artifact in record.artifacts] == [
        "task.json",
        "prompt.txt",
        "command.json",
        "stdout.bin",
        "stderr.bin",
        "patch.diff",
        "metadata.json",
    ]
    assert record.prompt.path.read_bytes() == "確認しろ。\n\n 末尾も保存する。  \n".encode()
    assert record.stdout.path.read_bytes() == b"stdout\x00\xff\n"
    assert record.stderr.path.read_bytes() == b"stderr\x80\n"
    assert record.patch.path.read_bytes() == b"diff --git a/a b/a\n@@ -1 +1 @@\n-old\n+new\n"
    assert json.loads(record.command.path.read_bytes()) == {
        "argv": ["codex", "exec", "--model", "gpt-test"]
    }
    for artifact in record.artifacts:
        content = artifact.path.read_bytes()
        assert artifact.sha256 == sha256_bytes(content)
        assert artifact.size_bytes == len(content)

    metadata_bytes = record.metadata.path.read_bytes()
    metadata = json.loads(metadata_bytes)
    assert metadata["artifact_origin"] == "worker_generated"
    assert metadata["identity"] == {
        "adapter": {"status": "known", "value": "codex"},
        "adapter_version": {"status": "unknown", "value": None},
        "base_commit": {"status": "known", "value": "0123456789abcdef"},
        "model": {"status": "known", "value": "gpt-test"},
    }
    assert metadata["timestamps"]["archived_at"]["status"] == "known"
    assert metadata["timestamps"]["started_at"]["status"] == "known"
    assert metadata["timestamps"]["finished_at"] == {"status": "unknown", "value": None}
    assert metadata["execution"] == {
        "exit_code": {"status": "known", "value": 7},
        "output_limited": {"status": "known", "value": True},
        "status": {"status": "known", "value": "failed"},
        "timed_out": {"status": "known", "value": False},
    }
    assert metadata["environment"] == {
        "names": ["PATH", "OPENAI_API_KEY"],
        "redaction_status": "values_omitted",
        "values_archived": False,
    }
    assert b"test-secret-value" not in metadata_bytes
    assert set(metadata["artifacts"]) == {
        "task.json",
        "prompt.txt",
        "command.json",
        "stdout.bin",
        "stderr.bin",
        "patch.diff",
    }
    for name, integrity in metadata["artifacts"].items():
        artifact = record.artifact(name)
        assert integrity == {
            "sha256": artifact.sha256,
            "size_bytes": artifact.size_bytes,
        }
    assert record.metadata.sha256 == sha256_bytes(metadata_bytes)
    assert record.reused_verified_orphan is False


def test_worker_archive_marks_unavailable_identity_and_execution_facts_unknown(
    tmp_path: Path,
) -> None:
    archive = WorkerRunArchive(tmp_path / "workers")

    record = archive.write(
        run_id="run_unknown",
        task={},
        prompt="",
        command=(),
        stdout=b"",
        stderr=b"",
        patch=b"",
        run_metadata=WorkerRunMetadata(environment_names=()),
        archived_at=_ARCHIVED_AT,
    )

    metadata = json.loads(record.metadata.path.read_bytes())
    assert all(value["status"] == "unknown" for value in metadata["identity"].values())
    assert metadata["timestamps"]["archived_at"]["status"] == "known"
    assert metadata["timestamps"]["started_at"]["status"] == "unknown"
    assert metadata["timestamps"]["finished_at"]["status"] == "unknown"
    assert all(value["status"] == "unknown" for value in metadata["execution"].values())


def test_worker_archive_records_are_immutable(tmp_path: Path) -> None:
    metadata = WorkerRunMetadata(adapter="codex", environment_names=("PATH",))
    record = _write_run(WorkerRunArchive(tmp_path / "workers"))

    with pytest.raises(FrozenInstanceError):
        metadata.adapter = "opencode"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        record.run_id = "changed"  # type: ignore[misc]


def test_worker_archive_reuses_only_a_complete_byte_identical_orphan(tmp_path: Path) -> None:
    archive = WorkerRunArchive(tmp_path / "workers")
    first = _write_run(archive)
    before = {artifact.name: artifact.path.stat().st_mtime_ns for artifact in first.artifacts}

    second = _write_run(archive)

    assert second.reused_verified_orphan is True
    assert [(item.name, item.sha256, item.size_bytes) for item in second.artifacts] == [
        (item.name, item.sha256, item.size_bytes) for item in first.artifacts
    ]
    assert {
        artifact.name: artifact.path.stat().st_mtime_ns for artifact in second.artifacts
    } == before


def test_worker_archive_load_verifies_and_returns_authoritative_bytes(tmp_path: Path) -> None:
    archive = WorkerRunArchive(tmp_path / "workers")
    record = _write_run(archive)

    snapshot = archive.load(run_id=record.run_id, archived_at=_ARCHIVED_AT)

    assert snapshot.record.directory == record.directory
    assert snapshot.task["goal"] == "Preserve exactly."
    assert snapshot.prompt == "確認しろ。\n\n 末尾も保存する。  \n"
    assert snapshot.command == ("codex", "exec", "--model", "gpt-test")
    assert snapshot.stdout == b"stdout\x00\xff\n"
    assert snapshot.stderr == b"stderr\x80\n"
    assert snapshot.patch == record.patch.path.read_bytes()


def test_worker_archive_load_rejects_tampering(tmp_path: Path) -> None:
    archive = WorkerRunArchive(tmp_path / "workers")
    record = _write_run(archive)
    record.stdout.path.write_bytes(b"tampered")

    with pytest.raises(WorkerArchiveError, match="integrity mismatch"):
        archive.load(run_id=record.run_id, archived_at=_ARCHIVED_AT)


@pytest.mark.parametrize(
    "damage", ["missing", "different", "artifact_symlink", "directory_symlink"]
)
def test_worker_archive_rejects_incomplete_different_or_symlink_orphans(
    tmp_path: Path,
    damage: str,
) -> None:
    archive = WorkerRunArchive(tmp_path / "workers")
    if damage == "directory_symlink":
        directory = archive.directory_for("run_test", _ARCHIVED_AT)
        directory.parent.mkdir(parents=True)
        target = tmp_path / "symlink-target"
        target.mkdir()
        directory.symlink_to(target, target_is_directory=True)
    else:
        record = _write_run(archive)
        if damage == "missing":
            record.metadata.path.unlink()
        elif damage == "different":
            record.stdout.path.write_bytes(b"different")
        else:
            target = tmp_path / "outside-patch"
            target.write_bytes(record.patch.path.read_bytes())
            record.patch.path.unlink()
            record.patch.path.symlink_to(target)

    with pytest.raises(WorkerArchiveError, match=r"orphan|different bytes|safe directory"):
        _write_run(archive)


def test_worker_archive_cleans_normal_partial_write_failure_and_can_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = WorkerRunArchive(tmp_path / "workers")
    original_fsync = worker_archive_module.os.fsync

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("injected fsync failure")

    monkeypatch.setattr(worker_archive_module.os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="injected fsync failure"):
        _write_run(archive)
    directory = archive.directory_for("run_test", _ARCHIVED_AT)
    assert not directory.exists()

    monkeypatch.setattr(worker_archive_module.os, "fsync", original_fsync)
    assert _write_run(archive).directory == directory


@pytest.mark.parametrize("run_id", ["../escape", "/absolute", "bad/name", ""])
def test_worker_archive_rejects_unsafe_run_ids(tmp_path: Path, run_id: str) -> None:
    with pytest.raises(WorkerArchiveError, match="run_id"):
        _write_run(WorkerRunArchive(tmp_path / "workers"), run_id=run_id)


def test_worker_archive_rejects_naive_archive_timestamp(tmp_path: Path) -> None:
    with pytest.raises(WorkerArchiveError, match="timezone-aware"):
        _write_run(
            WorkerRunArchive(tmp_path / "workers"),
            archived_at=dt.datetime(2026, 8, 31, 12, 30),
        )
