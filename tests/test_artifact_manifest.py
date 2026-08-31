from __future__ import annotations

from pathlib import Path

import pytest

from oracle_lab.artifact_manifest import artifact_manifest_view
from oracle_lab.validation_archive import ValidationArtifactRecord
from oracle_lab.worker_archive import WorkerArtifactRecord


def test_artifact_manifest_view_preserves_worker_and_validation_records() -> None:
    worker_artifacts = (
        WorkerArtifactRecord("stdout.bin", Path("/archive/worker/stdout.bin"), "a" * 64, 7),
        WorkerArtifactRecord("stderr.bin", Path("/archive/worker/stderr.bin"), "b" * 64, 3),
    )
    validation_artifacts = (
        ValidationArtifactRecord("task.json", Path("/archive/validation/task.json"), "c" * 64, 11),
        ValidationArtifactRecord(
            "metadata.json", Path("/archive/validation/metadata.json"), "d" * 64, 29
        ),
    )

    worker_view = artifact_manifest_view(worker_artifacts)
    validation_view = artifact_manifest_view(validation_artifacts)

    assert list(worker_view) == ["stdout.bin", "stderr.bin"]
    assert worker_view == {
        "stdout.bin": {
            "path": "/archive/worker/stdout.bin",
            "sha256": "a" * 64,
            "size_bytes": 7,
        },
        "stderr.bin": {
            "path": "/archive/worker/stderr.bin",
            "sha256": "b" * 64,
            "size_bytes": 3,
        },
    }
    assert list(validation_view) == ["task.json", "metadata.json"]
    assert validation_view == {
        "task.json": {
            "path": "/archive/validation/task.json",
            "sha256": "c" * 64,
            "size_bytes": 11,
        },
        "metadata.json": {
            "path": "/archive/validation/metadata.json",
            "sha256": "d" * 64,
            "size_bytes": 29,
        },
    }


def test_artifact_manifest_view_keeps_comprehension_overwrite_and_error_semantics() -> None:
    artifacts = (
        WorkerArtifactRecord("same.bin", Path("/first"), "a" * 64, 1),
        WorkerArtifactRecord("same.bin", Path("/second"), "b" * 64, 2),
    )

    assert artifact_manifest_view(artifacts) == {
        "same.bin": {"path": "/second", "sha256": "b" * 64, "size_bytes": 2}
    }
    with pytest.raises(AttributeError):
        artifact_manifest_view((object(),))  # type: ignore[arg-type]
