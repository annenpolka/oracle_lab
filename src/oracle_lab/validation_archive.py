"""Write-once archives for deterministic sandbox validation commands.

The archive preserves command output as exact bytes.  It deliberately accepts
neither environment data nor Host-authored summaries: callers can record those
elsewhere as derived events without changing the authoritative tool result.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oracle_lab.jsonutil import canonical_json, sha256_bytes

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$")
_CONTENT_ARTIFACT_NAMES = (
    "task.json",
    "command.json",
    "stdout.bin",
    "stderr.bin",
)
_ARTIFACT_NAMES = (*_CONTENT_ARTIFACT_NAMES, "metadata.json")


class ValidationArchiveError(RuntimeError):
    """Raised when validation cannot be archived without overwriting history."""


@dataclass(frozen=True, slots=True)
class ValidationRunMetadata:
    """Mechanically observed execution facts for one validation command.

    Unknown facts remain ``None``. ``status`` and ``error`` are the exact
    mechanical ToolResult fields, not a Host-authored summary or interpretation.
    """

    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    exit_code: int | None = None
    timed_out: bool | None = None
    output_limited: bool | None = None
    status: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("started_at", "finished_at"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, dt.datetime) or value.tzinfo is None):
                raise ValidationArchiveError(f"{field_name} must be timezone-aware or unknown")
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)
        ):
            raise ValidationArchiveError("exit_code must be an integer or unknown")
        for field_name in ("timed_out", "output_limited"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, bool):
                raise ValidationArchiveError(f"{field_name} must be a boolean or unknown")
        if self.status is not None and (not isinstance(self.status, str) or not self.status):
            raise ValidationArchiveError("status must be a non-empty string or unknown")
        if self.error is not None and not isinstance(self.error, str):
            raise ValidationArchiveError("error must be a string, null, or unknown")
        if self.status is None and self.error is not None:
            raise ValidationArchiveError("error cannot be known without ToolResult status")


@dataclass(frozen=True, slots=True)
class ValidationArtifactRecord:
    """Immutable location and integrity facts for one validation artifact."""

    name: str
    path: Path
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class ValidationArchiveRecord:
    """Immutable result of a new or safely reused validation archive."""

    run_id: str
    validation_id: str
    directory: Path
    archived_at: dt.datetime
    task: ValidationArtifactRecord
    command: ValidationArtifactRecord
    stdout: ValidationArtifactRecord
    stderr: ValidationArtifactRecord
    metadata: ValidationArtifactRecord
    reused_verified_orphan: bool = False

    @property
    def artifacts(self) -> tuple[ValidationArtifactRecord, ...]:
        """Return all artifacts in stable archive order."""

        return (self.task, self.command, self.stdout, self.stderr, self.metadata)

    def artifact(self, name: str) -> ValidationArtifactRecord:
        """Return one artifact by its canonical filename."""

        for artifact in self.artifacts:
            if artifact.name == name:
                return artifact
        raise KeyError(name)


@dataclass(frozen=True, slots=True)
class ValidationArchiveSnapshot:
    """Verified authoritative bytes loaded from a complete validation archive."""

    record: ValidationArchiveRecord
    task: Mapping[str, Any]
    command: tuple[str, ...]
    stdout: bytes
    stderr: bytes
    metadata: Mapping[str, Any]


def _known(value: Any) -> dict[str, Any]:
    return {"status": "unknown" if value is None else "known", "value": value}


def _timestamp_value(value: dt.datetime | None) -> dict[str, Any]:
    return _known(None if value is None else value.isoformat())


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def _path_present(path: Path) -> bool:
    """Return true for ordinary paths and broken symlinks."""

    return path.exists() or path.is_symlink()


def _ensure_directory(path: Path) -> None:
    if _path_present(path):
        if path.is_symlink() or not path.is_dir():
            raise ValidationArchiveError(f"unsafe validation archive directory: {path}")
        return
    try:
        path.mkdir(mode=0o700)
    except FileExistsError as error:
        raise ValidationArchiveError(
            f"validation archive directory changed concurrently: {path}"
        ) from error


def _ensure_directory_chain(path: Path) -> None:
    missing: list[Path] = []
    current = path
    while not _path_present(current):
        missing.append(current)
        parent = current.parent
        if parent == current:  # pragma: no cover - a filesystem root is present
            raise ValidationArchiveError(
                f"validation archive has no existing directory anchor: {path}"
            )
        current = parent
    if current.is_symlink() or not current.is_dir():
        raise ValidationArchiveError(f"unsafe validation archive directory: {current}")
    for directory in reversed(missing):
        _ensure_directory(directory)


def _write_exclusive(path: Path, content: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ValidationArchiveError(f"validation archive is write-once: {path}") from error
    try:
        try:
            view = memoryview(content)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:  # pragma: no cover - defensive OS boundary
                    raise ValidationArchiveError(f"short write in validation archive: {path}")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _artifact_record(path: Path, content: bytes) -> ValidationArtifactRecord:
    return ValidationArtifactRecord(
        name=path.name,
        path=path,
        sha256=sha256_bytes(content),
        size_bytes=len(content),
    )


def _record(
    *,
    run_id: str,
    validation_id: str,
    directory: Path,
    archived_at: dt.datetime,
    contents: Mapping[str, bytes],
    reused_verified_orphan: bool,
) -> ValidationArchiveRecord:
    artifacts = {
        name: _artifact_record(directory / name, contents[name]) for name in _ARTIFACT_NAMES
    }
    return ValidationArchiveRecord(
        run_id=run_id,
        validation_id=validation_id,
        directory=directory,
        archived_at=archived_at,
        task=artifacts["task.json"],
        command=artifacts["command.json"],
        stdout=artifacts["stdout.bin"],
        stderr=artifacts["stderr.bin"],
        metadata=artifacts["metadata.json"],
        reused_verified_orphan=reused_verified_orphan,
    )


def _verify_complete_orphan(directory: Path, expected: Mapping[str, bytes]) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise ValidationArchiveError(
            f"validation archive orphan is not a safe directory: {directory}"
        )
    actual_names = {entry.name for entry in directory.iterdir()}
    if actual_names != set(_ARTIFACT_NAMES):
        raise ValidationArchiveError(
            f"validation archive orphan is incomplete or contains unexpected artifacts: {directory}"
        )
    for name in _ARTIFACT_NAMES:
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValidationArchiveError(f"unsafe validation archive orphan artifact: {path}")
        if path.read_bytes() != expected[name]:
            raise ValidationArchiveError(
                f"validation archive identity collision with different bytes: {path}"
            )


class SandboxValidationArchive:
    """Archive exact sandbox validation results under a UTC date hierarchy."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().absolute()

    def directory_for(
        self,
        run_id: str,
        validation_id: str,
        archived_at: dt.datetime,
    ) -> Path:
        """Return ``YYYY/MM/DD/<run-id>/<validation-id>`` without creating it."""

        for field_name, value in (("run_id", run_id), ("validation_id", validation_id)):
            if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value):
                raise ValidationArchiveError(f"{field_name} is not safe for archive paths")
        if not isinstance(archived_at, dt.datetime) or archived_at.tzinfo is None:
            raise ValidationArchiveError("archive timestamps must be timezone-aware")
        timestamp = archived_at.astimezone(dt.UTC)
        return (
            self.root
            / f"{timestamp.year:04d}"
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / run_id
            / validation_id
        )

    def write(
        self,
        *,
        run_id: str,
        validation_id: str,
        task: Mapping[str, Any],
        command: Sequence[str],
        stdout: bytes,
        stderr: bytes,
        run_metadata: ValidationRunMetadata,
        archived_at: dt.datetime | None = None,
    ) -> ValidationArchiveRecord:
        """Write or safely reuse one complete, byte-identical validation archive."""

        if not isinstance(task, Mapping):
            raise ValidationArchiveError("task must be a mapping")
        if isinstance(command, (str, bytes, bytearray)) or not isinstance(command, Sequence):
            raise ValidationArchiveError("command must be an argument sequence")
        if not all(isinstance(argument, str) for argument in command):
            raise ValidationArchiveError("command arguments must be strings")
        for field_name, value in (("stdout", stdout), ("stderr", stderr)):
            if not isinstance(value, bytes):
                raise ValidationArchiveError(f"{field_name} must be exact bytes")
        if not isinstance(run_metadata, ValidationRunMetadata):
            raise ValidationArchiveError("run_metadata must be ValidationRunMetadata")

        raw_timestamp = dt.datetime.now(dt.UTC) if archived_at is None else archived_at
        if not isinstance(raw_timestamp, dt.datetime) or raw_timestamp.tzinfo is None:
            raise ValidationArchiveError("archive timestamps must be timezone-aware")
        timestamp = raw_timestamp.astimezone(dt.UTC)
        directory = self.directory_for(run_id, validation_id, timestamp)
        contents: dict[str, bytes] = {
            "task.json": _json_bytes(dict(task)),
            "command.json": _json_bytes({"argv": list(command)}),
            "stdout.bin": stdout,
            "stderr.bin": stderr,
        }
        content_integrity = {
            name: {
                "sha256": sha256_bytes(contents[name]),
                "size_bytes": len(contents[name]),
            }
            for name in _CONTENT_ARTIFACT_NAMES
        }
        metadata_document = {
            "schema_version": 1,
            "run_id": run_id,
            "validation_id": validation_id,
            "truth_domain": "sandbox",
            "artifact_origin": "tool_result",
            "authoritative_raw_artifacts": ["stdout.bin", "stderr.bin"],
            "timestamps": {
                "archived_at": _timestamp_value(timestamp),
                "started_at": _timestamp_value(run_metadata.started_at),
                "finished_at": _timestamp_value(run_metadata.finished_at),
            },
            "execution": {
                "status": _known(run_metadata.status),
                "error": {
                    "status": "known" if run_metadata.status is not None else "unknown",
                    "value": run_metadata.error,
                },
                "exit_code": {
                    "status": "known" if run_metadata.status is not None else "unknown",
                    "value": run_metadata.exit_code,
                },
                "timed_out": {
                    "status": "known" if run_metadata.timed_out is not None else "unknown",
                    "value": run_metadata.timed_out,
                },
                "output_limited": {
                    "status": "known" if run_metadata.output_limited is not None else "unknown",
                    "value": run_metadata.output_limited,
                },
            },
            "artifacts": content_integrity,
        }
        contents["metadata.json"] = _json_bytes(metadata_document)

        _ensure_directory_chain(self.root)
        relative_parent_parts = directory.relative_to(self.root).parts[:-1]
        current = self.root
        for part in relative_parent_parts:
            current /= part
            _ensure_directory(current)

        if _path_present(directory):
            _verify_complete_orphan(directory, contents)
            _verify_complete_orphan(directory, contents)
            return _record(
                run_id=run_id,
                validation_id=validation_id,
                directory=directory,
                archived_at=timestamp,
                contents=contents,
                reused_verified_orphan=True,
            )

        try:
            directory.mkdir(mode=0o700)
        except FileExistsError as error:
            raise ValidationArchiveError(
                f"validation archive appeared concurrently: {directory}"
            ) from error

        created: list[Path] = []
        try:
            for name in _ARTIFACT_NAMES:
                path = directory / name
                _write_exclusive(path, contents[name])
                created.append(path)
        except BaseException:
            for path in reversed(created):
                path.unlink(missing_ok=True)
            with contextlib.suppress(OSError):
                directory.rmdir()
            raise

        return _record(
            run_id=run_id,
            validation_id=validation_id,
            directory=directory,
            archived_at=timestamp,
            contents=contents,
            reused_verified_orphan=False,
        )

    def load(
        self,
        *,
        run_id: str,
        validation_id: str,
        archived_at: dt.datetime,
    ) -> ValidationArchiveSnapshot:
        """Load one complete archive after rechecking every artifact."""

        directory = self.directory_for(run_id, validation_id, archived_at)
        if directory.is_symlink() or not directory.is_dir():
            raise ValidationArchiveError(f"validation archive is unavailable: {directory}")
        actual_names = {entry.name for entry in directory.iterdir()}
        if actual_names != set(_ARTIFACT_NAMES):
            raise ValidationArchiveError(
                f"validation archive is incomplete or contains extra artifacts: {directory}"
            )
        contents: dict[str, bytes] = {}
        for name in _ARTIFACT_NAMES:
            path = directory / name
            if path.is_symlink() or not path.is_file():
                raise ValidationArchiveError(f"unsafe validation archive artifact: {path}")
            contents[name] = path.read_bytes()
        try:
            metadata = json.loads(contents["metadata.json"])
            task = json.loads(contents["task.json"])
            command_document = json.loads(contents["command.json"])
        except json.JSONDecodeError as error:
            raise ValidationArchiveError("validation archive JSON is invalid") from error
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("schema_version") != 1
            or metadata.get("run_id") != run_id
            or metadata.get("validation_id") != validation_id
            or metadata.get("truth_domain") != "sandbox"
            or metadata.get("artifact_origin") != "tool_result"
            or not isinstance(task, Mapping)
            or not isinstance(command_document, Mapping)
        ):
            raise ValidationArchiveError("validation archive identity metadata is invalid")
        argv = command_document.get("argv")
        if not isinstance(argv, list) or any(not isinstance(value, str) for value in argv):
            raise ValidationArchiveError("validation archive command is invalid")
        integrity = metadata.get("artifacts")
        if not isinstance(integrity, Mapping) or set(integrity) != set(_CONTENT_ARTIFACT_NAMES):
            raise ValidationArchiveError("validation archive manifest is incomplete")
        for name in _CONTENT_ARTIFACT_NAMES:
            item = integrity[name]
            if (
                not isinstance(item, Mapping)
                or item.get("sha256") != sha256_bytes(contents[name])
                or item.get("size_bytes") != len(contents[name])
            ):
                raise ValidationArchiveError(f"validation archive integrity mismatch: {name}")
        record = _record(
            run_id=run_id,
            validation_id=validation_id,
            directory=directory,
            archived_at=archived_at.astimezone(dt.UTC),
            contents=contents,
            reused_verified_orphan=True,
        )
        return ValidationArchiveSnapshot(
            record=record,
            task=dict(task),
            command=tuple(argv),
            stdout=contents["stdout.bin"],
            stderr=contents["stderr.bin"],
            metadata=dict(metadata),
        )


__all__ = [
    "SandboxValidationArchive",
    "ValidationArchiveError",
    "ValidationArchiveRecord",
    "ValidationArchiveSnapshot",
    "ValidationArtifactRecord",
    "ValidationRunMetadata",
]
