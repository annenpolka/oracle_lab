"""Write-once archives for untrusted coding-worker runs.

The archive preserves worker inputs and outputs as worker-generated material.  It
does not interpret stdout, stderr, or patches, and it never stores environment
values.  ``metadata.json`` records the integrity of the six content artifacts;
the returned :class:`WorkerArchiveRecord` additionally records the integrity of
``metadata.json`` itself without introducing a self-referential hash.
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

_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$")
_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONTENT_ARTIFACT_NAMES = (
    "task.json",
    "prompt.txt",
    "command.json",
    "stdout.bin",
    "stderr.bin",
    "patch.diff",
)
_ARTIFACT_NAMES = (*_CONTENT_ARTIFACT_NAMES, "metadata.json")


class WorkerArchiveError(RuntimeError):
    """Raised when a worker run cannot be archived without overwriting data."""


@dataclass(frozen=True, slots=True)
class WorkerRunMetadata:
    """Auditable worker identity and execution facts.

    Optional values are intentionally represented as unknown in the archived
    metadata rather than inferred.  Environment *values* are not accepted by
    this interface; only the names made available to the worker may be recorded.
    """

    adapter: str | None = None
    adapter_version: str | None = None
    model: str | None = None
    base_commit: str | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    status: str | None = None
    exit_code: int | None = None
    timed_out: bool | None = None
    output_limited: bool | None = None
    environment_names: tuple[str, ...] = ()
    artifact_origin: str = "worker_generated"

    def __post_init__(self) -> None:
        for field_name in ("adapter", "adapter_version", "model", "base_commit", "status"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise WorkerArchiveError(f"{field_name} must be a non-blank string or unknown")
        for field_name in ("started_at", "finished_at"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, dt.datetime) or value.tzinfo is None):
                raise WorkerArchiveError(f"{field_name} must be timezone-aware or unknown")
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)
        ):
            raise WorkerArchiveError("exit_code must be an integer or unknown")
        for field_name in ("timed_out", "output_limited"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, bool):
                raise WorkerArchiveError(f"{field_name} must be a boolean or unknown")
        names = tuple(self.environment_names)
        if any(
            not isinstance(name, str) or not _ENVIRONMENT_NAME.fullmatch(name) for name in names
        ):
            raise WorkerArchiveError("environment_names must contain valid environment names")
        if len(set(names)) != len(names):
            raise WorkerArchiveError("environment_names must be unique")
        if self.artifact_origin not in {"worker_generated", "host_generated"}:
            raise WorkerArchiveError(
                "worker archive artifact_origin must be worker_generated or host_generated"
            )
        object.__setattr__(self, "environment_names", names)


@dataclass(frozen=True, slots=True)
class WorkerArtifactRecord:
    """Immutable location and integrity facts for one archived artifact."""

    name: str
    path: Path
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class WorkerArchiveRecord:
    """Immutable result of one new or safely reused worker-run archive."""

    run_id: str
    directory: Path
    archived_at: dt.datetime
    task: WorkerArtifactRecord
    prompt: WorkerArtifactRecord
    command: WorkerArtifactRecord
    stdout: WorkerArtifactRecord
    stderr: WorkerArtifactRecord
    patch: WorkerArtifactRecord
    metadata: WorkerArtifactRecord
    reused_verified_orphan: bool = False

    @property
    def artifacts(self) -> tuple[WorkerArtifactRecord, ...]:
        """Return all seven artifacts in stable archive order."""

        return (
            self.task,
            self.prompt,
            self.command,
            self.stdout,
            self.stderr,
            self.patch,
            self.metadata,
        )

    def artifact(self, name: str) -> WorkerArtifactRecord:
        """Return one artifact by its canonical filename."""

        for artifact in self.artifacts:
            if artifact.name == name:
                return artifact
        raise KeyError(name)


@dataclass(frozen=True, slots=True)
class WorkerArchiveSnapshot:
    """Verified bytes loaded from a complete archived run."""

    record: WorkerArchiveRecord
    task: Mapping[str, Any]
    prompt: str
    command: tuple[str, ...]
    stdout: bytes
    stderr: bytes
    patch: bytes
    metadata: Mapping[str, Any]


def _known(value: Any) -> dict[str, Any]:
    return {
        "status": "unknown" if value is None else "known",
        "value": value,
    }


def _timestamp_value(value: dt.datetime | None) -> dict[str, Any]:
    return _known(None if value is None else value.isoformat())


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def _write_exclusive(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as error:
        raise WorkerArchiveError(f"worker archive is write-once: {path}") from error
    try:
        try:
            view = memoryview(content)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:  # pragma: no cover - defensive OS boundary
                    raise WorkerArchiveError(f"short write while archiving worker run: {path}")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _path_present(path: Path) -> bool:
    """Return true for ordinary paths and broken symlinks."""

    return path.exists() or path.is_symlink()


def _ensure_directory(path: Path) -> None:
    if _path_present(path):
        if path.is_symlink() or not path.is_dir():
            raise WorkerArchiveError(f"unsafe worker archive directory: {path}")
        return
    try:
        path.mkdir(mode=0o700)
    except FileExistsError as error:
        raise WorkerArchiveError(
            f"worker archive directory changed concurrently: {path}"
        ) from error


def _ensure_directory_chain(path: Path) -> None:
    missing: list[Path] = []
    current = path
    while not _path_present(current):
        missing.append(current)
        parent = current.parent
        if parent == current:  # pragma: no cover - a filesystem root is always present
            raise WorkerArchiveError(f"worker archive has no existing directory anchor: {path}")
        current = parent
    if current.is_symlink() or not current.is_dir():
        raise WorkerArchiveError(f"unsafe worker archive directory: {current}")
    for directory in reversed(missing):
        _ensure_directory(directory)


def _artifact_record(path: Path, content: bytes) -> WorkerArtifactRecord:
    return WorkerArtifactRecord(
        name=path.name,
        path=path,
        sha256=sha256_bytes(content),
        size_bytes=len(content),
    )


def _record(
    *,
    run_id: str,
    directory: Path,
    archived_at: dt.datetime,
    contents: Mapping[str, bytes],
    reused_verified_orphan: bool,
) -> WorkerArchiveRecord:
    artifacts = {
        name: _artifact_record(directory / name, contents[name]) for name in _ARTIFACT_NAMES
    }
    return WorkerArchiveRecord(
        run_id=run_id,
        directory=directory,
        archived_at=archived_at,
        task=artifacts["task.json"],
        prompt=artifacts["prompt.txt"],
        command=artifacts["command.json"],
        stdout=artifacts["stdout.bin"],
        stderr=artifacts["stderr.bin"],
        patch=artifacts["patch.diff"],
        metadata=artifacts["metadata.json"],
        reused_verified_orphan=reused_verified_orphan,
    )


def _verify_complete_orphan(directory: Path, expected: Mapping[str, bytes]) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise WorkerArchiveError(f"worker archive orphan is not a safe directory: {directory}")
    actual_names = {entry.name for entry in directory.iterdir()}
    expected_names = set(_ARTIFACT_NAMES)
    if actual_names != expected_names:
        raise WorkerArchiveError(
            f"worker archive orphan is incomplete or contains unexpected artifacts: {directory}"
        )
    for name in _ARTIFACT_NAMES:
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise WorkerArchiveError(f"unsafe worker archive orphan artifact: {path}")
        if path.read_bytes() != expected[name]:
            raise WorkerArchiveError(
                f"worker archive identity collision with different bytes: {path}"
            )


class WorkerRunArchive:
    """Archive exact worker-run artifacts under ``YYYY/MM/DD/<run-id>``."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().absolute()

    def directory_for(self, run_id: str, archived_at: dt.datetime) -> Path:
        """Return the deterministic directory for a run without creating it."""

        if not isinstance(run_id, str) or not _SAFE_RUN_ID.fullmatch(run_id):
            raise WorkerArchiveError("run_id is not safe for archive paths")
        if archived_at.tzinfo is None:
            raise WorkerArchiveError("archive timestamps must be timezone-aware")
        timestamp = archived_at.astimezone(dt.UTC)
        return (
            self.root
            / f"{timestamp.year:04d}"
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / run_id
        )

    def write(
        self,
        *,
        run_id: str,
        task: Mapping[str, Any],
        prompt: str,
        command: Sequence[str],
        stdout: bytes,
        stderr: bytes,
        patch: bytes,
        run_metadata: WorkerRunMetadata,
        archived_at: dt.datetime | None = None,
    ) -> WorkerArchiveRecord:
        """Write or safely reuse one complete, byte-identical run archive."""

        if not isinstance(task, Mapping):
            raise WorkerArchiveError("task must be a mapping")
        if not isinstance(prompt, str):
            raise WorkerArchiveError("prompt must be text")
        if isinstance(command, (str, bytes, bytearray)) or not isinstance(command, Sequence):
            raise WorkerArchiveError("command must be an argument sequence")
        if not all(isinstance(argument, str) for argument in command):
            raise WorkerArchiveError("command arguments must be strings")
        for field_name, value in (("stdout", stdout), ("stderr", stderr), ("patch", patch)):
            if not isinstance(value, bytes):
                raise WorkerArchiveError(f"{field_name} must be exact bytes")
        if not isinstance(run_metadata, WorkerRunMetadata):
            raise WorkerArchiveError("run_metadata must be WorkerRunMetadata")

        raw_timestamp = dt.datetime.now(dt.UTC) if archived_at is None else archived_at
        if not isinstance(raw_timestamp, dt.datetime) or raw_timestamp.tzinfo is None:
            raise WorkerArchiveError("archive timestamps must be timezone-aware")
        timestamp = raw_timestamp.astimezone(dt.UTC)
        directory = self.directory_for(run_id, timestamp)
        contents: dict[str, bytes] = {
            "task.json": _json_bytes(dict(task)),
            "prompt.txt": prompt.encode("utf-8"),
            "command.json": _json_bytes({"argv": list(command)}),
            "stdout.bin": stdout,
            "stderr.bin": stderr,
            "patch.diff": patch,
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
            "artifact_origin": run_metadata.artifact_origin,
            "identity": {
                "adapter": _known(run_metadata.adapter),
                "adapter_version": _known(run_metadata.adapter_version),
                "model": _known(run_metadata.model),
                "base_commit": _known(run_metadata.base_commit),
            },
            "timestamps": {
                "archived_at": _timestamp_value(timestamp),
                "started_at": _timestamp_value(run_metadata.started_at),
                "finished_at": _timestamp_value(run_metadata.finished_at),
            },
            "execution": {
                "status": _known(run_metadata.status),
                "exit_code": _known(run_metadata.exit_code),
                "timed_out": _known(run_metadata.timed_out),
                "output_limited": _known(run_metadata.output_limited),
            },
            "environment": {
                "names": list(run_metadata.environment_names),
                "values_archived": False,
                "redaction_status": "values_omitted",
            },
            "artifacts": content_integrity,
        }
        contents["metadata.json"] = _json_bytes(metadata_document)

        # Build date parents one component at a time so an existing symlink is
        # rejected instead of silently followed into another archive root.
        _ensure_directory_chain(self.root)
        relative_parts = directory.relative_to(self.root).parts[:-1]
        current = self.root
        for part in relative_parts:
            current /= part
            _ensure_directory(current)

        if _path_present(directory):
            _verify_complete_orphan(directory, contents)
            # Recheck immediately before returning the reused identity.  This
            # mirrors the later DB append boundary used by the service layer.
            _verify_complete_orphan(directory, contents)
            return _record(
                run_id=run_id,
                directory=directory,
                archived_at=timestamp,
                contents=contents,
                reused_verified_orphan=True,
            )

        try:
            directory.mkdir(mode=0o700)
        except FileExistsError as error:
            raise WorkerArchiveError(
                f"worker run archive appeared concurrently: {directory}"
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
            # Never remove or reinterpret a concurrently introduced path.
            with contextlib.suppress(OSError):
                directory.rmdir()
            raise

        return _record(
            run_id=run_id,
            directory=directory,
            archived_at=timestamp,
            contents=contents,
            reused_verified_orphan=False,
        )

    def load(
        self,
        *,
        run_id: str,
        archived_at: dt.datetime,
    ) -> WorkerArchiveSnapshot:
        """Load a complete orphan/registered run after verifying every byte."""

        directory = self.directory_for(run_id, archived_at)
        if directory.is_symlink() or not directory.is_dir():
            raise WorkerArchiveError(f"worker run archive is unavailable: {directory}")
        actual_names = {entry.name for entry in directory.iterdir()}
        if actual_names != set(_ARTIFACT_NAMES):
            raise WorkerArchiveError(
                f"worker run archive is incomplete or contains extra artifacts: {directory}"
            )
        contents: dict[str, bytes] = {}
        for name in _ARTIFACT_NAMES:
            path = directory / name
            if path.is_symlink() or not path.is_file():
                raise WorkerArchiveError(f"unsafe worker run archive artifact: {path}")
            contents[name] = path.read_bytes()
        try:
            metadata = json.loads(contents["metadata.json"])
            task = json.loads(contents["task.json"])
            command_document = json.loads(contents["command.json"])
            prompt = contents["prompt.txt"].decode("utf-8")
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise WorkerArchiveError("worker run archive JSON/text is invalid") from error
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("schema_version") != 1
            or metadata.get("run_id") != run_id
            or metadata.get("artifact_origin") not in {"worker_generated", "host_generated"}
            or not isinstance(task, Mapping)
            or not isinstance(command_document, Mapping)
        ):
            raise WorkerArchiveError("worker run archive identity metadata is invalid")
        argv = command_document.get("argv")
        if not isinstance(argv, list) or any(not isinstance(argument, str) for argument in argv):
            raise WorkerArchiveError("worker run archive command is invalid")
        integrity = metadata.get("artifacts")
        if not isinstance(integrity, Mapping) or set(integrity) != set(_CONTENT_ARTIFACT_NAMES):
            raise WorkerArchiveError("worker run archive integrity manifest is incomplete")
        for name in _CONTENT_ARTIFACT_NAMES:
            item = integrity[name]
            if (
                not isinstance(item, Mapping)
                or item.get("sha256") != sha256_bytes(contents[name])
                or item.get("size_bytes") != len(contents[name])
            ):
                raise WorkerArchiveError(f"worker run archive integrity mismatch: {name}")
        record = _record(
            run_id=run_id,
            directory=directory,
            archived_at=archived_at.astimezone(dt.UTC),
            contents=contents,
            reused_verified_orphan=True,
        )
        return WorkerArchiveSnapshot(
            record=record,
            task=dict(task),
            prompt=prompt,
            command=tuple(argv),
            stdout=contents["stdout.bin"],
            stderr=contents["stderr.bin"],
            patch=contents["patch.diff"],
            metadata=dict(metadata),
        )


__all__ = [
    "WorkerArchiveError",
    "WorkerArchiveRecord",
    "WorkerArchiveSnapshot",
    "WorkerArtifactRecord",
    "WorkerRunArchive",
    "WorkerRunMetadata",
]
