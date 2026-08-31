"""Hardened, write-once archives for no-model ``sbx`` observations.

This module is intentionally independent from the probe implementation.  A
separate contract builder rejects arbitrary structural objects and freezes a
canonical payload before this filesystem sink creates any directory.  The sink
therefore writes filenames and exact bytes without decoding SBX JSON or deciding
truth domains.

Filesystem access is anchored to directory descriptors and every traversed
component is opened with ``O_NOFOLLOW``.  A complete staging directory is
fsynced and committed with the platform's atomic *no-replace* rename primitive.
If that primitive is unavailable, the archive fails closed rather than falling
back to an overwriting ``rename``.  A failed staging attempt is deliberately
left in place: reusing that probe ID then fails, preserving write-once
semantics and the evidence that the archive did not complete.
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import os
import platform
import stat
from collections.abc import Mapping as Mapping
from collections.abc import Sequence as Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oracle_lab.jsonutil import sha256_bytes
from oracle_lab.sbx_observation_payload import (
    SbxObservationLike,
    SbxObservationPayloadError,
    SbxObservationReportLike,
    build_canonical_sbx_observation_payload,
)


class SbxObservationArchiveError(RuntimeError):
    """A secret-free archive failure identified only by a stable reason ID."""

    __slots__ = ("reason_id",)

    def __init__(self, reason_id: str) -> None:
        self.reason_id = reason_id
        super().__init__(reason_id)


@dataclass(frozen=True, slots=True)
class SbxObservationArchiveRecord:
    """Public integrity and location facts for one committed archive."""

    probe_id: str
    directory: Path
    manifest_path: Path
    manifest_sha256: str
    raw_file_count: int

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "probe_id": self.probe_id,
            "directory_sha256": sha256_bytes(str(self.directory).encode("utf-8")),
            "manifest_path_sha256": sha256_bytes(str(self.manifest_path).encode("utf-8")),
            "manifest_sha256": self.manifest_sha256,
            "raw_file_count": self.raw_file_count,
        }


def _raise(reason_id: str) -> None:
    # Invoke this after leaving provider/OSError ``except`` suites so a stable
    # archive failure never retains secret-bearing exception context.
    raise SbxObservationArchiveError(reason_id) from None


def _archive_root_path(root: str | Path) -> Path:
    try:
        candidate = Path(root).expanduser()
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        candidate = None
    if candidate is None or not candidate.is_absolute():
        _raise("sbx_observation_archive_root_invalid")
    parts = candidate.parts
    if (
        not parts
        or parts[0] != candidate.anchor
        or any(part in {"", ".", ".."} or "\x00" in part for part in parts[1:])
    ):
        _raise("sbx_observation_archive_root_invalid")
    return candidate


def _directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    if nofollow == 0 or directory == 0 or os.open not in os.supports_dir_fd:
        _raise("sbx_observation_archive_platform_unsupported")
    return os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0)


def _try_open_directory(name: str, *, dir_fd: int | None) -> tuple[int | None, int | None]:
    try:
        descriptor = os.open(name, _directory_flags(), dir_fd=dir_fd)
    except OSError as error:
        descriptor = None
        error_number = error.errno
    else:
        error_number = None
    return descriptor, error_number


def _fstat_directory(descriptor: int) -> os.stat_result:
    try:
        details = os.fstat(descriptor)
    except OSError:
        details = None
    if details is None or not stat.S_ISDIR(details.st_mode):
        _raise("sbx_observation_archive_root_invalid")
    return details


def _fsync(descriptor: int, reason_id: str) -> None:
    try:
        os.fsync(descriptor)
    except OSError:
        failed = True
    else:
        failed = False
    if failed:
        _raise(reason_id)


def _set_and_verify_mode(
    descriptor: int,
    expected_mode: int,
    *,
    regular_file: bool,
    reason_id: str,
) -> os.stat_result:
    try:
        os.fchmod(descriptor, expected_mode)
        details = os.fstat(descriptor)
    except OSError:
        details = None
    expected_kind = stat.S_ISREG if regular_file else stat.S_ISDIR
    if (
        details is None
        or not expected_kind(details.st_mode)
        or stat.S_IMODE(details.st_mode) != expected_mode
        or details.st_uid != os.geteuid()
    ):
        _raise(reason_id)
    return details


def _close(descriptor: int | None) -> None:
    if descriptor is None:
        return
    with contextlib.suppress(OSError):
        os.close(descriptor)


def _mkdir_at(parent_fd: int, name: str) -> tuple[bool, int | None]:
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    except OSError as error:
        created = False
        error_number = error.errno
    else:
        created = True
        error_number = None
    return created, error_number


def _open_archive_root(path: Path) -> tuple[int, os.stat_result]:
    flags = _directory_flags()
    try:
        descriptor = os.open(path.anchor, flags)
    except OSError:
        descriptor = None
    if descriptor is None:
        _raise("sbx_observation_archive_root_invalid")

    current = descriptor
    try:
        for component in path.parts[1:]:
            child, error_number = _try_open_directory(component, dir_fd=current)
            if child is None and error_number == errno.ENOENT:
                created, mkdir_error = _mkdir_at(current, component)
                if not created and mkdir_error != errno.EEXIST:
                    _raise("sbx_observation_archive_root_invalid")
                if created:
                    _fsync(current, "sbx_observation_archive_root_sync_failed")
                child, _error_number = _try_open_directory(component, dir_fd=current)
            if child is None:
                _raise("sbx_observation_archive_root_invalid")
            _close(current)
            current = child
            _fstat_directory(current)
        details = _fstat_directory(current)
        if details.st_uid != os.geteuid() or stat.S_IMODE(details.st_mode) & 0o022:
            _raise("sbx_observation_archive_root_permissions_unsafe")
        return current, details
    except BaseException:
        _close(current)
        raise


def _entry_exists(parent_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError:
        failed = True
    else:
        failed = False
    if failed:
        _raise("sbx_observation_archive_root_invalid")
    return True


def _write_file(parent_fd: int, name: str, content: bytes) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        descriptor = os.open(name, flags, 0o600, dir_fd=parent_fd)
    except OSError:
        descriptor = None
    if descriptor is None:
        _raise("sbx_observation_archive_write_failed")
    try:
        _set_and_verify_mode(
            descriptor,
            0o600,
            regular_file=True,
            reason_id="sbx_observation_archive_write_failed",
        )
        view = memoryview(content)
        while view:
            try:
                written = os.write(descriptor, view)
            except OSError:
                written = -1
            if written <= 0:
                _raise("sbx_observation_archive_write_failed")
            view = view[written:]
        _fsync(descriptor, "sbx_observation_archive_sync_failed")
    finally:
        _close(descriptor)


def _atomic_rename_noreplace(parent_fd: int, source: str, destination: str) -> None:
    system = platform.system()
    try:
        library = ctypes.CDLL(None, use_errno=True)
        if system == "Darwin":
            rename = library.renameatx_np
            flag = 0x00000004  # RENAME_EXCL
        elif system == "Linux":
            rename = library.renameat2
            flag = 0x00000001  # RENAME_NOREPLACE
        else:
            raise NotImplementedError
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        result = rename(
            parent_fd,
            source.encode("ascii"),
            parent_fd,
            destination.encode("ascii"),
            flag,
        )
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        result = -1
        ctypes.set_errno(errno.ENOSYS)
    error_number = ctypes.get_errno() if result != 0 else None
    if result == 0:
        return
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        _raise("sbx_observation_archive_not_write_once")
    if error_number in {errno.ENOSYS, errno.ENOTSUP}:
        _raise("sbx_observation_archive_atomic_commit_unsupported")
    _raise("sbx_observation_archive_commit_failed")


def _same_directory(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


class HardenedSbxObservationArchive:
    """Commit an exact no-model observation archive without following links."""

    def __init__(self, root: str | Path) -> None:
        self.root = root

    def write(self, report: SbxObservationReportLike) -> SbxObservationArchiveRecord:
        failure_reason: str | None = None
        try:
            prepared = build_canonical_sbx_observation_payload(report)
        except SbxObservationPayloadError as error:
            failure_reason = error.reason_id
            prepared = None
        if failure_reason is not None or prepared is None:
            _raise(failure_reason or "sbx_observation_archive_report_invalid")
        root_path = _archive_root_path(self.root)
        root_fd, root_details = _open_archive_root(root_path)
        stage_fd: int | None = None
        committed_fd: int | None = None
        try:
            final_name = prepared.probe_id
            staging_name = f".{prepared.probe_id}.staging"
            if _entry_exists(root_fd, final_name):
                _raise("sbx_observation_archive_not_write_once")
            created, error_number = _mkdir_at(root_fd, staging_name)
            if not created:
                if error_number == errno.EEXIST:
                    _raise("sbx_observation_archive_staging_collision")
                _raise("sbx_observation_archive_staging_create_failed")
            _fsync(root_fd, "sbx_observation_archive_root_sync_failed")
            stage_fd, _error_number = _try_open_directory(staging_name, dir_fd=root_fd)
            if stage_fd is None:
                _raise("sbx_observation_archive_staging_invalid")
            stage_details = _set_and_verify_mode(
                stage_fd,
                0o700,
                regular_file=False,
                reason_id="sbx_observation_archive_staging_invalid",
            )
            for artifact in prepared.raw_files:
                _write_file(stage_fd, artifact.filename, artifact.content)

            manifest_bytes = prepared.manifest_bytes
            _write_file(stage_fd, "manifest.json", manifest_bytes)
            _fsync(stage_fd, "sbx_observation_archive_sync_failed")

            _atomic_rename_noreplace(root_fd, staging_name, final_name)
            _fsync(root_fd, "sbx_observation_archive_root_sync_failed")
            committed_fd, _error_number = _try_open_directory(final_name, dir_fd=root_fd)
            if committed_fd is None or not _same_directory(
                stage_details, _fstat_directory(committed_fd)
            ):
                _raise("sbx_observation_archive_commit_identity_changed")

            reopened_fd, reopened_details = _open_archive_root(root_path)
            try:
                if not _same_directory(root_details, reopened_details):
                    _raise("sbx_observation_archive_root_identity_changed")
            finally:
                _close(reopened_fd)

            directory = root_path / final_name
            manifest_path = directory / "manifest.json"
            return SbxObservationArchiveRecord(
                probe_id=prepared.probe_id,
                directory=directory,
                manifest_path=manifest_path,
                manifest_sha256=sha256_bytes(manifest_bytes),
                raw_file_count=len(prepared.raw_files),
            )
        finally:
            _close(committed_fd)
            _close(stage_fd)
            _close(root_fd)


__all__ = [
    "HardenedSbxObservationArchive",
    "SbxObservationArchiveError",
    "SbxObservationArchiveRecord",
    "SbxObservationLike",
    "SbxObservationReportLike",
]
