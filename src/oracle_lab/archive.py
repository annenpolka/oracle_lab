"""Immutable raw provider-response archive with integrity sidecars."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from oracle_lab.jsonutil import canonical_json, sha256_bytes
from oracle_lab.providers import (
    OracleGenerateRequest,
    OracleGenerateResponse,
    thaw_provider_value,
)

_SAFE_EVENT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$")


class ArchiveError(RuntimeError):
    pass


class ArchiveIntegrityError(ArchiveError):
    pass


@dataclass(frozen=True, slots=True)
class ArchiveRecord:
    event_id: str
    raw_path: Path
    metadata_path: Path
    sha256: str
    size_bytes: int
    created_at: dt.datetime
    metadata: Mapping[str, Any]


def _write_exclusive(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:  # pragma: no cover - defensive OS boundary
                raise ArchiveError(f"short write while archiving {path}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class RawResponseArchive:
    """Write-once archive rooted at ``archive/raw``.

    The raw file is always the exact bytes supplied by the HTTP adapter.  All
    parsed/provider metadata lives in a separate canonical-JSON sidecar.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def paths_for(self, event_id: str, created_at: dt.datetime) -> tuple[Path, Path]:
        if not _SAFE_EVENT_ID.fullmatch(event_id):
            raise ArchiveError("event_id is not safe for archive paths")
        if created_at.tzinfo is None:
            raise ArchiveError("archive timestamps must be timezone-aware")
        directory = (
            self.root
            / f"{created_at.year:04d}"
            / f"{created_at.month:02d}"
            / f"{created_at.day:02d}"
        )
        return directory / f"{event_id}.json", directory / f"{event_id}.metadata.json"

    def write(
        self,
        *,
        event_id: str,
        raw_bytes: bytes,
        metadata: Mapping[str, Any],
        created_at: dt.datetime | None = None,
    ) -> ArchiveRecord:
        timestamp = created_at or dt.datetime.now(dt.UTC)
        raw_path, metadata_path = self.paths_for(event_id, timestamp)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw = bytes(raw_bytes)
        digest = sha256_bytes(raw)
        sidecar = {
            **thaw_provider_value(dict(metadata)),
            "schema_version": 1,
            "event_id": event_id,
            "created_at": timestamp.isoformat(),
            "raw_file": raw_path.name,
            "raw_sha256": digest,
            "raw_size_bytes": len(raw),
        }
        # Raw is committed first: a crash can leave an obviously incomplete
        # sidecar, but can never leave a metadata record pointing at missing or
        # transformed source material.
        try:
            _write_exclusive(raw_path, raw)
            _write_exclusive(
                metadata_path,
                (canonical_json(sidecar) + "\n").encode("utf-8"),
            )
        except FileExistsError as exc:
            raise ArchiveError(f"archive is write-once and already exists: {exc.filename}") from exc
        except OSError as exc:
            raise ArchiveError(f"failed to archive provider response: {exc}") from exc
        return ArchiveRecord(
            event_id=event_id,
            raw_path=raw_path,
            metadata_path=metadata_path,
            sha256=digest,
            size_bytes=len(raw),
            created_at=timestamp,
            metadata=MappingProxyType(sidecar),
        )

    def archive_response(
        self,
        *,
        event_id: str,
        request: OracleGenerateRequest,
        response: OracleGenerateResponse,
        created_at: dt.datetime | None = None,
    ) -> ArchiveRecord:
        generation_settings = {
            **thaw_provider_value(response.generation_settings),
            "model_profile_id": request.model_profile_id,
        }
        if not response.generation_settings:
            generation_settings.update(
                {
                    "temperature": request.temperature,
                    "top_p": request.top_p,
                    "max_tokens": request.max_tokens,
                    "provider_pin": request.provider_pin,
                    "seed": request.seed,
                }
            )
        metadata = {
            "provider_name": response.provider_name,
            "routed_provider_name": response.routed_provider_name,
            "provider_model_id": response.provider_model_id,
            "http_status": response.status_code,
            "http_headers": thaw_provider_value(response.headers),
            "generation_settings": generation_settings,
            "response_timing_ms": response.elapsed_ms,
            "usage": thaw_provider_value(response.usage),
            "reasoning": thaw_provider_value(response.reasoning),
            "finish_reason": response.finish_reason,
            "api_revision": response.api_revision,
            "provider_request_id": response.request_id,
            "request_sha256": request.request_hash,
            "request_metadata": thaw_provider_value(request.metadata),
            "material_origin": response.material_origin,
        }
        return self.write(
            event_id=event_id,
            raw_bytes=response.raw_bytes,
            metadata=metadata,
            created_at=created_at,
        )

    def read(self, record: ArchiveRecord | str | Path) -> bytes:
        path = record.raw_path if isinstance(record, ArchiveRecord) else Path(record)
        raw = path.read_bytes()
        if isinstance(record, ArchiveRecord) and sha256_bytes(raw) != record.sha256:
            raise ArchiveIntegrityError(f"raw archive hash mismatch: {path}")
        return raw

    def read_sidecar(self, record: ArchiveRecord | str | Path) -> dict[str, Any]:
        path = record.metadata_path if isinstance(record, ArchiveRecord) else Path(record)
        try:
            value = json.loads(path.read_bytes())
        except (OSError, json.JSONDecodeError) as exc:
            raise ArchiveError(f"invalid archive sidecar {path}: {exc}") from exc
        if not isinstance(value, dict):
            raise ArchiveError(f"archive sidecar must contain a JSON object: {path}")
        raw_file = value.get("raw_file")
        if (
            not isinstance(raw_file, str)
            or not raw_file
            or Path(raw_file).is_absolute()
            or Path(raw_file).name != raw_file
            or raw_file in {".", ".."}
        ):
            raise ArchiveIntegrityError("archive sidecar contains an unsafe raw_file")
        raw_path = path.with_name(raw_file)
        if not raw_path.is_file():
            raise ArchiveIntegrityError(f"archive sidecar references missing raw file: {raw_path}")
        actual = sha256_bytes(raw_path.read_bytes())
        if actual != value.get("raw_sha256"):
            raise ArchiveIntegrityError(f"raw archive hash mismatch: {raw_path}")
        return value


__all__ = [
    "ArchiveError",
    "ArchiveIntegrityError",
    "ArchiveRecord",
    "RawResponseArchive",
]
