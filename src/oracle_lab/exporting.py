"""Lossless research, transcript, and curated-corpus exports."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from oracle_lab.jsonutil import canonical_json, sha256_bytes, sha256_text
from oracle_lab.material import mapping_is_explicit_worker_artifact
from oracle_lab.public_view import public_view

_SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")
_WORKER_ARTIFACT_NAMES = frozenset(
    {
        "task.json",
        "prompt.txt",
        "command.json",
        "stdout.bin",
        "stderr.bin",
        "patch.diff",
        "metadata.json",
    }
)
_VALIDATION_ARTIFACT_NAMES = frozenset(
    {"task.json", "command.json", "stdout.bin", "stderr.bin", "metadata.json"}
)
_PUBLIC_GENERATION_IDENTITY_FIELDS = (
    "model",
    "provider",
    "context_hash",
    "archive_sha256",
    "archive_size_bytes",
    "archive_byte_count",
)
_PUBLIC_SAMPLING_FIELDS = (
    "temperature",
    "top_p",
    "top_k",
    "min_p",
    "max_tokens",
    "max_completion_tokens",
    "min_tokens",
    "stop_tokens",
    "frequency_penalty",
    "presence_penalty",
    "provider_pin",
    "seed",
    "stop",
)
_PUBLIC_MODEL_IDENTITY_FIELDS = (
    "requested_model_profile_id",
    "requested_model_slug",
    "model_family",
    "checkpoint",
    "runtime",
    "quantization",
    "requested_provider_id",
    "actual_provider",
    "actual_model_identifier",
    "fallback_occurred",
    "unknown_fields",
)
_PUBLIC_PROVIDER_ROUTING_FIELDS = ("pin_provider", "allow_fallback")
_PUBLIC_BUNDLE_REDACTION_POLICY = {
    "policy": "generation_identity_allowlist",
    "omitted_categories": [
        "arbitrary_event_and_payload_metadata",
        "provider_transport_metadata",
        "local_archive_paths",
        "raw_archive_artifacts",
        "worker_and_validation_artifacts",
    ],
    "transformed_categories": [
        "credential_and_cookie_metadata",
        "secret_like_generation_metadata",
    ],
    "preserved_categories": [
        "human_kept_genuine_oracle_text",
        "oracle_text_hash",
        "event_provenance",
        "generation_identity_allowlist",
    ],
}


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        if isinstance(result, Mapping):
            return dict(result)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        result = model_dump(mode="json")
        if isinstance(result, Mapping):
            return dict(result)
    raise TypeError(f"cannot export {type(value).__name__}")


def _payload(event: Mapping[str, Any]) -> dict[str, Any]:
    value = event.get("payload", {})
    return dict(value) if isinstance(value, Mapping) else {}


def _metadata(event: Mapping[str, Any]) -> dict[str, Any]:
    value = event.get("metadata", {})
    return dict(value) if isinstance(value, Mapping) else {}


def _is_human_event(event: Mapping[str, Any]) -> bool:
    actor = event.get("actor")
    return isinstance(actor, Mapping) and actor.get("kind") == "human"


def _is_synthetic_fixture(event: Mapping[str, Any]) -> bool:
    payload = _payload(event)
    metadata = _metadata(event)
    return (
        metadata.get("synthetic_fixture") is True
        or payload.get("synthetic_fixture") is True
        or metadata.get("material_origin") == "synthetic_fixture"
        or payload.get("material_origin") == "synthetic_fixture"
    )


def _is_unlabelled_oracle_output(event: Mapping[str, Any]) -> bool:
    if event.get("type") != "oracle.output":
        return False
    origin = _payload(event).get("material_origin") or _metadata(event).get("material_origin")
    return origin not in {"oracle_generated", "historical_fixture", "synthetic_fixture"}


def _genuine_oracle_material_origin(event: Mapping[str, Any]) -> str | None:
    """Return an explicitly genuine origin for canonical Oracle output only."""

    if event.get("type") != "oracle.output":
        return None
    actor = event.get("actor")
    if not isinstance(actor, Mapping) or actor.get("kind") != "model":
        return None
    payload_origin = _payload(event).get("material_origin")
    metadata_origin = _metadata(event).get("material_origin")
    if payload_origin != metadata_origin:
        return None
    if payload_origin not in {"oracle_generated", "historical_fixture"}:
        return None
    return str(payload_origin)


def _source_event_ids(event: Mapping[str, Any]) -> tuple[str, ...]:
    """Return event references that can carry material-origin lineage."""
    payload = _payload(event)
    identifiers: list[str] = []
    for value in (
        event.get("causation_id"),
        event.get("parent_event_id"),
        payload.get("source_event_id"),
        payload.get("target_event_id"),
        payload.get("event_id"),
        payload.get("verification_source_event_id"),
        payload.get("approver_event_id"),
    ):
        if isinstance(value, str):
            identifiers.append(value)
    values = payload.get("source_event_ids", ())
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        identifiers.extend(value for value in values if isinstance(value, str))
    own_id = event.get("id") or event.get("event_id")
    return tuple(dict.fromkeys(value for value in identifiers if value and value != own_id))


def _without_synthetic_lineage(
    events: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Exclude explicit fixtures and every event transitively derived from them."""
    values = list(events)
    synthetic_ids = {
        _event_id(event)
        for event in values
        if _is_synthetic_fixture(event) or _is_unlabelled_oracle_output(event)
    }
    changed = True
    while changed:
        changed = False
        for event in values:
            event_id = _event_id(event)
            if event_id in synthetic_ids:
                continue
            if any(source_id in synthetic_ids for source_id in _source_event_ids(event)):
                synthetic_ids.add(event_id)
                changed = True
    return [event for event in values if _event_id(event) not in synthetic_ids]


def _without_worker_lineage(
    events: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Exclude worker artifacts and all events that cite them transitively."""

    values = list(events)
    worker_ids = {
        _event_id(event) for event in values if mapping_is_explicit_worker_artifact(event)
    }
    changed = True
    while changed:
        changed = False
        for event in values:
            event_id = _event_id(event)
            if event_id in worker_ids:
                continue
            if any(source_id in worker_ids for source_id in _source_event_ids(event)):
                worker_ids.add(event_id)
                changed = True
    return [event for event in values if _event_id(event) not in worker_ids]


def _raw_text(event: Mapping[str, Any]) -> str | None:
    payload = _payload(event)
    for key in ("raw_text", "text", "content", "output", "note"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    for key in ("raw_text", "text", "content"):
        value = event.get(key)
        if isinstance(value, str):
            return value
    return None


def _event_id(event: Mapping[str, Any]) -> str:
    value = event.get("id") or event.get("event_id")
    if not isinstance(value, str) or not value:
        raise ValueError("exported events require an event ID")
    return value


def _safe_name(value: str) -> str:
    candidate = _SAFE_NAME.sub("_", value).strip("._")
    return candidate or sha256_text(value)[:16]


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        temporary_path.replace(path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _write_text(path: Path, content: str) -> None:
    _atomic_write(path, content.encode("utf-8"))


def _jsonl(values: Iterable[Any]) -> str:
    lines = [canonical_json(_mapping(value)) for value in values]
    return "" if not lines else "\n".join(lines) + "\n"


def _json_document(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"


def _write_raw_record(raw_directory: Path, event_id: str, value: Any) -> list[Path]:
    stem = _safe_name(event_id)
    if isinstance(value, Mapping) and "archive_raw_bytes" in value:
        raw = value["archive_raw_bytes"]
        if not isinstance(raw, (bytes, bytearray)):
            raise TypeError("archive_raw_bytes must be bytes")
        raw_path = raw_directory / f"{stem}.json"
        _atomic_write(raw_path, bytes(raw))
        paths = [raw_path]
        sidecar = value.get("archive_metadata_bytes")
        if sidecar is not None:
            if not isinstance(sidecar, (bytes, bytearray)):
                raise TypeError("archive_metadata_bytes must be bytes")
            metadata_path = raw_directory / f"{stem}.metadata.json"
            _atomic_write(metadata_path, bytes(sidecar))
            paths.append(metadata_path)
        return paths
    if isinstance(value, bytes):
        path = raw_directory / f"{stem}.bin"
        _atomic_write(path, value)
    elif isinstance(value, str):
        path = raw_directory / f"{stem}.txt"
        _write_text(path, value)
    else:
        path = raw_directory / f"{stem}.json"
        _write_text(path, _json_document(value))
    return [path]


def _write_portable_archives(
    root: Path,
    *,
    directory_name: str,
    records: Mapping[str, Mapping[str, bytes]],
    required_names: frozenset[str],
) -> None:
    """Write a closed set of exact archive artifacts into a bundle."""

    for event_id, artifacts in sorted(records.items()):
        safe_event_id = _safe_name(event_id)
        if not event_id or safe_event_id != event_id:
            raise ValueError(f"archive event ID is unsafe for a bundle path: {event_id!r}")
        if set(artifacts) != required_names:
            raise ValueError(
                f"{directory_name} archive {event_id} must contain exactly "
                f"{sorted(required_names)!r}"
            )
        directory = root / directory_name / event_id
        for name in sorted(required_names):
            value = artifacts[name]
            if not isinstance(value, bytes):
                raise TypeError(f"archive artifact {event_id}/{name} must be exact bytes")
            _atomic_write(directory / name, value)


def export_research_bundle(
    destination: str | Path,
    *,
    events: Iterable[Any],
    session_records: Iterable[Any] | None = None,
    raw_records: Mapping[str, Any] | None = None,
    claims: Iterable[Any] = (),
    motifs: Iterable[Any] = (),
    provenance: Iterable[Any] | Mapping[str, Any] = (),
    manifest: Mapping[str, Any] | None = None,
    worker_archives: Mapping[str, Mapping[str, bytes]] | None = None,
    validation_archives: Mapping[str, Mapping[str, bytes]] | None = None,
) -> Path:
    """Write the complete research-bundle layout from Section 29.1."""
    root = Path(destination)
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise FileExistsError("research bundle destination must be absent or an empty directory")
    root.mkdir(parents=True, exist_ok=True)
    raw_directory = root / "raw"
    raw_directory.mkdir(parents=True, exist_ok=True)

    mapped_events = [_mapping(event) for event in events]
    event_values = list(_without_synthetic_lineage(mapped_events))
    mapped_session_records = (
        None if session_records is None else [_mapping(record) for record in session_records]
    )
    allowed_event_ids = {_event_id(event) for event in event_values}
    session_values = (
        [record for record in mapped_session_records if _event_id(record) in allowed_event_ids]
        if mapped_session_records is not None
        else [event for event in event_values if event.get("type") == "oracle.context_built"]
    )

    def cited_allowed(value: Mapping[str, Any]) -> bool:
        direct = value.get("source_event_id")
        if isinstance(direct, str):
            return direct in allowed_event_ids
        raw_sources = value.get("source_event_ids")
        if isinstance(raw_sources, Sequence) and not isinstance(
            raw_sources, (str, bytes, bytearray)
        ):
            sources = [item for item in raw_sources if isinstance(item, str)]
            return bool(sources) and all(item in allowed_event_ids for item in sources)
        return False

    claim_values = [value for claim in claims if cited_allowed(value := _mapping(claim))]
    motif_values = [value for motif in motifs if cited_allowed(value := _mapping(motif))]
    if isinstance(provenance, Mapping):
        provenance_value: Any = {
            str(derived_id): [
                source_id for source_id in source_ids if source_id in allowed_event_ids
            ]
            for derived_id, raw_source_ids in provenance.items()
            if (
                source_ids := (
                    [raw_source_ids]
                    if isinstance(raw_source_ids, str)
                    else [source_id for source_id in raw_source_ids if isinstance(source_id, str)]
                    if isinstance(raw_source_ids, Sequence)
                    else []
                )
            )
            and any(source_id in allowed_event_ids for source_id in source_ids)
        }
    else:
        provenance_value = [value for edge in provenance if cited_allowed(value := _mapping(edge))]

    _write_text(root / "events.jsonl", _jsonl(event_values))
    _write_text(root / "session.jsonl", _jsonl(session_values))
    _write_text(root / "claims.json", _json_document(claim_values))
    _write_text(root / "motifs.json", _json_document(motif_values))
    _write_text(root / "provenance.json", _json_document(provenance_value))

    records = {
        event_id: value
        for event_id, value in dict(raw_records or {}).items()
        if event_id in allowed_event_ids
    }
    for event in event_values:
        if event.get("type") != "oracle.output":
            continue
        event_id = _event_id(event)
        if event_id in records:
            continue
        raw = _raw_text(event)
        if raw is not None:
            records[event_id] = raw
    for event_id, value in records.items():
        _write_raw_record(raw_directory, event_id, value)

    worker_values = dict(worker_archives or {})
    validation_values = dict(validation_archives or {})
    worker_event_ids = {
        _event_id(event)
        for event in event_values
        if event.get("type") in {"worker.run_completed", "worker.run_failed"}
    }
    validation_event_ids = {
        _event_id(event)
        for event in event_values
        if event.get("type") in {"worker.validation_completed", "worker.validation_failed"}
    }
    if set(worker_values) != worker_event_ids:
        raise ValueError(
            "worker archive records must match terminal worker events exactly: "
            f"missing={sorted(worker_event_ids - set(worker_values))!r} "
            f"extra={sorted(set(worker_values) - worker_event_ids)!r}"
        )
    if set(validation_values) != validation_event_ids:
        raise ValueError(
            "validation archive records must match validation events exactly: "
            f"missing={sorted(validation_event_ids - set(validation_values))!r} "
            f"extra={sorted(set(validation_values) - validation_event_ids)!r}"
        )
    _write_portable_archives(
        root,
        directory_name="workers",
        records=worker_values,
        required_names=_WORKER_ARTIFACT_NAMES,
    )
    _write_portable_archives(
        root,
        directory_name="validations",
        records=validation_values,
        required_names=_VALIDATION_ARTIFACT_NAMES,
    )

    file_hashes = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            file_hashes[str(path.relative_to(root))] = sha256_bytes(path.read_bytes())
    manifest_value: dict[str, Any] = {
        "format": "oracle-lab-research-bundle",
        "version": 1,
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "counts": {
            "events": len(event_values),
            "session_records": len(session_values),
            "raw_records": len(records),
            "claims": len(claim_values),
            "motifs": len(motif_values),
        },
        "archive_counts": {
            "workers": len(worker_values),
            "validations": len(validation_values),
        },
        "sha256": file_hashes,
    }
    if manifest:
        manifest_value["metadata"] = dict(manifest)
    _write_text(root / "manifest.json", _json_document(manifest_value))
    return root


def _curation_annotations(events: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    annotations: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        event_type = str(event.get("type", ""))
        if event_type not in {
            "human.keep",
            "human.reject",
            "human.star",
            "human.note",
            "human.quarantine",
            "human.revisit",
        }:
            continue
        if not _is_human_event(event) or _is_synthetic_fixture(event):
            continue
        payload = _payload(event)
        target = (
            payload.get("event_id")
            or payload.get("target_event_id")
            or payload.get("source_event_id")
            or event.get("parent_event_id")
        )
        if isinstance(target, str):
            annotations.setdefault(target, []).append(
                {
                    "event_id": _event_id(event),
                    "action": event_type.removeprefix("human."),
                    "note": payload.get("note"),
                }
            )
    return annotations


def render_transcript(events: Iterable[Any], *, title: str = "Oracle Lab transcript") -> str:
    """Render a human-readable Markdown transcript without rewriting raw text."""
    mapped_events = [_mapping(event) for event in events]
    event_values = list(_without_synthetic_lineage(mapped_events))
    annotations = _curation_annotations(event_values)
    parts = [f"# {title}\n\n"]
    for event in event_values:
        raw = _raw_text(event)
        if raw is None:
            continue
        event_id = _event_id(event)
        payload = _payload(event)
        metadata = _metadata(event)
        model = (
            payload.get("model")
            or payload.get("provider_model_id")
            or payload.get("model_profile_id")
            or metadata.get("model")
            or "unknown"
        )
        provider = (
            payload.get("provider")
            or payload.get("provider_name")
            or metadata.get("provider")
            or "unknown"
        )
        sampling = payload.get("sampling") or metadata.get("sampling") or "unknown"
        timestamp = event.get("created_at") or event.get("timestamp") or "unknown"
        branch = event.get("branch_id") or "unknown"
        curation = annotations.get(event_id, [])
        sampling_text = (
            canonical_json(sampling) if isinstance(sampling, (dict, list)) else str(sampling)
        )
        parts.extend(
            [
                f"## {event.get('type', 'event')} `{event_id}`\n\n",
                f"- timestamp: `{timestamp}`\n",
                f"- model: `{model}`\n",
                f"- provider: `{provider}`\n",
                f"- sampling: `{sampling_text}`\n",
                f"- branch: `{branch}`\n",
                f"- curation: `{canonical_json(curation)}`\n\n",
                (
                    f"<!-- oracle-lab-raw event={event_id} bytes={len(raw.encode('utf-8'))} "
                    f"sha256={sha256_text(raw)} -->\n"
                ),
                raw,
                "\n<!-- /oracle-lab-raw -->\n\n",
            ]
        )
    return "".join(parts)


def export_transcript(
    destination: str | Path,
    *,
    events: Iterable[Any],
    title: str = "Oracle Lab transcript",
) -> Path:
    path = Path(destination)
    _write_text(path, render_transcript(events, title=title))
    return path


def _kept_event_ids(events: Sequence[Mapping[str, Any]]) -> set[str]:
    kept: set[str] = set()
    for event in events:
        if event.get("type") != "human.keep":
            continue
        if not _is_human_event(event) or _is_synthetic_fixture(event):
            continue
        payload = _payload(event)
        target = (
            payload.get("event_id")
            or payload.get("target_event_id")
            or payload.get("source_event_id")
            or event.get("parent_event_id")
        )
        if isinstance(target, str):
            kept.add(target)
    return kept


def selected_corpus_records(
    events: Iterable[Any],
    *,
    provenance: Mapping[str, Sequence[str]] | None = None,
) -> list[dict[str, Any]]:
    """Return only human-kept source outputs with explicit provenance IDs."""
    mapped_events = [_mapping(event) for event in events]
    event_values = list(_without_worker_lineage(_without_synthetic_lineage(mapped_events)))
    excluded_event_ids = {_event_id(event) for event in mapped_events} - {
        _event_id(event) for event in event_values
    }
    kept = _kept_event_ids(event_values)
    by_id = {_event_id(event): event for event in event_values}
    records = []
    for event_id in sorted(kept):
        source = by_id.get(event_id)
        if source is None or _is_synthetic_fixture(source):
            continue
        material_origin = _genuine_oracle_material_origin(source)
        if material_origin is None:
            continue
        raw = _raw_text(source)
        if raw is None:
            continue
        actor = source.get("actor")
        actor_value = dict(actor) if isinstance(actor, Mapping) else None
        provenance_ids = [
            identifier
            for identifier in (provenance or {}).get(event_id, ())
            if identifier not in excluded_event_ids
        ]
        if event_id not in provenance_ids:
            provenance_ids.insert(0, event_id)
        records.append(
            {
                "event_id": event_id,
                "event_type": source.get("type"),
                "actor": actor_value,
                "material_origin": material_origin,
                "session_id": source.get("session_id"),
                "branch_id": source.get("branch_id"),
                "raw_text": raw,
                "raw_sha256": sha256_text(raw),
                "provenance_ids": provenance_ids,
            }
        )
    return records


def export_selected_corpus(
    destination: str | Path,
    *,
    events: Iterable[Any],
    provenance: Mapping[str, Sequence[str]] | None = None,
) -> Path:
    path = Path(destination)
    records = selected_corpus_records(events, provenance=provenance)
    _write_text(path, _jsonl(records))
    return path


def _public_generation_identity(event: Mapping[str, Any]) -> dict[str, Any]:
    """Copy only public generation identity fields and redact their metadata values."""

    payload = _payload(event)
    identity = {
        field: payload[field] for field in _PUBLIC_GENERATION_IDENTITY_FIELDS if field in payload
    }
    if "model" not in identity:
        model = payload.get("provider_model_id") or payload.get("model_profile_id")
        if model is not None:
            identity["model"] = model
    if "provider" not in identity and payload.get("provider_name") is not None:
        identity["provider"] = payload["provider_name"]
    for field in ("sampling", "effective_sampling"):
        if field not in payload:
            continue
        value = payload[field]
        identity[field] = (
            {
                name: value[name]
                for name in _PUBLIC_SAMPLING_FIELDS
                if isinstance(value, Mapping) and name in value
            }
            if isinstance(value, Mapping)
            else value
        )
    if "model_identity" in payload:
        model_identity = payload["model_identity"]
        if isinstance(model_identity, Mapping):
            public_model_identity = {
                name: model_identity[name]
                for name in _PUBLIC_MODEL_IDENTITY_FIELDS
                if name in model_identity
            }
            routing = model_identity.get("provider_routing")
            if isinstance(routing, Mapping):
                public_model_identity["provider_routing"] = {
                    name: routing[name]
                    for name in _PUBLIC_PROVIDER_ROUTING_FIELDS
                    if name in routing
                }
            elif "provider_routing" in model_identity:
                public_model_identity["provider_routing"] = routing
            identity["model_identity"] = public_model_identity
        else:
            identity["model_identity"] = model_identity
    # Generation identity is infrastructure metadata even though the public bundle
    # deliberately does not expose the canonical api_response_metadata envelope.
    viewed = public_view({"api_response_metadata": identity})
    if not isinstance(viewed, Mapping):
        raise TypeError("public view must preserve the generation identity mapping")
    viewed_identity = viewed.get("api_response_metadata")
    if not isinstance(viewed_identity, Mapping):
        raise TypeError("public view must preserve the generation identity payload")
    return dict(viewed_identity)


def public_bundle_records(
    events: Iterable[Any],
    *,
    provenance: Mapping[str, Sequence[str]] | None = None,
) -> list[dict[str, Any]]:
    """Build the non-authoritative public view of Human-kept Oracle outputs."""

    mapped_events = [_mapping(event) for event in events]
    by_id = {_event_id(event): event for event in mapped_events}
    records = selected_corpus_records(mapped_events, provenance=provenance)
    public_records: list[dict[str, Any]] = []
    for record in records:
        source = by_id[record["event_id"]]
        public_record = {
            **record,
            "generation_identity": _public_generation_identity(source),
        }
        viewed_record = public_view(public_record)
        if not isinstance(viewed_record, Mapping):
            raise TypeError("public view must preserve public bundle records")
        public_records.append(dict(viewed_record))
    return public_records


def export_public_bundle(
    destination: str | Path,
    *,
    events: Iterable[Any],
    provenance: Mapping[str, Sequence[str]] | None = None,
) -> Path:
    """Write a non-importable public bundle without canonical provider artifacts."""

    root = Path(destination)
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise FileExistsError("public bundle destination must be absent or an empty directory")
    root.mkdir(parents=True, exist_ok=True)

    records = public_bundle_records(events, provenance=provenance)
    _write_text(root / "records.jsonl", _jsonl(records))
    _write_text(root / "redactions.json", _json_document(_PUBLIC_BUNDLE_REDACTION_POLICY))

    file_hashes = {
        path.name: sha256_bytes(path.read_bytes())
        for path in sorted(root.iterdir())
        if path.is_file()
    }
    manifest = {
        "format": "oracle-lab-public-bundle",
        "version": 1,
        "authority": "derived_public_view",
        "importable": False,
        "content_review_required": True,
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "counts": {"records": len(records)},
        "sha256": file_hashes,
    }
    _write_text(root / "manifest.json", _json_document(manifest))
    return root


__all__ = [
    "export_public_bundle",
    "export_research_bundle",
    "export_selected_corpus",
    "export_transcript",
    "public_bundle_records",
    "render_transcript",
    "selected_corpus_records",
]
