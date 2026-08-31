"""Verified reconstruction of portable Oracle Lab research bundles.

The event log is the only imported authority.  Claims, motifs, provenance, and
context snapshot files are integrity-checked bundle evidence, but projections
are always rebuilt from ``events.jsonl``.  Provider archive paths embedded in
exported events are never dereferenced; verified files below ``bundle/raw``
replace them while retaining the old path as audit metadata.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from oracle_lab.archive import RawResponseArchive
from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.ids import new_id
from oracle_lab.jsonutil import sha256_bytes, sha256_json
from oracle_lab.session import (
    BuiltContext,
    ContextConstructionError,
    validate_built_context_sources,
)
from oracle_lab.store import EventStore

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_FILES = {
    "events.jsonl",
    "session.jsonl",
    "claims.json",
    "motifs.json",
    "provenance.json",
}
_COUNT_KEYS = {"events", "session_records", "raw_records", "claims", "motifs"}
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
_WORKER_TERMINAL_TYPES = {EventType.WORKER_RUN_COMPLETED, EventType.WORKER_RUN_FAILED}
_VALIDATION_TYPES = {
    EventType.WORKER_VALIDATION_COMPLETED,
    EventType.WORKER_VALIDATION_FAILED,
}
_JOB_LIFECYCLE_TYPES = {
    EventType.JOB_ENQUEUED,
    EventType.JOB_LEASED,
    EventType.JOB_HEARTBEAT,
    EventType.JOB_COMPLETED,
    EventType.JOB_FAILED,
    EventType.JOB_CANCELLED,
    EventType.JOB_REQUEUED,
    EventType.JOB_RETRIED,
}
_CURATION_TYPES = {
    EventType.HUMAN_KEEP,
    EventType.HUMAN_REJECT,
    EventType.HUMAN_STAR,
    EventType.HUMAN_UNSTAR,
    EventType.HUMAN_PIN,
    EventType.HUMAN_UNPIN,
    EventType.HUMAN_NOTE,
    EventType.HUMAN_QUARANTINE,
    EventType.HUMAN_REVISIT,
}
_EVENT_REFERENCE_KEYS = {
    "analysis_event_ids",
    "analysis_source_event_id",
    "approval_event_id",
    "approver_event_id",
    "assistant_event_ids",
    "attractor_event_id",
    "authorizer_event_id",
    "context_event_id",
    "direct_source_event_ids",
    "equivalent_event_id",
    "event_id",
    "event_ids",
    "evidence_event_ids",
    "fork_event_id",
    "from_event_id",
    "generated_event_ids",
    "import_event_id",
    "input_event_ids",
    "mechanism_event_id",
    "mechanism_source_event_ids",
    "message_event_ids",
    "output_event_id",
    "output_event_ids",
    "preceding_contradiction_event_ids",
    "prompt_event_id",
    "prompt_event_ids",
    "proposal_event_id",
    "removed_source_event_ids",
    "replay_event_id",
    "request_event_id",
    "result_event_id",
    "retained_source_event_ids",
    "root_event_id",
    "source_context_event_id",
    "source_event_id",
    "source_event_ids",
    "stop_event_id",
    "target_event_id",
    "task_event_id",
    "patch_event_id",
    "application_event_id",
    "terminal_event_id",
    "started_event_id",
    "rejection_event_id",
    "produced_event_ids",
    "authorized_curation_event_ids",
    "tip_event_id",
    "tool_output_event_id",
    "tool_request_event_id",
    "truncated_source_event_ids",
    "verification_authorizer_event_id",
    "verification_fork_request_event_id",
    "verification_source_event_id",
}
_EVENT_REFERENCE_SEQUENCE_KEYS = {
    key for key in _EVENT_REFERENCE_KEYS if key.endswith("_event_ids")
} | {"provenance"}
_EVENT_REFERENCE_KEYS.add("provenance")
_OPAQUE_EVENT_DATA_KEYS = {
    "api_response_metadata",
    "historical_identity",
    "model_identity",
    "provider_routing",
    "request_metadata",
    "source_file",
}


class BundleImportError(ValueError):
    """Raised before a bundle can alter the authoritative event store."""


@dataclass(frozen=True, slots=True)
class BundleImportResult:
    """Stable identities and integrity facts produced by one bundle import."""

    session_id: str
    branch_id: str
    audit_event_id: str
    event_ids: tuple[str, ...]
    oracle_output_event_ids: tuple[str, ...]
    human_curation_event_ids: tuple[str, ...]
    raw_event_ids: tuple[str, ...]
    manifest_sha256: str
    source_bundle: str


@dataclass(frozen=True, slots=True)
class _VerifiedBundle:
    root: Path
    manifest: Mapping[str, Any]
    manifest_sha256: str
    files: Mapping[str, Path]
    event_records: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class _ArchiveCopy:
    event_id: str
    raw_bytes: bytes
    sidecar_bytes: bytes
    raw_path: Path
    sidecar_path: Path
    reuse_verified_orphan: bool = False


@dataclass(frozen=True, slots=True)
class _DirectoryArchiveCopy:
    """One complete worker or validation archive prepared for write-once import."""

    event_id: str
    kind: str
    directory: Path
    contents: Mapping[str, bytes]
    reuse_verified_orphan: bool = False


def _load_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise BundleImportError(f"invalid {label}: {error}") from error


def _load_jsonl(path: Path, *, label: str) -> list[Mapping[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise BundleImportError(f"invalid {label}: {error}") from error
    records: list[Mapping[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise BundleImportError(
                f"invalid {label} record at line {line_number}: {error.msg}"
            ) from error
        if not isinstance(value, Mapping):
            raise BundleImportError(f"{label} record at line {line_number} must be an object")
        records.append(dict(value))
    return records


def _safe_manifest_path(value: str) -> PurePosixPath:
    if not value or "\\" in value or "\x00" in value:
        raise BundleImportError(f"unsafe manifest path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise BundleImportError(f"unsafe manifest path: {value!r}")
    if path.as_posix() != value:
        raise BundleImportError(f"non-canonical manifest path: {value!r}")
    return path


def _enumerate_regular_files(root: Path) -> dict[str, Path]:
    if root.is_symlink():
        raise BundleImportError("research bundle root must not be a symlink")
    files: dict[str, Path] = {}
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in directory_names:
            candidate = base / name
            if candidate.is_symlink():
                raise BundleImportError(f"research bundle contains a symlink: {candidate}")
        for name in file_names:
            candidate = base / name
            if candidate.is_symlink() or not candidate.is_file():
                raise BundleImportError(f"research bundle contains a non-regular file: {candidate}")
            relative = candidate.relative_to(root).as_posix()
            _safe_manifest_path(relative)
            files[relative] = candidate
    return files


def _validated_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) != _COUNT_KEYS:
        raise BundleImportError("manifest counts must contain the exact version-1 count keys")
    counts: dict[str, int] = {}
    for key in sorted(_COUNT_KEYS):
        count = value.get(key)
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise BundleImportError(f"manifest count {key!r} must be a non-negative integer")
        counts[key] = count
    return counts


def _parent_history(event: Event, by_id: Mapping[str, Event]) -> set[str]:
    visible: set[str] = set()
    current_id = event.parent_event_id
    while current_id is not None:
        if current_id in visible:
            raise BundleImportError(f"event parent graph contains a cycle at {current_id}")
        current = by_id.get(current_id)
        if current is None:
            raise BundleImportError(f"context event {event.id} has a dangling parent: {current_id}")
        if current.session_id != event.session_id:
            raise BundleImportError(
                f"context event {event.id} has a cross-session parent: {current_id}"
            )
        visible.add(current_id)
        current_id = current.parent_event_id
    return visible


def _validate_branch_lineage(events: Sequence[Event]) -> None:
    by_id = {event.id: event for event in events}
    roots = [
        event
        for event in events
        if event.type is EventType.HUMAN_CHECKPOINT
        and event.payload.get("operation") == "session.created"
    ]
    if len(roots) != 1 or roots[0].branch_id is None:
        raise BundleImportError("research bundle requires exactly one session root event")
    root_branch = roots[0].branch_id
    branch_ids = {event.branch_id for event in events}
    if None in branch_ids:
        raise BundleImportError("every portable research-bundle event requires a branch ID")
    fork_events: dict[str, list[Event]] = {}
    for event in events:
        if event.type is EventType.SESSION_FORKED and event.branch_id is not None:
            fork_events.setdefault(event.branch_id, []).append(event)
    for branch_id in branch_ids - {root_branch}:
        forks = fork_events.get(str(branch_id), [])
        if len(forks) != 1:
            raise BundleImportError(
                f"branch {branch_id} requires exactly one session.forked origin"
            )
    if root_branch in fork_events:
        raise BundleImportError("root branch must not be introduced by session.forked")

    for event in events:
        if event.parent_event_id is None:
            continue
        parent = by_id.get(event.parent_event_id)
        if parent is None or parent.branch_id == event.branch_id:
            continue
        if event.type is not EventType.SESSION_FORKED:
            raise BundleImportError(f"event {event.id} crosses branches without session.forked")
        if (
            event.payload.get("fork_event_id") != parent.id
            or event.payload.get("parent_branch_id") != parent.branch_id
            or event.payload.get("branch_id") != event.branch_id
        ):
            raise BundleImportError(
                f"session.forked event {event.id} disagrees with its branch origin"
            )


def _validate_context_snapshots(
    events: Sequence[Event],
    session_records: Sequence[Event],
) -> None:
    by_id = {event.id: event for event in events}
    context_events = {
        event.id: event for event in events if event.type is EventType.ORACLE_CONTEXT_BUILT
    }
    snapshot_ids = {event.id for event in session_records}
    if snapshot_ids != set(context_events):
        missing = sorted(set(context_events) - snapshot_ids)
        extra = sorted(snapshot_ids - set(context_events))
        raise BundleImportError(
            "session.jsonl must contain every persisted context exactly once: "
            f"missing={missing!r} extra={extra!r}"
        )

    contexts_by_request: dict[str, tuple[str, str]] = {}
    for context in context_events.values():
        raw_messages = context.payload.get("messages")
        if not isinstance(raw_messages, Sequence) or isinstance(
            raw_messages, (str, bytes, bytearray)
        ):
            raise BundleImportError(f"context event {context.id} has no messages list")
        if any(not isinstance(message, Mapping) for message in raw_messages):
            raise BundleImportError(f"context event {context.id} has a non-object message")
        messages = [dict(message) for message in raw_messages]
        digest = sha256_json(messages)
        if context.payload.get("sha256") != digest:
            raise BundleImportError(f"context event {context.id} has a mismatched context hash")

        raw_source_ids = context.payload.get("source_event_ids")
        if not isinstance(raw_source_ids, Sequence) or isinstance(
            raw_source_ids, (str, bytes, bytearray)
        ):
            raise BundleImportError(f"context event {context.id} has no source_event_ids list")
        if any(not isinstance(value, str) or not value for value in raw_source_ids):
            raise BundleImportError(f"context event {context.id} has an invalid source event ID")
        source_ids = tuple(raw_source_ids)
        if len(source_ids) != len(messages):
            raise BundleImportError(
                f"context event {context.id} must cite one source event per message"
            )
        visible_ids = _parent_history(context, by_id)
        outside = [event_id for event_id in source_ids if event_id not in visible_ids]
        if outside:
            raise BundleImportError(
                f"context event {context.id} cites events outside visible history: "
                + ", ".join(outside)
            )

        raw_truncated = context.payload.get("truncated_source_event_ids", ())
        if not isinstance(raw_truncated, Sequence) or isinstance(
            raw_truncated, (str, bytes, bytearray)
        ):
            raise BundleImportError(
                f"context event {context.id} has invalid truncated_source_event_ids"
            )
        if any(not isinstance(value, str) or not value for value in raw_truncated):
            raise BundleImportError(
                f"context event {context.id} has an invalid truncated source event ID"
            )
        truncated_ids = tuple(raw_truncated)
        outside_truncated = [event_id for event_id in truncated_ids if event_id not in visible_ids]
        if outside_truncated:
            raise BundleImportError(
                f"context event {context.id} cites truncated events outside visible history: "
                + ", ".join(outside_truncated)
            )
        original_count = context.payload.get(
            "original_message_count",
            len(messages) + len(truncated_ids),
        )
        if (
            isinstance(original_count, bool)
            or not isinstance(original_count, int)
            or original_count < len(messages)
        ):
            raise BundleImportError(
                f"context event {context.id} has an invalid original_message_count"
            )
        strategy = context.payload.get("truncation_strategy")
        if strategy is not None and not isinstance(strategy, str):
            raise BundleImportError(
                f"context event {context.id} has an invalid truncation strategy"
            )

        request_id = context.causation_id
        request = by_id.get(request_id) if request_id is not None else None
        built_context = BuiltContext(
            messages=tuple(messages),
            sha256=digest,
            source_event_ids=source_ids,
            session_id=str(context.session_id),
            branch_id=str(context.branch_id),
            original_message_count=original_count,
            truncated_source_event_ids=truncated_ids,
            truncation_strategy=strategy,
        )
        model_profile_id = (
            request.payload.get("model_profile_id")
            if request is not None and request.type is EventType.ORACLE_REQUEST
            else None
        )
        context_policy = (
            request.payload.get("context_policy")
            if request is not None and request.type is EventType.ORACLE_REQUEST
            else None
        )
        include_reasoning = (
            context_policy.get("include_reasoning_in_next_turn")
            if isinstance(context_policy, Mapping)
            else None
        )
        try:
            validate_built_context_sources(
                built_context,
                [event for event in events if event.id in visible_ids],
                model_profile_id=(model_profile_id if isinstance(model_profile_id, str) else None),
                include_reasoning=(
                    include_reasoning if isinstance(include_reasoning, bool) else None
                ),
            )
        except ContextConstructionError as error:
            raise BundleImportError(
                f"context event {context.id} has invalid message provenance: {error}"
            ) from error
        if request is not None and request.type is EventType.ORACLE_REQUEST:
            request_hash = request.payload.get("context_hash")
            if request_hash is not None and request_hash != digest:
                raise BundleImportError(
                    f"oracle request {request.id} disagrees with context {context.id}"
                )
            prior = contexts_by_request.get(request.id)
            if prior is not None and prior != (context.id, digest):
                raise BundleImportError(
                    f"oracle request {request.id} has multiple persisted contexts"
                )
            contexts_by_request[request.id] = (context.id, digest)

    for output in events:
        if output.type is not EventType.ORACLE_OUTPUT or output.causation_id is None:
            continue
        related = contexts_by_request.get(output.causation_id)
        origin = output.payload.get("material_origin") or output.metadata.get("material_origin")
        if related is None:
            if origin != "synthetic_fixture":
                raise BundleImportError(
                    f"oracle output {output.id} has no persisted request context"
                )
            continue
        if output.payload.get("context_hash") != related[1]:
            raise BundleImportError(
                f"oracle output {output.id} disagrees with context {related[0]}"
            )


def _verify_bundle(source: str | Path) -> _VerifiedBundle:
    root = Path(source).expanduser()
    if root.is_symlink():
        raise BundleImportError("research bundle root must not be a symlink")
    if not root.is_dir():
        raise BundleImportError(f"research bundle is not a directory: {root}")
    root = root.resolve()
    files = _enumerate_regular_files(root)
    manifest_path = files.get("manifest.json")
    if manifest_path is None:
        raise BundleImportError("research bundle is missing manifest.json")
    manifest_raw = manifest_path.read_bytes()
    try:
        manifest = json.loads(manifest_raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise BundleImportError(f"invalid manifest.json: {error}") from error
    if not isinstance(manifest, Mapping):
        raise BundleImportError("manifest.json must contain an object")
    if manifest.get("format") != "oracle-lab-research-bundle":
        raise BundleImportError("unsupported research bundle format")
    if manifest.get("version") != 1:
        raise BundleImportError("unsupported research bundle version")
    counts = _validated_counts(manifest.get("counts"))
    archive_counts = manifest.get("archive_counts")
    if archive_counts is not None:
        if not isinstance(archive_counts, Mapping) or set(archive_counts) != {
            "workers",
            "validations",
        }:
            raise BundleImportError("manifest archive_counts must contain workers and validations")
        for key in ("workers", "validations"):
            value = archive_counts.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise BundleImportError(
                    f"manifest archive count {key!r} must be a non-negative integer"
                )
    declared = manifest.get("sha256")
    if not isinstance(declared, Mapping):
        raise BundleImportError("manifest sha256 must contain a path-to-hash object")

    declared_hashes: dict[str, str] = {}
    for raw_name, raw_digest in declared.items():
        if not isinstance(raw_name, str) or not isinstance(raw_digest, str):
            raise BundleImportError("manifest hashes require string paths and digests")
        name = _safe_manifest_path(raw_name).as_posix()
        if name == "manifest.json":
            raise BundleImportError("manifest.json must not hash itself")
        if not _SHA256_RE.fullmatch(raw_digest):
            raise BundleImportError(f"invalid SHA-256 digest for {name}")
        declared_hashes[name] = raw_digest
    if not set(declared_hashes) >= _REQUIRED_FILES:
        missing = sorted(_REQUIRED_FILES - set(declared_hashes))
        raise BundleImportError("manifest is missing required files: " + ", ".join(missing))
    actual_names = set(files) - {"manifest.json"}
    if actual_names != set(declared_hashes):
        missing = sorted(set(declared_hashes) - actual_names)
        extra = sorted(actual_names - set(declared_hashes))
        detail = []
        if missing:
            detail.append("missing=" + ",".join(missing))
        if extra:
            detail.append("unlisted=" + ",".join(extra))
        raise BundleImportError("bundle file set differs from manifest: " + " ".join(detail))
    for name, expected in declared_hashes.items():
        if sha256_bytes(files[name].read_bytes()) != expected:
            raise BundleImportError(f"bundle file hash mismatch: {name}")

    # Only the documented version-1 layout is accepted. Optional worker and
    # validation archives extend v1 without making historical v1 bundles
    # unreadable. The closed filename sets prevent a validly hashed executable
    # or unrelated nested path from becoming an attractive target later.
    for name in actual_names:
        if name in _REQUIRED_FILES:
            continue
        parts = PurePosixPath(name).parts
        if len(parts) == 2 and parts[0] == "raw":
            continue
        if (
            len(parts) == 3
            and parts[0] == "workers"
            and parts[1]
            and _safe_manifest_path(parts[1]).as_posix() == parts[1]
            and parts[2] in _WORKER_ARTIFACT_NAMES
        ):
            continue
        if (
            len(parts) == 3
            and parts[0] == "validations"
            and parts[1]
            and _safe_manifest_path(parts[1]).as_posix() == parts[1]
            and parts[2] in _VALIDATION_ARTIFACT_NAMES
        ):
            continue
        else:
            raise BundleImportError(f"unexpected version-1 bundle path: {name}")

    event_records = _load_jsonl(files["events.jsonl"], label="events.jsonl")
    session_records = _load_jsonl(files["session.jsonl"], label="session.jsonl")
    claims = _load_json(files["claims.json"], label="claims.json")
    motifs = _load_json(files["motifs.json"], label="motifs.json")
    _load_json(files["provenance.json"], label="provenance.json")
    if not isinstance(claims, list) or not isinstance(motifs, list):
        raise BundleImportError("claims.json and motifs.json must contain arrays")
    raw_records = sum(
        1
        for name in actual_names
        if name.startswith("raw/") and not name.endswith(".metadata.json")
    )
    observed_counts = {
        "events": len(event_records),
        "session_records": len(session_records),
        "raw_records": raw_records,
        "claims": len(claims),
        "motifs": len(motifs),
    }
    if observed_counts != counts:
        raise BundleImportError(
            f"manifest counts do not match bundle contents: {observed_counts!r}"
        )

    event_by_id: dict[str, Mapping[str, Any]] = {}
    validated_events: list[Event] = []
    for record in event_records:
        event = Event.from_dict(record)
        if event.id in event_by_id:
            raise BundleImportError(f"events.jsonl contains duplicate event ID: {event.id}")
        event_by_id[event.id] = record
        validated_events.append(event)

    worker_event_ids = {
        event.id for event in validated_events if event.type in _WORKER_TERMINAL_TYPES
    }
    validation_event_ids = {
        event.id for event in validated_events if event.type in _VALIDATION_TYPES
    }
    actual_worker_paths = {name for name in actual_names if name.startswith("workers/")}
    expected_worker_paths = {
        f"workers/{event_id}/{name}"
        for event_id in worker_event_ids
        for name in _WORKER_ARTIFACT_NAMES
    }
    actual_validation_paths = {name for name in actual_names if name.startswith("validations/")}
    expected_validation_paths = {
        f"validations/{event_id}/{name}"
        for event_id in validation_event_ids
        for name in _VALIDATION_ARTIFACT_NAMES
    }
    if actual_worker_paths != expected_worker_paths:
        raise BundleImportError("bundle worker archives do not match terminal worker events")
    if actual_validation_paths != expected_validation_paths:
        raise BundleImportError("bundle validation archives do not match validation events")
    if isinstance(archive_counts, Mapping) and (
        archive_counts.get("workers") != len(worker_event_ids)
        or archive_counts.get("validations") != len(validation_event_ids)
    ):
        raise BundleImportError("manifest archive counts do not match bundle events")
    seen_snapshots: set[str] = set()
    validated_snapshots: list[Event] = []
    for record in session_records:
        snapshot = Event.from_dict(record)
        if snapshot.type is not EventType.ORACLE_CONTEXT_BUILT:
            raise BundleImportError("session.jsonl may contain only oracle.context_built events")
        if snapshot.id in seen_snapshots:
            raise BundleImportError(f"session.jsonl contains duplicate snapshot: {snapshot.id}")
        seen_snapshots.add(snapshot.id)
        source = event_by_id.get(snapshot.id)
        if source is None or Event.from_dict(source).to_dict() != snapshot.to_dict():
            raise BundleImportError(
                f"session.jsonl snapshot does not match events.jsonl: {snapshot.id}"
            )
        validated_snapshots.append(snapshot)
    _validate_branch_lineage(validated_events)
    _validate_context_snapshots(validated_events, validated_snapshots)

    return _VerifiedBundle(
        root=root,
        manifest=dict(manifest),
        manifest_sha256=sha256_bytes(manifest_raw),
        files=files,
        event_records=tuple(event_records),
    )


def _payload_event_references(
    payload: Mapping[str, Any],
    *,
    descend: bool = True,
) -> set[str]:
    references: set[str] = set()

    def visit(value: Any, key: str | None = None) -> None:
        if key in _OPAQUE_EVENT_DATA_KEYS:
            return
        if key in _EVENT_REFERENCE_KEYS and key not in _EVENT_REFERENCE_SEQUENCE_KEYS:
            if isinstance(value, str):
                references.add(value)
            return
        if key in _EVENT_REFERENCE_SEQUENCE_KEYS:
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                references.update(item for item in value if isinstance(item, str))
            return
        if descend and isinstance(value, Mapping):
            for child_key, child in value.items():
                visit(child, str(child_key))
        elif (
            descend
            and isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray))
        ):
            for child in value:
                visit(child)

    for top_key, top_value in payload.items():
        visit(top_value, str(top_key))
    return references


def _event_dependencies(event: Event) -> set[str]:
    dependencies = _payload_event_references(event.payload)
    dependencies.update(_payload_event_references(event.metadata, descend=False))
    dependencies.update(
        identifier
        for identifier in (event.parent_event_id, event.causation_id)
        if identifier is not None
    )
    if event.id in dependencies:
        raise BundleImportError(f"event {event.id} contains a self-reference")
    return dependencies


def _topological_events(events: Sequence[Event]) -> tuple[Event, ...]:
    by_id = {event.id: event for event in events}
    if len(by_id) != len(events):
        raise BundleImportError("bundle contains duplicate event IDs")

    def order_key(event_id: str) -> tuple[dt.datetime, str]:
        event = by_id[event_id]
        return event.created_at, event.id

    dependencies: dict[str, set[str]] = {}
    dependants: dict[str, set[str]] = {event.id: set() for event in events}
    for event in events:
        refs = _event_dependencies(event)
        missing = sorted(refs - set(by_id))
        if missing:
            raise BundleImportError(
                f"event {event.id} has dangling event references: {', '.join(missing)}"
            )
        for reference in refs:
            target = by_id[reference]
            if target.session_id != event.session_id:
                raise BundleImportError(
                    f"event {event.id} has a cross-session reference to {reference}"
                )
            dependants[reference].add(event.id)
        dependencies[event.id] = refs

    ready = sorted(
        (event.id for event in events if not dependencies[event.id]),
        key=order_key,
    )
    ordered: list[Event] = []
    emitted: set[str] = set()
    while ready:
        event_id = ready.pop(0)
        ordered.append(by_id[event_id])
        emitted.add(event_id)
        for dependant in sorted(dependants[event_id], key=order_key):
            dependencies[dependant].discard(event_id)
            if not dependencies[dependant] and dependant not in emitted and dependant not in ready:
                ready.append(dependant)
        ready.sort(key=order_key)
    if len(ordered) != len(events):
        cyclic = sorted(event_id for event_id, refs in dependencies.items() if refs)
        raise BundleImportError("bundle event graph contains a cycle: " + ", ".join(cyclic))
    return tuple(ordered)


def _repoint_archives(
    bundle: _VerifiedBundle,
    events: Sequence[Event],
    *,
    archive_root: str | Path | None,
) -> tuple[
    tuple[Event, ...],
    dict[str, dict[str, Any]],
    tuple[_ArchiveCopy, ...],
]:
    rewritten: list[Event] = []
    repoints: dict[str, dict[str, Any]] = {}
    copies: list[_ArchiveCopy] = []
    destination_root: Path | None = None
    archive: RawResponseArchive | None = None
    for event in events:
        if event.type is not EventType.ORACLE_OUTPUT:
            rewritten.append(event)
            continue
        payload = thaw_json(event.payload)
        metadata = thaw_json(event.metadata)
        original_path = payload.get("archive_path")
        origin = payload.get("material_origin") or metadata.get("material_origin")
        requires_archive = origin == "oracle_generated"
        if requires_archive and not isinstance(original_path, str):
            raise BundleImportError(
                f"oracle_generated output has no original archive reference: {event.id}"
            )
        if not requires_archive and not isinstance(original_path, str):
            rewritten.append(event)
            continue
        if archive_root is None:
            raise BundleImportError(
                "importing archived oracle output requires a local archive_root"
            )
        if destination_root is None:
            destination_root = Path(archive_root).expanduser().resolve(strict=False)
            if destination_root.is_relative_to(bundle.root):
                raise BundleImportError("local import archive must be outside the portable bundle")
            archive = RawResponseArchive(destination_root)
        assert archive is not None
        raw_name = f"raw/{event.id}.json"
        sidecar_name = f"raw/{event.id}.metadata.json"
        raw_path = bundle.files.get(raw_name)
        sidecar_path = bundle.files.get(sidecar_name)
        if raw_path is None or sidecar_path is None:
            raise BundleImportError(
                f"archived oracle output is missing bundle raw files: {event.id}"
            )
        raw = raw_path.read_bytes()
        sidecar_bytes = sidecar_path.read_bytes()
        digest = sha256_bytes(raw)
        if payload.get("archive_sha256") != digest:
            raise BundleImportError(f"oracle archive hash mismatch for {event.id}")
        try:
            sidecar = json.loads(sidecar_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise BundleImportError(f"invalid {sidecar_name}: {error}") from error
        if not isinstance(sidecar, Mapping):
            raise BundleImportError(f"archive sidecar must contain an object: {event.id}")
        if (
            sidecar.get("event_id") != event.id
            or sidecar.get("raw_file") != raw_path.name
            or sidecar.get("raw_sha256") != digest
            or sidecar.get("raw_size_bytes") != len(raw)
            or sidecar.get("material_origin") != origin
        ):
            raise BundleImportError(f"archive sidecar does not match raw response: {event.id}")
        request_sha256 = sidecar.get("request_sha256")
        if not isinstance(request_sha256, str) or not _SHA256_RE.fullmatch(request_sha256):
            raise BundleImportError(f"archive sidecar has no valid request hash: {event.id}")
        imported_raw_path, imported_sidecar_path = archive.paths_for(
            event.id,
            event.created_at,
        )
        raw_exists = imported_raw_path.exists()
        sidecar_exists = imported_sidecar_path.exists()
        reuse_verified_orphan = False
        if raw_exists or sidecar_exists:
            if (
                not raw_exists
                or not sidecar_exists
                or imported_raw_path.is_symlink()
                or imported_sidecar_path.is_symlink()
                or not imported_raw_path.is_file()
                or not imported_sidecar_path.is_file()
            ):
                raise BundleImportError(
                    f"local archive contains an incomplete or unsafe orphan: {event.id}"
                )
            if (
                imported_raw_path.read_bytes() != raw
                or imported_sidecar_path.read_bytes() != sidecar_bytes
            ):
                raise BundleImportError(
                    f"local archive identity collision with different bytes: {event.id}"
                )
            reuse_verified_orphan = True
        audit = {
            "manifest_sha256": bundle.manifest_sha256,
            "original_archive_path": original_path,
            "bundle_archive_path": str(raw_path),
            "imported_archive_path": str(imported_raw_path),
            "archive_sha256": digest,
            "reused_verified_orphan": reuse_verified_orphan,
        }
        payload["archive_path"] = str(imported_raw_path)
        metadata["bundle_import"] = audit
        rewritten.append(
            Event.from_dict({**event.to_dict(), "payload": payload, "metadata": metadata})
        )
        repoints[event.id] = audit
        copies.append(
            _ArchiveCopy(
                event_id=event.id,
                raw_bytes=raw,
                sidecar_bytes=sidecar_bytes,
                raw_path=imported_raw_path,
                sidecar_path=imported_sidecar_path,
                reuse_verified_orphan=reuse_verified_orphan,
            )
        )
    return tuple(rewritten), repoints, tuple(copies)


def _archived_at_from_metadata(
    metadata: Mapping[str, Any],
    *,
    event_id: str,
) -> dt.datetime:
    timestamps = metadata.get("timestamps")
    archived = timestamps.get("archived_at") if isinstance(timestamps, Mapping) else None
    value = archived.get("value") if isinstance(archived, Mapping) else None
    if (
        not isinstance(archived, Mapping)
        or archived.get("status") != "known"
        or not isinstance(value, str)
    ):
        raise BundleImportError(f"archive metadata has no known archived_at: {event_id}")
    try:
        timestamp = dt.datetime.fromisoformat(value)
    except ValueError as error:
        raise BundleImportError(f"archive metadata has invalid archived_at: {event_id}") from error
    if timestamp.tzinfo is None:
        raise BundleImportError(f"archive metadata archived_at is timezone-naive: {event_id}")
    return timestamp


def _portable_archive_contents(
    bundle: _VerifiedBundle,
    event: Event,
    *,
    prefix: str,
    artifact_names: frozenset[str],
) -> tuple[dict[str, bytes], Mapping[str, Any]]:
    manifest = event.payload.get("archive_manifest")
    archive_path = event.payload.get("archive_path")
    if (
        not isinstance(archive_path, str)
        or not archive_path
        or not isinstance(manifest, Mapping)
        or set(manifest) != artifact_names
    ):
        raise BundleImportError(f"event archive manifest is incomplete: {event.id}")
    contents: dict[str, bytes] = {}
    for name in sorted(artifact_names):
        bundled_name = f"{prefix}/{event.id}/{name}"
        bundled_path = bundle.files.get(bundled_name)
        if bundled_path is None:
            raise BundleImportError(f"bundle archive artifact is missing: {bundled_name}")
        integrity = manifest[name]
        if not isinstance(integrity, Mapping):
            raise BundleImportError(f"archive integrity is invalid: {event.id}/{name}")
        original_artifact_path = integrity.get("path")
        digest = integrity.get("sha256")
        size = integrity.get("size_bytes")
        if (
            not isinstance(original_artifact_path, str)
            or original_artifact_path.replace("\\", "/").rsplit("/", 1)[-1] != name
            or not isinstance(digest, str)
            or not _SHA256_RE.fullmatch(digest)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
        ):
            raise BundleImportError(f"archive identity is invalid: {event.id}/{name}")
        content = bundled_path.read_bytes()
        if len(content) != size or sha256_bytes(content) != digest:
            raise BundleImportError(
                f"archive artifact differs from event manifest: {event.id}/{name}"
            )
        contents[name] = content
    try:
        metadata = json.loads(contents["metadata.json"])
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise BundleImportError(f"archive metadata is invalid: {event.id}") from error
    if not isinstance(metadata, Mapping):
        raise BundleImportError(f"archive metadata must contain an object: {event.id}")
    return contents, metadata


def _verify_directory_orphan(directory: Path, contents: Mapping[str, bytes], *, label: str) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise BundleImportError(f"local {label} archive is an unsafe orphan: {directory}")
    try:
        entries = tuple(directory.iterdir())
    except OSError as error:
        raise BundleImportError(
            f"local {label} archive cannot be inspected: {directory}"
        ) from error
    if {entry.name for entry in entries} != set(contents):
        raise BundleImportError(f"local {label} archive orphan is incomplete: {directory}")
    for name, expected in contents.items():
        path = directory / name
        if path.is_symlink() or not path.is_file() or path.read_bytes() != expected:
            raise BundleImportError(
                f"local {label} archive identity collision with different bytes: {path}"
            )


def _validate_destination_components(root: Path, directory: Path, *, label: str) -> None:
    """Reject symlinks and non-directories inside a managed archive root."""

    try:
        relative_parts = directory.relative_to(root).parts
    except ValueError as error:  # pragma: no cover - directory builders are root-bound
        raise BundleImportError(f"local {label} archive escapes its configured root") from error
    components = [*reversed(root.parents), root]
    current = root
    for part in relative_parts:
        current /= part
        components.append(current)
    for current in components:
        if current.is_symlink():
            raise BundleImportError(f"local {label} archive contains a symlink: {current}")
        if current.exists() and not current.is_dir():
            raise BundleImportError(
                f"local {label} archive contains a non-directory component: {current}"
            )


def _repoint_worker_archives(
    bundle: _VerifiedBundle,
    events: Sequence[Event],
    *,
    worker_archive_root: str | Path | None,
    validation_archive_root: str | Path | None,
) -> tuple[tuple[Event, ...], dict[str, dict[str, Any]], tuple[_DirectoryArchiveCopy, ...]]:
    worker_events = [event for event in events if event.type in _WORKER_TERMINAL_TYPES]
    validation_events = [event for event in events if event.type in _VALIDATION_TYPES]
    if worker_events and worker_archive_root is None:
        raise BundleImportError("importing worker runs requires a local worker_archive_root")
    if validation_events and validation_archive_root is None:
        raise BundleImportError(
            "importing sandbox validations requires a local validation_archive_root"
        )

    from oracle_lab.validation_archive import SandboxValidationArchive
    from oracle_lab.worker_archive import WorkerRunArchive

    worker_root = (
        None if worker_archive_root is None else Path(worker_archive_root).expanduser().absolute()
    )
    validation_root = (
        None
        if validation_archive_root is None
        else Path(validation_archive_root).expanduser().absolute()
    )
    for label, root in (("worker", worker_root), ("validation", validation_root)):
        if root is not None and root.resolve(strict=False).is_relative_to(bundle.root):
            raise BundleImportError(f"local {label} archive must be outside the portable bundle")

    copies: list[_DirectoryArchiveCopy] = []
    repoints: dict[str, dict[str, Any]] = {}
    destinations: set[Path] = set()
    worker_destination_by_run: dict[str, Path] = {}
    worker_integrity_by_run: dict[str, Mapping[str, Any]] = {}
    event_rewrites: dict[str, tuple[Path, dict[str, Any]]] = {}

    for event in worker_events:
        assert worker_root is not None
        contents, metadata = _portable_archive_contents(
            bundle,
            event,
            prefix="workers",
            artifact_names=_WORKER_ARTIFACT_NAMES,
        )
        run_id = event.payload.get("run_id")
        event_origin = event.metadata.get("artifact_origin")
        if (
            not isinstance(run_id, str)
            or metadata.get("schema_version") != 1
            or metadata.get("run_id") != run_id
            or event_origin not in {"worker_generated", "host_generated"}
            or metadata.get("artifact_origin") != event_origin
        ):
            raise BundleImportError(f"worker archive identity metadata mismatch: {event.id}")
        archived_at = _archived_at_from_metadata(metadata, event_id=event.id)
        try:
            directory = WorkerRunArchive(worker_root).directory_for(run_id, archived_at)
        except (ValueError, RuntimeError) as error:
            raise BundleImportError(f"unsafe worker archive identity: {event.id}") from error
        if directory in destinations or run_id in worker_destination_by_run:
            raise BundleImportError(f"duplicate worker archive identity: {run_id}")
        destinations.add(directory)
        _validate_destination_components(worker_root, directory, label="worker")
        worker_destination_by_run[run_id] = directory
        worker_integrity_by_run[run_id] = event.payload["archive_manifest"]
        reuse = directory.exists() or directory.is_symlink()
        if reuse:
            _verify_directory_orphan(directory, contents, label="worker")
        rewritten_manifest = {
            name: {
                **dict(event.payload["archive_manifest"][name]),
                "path": str(directory / name),
            }
            for name in sorted(_WORKER_ARTIFACT_NAMES)
        }
        event_rewrites[event.id] = (directory, rewritten_manifest)
        original_path = str(event.payload["archive_path"])
        repoints[event.id] = {
            "manifest_sha256": bundle.manifest_sha256,
            "original_archive_path": original_path,
            "imported_archive_path": str(directory),
            "reused_verified_orphan": reuse,
        }
        copies.append(
            _DirectoryArchiveCopy(
                event_id=event.id,
                kind="worker",
                directory=directory,
                contents=contents,
                reuse_verified_orphan=reuse,
            )
        )

    for event in validation_events:
        assert validation_root is not None
        contents, metadata = _portable_archive_contents(
            bundle,
            event,
            prefix="validations",
            artifact_names=_VALIDATION_ARTIFACT_NAMES,
        )
        run_id = metadata.get("run_id")
        validation_id = metadata.get("validation_id")
        if (
            not isinstance(run_id, str)
            or not isinstance(validation_id, str)
            or metadata.get("schema_version") != 1
            or metadata.get("truth_domain") != "sandbox"
            or metadata.get("artifact_origin") != "tool_result"
        ):
            raise BundleImportError(f"validation archive identity metadata mismatch: {event.id}")
        archived_at = _archived_at_from_metadata(metadata, event_id=event.id)
        try:
            directory = SandboxValidationArchive(validation_root).directory_for(
                run_id,
                validation_id,
                archived_at,
            )
        except (ValueError, RuntimeError) as error:
            raise BundleImportError(f"unsafe validation archive identity: {event.id}") from error
        if directory in destinations:
            raise BundleImportError(f"duplicate validation archive identity: {event.id}")
        destinations.add(directory)
        _validate_destination_components(validation_root, directory, label="validation")
        reuse = directory.exists() or directory.is_symlink()
        if reuse:
            _verify_directory_orphan(directory, contents, label="validation")
        rewritten_manifest = {
            name: {
                **dict(event.payload["archive_manifest"][name]),
                "path": str(directory / name),
            }
            for name in sorted(_VALIDATION_ARTIFACT_NAMES)
        }
        event_rewrites[event.id] = (directory, rewritten_manifest)
        repoints[event.id] = {
            "manifest_sha256": bundle.manifest_sha256,
            "original_archive_path": str(event.payload["archive_path"]),
            "imported_archive_path": str(directory),
            "reused_verified_orphan": reuse,
        }
        copies.append(
            _DirectoryArchiveCopy(
                event_id=event.id,
                kind="validation",
                directory=directory,
                contents=contents,
                reuse_verified_orphan=reuse,
            )
        )

    rewritten: list[Event] = []
    for event in events:
        payload = thaw_json(event.payload)
        metadata = thaw_json(event.metadata)
        direct = event_rewrites.get(event.id)
        if direct is not None:
            directory, rewritten_manifest = direct
            payload["archive_path"] = str(directory)
            payload["archive_manifest"] = rewritten_manifest
            if event.type in _WORKER_TERMINAL_TYPES:
                candidate = payload.get("candidate_patch")
                if isinstance(candidate, Mapping):
                    candidate_value = dict(candidate)
                    candidate_value["patch_archive_path"] = str(directory / "patch.diff")
                    payload["candidate_patch"] = candidate_value
            metadata["bundle_import"] = repoints[event.id]
        if event.type is EventType.WORKER_PATCH_PROPOSED:
            run_id = payload.get("worker_run_id")
            directory = worker_destination_by_run.get(str(run_id))
            integrity = worker_integrity_by_run.get(str(run_id))
            if directory is None or not isinstance(integrity, Mapping):
                raise BundleImportError(
                    f"worker patch has no portable terminal archive: {event.id}"
                )
            patch_integrity = integrity.get("patch.diff")
            if (
                not isinstance(patch_integrity, Mapping)
                or patch_integrity.get("sha256") != payload.get("patch_sha256")
                or patch_integrity.get("size_bytes") != payload.get("patch_size_bytes")
            ):
                raise BundleImportError(
                    f"worker patch identity differs from portable archive: {event.id}"
                )
            original_patch_path = payload.get("patch_archive_path")
            payload["patch_archive_path"] = str(directory / "patch.diff")
            patch_audit = {
                "manifest_sha256": bundle.manifest_sha256,
                "original_patch_archive_path": original_patch_path,
                "imported_patch_archive_path": str(directory / "patch.diff"),
            }
            metadata["bundle_import"] = patch_audit
            repoints[event.id] = patch_audit
        rewritten.append(
            Event.from_dict({**event.to_dict(), "payload": payload, "metadata": metadata})
        )
    return tuple(rewritten), repoints, tuple(copies)


def _write_exclusive(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as error:
        raise BundleImportError(
            f"local archive is write-once and already exists: {path}"
        ) from error
    try:
        try:
            view = memoryview(content)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:  # pragma: no cover - defensive OS boundary
                    raise BundleImportError(f"short write while importing archive: {path}")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _materialize_archives(copies: Sequence[_ArchiveCopy], created: list[Path]) -> None:
    for copy in copies:
        if copy.reuse_verified_orphan:
            if (
                copy.raw_path.is_symlink()
                or copy.sidecar_path.is_symlink()
                or not copy.raw_path.is_file()
                or not copy.sidecar_path.is_file()
                or copy.raw_path.read_bytes() != copy.raw_bytes
                or copy.sidecar_path.read_bytes() != copy.sidecar_bytes
            ):
                raise BundleImportError(
                    f"verified archive orphan changed during import: {copy.event_id}"
                )
            continue
        _write_exclusive(copy.raw_path, copy.raw_bytes)
        created.append(copy.raw_path)
        _write_exclusive(copy.sidecar_path, copy.sidecar_bytes)
        created.append(copy.sidecar_path)


def _ensure_safe_directory_chain(path: Path, created_directories: list[Path]) -> None:
    missing: list[Path] = []
    current = path
    while not current.exists() and not current.is_symlink():
        missing.append(current)
        parent = current.parent
        if parent == current:  # pragma: no cover - a filesystem root exists
            raise BundleImportError(f"archive directory has no safe anchor: {path}")
        current = parent
    if current.is_symlink() or not current.is_dir():
        raise BundleImportError(f"unsafe archive directory component: {current}")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError as error:
            raise BundleImportError(
                f"archive directory changed concurrently: {directory}"
            ) from error
        created_directories.append(directory)


def _materialize_directory_archives(
    copies: Sequence[_DirectoryArchiveCopy],
    created_files: list[Path],
    created_directories: list[Path],
) -> None:
    for copy in copies:
        if copy.reuse_verified_orphan:
            _verify_directory_orphan(copy.directory, copy.contents, label=copy.kind)
            continue
        _ensure_safe_directory_chain(copy.directory.parent, created_directories)
        if copy.directory.exists() or copy.directory.is_symlink():
            raise BundleImportError(
                f"local {copy.kind} archive appeared concurrently: {copy.directory}"
            )
        try:
            copy.directory.mkdir(mode=0o700)
        except FileExistsError as error:
            raise BundleImportError(
                f"local {copy.kind} archive appeared concurrently: {copy.directory}"
            ) from error
        created_directories.append(copy.directory)
        for name in sorted(copy.contents):
            if copy.directory.is_symlink() or not copy.directory.is_dir():
                raise BundleImportError(
                    f"local {copy.kind} archive changed during import: {copy.directory}"
                )
            path = copy.directory / name
            _write_exclusive(path, copy.contents[name])
            created_files.append(path)
        _verify_directory_orphan(copy.directory, copy.contents, label=copy.kind)


def _existing_values(
    store: EventStore,
    *,
    table: str,
    column: str,
    values: Sequence[str],
) -> set[str]:
    existing: set[str] = set()
    for offset in range(0, len(values), 500):
        chunk = tuple(values[offset : offset + 500])
        if not chunk:
            continue
        rows = store.connection.execute(
            f"SELECT {column} FROM {table} WHERE {column} IN ({','.join('?' for _ in chunk)})",
            chunk,
        )
        existing.update(str(row[0]) for row in rows if row[0] is not None)
    return existing


def _mark_imported_worker_authority(events: Sequence[Event]) -> tuple[Event, ...]:
    """Keep imported worker history while removing local execution authority."""

    rewritten: list[Event] = []
    for event in events:
        if not event.type.value.startswith("worker.") and event.type not in {
            EventType.HUMAN_PATCH_APPROVED,
            EventType.HUMAN_PATCH_REJECTED,
        }:
            rewritten.append(event)
            continue
        metadata = thaw_json(event.metadata)
        metadata["bundle_import_authority"] = "historical_only"
        rewritten.append(Event.from_dict({**event.to_dict(), "metadata": metadata}))
    return tuple(rewritten)


def _pending_imported_worker_jobs(events: Sequence[Event]) -> tuple[Event, ...]:
    """Return latest runnable worker-job snapshots that must be quarantined."""

    worker_task_job_ids = {
        str(event.payload["job_id"])
        for event in events
        if event.type is EventType.WORKER_TASK_REQUESTED
        and isinstance(event.payload.get("job_id"), str)
    }
    latest: dict[str, tuple[tuple[dt.datetime, dt.datetime, str], Event]] = {}
    for event in events:
        if event.type not in _JOB_LIFECYCLE_TYPES:
            continue
        job_id = event.payload.get("id")
        kind = event.payload.get("kind")
        status = event.payload.get("status")
        if (
            not isinstance(job_id, str)
            or not isinstance(kind, str)
            or status not in {"pending", "leased"}
            or (job_id not in worker_task_job_ids and not kind.startswith("worker."))
        ):
            continue
        raw_updated_at = event.payload.get("updated_at")
        try:
            updated_at = (
                dt.datetime.fromisoformat(raw_updated_at)
                if isinstance(raw_updated_at, str)
                else event.created_at
            )
        except ValueError:
            updated_at = event.created_at
        if updated_at.tzinfo is None:
            updated_at = event.created_at
        key = (updated_at, event.created_at, event.id)
        if job_id not in latest or key > latest[job_id][0]:
            latest[job_id] = (key, event)

    # A later terminal snapshot for a job cancels an earlier pending candidate.
    all_latest: dict[str, tuple[tuple[dt.datetime, dt.datetime, str], Event]] = dict(latest)
    for event in events:
        if event.type not in _JOB_LIFECYCLE_TYPES:
            continue
        job_id = event.payload.get("id")
        if not isinstance(job_id, str) or job_id not in all_latest:
            continue
        raw_updated_at = event.payload.get("updated_at")
        try:
            updated_at = (
                dt.datetime.fromisoformat(raw_updated_at)
                if isinstance(raw_updated_at, str)
                else event.created_at
            )
        except ValueError:
            updated_at = event.created_at
        if updated_at.tzinfo is None:
            updated_at = event.created_at
        key = (updated_at, event.created_at, event.id)
        if key > all_latest[job_id][0]:
            all_latest[job_id] = (key, event)
    return tuple(
        event
        for _, event in sorted(all_latest.values(), key=lambda item: item[0])
        if event.payload.get("status") in {"pending", "leased"}
    )


def _quarantine_job_events(
    jobs: Sequence[Event],
    *,
    audit_event: Event,
) -> tuple[Event, ...]:
    quarantines: list[Event] = []
    timestamp = audit_event.created_at
    for index, event in enumerate(jobs, start=1):
        payload = thaw_json(event.payload)
        payload.update(
            {
                "status": "cancelled",
                "lease_until": None,
                "worker_id": None,
                "cancel_requested": True,
                "updated_at": (timestamp + dt.timedelta(microseconds=index)).isoformat(),
                "last_error": "bundle_import_quarantine",
            }
        )
        quarantines.append(
            Event.new(
                EventType.JOB_CANCELLED,
                id=new_id("evt"),
                created_at=timestamp + dt.timedelta(microseconds=index),
                actor=Actor(kind=ActorKind.SYSTEM, id="bundle-import-quarantine"),
                session_id=event.session_id,
                branch_id=event.branch_id,
                parent_event_id=event.id,
                causation_id=audit_event.id,
                correlation_id=event.correlation_id,
                payload=payload,
                metadata={
                    "schema_version": 1,
                    "bundle_import_quarantine": True,
                    "import_event_id": audit_event.id,
                    "historical_job_event_id": event.id,
                },
            )
        )
    return tuple(quarantines)


class ResearchBundleImporter:
    """Validate and atomically reconstruct one portable research bundle."""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def import_directory(
        self,
        source: str | Path,
        *,
        archive_root: str | Path | None = None,
        worker_archive_root: str | Path | None = None,
        validation_archive_root: str | Path | None = None,
        authorizer: Actor | None = None,
        authorize_human_curation: bool = False,
    ) -> BundleImportResult:
        bundle = _verify_bundle(source)
        parsed = tuple(Event.from_dict(record) for record in bundle.event_records)
        session_ids = {event.session_id for event in parsed}
        if None in session_ids or len(session_ids) != 1:
            raise BundleImportError("a portable research bundle must contain exactly one session")
        session_id = next(iter(session_ids))
        assert session_id is not None
        branch_ids = {event.branch_id for event in parsed}
        if None in branch_ids:
            raise BundleImportError("every portable research-bundle event requires a branch ID")
        manifest_metadata = bundle.manifest.get("metadata", {})
        if manifest_metadata is not None and not isinstance(manifest_metadata, Mapping):
            raise BundleImportError("manifest metadata must contain an object")
        if isinstance(manifest_metadata, Mapping):
            declared_session = manifest_metadata.get("session_id")
            if declared_session is not None and declared_session != session_id:
                raise BundleImportError("manifest session_id does not match events.jsonl")

        curation = tuple(event for event in parsed if event.type in _CURATION_TYPES)
        if curation and (
            not authorize_human_curation
            or authorizer is None
            or authorizer.kind is not ActorKind.HUMAN
        ):
            raise BundleImportError(
                "preserving human curation requires an explicit human-authorized bundle import"
            )
        import_actor = authorizer or Actor(kind=ActorKind.HOST, id="bundle-importer")
        raw_repointed, raw_repoints, archive_copies = _repoint_archives(
            bundle,
            parsed,
            archive_root=archive_root,
        )
        repointed, worker_repoints, directory_archive_copies = _repoint_worker_archives(
            bundle,
            raw_repointed,
            worker_archive_root=worker_archive_root,
            validation_archive_root=validation_archive_root,
        )
        repointed = _mark_imported_worker_authority(repointed)
        repoints = {**raw_repoints, **worker_repoints}
        ordered = _topological_events(repointed)
        quarantined_jobs = _pending_imported_worker_jobs(ordered)

        declared_branch = (
            manifest_metadata.get("current_branch_id")
            if isinstance(manifest_metadata, Mapping)
            else None
        )
        if declared_branch is not None and declared_branch not in branch_ids:
            raise BundleImportError("manifest current_branch_id is not present in events.jsonl")
        if isinstance(declared_branch, str):
            branch_id = declared_branch
        else:
            branch_id = max(repointed, key=lambda item: (item.created_at, item.id)).branch_id
            assert branch_id is not None
        branch_events = [event for event in repointed if event.branch_id == branch_id]
        tip = max(branch_events, key=lambda item: (item.created_at, item.id))
        audit = Event.new(
            EventType.SESSION_IMPORTED,
            id=new_id("evt"),
            created_at=max(
                dt.datetime.now(dt.UTC),
                max(event.created_at for event in repointed) + dt.timedelta(microseconds=1),
            ),
            actor=import_actor,
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=tip.id,
            causation_id=tip.id,
            correlation_id=tip.correlation_id or new_id("corr"),
            payload={
                "operation": "research_bundle.import",
                "bundle_format": bundle.manifest["format"],
                "bundle_version": bundle.manifest["version"],
                "manifest_sha256": bundle.manifest_sha256,
                "source_bundle": str(bundle.root),
                "imported_event_ids": [event.id for event in parsed],
                "human_curation_authorized": bool(curation),
                "authorized_curation_event_ids": [event.id for event in curation],
                "archive_repoints": repoints,
                "quarantined_worker_job_ids": [
                    str(event.payload["id"]) for event in quarantined_jobs
                ],
            },
        )
        quarantine_events = _quarantine_job_events(quarantined_jobs, audit_event=audit)

        imported_event_ids = tuple(event.id for event in parsed)
        imported_session_ids = (session_id,)
        imported_branch_ids = tuple(sorted(str(value) for value in branch_ids))
        created_archive_paths: list[Path] = []
        created_archive_directories: list[Path] = []
        try:
            with self.store.transaction():
                existing_event_ids = _existing_values(
                    self.store,
                    table="events",
                    column="id",
                    values=imported_event_ids,
                )
                existing_sessions = _existing_values(
                    self.store,
                    table="sessions",
                    column="id",
                    values=imported_session_ids,
                ) | _existing_values(
                    self.store,
                    table="events",
                    column="session_id",
                    values=imported_session_ids,
                )
                existing_branches = _existing_values(
                    self.store,
                    table="branches",
                    column="id",
                    values=imported_branch_ids,
                ) | _existing_values(
                    self.store,
                    table="events",
                    column="branch_id",
                    values=imported_branch_ids,
                )
                if existing_event_ids or existing_sessions or existing_branches:
                    collisions = sorted(existing_event_ids | existing_sessions | existing_branches)
                    raise BundleImportError("bundle identity collision: " + ", ".join(collisions))
                _materialize_archives(archive_copies, created_archive_paths)
                _materialize_directory_archives(
                    directory_archive_copies,
                    created_archive_paths,
                    created_archive_directories,
                )
                self.store.append_many((*ordered, audit, *quarantine_events))
                self.store.rebuild_projections()
        except BaseException:
            for created_path in reversed(created_archive_paths):
                created_path.unlink(missing_ok=True)
            for created_directory in reversed(created_archive_directories):
                with contextlib.suppress(OSError):
                    created_directory.rmdir()
            raise

        return BundleImportResult(
            session_id=session_id,
            branch_id=branch_id,
            audit_event_id=audit.id,
            event_ids=imported_event_ids,
            oracle_output_event_ids=tuple(
                event.id for event in parsed if event.type is EventType.ORACLE_OUTPUT
            ),
            human_curation_event_ids=tuple(event.id for event in curation),
            raw_event_ids=tuple(raw_repoints),
            manifest_sha256=bundle.manifest_sha256,
            source_bundle=str(bundle.root),
        )


__all__ = [
    "BundleImportError",
    "BundleImportResult",
    "ResearchBundleImporter",
]
