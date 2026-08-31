"""Immutable event contracts for Oracle Lab.

The event envelope is the public protocol shared by the CLI, workers, and
storage layer.  Domain code should create :class:`Event` instances and append
them through :class:`oracle_lab.store.EventStore`; it must never update an
already appended event.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from collections.abc import Mapping, Sequence
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from oracle_lab.ids import new_id
from oracle_lab.jsonutil import canonical_json

_EVENT_ID_RE = re.compile(r"^evt_[0-7][0-9A-HJKMNP-TV-Z]{25}$")


class ActorKind(StrEnum):
    """Kinds of actors that may originate an event."""

    HUMAN = "human"
    MODEL = "model"
    HOST = "host"
    TOOL = "tool"
    SYSTEM = "system"
    WORKER = "worker"


class EventType(StrEnum):
    """Validated event taxonomy from the development specification."""

    HUMAN_INPUT = "human.input"
    HUMAN_NOTE = "human.note"
    HUMAN_KEEP = "human.keep"
    HUMAN_REJECT = "human.reject"
    HUMAN_STAR = "human.star"
    HUMAN_UNSTAR = "human.unstar"
    HUMAN_PIN = "human.pin"
    HUMAN_UNPIN = "human.unpin"
    HUMAN_QUARANTINE = "human.quarantine"
    HUMAN_REVISIT = "human.revisit"
    HUMAN_REQUEST_PROBE = "human.request_probe"
    HUMAN_REQUEST_TOOL = "human.request_tool"
    HUMAN_REQUEST_COMPARE = "human.request_compare"
    HUMAN_REQUEST_FORK = "human.request_fork"
    HUMAN_CHECKPOINT = "human.checkpoint"
    HUMAN_PAUSE = "human.pause"
    HUMAN_RESUME = "human.resume"
    HUMAN_PATCH_APPROVED = "human.patch_approved"
    HUMAN_PATCH_REJECTED = "human.patch_rejected"

    ORACLE_REQUEST = "oracle.request"
    ORACLE_OUTPUT = "oracle.output"
    ORACLE_ERROR = "oracle.error"
    ORACLE_RETRY = "oracle.retry"
    ORACLE_PROVIDER_FALLBACK = "oracle.provider_fallback"
    ORACLE_CONTEXT_BUILT = "oracle.context_built"
    ORACLE_CONTEXT_TRUNCATED = "oracle.context_truncated"
    ORACLE_CONTEXT_MESSAGE = "oracle.context_message"
    ORACLE_SAMPLE_GROUP_CREATED = "oracle.sample_group_created"

    ANALYSIS_CLAIM_DETECTED = "analysis.claim_detected"
    ANALYSIS_ENTITY_DETECTED = "analysis.entity_detected"
    ANALYSIS_MOTIF_DETECTED = "analysis.motif_detected"
    ANALYSIS_RECURRENCE_DETECTED = "analysis.recurrence_detected"
    ANALYSIS_CONTRADICTION_DETECTED = "analysis.contradiction_detected"
    ANALYSIS_NUMERIC_INCONSISTENCY = "analysis.numeric_inconsistency"
    ANALYSIS_FORMAT_ATTRACTOR_DETECTED = "analysis.format_attractor_detected"
    ANALYSIS_PROBE_PROPOSED = "analysis.probe_proposed"
    ANALYSIS_TOOL_INTENT_DETECTED = "analysis.tool_intent_detected"
    ANALYSIS_CANON_CANDIDATE = "analysis.canon_candidate"
    ANALYSIS_SESSION_SUMMARY_UPDATED = "analysis.session_summary_updated"
    ANALYSIS_NOVELTY_SCORE = "analysis.novelty_score"
    ANALYSIS_NEW_MECHANISM_DETECTED = "analysis.new_mechanism_detected"
    ANALYSIS_PROMOTED_TO_ORACLE = "analysis.promoted_to_oracle"
    ANALYSIS_BRANCH_PROPOSED = "analysis.branch_proposed"

    TOOL_REQUEST = "tool.request"
    TOOL_APPROVED = "tool.approved"
    TOOL_DENIED = "tool.denied"
    TOOL_STARTED = "tool.started"
    TOOL_OUTPUT = "tool.output"
    TOOL_ERROR = "tool.error"
    TOOL_TIMEOUT = "tool.timeout"
    TOOL_VIRTUALIZED = "tool.virtualized"
    TOOL_RESULT_ADAPTED = "tool.result_adapted"

    CLAIM_PROVISIONAL = "claim.provisional"
    CLAIM_OBSERVED = "claim.observed"
    CLAIM_PROMOTED = "claim.promoted"
    CLAIM_DEMOTED = "claim.demoted"
    CLAIM_CONFLICTED = "claim.conflicted"
    CLAIM_SUPERSEDED = "claim.superseded"
    ENTITY_CREATED = "entity.created"
    ENTITY_UPDATED = "entity.updated"
    RELATION_CREATED = "relation.created"
    VIRTUAL_FILE_CREATED = "virtual_file.created"
    VIRTUAL_FILE_UPDATED = "virtual_file.updated"
    VIRTUAL_PROCESS_CREATED = "virtual_process.created"
    VIRTUAL_PROCESS_SIGNAL_RECEIVED = "virtual_process.signal_received"
    VIRTUAL_CLOCK_CREATED = "virtual_clock.created"
    VIRTUAL_CLOCK_SET = "virtual_clock.set"
    VIRTUAL_CLOCK_ADVANCED = "virtual_clock.advanced"
    VIRTUAL_CLOCK_CONTRADICTION_DETECTED = "virtual_clock.contradiction_detected"

    SESSION_FORKED = "session.forked"
    SESSION_MERGED = "session.merged"
    SESSION_CHECKPOINTED = "session.checkpointed"
    SESSION_IMPORTED = "session.imported"
    SESSION_REPLAYED = "session.replayed"
    BRANCH_ARCHIVED = "branch.archived"

    JOB_ENQUEUED = "job.enqueued"
    JOB_LEASED = "job.leased"
    JOB_HEARTBEAT = "job.heartbeat"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_CANCELLED = "job.cancelled"
    JOB_REQUEUED = "job.requeued"
    JOB_RETRIED = "job.retried"

    WORKER_TASK_REQUESTED = "worker.task_requested"
    WORKER_RUN_STARTED = "worker.run_started"
    WORKER_RUN_COMPLETED = "worker.run_completed"
    WORKER_RUN_FAILED = "worker.run_failed"
    WORKER_PATCH_PROPOSED = "worker.patch_proposed"
    WORKER_PATCH_SECURITY_REJECTED = "worker.patch_security_rejected"
    WORKER_PATCH_APPLIED = "worker.patch_applied"
    WORKER_PATCH_CONFLICT = "worker.patch_conflict"
    WORKER_VALIDATION_COMPLETED = "worker.validation_completed"
    WORKER_VALIDATION_FAILED = "worker.validation_failed"

    USAGE_ORACLE = "usage.oracle"
    USAGE_HOST = "usage.host"
    USAGE_TOOL = "usage.tool"

    SYSTEM_AUTOMATION_STOPPED = "system.automation_stopped"


def _freeze_json(value: Any) -> Any:
    """Return an immutable, defensive copy of a JSON-like value."""
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_freeze_json(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("event JSON may not contain NaN or infinity")
    return value


def thaw_json(value: Any) -> Any:
    """Convert an immutable event value back to ordinary JSON containers."""
    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


class Actor(BaseModel):
    """The immutable identity of the human, model, host, or tool acting."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ActorKind
    id: str | None = None

    @field_validator("id")
    @classmethod
    def _non_empty_id(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("actor id may not be blank")
        return value


class Event(BaseModel):
    """A frozen event envelope with deeply immutable payload and metadata.

    ``metadata["schema_version"]`` is the wire-level schema version.  The
    SQLite store extracts the same value into a column and verifies equality
    while reading, so protocol and database migrations cannot silently drift.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", use_enum_values=False)

    id: str = Field(default_factory=lambda: new_id("evt"))
    type: EventType
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))
    session_id: str | None = None
    branch_id: str | None = None
    parent_event_id: str | None = None
    causation_id: str | None = None
    correlation_id: str | None = None
    actor: Actor
    payload: Mapping[str, Any] = Field(default_factory=dict)
    metadata: Mapping[str, Any] = Field(default_factory=lambda: {"schema_version": 1})

    @field_validator("id")
    @classmethod
    def _valid_event_id(cls, value: str) -> str:
        if not _EVENT_ID_RE.fullmatch(value):
            raise ValueError("event id must be a prefixed canonical ULID")
        return value

    @field_validator("created_at")
    @classmethod
    def _timezone_aware(cls, value: dt.datetime) -> dt.datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must include a timezone")
        return value

    @field_validator(
        "session_id",
        "branch_id",
        "parent_event_id",
        "causation_id",
        "correlation_id",
    )
    @classmethod
    def _non_blank_optional(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("event identifiers may not be blank")
        return value

    @field_validator("payload", mode="after")
    @classmethod
    def _immutable_payload(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise TypeError("payload must be a mapping")
        frozen = _freeze_json(value)
        canonical_json(thaw_json(frozen))
        return frozen

    @field_validator("metadata", mode="after")
    @classmethod
    def _immutable_metadata(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise TypeError("metadata must be a mapping")
        version = value.get("schema_version")
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValueError("metadata.schema_version must be a positive integer")
        frozen = _freeze_json(value)
        canonical_json(thaw_json(frozen))
        return frozen

    @field_serializer("payload", "metadata")
    def _serialize_mappings(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)

    @property
    def schema_version(self) -> int:
        """Return the validated wire schema version."""
        return int(self.metadata["schema_version"])

    def to_dict(self) -> dict[str, Any]:
        """Return the public JSON envelope without database-only fields."""
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Self:
        """Validate and freeze a public JSON event envelope."""
        return cls.model_validate(value)

    @classmethod
    def new(
        cls,
        event_type: EventType | str,
        *,
        actor: Actor,
        payload: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        **envelope: Any,
    ) -> Self:
        """Create a new event; direct oracle-like fixtures default to synthetic.

        Genuine oracle outputs are created by ``OracleWorker`` with an explicit
        ``oracle_generated`` origin. Historical outputs are created by the
        importer. A caller using this convenience constructor cannot
        accidentally mint unlabeled genuine oracle material.
        """
        payload_value = {} if payload is None else dict(payload)
        metadata_value = {"schema_version": 1} if metadata is None else dict(metadata)
        if EventType(event_type) is EventType.ORACLE_OUTPUT:
            payload_origin = payload_value.get("material_origin")
            metadata_origin = metadata_value.get("material_origin")
            if (
                payload_origin is not None
                and metadata_origin is not None
                and payload_origin != metadata_origin
            ):
                raise ValueError("oracle.output material_origin labels disagree")
            origin = payload_origin or metadata_origin
            if origin is None:
                if (
                    payload_value.get("historical_fixture") is True
                    or metadata_value.get("historical_fixture") is True
                ):
                    origin = "historical_fixture"
                else:
                    origin = "synthetic_fixture"
            payload_value["material_origin"] = origin
            metadata_value["material_origin"] = origin
            if origin == "synthetic_fixture":
                payload_value.setdefault("synthetic_fixture", True)
                payload_value.setdefault("archive_path", None)
                payload_value.setdefault("archive_sha256", None)
        if EventType(event_type) in {
            EventType.TOOL_OUTPUT,
            EventType.TOOL_ERROR,
            EventType.TOOL_TIMEOUT,
            EventType.TOOL_DENIED,
            EventType.TOOL_VIRTUALIZED,
        }:
            payload_domain = payload_value.get("truth_domain")
            metadata_domain = metadata_value.get("truth_domain")
            if (
                payload_domain is not None
                and metadata_domain is not None
                and payload_domain != metadata_domain
            ):
                raise ValueError("tool result truth_domain labels disagree")
            domain = payload_domain or metadata_domain
            if domain is None:
                domain = "synthetic"
            payload_value["truth_domain"] = domain
            metadata_value["truth_domain"] = domain
        return cls(
            type=event_type,
            actor=actor,
            payload=payload_value,
            metadata=metadata_value,
            **envelope,
        )


def known_event_types() -> frozenset[str]:
    """Return all accepted taxonomy values for policy/config validation."""
    return frozenset(item.value for item in EventType)


__all__ = [
    "Actor",
    "ActorKind",
    "Event",
    "EventType",
    "known_event_types",
    "thaw_json",
]
