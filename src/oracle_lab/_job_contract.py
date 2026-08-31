"""Internal immutable contracts shared by the job queue and its projection."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from oracle_lab.events import EventType, _freeze_json, thaw_json


class JobStatus(StrEnum):
    """Persistent queue states."""

    PENDING = "pending"
    LEASED = "leased"
    COMPLETED = "completed"
    DEAD_LETTER = "dead_letter"
    CANCELLED = "cancelled"


class Job(BaseModel):
    """An immutable snapshot of a mutable queue row."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    kind: str
    status: JobStatus
    source_event_id: str | None = None
    available_at: dt.datetime
    lease_until: dt.datetime | None = None
    worker_id: str | None = None
    attempts: int = 0
    payload: Mapping[str, Any] = Field(default_factory=dict)
    created_at: dt.datetime
    updated_at: dt.datetime
    idempotency_key: str | None = None
    priority: int = 0
    provider_id: str | None = None
    session_id: str | None = None
    branch_id: str | None = None
    serialize_branch: bool = False
    max_attempts: int = 5
    last_error: str | None = None
    cancel_requested: bool = False

    @field_validator("available_at", "lease_until", "created_at", "updated_at")
    @classmethod
    def _aware(cls, value: dt.datetime | None) -> dt.datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("job timestamps must include a timezone")
        return value

    @field_validator("payload", mode="after")
    @classmethod
    def _freeze_payload(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return _freeze_json(value)

    @field_serializer("payload")
    def _serialize_payload(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


_JOB_LIFECYCLE_EVENT_TYPES = frozenset(
    {
        EventType.JOB_ENQUEUED,
        EventType.JOB_LEASED,
        EventType.JOB_HEARTBEAT,
        EventType.JOB_COMPLETED,
        EventType.JOB_FAILED,
        EventType.JOB_CANCELLED,
        EventType.JOB_REQUEUED,
        EventType.JOB_RETRIED,
    }
)
_JOB_EVENT_STATUSES = {
    EventType.JOB_ENQUEUED: frozenset({JobStatus.PENDING}),
    EventType.JOB_LEASED: frozenset({JobStatus.LEASED}),
    EventType.JOB_HEARTBEAT: frozenset({JobStatus.LEASED}),
    EventType.JOB_COMPLETED: frozenset({JobStatus.COMPLETED}),
    EventType.JOB_FAILED: frozenset({JobStatus.PENDING, JobStatus.DEAD_LETTER}),
    EventType.JOB_CANCELLED: frozenset({JobStatus.CANCELLED}),
    EventType.JOB_REQUEUED: frozenset({JobStatus.PENDING, JobStatus.DEAD_LETTER}),
    EventType.JOB_RETRIED: frozenset({JobStatus.PENDING}),
}


def _iso(value: dt.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.isoformat()


# These types have historically been public globals in ``oracle_lab.jobs``.
# Keep that module as their pickle lookup owner while reusing the exact objects
# from this internal contract module.
JobStatus.__module__ = "oracle_lab.jobs"
Job.__module__ = "oracle_lab.jobs"
