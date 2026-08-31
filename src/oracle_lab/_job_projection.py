"""Rebuildable job projection, independent from the mutable queue service."""

from __future__ import annotations

import sqlite3

from oracle_lab._job_contract import (
    _JOB_EVENT_STATUSES,
    _JOB_LIFECYCLE_EVENT_TYPES,
    Job,
    _iso,
)
from oracle_lab.events import Event, EventType, thaw_json
from oracle_lab.jsonutil import canonical_json


class JobProjection:
    """Rebuild the current queue row from full post-transition snapshots."""

    name = "jobs"
    tables = ("jobs",)

    def apply(self, connection: sqlite3.Connection, event: Event) -> None:
        if event.type not in _JOB_LIFECYCLE_EVENT_TYPES:
            return
        job = Job.model_validate(thaw_json(event.payload))
        if job.status not in _JOB_EVENT_STATUSES[event.type]:
            raise ValueError(f"{event.type.value} cannot project job status {job.status.value}")
        if event.correlation_id is None:
            raise ValueError("job lifecycle events require a correlation ID")
        source_row = None
        if job.source_event_id is not None:
            source_row = connection.execute(
                "SELECT session_id, branch_id, correlation_id FROM events WHERE id = ?",
                (job.source_event_id,),
            ).fetchone()
            if source_row is None:
                raise ValueError(f"job source event does not exist: {job.source_event_id}")
        expected_session_id = (
            job.session_id
            if job.session_id is not None
            else None
            if source_row is None
            else source_row["session_id"]
        )
        expected_branch_id = (
            job.branch_id
            if job.branch_id is not None
            else None
            if source_row is None
            else source_row["branch_id"]
        )
        if event.session_id != expected_session_id or event.branch_id != expected_branch_id:
            raise ValueError("job lifecycle event context does not match its snapshot")
        if (
            source_row is not None
            and source_row["correlation_id"] is not None
            and event.correlation_id != source_row["correlation_id"]
        ):
            raise ValueError("job lifecycle correlation does not match its source event")
        if event.type is EventType.JOB_ENQUEUED:
            expected_source = job.source_event_id
            if event.parent_event_id != expected_source or event.causation_id != expected_source:
                raise ValueError("job.enqueued must be caused by its source event")
        connection.execute(
            """
            INSERT INTO jobs (
                id, kind, status, source_event_id, available_at,
                lease_until, worker_id, attempts, payload_json,
                created_at, updated_at, idempotency_key, priority,
                provider_id, session_id, branch_id, serialize_branch,
                max_attempts, last_error, cancel_requested
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                kind = excluded.kind,
                status = excluded.status,
                source_event_id = excluded.source_event_id,
                available_at = excluded.available_at,
                lease_until = excluded.lease_until,
                worker_id = excluded.worker_id,
                attempts = excluded.attempts,
                payload_json = excluded.payload_json,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                idempotency_key = excluded.idempotency_key,
                priority = excluded.priority,
                provider_id = excluded.provider_id,
                session_id = excluded.session_id,
                branch_id = excluded.branch_id,
                serialize_branch = excluded.serialize_branch,
                max_attempts = excluded.max_attempts,
                last_error = excluded.last_error,
                cancel_requested = excluded.cancel_requested
            """,
            (
                job.id,
                job.kind,
                job.status.value,
                job.source_event_id,
                _iso(job.available_at),
                None if job.lease_until is None else _iso(job.lease_until),
                job.worker_id,
                job.attempts,
                canonical_json(job.payload),
                _iso(job.created_at),
                _iso(job.updated_at),
                job.idempotency_key,
                job.priority,
                job.provider_id,
                job.session_id,
                job.branch_id,
                int(job.serialize_branch),
                job.max_attempts,
                job.last_error,
                int(job.cancel_requested),
            ),
        )


# Preserve the historical public and pickle identity after moving the class.
JobProjection.__module__ = "oracle_lab.jobs"
