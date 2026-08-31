"""SQLite job queue with leases, retries, and bounded concurrency."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from collections.abc import Iterable, Mapping
from typing import Any

from oracle_lab._job_contract import _JOB_EVENT_STATUSES as _JOB_EVENT_STATUSES
from oracle_lab._job_contract import _JOB_LIFECYCLE_EVENT_TYPES, _iso
from oracle_lab._job_contract import Job as Job
from oracle_lab._job_contract import JobStatus as JobStatus
from oracle_lab._job_projection import JobProjection as JobProjection
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.ids import new_id
from oracle_lab.jsonutil import canonical_json
from oracle_lab.store import EventNotFoundError, EventStore


class JobQueueError(RuntimeError):
    """Base class for queue errors."""


class JobNotFoundError(JobQueueError):
    """Raised when a requested job does not exist."""


class JobLeaseError(JobQueueError):
    """Raised when a worker attempts an invalid lease transition."""


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class JobQueue:
    """Manage at-least-once work in the event store database.

    Jobs are ordered by descending priority and then availability.  Leases
    increment ``attempts`` before work starts; a worker that dies causes the
    lease to be requeued or moved to the dead-letter state.  Set
    ``serialize_branch=True`` for oracle calls that must be ordered within a
    branch.  Provider limits cap simultaneous active leases per provider.
    """

    def __init__(
        self,
        store: EventStore,
        *,
        provider_limits: Mapping[str, int] | None = None,
        backoff_base_seconds: float = 1.0,
        max_backoff_seconds: float = 300.0,
    ) -> None:
        if backoff_base_seconds < 0 or max_backoff_seconds < 0:
            raise ValueError("backoff durations may not be negative")
        self.store = store
        self.provider_limits = dict(provider_limits or {})
        if any(limit < 1 for limit in self.provider_limits.values()):
            raise ValueError("provider concurrency limits must be positive")
        self.backoff_base_seconds = backoff_base_seconds
        self.max_backoff_seconds = max_backoff_seconds

    def enqueue(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        source_event_id: str | None = None,
        available_at: dt.datetime | None = None,
        idempotency_key: str | None = None,
        priority: int = 0,
        provider_id: str | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
        serialize_branch: bool = False,
        max_attempts: int = 5,
        job_id: str | None = None,
        now: dt.datetime | None = None,
    ) -> Job:
        """Create a pending job, or return the existing idempotent job."""
        if not kind.strip():
            raise ValueError("job kind may not be blank")
        if not isinstance(payload, Mapping):
            raise TypeError("job payload must be a mapping")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if serialize_branch and (session_id is None or branch_id is None):
            raise ValueError("serialized jobs require session_id and branch_id")
        if source_event_id is not None and self.store.get(source_event_id) is None:
            raise EventNotFoundError(f"source event not found: {source_event_id}")
        timestamp = now or _utcnow()
        ready_at = available_at or timestamp
        _iso(timestamp)
        _iso(ready_at)
        payload_json = canonical_json(payload)

        with self.store.transaction() as connection:
            if idempotency_key is not None:
                row = connection.execute(
                    "SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if row is not None:
                    return self._row_to_job(row)
            identifier = job_id or new_id("job")
            try:
                connection.execute(
                    """
                    INSERT INTO jobs (
                        id, kind, status, source_event_id, available_at,
                        lease_until, worker_id, attempts, payload_json,
                        created_at, updated_at, idempotency_key, priority,
                        provider_id, session_id, branch_id, serialize_branch,
                        max_attempts, last_error, cancel_requested
                    ) VALUES (?, ?, ?, ?, ?, NULL, NULL, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)
                    """,
                    (
                        identifier,
                        kind,
                        JobStatus.PENDING.value,
                        source_event_id,
                        _iso(ready_at),
                        payload_json,
                        _iso(timestamp),
                        _iso(timestamp),
                        idempotency_key,
                        priority,
                        provider_id,
                        session_id,
                        branch_id,
                        int(serialize_branch),
                        max_attempts,
                    ),
                )
            except sqlite3.IntegrityError as error:
                if idempotency_key is not None:
                    row = connection.execute(
                        "SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,)
                    ).fetchone()
                    if row is not None:
                        return self._row_to_job(row)
                raise JobQueueError(str(error)) from error
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (identifier,)).fetchone()
            return self._record_transition(connection, EventType.JOB_ENQUEUED, row)

    def lease(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        lease_seconds: float = 60.0,
        kinds: Iterable[str] | None = None,
        excluded_branches: Iterable[tuple[str, str]] | None = None,
        recover_expired_job_ids: Iterable[str] | None = None,
        allow_archive_recovery: bool = False,
        now: dt.datetime | None = None,
    ) -> list[Job]:
        """Lease up to ``limit`` ready jobs while enforcing concurrency rules."""
        if not worker_id.strip():
            raise ValueError("worker_id may not be blank")
        if limit < 1:
            raise ValueError("limit must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if not isinstance(allow_archive_recovery, bool):
            raise TypeError("allow_archive_recovery must be a boolean")
        timestamp = now or _utcnow()
        lease_until = timestamp + dt.timedelta(seconds=lease_seconds)
        kind_values = list(kinds or ())
        branch_exclusions = self._normalize_branch_exclusions(excluded_branches)
        recoverable_expired = self._normalize_job_ids(recover_expired_job_ids)

        with self.store.transaction() as connection:
            self._requeue_expired(
                connection,
                timestamp,
                recover_expired_job_ids=recoverable_expired,
            )
            params: list[Any] = [JobStatus.PENDING.value, _iso(timestamp)]
            where = "status = ? AND available_at <= ? AND cancel_requested = 0"
            if kind_values:
                where += f" AND kind IN ({','.join('?' for _ in kind_values)})"
                params.extend(kind_values)
            candidates = connection.execute(
                f"""
                SELECT * FROM jobs
                WHERE {where}
                ORDER BY priority DESC, available_at ASC, id ASC
                """,
                params,
            ).fetchall()
            leased: list[Job] = []
            for candidate in candidates:
                if len(leased) >= limit:
                    break
                candidate_branch = (candidate["session_id"], candidate["branch_id"])
                if candidate_branch in branch_exclusions:
                    continue
                recovery_metadata = self._pending_archive_recovery_metadata(
                    connection,
                    str(candidate["id"]),
                )
                if recovery_metadata is not None and not allow_archive_recovery:
                    continue
                if not self._within_provider_limit(connection, candidate, timestamp):
                    continue
                if not self._branch_available(connection, candidate, timestamp):
                    continue
                updated = connection.execute(
                    """
                    UPDATE jobs
                    SET status = ?, lease_until = ?, worker_id = ?,
                        attempts = attempts + 1, updated_at = ?
                    WHERE id = ? AND status = ? AND cancel_requested = 0
                    """,
                    (
                        JobStatus.LEASED.value,
                        _iso(lease_until),
                        worker_id,
                        _iso(timestamp),
                        candidate["id"],
                        JobStatus.PENDING.value,
                    ),
                )
                if updated.rowcount:
                    row = connection.execute(
                        "SELECT * FROM jobs WHERE id = ?", (candidate["id"],)
                    ).fetchone()
                    leased.append(
                        self._record_transition(
                            connection,
                            EventType.JOB_LEASED,
                            row,
                            actor=Actor(kind=ActorKind.WORKER, id=worker_id),
                            metadata=(
                                {"lease_expiry_recovery": recovery_metadata}
                                if recovery_metadata is not None
                                else None
                            ),
                        )
                    )
            return leased

    def lease_one(self, worker_id: str, **options: Any) -> Job | None:
        """Lease one job and return ``None`` when no work is ready."""
        jobs = self.lease(worker_id, limit=1, **options)
        return jobs[0] if jobs else None

    def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        *,
        lease_seconds: float = 60.0,
        now: dt.datetime | None = None,
    ) -> Job:
        """Extend a lease owned by ``worker_id``."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        timestamp = now or _utcnow()
        until = timestamp + dt.timedelta(seconds=lease_seconds)
        with self.store.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE jobs SET lease_until = ?, updated_at = ?
                WHERE id = ? AND status = ? AND worker_id = ? AND cancel_requested = 0
                """,
                (
                    _iso(until),
                    _iso(timestamp),
                    job_id,
                    JobStatus.LEASED.value,
                    worker_id,
                ),
            )
            if not updated.rowcount:
                raise JobLeaseError(f"active lease not owned by {worker_id}: {job_id}")
            return self._record_transition(
                connection,
                EventType.JOB_HEARTBEAT,
                self._require_row(connection, job_id),
                actor=Actor(kind=ActorKind.WORKER, id=worker_id),
            )

    def complete(self, job_id: str, *, worker_id: str, now: dt.datetime | None = None) -> Job:
        """Acknowledge successful completion of an active lease."""
        timestamp = now or _utcnow()
        with self.store.transaction() as connection:
            row = self._require_row(connection, job_id)
            self._assert_lease(row, worker_id)
            updated = connection.execute(
                """
                UPDATE jobs
                SET status = ?, lease_until = NULL, worker_id = NULL, updated_at = ?
                WHERE id = ? AND status = ? AND worker_id = ? AND cancel_requested = 0
                """,
                (
                    JobStatus.COMPLETED.value,
                    _iso(timestamp),
                    job_id,
                    JobStatus.LEASED.value,
                    worker_id,
                ),
            )
            if not updated.rowcount:
                raise JobLeaseError(f"lease not owned by {worker_id}: {job_id}")
            return self._record_transition(
                connection,
                EventType.JOB_COMPLETED,
                self._require_row(connection, job_id),
                actor=Actor(kind=ActorKind.WORKER, id=worker_id),
            )

    def fail(
        self,
        job_id: str,
        error: str,
        *,
        worker_id: str,
        retryable: bool = True,
        now: dt.datetime | None = None,
    ) -> Job:
        """Nack a lease, applying exponential backoff or dead-lettering it."""
        timestamp = now or _utcnow()
        with self.store.transaction() as connection:
            row = self._require_row(connection, job_id)
            self._assert_lease(row, worker_id)
            exhausted = int(row["attempts"]) >= int(row["max_attempts"])
            if retryable and not exhausted:
                delay = min(
                    self.max_backoff_seconds,
                    self.backoff_base_seconds * (2 ** max(0, int(row["attempts"]) - 1)),
                )
                status = JobStatus.PENDING
                available_at = timestamp + dt.timedelta(seconds=delay)
            else:
                status = JobStatus.DEAD_LETTER
                available_at = timestamp
            updated = connection.execute(
                """
                UPDATE jobs
                SET status = ?, available_at = ?, lease_until = NULL,
                    worker_id = NULL, last_error = ?, updated_at = ?
                WHERE id = ? AND status = ? AND worker_id = ? AND cancel_requested = 0
                """,
                (
                    status.value,
                    _iso(available_at),
                    error,
                    _iso(timestamp),
                    job_id,
                    JobStatus.LEASED.value,
                    worker_id,
                ),
            )
            if not updated.rowcount:
                raise JobLeaseError(f"lease not owned by {worker_id}: {job_id}")
            return self._record_transition(
                connection,
                EventType.JOB_FAILED,
                self._require_row(connection, job_id),
                actor=Actor(kind=ActorKind.WORKER, id=worker_id),
            )

    def cancel(self, job_id: str, *, now: dt.datetime | None = None) -> Job:
        """Cancel pending or leased work; terminal jobs remain unchanged."""
        timestamp = now or _utcnow()
        with self.store.transaction() as connection:
            row = self._require_row(connection, job_id)
            if row["status"] not in {
                JobStatus.COMPLETED.value,
                JobStatus.DEAD_LETTER.value,
                JobStatus.CANCELLED.value,
            }:
                connection.execute(
                    """
                    UPDATE jobs
                    SET status = ?, cancel_requested = 1, lease_until = NULL,
                        worker_id = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (JobStatus.CANCELLED.value, _iso(timestamp), job_id),
                )
                return self._record_transition(
                    connection,
                    EventType.JOB_CANCELLED,
                    self._require_row(connection, job_id),
                )
            return self._row_to_job(row)

    def requeue_expired(
        self,
        *,
        recover_expired_job_ids: Iterable[str] | None = None,
        now: dt.datetime | None = None,
    ) -> int:
        """Recover expired leases and return the number of transitioned jobs."""
        timestamp = now or _utcnow()
        recoverable_expired = self._normalize_job_ids(recover_expired_job_ids)
        with self.store.transaction() as connection:
            return self._requeue_expired(
                connection,
                timestamp,
                recover_expired_job_ids=recoverable_expired,
            )

    def expired_leases(self, *, now: dt.datetime | None = None) -> list[Job]:
        """Return leases whose durable deadline has passed, without changing them."""

        timestamp = now or _utcnow()
        rows = self.store.connection.execute(
            """
            SELECT * FROM jobs
            WHERE status = ? AND lease_until <= ?
            ORDER BY lease_until ASC, id ASC
            """,
            (JobStatus.LEASED.value, _iso(timestamp)),
        ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def is_archive_recovery_lease(
        self,
        job_id: str,
        *,
        worker_id: str | None = None,
    ) -> bool:
        """Return whether the current lease is the one bounded archive recovery lease."""

        row = self.store.connection.execute(
            "SELECT status, worker_id FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        if row is None or row["status"] != JobStatus.LEASED.value:
            return False
        if worker_id is not None and row["worker_id"] != worker_id:
            return False
        latest = self._latest_lifecycle_event_row(self.store.connection, job_id)
        if latest is None or latest["type"] != EventType.JOB_LEASED.value:
            return False
        metadata = json.loads(str(latest["metadata_json"]))
        recovery = metadata.get("lease_expiry_recovery")
        return (
            isinstance(recovery, Mapping)
            and recovery.get("ordinal") == 1
            and recovery.get("recovery_only") is True
        )

    def retry_dead_letter(
        self, job_id: str, *, reset_attempts: bool = False, now: dt.datetime | None = None
    ) -> Job:
        """Explicitly move a dead-letter job back to pending."""
        timestamp = now or _utcnow()
        with self.store.transaction() as connection:
            row = self._require_row(connection, job_id)
            if row["status"] != JobStatus.DEAD_LETTER.value:
                raise JobQueueError(f"job is not dead-lettered: {job_id}")
            connection.execute(
                """
                UPDATE jobs
                SET status = ?, attempts = CASE WHEN ? THEN 0 ELSE attempts END,
                    available_at = ?, last_error = NULL, updated_at = ?
                WHERE id = ?
                """,
                (
                    JobStatus.PENDING.value,
                    int(reset_attempts),
                    _iso(timestamp),
                    _iso(timestamp),
                    job_id,
                ),
            )
            return self._record_transition(
                connection,
                EventType.JOB_RETRIED,
                self._require_row(connection, job_id),
            )

    def get(self, job_id: str) -> Job | None:
        """Return a job snapshot by ID."""
        row = self.store.connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return None if row is None else self._row_to_job(row)

    def require(self, job_id: str) -> Job:
        """Return a job or raise :class:`JobNotFoundError`."""
        job = self.get(job_id)
        if job is None:
            raise JobNotFoundError(f"job not found: {job_id}")
        return job

    def list_jobs(
        self,
        *,
        status: JobStatus | str | None = None,
        kind: str | None = None,
        provider_id: str | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
        limit: int | None = None,
    ) -> list[Job]:
        """List jobs in scheduling order."""
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("kind", kind),
            ("provider_id", provider_id),
            ("session_id", session_id),
            ("branch_id", branch_id),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if status is not None:
            status_value = (
                status.value if isinstance(status, JobStatus) else JobStatus(status).value
            )
            clauses.append("status = ?")
            params.append(status_value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM jobs{where} ORDER BY priority DESC, available_at ASC, id ASC"
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be positive")
            sql += " LIMIT ?"
            params.append(limit)
        return [self._row_to_job(row) for row in self.store.connection.execute(sql, params)]

    def pending_count(self) -> int:
        """Return ready-or-delayed pending work for metrics."""
        row = self.store.connection.execute(
            "SELECT COUNT(*) FROM jobs WHERE status = ?", (JobStatus.PENDING.value,)
        ).fetchone()
        return int(row[0])

    def set_provider_limit(self, provider_id: str, limit: int) -> None:
        """Set a positive in-process lease limit for one provider."""
        if limit < 1:
            raise ValueError("provider concurrency limit must be positive")
        self.provider_limits[provider_id] = limit

    def _within_provider_limit(
        self, connection: sqlite3.Connection, candidate: sqlite3.Row, now: dt.datetime
    ) -> bool:
        provider = candidate["provider_id"]
        if provider is None or provider not in self.provider_limits:
            return True
        row = connection.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE provider_id = ? AND status = ? AND lease_until > ?
            """,
            (provider, JobStatus.LEASED.value, _iso(now)),
        ).fetchone()
        return int(row[0]) < self.provider_limits[provider]

    def _branch_available(
        self, connection: sqlite3.Connection, candidate: sqlite3.Row, now: dt.datetime
    ) -> bool:
        if not bool(candidate["serialize_branch"]):
            return True
        earlier = connection.execute(
            """
            SELECT 1 FROM jobs
            WHERE session_id = ? AND branch_id = ? AND serialize_branch = 1
              AND status IN (?, ?)
              AND (created_at < ? OR (created_at = ? AND id < ?))
            LIMIT 1
            """,
            (
                candidate["session_id"],
                candidate["branch_id"],
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
                candidate["created_at"],
                candidate["created_at"],
                candidate["id"],
            ),
        ).fetchone()
        if earlier is not None:
            return False
        row = connection.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE session_id = ? AND branch_id = ? AND status = ? AND lease_until > ?
            """,
            (
                candidate["session_id"],
                candidate["branch_id"],
                JobStatus.LEASED.value,
                _iso(now),
            ),
        ).fetchone()
        return int(row[0]) == 0

    def _requeue_expired(
        self,
        connection: sqlite3.Connection,
        now: dt.datetime,
        *,
        recover_expired_job_ids: frozenset[str] = frozenset(),
    ) -> int:
        expired = connection.execute(
            """
            SELECT * FROM jobs
            WHERE status = ? AND lease_until <= ?
            """,
            (JobStatus.LEASED.value, _iso(now)),
        ).fetchall()
        for row in expired:
            job_id = str(row["id"])
            expired_recovery_lease = self._leased_as_archive_recovery(connection, job_id)
            recovery_already_granted = self._archive_recovery_was_granted(connection, job_id)
            grant_archive_recovery = (
                not expired_recovery_lease
                and job_id in recover_expired_job_ids
                and not recovery_already_granted
            )
            if expired_recovery_lease:
                status = JobStatus.DEAD_LETTER
            elif grant_archive_recovery:
                status = JobStatus.PENDING
            else:
                status = (
                    JobStatus.DEAD_LETTER
                    if int(row["attempts"]) >= int(row["max_attempts"])
                    else JobStatus.PENDING
                )
            transition_error = (
                "lease expired during bounded archive recovery"
                if expired_recovery_lease
                else "lease expired"
            )
            connection.execute(
                """
                UPDATE jobs
                SET status = ?, available_at = ?, lease_until = NULL,
                    worker_id = NULL, last_error = COALESCE(last_error, ?), updated_at = ?
                WHERE id = ?
                """,
                (
                    status.value,
                    _iso(now),
                    transition_error,
                    _iso(now),
                    job_id,
                ),
            )
            recovery_metadata = None
            if grant_archive_recovery:
                recovery_metadata = {
                    "ordinal": 1,
                    "recovery_only": True,
                    "same_job": True,
                    "reason": "verified_archive_after_lease_expiry",
                    "previous_attempt": int(row["attempts"]),
                    "previous_worker_id": row["worker_id"],
                    "previous_lease_until": row["lease_until"],
                }
            elif expired_recovery_lease:
                recovery_metadata = {
                    "ordinal": 1,
                    "recovery_only": True,
                    "same_job": True,
                    "reason": "archive_recovery_lease_expired",
                    "exhausted": True,
                }
            self._record_transition(
                connection,
                EventType.JOB_REQUEUED,
                self._require_row(connection, job_id),
                metadata=(
                    {"lease_expiry_recovery": recovery_metadata}
                    if recovery_metadata is not None
                    else {"transition_reason": "lease_expired"}
                ),
            )
        return len(expired)

    @staticmethod
    def _normalize_branch_exclusions(
        values: Iterable[tuple[str, str]] | None,
    ) -> frozenset[tuple[str, str]]:
        normalized: set[tuple[str, str]] = set()
        for value in values or ():
            if (
                not isinstance(value, tuple)
                or len(value) != 2
                or any(not isinstance(item, str) or not item for item in value)
            ):
                raise ValueError(
                    "excluded branches must be non-blank (session_id, branch_id) pairs"
                )
            normalized.add(value)
        return frozenset(normalized)

    @staticmethod
    def _normalize_job_ids(values: Iterable[str] | None) -> frozenset[str]:
        normalized: set[str] = set()
        for value in values or ():
            if not isinstance(value, str) or not value:
                raise ValueError("recoverable expired job IDs must be non-blank strings")
            normalized.add(value)
        return frozenset(normalized)

    @staticmethod
    def _latest_lifecycle_event_row(
        connection: sqlite3.Connection,
        job_id: str,
    ) -> sqlite3.Row | None:
        placeholders = ",".join("?" for _ in _JOB_LIFECYCLE_EVENT_TYPES)
        return connection.execute(
            f"""
            SELECT type, metadata_json FROM events
            WHERE type IN ({placeholders})
              AND json_extract(payload_json, '$.id') = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (*sorted(item.value for item in _JOB_LIFECYCLE_EVENT_TYPES), job_id),
        ).fetchone()

    def _leased_as_archive_recovery(
        self,
        connection: sqlite3.Connection,
        job_id: str,
    ) -> bool:
        latest = self._latest_lifecycle_event_row(connection, job_id)
        if latest is None or latest["type"] != EventType.JOB_LEASED.value:
            return False
        metadata = json.loads(str(latest["metadata_json"]))
        recovery = metadata.get("lease_expiry_recovery")
        return (
            isinstance(recovery, Mapping)
            and recovery.get("ordinal") == 1
            and recovery.get("recovery_only") is True
        )

    def _archive_recovery_was_granted(
        self,
        connection: sqlite3.Connection,
        job_id: str,
    ) -> bool:
        placeholders = ",".join("?" for _ in _JOB_LIFECYCLE_EVENT_TYPES)
        row = connection.execute(
            f"""
            SELECT 1 FROM events
            WHERE type IN ({placeholders})
              AND json_extract(payload_json, '$.id') = ?
              AND json_extract(
                    metadata_json,
                    '$.lease_expiry_recovery.ordinal'
                  ) = 1
            LIMIT 1
            """,
            (*sorted(item.value for item in _JOB_LIFECYCLE_EVENT_TYPES), job_id),
        ).fetchone()
        return row is not None

    def _pending_archive_recovery_metadata(
        self,
        connection: sqlite3.Connection,
        job_id: str,
    ) -> Mapping[str, Any] | None:
        latest = self._latest_lifecycle_event_row(connection, job_id)
        if latest is None or latest["type"] != EventType.JOB_REQUEUED.value:
            return None
        metadata = json.loads(str(latest["metadata_json"]))
        recovery = metadata.get("lease_expiry_recovery")
        if (
            isinstance(recovery, Mapping)
            and recovery.get("ordinal") == 1
            and recovery.get("recovery_only") is True
            and recovery.get("exhausted") is not True
        ):
            return dict(recovery)
        return None

    def _record_transition(
        self,
        connection: sqlite3.Connection,
        event_type: EventType,
        row: sqlite3.Row,
        *,
        actor: Actor | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Job:
        """Append one lifecycle snapshot in the surrounding row transaction."""
        if event_type not in _JOB_LIFECYCLE_EVENT_TYPES:
            raise ValueError(f"not a job lifecycle event: {event_type}")
        job = self._row_to_job(row)
        placeholders = ",".join("?" for _ in _JOB_LIFECYCLE_EVENT_TYPES)
        previous = connection.execute(
            f"""
            SELECT id, created_at, correlation_id FROM events
            WHERE type IN ({placeholders})
              AND json_extract(payload_json, '$.id') = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (*sorted(item.value for item in _JOB_LIFECYCLE_EVENT_TYPES), job.id),
        ).fetchone()
        source = None if job.source_event_id is None else self.store.require(job.source_event_id)
        parent_event_id = (
            str(previous["id"]) if previous is not None else None if source is None else source.id
        )
        correlation_id = (
            (None if previous is None else previous["correlation_id"])
            or (None if source is None else source.correlation_id)
            or new_id("cor")
        )
        created_at = _utcnow()
        if previous is not None:
            previous_created_at = dt.datetime.fromisoformat(
                str(previous["created_at"]).replace("Z", "+00:00")
            )
            if created_at <= previous_created_at:
                created_at = previous_created_at + dt.timedelta(microseconds=1)
        session_id = (
            job.session_id if job.session_id is not None else getattr(source, "session_id", None)
        )
        branch_id = (
            job.branch_id if job.branch_id is not None else getattr(source, "branch_id", None)
        )
        event = Event(
            type=event_type,
            created_at=created_at,
            actor=actor or Actor(kind=ActorKind.SYSTEM, id="job_queue"),
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent_event_id,
            causation_id=parent_event_id,
            correlation_id=str(correlation_id),
            payload=job.model_dump(mode="json"),
            metadata={"schema_version": 1, **dict(metadata or {})},
        )
        self.store.append(event)
        return job

    def _assert_lease(self, row: sqlite3.Row, worker_id: str) -> None:
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id may not be blank")
        if row["status"] != JobStatus.LEASED.value:
            raise JobLeaseError(f"job is not leased: {row['id']}")
        if row["worker_id"] != worker_id:
            raise JobLeaseError(f"lease not owned by {worker_id}: {row['id']}")
        if bool(row["cancel_requested"]):
            raise JobLeaseError(f"job was cancelled: {row['id']}")

    def _require_row(self, connection: sqlite3.Connection, job_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise JobNotFoundError(f"job not found: {job_id}")
        return row

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        def parse(value: str | None) -> dt.datetime | None:
            return (
                None if value is None else dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            )

        return Job(
            id=row["id"],
            kind=row["kind"],
            status=row["status"],
            source_event_id=row["source_event_id"],
            available_at=parse(row["available_at"]),
            lease_until=parse(row["lease_until"]),
            worker_id=row["worker_id"],
            attempts=row["attempts"],
            payload=json.loads(row["payload_json"]),
            created_at=parse(row["created_at"]),
            updated_at=parse(row["updated_at"]),
            idempotency_key=row["idempotency_key"],
            priority=row["priority"],
            provider_id=row["provider_id"],
            session_id=row["session_id"],
            branch_id=row["branch_id"],
            serialize_branch=bool(row["serialize_branch"]),
            max_attempts=row["max_attempts"],
            last_error=row["last_error"],
            cancel_requested=bool(row["cancel_requested"]),
        )

    ack = complete
    nack = fail


__all__ = [
    "Job",
    "JobLeaseError",
    "JobNotFoundError",
    "JobProjection",
    "JobQueue",
    "JobQueueError",
    "JobStatus",
]
