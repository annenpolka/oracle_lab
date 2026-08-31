from __future__ import annotations

import datetime as dt

import pytest

from oracle_lab.jobs import JobLeaseError, JobQueue, JobStatus
from oracle_lab.store import EventStore

NOW = dt.datetime(2026, 8, 30, tzinfo=dt.UTC)


def test_idempotency_priority_and_provider_concurrency() -> None:
    store = EventStore()
    queue = JobQueue(store, provider_limits={"openrouter": 1})
    low = queue.enqueue(
        "oracle.generate",
        {"prompt": "low"},
        idempotency_key="same",
        priority=1,
        provider_id="openrouter",
        now=NOW,
    )
    assert (
        queue.enqueue("oracle.generate", {"prompt": "ignored"}, idempotency_key="same", now=NOW).id
        == low.id
    )
    high = queue.enqueue(
        "oracle.generate",
        {"prompt": "high"},
        priority=10,
        provider_id="openrouter",
        now=NOW,
    )

    leased = queue.lease("worker", limit=2, now=NOW)

    assert [job.id for job in leased] == [high.id]
    assert leased[0].attempts == 1
    queue.complete(high.id, worker_id="worker", now=NOW)
    assert queue.lease_one("worker", now=NOW).id == low.id


def test_serialized_branch_jobs_never_lease_in_parallel() -> None:
    store = EventStore()
    queue = JobQueue(store)
    first = queue.enqueue(
        "oracle.generate",
        {},
        session_id="ses",
        branch_id="br",
        serialize_branch=True,
        priority=10,
        now=NOW,
    )
    second = queue.enqueue(
        "oracle.generate",
        {},
        session_id="ses",
        branch_id="br",
        serialize_branch=True,
        priority=1,
        now=NOW,
    )
    other = queue.enqueue(
        "oracle.generate",
        {},
        session_id="ses",
        branch_id="other",
        serialize_branch=True,
        now=NOW,
    )

    leased = queue.lease("worker", limit=3, now=NOW)

    assert {job.id for job in leased} == {first.id, other.id}
    queue.complete(first.id, worker_id="worker", now=NOW)
    assert queue.lease_one("worker-2", now=NOW).id == second.id


def test_lease_excludes_exact_branch_pairs_without_interpolating_identifiers() -> None:
    queue = JobQueue(EventStore())
    excluded = queue.enqueue(
        "fixture",
        {"branch": "paused"},
        session_id="ses-paused",
        branch_id="br') OR 1=1 --",
        priority=100,
        now=NOW,
    )
    allowed = queue.enqueue(
        "fixture",
        {"branch": "running"},
        session_id="ses-running",
        branch_id="br-running",
        now=NOW,
    )

    leased = queue.lease(
        "worker",
        limit=2,
        excluded_branches={("ses-paused", "br') OR 1=1 --")},
        now=NOW,
    )

    assert [job.id for job in leased] == [allowed.id]
    assert queue.require(excluded.id).status is JobStatus.PENDING
    assert queue.require(excluded.id).attempts == 0


def test_serialized_branch_jobs_are_strict_fifo_before_priority() -> None:
    queue = JobQueue(EventStore())
    first = queue.enqueue(
        "oracle.generate",
        {"ordinal": 1},
        session_id="ses",
        branch_id="br",
        serialize_branch=True,
        available_at=NOW + dt.timedelta(seconds=10),
        priority=1,
        now=NOW,
    )
    later = queue.enqueue(
        "oracle.generate",
        {"ordinal": 2},
        session_id="ses",
        branch_id="br",
        serialize_branch=True,
        priority=100,
        now=NOW,
    )

    assert first.created_at == later.created_at
    assert first.id < later.id
    assert queue.lease("worker", now=NOW + dt.timedelta(seconds=1)) == []
    assert queue.lease_one("worker", now=NOW + dt.timedelta(seconds=10)).id == first.id
    assert queue.lease("worker-2", now=NOW + dt.timedelta(seconds=10)) == []

    queue.complete(first.id, worker_id="worker", now=NOW + dt.timedelta(seconds=11))
    assert queue.lease_one("worker-2", now=NOW + dt.timedelta(seconds=11)).id == later.id


def test_exponential_backoff_dead_letter_and_explicit_retry() -> None:
    queue = JobQueue(EventStore(), backoff_base_seconds=2, max_backoff_seconds=10)
    job = queue.enqueue("host.analyze", {}, max_attempts=2, now=NOW)

    queue.lease_one("worker", now=NOW)
    retried = queue.fail(job.id, "transient", worker_id="worker", now=NOW)
    assert retried.status is JobStatus.PENDING
    assert retried.available_at == NOW + dt.timedelta(seconds=2)
    assert queue.lease_one("worker", now=NOW + dt.timedelta(seconds=1)) is None

    queue.lease_one("worker", now=NOW + dt.timedelta(seconds=2))
    dead = queue.fail(
        job.id,
        "still broken",
        worker_id="worker",
        now=NOW + dt.timedelta(seconds=2),
    )
    assert dead.status is JobStatus.DEAD_LETTER
    reset = queue.retry_dead_letter(job.id, reset_attempts=True, now=NOW)
    assert reset.status is JobStatus.PENDING
    assert reset.attempts == 0


def test_expired_final_lease_goes_to_dlq_and_cancel_is_terminal() -> None:
    queue = JobQueue(EventStore())
    expired = queue.enqueue("tool.execute", {}, max_attempts=1, now=NOW)
    queue.lease_one("worker", lease_seconds=1, now=NOW)

    assert queue.requeue_expired(now=NOW + dt.timedelta(seconds=2)) == 1
    assert queue.require(expired.id).status is JobStatus.DEAD_LETTER

    pending = queue.enqueue("tool.execute", {}, now=NOW)
    assert queue.cancel(pending.id, now=NOW).status is JobStatus.CANCELLED
    assert pending.id not in {job.id for job in queue.lease("worker", now=NOW)}


def test_expired_runner_is_fenced_after_another_owner_re_leases_the_job() -> None:
    queue = JobQueue(EventStore())
    job = queue.enqueue("fixture", {}, max_attempts=3, now=NOW)
    first = queue.lease_one("runner:first", lease_seconds=1, now=NOW)
    assert first is not None

    second = queue.lease_one(
        "runner:second",
        lease_seconds=10,
        now=NOW + dt.timedelta(seconds=2),
    )

    assert second is not None and second.id == job.id
    assert second.worker_id == "runner:second"
    with pytest.raises(JobLeaseError, match="not owned"):
        queue.heartbeat(
            job.id,
            "runner:first",
            lease_seconds=10,
            now=NOW + dt.timedelta(seconds=3),
        )
    with pytest.raises(JobLeaseError, match="not owned"):
        queue.complete(
            job.id,
            worker_id="runner:first",
            now=NOW + dt.timedelta(seconds=3),
        )
    with pytest.raises(JobLeaseError, match="not owned"):
        queue.fail(
            job.id,
            "stale failure",
            worker_id="runner:first",
            now=NOW + dt.timedelta(seconds=3),
        )
    assert queue.require(job.id).worker_id == "runner:second"


def test_terminal_transitions_require_a_non_blank_explicit_lease_owner() -> None:
    queue = JobQueue(EventStore())
    job = queue.enqueue("fixture", {}, now=NOW)
    leased = queue.lease_one("runner", now=NOW)
    assert leased is not None and leased.id == job.id

    with pytest.raises(TypeError, match="worker_id"):
        queue.complete(job.id, now=NOW)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="worker_id"):
        queue.complete(job.id, worker_id=None, now=NOW)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="worker_id"):
        queue.fail(job.id, "error", worker_id=" ", now=NOW)

    current = queue.require(job.id)
    assert current.status is JobStatus.LEASED
    assert current.worker_id == "runner"


def test_verified_archive_gets_one_recovery_only_lease_beyond_attempt_limit() -> None:
    store = EventStore()
    queue = JobQueue(store)
    job = queue.enqueue("repository_edit", {}, max_attempts=1, now=NOW)
    first = queue.lease_one("worker", lease_seconds=1, now=NOW)
    assert first is not None and first.attempts == 1

    assert (
        queue.lease_one(
            "ordinary-worker",
            recover_expired_job_ids={job.id},
            now=NOW + dt.timedelta(seconds=2),
        )
        is None
    )
    assert queue.require(job.id).status is JobStatus.PENDING
    assert queue.require(job.id).attempts == 1

    recovery = queue.lease_one(
        "recovery-worker",
        lease_seconds=1,
        allow_archive_recovery=True,
        now=NOW + dt.timedelta(seconds=2),
    )

    assert recovery is not None
    assert recovery.id == job.id
    assert recovery.attempts == 2
    assert queue.is_archive_recovery_lease(job.id, worker_id="recovery-worker") is True
    lifecycle = [event for event in store.list_events() if event.payload.get("id") == job.id]
    recovery_events = [
        event
        for event in lifecycle
        if event.metadata.get("lease_expiry_recovery", {}).get("ordinal") == 1
    ]
    assert [event.type.value for event in recovery_events] == [
        "job.requeued",
        "job.leased",
    ]
    assert all(
        event.metadata["lease_expiry_recovery"]["recovery_only"] is True
        for event in recovery_events
    )

    assert (
        queue.lease_one(
            "third-worker",
            recover_expired_job_ids={job.id},
            now=NOW + dt.timedelta(seconds=4),
        )
        is None
    )
    dead = queue.require(job.id)
    assert dead.status is JobStatus.DEAD_LETTER
    assert dead.attempts == 2
    exhausted = [
        event
        for event in store.list_events()
        if event.payload.get("id") == job.id
        and event.metadata.get("lease_expiry_recovery", {}).get("exhausted") is True
    ]
    assert len(exhausted) == 1
