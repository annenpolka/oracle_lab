from __future__ import annotations

import datetime as dt
from itertools import pairwise

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jobs import Job, JobQueue, JobStatus
from oracle_lab.store import EventIntegrityError, EventStore

NOW = dt.datetime(2026, 8, 30, tzinfo=dt.UTC)
JOB_EVENT_TYPES = {
    EventType.JOB_ENQUEUED,
    EventType.JOB_LEASED,
    EventType.JOB_HEARTBEAT,
    EventType.JOB_COMPLETED,
    EventType.JOB_FAILED,
    EventType.JOB_CANCELLED,
    EventType.JOB_REQUEUED,
    EventType.JOB_RETRIED,
}


def _current_snapshots(queue: JobQueue) -> dict[str, dict]:
    return {job.id: job.model_dump(mode="json") for job in queue.list_jobs()}


def _job_events(store: EventStore, job_id: str) -> list[Event]:
    return [
        event
        for event in store.list_events()
        if event.type in JOB_EVENT_TYPES and event.payload["id"] == job_id
    ]


def test_mixed_job_lifecycle_rebuilds_exact_queue_snapshots() -> None:
    store = EventStore()
    source = store.append(
        Event(
            type=EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="operator"),
            created_at=NOW - dt.timedelta(minutes=1),
            session_id="ses_jobs",
            branch_id="br_main",
            correlation_id="cor_jobs",
            payload={"content": "run queue fixture"},
        )
    )
    queue = JobQueue(store)
    pending = queue.enqueue("pending", {"state": "ready"}, now=NOW)
    completed = queue.enqueue(
        "completed",
        {"state": "work"},
        source_event_id=source.id,
        session_id=source.session_id,
        branch_id=source.branch_id,
        now=NOW,
    )
    queue.lease_one("worker-complete", kinds=["completed"], now=NOW + dt.timedelta(seconds=1))
    queue.heartbeat(
        completed.id,
        "worker-complete",
        lease_seconds=30,
        now=NOW + dt.timedelta(seconds=2),
    )
    queue.complete(
        completed.id,
        worker_id="worker-complete",
        now=NOW + dt.timedelta(seconds=3),
    )

    dead = queue.enqueue("dead", {}, max_attempts=1, now=NOW)
    queue.lease_one("worker-dead", kinds=["dead"], now=NOW + dt.timedelta(seconds=1))
    queue.fail(
        dead.id,
        "permanent",
        worker_id="worker-dead",
        retryable=False,
        now=NOW + dt.timedelta(seconds=2),
    )

    cancelled = queue.enqueue("cancelled", {}, now=NOW)
    queue.cancel(cancelled.id, now=NOW + dt.timedelta(seconds=1))

    recovered = queue.enqueue("recovered", {}, max_attempts=2, now=NOW)
    queue.lease_one("worker-retry", kinds=["recovered"], now=NOW + dt.timedelta(seconds=1))
    queue.fail(
        recovered.id,
        "manual retry",
        worker_id="worker-retry",
        retryable=False,
        now=NOW + dt.timedelta(seconds=2),
    )
    queue.retry_dead_letter(
        recovered.id,
        reset_attempts=True,
        now=NOW + dt.timedelta(seconds=3),
    )
    queue.lease_one(
        "worker-expire",
        kinds=["recovered"],
        lease_seconds=1,
        now=NOW + dt.timedelta(seconds=4),
    )
    assert queue.requeue_expired(now=NOW + dt.timedelta(seconds=6)) == 1

    expected = _current_snapshots(queue)
    assert {snapshot["status"] for snapshot in expected.values()} == {
        JobStatus.PENDING.value,
        JobStatus.COMPLETED.value,
        JobStatus.DEAD_LETTER.value,
        JobStatus.CANCELLED.value,
    }
    observed_types = {event.type for event in store.list_events() if event.type in JOB_EVENT_TYPES}
    assert observed_types == JOB_EVENT_TYPES
    assert all(
        set(event.payload) == set(Job.model_fields)
        for event in store.list_events()
        if event.type in JOB_EVENT_TYPES
    )

    store.connection.execute(
        "UPDATE jobs SET last_error = 'projection drift' WHERE id = ?", (pending.id,)
    )
    store.rebuild_projections()

    rebuilt = JobQueue(store)
    assert _current_snapshots(rebuilt) == expected

    sourced = _job_events(store, completed.id)
    assert sourced[0].parent_event_id == source.id
    assert sourced[0].causation_id == source.id
    assert all(event.session_id == source.session_id for event in sourced)
    assert all(event.branch_id == source.branch_id for event in sourced)
    assert all(event.correlation_id == source.correlation_id for event in sourced)
    assert all(
        current.parent_event_id == previous.id and current.causation_id == previous.id
        for previous, current in pairwise(sourced)
    )

    source_less = _job_events(store, cancelled.id)
    assert source_less[0].parent_event_id is None
    assert source_less[0].causation_id is None
    assert source_less[0].correlation_id
    assert source_less[1].parent_event_id == source_less[0].id
    assert source_less[1].causation_id == source_less[0].id
    assert source_less[1].correlation_id == source_less[0].correlation_id


def test_job_row_and_lifecycle_event_append_are_atomic() -> None:
    store = EventStore()
    store.connection.execute(
        """
        CREATE TRIGGER reject_job_lifecycle_fixture
        BEFORE INSERT ON events
        WHEN NEW.type = 'job.enqueued'
        BEGIN
            SELECT RAISE(ABORT, 'fixture rejects lifecycle event');
        END
        """
    )
    queue = JobQueue(store)

    with pytest.raises(EventIntegrityError, match="fixture rejects lifecycle event"):
        queue.enqueue("atomic", {}, job_id="job_atomic", now=NOW)

    assert queue.get("job_atomic") is None
    assert store.list_events(event_type=EventType.JOB_ENQUEUED) == []
