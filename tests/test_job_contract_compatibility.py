from __future__ import annotations

import datetime as dt
import hashlib
import json
import pickle
import subprocess
import sys

import oracle_lab._job_contract as internal_contract
import oracle_lab._job_projection as internal_projection
import oracle_lab.jobs as jobs
from oracle_lab.projections import default_projections


def test_default_projection_registry_does_not_import_mutable_job_queue() -> None:
    script = """
import sys

from oracle_lab.projections import default_projections

assert "oracle_lab.jobs" not in sys.modules
assert any(projection.name == "jobs" for projection in default_projections())
assert "oracle_lab.jobs" not in sys.modules
"""

    subprocess.run([sys.executable, "-c", script], check=True)


def test_job_symbols_keep_historical_identity_and_pickle_lookup() -> None:
    assert jobs.Job is internal_contract.Job
    assert jobs.JobStatus is internal_contract.JobStatus
    assert jobs.JobProjection is internal_projection.JobProjection
    assert (
        type(next(item for item in default_projections() if item.name == "jobs"))
        is jobs.JobProjection
    )

    for symbol in (jobs.Job, jobs.JobStatus, jobs.JobProjection):
        assert symbol.__module__ == "oracle_lab.jobs"
        assert pickle.loads(pickle.dumps(symbol)) is symbol

    assert pickle.loads(pickle.dumps(jobs.JobStatus.PENDING)) is jobs.JobStatus.PENDING
    assert type(pickle.loads(pickle.dumps(jobs.JobProjection()))) is jobs.JobProjection


def test_job_payload_and_pydantic_schema_match_the_historical_contract() -> None:
    timestamp = dt.datetime(2026, 8, 31, tzinfo=dt.UTC)
    job = jobs.Job(
        id="job_contract",
        kind="oracle.generate",
        status=jobs.JobStatus.PENDING,
        source_event_id="evt_source",
        available_at=timestamp,
        lease_until=None,
        worker_id=None,
        attempts=0,
        payload={"nested": {"value": [1, "two"]}},
        created_at=timestamp,
        updated_at=timestamp,
        idempotency_key="once",
        priority=3,
        provider_id="openrouter",
        session_id="ses",
        branch_id="br",
        serialize_branch=True,
        max_attempts=5,
        last_error=None,
        cancel_requested=False,
    )

    assert tuple(jobs.Job.model_fields) == (
        "id",
        "kind",
        "status",
        "source_event_id",
        "available_at",
        "lease_until",
        "worker_id",
        "attempts",
        "payload",
        "created_at",
        "updated_at",
        "idempotency_key",
        "priority",
        "provider_id",
        "session_id",
        "branch_id",
        "serialize_branch",
        "max_attempts",
        "last_error",
        "cancel_requested",
    )
    assert job.model_dump(mode="json") == {
        "id": "job_contract",
        "kind": "oracle.generate",
        "status": "pending",
        "source_event_id": "evt_source",
        "available_at": "2026-08-31T00:00:00Z",
        "lease_until": None,
        "worker_id": None,
        "attempts": 0,
        "payload": {"nested": {"value": [1, "two"]}},
        "created_at": "2026-08-31T00:00:00Z",
        "updated_at": "2026-08-31T00:00:00Z",
        "idempotency_key": "once",
        "priority": 3,
        "provider_id": "openrouter",
        "session_id": "ses",
        "branch_id": "br",
        "serialize_branch": True,
        "max_attempts": 5,
        "last_error": None,
        "cancel_requested": False,
    }
    schema = json.dumps(
        jobs.Job.model_json_schema(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert hashlib.sha256(schema).hexdigest() == (
        "6094eb2b1d5af42ecd55f25e2d4c5d4db57d7984cde0ab007b0578adf560b2ee"
    )
