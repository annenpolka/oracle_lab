from __future__ import annotations

from pathlib import Path

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jobs import JobStatus
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"


def _service(tmp_path: Path) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )


def _candidate(
    service: OracleLabService,
    *,
    claim_id: str,
    source_event_ids: list[str] | None = None,
    artifact_origin: str | None = None,
) -> Event:
    session = service.new_session(claim_id)
    root = service.store.require(session["root_event_id"])
    detected = service.store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="extractor"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={
                "claims": [{"id": claim_id, "raw": f"{claim_id} = observed"}],
                "source_event_ids": [root.id],
            },
        )
    )
    payload: dict[str, object] = {
        "claim_id": claim_id,
        "source_event_ids": source_event_ids or [detected.id],
    }
    if artifact_origin is not None:
        payload["artifact_origin"] = artifact_origin
    return service.store.append(
        Event.new(
            EventType.ANALYSIS_CANON_CANDIDATE,
            actor=Actor(kind=ActorKind.HOST, id="canon-review"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=detected.id,
            causation_id=detected.id,
            correlation_id=root.correlation_id,
            payload=payload,
            metadata={
                "schema_version": 1,
                **({} if artifact_origin is None else {"artifact_origin": artifact_origin}),
            },
        )
    )


def test_public_canon_approval_is_atomic_idempotent_and_rebuildable(tmp_path: Path) -> None:
    service = _service(tmp_path)
    candidate = _candidate(service, claim_id="clm_phase")
    pending = service._dispatcher().dispatch(candidate)

    assert pending[0].status == "pending_approval"
    assert service.store.list_events(event_type=EventType.CLAIM_PROMOTED) == []
    gate = service._job_queue().list_jobs(kind="await_human_approval")[0]

    first = service.approve_canon_candidate(candidate.id)
    second = service.approve_canon_candidate(candidate.id)

    approval = service.store.require(first["approval_event"]["id"])
    promotion = service.store.require(first["promotion_event"]["id"])
    assert second["approval_event"]["id"] == approval.id
    assert second["promotion_event"]["id"] == promotion.id
    assert approval.type is EventType.HUMAN_KEEP
    assert approval.actor.kind is ActorKind.HUMAN
    assert approval.payload["claim_id"] == "clm_phase"
    assert approval.payload["target_event_id"] == candidate.id
    assert promotion.type is EventType.CLAIM_PROMOTED
    assert promotion.actor == Actor(kind=ActorKind.SYSTEM, id="dispatcher")
    assert promotion.payload["claim_id"] == "clm_phase"
    assert promotion.payload["to_status"] == "canonical"
    assert promotion.payload["source_event_id"] == candidate.id
    assert promotion.payload["approver_event_id"] == approval.id
    assert service._job_queue().require(gate.id).status is JobStatus.CANCELLED
    assert len(service.store.list_events(event_type=EventType.HUMAN_KEEP)) == 1
    assert len(service.store.list_events(event_type=EventType.CLAIM_PROMOTED)) == 1

    before = [
        tuple(row)
        for row in service.store.connection.execute(
            """
            SELECT event_id, claim_id, from_status, to_status
            FROM claim_transitions ORDER BY created_at, event_id
            """
        )
    ]
    service.store.rebuild_projections()
    after = [
        tuple(row)
        for row in service.store.connection.execute(
            """
            SELECT event_id, claim_id, from_status, to_status
            FROM claim_transitions ORDER BY created_at, event_id
            """
        )
    ]
    status = service.store.connection.execute(
        "SELECT status FROM branch_claim_states WHERE claim_id = ? AND branch_id = ?",
        ("clm_phase", candidate.branch_id),
    ).fetchone()[0]
    assert after == before
    assert status == "canonical"


def test_canon_approval_is_bound_to_the_exact_candidate_and_claim(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first_candidate = _candidate(service, claim_id="clm_first")
    first = service.approve_canon_candidate(first_candidate.id)
    second_candidate = _candidate(service, claim_id="clm_second")
    second = service.approve_canon_candidate(second_candidate.id)

    assert first["approval_event"]["id"] != second["approval_event"]["id"]
    assert first["promotion_event"]["id"] != second["promotion_event"]["id"]
    assert first["approval_event"]["payload"] == {
        "claim_id": "clm_first",
        "candidate_event_id": first_candidate.id,
        "event_id": first_candidate.id,
        "target_event_id": first_candidate.id,
    }
    assert second["approval_event"]["payload"] == {
        "claim_id": "clm_second",
        "candidate_event_id": second_candidate.id,
        "event_id": second_candidate.id,
        "target_event_id": second_candidate.id,
    }


def test_canon_approval_rejects_wrong_type_and_tainted_lineage(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("boundaries")
    root = service.store.require(session["root_event_id"])
    with pytest.raises(ServiceError, match="not a canon candidate"):
        service.approve_canon_candidate(root.id)

    valid = _candidate(service, claim_id="clm_valid")
    synthetic_source = service.store.append(
        Event.new(
            EventType.HUMAN_NOTE,
            actor=Actor(kind=ActorKind.HUMAN, id="fixture-author"),
            session_id=valid.session_id,
            branch_id=valid.branch_id,
            parent_event_id=valid.id,
            causation_id=valid.id,
            correlation_id=valid.correlation_id,
            payload={
                "event_id": valid.id,
                "target_event_id": valid.id,
                "material_origin": "synthetic_fixture",
            },
        )
    )
    synthetic_candidate = service.store.append(
        Event.new(
            EventType.ANALYSIS_CANON_CANDIDATE,
            actor=Actor(kind=ActorKind.HOST, id="canon-review"),
            session_id=valid.session_id,
            branch_id=valid.branch_id,
            parent_event_id=valid.id,
            causation_id=valid.id,
            correlation_id=valid.correlation_id,
            payload={
                "claim_id": "clm_valid",
                "source_event_ids": [valid.id, synthetic_source.id],
            },
        )
    )
    with pytest.raises(ServiceError, match="synthetic fixture lineage"):
        service.approve_canon_candidate(synthetic_candidate.id)

    worker_candidate = service.store.append(
        Event.new(
            EventType.ANALYSIS_CANON_CANDIDATE,
            actor=Actor(kind=ActorKind.HOST, id="canon-review"),
            session_id=valid.session_id,
            branch_id=valid.branch_id,
            parent_event_id=valid.id,
            causation_id=valid.id,
            correlation_id=valid.correlation_id,
            payload={
                "claim_id": "clm_valid",
                "source_event_ids": [valid.id],
                "artifact_origin": "worker_generated",
            },
            metadata={"schema_version": 1, "artifact_origin": "worker_generated"},
        )
    )
    with pytest.raises(ServiceError, match="worker-generated lineage"):
        service.approve_canon_candidate(worker_candidate.id)

    assert service.store.list_events(event_type=EventType.HUMAN_KEEP) == []
    assert service.store.list_events(event_type=EventType.CLAIM_PROMOTED) == []
