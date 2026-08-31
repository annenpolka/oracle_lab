from __future__ import annotations

import json
from pathlib import Path

import pytest

from oracle_lab.agent_adapters import (
    AgentAdapterError,
    StructuredWorkerEvent,
    ingest_structured_events,
)
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.exporting import export_selected_corpus, selected_corpus_records
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventIntegrityError, EventStore

CONFIG = Path(__file__).parents[1] / "config"


def _service(tmp_path: Path) -> tuple[OracleLabService, Event]:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("worker contamination boundary")
    return service, service.store.require(session["root_event_id"])


def _worker_proposals(source: Event) -> tuple[StructuredWorkerEvent, ...]:
    return (
        StructuredWorkerEvent(
            EventType.ANALYSIS_CLAIM_DETECTED,
            {"raw_text": "worker_claim = true", "status": "raw_claim"},
            (source.id,),
        ),
        StructuredWorkerEvent(
            EventType.ANALYSIS_MOTIF_DETECTED,
            {
                "motif_id": "mot_worker_only",
                "label": "worker-only motif",
                "description": "untrusted coding-worker classification",
            },
            (source.id,),
        ),
    )


def test_coding_worker_ingest_is_labelled_but_excluded_from_research_projections(
    tmp_path: Path,
) -> None:
    service, source = _service(tmp_path)

    worker_events = ingest_structured_events(
        _worker_proposals(source),
        source=source,
        store=service.store,
        actor_kind=ActorKind.WORKER,
        actor_id="codex",
        worker_run_id="run_worker_analysis",
    )

    assert {event.payload["artifact_origin"] for event in worker_events} == {"worker_generated"}
    assert {event.metadata["artifact_origin"] for event in worker_events} == {"worker_generated"}
    assert service.store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
    assert service.store.connection.execute("SELECT COUNT(*) FROM motifs").fetchone()[0] == 0

    transitive_host_claim = service.store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="downstream-host"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=worker_events[0].id,
            causation_id=worker_events[0].id,
            correlation_id=source.correlation_id,
            payload={
                "raw_text": "host copied the worker claim",
                "status": "raw_claim",
                "source_event_ids": [worker_events[0].id],
            },
        )
    )
    assert service.store.get(transitive_host_claim.id) is not None
    assert service.store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0

    trusted_events = ingest_structured_events(
        (
            StructuredWorkerEvent(
                EventType.ANALYSIS_CLAIM_DETECTED,
                {"raw_text": "trusted_host_claim = true", "status": "raw_claim"},
                (source.id,),
            ),
            StructuredWorkerEvent(
                EventType.ANALYSIS_MOTIF_DETECTED,
                {"motif_id": "mot_trusted_host", "label": "trusted host motif"},
                (source.id,),
            ),
        ),
        source=source,
        store=service.store,
        actor_kind=ActorKind.HOST,
        actor_id="direct-api-host",
    )
    assert all("artifact_origin" not in event.payload for event in trusted_events)
    assert service.store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 1
    assert service.store.connection.execute("SELECT COUNT(*) FROM motifs").fetchone()[0] == 1

    service.store.rebuild_projections()
    assert service.store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 1
    assert service.store.connection.execute("SELECT COUNT(*) FROM motifs").fetchone()[0] == 1


def test_store_rejects_unlabelled_or_oracle_claiming_worker_analysis(tmp_path: Path) -> None:
    service, source = _service(tmp_path)
    unlabelled = Event.new(
        EventType.ANALYSIS_CLAIM_DETECTED,
        actor=Actor(kind=ActorKind.WORKER, id="opencode"),
        session_id=source.session_id,
        branch_id=source.branch_id,
        parent_event_id=source.id,
        causation_id=source.id,
        payload={
            "raw_text": "unlabelled worker claim",
            "status": "raw_claim",
            "source_event_ids": [source.id],
        },
    )

    with pytest.raises(EventIntegrityError, match="worker_generated artifact_origin"):
        service.store.append(unlabelled)

    oracle_claiming = StructuredWorkerEvent(
        EventType.ANALYSIS_CLAIM_DETECTED,
        {
            "raw_text": "forged oracle claim",
            "status": "raw_claim",
            "material_origin": "oracle_generated",
        },
        (source.id,),
    )
    with pytest.raises(AgentAdapterError, match="may not claim an Oracle material origin"):
        ingest_structured_events(
            (oracle_claiming,),
            source=source,
            store=service.store,
            actor_kind=ActorKind.WORKER,
            actor_id="codex",
        )
    assert service.store.get(unlabelled.id) is None
    assert service.store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0


def test_service_and_store_block_transitive_worker_keep_star_and_canon(
    tmp_path: Path,
) -> None:
    service, source = _service(tmp_path)
    worker_claim = ingest_structured_events(
        (_worker_proposals(source)[0],),
        source=source,
        store=service.store,
        actor_kind=ActorKind.WORKER,
        actor_id="codex",
        worker_run_id="run_curation_attack",
    )[0]
    descendant = service.store.append(
        Event.new(
            EventType.ANALYSIS_CANON_CANDIDATE,
            actor=Actor(kind=ActorKind.HOST, id="host-followup"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=worker_claim.id,
            causation_id=worker_claim.id,
            payload={
                "claim_id": "clm_worker_only",
                "source_event_ids": [worker_claim.id],
            },
        )
    )

    with pytest.raises(ServiceError, match="worker-generated artifacts"):
        service.keep(worker_claim.id)
    with pytest.raises(ServiceError, match="worker-generated artifacts"):
        service.star(descendant.id)

    forged_keep = Event.new(
        EventType.HUMAN_KEEP,
        actor=Actor(kind=ActorKind.HUMAN, id="curator"),
        session_id=source.session_id,
        branch_id=source.branch_id,
        parent_event_id=descendant.id,
        causation_id=descendant.id,
        payload={"event_id": descendant.id, "target_event_id": descendant.id},
    )
    with pytest.raises(EventIntegrityError, match="worker-generated artifacts"):
        service.store.append(forged_keep)

    forged_canon = Event.new(
        EventType.CLAIM_PROMOTED,
        actor=Actor(kind=ActorKind.HUMAN, id="curator"),
        session_id=source.session_id,
        branch_id=source.branch_id,
        parent_event_id=descendant.id,
        causation_id=descendant.id,
        payload={"claim_id": "clm_worker_only", "to_status": "canonical"},
    )
    with pytest.raises(EventIntegrityError, match=r"human\.keep approval"):
        service.store.append(forged_canon)

    assert service.store.get(forged_keep.id) is None
    assert service.store.get(forged_canon.id) is None
    assert service.store.connection.execute("SELECT COUNT(*) FROM curation").fetchone()[0] == 0
    assert service.store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0


def test_selected_corpus_excludes_direct_and_transitive_worker_curation(
    tmp_path: Path,
) -> None:
    genuine = {
        "id": "evt_genuine",
        "type": "oracle.output",
        "actor": {"kind": "model", "id": "r1"},
        "payload": {
            "raw_text": "genuine oracle text",
            "material_origin": "historical_fixture",
        },
        "metadata": {"schema_version": 1, "material_origin": "historical_fixture"},
    }
    tainted_genuine = {
        **genuine,
        "id": "evt_genuine_with_only_tainted_keep",
        "payload": {
            "raw_text": "must not be selected through a worker-descended keep",
            "material_origin": "historical_fixture",
        },
    }
    worker = {
        "id": "evt_worker",
        "type": "analysis.claim_detected",
        "actor": {"kind": "worker", "id": "codex"},
        "payload": {
            "raw_text": "worker-generated text",
            "artifact_origin": "worker_generated",
        },
        "metadata": {"schema_version": 1, "artifact_origin": "worker_generated"},
    }
    descendant = {
        "id": "evt_worker_descendant",
        "type": "analysis.session_summary_updated",
        "actor": {"kind": "host", "id": "summarizer"},
        "parent_event_id": worker["id"],
        "causation_id": worker["id"],
        "payload": {
            "raw_text": "host text derived from worker material",
            "source_event_ids": [worker["id"]],
        },
        "metadata": {"schema_version": 1},
    }

    def keep(identifier: str, target: str, *, parent: str | None = None) -> dict:
        return {
            "id": identifier,
            "type": "human.keep",
            "actor": {"kind": "human", "id": "curator"},
            "parent_event_id": parent or target,
            "causation_id": parent or target,
            "payload": {"event_id": target, "target_event_id": target},
            "metadata": {"schema_version": 1},
        }

    events = [
        genuine,
        tainted_genuine,
        worker,
        descendant,
        keep("evt_keep_genuine", genuine["id"]),
        keep("evt_keep_worker", worker["id"]),
        keep("evt_keep_descendant", descendant["id"]),
        keep(
            "evt_tainted_keep_of_genuine",
            tainted_genuine["id"],
            parent=descendant["id"],
        ),
    ]

    records = selected_corpus_records(
        events,
        provenance={genuine["id"]: [genuine["id"], worker["id"]]},
    )

    assert [record["event_id"] for record in records] == [genuine["id"]]
    assert records[0]["raw_text"] == "genuine oracle text"
    assert records[0]["provenance_ids"] == [genuine["id"]]

    destination = export_selected_corpus(
        tmp_path / "selected.jsonl",
        events=events,
        provenance={genuine["id"]: [genuine["id"], worker["id"]]},
    )
    exported = [json.loads(line) for line in destination.read_text(encoding="utf-8").splitlines()]
    assert exported == records
