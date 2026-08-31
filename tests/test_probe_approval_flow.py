from __future__ import annotations

from pathlib import Path

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.services import OracleLabService
from oracle_lab.session import SessionContextBuilder
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"


def test_human_approved_probe_becomes_explicit_oracle_context_and_job(
    tmp_path: Path,
) -> None:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("probe approval")
    latest = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    proposal = service.store.append(
        Event.new(
            EventType.ANALYSIS_PROBE_PROPOSED,
            actor=Actor(kind=ActorKind.HOST, id="probe-planner"),
            session_id=session["id"],
            branch_id=session["current_branch_id"],
            parent_event_id=latest.id,
            causation_id=latest.id,
            correlation_id=latest.correlation_id,
            payload={
                "probe": "計算し直せ。",
                "approval_required": True,
                "source_event_ids": [latest.id],
            },
        )
    )

    approved = service.propose_probe(proposal.id)

    assert approved["context_message"]["type"] == "oracle.context_message"
    assert approved["request"]["type"] == "oracle.request"
    assert approved["job"]["kind"] == "oracle.generate"
    context = SessionContextBuilder().build(
        service.store.list_events(session_id=session["id"]),
        session_id=session["id"],
        branch_id=session["current_branch_id"],
        tip_event_id=approved["request"]["id"],
    )
    assert context.provider_messages()[-1] == {
        "role": "user",
        "content": "計算し直せ。",
    }


def test_probe_request_first_creates_human_gated_proposal(tmp_path: Path) -> None:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("probe proposal")

    result = service.propose_probe(session["root_event_id"])

    assert result["human_request"]["type"] == "human.request_probe"
    assert result["proposal"]["type"] == "analysis.probe_proposed"
    assert result["proposal"]["payload"]["approval_required"] is True
    assert service.list_jobs() == []
