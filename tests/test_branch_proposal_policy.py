from __future__ import annotations

import dataclasses
from pathlib import Path

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"


def _service(tmp_path: Path, *, gated: bool) -> OracleLabService:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    config = service.runtime_config
    service._config = dataclasses.replace(
        config,
        policies=dataclasses.replace(
            config.policies,
            human_gate={**config.policies.human_gate, "branch_creation": gated},
        ),
    )
    return service


def _proposal(service: OracleLabService) -> tuple[Event, Event]:
    session = service.new_session("branch proposal")
    root = service.store.require(session["root_event_id"])
    proposal = service.store.append(
        Event.new(
            EventType.ANALYSIS_BRANCH_PROPOSED,
            actor=Actor(kind=ActorKind.HOST, id="branch-planner"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={
                "fork_event_id": root.id,
                "title": "before contradiction",
                "reason": "preserve alternative observation",
                "source_event_ids": [root.id],
            },
        )
    )
    return root, proposal


def test_ungated_branch_proposal_runs_as_idempotent_policy_job(tmp_path: Path) -> None:
    service = _service(tmp_path, gated=False)
    root, proposal = _proposal(service)

    decisions = service._dispatcher().dispatch(proposal)
    run = service.run_automation(max_jobs=1)

    assert any(item.rule_id == "branch-proposal-creation" for item in decisions)
    assert run["processed"][0]["status"] == "completed"
    fork = service.store.list_events(event_type=EventType.SESSION_FORKED)[0]
    assert fork.parent_event_id == root.id
    assert fork.causation_id == proposal.id
    assert fork.payload["proposal_event_id"] == proposal.id
    assert fork.actor == Actor(kind=ActorKind.SYSTEM, id="branch-policy")

    service._dispatcher().dispatch(proposal)
    assert len(service._job_queue().list_jobs(kind="branch.create")) == 1


def test_gated_branch_proposal_requires_explicit_human_approval(tmp_path: Path) -> None:
    service = _service(tmp_path, gated=True)
    _, proposal = _proposal(service)
    service._dispatcher().dispatch(proposal)

    stopped = service.run_automation(until_human=True)
    approved = service.approve_branch(proposal.id)
    run = service.run_automation(max_jobs=1)

    assert stopped["stopped"] == "human_judgment"
    assert stopped["event_id"] == proposal.id
    assert approved["approval_event"]["actor"]["kind"] == ActorKind.HUMAN.value
    assert run["processed"][0]["status"] == "completed"
    fork = service.store.list_events(event_type=EventType.SESSION_FORKED)[0]
    assert fork.actor.kind is ActorKind.HUMAN
    assert fork.causation_id == proposal.id
