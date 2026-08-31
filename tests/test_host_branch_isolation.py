from __future__ import annotations

from pathlib import Path

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"


def _oracle_output(
    service: OracleLabService,
    *,
    session_id: str,
    branch_id: str,
    parent_event_id: str,
    text: str,
) -> Event:
    return service.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="replay"),
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent_event_id,
            causation_id=parent_event_id,
            payload={"content": text},
        )
    )


def test_host_analysis_does_not_read_parent_branch_future(tmp_path: Path) -> None:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("branch isolation")
    root = service.store.require(session["root_event_id"])

    main_output = _oracle_output(
        service,
        session_id=session["id"],
        branch_id=session["current_branch_id"],
        parent_event_id=service.store.list_events(branch_id=session["current_branch_id"])[-1].id,
        text="BRANCH_VALUE=1",
    )
    service._run_host_analysis(main_output)

    child = service._branch_service().fork(root.id, title="child")
    child_tip = service.store.list_events(branch_id=child.id)[-1]
    child_output = _oracle_output(
        service,
        session_id=session["id"],
        branch_id=child.id,
        parent_event_id=child_tip.id,
        text="BRANCH_VALUE=2",
    )
    child_analysis = service._run_host_analysis(child_output)

    contradictions = [
        event for event in child_analysis if event.type == EventType.ANALYSIS_CONTRADICTION_DETECTED
    ]
    assert contradictions == []
    assert main_output.id not in {
        source_id
        for event in child_analysis
        for source_id in event.payload.get("source_event_ids", ())
    }
