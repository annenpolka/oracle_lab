from __future__ import annotations

import json
from pathlib import Path

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.projections import VirtualStateService
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.virtual import SourceEvidence, VirtualNodeKind, VirtualWorldRuntime

CONFIG = Path(__file__).parents[1] / "config"


def _append_virtual_request(
    service: OracleLabService,
    *,
    source: Event,
    command: str,
) -> Event:
    latest = service.store.list_events(
        session_id=source.session_id,
        branch_id=source.branch_id,
        ascending=False,
        limit=1,
    )[0]
    request = Event.new(
        EventType.TOOL_REQUEST,
        actor=Actor(kind=ActorKind.HUMAN, id="tester"),
        session_id=source.session_id,
        branch_id=source.branch_id,
        parent_event_id=latest.id,
        causation_id=source.id,
        correlation_id=source.correlation_id,
        payload={
            "tool": "virtual",
            "execution": "virtual",
            "input": {"command": command},
            "source_event_id": source.id,
            "resume_oracle": False,
            "timeout_ms": 5_000,
        },
    )
    return service.store.append(request)


def test_virtual_tool_uses_branch_projection_after_service_restart(tmp_path: Path) -> None:
    database = tmp_path / "oracle.db"
    home = tmp_path / "home"
    first = OracleLabService(
        EventStore(database),
        home=home,
        config_dir=CONFIG,
        owns_store=True,
    )
    session = first.new_session("virtual persistence")
    source = first.store.require(session["root_event_id"])
    sink = VirtualStateService(first.store).mutation_sink(
        session_id=str(source.session_id),
        branch_id=str(source.branch_id),
    )
    runtime = VirtualWorldRuntime(mutation_sink=sink)
    runtime.fs.create(
        "/dev/void",
        evidence=SourceEvidence((source.id,), "explicit"),
        kind=VirtualNodeKind.CHARACTER_DEVICE,
        content="observer interface",
    )
    first_request = _append_virtual_request(first, source=source, command="cat /dev/void")
    first.request_tool(first_request.id)
    first_run = first.run_automation(max_jobs=1)
    first.close()

    second = OracleLabService(
        EventStore(database),
        home=home,
        config_dir=CONFIG,
        owns_store=True,
    )
    second_request = _append_virtual_request(second, source=source, command="stat /dev/void")
    second.request_tool(second_request.id)
    second_run = second.run_automation(max_jobs=1)

    assert first_run["processed"][0]["result"]["payload"]["output"] == "observer interface"
    second_output = second_run["processed"][0]["result"]["payload"]["output"]
    assert "/dev/void" in second_output
    assert "observer interface" in second_output
    second.close()


def test_oracle_path_mention_materializes_only_for_later_virtual_operation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "oracle.db"
    home = tmp_path / "home"
    first = OracleLabService(
        EventStore(database),
        home=home,
        config_dir=CONFIG,
        owns_store=True,
    )
    session = first.new_session("lazy virtual artifact")
    root = first.store.require(session["root_event_id"])
    output = first.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1-test"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "The path /dev/void exists."},
        )
    )

    # Structural fixture analysis is explicit; normal post-processing excludes
    # synthetic oracle-like text from genuine research projections.
    first._run_host_analysis(output)

    mentions = [
        event
        for event in first.store.list_events(event_type=EventType.ANALYSIS_ENTITY_DETECTED)
        if event.payload.get("canonical_name") == "/dev/void"
    ]
    assert len(mentions) == 1
    assert first.store.list_events(event_type=EventType.TOOL_REQUEST) == []
    assert first.store.list_events(event_type=EventType.VIRTUAL_FILE_CREATED) == []

    first_request = _append_virtual_request(first, source=output, command="stat /dev/void")
    first.request_tool(first_request.id)

    first_run = first.run_automation(max_jobs=1)

    assert first_run["processed"][0]["status"] == "completed"
    first_results = first.store.list_events(
        event_type=[EventType.TOOL_OUTPUT, EventType.TOOL_ERROR]
    )
    first_result = first_results[0]
    assert first_result.payload.get("error") is None, first_result.payload.get("error")
    assert [event.type for event in first_results] == [EventType.TOOL_OUTPUT], (
        first_result.to_dict(),
        first_run,
    )
    assert first_result.payload["truth_domain"] == "virtual"
    assert "/dev/void" in first_result.payload["output"]
    target_creation = next(
        event
        for event in first.store.list_events(event_type=EventType.VIRTUAL_FILE_CREATED)
        if event.payload["node"]["path"] == "/dev/void"
    )
    assert target_creation.actor == Actor(kind=ActorKind.HOST, id="virtual-materializer")
    assert target_creation.payload["evidence_basis"] == "synthesized"
    assert target_creation.payload["node"]["content_versions"] == ()
    assert set(target_creation.payload["node"]["unresolved_fields"]) == {
        "major",
        "minor",
        "read_semantics",
    }
    assert set(target_creation.payload["source_event_ids"]) == {
        output.id,
        mentions[0].id,
        first_request.id,
    }
    first.close()

    second = OracleLabService(
        EventStore(database),
        home=home,
        config_dir=CONFIG,
        owns_store=True,
    )
    persisted_output = second.store.require(output.id)
    second_request = _append_virtual_request(
        second,
        source=persisted_output,
        command="stat /dev/void",
    )
    second.request_tool(second_request.id)
    second_run = second.run_automation(max_jobs=1)

    assert second_run["processed"][0]["status"] == "completed"
    assert second_run["processed"][0]["result"]["payload"]["truth_domain"] == "virtual"
    assert "/dev/void" in second_run["processed"][0]["result"]["payload"]["output"]
    target_creations = [
        event
        for event in second.store.list_events(event_type=EventType.VIRTUAL_FILE_CREATED)
        if event.payload["node"]["path"] == "/dev/void"
    ]
    assert target_creations == [target_creation]
    second.close()


def test_configured_virtual_kill_emits_signal_event_and_updates_process(
    tmp_path: Path,
) -> None:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("virtual kill")
    root = service.store.require(session["root_event_id"])
    sink = VirtualStateService(service.store).mutation_sink(
        session_id=str(root.session_id),
        branch_id=str(root.branch_id),
        actor=Actor(kind=ActorKind.HOST, id="virtual-materializer"),
    )
    runtime = VirtualWorldRuntime(mutation_sink=sink)
    runtime.processes.create(
        "reality_monitor",
        ("--target", "/dev/void"),
        evidence=SourceEvidence((root.id,), "explicit"),
        event_callbacks={"TERM": "observer.stopped"},
        pid=4242,
    )
    request = _append_virtual_request(service, source=root, command="kill -TERM 4242")
    service.request_tool(request.id)

    run = service.run_automation(max_jobs=1)

    assert run["processed"][0]["status"] == "completed"
    result = service.store.list_events(event_type=EventType.TOOL_OUTPUT)[0]
    assert result.payload["truth_domain"] == "virtual"
    assert result.payload["output"] == "pid=4242 signal=TERM state=terminated"
    signal = service.store.list_events(event_type=EventType.VIRTUAL_PROCESS_SIGNAL_RECEIVED)[0]
    assert signal.actor == Actor(kind=ActorKind.TOOL, id="virtual-runtime")
    assert signal.causation_id == request.id
    assert signal.payload["source_event_ids"] == (request.id,)
    assert signal.payload["signal"] == "TERM"
    assert signal.payload["state"] == "terminated"
    assert signal.payload["callback"] == "observer.stopped"
    persisted = VirtualStateService(service.store).hydrate(str(root.branch_id))
    assert persisted.processes.require(4242).state == "terminated"
    assert persisted.processes.require(4242).signals == ["TERM"]


def test_arbitrary_mentioned_path_materializes_as_unknown_not_invented_lore(
    tmp_path: Path,
) -> None:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("generic lazy artifact")
    root = service.store.require(session["root_event_id"])
    output = service.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="synthetic-fixture"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "Inspect /opt/hope_filter.cfg."},
        )
    )
    service._run_host_analysis(output)
    request = _append_virtual_request(
        service,
        source=output,
        command="stat /opt/hope_filter.cfg",
    )
    service.request_tool(request.id)

    run = service.run_automation(max_jobs=1)

    assert run["processed"][0]["status"] == "completed"
    created = next(
        event
        for event in service.store.list_events(event_type=EventType.VIRTUAL_FILE_CREATED)
        if event.payload["node"]["path"] == "/opt/hope_filter.cfg"
    )
    assert created.payload["node"]["kind"] == "unknown"
    assert created.payload["node"]["properties"] == {}
    assert created.payload["node"]["content_versions"] == ()
    assert set(created.payload["node"]["unresolved_fields"]) == {
        "content",
        "kind",
        "read_semantics",
    }


def test_virtual_clock_requires_explicit_create_and_survives_service_restart(
    tmp_path: Path,
) -> None:
    database = tmp_path / "oracle.db"
    home = tmp_path / "home"
    first = OracleLabService(
        EventStore(database),
        home=home,
        config_dir=CONFIG,
        owns_store=True,
    )
    session = first.new_session("sparse virtual clock")
    root = first.store.require(session["root_event_id"])
    mention = first.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="synthetic-fixture"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "The observer clock reads 148 hours."},
        )
    )
    first._run_host_analysis(mention)
    assert first.store.list_events(event_type=EventType.VIRTUAL_CLOCK_CREATED) == []

    missing_query = _append_virtual_request(
        first,
        source=mention,
        command="clock query observer",
    )
    first.request_tool(missing_query.id)
    missing_run = first.run_automation(max_jobs=1)
    assert missing_run["processed"][0]["status"] == "completed"
    missing_result = first.store.list_events(event_type=EventType.TOOL_ERROR)[-1]
    assert missing_result.payload["truth_domain"] == "virtual"
    assert "virtual clock does not exist" in missing_result.payload["error"]
    assert first.store.list_events(event_type=EventType.VIRTUAL_CLOCK_CREATED) == []

    create_request = _append_virtual_request(
        first,
        source=mention,
        command="clock create observer",
    )
    first.request_tool(create_request.id)
    first.run_automation(max_jobs=1)
    creation = first.store.list_events(event_type=EventType.VIRTUAL_CLOCK_CREATED)[0]
    assert creation.actor == Actor(kind=ActorKind.HOST, id="virtual-materializer")
    assert creation.causation_id == create_request.id
    assert creation.payload["source_event_ids"] == (create_request.id,)
    assert creation.payload["truth_domain"] == "virtual"
    assert creation.metadata["truth_domain"] == "virtual"
    assert creation.payload["clock"]["value"] is None
    assert creation.payload["clock"]["unit"] is None
    assert creation.payload["clock"]["unresolved_fields"] == ("unit", "value")

    set_request = _append_virtual_request(
        first,
        source=mention,
        command="clock set observer 10 pulse",
    )
    first.request_tool(set_request.id)
    first.run_automation(max_jobs=1)
    set_event = first.store.list_events(event_type=EventType.VIRTUAL_CLOCK_SET)[0]
    assert set_event.actor == Actor(kind=ActorKind.TOOL, id="virtual-runtime")
    assert set_event.causation_id == set_request.id
    assert set_event.payload["truth_domain"] == "virtual"
    first.close()

    second = OracleLabService(
        EventStore(database),
        home=home,
        config_dir=CONFIG,
        owns_store=True,
    )
    advance_request = _append_virtual_request(
        second,
        source=mention,
        command="clock advance observer 2 pulse",
    )
    second.request_tool(advance_request.id)
    second.run_automation(max_jobs=1)
    query_request = _append_virtual_request(
        second,
        source=mention,
        command="clock query observer",
    )
    second.request_tool(query_request.id)
    second.run_automation(max_jobs=1)

    result = next(
        event
        for event in second.store.list_events(event_type=EventType.TOOL_OUTPUT)
        if event.causation_id == query_request.id
    )
    value = json.loads(result.payload["output"])
    assert result.payload["truth_domain"] == "virtual"
    assert value["value"] == "12"
    assert value["unit"] == "pulse"
    assert [revision["operation"] for revision in value["revisions"]] == [
        "set",
        "advance",
    ]
    second.close()
