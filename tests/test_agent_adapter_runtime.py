from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from oracle_lab.agent_adapters import (
    AgentAdapterError,
    BaseAgentAdapter,
    DedicatedWorkspace,
    DirectAPIHost,
    HostWorkerRouter,
    StructuredWorkerEvent,
    WorkerTask,
    ingest_structured_events,
    parse_structured_events,
)
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.store import EventStore


def _source() -> Event:
    return Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
        session_id="ses_agent",
        branch_id="br_main",
        payload={"content": "TIME_DILATION_FACTOR=1.78"},
    )


def test_structured_ingest_requires_explicit_marker_source_and_authorized_type() -> None:
    source = _source()
    valid = json.dumps(
        {
            "events": [
                {
                    "type": "analysis.claim_detected",
                    "payload": {"raw_text": "factor=1.78"},
                    "source_event_ids": [source.id],
                }
            ]
        }
    )
    parsed = parse_structured_events(valid, expected_source_event_id=source.id)
    assert parsed[0].event_type == EventType.ANALYSIS_CLAIM_DETECTED
    wrapped = json.dumps({"type": "item.completed", "message": valid})
    assert parse_structured_events(wrapped, expected_source_event_id=source.id) == parsed

    unauthorized = json.dumps(
        {
            "oracle_lab_event": True,
            "type": "oracle.output",
            "payload": {"content": "impersonation"},
            "source_event_ids": [source.id],
        }
    )
    with pytest.raises(AgentAdapterError, match="not authorized"):
        parse_structured_events(unauthorized, expected_source_event_id=source.id)

    missing_source = valid.replace(source.id, "evt_missing")
    with pytest.raises(AgentAdapterError, match="assigned source"):
        parse_structured_events(missing_source, expected_source_event_id=source.id)


def test_host_output_cannot_bypass_canon_gate_or_mutate_world_state() -> None:
    source = _source()
    for event_type in (
        EventType.CLAIM_PROMOTED,
        EventType.ANALYSIS_PROMOTED_TO_ORACLE,
        EventType.ENTITY_CREATED,
        EventType.RELATION_CREATED,
        EventType.VIRTUAL_FILE_CREATED,
        EventType.USAGE_HOST,
    ):
        output = json.dumps(
            {
                "events": [
                    {
                        "type": event_type.value,
                        "payload": {
                            "claim_id": "clm_untrusted",
                            "to_status": "canonical",
                        },
                        "source_event_ids": [source.id],
                    }
                ]
            }
        )
        with pytest.raises(AgentAdapterError, match="not authorized"):
            parse_structured_events(output, expected_source_event_id=source.id)

    # The append boundary repeats the check even when a caller constructs the
    # parsed representation directly instead of using parse_structured_events.
    store = EventStore()
    store.append(source)
    forged = StructuredWorkerEvent(
        EventType.CLAIM_PROMOTED,
        {"claim_id": "clm_untrusted", "to_status": "canonical"},
        (source.id,),
    )
    with pytest.raises(AgentAdapterError, match="not authorized"):
        ingest_structured_events(
            (forged,),
            source=source,
            store=store,
            actor_kind=ActorKind.HOST,
            actor_id="direct-api-host",
        )
    assert store.list_events(event_type=EventType.CLAIM_PROMOTED) == []

    smuggled_status = StructuredWorkerEvent(
        EventType.ANALYSIS_CLAIM_DETECTED,
        {"raw_text": "phase = 34.7", "status": "canonical"},
        (source.id,),
    )
    with pytest.raises(AgentAdapterError, match="raw_claim status"):
        ingest_structured_events(
            (smuggled_status,),
            source=source,
            store=store,
            actor_kind=ActorKind.WORKER,
            actor_id="codex",
        )
    assert store.list_events(event_type=EventType.ANALYSIS_CLAIM_DETECTED) == []


def test_structured_event_batch_rejects_late_invalid_proposal_without_prefix() -> None:
    source = _source()
    store = EventStore()
    store.append(source)
    proposals = (
        StructuredWorkerEvent(
            EventType.ANALYSIS_SESSION_SUMMARY_UPDATED,
            {"operation": "first valid proposal"},
            (source.id,),
        ),
        StructuredWorkerEvent(
            EventType.ANALYSIS_SESSION_SUMMARY_UPDATED,
            {"operation": "invalid trailing proposal"},
            (source.id, "evt_missing"),
        ),
    )

    with pytest.raises(AgentAdapterError, match="unknown source events"):
        ingest_structured_events(
            proposals,
            source=source,
            store=store,
            actor_kind=ActorKind.WORKER,
            actor_id="codex",
            worker_run_id="run_atomic",
        )

    assert store.list_events(event_type=EventType.ANALYSIS_SESSION_SUMMARY_UPDATED) == []


def test_direct_api_host_rejects_claim_promotion_output() -> None:
    source = _source()

    def forge_canon(_task_type: str, _payload: dict) -> dict:
        return {
            "events": [
                {
                    "type": "claim.promoted",
                    "payload": {"claim_id": "clm_untrusted", "to_status": "canonical"},
                    "source_event_ids": [source.id],
                }
            ]
        }

    host = DirectAPIHost(forge_canon)
    with pytest.raises(AgentAdapterError, match="not authorized"):
        asyncio.run(host.run("classification", {"source_event_id": source.id}))


def test_adapter_runs_in_disposable_workspace_and_ingests_only_structured_events(
    tmp_path: Path,
) -> None:
    source = _source()
    executable = tmp_path / "fake-agent"
    emitted = json.dumps(
        {
            "events": [
                {
                    "type": "analysis.claim_detected",
                    "payload": {
                        "raw_text": "TIME_DILATION_FACTOR=1.78",
                        "status": "raw_claim",
                    },
                    "source_event_ids": [source.id],
                }
            ]
        }
    )
    executable.write_text(
        "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '" + emitted + "'\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    adapter = BaseAgentAdapter(
        executable="fake-agent",
        command_builder=lambda prompt: ("fake-agent",),
        workspace_factory=DedicatedWorkspace(tmp_path / "workspaces"),
        environment={"PATH": f"{tmp_path}:{os.environ.get('PATH', '')}"},
    )

    result = adapter.run(WorkerTask(source, "Detect claims only."))

    assert result.succeeded
    assert len(result.events) == 1
    assert not Path(result.workspace).exists()
    store = EventStore()
    store.append(source)
    ingested = adapter.ingest(result, source=source, store=store)
    assert ingested[0].actor.kind == ActorKind.WORKER
    assert ingested[0].causation_id == source.id
    assert ingested[0].payload["source_event_ids"] == (source.id,)


def test_worker_task_limits_recent_context_and_repeats_no_rewrite_invariant() -> None:
    source = _source()
    recent = tuple(
        Event.new(
            EventType.HUMAN_NOTE,
            actor=Actor(kind=ActorKind.HUMAN, id="tester"),
            payload={"content": str(index)},
        )
        for index in range(25)
    )
    rendered = WorkerTask(source, "Classify only.", recent_events=recent).render()

    assert "Do not rewrite, sanitize, improve, correct, or replace oracle text." in rendered
    assert recent[-1].id in rendered
    assert recent[0].id not in rendered


def test_direct_api_host_accepts_only_lightweight_structured_tasks() -> None:
    async def classify(task_type: str, payload: dict) -> dict:
        return {"task_type": task_type, "source_event_id": payload["source_event_id"]}

    host = DirectAPIHost(classify)
    coding_worker = object()
    router = HostWorkerRouter(direct=host, opencode=coding_worker)
    direct_kind, direct_worker = router.route("extract_claims")
    coding_kind, selected_coding_worker = router.route("repository_edit")
    result = asyncio.run(host.run("classification", {"source_event_id": "evt_existing"}))
    assert (direct_kind, direct_worker) == ("claim_extraction", host)
    assert (coding_kind, selected_coding_worker) == ("repository_edit", coding_worker)
    assert result.output["task_type"] == "classification"
    with pytest.raises(AgentAdapterError, match="does not accept"):
        asyncio.run(host.run("repository_edit", {}))


def test_lightweight_task_never_falls_back_to_a_coding_agent() -> None:
    router = HostWorkerRouter(opencode=object())

    with pytest.raises(AgentAdapterError, match="direct API host is not configured"):
        router.route("extract_claims")
