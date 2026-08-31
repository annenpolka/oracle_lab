from __future__ import annotations

import contextlib
import dataclasses
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any

from oracle_lab.agent_adapters import DirectAPIHost, HostWorkerRouter
from oracle_lab.dispatcher import DecisionStatus, EventDispatcher, default_rules
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.observability import ObservabilityService
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.usage import UsageKind, UsageService

CONFIG = Path(__file__).parents[1] / "config"


def _service(
    tmp_path: Path,
    *,
    host_worker_router: HostWorkerRouter | None = None,
) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        host_worker_router=host_worker_router,
    )


def _set_policies(
    service: OracleLabService,
    *,
    analysis: dict[str, bool] | None = None,
    human_gate: dict[str, bool] | None = None,
) -> None:
    config = service.runtime_config
    policies = dataclasses.replace(
        config.policies,
        analysis=config.policies.analysis if analysis is None else analysis,
        human_gate=config.policies.human_gate if human_gate is None else human_gate,
    )
    service._config = dataclasses.replace(config, policies=policies)


def _oracle_output(service: OracleLabService, *, content: str) -> Event:
    session_id, branch_id = service._active()
    parent = service.store.list_events(branch_id=branch_id, ascending=False, limit=1)[0]
    return service.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent.id,
            causation_id=parent.id,
            correlation_id=parent.correlation_id,
            payload={
                "content": content,
                "model_profile_id": "r1-initial-openrouter",
                "provider": "openrouter",
            },
        )
    )


def test_analysis_policy_filters_local_consumers_and_host_jobs(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _set_policies(
        service,
        analysis={
            "claims": False,
            "mechanisms": False,
            "contradictions": False,
            "attractors": False,
            "motifs": True,
        },
    )
    service.new_session("analysis switches")
    output = _oracle_output(
        service,
        content="## report\nTIME_DILATION_FACTOR=1.78\n99 hours\n/dev/void",
    )

    derived = service._run_host_analysis(output)

    event_types = {event.type for event in derived}
    assert EventType.ANALYSIS_CLAIM_DETECTED not in event_types
    assert EventType.ANALYSIS_NEW_MECHANISM_DETECTED not in event_types
    assert EventType.ANALYSIS_NUMERIC_INCONSISTENCY not in event_types
    assert EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED not in event_types
    assert EventType.ANALYSIS_MOTIF_DETECTED in event_types

    second = _oracle_output(service, content="void")
    service._enqueue_host_analysis_jobs(second)
    assert {job.kind for job in service._job_queue().list_jobs()} == {
        "extract_entities",
        "detect_motifs",
        "detect_recurrence",
        "detect_tool_intent",
    }


def test_probe_gate_can_be_automatic_but_canon_always_requires_human(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    _set_policies(
        service,
        human_gate={
            "probe_generation": False,
            "canon_promotion": False,
            "branch_creation": False,
        },
    )
    session = service.new_session("automatic gates")
    root = service.store.require(session["root_event_id"])

    probe_result = service.propose_probe(root.id)

    assert probe_result["proposal"]["payload"]["approval_required"] is False
    assert service.store.list_events(event_type=EventType.ORACLE_CONTEXT_MESSAGE)
    assert service.store.list_events(event_type=EventType.ORACLE_REQUEST)
    assert service._job_queue().list_jobs(kind="await_human_approval") == []
    assert service._job_queue().list_jobs(kind="oracle.generate")
    assert service._pending_human_judgment() is None

    _oracle_output(service, content="synthetic fixture: pain phase = 34.7")
    claim_event = service.store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="extract_claims"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={
                "raw_text": "pain phase = 34.7",
                "status": "raw_claim",
                "source_event_ids": [root.id],
            },
        )
    )
    claim_id = service.store.connection.execute(
        "SELECT id FROM claims WHERE source_event_id = ?", (root.id,)
    ).fetchone()[0]
    candidate = service.store.append(
        Event.new(
            EventType.ANALYSIS_CANON_CANDIDATE,
            actor=Actor(kind=ActorKind.HOST, id="canon-review"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=claim_event.id,
            causation_id=claim_event.id,
            correlation_id=root.correlation_id,
            payload={"claim_id": claim_id, "source_event_ids": [claim_event.id]},
        )
    )
    gated = EventDispatcher(default_rules()).evaluate(candidate)
    assert gated[0].status == DecisionStatus.PENDING_APPROVAL

    decisions = service._dispatcher().dispatch(candidate)

    assert decisions[0].status == DecisionStatus.PENDING_APPROVAL
    assert service.store.list_events(event_type=EventType.CLAIM_PROMOTED) == []
    assert service._job_queue().list_jobs(kind="await_human_approval")
    assert service._pending_human_judgment() == candidate
    status = service.store.connection.execute(
        "SELECT status FROM claims WHERE id = ?", (claim_id,)
    ).fetchone()[0]
    assert status == "raw_claim"


def test_python_policy_mapping_and_virtual_command_allowlist_reach_broker(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    config = service.runtime_config
    service._config = dataclasses.replace(
        config,
        tools=dataclasses.replace(config.tools, allowed_virtual_commands=("ls",)),
    )
    session = service.new_session("tool policy wiring")
    source = service.store.require(session["root_event_id"])
    request = service.store.append(
        Event.new(
            EventType.TOOL_REQUEST,
            actor=Actor(kind=ActorKind.HUMAN, id="tester"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload={
                "tool": "virtual",
                "execution": "virtual",
                "input": {"command": "cat /"},
                "source_event_id": source.id,
                "resume_oracle": False,
                "timeout_ms": 5_000,
            },
        )
    )

    assert service._tool_policy_modes()["python"] == "auto"
    assert service._broker().policy.mode_for("python") == "auto"
    service.request_tool(request.id)
    run = service.run_automation(max_jobs=1)

    result = run["processed"][0]["result"]
    assert result["type"] == EventType.TOOL_DENIED.value
    assert "not allowed by policy: cat" in result["payload"]["error"]


class _Span:
    def __init__(self) -> None:
        self.attributes: dict[str, Any] = {}
        self.exceptions: list[Exception] = []

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def record_exception(self, error: Exception) -> None:
        self.exceptions.append(error)


class _Tracer:
    def __init__(self) -> None:
        self.names: list[str] = []
        self.spans: list[_Span] = []

    @contextlib.contextmanager
    def start_as_current_span(self, name: str, *, attributes: dict[str, Any]):
        self.names.append(name)
        span = _Span()
        span.attributes.update(attributes)
        self.spans.append(span)
        yield span


def test_each_host_call_records_usage_and_service_observability(
    tmp_path: Path,
    caplog,
) -> None:
    calls = 0

    def frontier_host(_task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("host unavailable")
        source_id = payload["source_event_id"]
        return {
            "provider": "frontier-provider",
            "model": "frontier-host",
            "usage": {
                "prompt_tokens": 17,
                "completion_tokens": 5,
                "reasoning_tokens": 2,
                "cost": "0.0042",
                "ttft_ms": 3.5,
            },
            "events": [
                {
                    "type": "analysis.claim_detected",
                    "payload": {"raw_text": "factor=1.78", "status": "raw_claim"},
                    "source_event_ids": [source_id],
                }
            ],
        }

    router = HostWorkerRouter(direct=DirectAPIHost(frontier_host))
    service = _service(tmp_path, host_worker_router=router)
    tracer = _Tracer()
    logger = logging.getLogger("oracle_lab.test.host")
    service.observability = ObservabilityService(service.store, logger=logger, tracer=tracer)
    service.new_session("host telemetry")
    output = _oracle_output(service, content="factor=1.78")
    service._enqueue_host_analysis_jobs(output)

    with caplog.at_level(logging.INFO, logger=logger.name):
        first = service.run_automation(max_jobs=1)
        second = service.run_automation(max_jobs=1)

    assert first["processed"][0]["status"] == "completed"
    assert second["processed"][0]["status"] == "failed"
    records = UsageService(service.store).list_records(kind=UsageKind.HOST)
    assert len(records) == 2
    assert records[0].provider_id == "frontier-provider"
    assert records[0].model_id == "frontier-host"
    assert records[0].prompt_tokens == 17
    assert records[0].completion_tokens == 5
    assert records[0].reasoning_tokens == 2
    assert records[0].provider_cost == Decimal("0.0042")
    assert records[0].latency_ms > 0
    assert records[0].ttft_ms == 3.5
    failed_usage = service.store.require(records[1].event_id)
    assert failed_usage.metadata["status"] == "failed"
    assert failed_usage.payload["latency_ms"] >= 0
    assert "host.worker.call" in tracer.names
    operation_logs = [
        record.oracle_lab for record in caplog.records if hasattr(record, "oracle_lab")
    ]
    assert any(
        item.get("operation") == "host.worker.call" and item.get("status") == "completed"
        for item in operation_logs
    )
    assert any(
        item.get("operation") == "host.worker.call" and item.get("status") == "failed"
        for item in operation_logs
    )
