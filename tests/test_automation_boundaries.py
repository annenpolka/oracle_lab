from __future__ import annotations

import dataclasses
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jobs import JobQueue, JobStatus
from oracle_lab.providers import OracleGenerateResponse, ProviderError
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.tooling import ToolRequest, ToolResult, ToolStatus

CONFIG = Path(__file__).parents[1] / "config"


class StaticProvider:
    async def generate(self, request: Any) -> OracleGenerateResponse:
        body = {
            "id": "automation-response",
            "model": request.model_profile_id,
            "choices": [{"message": {"content": "done"}, "finish_reason": "stop"}],
            "usage": {},
        }
        return OracleGenerateResponse(
            raw_bytes=json.dumps(body).encode(),
            status_code=200,
            headers={},
            provider_name="replay",
            provider_model_id=request.model_profile_id,
            content="done",
            finish_reason="stop",
            usage={},
            elapsed_ms=1.0,
            request_id="automation-response",
            parsed=body,
        )


class FailingProvider:
    async def generate(self, request: Any) -> OracleGenerateResponse:
        del request
        raise ProviderError("provider unavailable")


class ConstantBroker:
    def __init__(self, *, status: ToolStatus = ToolStatus.OK) -> None:
        self.status = status

    def execute(self, request: ToolRequest, *, approved: bool = False) -> ToolResult:
        del approved
        return ToolResult(
            request_id=request.id,
            status=self.status,
            output="153792.0" if self.status is ToolStatus.OK else "",
            error=None if self.status is ToolStatus.OK else "calculation failed",
            elapsed_ms=1.0,
        )


def _service(tmp_path: Path, *, provider: Any | None = None) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        provider_factory=None if provider is None else lambda _profile: provider,
    )


def _tool_request(
    service: OracleLabService,
    source: Event,
    *,
    depth: int = 0,
    budget: int = 16,
) -> Event:
    request = ToolRequest(
        tool="calculator",
        execution="real_deterministic",
        input={"expression": "1.78 * 86400"},
        source_event_id=source.id,
        resume_oracle=True,
    )
    return service.store.append(
        Event.new(
            EventType.TOOL_REQUEST,
            actor=Actor(kind=ActorKind.HOST, id="test-dispatcher"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload={
                **request.to_dict(),
                "automation_depth": depth,
                "automation_budget_remaining": budget,
                "automation_loop_detector": "sha256-equivalent-event-v1",
            },
        )
    )


def _execute(service: OracleLabService, request: Event) -> Event:
    return service._execute_tool_job(
        SimpleNamespace(payload={"request_event_id": request.id, "approved": False})
    )


def test_tool_adapter_is_mechanical_traceable_and_budgeted(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("mechanical adapter")
    root = service.store.require(session["root_event_id"])
    request = _tool_request(service, root)
    service._tool_broker = ConstantBroker()

    result = _execute(service, request)

    adapter = service.store.list_events(event_type=EventType.TOOL_RESULT_ADAPTED)[0]
    continuation = next(
        event
        for event in service.store.list_events(event_type=EventType.ORACLE_REQUEST)
        if event.payload.get("operation") == "tool-result"
    )
    assert adapter.payload["content"] == "$ calculator 1.78 * 86400\n153792.0"
    assert "Tool calculator result" not in adapter.payload["content"]
    assert adapter.payload["formatter_id"] == "mechanical-tool-result"
    assert adapter.payload["formatter_version"] == 1
    assert adapter.payload["truth_domain"] == "real"
    assert adapter.payload["source_event_ids"] == (result.id, request.id)
    assert adapter.payload["automation_depth"] == 1
    assert adapter.payload["automation_budget_remaining"] == 15
    assert continuation.payload["automation_depth"] == 1
    assert continuation.payload["automation_budget_remaining"] == 14
    assert continuation.correlation_id == request.correlation_id


def test_repeated_equivalent_tool_event_stops_the_chain(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("loop detector")
    root = service.store.require(session["root_event_id"])
    service._tool_broker = ConstantBroker()
    first = _tool_request(service, root)
    _execute(service, first)
    second = _tool_request(service, root, depth=1, budget=12)

    _execute(service, second)

    stops = service.store.list_events(event_type=EventType.SYSTEM_AUTOMATION_STOPPED)
    assert stops[-1].payload["reason"] == "repeated_equivalent_event"
    assert stops[-1].payload["equivalent_event_id"]
    assert len(service.store.list_events(event_type=EventType.TOOL_RESULT_ADAPTED)) == 1


def test_depth_budget_and_tool_failure_are_explicit_stop_events(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("terminal boundaries")
    root = service.store.require(session["root_event_id"])
    service._tool_broker = ConstantBroker()

    _execute(
        service,
        _tool_request(
            service,
            root,
            depth=service.runtime_config.policies.max_auto_depth,
        ),
    )
    _execute(service, _tool_request(service, root, budget=0))
    service._tool_broker = ConstantBroker(status=ToolStatus.ERROR)
    _execute(service, _tool_request(service, root, depth=1, budget=8))

    reasons = {
        event.payload["reason"]
        for event in service.store.list_events(event_type=EventType.SYSTEM_AUTOMATION_STOPPED)
    }
    assert {"max_depth", "budget_exhausted", "tool_failure"} <= reasons


def test_explicit_pause_blocks_queued_work_until_human_resume(tmp_path: Path) -> None:
    service = _service(tmp_path, provider=StaticProvider())
    service.new_session("pause boundary")
    service.ask("確認しろ。")

    pause = service.pause("inspect first")
    stopped = service.run_automation(max_jobs=1)
    resumed = service.resume("continue")
    completed = service.run_automation(max_jobs=1)

    assert pause["type"] == EventType.HUMAN_PAUSE.value
    assert stopped == {"processed": [], "stopped": "paused", "event_id": pause["id"]}
    assert resumed["type"] == EventType.HUMAN_RESUME.value
    assert completed["processed"][0]["status"] == "completed"


def test_paused_branch_is_excluded_while_an_unpaused_branch_keeps_running(
    tmp_path: Path,
) -> None:
    handled: list[str] = []
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        job_handler=lambda job: handled.append(str(job.payload["label"])),
    )
    session = service.new_session("multi-branch pause boundary")
    session_id = str(session["id"])
    main_branch = str(session["current_branch_id"])
    forked = service.fork(str(session["root_event_id"]), "unpaused sibling")
    sibling_branch = str(forked["id"])
    service.switch_branch(session_id, main_branch)
    pause = service.pause("hold only main")
    queue = service._job_queue()
    paused_job = queue.enqueue(
        "fixture",
        {"label": "paused"},
        session_id=session_id,
        branch_id=main_branch,
        priority=100,
    )
    running_job = queue.enqueue(
        "fixture",
        {"label": "running"},
        session_id=session_id,
        branch_id=sibling_branch,
    )

    first = service.run_automation(max_jobs=1)

    assert first["processed"][0]["job_id"] == running_job.id
    assert handled == ["running"]
    assert queue.require(paused_job.id).attempts == 0
    assert queue.require(paused_job.id).status.value == "pending"
    stopped = service.run_automation(max_jobs=1)
    assert stopped == {
        "processed": [],
        "stopped": "paused",
        "event_id": pause["id"],
    }

    service.resume("release main")
    completed = service.run_automation(max_jobs=1)
    assert completed["processed"][0]["job_id"] == paused_job.id
    assert handled == ["running", "paused"]


def test_ordinary_handler_failure_does_not_gain_archive_recovery_attempt(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def fail(job: Any) -> None:
        calls.append(job.id)
        raise RuntimeError("ordinary handler failure")

    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        job_handler=fail,
    )
    session = service.new_session("ordinary retry boundary")
    job = service._job_queue().enqueue(
        "fixture",
        {},
        session_id=str(session["id"]),
        branch_id=str(session["current_branch_id"]),
        max_attempts=1,
    )

    failed = service.run_automation(max_jobs=1)
    idle = service.run_automation(max_jobs=1)

    assert failed["processed"] == [
        {
            "job_id": job.id,
            "status": "failed",
            "error": "ordinary handler failure",
        }
    ]
    assert idle == {"processed": [], "stopped": "idle"}
    assert calls == [job.id]
    assert service._job_queue().require(job.id).status.value == "dead_letter"
    assert not [
        event
        for event in service.store.list_events()
        if event.payload.get("id") == job.id
        and event.metadata.get("lease_expiry_recovery") is not None
    ]


def test_long_handler_heartbeat_prevents_a_second_runner_from_re_leasing_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "oracle.db"
    handler_started = threading.Event()
    release_handler = threading.Event()
    first_calls: list[str] = []
    second_calls: list[str] = []
    run_results: list[dict[str, Any]] = []
    run_errors: list[BaseException] = []

    def blocking_handler(job: Any) -> str:
        first_calls.append(job.id)
        handler_started.set()
        if not release_handler.wait(timeout=3):
            raise RuntimeError("test handler release timed out")
        return "first-runner"

    first = OracleLabService(
        EventStore(database),
        home=tmp_path / "first-home",
        config_dir=CONFIG,
        job_handler=blocking_handler,
    )
    second = OracleLabService(
        EventStore(database),
        home=tmp_path / "second-home",
        config_dir=CONFIG,
        job_handler=lambda job: second_calls.append(job.id),
    )
    monkeypatch.setattr(OracleLabService, "_automation_lease_seconds", lambda _self: 0.12)
    job = first._job_queue().enqueue("fixture", {}, max_attempts=2)

    def run_first() -> None:
        try:
            run_results.append(first.run_automation(max_jobs=1))
        except BaseException as error:
            run_errors.append(error)

    started_at = time.monotonic()
    runner = threading.Thread(target=run_first, name="test-first-automation-runner")
    runner.start()
    assert handler_started.wait(timeout=2)

    deadline = time.monotonic() + 2
    heartbeats: list[Event] = []
    while time.monotonic() < deadline:
        heartbeats = first.store.list_events(event_type=EventType.JOB_HEARTBEAT)
        if len(heartbeats) >= 3 and time.monotonic() - started_at >= 0.3:
            break
        time.sleep(0.01)
    assert len(heartbeats) >= 3
    assert time.monotonic() - started_at >= 0.3

    competing = second.run_automation(max_jobs=1)

    assert competing == {"processed": [], "stopped": "idle"}
    assert second_calls == []
    assert first._job_queue().require(job.id).status is JobStatus.LEASED
    release_handler.set()
    runner.join(timeout=3)
    assert not runner.is_alive()
    assert run_errors == []
    assert run_results[0]["processed"][0]["status"] == "completed"
    completed = first._job_queue().require(job.id)
    assert completed.status is JobStatus.COMPLETED
    assert completed.attempts == 1
    assert first_calls == [job.id]
    leases = [
        event
        for event in first.store.list_events(event_type=EventType.JOB_LEASED)
        if event.payload["id"] == job.id
    ]
    assert len(leases) == 1
    assert str(leases[0].payload["worker_id"]).startswith("oracle-cli:")
    assert not [
        thread
        for thread in threading.enumerate()
        if thread.name == f"oracle-lease-heartbeat-{job.id}"
    ]


def test_heartbeat_failure_fails_closed_and_leaves_no_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    heartbeat_attempted = threading.Event()

    def handler(_job: Any) -> str:
        if not heartbeat_attempted.wait(timeout=2):
            raise RuntimeError("heartbeat was not attempted")
        return "handler-finished"

    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        job_handler=handler,
    )
    monkeypatch.setattr(service, "_automation_lease_seconds", lambda: 0.12)

    def fail_heartbeat(
        _queue: JobQueue,
        _job_id: str,
        _worker_id: str,
        **_options: Any,
    ) -> None:
        heartbeat_attempted.set()
        raise RuntimeError("injected heartbeat failure")

    monkeypatch.setattr(JobQueue, "heartbeat", fail_heartbeat)
    job = service._job_queue().enqueue("fixture", {}, max_attempts=1)

    result = service.run_automation(max_jobs=1)

    assert result["processed"] == [
        {
            "job_id": job.id,
            "status": "failed",
            "error": f"lease heartbeat failed for job {job.id}: injected heartbeat failure",
        }
    ]
    assert service._job_queue().require(job.id).status is JobStatus.DEAD_LETTER
    assert not [
        thread
        for thread in threading.enumerate()
        if thread.name == f"oracle-lease-heartbeat-{job.id}"
    ]


def test_provider_failure_emits_a_terminal_automation_event(tmp_path: Path) -> None:
    service = _service(tmp_path, provider=FailingProvider())
    config = service.runtime_config
    profile = next(iter(config.models.values()))
    provider = config.providers[profile.provider]
    service._config = dataclasses.replace(
        config,
        providers={
            **config.providers,
            profile.provider: dataclasses.replace(provider, max_retries=0),
        },
    )
    service.new_session("provider failure")
    service.ask("確認しろ。")

    run = service.run_automation(max_jobs=1)

    assert run["processed"][0]["status"] == "failed"
    stop = service.store.list_events(event_type=EventType.SYSTEM_AUTOMATION_STOPPED)[0]
    assert stop.payload["reason"] == "provider_failure"
