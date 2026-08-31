from __future__ import annotations

import json
import subprocess

import pytest

from oracle_lab.config import ConfigError, SandboxConfig
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.store import EventStore
from oracle_lab.tooling import (
    DockerShellSandbox,
    SafeCalculator,
    ToolApprovalRequired,
    ToolBroker,
    ToolExecution,
    ToolPolicy,
    ToolPolicyError,
    ToolRequest,
    ToolResult,
    ToolStatus,
    ToolWorker,
)


class StubShell:
    def run(self, command, *, request_id, **_kwargs):
        return ToolResult(request_id, ToolStatus.OK, output=command)


def test_safe_calculator_allows_arithmetic_and_rejects_code() -> None:
    calculator = SafeCalculator()
    assert calculator.evaluate("1.78 * 86400") == 153792
    with pytest.raises(ToolPolicyError):
        calculator.evaluate("__import__('os').system('id')")
    with pytest.raises(ToolPolicyError):
        calculator.evaluate("9999999 ** 9999999")


def test_docker_sandbox_argv_has_no_egress_mount_or_host_fallback() -> None:
    config = SandboxConfig()
    sandbox = DockerShellSandbox(config, docker_executable="docker-does-not-exist")
    argv = sandbox._command(
        container_name="oracle-lab-test",
        command="python -c 'print(1)'",
        files={"input.txt": "explicit"},
    )
    joined = " ".join(argv)
    assert "--pull never" in joined
    assert "--network none" in joined
    assert "--read-only" in argv
    assert "--cap-drop ALL" in joined
    assert "--security-opt no-new-privileges" in joined
    assert "--memory 256m" in joined
    assert "--pids-limit 64" in joined
    assert "--mount" not in argv
    assert "--volume" not in argv
    assert "-v" not in argv

    result = sandbox.run("echo forbidden", request_id="tlr_none", source_event_id="evt_none")
    assert result.status == ToolStatus.ERROR
    assert "host execution fallback is forbidden" in (result.error or "")
    with pytest.raises(ToolPolicyError):
        sandbox._command(
            container_name="oracle-lab-test",
            command="true",
            files={"../escape": "bad"},
        )


def test_sandbox_config_rejects_writable_root_contract() -> None:
    with pytest.raises(ConfigError, match="root filesystem must be read-only"):
        SandboxConfig(read_only_root=False)


def test_docker_sandbox_resolves_and_runs_immutable_local_image_id(monkeypatch) -> None:
    digest = "sha256:" + "a" * 64
    sandbox = DockerShellSandbox(SandboxConfig(image="python:fixture"))

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=(digest + "\n").encode(), stderr=b""
        ),
    )

    assert sandbox._resolve_image_identifier("/usr/bin/docker") == digest
    argv = sandbox._command(
        container_name="oracle-lab-test",
        command="true",
        files=None,
        image=digest,
    )
    assert digest in argv
    assert "python:fixture" not in argv


def test_pending_approval_is_not_misreported_as_denial() -> None:
    broker = ToolBroker(policy=ToolPolicy({"shell": "ask"}))
    request = ToolRequest(
        "shell",
        ToolExecution.REAL_SANDBOX,
        {"command": "echo hi"},
        "evt_source",
    )
    result = broker.execute(request)
    assert result.status == ToolStatus.PENDING_APPROVAL
    with pytest.raises(ToolPolicyError, match="not a denial"):
        result.to_event(request)


def test_tool_request_defensively_freezes_nested_input() -> None:
    raw = {"command": "cat input.txt", "files": {"input.txt": bytearray(b"before")}}
    request = ToolRequest(
        "shell",
        ToolExecution.REAL_SANDBOX,
        raw,
        "evt_source",
    )
    raw["files"]["input.txt"][:] = b"after!"
    assert request.to_dict()["input"]["files"]["input.txt"] == b"before"
    with pytest.raises(TypeError):
        request.input["files"]["input.txt"] = b"forbidden"


def test_tool_worker_records_started_output_and_usage() -> None:
    store = EventStore()
    request_event = Event.new(
        EventType.TOOL_REQUEST,
        actor=Actor(kind=ActorKind.HUMAN, id="tester"),
        session_id="ses_tool",
        branch_id="br_main",
        payload={
            "tool": "calculator",
            "execution": "real_deterministic",
            "input": {"expression": "1.78 * 86400"},
            "resume_oracle": False,
            "timeout_ms": 5000,
        },
    )
    store.append(request_event)
    worker = ToolWorker(ToolBroker(), store)

    result = worker.run(request_event)

    assert result.type == EventType.TOOL_OUTPUT
    assert result.payload["output"] == "153792.0"
    assert result.payload["truth_domain"] == "real"
    assert result.metadata["truth_domain"] == "real"
    started = store.list_events(event_type=EventType.TOOL_STARTED)
    usage = store.list_events(event_type=EventType.USAGE_TOOL)
    assert result.parent_event_id == started[-1].id
    assert usage[-1].parent_event_id == result.id
    assert usage[-1].payload["tool_id"] == "calculator"
    assert len(worker.broker.audit_log) == 1

    replayed = worker.run(request_event)
    assert replayed.id == result.id
    assert len(store.list_events(event_type=EventType.TOOL_OUTPUT)) == 1
    assert len(store.list_events(event_type=EventType.USAGE_TOOL)) == 1
    assert len(worker.broker.audit_log) == 1


def test_tool_worker_requires_human_before_shell_started_event() -> None:
    store = EventStore()
    request_event = Event.new(
        EventType.TOOL_REQUEST,
        actor=Actor(kind=ActorKind.HUMAN, id="tester"),
        payload={
            "tool": "shell",
            "execution": "real_sandbox",
            "input": {"command": "echo hi"},
            "timeout_ms": 1000,
        },
    )
    store.append(request_event)

    with pytest.raises(ToolApprovalRequired):
        ToolWorker(ToolBroker(), store).run(request_event)
    assert store.list_events(event_type=EventType.TOOL_STARTED) == []


def test_tool_worker_rejects_forged_bool_and_requires_persisted_human_approval() -> None:
    store = EventStore()
    request_event = store.append(
        Event.new(
            EventType.TOOL_REQUEST,
            actor=Actor(kind=ActorKind.HOST, id="test"),
            session_id="ses_tool",
            branch_id="br_main",
            payload={
                "tool": "shell",
                "execution": "real_sandbox",
                "input": {"command": "echo contained"},
                "timeout_ms": 1000,
            },
        )
    )
    worker = ToolWorker(
        ToolBroker(policy=ToolPolicy({"shell": "ask"}), shell=StubShell()),
        store,
    )

    with pytest.raises(ToolApprovalRequired, match="persisted matching human"):
        worker.run(request_event, approved=True)

    approval = store.append(
        Event.new(
            EventType.TOOL_APPROVED,
            actor=Actor(kind=ActorKind.HUMAN, id="curator"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=request_event.id,
            causation_id=request_event.id,
            payload={"request_event_id": request_event.id},
        )
    )
    result = worker.run(request_event, approved=True, approval_event=approval)

    assert result.type == EventType.TOOL_OUTPUT
    assert result.payload["output"] == "echo contained"
    assert result.payload["truth_domain"] == "sandbox"
    assert store.list_events(event_type=EventType.TOOL_STARTED)[-1].parent_event_id == approval.id


def test_policy_config_names_map_to_broker_names() -> None:
    policy = ToolPolicy.from_config(
        {
            "calculator": "auto",
            "unit_conversion": "auto",
            "regex_text": "auto",
            "checksum": "auto",
            "file_parsing": "auto",
            "python_sandbox": "ask",
            "shell_sandbox": "deny",
            "virtual_world": "auto",
            "web_verify": "ask",
        }
    )
    assert policy.mode_for("python") == "ask"
    assert policy.mode_for("unit_convert") == "auto"
    assert policy.mode_for("regex") == "auto"
    assert policy.mode_for("checksum") == "auto"
    assert policy.mode_for("file_parse") == "auto"
    assert policy.mode_for("shell") == "deny"
    assert policy.mode_for("virtual") == "auto"


@pytest.mark.parametrize(
    ("tool", "input_value", "expected"),
    [
        (
            "unit_convert",
            {"value": "1.78", "from_unit": "day", "to_unit": "hour"},
            {"from_unit": "day", "input": "1.78", "to_unit": "h", "value": "42.72"},
        ),
        (
            "regex",
            {"operation": "findall", "pattern": r"\d+[.\d]*", "text": "34.7 and 42"},
            {"operation": "findall", "result": ["34.7", "42"]},
        ),
        (
            "checksum",
            {"algorithm": "sha256", "content": "abc"},
            {
                "algorithm": "sha256",
                "bytes": 3,
                "digest": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            },
        ),
        (
            "file_parse",
            {"format": "toml", "content": '[model]\nslug = "r1"\n'},
            {"format": "toml", "parsed": {"model": {"slug": "r1"}}},
        ),
    ],
)
def test_bounded_deterministic_tools_are_real_truth_domain(
    tool: str,
    input_value: dict[str, object],
    expected: dict[str, object],
) -> None:
    broker = ToolBroker()
    request = ToolRequest(
        tool,
        ToolExecution.REAL_DETERMINISTIC,
        input_value,
        "evt_source",
    )

    result = broker.execute(request)
    event = result.to_event(request)

    assert result.status is ToolStatus.OK
    assert json.loads(result.output) == expected
    assert result.metadata["truth_domain"] == "real"
    assert event.payload["truth_domain"] == "real"
