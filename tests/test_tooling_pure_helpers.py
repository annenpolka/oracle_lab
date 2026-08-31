from __future__ import annotations

import hashlib
from typing import Any

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.tooling import (
    ToolExecution,
    ToolRequest,
    mechanical_tool_result_content,
    tool_loop_signature,
)


def _result_event(**overrides: Any) -> Event:
    payload = {
        "status": "ok",
        "output": "3",
        "error": None,
        "exit_code": 0,
        "truth_domain": "real",
        **overrides,
    }
    return Event.new(
        EventType.TOOL_OUTPUT,
        actor=Actor(kind=ActorKind.TOOL, id="fixture-tool"),
        session_id="ses_fixture",
        branch_id="br_fixture",
        payload=payload,
    )


@pytest.mark.parametrize(
    ("tool", "execution", "tool_input", "output", "expected"),
    [
        (
            "shell",
            ToolExecution.REAL_SANDBOX,
            {"command": "printf exact"},
            "exact output",
            "$ printf exact\nexact output",
        ),
        (
            "calculator",
            ToolExecution.REAL_DETERMINISTIC,
            {"expression": "1 + 2"},
            "3",
            "$ calculator 1 + 2\n3",
        ),
        (
            "web_verify",
            ToolExecution.VERIFICATION,
            {"url": "https://example.invalid/evidence"},
            "verified",
            "$ GET https://example.invalid/evidence\nverified",
        ),
        (
            "custom",
            ToolExecution.VIRTUAL,
            {"z": 2, "a": {"b": 1}},
            "canonical",
            '$ custom {"a":{"b":1},"z":2}\ncanonical',
        ),
        (
            "shell",
            ToolExecution.REAL_SANDBOX,
            {"command": "exit 7"},
            7,
            "$ exit 7\n7",
        ),
    ],
)
def test_mechanical_tool_result_content_preserves_exact_formatting(
    tool: str,
    execution: ToolExecution,
    tool_input: dict[str, Any],
    output: Any,
    expected: str,
) -> None:
    request = ToolRequest(tool, execution, tool_input, "evt_source")

    assert mechanical_tool_result_content(request, _result_event(output=output)) == expected


def test_tool_loop_signature_ignores_envelope_identity() -> None:
    first_request = ToolRequest(
        "calculator",
        ToolExecution.REAL_DETERMINISTIC,
        {"expression": "1 + 2"},
        "evt_source_a",
        id="tlr_identity_a",
    )
    second_request = ToolRequest(
        "calculator",
        ToolExecution.REAL_DETERMINISTIC,
        {"expression": "1 + 2"},
        "evt_source_b",
        id="tlr_identity_b",
    )
    first_event = _result_event()
    second_event = Event.new(
        EventType.TOOL_OUTPUT,
        actor=Actor(kind=ActorKind.TOOL, id="another-envelope-actor"),
        session_id="ses_other",
        branch_id="br_other",
        parent_event_id="evt_parent_other",
        correlation_id="cor_other",
        payload={
            "status": "ok",
            "output": "3",
            "error": None,
            "exit_code": 0,
            "truth_domain": "real",
        },
    )

    signature = tool_loop_signature(first_request, first_event)
    expected_document = (
        '{"error":null,"execution":"real_deterministic","exit_code":0,'
        '"input":{"expression":"1 + 2"},"output":"3","status":"ok",'
        '"tool":"calculator","truth_domain":"real"}'
    )

    assert signature == hashlib.sha256(expected_document.encode()).hexdigest()
    assert signature == tool_loop_signature(second_request, second_event)


@pytest.mark.parametrize(
    "semantic_field",
    ["tool", "execution", "input", "status", "output", "error", "exit_code", "truth_domain"],
)
def test_tool_loop_signature_changes_for_every_semantic_field(semantic_field: str) -> None:
    base_request = ToolRequest(
        "calculator",
        ToolExecution.REAL_DETERMINISTIC,
        {"expression": "1 + 2"},
        "evt_source",
    )
    request = base_request
    result_overrides: dict[str, Any] = {}
    if semantic_field == "tool":
        request = ToolRequest(
            "python",
            ToolExecution.REAL_DETERMINISTIC,
            {"expression": "1 + 2"},
            "evt_source",
        )
    elif semantic_field == "execution":
        request = ToolRequest(
            "calculator",
            ToolExecution.VIRTUAL,
            {"expression": "1 + 2"},
            "evt_source",
        )
    elif semantic_field == "input":
        request = ToolRequest(
            "calculator",
            ToolExecution.REAL_DETERMINISTIC,
            {"expression": "2 + 2"},
            "evt_source",
        )
    else:
        result_overrides[semantic_field] = {
            "status": "error",
            "output": "4",
            "error": "failed",
            "exit_code": 1,
            "truth_domain": "sandbox",
        }[semantic_field]

    base_signature = tool_loop_signature(base_request, _result_event())
    changed_signature = tool_loop_signature(request, _result_event(**result_overrides))

    assert changed_signature != base_signature
