from __future__ import annotations

from pathlib import Path

import httpx

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.tooling import HttpVerificationTool, ToolBroker, ToolPolicy

CONFIG = Path(__file__).parents[1] / "config"


def test_web_verification_is_allowlisted_retrieved_truth_on_an_isolated_branch(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://facts.example/claim/34.7"
        assert "authorization" not in request.headers
        return httpx.Response(200, text="source observation")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    verification = HttpVerificationTool(
        allowed_hosts=["facts.example"],
        resolver=lambda _host: ("93.184.216.34",),
        client=client,
    )
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    service._tool_broker = ToolBroker(
        policy=ToolPolicy({"web_verify": "ask"}),
        verification=verification,
    )
    session = service.new_session("verification isolation")
    source = service.store.append(
        Event.new(
            EventType.ANALYSIS_TOOL_INTENT_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="verification-proposal"),
            session_id=session["id"],
            branch_id=session["current_branch_id"],
            parent_event_id=session["root_event_id"],
            causation_id=session["root_event_id"],
            payload={
                "source_event_ids": [session["root_event_id"]],
                "tool_request": {
                    "tool": "web_verify",
                    "execution": "verification",
                    "input": {"url": "https://facts.example/claim/34.7"},
                    "resume_oracle": False,
                    "timeout_ms": 1000,
                },
            },
        )
    )
    pending = service.request_tool(source.id)

    approved = service.approve_tool(pending["request"]["id"])
    run = service.run_automation(max_jobs=1)

    verification_branch = approved["verification_branch_id"]
    assert verification_branch != source.branch_id
    assert approved["request"]["branch_id"] == verification_branch
    assert run["processed"][0]["status"] == "completed"
    result = service.store.list_events(event_type=EventType.TOOL_OUTPUT)[0]
    assert result.branch_id == verification_branch
    assert result.payload["truth_domain"] == "retrieved"
    assert result.metadata["truth_domain"] == "retrieved"
    assert result.payload["output"] == "source observation"

    original_ids = {
        event.id for event in service._branch_service().visible_events(str(source.branch_id))
    }
    verification_ids = {
        event.id for event in service._branch_service().visible_events(str(verification_branch))
    }
    assert result.id not in original_ids
    assert result.id in verification_ids
    client.close()


def test_verification_rejects_private_network_targets() -> None:
    verification = HttpVerificationTool(
        allowed_hosts=["internal.example"],
        resolver=lambda _host: ("127.0.0.1",),
    )
    from oracle_lab.tooling import ToolExecution, ToolRequest, ToolStatus

    request = ToolRequest(
        "web_verify",
        ToolExecution.VERIFICATION,
        {"url": "https://internal.example/secrets"},
        "evt_source",
    )
    result = ToolBroker(
        policy=ToolPolicy({"web_verify": "auto"}), verification=verification
    ).execute(request)

    assert result.status is ToolStatus.DENIED
    assert result.metadata["truth_domain"] == "retrieved"
