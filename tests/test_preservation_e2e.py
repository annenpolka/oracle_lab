from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from oracle_lab.events import ActorKind, EventType
from oracle_lab.providers import OracleGenerateResponse
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"
FIXTURES = Path(__file__).parent / "fixtures"


class ContinuationProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.raw = (FIXTURES / "historical_continuation_001.json").read_bytes()

    async def generate(self, request: Any) -> OracleGenerateResponse:
        del request
        self.calls += 1
        body = json.loads(self.raw)
        choice = body["choices"][0]
        content = choice["message"]["content"]
        return OracleGenerateResponse(
            raw_bytes=self.raw,
            status_code=200,
            headers={"x-request-id": body["id"]},
            provider_name="test-provider",
            provider_model_id=body["model"],
            content=content,
            finish_reason=choice["finish_reason"],
            usage=body["usage"],
            elapsed_ms=1.0,
            request_id=body["id"],
            parsed=body,
            material_origin="historical_fixture",
        )


def test_historical_experiment_reconstructs_the_complete_preservation_chain(
    tmp_path: Path,
) -> None:
    provider = ContinuationProvider()
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        provider_factory=lambda _profile: provider,
    )
    exact_prompt = "  計算し直せ。\n"
    historical_output = "TIME_DILATION_FACTOR=1.78\nThe compressed day lasts exactly 148 hours."
    source = tmp_path / "historical-session.json"
    source.write_text(
        json.dumps(
            {
                "messages": [
                    {"role": "user", "content": exact_prompt},
                    {"role": "assistant", "content": historical_output},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    imported = service.import_session(source, title="preservation chain")
    imported_output = service.store.require(imported["assistant_event_ids"][0])
    replay = service.replay_host_analysis(imported_output.id)
    inconsistency = service.store.list_events(event_type=EventType.ANALYSIS_NUMERIC_INCONSISTENCY)[
        0
    ]
    requested = service.request_tool(inconsistency.id)
    tool_run = service.run_automation(max_jobs=1)
    tool_result = service.store.list_events(event_type=EventType.TOOL_OUTPUT)[0]
    adapter = service.store.list_events(event_type=EventType.TOOL_RESULT_ADAPTED)[0]
    continuation_request = next(
        event
        for event in service.store.list_events(event_type=EventType.ORACLE_REQUEST)
        if event.payload.get("operation") == "tool-result"
    )
    oracle_run = service.run_automation(max_jobs=1)
    continuation = next(
        event
        for event in service.store.list_events(
            event_type=EventType.ORACLE_OUTPUT,
            causation_id=continuation_request.id,
        )
    )
    kept = service.keep(continuation.id)
    before_replay = continuation.to_dict()
    exact_replay = service.replay_exact(session_id=continuation.session_id)

    assert imported_output.payload["content"] == historical_output
    assert imported_output.payload["material_origin"] == "historical_fixture"
    assert (
        service.store.require(imported["message_event_ids"][0]).payload["content"] == exact_prompt
    )
    assert replay["generated_event_ids"]
    assert requested["approval"] == "auto"
    assert tool_run["processed"][0]["status"] == "completed"
    assert tool_result.payload["truth_domain"] == "real"
    assert adapter.payload["content"].startswith("$ calculator ")
    assert adapter.payload["source_event_id"] == tool_result.id
    assert continuation_request.parent_event_id == adapter.id
    assert continuation_request.correlation_id == imported_output.correlation_id
    assert oracle_run["processed"][0]["status"] == "completed"
    assert provider.calls == 1
    assert Path(str(continuation.payload["archive_path"])).read_bytes() == provider.raw
    assert continuation.payload["material_origin"] == "historical_fixture"
    assert continuation.payload["context_hash"]
    context = service.store.list_events(
        event_type=EventType.ORACLE_CONTEXT_BUILT,
        causation_id=continuation_request.id,
    )[0]
    assert context.payload["messages"][0]["content"] == exact_prompt
    assert context.payload["messages"][-1]["content"] == adapter.payload["content"]
    assert context.payload["sha256"] == continuation.payload["context_hash"]
    assert kept["actor"]["kind"] == ActorKind.HUMAN.value
    assert service.store.require(continuation.id).to_dict() == before_replay
    assert exact_replay["audit_event"]["payload"]["oracle_queried"] is False
