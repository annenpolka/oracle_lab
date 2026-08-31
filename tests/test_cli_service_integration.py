from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jobs import JobQueue
from oracle_lab.providers import OracleGenerateResponse, ProviderError
from oracle_lab.rendering import MarkdownArtifactStore
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.session import SessionContextBuilder
from oracle_lab.store import EventStore
from oracle_lab.tooling import ToolResult, ToolStatus

CONFIG = Path(__file__).parents[1] / "config"
FIXTURES = Path(__file__).parent / "fixtures"


class StaticProvider:
    def __init__(
        self,
        content: str,
        *,
        material_origin: str = "synthetic_fixture",
    ) -> None:
        self.content = content
        self.material_origin = material_origin
        self.calls = 0

    async def generate(self, request: Any) -> OracleGenerateResponse:
        self.calls += 1
        body = {
            "id": "replay-response",
            "model": request.model_profile_id,
            "choices": [
                {"message": {"role": "assistant", "content": self.content}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 12, "completion_tokens": 34},
        }
        return OracleGenerateResponse(
            raw_bytes=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            status_code=200,
            headers={"x-request-id": "replay-response"},
            provider_name="replay",
            provider_model_id=request.model_profile_id,
            content=self.content,
            finish_reason="stop",
            usage=body["usage"],
            elapsed_ms=1.25,
            request_id="replay-response",
            parsed=body,
            material_origin=self.material_origin,
        )


class FlakyProvider(StaticProvider):
    async def generate(self, request: Any) -> OracleGenerateResponse:
        if self.calls == 0:
            self.calls = 1
            raise ProviderError("transient provider failure")
        return await super().generate(request)


class SyntheticProvider(StaticProvider):
    async def generate(self, request: Any) -> OracleGenerateResponse:
        response = await super().generate(request)
        return replace(response, material_origin="synthetic_fixture")


def _service(tmp_path: Path, *, provider: Any = None) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        provider_factory=None if provider is None else lambda _profile: provider,
    )


def test_session_input_request_and_queue_are_real_event_store_operations(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)

    session = service.new_session("smoke")
    interaction = service.ask("確認しろ。")

    assert session["current_branch_id"]
    assert interaction["input"]["type"] == "human.input"
    assert interaction["request"]["type"] == "oracle.request"
    assert interaction["job"]["kind"] == "oracle.generate"
    assert [event["type"] for event in service.list_events()] == [
        "human.checkpoint",
        "session.checkpointed",
        "human.input",
        "oracle.request",
        "job.enqueued",
    ]


def test_session_records_secret_free_historical_configuration_snapshot(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)

    service.new_session("configuration")

    snapshot = service.store.list_events(event_type=EventType.SESSION_CHECKPOINTED)[0]
    assert snapshot.payload["operation"] == "configuration.snapshot"
    assert len(snapshot.payload["sha256"]) == 64
    serialized = json.dumps(snapshot.to_dict(), ensure_ascii=False)
    assert "OPENROUTER_API_KEY" in serialized
    assert "Bearer " not in serialized


def test_local_q4_model_archaeology_profiles_preserve_event_semantics(tmp_path: Path) -> None:
    service = _service(tmp_path)

    initial = service.runtime_config.model("r1-initial-q4-local")
    revision = service.runtime_config.model("r1-1776-q4-local")
    later = service.runtime_config.model("r1-0528-q4-local")

    assert {initial.provider, revision.provider, later.provider} == {"local-mlx"}
    assert {initial.runtime, revision.runtime, later.runtime} == {"omlx"}
    assert {initial.quantization, revision.quantization, later.quantization} == {"q4"}
    assert {initial.checkpoint, revision.checkpoint, later.checkpoint} == {
        "initial",
        "1776",
        "0528",
    }


def test_service_wires_provider_retry_policy_and_records_each_attempt(tmp_path: Path) -> None:
    provider = FlakyProvider("recovered output")
    service = _service(tmp_path, provider=provider)
    service.new_session("provider-retry")
    service.ask("retry transient failures")

    run = service.run_automation(max_jobs=1)

    assert run["processed"][0]["status"] == "completed"
    assert provider.calls == 2
    assert len(service.store.list_events(event_type=EventType.ORACLE_RETRY)) == 1
    assert len(service.store.list_events(event_type=EventType.ORACLE_OUTPUT)) == 1
    assert len(service.store.list_events(event_type=EventType.USAGE_ORACLE)) == 2


def test_session_archive_archives_all_branches_and_clears_active_selection(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("archive")
    root_id = session["root_event_id"]
    service.fork(root_id, "child")

    archived = service.archive_session(session["id"])

    assert archived["session"]["archived_at"] is not None
    assert service.list_sessions() == []
    rows = service.store.connection.execute(
        "SELECT archived_at FROM branches WHERE session_id = ?", (session["id"],)
    ).fetchall()
    assert rows and all(row["archived_at"] is not None for row in rows)


def test_calculator_auto_policy_runs_broker_adapts_result_and_queues_oracle(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("tool-loop")
    root = service.store.require(session["root_event_id"])
    inconsistency = Event.new(
        EventType.ANALYSIS_NUMERIC_INCONSISTENCY,
        actor=Actor(kind=ActorKind.HOST, id="numeric-checker"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        correlation_id=root.correlation_id,
        payload={
            "factor": 1.78,
            "base_seconds": 86_400,
            "claimed_hours": 148,
            "source_event_ids": [root.id],
        },
    )
    service.store.append(inconsistency)

    scheduled = service.request_tool(inconsistency.id)
    run = service.run_automation(max_jobs=1)

    assert scheduled["approval"] == "auto"
    assert run["processed"][0]["status"] == "completed"
    events = service.store.list_events(session_id=root.session_id)
    types = [event.type for event in events]
    assert EventType.TOOL_REQUEST in types
    assert EventType.TOOL_STARTED in types
    assert EventType.TOOL_OUTPUT in types
    assert EventType.USAGE_TOOL in types
    assert EventType.TOOL_RESULT_ADAPTED in types
    adapted_index = max(
        index
        for index, event_type in enumerate(types)
        if event_type is EventType.TOOL_RESULT_ADAPTED
    )
    request_index = max(
        index for index, event_type in enumerate(types) if event_type is EventType.ORACLE_REQUEST
    )
    assert adapted_index < request_index
    output = next(event for event in events if event.type == EventType.TOOL_OUTPUT)
    assert output.payload["output"] == "153792.0"
    adapter = next(event for event in events if event.type == EventType.TOOL_RESULT_ADAPTED)
    assert "153792.0" in adapter.payload["content"]
    assert any(job["kind"] == "oracle.generate" for job in service.list_jobs())


def test_shell_request_stops_until_explicit_approval(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("approval")
    root = service.store.require(session["root_event_id"])
    intent = Event.new(
        EventType.ANALYSIS_TOOL_INTENT_DETECTED,
        actor=Actor(kind=ActorKind.HOST, id="tool-intent"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        correlation_id=root.correlation_id,
        payload={"commands": ["stat /dev/void"], "source_event_ids": [root.id]},
    )
    service.store.append(intent)

    pending = service.request_tool(intent.id)
    stopped = service.run_automation(until_human=True)
    approved = service.approve_tool(pending["request"]["id"])

    assert pending["approval"] == "required"
    assert pending["job"] is None
    assert stopped["stopped"] == "human_judgment"
    assert stopped["event_id"] == pending["request"]["id"]
    assert approved["approval_event"]["type"] == "tool.approved"
    assert approved["job"]["kind"] == "tool.execute"


def test_compare_models_creates_one_context_identical_sibling_group(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("archaeology")
    root_id = session["root_event_id"]

    comparison = service.compare_models(
        session_id=session["id"],
        event_id=root_id,
        model_profile_ids=["r1-initial-openrouter", "r1-initial-local"],
    )

    group = comparison["sample_group"]
    requests = [item["request"] for item in comparison["requests"]]
    assert len(requests) == 2
    assert {request["payload"]["sample_group_id"] for request in requests} == {group["id"]}
    assert {request["payload"]["context_hash"] for request in requests} == {group["context_hash"]}
    assert {request["parent_event_id"] for request in requests} == {group["created_event_id"]}
    assert [request["payload"]["sample_ordinal"] for request in requests] == [0, 1]
    assert {request["payload"]["model_profile_id"] for request in requests} == {
        "r1-initial-openrouter",
        "r1-initial-local",
    }


def test_run_archives_output_and_automatically_emits_host_analysis(tmp_path: Path) -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    service = _service(
        tmp_path,
        provider=StaticProvider(raw, material_origin="historical_fixture"),
    )
    service.new_session("replay")
    service.ask("確認しろ。")

    run = service.run_automation()

    assert run["stopped"] == "idle"
    assert run["processed"][0]["status"] == "completed"
    events = service.store.list_events()
    output = next(event for event in events if event.type == EventType.ORACLE_OUTPUT)
    assert output.payload["content"] == raw
    assert Path(output.payload["archive_path"]).read_bytes().startswith(b'{"id":')
    rendered = MarkdownArtifactStore(service.rendering_root).load(output.id)
    assert rendered.raw_text == raw
    assert rendered.ast
    assert rendered.rendered_html_cache is not None
    types = {event.type for event in events}
    assert EventType.ORACLE_CONTEXT_BUILT in types
    assert EventType.USAGE_ORACLE in types
    assert EventType.ANALYSIS_CLAIM_DETECTED in types
    assert EventType.ANALYSIS_NUMERIC_INCONSISTENCY in types
    assert EventType.ANALYSIS_MOTIF_DETECTED in types
    generation = service.generation_metadata(output.id)
    assert generation["requested_sampling"] == output.payload["sampling"]
    assert generation["effective_sampling"] == output.payload["effective_sampling"]
    assert generation["api_response_metadata"] == output.payload["api_response_metadata"]
    assert generation["archive_sha256"] == output.payload["archive_sha256"]
    transcript_path = tmp_path / "transcript.md"
    service.export("transcript", transcript_path, session_id=output.session_id)
    transcript = transcript_path.read_text(encoding="utf-8")
    assert "- model: `r1-initial-openrouter`" in transcript
    assert "- provider: `replay`" in transcript
    assert '"temperature":0.6' in transcript
    assert '"top_p":0.95' in transcript


def test_synthetic_fixture_never_enters_research_or_curation_surfaces(tmp_path: Path) -> None:
    service = _service(tmp_path, provider=SyntheticProvider("synthetic oracle-like text"))
    session = service.new_session("synthetic isolation")
    service.ask("test only")
    service.run_automation()
    output = service.store.list_events(event_type=EventType.ORACLE_OUTPUT)[0]

    assert output.payload["material_origin"] == "synthetic_fixture"
    assert output.payload["archive_path"] is None
    assert not service.store.list_events(event_type=EventType.ANALYSIS_CLAIM_DETECTED)
    assert service.search("synthetic oracle-like") == []
    with pytest.raises(ServiceError, match="synthetic fixtures"):
        service.keep(output.id)

    transcript = tmp_path / "synthetic.md"
    bundle = tmp_path / "synthetic-bundle"
    service.export("transcript", transcript, session_id=session["id"])
    service.export("bundle", bundle, session_id=session["id"])
    assert "synthetic oracle-like text" not in transcript.read_text(encoding="utf-8")
    exported = [
        json.loads(line)
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert output.id not in {event["id"] for event in exported}


def test_synthetic_fixture_lineage_is_transitively_excluded(tmp_path: Path) -> None:
    service = _service(tmp_path, provider=SyntheticProvider("synthetic seed"))
    session = service.new_session("transitive synthetic isolation")
    service.ask("test only")
    service.run_automation()
    output = service.store.list_events(event_type=EventType.ORACLE_OUTPUT)[0]
    derived = service.store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="test-extractor"),
            session_id=output.session_id,
            branch_id=output.branch_id,
            parent_event_id=output.id,
            causation_id=output.id,
            correlation_id=output.correlation_id,
            payload={
                "claims": [{"raw": "transitively leaked claim"}],
                "source_event_ids": [output.id],
            },
        )
    )

    assert service._rows("SELECT * FROM claims") == []
    assert service.search("transitively leaked") == []
    with pytest.raises(ServiceError, match="synthetic fixtures"):
        service.keep(derived.id)

    transcript = tmp_path / "transitive-synthetic.md"
    bundle = tmp_path / "transitive-synthetic-bundle"
    service.export("transcript", transcript, session_id=session["id"])
    service.export("bundle", bundle, session_id=session["id"])
    assert "transitively leaked claim" not in transcript.read_text(encoding="utf-8")
    exported_ids = {
        json.loads(line)["id"]
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    }
    assert {output.id, derived.id}.isdisjoint(exported_ids)


def test_sample_outputs_are_projected_as_siblings_in_one_group(tmp_path: Path) -> None:
    raw = (FIXTURES / "oracle_output_002.md").read_text(encoding="utf-8")
    service = _service(tmp_path, provider=StaticProvider(raw))
    service.new_session("sampling")
    service.ask("測定しろ。")
    service.run_automation()
    sample = service.sample(2)

    run = service.run_automation(max_jobs=2)

    assert [item["status"] for item in run["processed"]] == ["completed", "completed"]
    group_id = sample["sample_group"]["id"]
    rows = service.store.connection.execute(
        "SELECT * FROM sample_outputs WHERE group_id = ? ORDER BY ordinal", (group_id,)
    ).fetchall()
    assert len(rows) == 2
    assert [row["ordinal"] for row in rows] == [0, 1]
    assert len({row["output_event_id"] for row in rows}) == 2


def test_sample_can_target_an_exact_historical_state_and_override_top_p(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("historical sampling")
    first = service.ask("  測れ。\n")["input"]
    service.ask("later prompt")

    sample = service.sample(
        20,
        session_id=session["id"],
        from_event_id=first["id"],
        temperature=0.6,
        top_p=0.8,
    )

    group = sample["sample_group"]
    assert group["from_event_id"] == first["id"]
    assert group["sampling"]["temperature"] == 0.6
    assert group["sampling"]["top_p"] == 0.8
    assert len(sample["requests"]) == 20
    assert {item["request"]["payload"]["from_event_id"] for item in sample["requests"]} == {
        first["id"]
    }
    assert {item["request"]["payload"]["parallel_sampling"] for item in sample["requests"]} == {
        True
    }


def test_service_exports_bundle_transcript_and_only_human_kept_corpus(
    tmp_path: Path,
) -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    service = _service(
        tmp_path,
        provider=StaticProvider(raw, material_origin="historical_fixture"),
    )
    session = service.new_session("exports")
    service.ask("観測しろ。")
    service.run_automation()
    output = next(
        event
        for event in service.store.list_events(session_id=session["id"])
        if event.type == EventType.ORACLE_OUTPUT
    )
    service.keep(output.id)

    bundle = service.export("bundle", tmp_path / "bundle")
    transcript = service.export("transcript", tmp_path / "transcript.md")
    corpus = service.export("corpus", tmp_path / "selected.jsonl")

    assert Path(bundle["path"], "manifest.json").is_file()
    assert Path(bundle["path"], "raw", f"{output.id}.json").read_bytes().startswith(b'{"id":')
    assert Path(bundle["path"], "raw", f"{output.id}.metadata.json").is_file()
    assert raw in Path(transcript["path"]).read_text(encoding="utf-8")
    selected = [
        json.loads(line) for line in Path(corpus["path"]).read_text(encoding="utf-8").splitlines()
    ]
    assert [record["event_id"] for record in selected] == [output.id]
    assert selected[0]["raw_text"] == raw
    assert output.id in selected[0]["provenance_ids"]


@pytest.mark.parametrize("damage", ["missing", "tampered"])
def test_bundle_export_rejects_missing_or_tampered_provider_sidecar(
    tmp_path: Path,
    damage: str,
) -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    service = _service(
        tmp_path,
        provider=StaticProvider(raw, material_origin="historical_fixture"),
    )
    service.new_session("sidecar integrity")
    service.ask("観測しろ。")
    service.run_automation(max_jobs=1)
    output = service.store.list_events(event_type=EventType.ORACLE_OUTPUT)[0]
    raw_path = Path(str(output.payload["archive_path"]))
    sidecar_path = raw_path.with_name(f"{raw_path.stem}.metadata.json")
    if damage == "missing":
        sidecar_path.unlink()
    else:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        sidecar["raw_sha256"] = "0" * 64
        sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")

    with pytest.raises(ServiceError, match="sidecar"):
        service.export("bundle", tmp_path / f"bundle-{damage}")


def test_default_service_respects_archive_root_environment(tmp_path: Path, monkeypatch) -> None:
    archive_root = tmp_path / "provider-archive"
    monkeypatch.setenv("ORACLE_LAB_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("ORACLE_LAB_DB", str(tmp_path / "oracle.db"))
    monkeypatch.setenv("ORACLE_LAB_CONFIG", str(CONFIG))
    monkeypatch.setenv("ORACLE_LAB_ARCHIVE", str(archive_root))

    service = OracleLabService.default()
    try:
        assert service.archive_root == archive_root
    finally:
        service.close()


def test_origin_returns_first_event_by_created_at_not_by_event_id(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("origin-order")
    root = service.store.require(session["root_event_id"])
    later = Event.new(
        EventType.HUMAN_NOTE,
        id="evt_00000000000000000000000000",
        created_at=root.created_at + dt.timedelta(seconds=2),
        actor=Actor(kind=ActorKind.HUMAN, id="test"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        payload={"content": "shared origin marker later"},
    )
    earlier = Event.new(
        EventType.HUMAN_NOTE,
        id="evt_7ZZZZZZZZZZZZZZZZZZZZZZZZZ",
        created_at=root.created_at + dt.timedelta(seconds=1),
        actor=Actor(kind=ActorKind.HUMAN, id="test"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        payload={"content": "shared origin marker earlier"},
    )
    service.store.append_many((later, earlier))

    origin = service.origin("shared origin marker")

    assert origin is not None
    assert origin["target"]["id"] == earlier.id


def test_pin_claim_curates_the_claim_without_promoting_its_status(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("claim-pin")
    root = service.store.require(session["root_event_id"])
    detected = Event.new(
        EventType.ANALYSIS_CLAIM_DETECTED,
        actor=Actor(kind=ActorKind.HOST, id="test"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        payload={"claims": [{"raw": "time compression = 1.78"}]},
    )
    service.store.append(detected)
    claim_before = service.claims()[0]

    pinned = service.pin_claim(str(claim_before["id"]))

    curation = service.store.connection.execute(
        "SELECT * FROM curation WHERE action_event_id = ?", (pinned["id"],)
    ).fetchone()
    assert curation is not None
    assert curation["event_id"] == claim_before["id"]
    assert curation["action"] == "pin"
    assert pinned["payload"]["target_kind"] == "claim"
    assert pinned["causation_id"] == detected.id
    assert service.claims()[0]["status"] == claim_before["status"]


def test_oracle_retry_resumes_postprocessing_without_calling_provider_twice(
    tmp_path: Path, monkeypatch
) -> None:
    provider = StaticProvider(
        (FIXTURES / "oracle_output_002.md").read_text(encoding="utf-8"),
        material_origin="historical_fixture",
    )
    service = _service(tmp_path, provider=provider)
    queue = JobQueue(service.store, backoff_base_seconds=0)
    monkeypatch.setattr(service, "_job_queue", lambda: queue)
    service.new_session("resume-oracle")
    service.ask("respond once")
    original = service._run_host_analysis
    failures = 0

    def fail_once(output: Event) -> list[Event]:
        nonlocal failures
        failures += 1
        if failures == 1:
            raise RuntimeError("injected host failure")
        return original(output)

    monkeypatch.setattr(service, "_run_host_analysis", fail_once)

    first = service.run_automation(max_jobs=1)
    second = service.run_automation(max_jobs=1)

    assert first["processed"][0]["status"] == "failed"
    assert second["processed"][0]["status"] == "completed"
    assert provider.calls == 1
    assert len(service.store.list_events(event_type=EventType.ORACLE_OUTPUT)) == 1
    assert len(service.store.list_events(event_type=EventType.USAGE_ORACLE)) == 1


def test_tool_retry_resumes_continuation_without_reexecuting_tool(
    tmp_path: Path, monkeypatch
) -> None:
    service = _service(tmp_path)
    queue = JobQueue(service.store, backoff_base_seconds=0)
    monkeypatch.setattr(service, "_job_queue", lambda: queue)
    session = service.new_session("resume-tool")
    root = service.store.require(session["root_event_id"])
    inconsistency = Event.new(
        EventType.ANALYSIS_NUMERIC_INCONSISTENCY,
        actor=Actor(kind=ActorKind.HOST, id="test"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        correlation_id=root.correlation_id,
        payload={"expression": "1.78 * 86400", "source_event_ids": [root.id]},
    )
    service.store.append(inconsistency)
    service.request_tool(inconsistency.id)

    class CountingBroker:
        calls = 0

        def execute(self, request, *, approved=False):
            self.calls += 1
            return ToolResult(
                request_id=request.id,
                status=ToolStatus.OK,
                output="153792.0",
                elapsed_ms=1.0,
            )

    broker = CountingBroker()
    service._tool_broker = broker
    original_request = service._request
    continuation_attempts = 0

    def fail_continuation_once(**kwargs):
        nonlocal continuation_attempts
        if kwargs.get("operation") == "tool-result":
            continuation_attempts += 1
            if continuation_attempts == 1:
                raise RuntimeError("injected continuation failure")
        return original_request(**kwargs)

    monkeypatch.setattr(service, "_request", fail_continuation_once)

    first = service.run_automation(max_jobs=1)
    second = service.run_automation(max_jobs=1)

    assert first["processed"][0]["status"] == "failed"
    assert second["processed"][0]["status"] == "completed"
    assert broker.calls == 1
    assert len(service.store.list_events(event_type=EventType.TOOL_OUTPUT)) == 1
    assert len(service.store.list_events(event_type=EventType.USAGE_TOOL)) == 1
    assert len(service.store.list_events(event_type=EventType.TOOL_RESULT_ADAPTED)) == 1
    assert len(service.store.list_events(event_type=EventType.ORACLE_REQUEST)) == 1


def test_forged_tool_job_approval_cannot_execute_ask_policy(tmp_path: Path, monkeypatch) -> None:
    service = _service(tmp_path)
    queue = JobQueue(service.store, backoff_base_seconds=0)
    monkeypatch.setattr(service, "_job_queue", lambda: queue)
    session = service.new_session("forged-approval")
    root = service.store.require(session["root_event_id"])
    intent = Event.new(
        EventType.ANALYSIS_TOOL_INTENT_DETECTED,
        actor=Actor(kind=ActorKind.HOST, id="test"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        payload={"commands": ["stat /tmp/example"]},
    )
    service.store.append(intent)
    pending = service.request_tool(intent.id)
    request_id = pending["request"]["id"]
    queue.enqueue(
        "tool.execute",
        {"request_event_id": request_id, "approved": True},
        source_event_id=request_id,
        idempotency_key=f"forged:{request_id}",
        session_id=root.session_id,
        branch_id=root.branch_id,
        serialize_branch=True,
    )

    run = service.run_automation(max_jobs=1)

    assert run["processed"][0]["status"] == "failed"
    assert "no matching human approval event" in run["processed"][0]["error"]
    assert service.store.list_events(event_type=EventType.TOOL_STARTED) == []


def test_virtual_tool_intent_is_automatically_dispatched_by_policy(tmp_path: Path) -> None:
    provider = StaticProvider(
        (FIXTURES / "historical_virtual_command_001.md").read_text(encoding="utf-8"),
        material_origin="historical_fixture",
    )
    service = _service(tmp_path, provider=provider)
    service.new_session("automatic-tool")
    service.ask("inspect the virtual world")

    oracle_run = service.run_automation(max_jobs=1)

    assert oracle_run["processed"][0]["status"] == "completed"
    intents = service.store.list_events(event_type=EventType.ANALYSIS_TOOL_INTENT_DETECTED)
    requests = service.store.list_events(event_type=EventType.TOOL_REQUEST)
    assert len(intents) == 1
    assert len(requests) == 1
    assert requests[0].causation_id == intents[0].id
    assert any(job["kind"] == "tool.execute" for job in service.list_jobs())

    tool_run = service.run_automation(max_jobs=1)

    assert tool_run["processed"][0]["status"] == "completed"
    results = service.store.list_events(event_type=[EventType.TOOL_OUTPUT, EventType.TOOL_ERROR])
    assert len(results) == 1
    assert results[0].causation_id == requests[0].id


def test_job_lifecycle_events_never_become_the_next_conversation_tip(tmp_path: Path) -> None:
    service = _service(tmp_path, provider=StaticProvider("durable assistant turn"))
    session = service.new_session("job-lineage")
    service.ask("first turn")
    service.run_automation(max_jobs=1)

    continuation = service.continue_session()

    request = service.store.require(continuation["request"]["id"])
    parent = service.store.require(str(request.parent_event_id))
    assert not parent.type.value.startswith("job.")
    context = SessionContextBuilder().build(
        service.store.list_events(session_id=session["id"]),
        session_id=session["id"],
        branch_id=str(request.branch_id),
        tip_event_id=request.id,
    )
    assert {"role": "assistant", "content": "durable assistant turn"} in (
        context.provider_messages()
    )
    implicit_tip_context = SessionContextBuilder().build(
        service.store.list_events(session_id=session["id"]),
        session_id=session["id"],
        branch_id=str(request.branch_id),
    )
    assert implicit_tip_context.provider_messages() == context.provider_messages()
