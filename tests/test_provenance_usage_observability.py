from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

import oracle_lab.cli as cli
from oracle_lab.branching import BranchService
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.observability import (
    ObservabilityService,
    bind_event,
    current_trace_context,
)
from oracle_lab.projections import VirtualStateService
from oracle_lab.provenance import ProvenanceRelation, ProvenanceService
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.usage import UsageKind, UsageService
from oracle_lab.virtual import SourceEvidence, VirtualNodeKind, VirtualWorldRuntime
from tests.support import historical_oracle_fixture

FIXTURES = Path(__file__).parent / "fixtures"


def _correlated_claim(store: EventStore) -> tuple[Event, str]:
    session = BranchService(store).create_session()
    prompt = store.append(
        Event(
            type="human.input",
            actor=Actor(kind="human", id="curator"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=session.root_event_id,
            causation_id=session.root_event_id,
            correlation_id="corr-cycle",
            payload={"content": "state the phase"},
        )
    )
    output = store.append(
        historical_oracle_fixture(
            (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8"),
            source_path=FIXTURES / "oracle_output_001.md",
            actor_id="historical-r1",
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=prompt.id,
            causation_id=prompt.id,
            correlation_id="corr-cycle",
        )
    )
    analysis = store.append(
        Event(
            type="analysis.claim_detected",
            actor=Actor(kind="host", id="extractor"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=output.id,
            causation_id=output.id,
            correlation_id="corr-cycle",
            payload={
                "claims": [{"raw": "pain phase = 34.7°"}],
                "source_event_ids": [output.id],
            },
        )
    )
    claim_id = store.connection.execute(
        "SELECT id FROM claims WHERE source_event_id = ?", (output.id,)
    ).fetchone()[0]
    assert analysis.causation_id == output.id
    return output, claim_id


def test_claim_provenance_resolves_model_human_and_explicit_links() -> None:
    store = EventStore()
    output, claim_id = _correlated_claim(store)
    provenance = ProvenanceService(store)

    edges = provenance.edges_for("claim", claim_id)
    assert {edge.source_event_id for edge in edges} == {output.id}
    assert "model" in provenance.actor_origins("claim", claim_id)
    assert "human" in provenance.actor_origins("claim", claim_id)

    linked = provenance.link(
        "concept",
        "pain-phase",
        output.id,
        relation=ProvenanceRelation.INTRODUCED,
    )
    assert linked.source_event_id == output.id
    assert provenance.validate() == []


def test_default_single_claim_and_entity_ids_have_provenance_after_rebuild() -> None:
    store = EventStore()
    source = store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            payload={"content": "TIME_DILATION_FACTOR=1.78 at /dev/void"},
        )
    )
    claim_event = store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="claim-extractor"),
            parent_event_id=source.id,
            causation_id=source.id,
            payload={
                "raw_text": "TIME_DILATION_FACTOR=1.78",
                "source_event_id": source.id,
            },
        )
    )
    entity_event = store.append(
        Event.new(
            EventType.ANALYSIS_ENTITY_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="entity-extractor"),
            parent_event_id=source.id,
            causation_id=source.id,
            payload={
                "canonical_name": "/dev/void",
                "entity_type": "path",
                "source_event_id": source.id,
            },
        )
    )
    provenance = ProvenanceService(store)
    claim_id = f"clm_{claim_event.id.removeprefix('evt_')}_000"
    entity_id = f"ent_{entity_event.id.removeprefix('evt_')}"

    assert {edge.source_event_id for edge in provenance.edges_for("claim", claim_id)} == {source.id}
    assert {edge.source_event_id for edge in provenance.edges_for("entity", entity_id)} == {
        source.id
    }

    store.rebuild_projections()
    assert provenance.edges_for("claim", claim_id)
    assert provenance.edges_for("entity", entity_id)


def test_dev_void_research_trace_distinguishes_all_actors_and_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = EventStore()
    human = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id="ses_void",
            branch_id="br_main",
            payload={"content": "Investigate /dev/void."},
        )
    )
    oracle = store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            session_id=human.session_id,
            branch_id=human.branch_id,
            parent_event_id=human.id,
            causation_id=human.id,
            payload={"content": "/dev/void is an observer interface."},
        )
    )
    analysis = store.append(
        Event.new(
            EventType.ANALYSIS_ENTITY_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="entity-extractor"),
            session_id=human.session_id,
            branch_id=human.branch_id,
            parent_event_id=oracle.id,
            causation_id=oracle.id,
            payload={
                "entity_id": "ent_dev_void",
                "canonical_name": "/dev/void",
                "source_event_ids": [oracle.id],
            },
        )
    )
    tool = store.append(
        Event.new(
            EventType.TOOL_OUTPUT,
            actor=Actor(kind=ActorKind.TOOL, id="virtual-inspector"),
            session_id=human.session_id,
            branch_id=human.branch_id,
            parent_event_id=analysis.id,
            causation_id=analysis.id,
            payload={
                "output": "character device, major/minor unresolved",
                "source_event_ids": [analysis.id],
            },
        )
    )
    virtual = VirtualStateService(store)
    runtime = VirtualWorldRuntime(
        mutation_sink=virtual.mutation_sink(
            session_id="ses_void",
            branch_id="br_main",
            actor=Actor(kind=ActorKind.TOOL, id="virtual-runtime"),
        )
    )
    runtime.fs.create(
        "/dev/void",
        kind=VirtualNodeKind.CHARACTER_DEVICE,
        content="observer interface",
        evidence=SourceEvidence((oracle.id, analysis.id, tool.id), "synthesized"),
    )
    virtual_events = store.list_events(event_type=EventType.VIRTUAL_FILE_CREATED)
    implicit_parent, created = virtual_events[-2:]
    service = OracleLabService(store, home=tmp_path / "home")

    artifact_trace = service.provenance_trace("virtual_file", "/dev/void")
    event_trace = service.trace_event(created.id)
    origin_trace = service.origin(created.id)

    relevant_sources = {human.id, oracle.id, analysis.id, tool.id}
    assert set(artifact_trace["direct_source_event_ids"]) == {
        oracle.id,
        analysis.id,
        tool.id,
    }
    assert set(artifact_trace["source_event_ids"]) == relevant_sources
    assert set(artifact_trace["actor_origins"]) == {"human", "model", "host", "tool"}
    assert artifact_trace["creator_event_ids"] == [created.id]
    assert [origin["depth"] for origin in artifact_trace["origins"]] == sorted(
        origin["depth"] for origin in artifact_trace["origins"]
    )
    assert set(event_trace["source_event_ids"]) == relevant_sources | {implicit_parent.id}
    assert set(event_trace["actor_origins"]) == {"human", "model", "host", "tool"}
    assert origin_trace == event_trace
    assert event_trace["target"]["id"] == created.id
    assert event_trace["target"]["event"]["type"] == "virtual_file.created"
    assert event_trace["source_event_ids"] != [created.id]

    monkeypatch.setattr(cli, "_service_factory", lambda: service)
    result = CliRunner().invoke(cli.app, ["provenance", "trace", "virtual_file", "/dev/void"])
    assert result.exit_code == 0, result.output
    cli_trace = json.loads(result.output)
    assert set(cli_trace["source_event_ids"]) == relevant_sources
    assert set(cli_trace["actor_origins"]) == {"human", "model", "host", "tool"}


def test_usage_aggregates_by_session_branch_model_and_never_mutates_requests() -> None:
    store = EventStore()
    output, _ = _correlated_claim(store)
    usage = UsageService(store)
    before = output.to_dict()
    usage.record(
        UsageKind.ORACLE,
        request_event_id=output.id,
        provider_id="openrouter",
        model_id="r1",
        prompt_tokens=100,
        completion_tokens=20,
        reasoning_tokens=5,
        provider_cost="0.0123",
        latency_ms=250,
        ttft_ms=40,
    )
    usage.record(
        UsageKind.HOST,
        request_event_id=output.id,
        model_id="host",
        prompt_tokens=10,
        completion_tokens=3,
        provider_cost=Decimal("0.001"),
        latency_ms=50,
    )

    totals = usage.totals(session_id=output.session_id)

    assert totals.prompt_tokens == 110
    assert totals.completion_tokens == 23
    assert totals.reasoning_tokens == 5
    assert totals.provider_cost == Decimal("0.0133")
    assert totals.request_count == 2
    assert usage.totals(model_id="r1").request_count == 1
    assert store.require(output.id).to_dict() == before


def test_observability_structured_log_metrics_and_correlation_trace(caplog) -> None:
    store = EventStore()
    output, _ = _correlated_claim(store)
    UsageService(store).record(
        UsageKind.ORACLE,
        request_event_id=output.id,
        prompt_tokens=2,
        completion_tokens=1,
        latency_ms=20,
    )
    service = ObservabilityService(store)

    with caplog.at_level(logging.INFO, logger="oracle_lab"):
        service.log_event(output)
    record = caplog.records[-1]
    assert record.oracle_lab["event_id"] == output.id
    assert record.oracle_lab["correlation_id"] == "corr-cycle"

    metrics = service.metrics()
    assert metrics.prompt_tokens == 2
    assert metrics.completion_tokens == 1
    assert metrics.oracle_latency_ms == 20
    assert metrics.branch_count == 1
    assert [event.correlation_id for event in service.correlation_trace("corr-cycle")] == [
        "corr-cycle",
        "corr-cycle",
        "corr-cycle",
        "corr-cycle",
    ]

    with bind_event(output):
        context = current_trace_context()
        assert context is not None
        assert context.event_id == output.id
        assert context.correlation_id == "corr-cycle"
    assert current_trace_context() is None
