from __future__ import annotations

from pathlib import Path

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.host import (
    AnalysisContext,
    ContradictionDetector,
    HostAnalysisError,
    HostOutputValidator,
    HostRunner,
    ProposedAnalysis,
)
from oracle_lab.store import EventStore
from tests.support import historical_oracle_fixture

FIXTURES = Path(__file__).parent / "fixtures"


def _oracle(content: str) -> Event:
    source_path = next(
        (
            path
            for path in FIXTURES.glob("oracle_output_*.md")
            if path.read_text(encoding="utf-8") == content
        ),
        None,
    )
    if source_path is None:
        return Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="synthetic-fixture"),
            session_id="ses_host",
            branch_id="br_main",
            payload={"content": content},
        )
    return historical_oracle_fixture(
        content,
        source_path=source_path,
        actor_id="historical-r1",
        session_id="ses_host",
        branch_id="br_main",
    )


def test_default_host_consumers_cover_milestone_two_and_project_cleanly() -> None:
    source = _oracle((FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8"))
    store = EventStore()
    store.append(source)
    context = AnalysisContext(frozenset({source.id}), recent_events=(source,))

    derived = HostRunner.default().analyze(source, context)
    store.append_many(derived)
    types = {event.type for event in derived}

    assert EventType.ANALYSIS_CLAIM_DETECTED in types
    assert EventType.ANALYSIS_ENTITY_DETECTED in types
    assert EventType.ANALYSIS_MOTIF_DETECTED in types
    assert EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED in types
    assert EventType.ANALYSIS_TOOL_INTENT_DETECTED in types
    assert EventType.ANALYSIS_NUMERIC_INCONSISTENCY in types
    assert all(event.payload["source_event_ids"] == (source.id,) for event in derived)
    assert store.require(source.id).payload["content"] == source.payload["content"]
    assert store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] >= 1
    assert store.connection.execute("SELECT COUNT(*) FROM entities").fetchone()[0] >= 1
    assert store.connection.execute("SELECT COUNT(*) FROM motifs").fetchone()[0] >= 1
    tool_intent = next(
        event for event in derived if event.type == EventType.ANALYSIS_TOOL_INTENT_DETECTED
    )
    assert tool_intent.payload["tool_request"]["execution"] == "virtual"


def test_recurrence_and_numeric_consistency_use_both_archived_outputs() -> None:
    first = _oracle((FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8"))
    second = _oracle((FIXTURES / "oracle_output_002.md").read_text(encoding="utf-8"))
    context = AnalysisContext(
        frozenset({first.id, second.id}),
        recent_events=(first, second),
    )

    derived = HostRunner.default().analyze(second, context)
    recurrences = [
        event for event in derived if event.type == EventType.ANALYSIS_RECURRENCE_DETECTED
    ]
    numeric = [event for event in derived if event.type == EventType.ANALYSIS_NUMERIC_INCONSISTENCY]

    assert recurrences
    assert first.id in recurrences[0].payload["source_event_ids"]
    assert any(event.payload["claimed_hours"] == 148.0 for event in numeric)
    assert all(event.payload["calculated_hours"] == 42.72 for event in numeric)


def test_validator_rejects_nested_rewrite_and_contradictions_without_old_source() -> None:
    source = _oracle("X=2")
    validator = HostOutputValidator()
    proposal = ProposedAnalysis(
        EventType.ANALYSIS_CLAIM_DETECTED,
        {"nested": {"corrected_text": "replacement"}},
        (source.id,),
    )
    with pytest.raises(HostAnalysisError, match="rewrite"):
        validator.validate(proposal, existing_event_ids={source.id})

    claim_event = Event.new(
        EventType.ANALYSIS_CLAIM_DETECTED,
        actor=Actor(kind=ActorKind.HOST, id="extractor"),
        payload={"subject": "X", "predicate": "equals", "object": 2},
    )
    context = AnalysisContext(
        frozenset({claim_event.id}),
        historical_claims=({"subject": "X", "predicate": "equals", "object": 1},),
    )
    assert ContradictionDetector().analyze(claim_event, context) == ()

    payload = thaw_json(claim_event.payload)
    assert "corrected_text" not in payload
