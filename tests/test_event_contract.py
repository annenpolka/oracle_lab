from __future__ import annotations

import datetime as dt

import pytest
from pydantic import ValidationError

from oracle_lab.events import Actor, ActorKind, Event, EventType, known_event_types
from oracle_lab.ids import new_id


def test_event_is_frozen_deeply_and_round_trips_wire_envelope() -> None:
    event = Event(
        type=EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="deepseek-r1"),
        created_at=dt.datetime(2026, 8, 30, 15, 52, tzinfo=dt.timezone(dt.timedelta(hours=9))),
        session_id="ses_a",
        branch_id="br_main",
        payload={"content": "raw\r\ntext  \n", "nested": [{"value": 1}]},
        metadata={"schema_version": 3, "provider": {"id": "openrouter"}},
    )

    assert Event.from_dict(event.to_dict()) == event
    assert event.schema_version == 3
    with pytest.raises(TypeError):
        event.payload["content"] = "rewritten"
    with pytest.raises(TypeError):
        event.payload["nested"][0]["value"] = 2
    with pytest.raises(ValidationError):
        event.type = EventType.HUMAN_INPUT


def test_taxonomy_rejects_unknown_events_and_includes_runtime_extensions() -> None:
    actor = Actor(kind="system")
    with pytest.raises(ValidationError):
        Event(type="analysis.plausible_but_unknown", actor=actor)

    expected = {
        "analysis.new_mechanism_detected",
        "oracle.context_message",
        "tool.result_adapted",
        "analysis.promoted_to_oracle",
        "job.enqueued",
        "usage.oracle",
        "virtual_clock.created",
        "virtual_clock.contradiction_detected",
    }
    assert expected <= known_event_types()


def test_event_requires_timezone_and_positive_metadata_schema_version() -> None:
    actor = Actor(kind="human")
    with pytest.raises(ValidationError):
        Event(type="human.input", actor=actor, created_at=dt.datetime(2026, 1, 1))
    with pytest.raises(ValidationError):
        Event(type="human.input", actor=actor, metadata={"schema_version": 0})
    with pytest.raises(ValidationError):
        Event(type="human.input", actor=actor, metadata={"schema_version": True})


def test_prefixed_ulids_sort_by_distinct_timestamp() -> None:
    earlier = new_id("evt", timestamp_ms=1_000)
    later = new_id("evt", timestamp_ms=1_001)

    assert len(earlier.removeprefix("evt_")) == 26
    assert earlier < later
