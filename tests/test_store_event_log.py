from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from oracle_lab.events import Actor, Event
from oracle_lab.ids import new_id
from oracle_lab.projections import ProjectionError
from oracle_lab.store import EventIntegrityError, EventStore

FIXTURES = Path(__file__).parent / "fixtures"


def test_file_store_uses_wal_and_preserves_raw_output_exactly(tmp_path: Path) -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    store = EventStore(tmp_path / "events.db")
    event = Event.new(
        "oracle.output",
        actor=Actor(kind="model", id="r1"),
        session_id="ses",
        branch_id="br",
        payload={"content": raw},
        metadata={"schema_version": 2, "raw_archive": "fixture"},
    )

    store.append(event)
    loaded = store.require(event.id)

    assert store.journal_mode == "wal"
    assert loaded == event
    assert loaded.payload["content"].encode() == raw.encode()
    assert (
        store.connection.execute(
            "SELECT schema_version FROM events WHERE id = ?", (event.id,)
        ).fetchone()[0]
        == 2
    )
    assert (
        store.connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()[0][0]
        == 1
    )


def test_database_triggers_reject_event_update_and_delete() -> None:
    store = EventStore()
    event = store.append(Event(type="human.input", actor=Actor(kind="human")))

    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        store.connection.execute("UPDATE events SET type = type WHERE id = ?", (event.id,))
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        store.connection.execute("DELETE FROM events WHERE id = ?", (event.id,))


def test_database_rejects_metadata_and_column_schema_version_mismatch() -> None:
    store = EventStore()

    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
        store.connection.execute(
            """
            INSERT INTO events (
                id, type, created_at, actor_kind, payload_json,
                metadata_json, schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("evt"),
                "human.input",
                "2026-08-30T00:00:00+00:00",
                "human",
                "{}",
                '{"schema_version":2}',
                1,
            ),
        )


def test_projection_failure_rolls_back_event_so_corrected_retry_is_possible() -> None:
    store = EventStore()
    broken = Event(
        type="analysis.claim_detected",
        actor=Actor(kind="host"),
        payload={"claims": [1]},
    )

    with pytest.raises(ProjectionError):
        store.append(broken)

    assert store.get(broken.id) is None
    fixed = Event.from_dict(
        {
            **broken.to_dict(),
            "payload": {"claims": [{"raw": "x = 1"}]},
        }
    )
    store.append(fixed)
    assert store.get(fixed.id) == fixed


def test_parent_and_causation_must_reference_earlier_events() -> None:
    store = EventStore()
    event = Event.new(
        "oracle.output",
        actor=Actor(kind="model"),
        parent_event_id=Event(type="human.input", actor=Actor(kind="human")).id,
    )

    with pytest.raises(EventIntegrityError, match="earlier event"):
        store.append(event)


def test_query_order_is_created_at_then_id_and_cursor_is_exclusive() -> None:
    store = EventStore()
    first = Event(type="human.input", actor=Actor(kind="human"))
    second = Event(
        type="human.note",
        actor=Actor(kind="human"),
        created_at=first.created_at,
        parent_event_id=first.id,
    )
    store.append_many(sorted((first, second), key=lambda event: event.id))
    ordered = store.list_events()

    assert [event.id for event in ordered] == sorted((first.id, second.id))
    assert store.list_events(after=ordered[0].id)[0].id == ordered[1].id
    assert store.verify_integrity() == []
