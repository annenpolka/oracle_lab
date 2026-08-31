from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import oracle_lab.store as store_module
from oracle_lab.events import Actor, Event
from oracle_lab.jobs import JobQueue, JobStatus
from oracle_lab.store import (
    DatabaseVersionError,
    EventStore,
    MigrationDriftError,
    SchemaMigrationError,
)


def test_fresh_database_applies_every_migration_once(tmp_path: Path) -> None:
    database = tmp_path / "fresh.db"
    store = EventStore(database)

    history = [
        tuple(row)
        for row in store.connection.execute(
            """
            SELECT version, name, checksum, applied_at
            FROM schema_migrations ORDER BY version
            """
        )
    ]
    assert [(row[0], row[1]) for row in history] == [
        (1, "initial_event_store"),
        (2, "expand_job_projection"),
        (3, "worker_patch_projection"),
        (4, "sparse_virtual_clock"),
    ]
    assert all(len(row[2]) == 64 for row in history)
    assert {row[1] for row in store.connection.execute("PRAGMA table_info(schema_migrations)")} == {
        "version",
        "name",
        "checksum",
        "applied_at",
    }
    store.close()

    reopened = EventStore(database)
    assert [
        tuple(row)
        for row in reopened.connection.execute(
            """
            SELECT version, name, checksum, applied_at
            FROM schema_migrations ORDER BY version
            """
        )
    ] == history


def test_legacy_v1_database_upgrades_without_losing_events_or_jobs(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy-v1.db"
    connection = sqlite3.connect(database, isolation_level=None)
    connection.executescript(
        """
        CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );
        """
        + store_module._MIGRATIONS[0].sql
    )
    raw = "legacy oracle output\n\n  spacing stays exact\n"
    event = Event(
        type="oracle.output",
        actor=Actor(kind="model", id="legacy-r1"),
        session_id="ses_legacy",
        branch_id="br_legacy",
        payload={"content": raw},
    )
    connection.execute(
        """
        INSERT INTO events (
            id, type, created_at, session_id, branch_id,
            parent_event_id, causation_id, correlation_id,
            actor_kind, actor_id, payload_json, metadata_json,
            schema_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.id,
            event.type.value,
            event.created_at.isoformat(),
            event.session_id,
            event.branch_id,
            event.parent_event_id,
            event.causation_id,
            event.correlation_id,
            event.actor.kind.value,
            event.actor.id,
            json.dumps(dict(event.payload)),
            json.dumps(dict(event.metadata)),
            event.schema_version,
        ),
    )
    connection.execute(
        """
        INSERT INTO jobs (
            id, kind, status, source_event_id, available_at,
            lease_until, worker_id, attempts, payload_json,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "job_legacy",
            "oracle.generate",
            "pending",
            event.id,
            "2026-08-30T00:00:00+00:00",
            None,
            None,
            0,
            '{"prompt":"unchanged"}',
            "2026-08-30T00:00:00+00:00",
            "2026-08-30T00:00:00+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?)",
        ("2026-08-30T00:00:00+00:00",),
    )
    assert "priority" not in {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
    assert {row[1] for row in connection.execute("PRAGMA table_info(schema_migrations)")} == {
        "version",
        "applied_at",
    }
    connection.close()

    upgraded = EventStore(database)

    assert upgraded.require(event.id) == event
    assert upgraded.require(event.id).payload["content"].encode() == raw.encode()
    legacy_job = JobQueue(upgraded).get("job_legacy")
    assert legacy_job is not None
    assert legacy_job.status is JobStatus.PENDING
    assert dict(legacy_job.payload) == {"prompt": "unchanged"}
    assert legacy_job.priority == 0
    assert legacy_job.max_attempts == 5
    upgraded_history = [
        tuple(row)
        for row in upgraded.connection.execute(
            "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
        )
    ]
    assert [(row[0], row[1]) for row in upgraded_history] == [
        (1, "initial_event_store"),
        (2, "expand_job_projection"),
        (3, "worker_patch_projection"),
        (4, "sparse_virtual_clock"),
    ]
    assert upgraded_history[0][2] == "2026-08-30T00:00:00+00:00"
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        upgraded.connection.execute("UPDATE events SET type = type WHERE id = ?", (event.id,))


@pytest.mark.parametrize(
    ("column", "replacement"),
    (("name", "renamed_migration"), ("checksum", "0" * 64)),
)
def test_applied_migration_identity_drift_is_rejected(
    tmp_path: Path, column: str, replacement: str
) -> None:
    database = tmp_path / f"drift-{column}.db"
    EventStore(database).close()
    connection = sqlite3.connect(database, isolation_level=None)
    connection.execute(
        f"UPDATE schema_migrations SET {column} = ? WHERE version = 1",
        (replacement,),
    )
    connection.close()

    with pytest.raises(MigrationDriftError, match=column):
        EventStore(database)


def test_database_newer_than_code_is_rejected(tmp_path: Path) -> None:
    database = tmp_path / "newer.db"
    EventStore(database).close()
    connection = sqlite3.connect(database, isolation_level=None)
    connection.execute(
        """
        INSERT INTO schema_migrations (version, name, checksum, applied_at)
        VALUES (5, 'future_schema', ?, '2026-08-30T00:00:00+00:00')
        """,
        ("f" * 64,),
    )
    connection.close()

    with pytest.raises(DatabaseVersionError, match="newer than"):
        EventStore(database)


def test_failing_migration_rolls_back_all_of_its_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "failing.db"
    EventStore(database).close()
    failing = store_module._Migration(
        5,
        "deliberate_failure",
        """
        CREATE TABLE migration_partial_write (value TEXT NOT NULL);
        INSERT INTO migration_partial_write (value) VALUES ('must roll back');
        SELECT * FROM table_that_does_not_exist;
        """,
    )
    monkeypatch.setattr(store_module, "_MIGRATIONS", (*store_module._MIGRATIONS, failing))
    connection = sqlite3.connect(database, isolation_level=None)

    with pytest.raises(SchemaMigrationError, match="deliberate_failure"):
        EventStore(connection)

    assert connection.in_transaction is False
    assert (
        connection.execute(
            """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'migration_partial_write'
        """
        ).fetchone()
        is None
    )
    assert [
        tuple(row)
        for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version")
    ] == [(1,), (2,), (3,), (4,)]
