"""SQLite-backed append-only event store.

``EventStore`` owns persistence and ordering for immutable events.  Projection
tables live in the same database for convenient local operation, but the
``events`` table is authoritative and database triggers reject history edits.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.jsonutil import canonical_json, sha256_json
from oracle_lab.material import (
    MaterialOrigin,
    explicit_material_origin,
    is_synthetic_lineage,
    is_worker_lineage,
)

_V1_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    session_id TEXT,
    branch_id TEXT,
    parent_event_id TEXT,
    causation_id TEXT,
    correlation_id TEXT,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    CHECK (json_extract(metadata_json, '$.schema_version') = schema_version)
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_branch ON events(branch_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_correlation ON events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_events_parent ON events(parent_event_id);
CREATE INDEX IF NOT EXISTS idx_events_session_order
    ON events(session_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_events_branch_order
    ON events(branch_id, created_at, id);

CREATE TRIGGER IF NOT EXISTS events_reject_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS events_reject_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    title TEXT,
    root_event_id TEXT,
    current_branch_id TEXT,
    model_profile_id TEXT,
    created_at TEXT NOT NULL,
    archived_at TEXT
);

CREATE TABLE IF NOT EXISTS branches (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    parent_branch_id TEXT,
    fork_event_id TEXT,
    title TEXT,
    created_at TEXT NOT NULL,
    archived_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_branches_session ON branches(session_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_branches_parent ON branches(parent_branch_id);

CREATE TABLE IF NOT EXISTS claims (
    id TEXT PRIMARY KEY,
    source_event_id TEXT NOT NULL,
    normalized_subject TEXT,
    normalized_predicate TEXT,
    normalized_object TEXT,
    raw_text TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence REAL,
    first_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS branch_claim_states (
    claim_id TEXT NOT NULL,
    branch_id TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence REAL,
    last_event_id TEXT NOT NULL,
    inherited_from_branch_id TEXT,
    PRIMARY KEY(claim_id, branch_id)
);
CREATE INDEX IF NOT EXISTS idx_branch_claim_status
    ON branch_claim_states(branch_id, status);

CREATE TABLE IF NOT EXISTS claim_transitions (
    event_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    branch_id TEXT,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT,
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    created_at TEXT NOT NULL,
    PRIMARY KEY(event_id, claim_id)
);
CREATE INDEX IF NOT EXISTS idx_claim_transitions_claim
    ON claim_transitions(claim_id, created_at, event_id);

CREATE TABLE IF NOT EXISTS claim_occurrences (
    claim_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    branch_id TEXT,
    relation TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(claim_id, event_id, relation)
);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    type TEXT,
    properties_json TEXT NOT NULL CHECK (json_valid(properties_json))
);

CREATE TABLE IF NOT EXISTS motifs (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    description TEXT,
    embedding BLOB
);

CREATE TABLE IF NOT EXISTS event_motifs (
    event_id TEXT NOT NULL,
    motif_id TEXT NOT NULL,
    score REAL,
    PRIMARY KEY(event_id, motif_id)
);

CREATE TABLE IF NOT EXISTS curation (
    event_id TEXT NOT NULL,
    action TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    action_event_id TEXT NOT NULL,
    PRIMARY KEY(event_id, action, created_at)
);

CREATE TABLE IF NOT EXISTS provenance_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    derived_kind TEXT NOT NULL,
    derived_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    created_event_id TEXT NOT NULL,
    branch_id TEXT,
    UNIQUE(derived_kind, derived_id, source_event_id, relation, created_event_id)
);
CREATE INDEX IF NOT EXISTS idx_provenance_derived
    ON provenance_edges(derived_kind, derived_id);
CREATE INDEX IF NOT EXISTS idx_provenance_source
    ON provenance_edges(source_event_id);

CREATE TABLE IF NOT EXISTS sample_groups (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    branch_id TEXT,
    from_event_id TEXT NOT NULL,
    context_hash TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    sampling_json TEXT NOT NULL CHECK (json_valid(sampling_json)),
    created_event_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sample_groups_context ON sample_groups(context_hash);

CREATE TABLE IF NOT EXISTS sample_outputs (
    group_id TEXT NOT NULL,
    output_event_id TEXT NOT NULL UNIQUE,
    ordinal INTEGER NOT NULL,
    latency_ms REAL,
    provider_cost TEXT,
    classification_json TEXT CHECK (
        classification_json IS NULL OR json_valid(classification_json)
    ),
    PRIMARY KEY(group_id, ordinal)
);

CREATE TABLE IF NOT EXISTS usage_records (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    request_event_id TEXT,
    session_id TEXT,
    branch_id TEXT,
    correlation_id TEXT,
    provider_id TEXT,
    model_id TEXT,
    tool_id TEXT,
    prompt_tokens INTEGER NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
    completion_tokens INTEGER NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
    reasoning_tokens INTEGER CHECK (reasoning_tokens IS NULL OR reasoning_tokens >= 0),
    provider_cost TEXT,
    latency_ms REAL NOT NULL CHECK (latency_ms >= 0),
    ttft_ms REAL CHECK (ttft_ms IS NULL OR ttft_ms >= 0),
    request_count INTEGER NOT NULL DEFAULT 1 CHECK (request_count >= 0),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_session ON usage_records(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_usage_branch ON usage_records(branch_id, created_at);
CREATE INDEX IF NOT EXISTS idx_usage_model ON usage_records(model_id, created_at);

CREATE TABLE IF NOT EXISTS virtual_nodes (
    branch_id TEXT NOT NULL,
    path TEXT NOT NULL,
    inode TEXT NOT NULL,
    kind TEXT NOT NULL,
    properties_json TEXT NOT NULL CHECK (json_valid(properties_json)),
    unresolved_json TEXT NOT NULL CHECK (json_valid(unresolved_json)),
    provenance_json TEXT NOT NULL CHECK (json_valid(provenance_json)),
    last_mutation_event TEXT NOT NULL,
    PRIMARY KEY(branch_id, path)
);

CREATE TABLE IF NOT EXISTS virtual_content_versions (
    branch_id TEXT NOT NULL,
    path TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    content TEXT NOT NULL,
    source_event_ids_json TEXT NOT NULL CHECK (json_valid(source_event_ids_json)),
    event_id TEXT NOT NULL,
    PRIMARY KEY(branch_id, path, version)
);

CREATE TABLE IF NOT EXISTS virtual_commands (
    branch_id TEXT NOT NULL,
    command TEXT NOT NULL,
    version TEXT NOT NULL,
    first_seen_event TEXT NOT NULL,
    known_options_json TEXT NOT NULL CHECK (json_valid(known_options_json)),
    provenance_json TEXT NOT NULL CHECK (json_valid(provenance_json)),
    last_mutation_event TEXT NOT NULL,
    PRIMARY KEY(branch_id, command)
);

CREATE TABLE IF NOT EXISTS virtual_processes (
    branch_id TEXT NOT NULL,
    pid INTEGER NOT NULL,
    parent_pid INTEGER,
    executable TEXT NOT NULL,
    args_json TEXT NOT NULL CHECK (json_valid(args_json)),
    state TEXT NOT NULL,
    signals_json TEXT NOT NULL CHECK (json_valid(signals_json)),
    provenance_json TEXT NOT NULL CHECK (json_valid(provenance_json)),
    callbacks_json TEXT NOT NULL CHECK (json_valid(callbacks_json)),
    last_mutation_event TEXT NOT NULL,
    PRIMARY KEY(branch_id, pid)
);

CREATE TABLE IF NOT EXISTS virtual_process_signals (
    branch_id TEXT NOT NULL,
    pid INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    signal TEXT NOT NULL,
    state TEXT NOT NULL,
    callback TEXT,
    source_event_ids_json TEXT NOT NULL CHECK (json_valid(source_event_ids_json)),
    PRIMARY KEY(branch_id, pid, event_id)
);

CREATE TABLE IF NOT EXISTS projection_applied (
    projection_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    PRIMARY KEY(projection_name, event_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    source_event_id TEXT,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    worker_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_ready
    ON jobs(status, available_at, id);
"""


class EventStoreError(RuntimeError):
    """Base class for event store failures."""


class DuplicateEventError(EventStoreError):
    """Raised when an append attempts to reuse an event ID."""


class EventNotFoundError(EventStoreError):
    """Raised when a required event does not exist."""


class EventIntegrityError(EventStoreError):
    """Raised when a stored row violates the public event contract."""


class SchemaMigrationError(EventStoreError):
    """Raised when a database schema migration cannot be applied safely."""


class MigrationDriftError(SchemaMigrationError):
    """Raised when recorded migration identity differs from this build."""


class DatabaseVersionError(SchemaMigrationError):
    """Raised when a database was created by a newer Oracle Lab build."""


@dataclass(frozen=True, slots=True)
class _Migration:
    """One immutable, checksummed database migration."""

    version: int
    name: str
    sql: str
    runner: Callable[[sqlite3.Connection], None] | None = None

    @property
    def checksum(self) -> str:
        """Return the stable SHA-256 identity of the migration body."""
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()

    def apply(self, connection: sqlite3.Connection) -> None:
        """Apply this migration without opening or committing a transaction."""
        if self.runner is None:
            _execute_sql_script(connection, self.sql)
        else:
            self.runner(connection)


_MIGRATION_TABLE_SCHEMA = """
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
"""

_JOB_PROJECTION_COLUMNS = (
    "idempotency_key TEXT",
    "priority INTEGER NOT NULL DEFAULT 0",
    "provider_id TEXT",
    "session_id TEXT",
    "branch_id TEXT",
    "serialize_branch INTEGER NOT NULL DEFAULT 0 CHECK (serialize_branch IN (0, 1))",
    "max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0)",
    "last_error TEXT",
    "cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1))",
)
_LEGACY_V1_COLUMNS = (
    ("branches", "archived_at TEXT"),
    ("curation", "action_event_id TEXT NOT NULL DEFAULT ''"),
)
_JOB_PROJECTION_INDEXES = """
DROP INDEX IF EXISTS idx_jobs_ready;
CREATE INDEX idx_jobs_ready
    ON jobs(status, available_at, priority DESC, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON jobs(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_jobs_provider_lease
    ON jobs(provider_id, status, lease_until);
CREATE INDEX IF NOT EXISTS idx_jobs_branch_lease
    ON jobs(session_id, branch_id, status, lease_until);
"""
_V2_SCHEMA = "\n".join(
    (
        _V1_SCHEMA,
        *(
            f"ALTER TABLE {table} ADD COLUMN {definition};"
            for table, definition in _LEGACY_V1_COLUMNS
        ),
        *(f"ALTER TABLE jobs ADD COLUMN {definition};" for definition in _JOB_PROJECTION_COLUMNS),
        _JOB_PROJECTION_INDEXES,
    )
)

_V3_SCHEMA = """
CREATE TABLE IF NOT EXISTS worker_runs (
    run_id TEXT PRIMARY KEY,
    task_event_id TEXT NOT NULL,
    started_event_id TEXT NOT NULL UNIQUE,
    terminal_event_id TEXT,
    adapter_id TEXT,
    status TEXT NOT NULL,
    archive_path TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_worker_runs_task ON worker_runs(task_event_id);

CREATE TABLE IF NOT EXISTS candidate_patches (
    patch_event_id TEXT PRIMARY KEY,
    worker_run_id TEXT NOT NULL,
    session_id TEXT,
    branch_id TEXT,
    repository_path TEXT NOT NULL,
    base_commit TEXT NOT NULL,
    patch_sha256 TEXT NOT NULL,
    patch_archive_path TEXT NOT NULL,
    changed_paths_json TEXT NOT NULL CHECK (json_valid(changed_paths_json)),
    status TEXT NOT NULL,
    approval_event_id TEXT,
    rejection_event_id TEXT,
    application_event_id TEXT,
    staging_path TEXT,
    validation_status TEXT,
    validation_event_ids_json TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(validation_event_ids_json)),
    last_event_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_candidate_patches_run
    ON candidate_patches(worker_run_id);
CREATE INDEX IF NOT EXISTS idx_candidate_patches_status
    ON candidate_patches(status, created_at);
"""

_V4_SCHEMA = """
CREATE TABLE IF NOT EXISTS virtual_clocks (
    branch_id TEXT NOT NULL,
    clock_id TEXT NOT NULL,
    current_revision INTEGER,
    value TEXT,
    unit TEXT,
    unresolved_json TEXT NOT NULL CHECK (json_valid(unresolved_json)),
    provenance_json TEXT NOT NULL CHECK (json_valid(provenance_json)),
    last_mutation_event TEXT NOT NULL,
    PRIMARY KEY(branch_id, clock_id)
);

CREATE TABLE IF NOT EXISTS virtual_clock_revisions (
    branch_id TEXT NOT NULL,
    clock_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    operation TEXT NOT NULL CHECK (operation IN ('set', 'advance')),
    value TEXT NOT NULL,
    unit TEXT NOT NULL,
    delta TEXT,
    source_event_ids_json TEXT NOT NULL CHECK (json_valid(source_event_ids_json)),
    event_id TEXT NOT NULL,
    PRIMARY KEY(branch_id, clock_id, revision)
);

CREATE TABLE IF NOT EXISTS virtual_clock_contradictions (
    branch_id TEXT NOT NULL,
    clock_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    prior_revision INTEGER NOT NULL,
    conflicting_revision INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status = 'unresolved'),
    source_event_ids_json TEXT NOT NULL CHECK (json_valid(source_event_ids_json)),
    PRIMARY KEY(branch_id, clock_id, event_id)
);
"""


def _execute_sql_script(connection: sqlite3.Connection, script: str) -> None:
    """Execute a SQL script without sqlite3's implicit transaction commit.

    ``Connection.executescript`` commits any pending transaction before it
    starts.  Migrations instead accumulate statements with SQLite's own parser
    so DDL, DML, and trigger bodies all remain inside the caller's transaction.
    """
    pending = ""
    for line in script.splitlines(keepends=True):
        pending += line
        if not sqlite3.complete_statement(pending):
            continue
        if pending.strip():
            connection.execute(pending)
        pending = ""
    if pending.strip():
        raise SchemaMigrationError("migration SQL ends with an incomplete statement")


def _column_names(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _apply_v2_schema(connection: sqlite3.Connection) -> None:
    """Expand a v1 database to the schema consumed by current projections."""
    # Some pre-runner databases were stamped v1 after applying a monolithic
    # schema.  Replaying CREATE IF NOT EXISTS makes the migration work for both
    # those databases and deliberately minimal v1 fixtures.
    _execute_sql_script(connection, _V1_SCHEMA)
    for table, definition in _LEGACY_V1_COLUMNS:
        column = definition.split(maxsplit=1)[0]
        if column not in _column_names(connection, table):
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
    existing_job_columns = _column_names(connection, "jobs")
    for definition in _JOB_PROJECTION_COLUMNS:
        column = definition.split(maxsplit=1)[0]
        if column not in existing_job_columns:
            connection.execute(f"ALTER TABLE jobs ADD COLUMN {definition}")
            existing_job_columns.add(column)
    _execute_sql_script(connection, _JOB_PROJECTION_INDEXES)


_MIGRATIONS = (
    _Migration(1, "initial_event_store", _V1_SCHEMA),
    _Migration(2, "expand_job_projection", _V2_SCHEMA, _apply_v2_schema),
    _Migration(3, "worker_patch_projection", _V3_SCHEMA),
    _Migration(4, "sparse_virtual_clock", _V4_SCHEMA),
)


def _validate_migration_plan(migrations: Sequence[_Migration]) -> None:
    versions = [migration.version for migration in migrations]
    expected = list(range(1, len(migrations) + 1))
    if versions != expected:
        raise SchemaMigrationError(
            "migration versions must be contiguous and ordered: "
            f"expected {expected}, got {versions}"
        )
    names = [migration.name for migration in migrations]
    if len(set(names)) != len(names) or any(not name.strip() for name in names):
        raise SchemaMigrationError("migration names must be non-empty and unique")


def _iso(value: dt.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.isoformat()


def _validate_direct_host_terminal_archive(
    event: Event,
    *,
    task: Event,
    archive_contents: Mapping[str, bytes],
) -> None:
    """Bind a Direct Host terminal identity to its exact write-once archive."""

    try:
        task_document = json.loads(archive_contents["task.json"])
        command_document = json.loads(archive_contents["command.json"])
        metadata_document = json.loads(archive_contents["metadata.json"])
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EventIntegrityError("Direct Host archive JSON is invalid") from error
    if not all(
        isinstance(value, Mapping) for value in (task_document, command_document, metadata_document)
    ):
        raise EventIntegrityError("Direct Host archive documents have invalid shapes")

    event_payload = thaw_json(event.payload)
    task_payload = thaw_json(task.payload)
    response = task_document.get("direct_host_response")
    identity = event_payload.get("host_identity")
    if not isinstance(response, Mapping) or not isinstance(identity, Mapping):
        raise EventIntegrityError("Direct Host terminal requires archived response identity")
    expected_task = {
        "task_event_id": task.id,
        "job_id": event_payload.get("job_id"),
        "task_kind": task_payload.get("task_kind"),
        "source_event_id": task_payload.get("source_event_id"),
        "goal": task_payload.get("goal"),
        "worker_profile_id": task_payload.get("worker_profile_id"),
        "worker_execution_profile": task_payload.get("worker_execution_profile"),
        "worker_routing": task_payload.get("worker_routing"),
    }
    if any(task_document.get(key) != value for key, value in expected_task.items()):
        raise EventIntegrityError("Direct Host archive task identity differs from its event")

    expected_identity = {
        "prompt_contract": task_document.get("host_prompt_contract"),
        "profile_id": task_document.get("worker_profile_id"),
        "requested_provider_id": response.get("requested_provider_id"),
        "requested_model": response.get("requested_model"),
        "actual_provider": response.get("actual_provider"),
        "returned_model": response.get("returned_model"),
        "routing_settings": response.get("routing_settings"),
        "sampling_settings": response.get("sampling_settings"),
        "api_response_metadata": response.get("api_response_metadata"),
        "usage": response.get("usage"),
        "execution_profile": task_document.get("worker_execution_profile"),
        "routing": task_document.get("worker_routing"),
    }
    if any(identity.get(key) != value for key, value in expected_identity.items()):
        raise EventIntegrityError("Direct Host terminal identity differs from its archive")
    if command_document.get("argv") != [
        "direct-api",
        str(response.get("requested_provider_id") or "unknown"),
        str(response.get("requested_model") or "unknown"),
    ]:
        raise EventIntegrityError("Direct Host command identity differs from its response")

    execution = metadata_document.get("execution")
    if (
        metadata_document.get("schema_version") != 1
        or metadata_document.get("run_id") != event_payload.get("run_id")
        or metadata_document.get("artifact_origin") != "host_generated"
        or not isinstance(execution, Mapping)
    ):
        raise EventIntegrityError("Direct Host archive metadata identity is invalid")
    status_item = execution.get("status")
    output_limited_item = execution.get("output_limited")
    status = (
        status_item.get("value")
        if isinstance(status_item, Mapping) and status_item.get("status") == "known"
        else None
    )
    output_limited = (
        output_limited_item.get("value")
        if isinstance(output_limited_item, Mapping) and output_limited_item.get("status") == "known"
        else None
    )
    expected_completed = event.type is EventType.WORKER_RUN_COMPLETED
    if (
        (status == "completed") != expected_completed
        or not isinstance(output_limited, bool)
        or (not expected_completed and event_payload.get("output_limited") != output_limited)
        or (expected_completed and output_limited)
    ):
        raise EventIntegrityError("Direct Host terminal status differs from its archive")

    execution_profile = task_document.get("worker_execution_profile")
    max_output_bytes = (
        execution_profile.get("max_output_bytes")
        if isinstance(execution_profile, Mapping)
        else None
    )
    stdout = archive_contents["stdout.bin"]
    if (
        not isinstance(max_output_bytes, int)
        or isinstance(max_output_bytes, bool)
        or max_output_bytes <= 0
        or len(stdout) > max_output_bytes
    ):
        raise EventIntegrityError("Direct Host raw response exceeds its frozen output bound")
    response_metadata = response.get("api_response_metadata")
    disposition = (
        response_metadata.get("raw_response_disposition")
        if isinstance(response_metadata, Mapping)
        else None
    )
    if disposition == "bounded_prefix" and (
        not output_limited
        or len(stdout) != max_output_bytes
        or response_metadata.get("captured_bytes") != len(stdout)
        or response_metadata.get("max_output_bytes") != max_output_bytes
    ):
        raise EventIntegrityError("Direct Host bounded prefix metadata is inconsistent")
    if disposition == "quarantined_credential" and (
        stdout
        or response_metadata.get("captured_bytes") != 0
        or "credential_response_quarantined" not in event_payload.get("reasons", ())
    ):
        raise EventIntegrityError("Direct Host credential quarantine is inconsistent")


class EventStore:
    """Append, retrieve, and replay immutable events in SQLite.

    Args:
        database: Filesystem path, ``:memory:``, or an existing connection.
        auto_project: Apply registered rebuildable projections after appends.

    A store created from an external connection never closes that connection.
    The :attr:`connection` escape hatch is intended for projection/query
    modules; callers must not use it to mutate the ``events`` table.
    """

    def __init__(
        self,
        database: str | Path | sqlite3.Connection = ":memory:",
        *,
        auto_project: bool = True,
    ) -> None:
        self._lock = threading.RLock()
        self._savepoint_counter = 0
        self._auto_project = auto_project
        if isinstance(database, sqlite3.Connection):
            self._connection = database
            self._owns_connection = False
        else:
            self._connection = sqlite3.connect(
                str(database), isolation_level=None, check_same_thread=False
            )
            self._owns_connection = True
        self._connection.row_factory = sqlite3.Row
        try:
            self._initialize()
        except BaseException:
            if self._owns_connection:
                self._connection.close()
            raise

    def _initialize(self) -> None:
        with self._lock:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA busy_timeout = 5000")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = NORMAL")
            self._run_migrations()

    def _run_migrations(self, migrations: Sequence[_Migration] | None = None) -> None:
        """Validate migration history and apply every missing migration in order."""
        plan = tuple(_MIGRATIONS if migrations is None else migrations)
        _validate_migration_plan(plan)
        self._prepare_migration_table(plan)
        self._validate_applied_migrations(plan, self._applied_migrations())

        for migration in plan:
            if migration.version in {row[0] for row in self._applied_migrations()}:
                continue
            try:
                with self.transaction() as connection:
                    applied = self._applied_migrations()
                    self._validate_applied_migrations(plan, applied)
                    if migration.version in {row[0] for row in applied}:
                        continue
                    previous = 0 if not applied else applied[-1][0]
                    if previous != migration.version - 1:
                        raise MigrationDriftError(
                            f"cannot apply migration {migration.version} after version {previous}"
                        )
                    migration.apply(connection)
                    connection.execute(
                        """
                        INSERT INTO schema_migrations (
                            version, name, checksum, applied_at
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            migration.version,
                            migration.name,
                            migration.checksum,
                            dt.datetime.now(dt.UTC).isoformat(),
                        ),
                    )
            except (DatabaseVersionError, MigrationDriftError):
                raise
            except Exception as error:
                raise SchemaMigrationError(
                    f"migration {migration.version} ({migration.name}) failed: {error}"
                ) from error

    def _prepare_migration_table(self, plan: Sequence[_Migration]) -> None:
        """Create the history table or adopt the legacy two-column variant."""
        with self.transaction() as connection:
            exists = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'schema_migrations'
                """
            ).fetchone()
            other_objects = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE name NOT LIKE 'sqlite_%' AND name != 'schema_migrations'
                    """
                )
            }
            if exists is None:
                if other_objects:
                    raise MigrationDriftError(
                        "database contains schema objects but has no migration history"
                    )
                _execute_sql_script(connection, _MIGRATION_TABLE_SCHEMA)
                return

            table_info = connection.execute("PRAGMA table_info(schema_migrations)").fetchall()
            columns = {str(row[1]) for row in table_info}
            current_columns = {"version", "name", "checksum", "applied_at"}
            legacy_columns = {"version", "applied_at"}
            if columns == current_columns:
                info_by_name = {str(row[1]): row for row in table_info}
                if int(info_by_name["version"][5]) != 1 or any(
                    int(info_by_name[name][3]) != 1 for name in ("name", "checksum", "applied_at")
                ):
                    raise MigrationDriftError(
                        "schema_migrations constraints do not match the migration runner"
                    )
                applied = self._applied_migrations()
                if not applied and other_objects:
                    raise MigrationDriftError(
                        "database schema exists but migration history is empty"
                    )
                self._validate_applied_migrations(plan, applied)
                return
            if columns != legacy_columns:
                raise MigrationDriftError(
                    "schema_migrations has unsupported columns: " + ", ".join(sorted(columns))
                )

            legacy_rows = [
                (int(row[0]), str(row[1]))
                for row in connection.execute(
                    "SELECT version, applied_at FROM schema_migrations ORDER BY version"
                )
            ]
            versions = [row[0] for row in legacy_rows]
            newest_code_version = 0 if not plan else plan[-1].version
            if versions and versions[-1] > newest_code_version:
                raise DatabaseVersionError(
                    f"database schema version {versions[-1]} is newer than "
                    f"this build's version {newest_code_version}"
                )
            expected_versions = list(range(1, (versions[-1] if versions else 0) + 1))
            if versions != expected_versions:
                raise MigrationDriftError(f"legacy migration history is not contiguous: {versions}")
            if not legacy_rows and other_objects:
                raise MigrationDriftError(
                    "database schema exists but legacy migration history is empty"
                )
            if legacy_rows and "events" not in other_objects:
                raise MigrationDriftError(
                    "legacy migration history claims v1 but the events table is missing"
                )

            expected_by_version = {migration.version: migration for migration in plan}
            connection.execute(
                """
                CREATE TABLE schema_migrations_upgrade (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            connection.executemany(
                """
                INSERT INTO schema_migrations_upgrade (
                    version, name, checksum, applied_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    (
                        version,
                        expected_by_version[version].name,
                        expected_by_version[version].checksum,
                        applied_at,
                    )
                    for version, applied_at in legacy_rows
                ),
            )
            connection.execute("DROP TABLE schema_migrations")
            connection.execute("ALTER TABLE schema_migrations_upgrade RENAME TO schema_migrations")

    def _applied_migrations(self) -> list[tuple[int, str, str, str]]:
        return [
            (int(row[0]), str(row[1]), str(row[2]), str(row[3]))
            for row in self._connection.execute(
                """
                SELECT version, name, checksum, applied_at
                FROM schema_migrations ORDER BY version
                """
            )
        ]

    @staticmethod
    def _validate_applied_migrations(
        plan: Sequence[_Migration], applied: Sequence[tuple[int, str, str, str]]
    ) -> None:
        expected_by_version = {migration.version: migration for migration in plan}
        versions = [row[0] for row in applied]
        newest_code_version = 0 if not plan else plan[-1].version
        if versions and versions[-1] > newest_code_version:
            raise DatabaseVersionError(
                f"database schema version {versions[-1]} is newer than "
                f"this build's version {newest_code_version}"
            )
        expected_versions = list(range(1, (versions[-1] if versions else 0) + 1))
        if versions != expected_versions:
            raise MigrationDriftError(f"migration history is not contiguous: {versions}")
        for version, name, checksum, _applied_at in applied:
            expected = expected_by_version[version]
            if name != expected.name:
                raise MigrationDriftError(
                    f"migration {version} name drift: stored {name!r}, expected {expected.name!r}"
                )
            if checksum != expected.checksum:
                raise MigrationDriftError(
                    f"migration {version} checksum drift: stored {checksum!r}, "
                    f"expected {expected.checksum!r}"
                )

    @property
    def connection(self) -> sqlite3.Connection:
        """Return the configured connection for projection and read services."""
        return self._connection

    @property
    def journal_mode(self) -> str:
        """Return SQLite's active journal mode (``wal`` for file databases)."""
        row = self._connection.execute("PRAGMA journal_mode").fetchone()
        return str(row[0]).lower()

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Run a re-entrant transaction protected by the store lock."""
        with self._lock:
            nested = self._connection.in_transaction
            savepoint = ""
            if nested:
                self._savepoint_counter += 1
                savepoint = f"oracle_lab_sp_{self._savepoint_counter}"
                self._connection.execute(f"SAVEPOINT {savepoint}")
            else:
                self._connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield self._connection
            except BaseException:
                if nested:
                    self._connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    self._connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                else:
                    self._connection.rollback()
                raise
            else:
                if nested:
                    self._connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                else:
                    self._connection.commit()

    def append(self, event: Event | Mapping[str, Any]) -> Event:
        """Atomically append one event and update rebuildable projections."""
        result = self.append_many((event,))
        return result[0]

    def append_many(self, events: Iterable[Event | Mapping[str, Any]]) -> tuple[Event, ...]:
        """Atomically append events in the supplied causal order.

        Parent and causation references must already exist or refer to an
        earlier event in this batch.  On duplicate IDs, no event is appended.
        """
        validated = tuple(
            item if isinstance(item, Event) else Event.from_dict(item) for item in events
        )
        if not validated:
            return ()
        if len({item.id for item in validated}) != len(validated):
            raise DuplicateEventError("append batch contains duplicate event IDs")
        with self.transaction() as connection:
            known_ids = {
                str(row[0]) for row in connection.execute("SELECT id FROM events").fetchall()
            }
            for event in validated:
                # Validate in causal batch order.  Each check can therefore
                # resolve an earlier event in this transaction while any
                # later failure still rolls the whole append back.
                self._validate_human_event(event)
                self._validate_worker_event(event)
                self._validate_new_oracle_output(event)
                self._validate_new_tool_result(event)
                for field_name in ("parent_event_id", "causation_id"):
                    reference = getattr(event, field_name)
                    if reference is not None and reference not in known_ids:
                        raise EventIntegrityError(
                            f"{field_name} {reference!r} does not reference an earlier event"
                        )
                self._validate_declared_canonical_promotion(event)
                try:
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
                            _iso(event.created_at),
                            event.session_id,
                            event.branch_id,
                            event.parent_event_id,
                            event.causation_id,
                            event.correlation_id,
                            event.actor.kind.value,
                            event.actor.id,
                            canonical_json(thaw_json(event.payload)),
                            canonical_json(thaw_json(event.metadata)),
                            event.schema_version,
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    if "UNIQUE" in str(error).upper() or "PRIMARY" in str(error).upper():
                        raise DuplicateEventError(f"event already exists: {event.id}") from error
                    raise EventIntegrityError(str(error)) from error
                known_ids.add(event.id)
            # Projections share the outer transaction. A projection failure
            # therefore rolls back the append, making a corrected retry safe
            # instead of leaving an unprojected duplicate event behind.
            if self._auto_project:
                self.project(validated)
        return validated

    def _validate_human_event(self, event: Event) -> None:
        """Make human judgment an append-time identity boundary.

        This validation intentionally lives below projections and services so
        disabling projections cannot let a Host, worker, or model forge a
        human keep/star/canon gate or any other human-authored action. The
        historical importer's synthetic session-root checkpoint is the one
        system-authored taxonomy exception.
        """
        imported_root = (
            event.type is EventType.HUMAN_CHECKPOINT
            and event.actor.kind is ActorKind.SYSTEM
            and event.actor.id == "historical-importer"
            and event.payload.get("operation") == "session.created"
            and explicit_material_origin(event) is MaterialOrigin.HISTORICAL_FIXTURE
        )
        if (
            event.type.value.startswith("human.")
            and event.actor.kind is not ActorKind.HUMAN
            and not imported_root
        ):
            raise EventIntegrityError(f"{event.type.value} requires a human actor")
        if event.type in {EventType.HUMAN_KEEP, EventType.HUMAN_STAR}:
            if is_worker_lineage(event, self.get):
                raise EventIntegrityError("worker-generated artifacts cannot enter oracle curation")
            target_id = event.payload.get("target_event_id") or event.payload.get("event_id")
            target = self.get(target_id) if isinstance(target_id, str) else None
            if target is not None and is_synthetic_lineage(target, self.get):
                raise EventIntegrityError(
                    "synthetic fixture lineage cannot enter genuine oracle curation"
                )

    def _validate_declared_canonical_promotion(self, event: Event) -> None:
        """Bind every canonical promotion to one exact Human-approved candidate.

        Actor labels are not authority.  The immutable approval and candidate
        records, their claim identity, and their causal/session/branch edges
        must all agree before a canonical transition can enter the log.
        """
        if (
            event.type is not EventType.CLAIM_PROMOTED
            or event.payload.get("to_status") != "canonical"
        ):
            return
        promoted_claim = event.payload.get("claim_id")
        if not isinstance(promoted_claim, str) or not promoted_claim:
            raise EventIntegrityError("canonical promotion requires a claim_id")
        approver_event_id = event.payload.get("approver_event_id")
        approval = self.get(approver_event_id) if isinstance(approver_event_id, str) else None
        if (
            approval is None
            or approval.type is not EventType.HUMAN_KEEP
            or approval.actor.kind is not ActorKind.HUMAN
        ):
            raise EventIntegrityError(
                "canonical promotion requires an existing human.keep approval event"
            )
        approved_claim = approval.payload.get("claim_id")
        if approved_claim != promoted_claim:
            raise EventIntegrityError("canonical promotion approval references another claim")
        candidate_event_id = approval.payload.get("candidate_event_id")
        candidate = self.get(candidate_event_id) if isinstance(candidate_event_id, str) else None
        if candidate is None or candidate.type is not EventType.ANALYSIS_CANON_CANDIDATE:
            raise EventIntegrityError(
                "canonical promotion approval requires an existing canon candidate"
            )
        if candidate.payload.get("claim_id") != promoted_claim:
            raise EventIntegrityError("canonical promotion candidate references another claim")
        if event.payload.get("candidate_event_id") != candidate.id:
            raise EventIntegrityError("canonical promotion references another candidate")
        if (
            approval.payload.get("target_event_id") != candidate.id
            or approval.payload.get("event_id") != candidate.id
            or approval.parent_event_id != candidate.id
            or approval.causation_id != candidate.id
        ):
            raise EventIntegrityError(
                "canonical promotion approval does not target its canon candidate"
            )
        if (
            event.payload.get("source_event_id") != candidate.id
            or event.parent_event_id != candidate.id
            or event.causation_id != candidate.id
        ):
            raise EventIntegrityError(
                "canonical promotion causal source is not its canon candidate"
            )
        contexts = {
            (candidate.session_id, candidate.branch_id),
            (approval.session_id, approval.branch_id),
            (event.session_id, event.branch_id),
        }
        if len(contexts) != 1:
            raise EventIntegrityError(
                "canonical promotion approval, candidate, and promotion must share a branch"
            )
        if is_synthetic_lineage(event, self.get):
            raise EventIntegrityError(
                "synthetic fixture lineage cannot be canonized as oracle material"
            )
        if is_worker_lineage(event, self.get):
            raise EventIntegrityError(
                "worker-generated lineage cannot be canonized as oracle material"
            )

    def _validate_worker_event(self, event: Event) -> None:
        """Keep worker artifacts, human gates, and deterministic application separate."""

        if event.actor.kind is ActorKind.WORKER and event.type.value.startswith("analysis."):
            if (
                event.payload.get("artifact_origin") != "worker_generated"
                or event.metadata.get("artifact_origin") != "worker_generated"
            ):
                raise EventIntegrityError(
                    "coding-worker analysis requires worker_generated artifact_origin"
                )
            if (
                event.payload.get("material_origin") is not None
                or event.metadata.get("material_origin") is not None
            ):
                raise EventIntegrityError(
                    "coding-worker analysis may not claim an Oracle material origin"
                )

        if event.type is EventType.WORKER_TASK_REQUESTED:
            if event.actor.kind not in {ActorKind.HUMAN, ActorKind.HOST}:
                raise EventIntegrityError("worker.task_requested requires a human or host actor")
            required = ("job_id", "task_kind", "source_event_id", "goal")
            if any(
                not isinstance(event.payload.get(key), str) or not str(event.payload[key]).strip()
                for key in required
            ):
                raise EventIntegrityError(
                    "worker.task_requested requires job, kind, source, and exact goal"
                )
            source = self.get(str(event.payload["source_event_id"]))
            if source is None:
                raise EventIntegrityError("worker task source event does not exist")
            if source.session_id != event.session_id or source.branch_id != event.branch_id:
                raise EventIntegrityError("worker task source context does not match")

        run_events = {
            EventType.WORKER_RUN_STARTED,
            EventType.WORKER_RUN_COMPLETED,
            EventType.WORKER_RUN_FAILED,
        }
        if event.type in run_events:
            host_direct_run = (
                event.actor.kind is ActorKind.HOST
                and event.payload.get("adapter_id") == "direct"
                and event.payload.get("artifact_origin") == "host_generated"
                and event.metadata.get("artifact_origin") == "host_generated"
            )
            if event.actor.kind is not ActorKind.WORKER and not host_direct_run:
                raise EventIntegrityError(
                    f"{event.type.value} requires a worker actor or an explicit Host direct run"
                )
            if not isinstance(event.payload.get("run_id"), str):
                raise EventIntegrityError(f"{event.type.value} requires run_id")
            if not isinstance(event.payload.get("task_event_id"), str):
                raise EventIntegrityError(f"{event.type.value} requires task_event_id")
            task = self.get(str(event.payload["task_event_id"]))
            if task is None or task.type is not EventType.WORKER_TASK_REQUESTED:
                raise EventIntegrityError(f"{event.type.value} requires an existing worker task")
            if task.session_id != event.session_id or task.branch_id != event.branch_id:
                raise EventIntegrityError("worker run context does not match its task")
            started = [
                candidate
                for candidate in self.list_events(event_type=EventType.WORKER_RUN_STARTED)
                if candidate.payload.get("run_id") == event.payload.get("run_id")
            ]
            if event.type is EventType.WORKER_RUN_STARTED:
                if started:
                    raise EventIntegrityError("worker run_id already has a start event")
            else:
                if any(
                    candidate.payload.get("run_id") == event.payload.get("run_id")
                    for candidate in self.list_events(
                        event_type=[
                            EventType.WORKER_RUN_COMPLETED,
                            EventType.WORKER_RUN_FAILED,
                        ]
                    )
                ):
                    raise EventIntegrityError("worker run_id already has a terminal event")
                if len(started) != 1:
                    raise EventIntegrityError(
                        "terminal worker run requires exactly one existing start event"
                    )
                if started[0].payload.get("task_event_id") != event.payload.get("task_event_id"):
                    raise EventIntegrityError("worker run terminal cites another task")
                archive_path = event.payload.get("archive_path")
                manifest = event.payload.get("archive_manifest")
                if not isinstance(archive_path, str) or not archive_path:
                    raise EventIntegrityError("terminal worker run requires archive_path")
                archive_directory = Path(archive_path)
                if archive_directory.is_symlink() or not archive_directory.is_dir():
                    raise EventIntegrityError("worker run archive directory is unavailable")
                required_artifacts = {
                    "task.json",
                    "prompt.txt",
                    "command.json",
                    "stdout.bin",
                    "stderr.bin",
                    "patch.diff",
                    "metadata.json",
                }
                if not isinstance(manifest, Mapping) or set(manifest) != required_artifacts:
                    raise EventIntegrityError(
                        "terminal worker run requires a complete archive manifest"
                    )
                archive_contents: dict[str, bytes] = {}
                for name, integrity in manifest.items():
                    if not isinstance(integrity, Mapping):
                        raise EventIntegrityError(f"invalid archive entry: {name}")
                    digest = integrity.get("sha256")
                    size = integrity.get("size_bytes")
                    if (
                        not isinstance(digest, str)
                        or len(digest) != 64
                        or any(c not in "0123456789abcdef" for c in digest)
                        or not isinstance(size, int)
                        or isinstance(size, bool)
                        or size < 0
                    ):
                        raise EventIntegrityError(f"invalid archive integrity: {name}")
                    raw_path = integrity.get("path")
                    if not isinstance(raw_path, str):
                        raise EventIntegrityError(f"archive path is missing: {name}")
                    artifact_path = Path(raw_path)
                    if artifact_path.is_symlink() or not artifact_path.is_file():
                        raise EventIntegrityError(f"archive artifact is unavailable: {name}")
                    try:
                        relative = artifact_path.resolve().relative_to(archive_directory.resolve())
                    except ValueError as error:
                        raise EventIntegrityError(
                            f"archive artifact escapes its run directory: {name}"
                        ) from error
                    if relative.parts != (name,):
                        raise EventIntegrityError(f"archive artifact path mismatch: {name}")
                    content = artifact_path.read_bytes()
                    if len(content) != size or hashlib.sha256(content).hexdigest() != digest:
                        raise EventIntegrityError(f"archive artifact integrity mismatch: {name}")
                    archive_contents[name] = content
                if host_direct_run:
                    _validate_direct_host_terminal_archive(
                        event,
                        task=task,
                        archive_contents=archive_contents,
                    )

        if event.type is EventType.WORKER_PATCH_PROPOSED:
            if event.actor.kind is not ActorKind.WORKER:
                raise EventIntegrityError("worker.patch_proposed requires a worker actor")
            if event.payload.get("artifact_origin") != "worker_generated":
                raise EventIntegrityError(
                    "worker.patch_proposed requires worker_generated artifact_origin"
                )
            if event.payload.get("material_origin") is not None:
                raise EventIntegrityError("worker patches may not claim oracle material origin")
            required_strings = (
                "worker_run_id",
                "task_event_id",
                "repository_path",
                "base_commit",
                "patch_archive_path",
                "patch_sha256",
            )
            if any(
                not isinstance(event.payload.get(key), str) or not str(event.payload[key])
                for key in required_strings
            ):
                raise EventIntegrityError(
                    "worker.patch_proposed requires run, base, path, and hash"
                )
            patch_sha256 = str(event.payload["patch_sha256"])
            if len(patch_sha256) != 64 or any(
                character not in "0123456789abcdef" for character in patch_sha256
            ):
                raise EventIntegrityError("worker patch SHA-256 is invalid")
            source_ids = event.payload.get("source_event_ids")
            if (
                not isinstance(source_ids, Sequence)
                or isinstance(source_ids, (str, bytes, bytearray))
                or not source_ids
                or any(not isinstance(value, str) or not value for value in source_ids)
            ):
                raise EventIntegrityError(
                    "worker.patch_proposed requires non-empty source_event_ids"
                )
            run_id = str(event.payload["worker_run_id"])
            completed_runs = [
                candidate
                for candidate in self.list_events(event_type=EventType.WORKER_RUN_COMPLETED)
                if candidate.payload.get("run_id") == run_id
            ]
            if len(completed_runs) != 1:
                raise EventIntegrityError("worker.patch_proposed requires one completed worker run")
            if (
                completed_runs[0].session_id != event.session_id
                or completed_runs[0].branch_id != event.branch_id
            ):
                raise EventIntegrityError("worker patch context does not match its run")
            completed_run = completed_runs[0]
            task_event = self.get(str(event.payload["task_event_id"]))
            if (
                task_event is None
                or task_event.type is not EventType.WORKER_TASK_REQUESTED
                or task_event.session_id != event.session_id
                or task_event.branch_id != event.branch_id
                or completed_run.payload.get("task_event_id") != task_event.id
                or completed_run.causation_id != task_event.id
                or completed_run.actor != event.actor
                or event.causation_id != task_event.id
                or event.parent_event_id != completed_run.id
            ):
                raise EventIntegrityError("worker patch task/run ancestry is inconsistent")
            task_source_ids = task_event.payload.get("source_event_ids")
            if (
                tuple(source_ids) != tuple(task_source_ids or ())
                or event.payload.get("repository_path") != task_event.payload.get("repository_path")
                or event.payload.get("base_commit") != task_event.payload.get("base_commit")
            ):
                raise EventIntegrityError("worker patch source or repository differs from its task")
            capture = completed_run.payload.get("candidate_patch")
            if not isinstance(capture, Mapping):
                raise EventIntegrityError("worker patch run lacks a candidate capture")
            scalar_capture_fields = (
                "repository_path",
                "base_commit",
                "patch_archive_path",
                "patch_sha256",
                "patch_size_bytes",
                "workspace_head",
                "source_status_before_sha256",
                "source_status_after_sha256",
                "source_head_before",
                "source_head_after",
                "source_index_before_sha256",
                "source_index_after_sha256",
                "source_snapshot_before_sha256",
                "source_snapshot_after_sha256",
            )
            if any(event.payload.get(key) != capture.get(key) for key in scalar_capture_fields):
                raise EventIntegrityError("worker patch fields differ from its run capture")
            if (
                tuple(event.payload.get("changed_paths", ()))
                != tuple(capture.get("changed_paths", ()))
                or dict(event.payload.get("changed_modes", {}))
                != dict(capture.get("changed_modes", {}))
                or dict(event.payload.get("precondition_sha256", {}))
                != dict(capture.get("precondition_sha256", {}))
                or event.payload.get("worker_identity")
                != completed_run.payload.get("worker_identity")
            ):
                raise EventIntegrityError("worker patch capture identity is inconsistent")
            patch_artifact = completed_run.payload["archive_manifest"].get("patch.diff")
            if (
                not isinstance(patch_artifact, Mapping)
                or patch_artifact.get("path") != event.payload.get("patch_archive_path")
                or patch_artifact.get("sha256") != event.payload.get("patch_sha256")
                or patch_artifact.get("size_bytes") != event.payload.get("patch_size_bytes")
            ):
                raise EventIntegrityError("worker patch identity differs from its run archive")
            changed_paths = event.payload.get("changed_paths")
            if (
                not isinstance(changed_paths, Sequence)
                or isinstance(changed_paths, (str, bytes, bytearray))
                or not changed_paths
                or any(not isinstance(path, str) or not path for path in changed_paths)
            ):
                raise EventIntegrityError("worker patch requires non-empty changed_paths")
            for source_id in source_ids:
                cited = self.get(source_id)
                if cited is None or cited.session_id != event.session_id:
                    raise EventIntegrityError(
                        "worker patch source events must exist in the same session"
                    )
            if any(
                candidate.payload.get("worker_run_id") == run_id
                for candidate in self.list_events(event_type=EventType.WORKER_PATCH_PROPOSED)
            ):
                raise EventIntegrityError("worker run already has a candidate patch")

        if event.type is EventType.WORKER_PATCH_SECURITY_REJECTED:
            if event.actor.kind is not ActorKind.SYSTEM:
                raise EventIntegrityError("worker.patch_security_rejected requires a system actor")
            if not isinstance(event.payload.get("worker_run_id"), str):
                raise EventIntegrityError("worker.patch_security_rejected requires worker_run_id")
            rejected_run_id = str(event.payload["worker_run_id"])
            if not any(
                candidate.payload.get("run_id") == rejected_run_id
                for candidate in self.list_events(event_type=EventType.WORKER_RUN_COMPLETED)
            ):
                raise EventIntegrityError(
                    "worker.patch_security_rejected requires a completed worker run"
                )
            reasons = event.payload.get("reasons")
            if (
                not isinstance(reasons, Sequence)
                or isinstance(reasons, (str, bytes, bytearray))
                or not reasons
                or any(not isinstance(reason, str) or not reason for reason in reasons)
            ):
                raise EventIntegrityError(
                    "worker.patch_security_rejected requires non-empty reasons"
                )

        if event.type in {
            EventType.HUMAN_PATCH_APPROVED,
            EventType.HUMAN_PATCH_REJECTED,
        }:
            patch_event_id = event.payload.get("patch_event_id")
            patch = self.get(patch_event_id) if isinstance(patch_event_id, str) else None
            if patch is None or patch.type is not EventType.WORKER_PATCH_PROPOSED:
                raise EventIntegrityError(
                    f"{event.type.value} requires an existing worker.patch_proposed"
                )
            if (
                event.session_id != patch.session_id
                or event.branch_id != patch.branch_id
                or event.payload.get("patch_sha256") != patch.payload.get("patch_sha256")
                or event.payload.get("base_commit") != patch.payload.get("base_commit")
            ):
                raise EventIntegrityError(
                    f"{event.type.value} must freeze the matching patch hash and base"
                )
            if any(
                candidate.payload.get("patch_event_id") == patch_event_id
                for candidate in self.list_events(
                    event_type=[
                        EventType.HUMAN_PATCH_APPROVED,
                        EventType.HUMAN_PATCH_REJECTED,
                    ]
                )
            ):
                raise EventIntegrityError("candidate patch already has a human judgment")

        if event.type in {
            EventType.WORKER_PATCH_APPLIED,
            EventType.WORKER_PATCH_CONFLICT,
        }:
            if event.actor.kind in {ActorKind.WORKER, ActorKind.MODEL}:
                raise EventIntegrityError(
                    f"{event.type.value} requires deterministic application code"
                )
            patch_event_id = event.payload.get("patch_event_id")
            approval_event_id = event.payload.get("approval_event_id")
            patch = self.get(patch_event_id) if isinstance(patch_event_id, str) else None
            approval = self.get(approval_event_id) if isinstance(approval_event_id, str) else None
            if patch is None or patch.type is not EventType.WORKER_PATCH_PROPOSED:
                raise EventIntegrityError("patch application requires an existing patch event")
            if (
                approval is None
                or approval.type is not EventType.HUMAN_PATCH_APPROVED
                or approval.actor.kind is not ActorKind.HUMAN
                or approval.payload.get("patch_event_id") != patch_event_id
            ):
                raise EventIntegrityError(
                    "patch application requires its matching human approval event"
                )
            if (
                event.payload.get("patch_sha256") != patch.payload.get("patch_sha256")
                or event.payload.get("base_commit") != patch.payload.get("base_commit")
                or approval.payload.get("patch_sha256") != patch.payload.get("patch_sha256")
                or approval.payload.get("base_commit") != patch.payload.get("base_commit")
            ):
                raise EventIntegrityError("patch application hash or base does not match")
            if event.session_id != patch.session_id or event.branch_id != patch.branch_id:
                raise EventIntegrityError("patch application context does not match")
            if any(
                candidate.payload.get("patch_event_id") == patch_event_id
                for candidate in self.list_events(
                    event_type=[
                        EventType.WORKER_PATCH_APPLIED,
                        EventType.WORKER_PATCH_CONFLICT,
                    ]
                )
            ):
                raise EventIntegrityError("candidate patch already has an application result")

        if event.type in {
            EventType.WORKER_VALIDATION_COMPLETED,
            EventType.WORKER_VALIDATION_FAILED,
        }:
            if event.actor.kind is not ActorKind.TOOL:
                raise EventIntegrityError("worker validation requires a deterministic tool actor")
            if (
                event.payload.get("truth_domain") != "sandbox"
                or event.metadata.get("truth_domain") != "sandbox"
                or event.payload.get("artifact_origin") != "tool_result"
                or event.metadata.get("artifact_origin") != "tool_result"
            ):
                raise EventIntegrityError(
                    "worker validation requires sandbox tool-result provenance"
                )
            patch_event_id = event.payload.get("patch_event_id")
            patch = self.get(patch_event_id) if isinstance(patch_event_id, str) else None
            if patch is None or patch.type is not EventType.WORKER_PATCH_PROPOSED:
                raise EventIntegrityError("worker validation requires an existing patch event")
            if event.session_id != patch.session_id or event.branch_id != patch.branch_id:
                raise EventIntegrityError("worker validation context does not match")
            application_event_id = event.payload.get("application_event_id")
            application = (
                self.get(application_event_id) if isinstance(application_event_id, str) else None
            )
            approval_event_id = event.payload.get("approval_event_id")
            approval = self.get(approval_event_id) if isinstance(approval_event_id, str) else None
            if (
                application is None
                or application.type is not EventType.WORKER_PATCH_APPLIED
                or application.payload.get("patch_event_id") != patch_event_id
                or application.session_id != event.session_id
                or application.branch_id != event.branch_id
            ):
                raise EventIntegrityError(
                    "worker validation requires the matching patch application"
                )
            if (
                approval is None
                or approval.type is not EventType.HUMAN_PATCH_APPROVED
                or approval.actor.kind is not ActorKind.HUMAN
                or approval.payload.get("patch_event_id") != patch_event_id
                or application.payload.get("approval_event_id") != approval_event_id
            ):
                raise EventIntegrityError("worker validation requires the matching human approval")
            if event.parent_event_id != application.id or event.causation_id != patch.id:
                raise EventIntegrityError(
                    "worker validation ancestry must cite its application and patch"
                )
            target_tree = event.payload.get("target_tree")
            commands = event.payload.get("commands")
            if (
                not isinstance(target_tree, str)
                or not target_tree
                or target_tree != application.payload.get("target_tree")
                or not isinstance(commands, Sequence)
                or isinstance(commands, (str, bytes, bytearray))
                or not commands
                or any(not isinstance(command, str) or not command.strip() for command in commands)
            ):
                raise EventIntegrityError(
                    "worker validation target tree or commands differ from its application"
                )
            worker_task_id = patch.payload.get("task_event_id")
            worker_task = self.get(worker_task_id) if isinstance(worker_task_id, str) else None
            if (
                worker_task is None
                or worker_task.type is not EventType.WORKER_TASK_REQUESTED
                or tuple(worker_task.payload.get("validation_commands", ())) != tuple(commands)
            ):
                raise EventIntegrityError(
                    "worker validation commands differ from the frozen worker task"
                )
            if (
                event.payload.get("patch_sha256") != patch.payload.get("patch_sha256")
                or event.payload.get("base_commit") != patch.payload.get("base_commit")
                or application.payload.get("patch_sha256") != patch.payload.get("patch_sha256")
                or application.payload.get("base_commit") != patch.payload.get("base_commit")
                or approval.payload.get("patch_sha256") != patch.payload.get("patch_sha256")
                or approval.payload.get("base_commit") != patch.payload.get("base_commit")
            ):
                raise EventIntegrityError("worker validation patch identity does not match")
            if any(
                candidate.payload.get("application_event_id") == application.id
                for candidate in self.list_events(
                    event_type=[
                        EventType.WORKER_VALIDATION_COMPLETED,
                        EventType.WORKER_VALIDATION_FAILED,
                    ]
                )
            ):
                raise EventIntegrityError("patch application already has a validation terminal")
            archive_path = event.payload.get("archive_path")
            manifest = event.payload.get("archive_manifest")
            required_artifacts = {
                "task.json",
                "command.json",
                "stdout.bin",
                "stderr.bin",
                "metadata.json",
            }
            if (
                not isinstance(archive_path, str)
                or not archive_path
                or not isinstance(manifest, Mapping)
                or set(manifest) != required_artifacts
            ):
                raise EventIntegrityError("worker validation archive is incomplete")
            archive_directory = Path(archive_path)
            if archive_directory.is_symlink() or not archive_directory.is_dir():
                raise EventIntegrityError("worker validation archive is unavailable")
            archive_contents: dict[str, bytes] = {}
            for name, integrity in manifest.items():
                if not isinstance(integrity, Mapping):
                    raise EventIntegrityError(f"invalid validation archive entry: {name}")
                raw_path = integrity.get("path")
                digest = integrity.get("sha256")
                size = integrity.get("size_bytes")
                if (
                    not isinstance(raw_path, str)
                    or not isinstance(digest, str)
                    or len(digest) != 64
                    or any(character not in "0123456789abcdef" for character in digest)
                    or not isinstance(size, int)
                    or isinstance(size, bool)
                    or size < 0
                ):
                    raise EventIntegrityError(f"invalid validation archive integrity: {name}")
                artifact_path = Path(raw_path)
                if artifact_path.is_symlink() or not artifact_path.is_file():
                    raise EventIntegrityError(f"validation archive artifact is unavailable: {name}")
                try:
                    relative = artifact_path.resolve().relative_to(archive_directory.resolve())
                except ValueError as error:
                    raise EventIntegrityError(
                        f"validation archive artifact escapes its directory: {name}"
                    ) from error
                content = artifact_path.read_bytes()
                if (
                    relative.parts != (name,)
                    or len(content) != size
                    or hashlib.sha256(content).hexdigest() != digest
                ):
                    raise EventIntegrityError(
                        f"validation archive artifact integrity mismatch: {name}"
                    )
                archive_contents[name] = content
            try:
                validation_task_document = json.loads(archive_contents["task.json"])
                command_document = json.loads(archive_contents["command.json"])
                metadata_document = json.loads(archive_contents["metadata.json"])
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise EventIntegrityError("worker validation archive JSON is invalid") from error
            if (
                not isinstance(validation_task_document, Mapping)
                or not isinstance(command_document, Mapping)
                or not isinstance(metadata_document, Mapping)
            ):
                raise EventIntegrityError("worker validation archive documents must be objects")

            expected_task_values = {
                "patch_event_id": patch.id,
                "approval_event_id": approval.id,
                "application_event_id": application.id,
                "patch_sha256": patch.payload.get("patch_sha256"),
                "base_commit": patch.payload.get("base_commit"),
                "target_tree": target_tree,
                "staging_path": application.payload.get("staging_path"),
            }
            if any(
                validation_task_document.get(key) != value
                for key, value in expected_task_values.items()
            ) or tuple(validation_task_document.get("commands", ())) != tuple(commands):
                raise EventIntegrityError(
                    "worker validation task archive differs from its event identity"
                )
            event_payload = thaw_json(event.payload)
            if validation_task_document.get("sandbox_config") != event_payload.get(
                "sandbox_config"
            ) or validation_task_document.get("sandbox_image_identity") != event_payload.get(
                "sandbox_image_identity"
            ):
                raise EventIntegrityError(
                    "worker validation sandbox identity differs from its archive"
                )
            expected_command = "set -eu\n" + "\n".join(f"({command})" for command in commands)
            if command_document.get("argv") != ["/bin/sh", "-lc", expected_command]:
                raise EventIntegrityError(
                    "worker validation command archive differs from its frozen commands"
                )

            internal_integrity = metadata_document.get("artifacts")
            content_artifacts = {"task.json", "command.json", "stdout.bin", "stderr.bin"}
            if (
                metadata_document.get("schema_version") != 1
                or metadata_document.get("truth_domain") != "sandbox"
                or metadata_document.get("artifact_origin") != "tool_result"
                or not isinstance(metadata_document.get("run_id"), str)
                or not isinstance(metadata_document.get("validation_id"), str)
                or not isinstance(internal_integrity, Mapping)
                or set(internal_integrity) != content_artifacts
            ):
                raise EventIntegrityError("worker validation archive metadata is invalid")
            for name in content_artifacts:
                integrity = internal_integrity[name]
                content = archive_contents[name]
                if (
                    not isinstance(integrity, Mapping)
                    or integrity.get("sha256") != hashlib.sha256(content).hexdigest()
                    or integrity.get("size_bytes") != len(content)
                ):
                    raise EventIntegrityError(
                        f"worker validation internal archive integrity mismatch: {name}"
                    )

            execution = metadata_document.get("execution")
            observed_fields = (
                "status",
                "error",
                "exit_code",
                "timed_out",
                "output_limited",
            )
            if not isinstance(execution, Mapping):
                raise EventIntegrityError("worker validation execution metadata is missing")
            for field_name in observed_fields:
                observation = execution.get(field_name)
                if (
                    not isinstance(observation, Mapping)
                    or set(observation) != {"status", "value"}
                    or observation.get("status") != "known"
                    or field_name not in event.payload
                    or observation.get("value") != event.payload.get(field_name)
                ):
                    raise EventIntegrityError(
                        f"worker validation {field_name} differs from its archive"
                    )
            status = event.payload.get("status")
            error_value = event.payload.get("error")
            exit_code = event.payload.get("exit_code")
            timed_out = event.payload.get("timed_out")
            output_limited = event.payload.get("output_limited")
            valid_statuses = {
                "ok",
                "error",
                "denied",
                "pending_approval",
                "timeout",
                "output_limit",
            }
            if (
                status not in valid_statuses
                or (error_value is not None and not isinstance(error_value, str))
                or (
                    exit_code is not None
                    and (isinstance(exit_code, bool) or not isinstance(exit_code, int))
                )
                or not isinstance(timed_out, bool)
                or not isinstance(output_limited, bool)
                or timed_out != (status == "timeout")
                or output_limited != (status == "output_limit")
                or (event.type is EventType.WORKER_VALIDATION_COMPLETED) != (status == "ok")
            ):
                raise EventIntegrityError(
                    "worker validation terminal type contradicts its execution result"
                )

    def _validate_new_oracle_output(self, event: Event) -> None:
        if event.type is not EventType.ORACLE_OUTPUT:
            return
        origin = explicit_material_origin(event)
        if origin is MaterialOrigin.UNKNOWN:
            raise EventIntegrityError(
                "oracle.output requires oracle_generated, historical_fixture, "
                "or synthetic_fixture material_origin"
            )
        payload_origin = event.payload.get("material_origin")
        metadata_origin = event.metadata.get("material_origin")
        if (
            payload_origin is not None
            and metadata_origin is not None
            and payload_origin != metadata_origin
        ):
            raise EventIntegrityError("oracle.output material_origin labels disagree")
        if origin is MaterialOrigin.SYNTHETIC_FIXTURE:
            if (
                event.payload.get("archive_path") is not None
                or event.payload.get("archive_sha256") is not None
            ):
                raise EventIntegrityError("synthetic_fixture output may not claim a raw archive")
            return
        if event.actor.kind is not ActorKind.MODEL:
            raise EventIntegrityError("genuine oracle.output requires a model actor")
        identity = event.payload.get("model_identity")
        if not isinstance(identity, Mapping):
            raise EventIntegrityError("non-synthetic oracle.output requires model_identity")
        required_identity = {
            "requested_model_profile_id",
            "requested_model_slug",
            "model_family",
            "checkpoint",
            "runtime",
            "quantization",
            "requested_provider_id",
            "provider_routing",
            "actual_provider",
            "actual_model_identifier",
            "fallback_occurred",
        }
        missing = sorted(required_identity - set(identity))
        if missing:
            raise EventIntegrityError(
                "oracle.output model_identity is incomplete: " + ", ".join(missing)
            )
        context_hash = event.payload.get("context_hash")
        if not isinstance(context_hash, str) or not context_hash:
            raise EventIntegrityError("non-synthetic oracle.output requires context_hash")
        if "sampling" not in event.payload:
            raise EventIntegrityError("non-synthetic oracle.output requires sampling metadata")
        if "api_response_metadata" not in event.payload:
            raise EventIntegrityError(
                "non-synthetic oracle.output requires API response metadata or explicit unknown"
            )
        source_descriptor = event.payload.get("source_file") or event.payload.get("source_fixture")
        has_historical_source = (
            isinstance(source_descriptor, Mapping)
            and isinstance(source_descriptor.get("sha256"), str)
            and len(str(source_descriptor["sha256"])) == 64
        )
        if origin is MaterialOrigin.HISTORICAL_FIXTURE and not (
            has_historical_source
            or (
                isinstance(event.payload.get("archive_path"), str)
                and isinstance(event.payload.get("archive_sha256"), str)
            )
        ):
            raise EventIntegrityError(
                "historical_fixture output requires a SHA-addressed source or raw archive"
            )
        archive_required = origin is MaterialOrigin.ORACLE_GENERATED or (
            origin is MaterialOrigin.HISTORICAL_FIXTURE and not has_historical_source
        )
        sidecar: Mapping[str, Any] | None = None
        if archive_required:
            archive_path = event.payload.get("archive_path")
            archive_sha256 = event.payload.get("archive_sha256")
            if not isinstance(archive_path, str) or not isinstance(archive_sha256, str):
                raise EventIntegrityError(
                    f"{origin.value} output requires a committed raw archive reference"
                )
            raw_path = Path(archive_path)
            if not raw_path.is_file():
                raise EventIntegrityError(
                    f"{origin.value} raw archive does not exist at append time"
                )
            actual_sha256 = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            if actual_sha256 != archive_sha256:
                raise EventIntegrityError(f"{origin.value} raw archive SHA-256 mismatch")
            metadata_path = raw_path.with_name(f"{raw_path.stem}.metadata.json")
            if not metadata_path.is_file():
                raise EventIntegrityError(f"{origin.value} archive metadata sidecar is missing")
            try:
                sidecar = json.loads(metadata_path.read_bytes())
            except (OSError, json.JSONDecodeError) as error:
                raise EventIntegrityError(
                    f"{origin.value} archive metadata sidecar is invalid"
                ) from error
            if not isinstance(sidecar, Mapping):
                raise EventIntegrityError(f"{origin.value} archive sidecar must be an object")
            if sidecar.get("event_id") != event.id:
                raise EventIntegrityError(f"{origin.value} archive sidecar event ID mismatch")
            if sidecar.get("raw_file") != raw_path.name:
                raise EventIntegrityError(f"{origin.value} archive sidecar raw filename mismatch")
            if sidecar.get("raw_sha256") != archive_sha256:
                raise EventIntegrityError(f"{origin.value} archive sidecar raw SHA-256 mismatch")
            if sidecar.get("material_origin") != origin.value:
                raise EventIntegrityError(f"{origin.value} archive sidecar origin mismatch")
            request_sha256 = sidecar.get("request_sha256")
            if not isinstance(request_sha256, str) or len(request_sha256) != 64:
                raise EventIntegrityError(f"{origin.value} archive sidecar request hash is missing")
            self._validate_oracle_archive_identity(event, identity, sidecar)
            self._validate_oracle_provider_ancestry(event, identity)

    @staticmethod
    def _validate_oracle_archive_identity(
        event: Event,
        identity: Mapping[str, Any],
        sidecar: Mapping[str, Any],
    ) -> None:
        """Tie queryable model identity to the immutable provider sidecar."""

        request_metadata = sidecar.get("request_metadata")
        if not isinstance(request_metadata, Mapping):
            raise EventIntegrityError("oracle output archive has no request metadata")
        generation_settings = sidecar.get("generation_settings")
        if not isinstance(generation_settings, Mapping):
            raise EventIntegrityError("oracle output archive has no generation settings")
        archived_request_event_id = request_metadata.get("request_event_id")
        if (
            archived_request_event_id is not None
            and archived_request_event_id != event.causation_id
        ):
            raise EventIntegrityError("oracle output request ID differs from archive")
        expected_requested_slug = request_metadata.get("requested_model_slug")
        if expected_requested_slug is None:
            expected_requested_slug = generation_settings.get("model")
        identity_pairs = {
            "requested_model_profile_id": generation_settings.get("model_profile_id"),
            "requested_model_slug": expected_requested_slug,
            "model_family": request_metadata.get("model_family"),
            "checkpoint": request_metadata.get("checkpoint"),
            "runtime": request_metadata.get("runtime"),
            "quantization": request_metadata.get("quantization"),
            "requested_provider_id": request_metadata.get("requested_provider_id"),
            "provider_routing": request_metadata.get("provider_routing"),
            "actual_model_identifier": sidecar.get("provider_model_id"),
        }
        for key, expected in identity_pairs.items():
            if thaw_json(identity.get(key)) != thaw_json(expected):
                raise EventIntegrityError(
                    f"oracle output model_identity {key} differs from archive"
                )

        sampling = event.payload.get("sampling")
        provider_pin = sampling.get("provider_pin") if isinstance(sampling, Mapping) else None
        provider_name = sidecar.get("provider_name")
        routed_provider = sidecar.get("routed_provider_name")
        expected_actual_provider = (
            routed_provider if provider_pin else routed_provider or provider_name
        )
        if identity.get("actual_provider") != expected_actual_provider:
            raise EventIntegrityError(
                "oracle output model_identity actual_provider differs from archive"
            )
        if provider_pin and routed_provider is None:
            expected_fallback: bool | None = None
        elif provider_pin:
            expected_fallback = str(routed_provider).casefold() != str(provider_pin).casefold()
        else:
            expected_fallback = False
        if identity.get("fallback_occurred") is not expected_fallback:
            raise EventIntegrityError(
                "oracle output model_identity fallback status differs from archive"
            )

        scalar_fields = {
            "provider_name": provider_name,
            "routed_provider_name": routed_provider,
            "provider_model_id": sidecar.get("provider_model_id"),
        }
        for key, expected in scalar_fields.items():
            if event.payload.get(key) != expected:
                raise EventIntegrityError(f"oracle output {key} differs from archive")

        api_metadata = event.payload.get("api_response_metadata")
        if not isinstance(api_metadata, Mapping):
            raise EventIntegrityError("archive-backed oracle output requires API response metadata")
        archived_response_settings = {
            key: thaw_json(value)
            for key, value in generation_settings.items()
            if key != "model_profile_id"
        }
        api_pairs = {
            "http_status": sidecar.get("http_status"),
            "http_headers": sidecar.get("http_headers"),
            "provider_request_id": sidecar.get("provider_request_id"),
            "api_revision": sidecar.get("api_revision"),
            "provider_adapter": provider_name,
            "routed_provider_name": routed_provider,
        }
        for key, expected in api_pairs.items():
            if thaw_json(api_metadata.get(key)) != thaw_json(expected):
                raise EventIntegrityError(
                    f"oracle output API response metadata {key} differs from archive"
                )
        api_response_settings = api_metadata.get("generation_settings")
        if not isinstance(api_response_settings, Mapping) or any(
            archived_response_settings.get(key) != thaw_json(value)
            for key, value in api_response_settings.items()
        ):
            raise EventIntegrityError(
                "oracle output API response metadata generation_settings differs from archive"
            )

    def _validate_oracle_provider_ancestry(
        self,
        event: Event,
        identity: Mapping[str, Any],
    ) -> None:
        """Require the durable request/context/provider chain used by OracleWorker."""

        request = self.get(event.causation_id) if event.causation_id is not None else None
        if request is None or request.type is not EventType.ORACLE_REQUEST:
            raise EventIntegrityError(
                "archive-backed oracle.output requires an existing oracle.request"
            )
        if request.actor.kind not in {ActorKind.HOST, ActorKind.SYSTEM}:
            raise EventIntegrityError("oracle.request requires a Host or system actor")
        if (
            request.session_id != event.session_id
            or request.branch_id != event.branch_id
            or request.correlation_id != event.correlation_id
        ):
            raise EventIntegrityError("oracle output request ancestry crosses context boundaries")

        requested_profile = identity.get("requested_model_profile_id")
        if (
            not isinstance(requested_profile, str)
            or not requested_profile
            or request.payload.get("model_profile_id") != requested_profile
            or event.payload.get("model_profile_id") != requested_profile
            or event.actor.id != requested_profile
        ):
            raise EventIntegrityError(
                "oracle output actor and requested model profile do not agree"
            )

        context_hash = event.payload.get("context_hash")
        matching_contexts = [
            candidate
            for candidate in self.list_events(
                event_type=EventType.ORACLE_CONTEXT_BUILT,
                session_id=event.session_id,
                branch_id=event.branch_id,
            )
            if candidate.causation_id == request.id
            and candidate.payload.get("sha256") == context_hash
        ]
        if len(matching_contexts) != 1:
            raise EventIntegrityError(
                "archive-backed oracle.output requires exactly one matching context"
            )
        context = matching_contexts[0]
        if (
            context.actor.kind is not ActorKind.SYSTEM
            or context.parent_event_id != request.id
            or context.correlation_id != event.correlation_id
        ):
            raise EventIntegrityError("oracle output context ancestry is inconsistent")
        messages = context.payload.get("messages")
        source_ids = context.payload.get("source_event_ids")
        if (
            not isinstance(messages, Sequence)
            or isinstance(messages, (str, bytes, bytearray))
            or any(not isinstance(message, Mapping) for message in messages)
            or not isinstance(source_ids, Sequence)
            or isinstance(source_ids, (str, bytes, bytearray))
            or len(source_ids) != len(messages)
            or any(not isinstance(source_id, str) or not source_id for source_id in source_ids)
            or sha256_json(thaw_json(messages)) != context_hash
        ):
            raise EventIntegrityError("oracle output context snapshot is invalid")
        for source_id in source_ids:
            source = self.get(str(source_id))
            if source is None or source.session_id != event.session_id:
                raise EventIntegrityError("oracle output context cites an unavailable source")
        recorded_request_hash = request.payload.get("context_hash")
        if recorded_request_hash is not None and recorded_request_hash != context_hash:
            raise EventIntegrityError("oracle output context differs from its request")

        cursor = self.get(event.parent_event_id) if event.parent_event_id is not None else None
        seen: set[str] = set()
        fallback: Event | None = None
        truncation: Event | None = None
        ancestry_order = -1
        while cursor is not None and cursor.id != context.id:
            if cursor.id in seen:
                raise EventIntegrityError("oracle output ancestry contains a loop")
            seen.add(cursor.id)
            if (
                cursor.session_id != event.session_id
                or cursor.branch_id != event.branch_id
                or cursor.correlation_id != event.correlation_id
                or cursor.causation_id != request.id
                or cursor.actor.kind is not ActorKind.SYSTEM
            ):
                raise EventIntegrityError("oracle output provider ancestry is inconsistent")
            if cursor.type is EventType.ORACLE_PROVIDER_FALLBACK and fallback is None:
                if ancestry_order >= 0:
                    raise EventIntegrityError("oracle output provider ancestry order is invalid")
                fallback = cursor
                ancestry_order = 0
            elif cursor.type is EventType.ORACLE_CONTEXT_TRUNCATED and truncation is None:
                if ancestry_order >= 1:
                    raise EventIntegrityError("oracle output provider ancestry order is invalid")
                truncation = cursor
                ancestry_order = 1
            else:
                raise EventIntegrityError("oracle output has an invalid provider ancestry event")
            cursor = (
                self.get(cursor.parent_event_id) if cursor.parent_event_id is not None else None
            )
        if cursor is None:
            raise EventIntegrityError("oracle output parent chain does not reach its context")

        fallback_status = identity.get("fallback_occurred")
        if (fallback_status is True) != (fallback is not None):
            raise EventIntegrityError(
                "oracle output fallback ancestry disagrees with model identity"
            )
        if fallback is not None and (
            fallback.payload.get("actual_provider") != identity.get("actual_provider")
            or fallback.payload.get("provider_model_id") != identity.get("actual_model_identifier")
            or fallback.payload.get("provider_adapter") != event.payload.get("provider_name")
        ):
            raise EventIntegrityError("oracle output fallback event differs from model identity")
        if truncation is not None and truncation.payload.get("context_sha256") != context_hash:
            raise EventIntegrityError("oracle output truncation event differs from its context")

    @staticmethod
    def _validate_new_tool_result(event: Event) -> None:
        if event.type not in {
            EventType.TOOL_OUTPUT,
            EventType.TOOL_ERROR,
            EventType.TOOL_TIMEOUT,
            EventType.TOOL_DENIED,
            EventType.TOOL_VIRTUALIZED,
        }:
            return
        allowed = {"real", "sandbox", "virtual", "retrieved", "synthetic"}
        payload_domain = event.payload.get("truth_domain")
        metadata_domain = event.metadata.get("truth_domain")
        domain = payload_domain or metadata_domain
        if domain not in allowed:
            raise EventIntegrityError("tool result requires a valid truth_domain")
        if (
            payload_domain is not None
            and metadata_domain is not None
            and payload_domain != metadata_domain
        ):
            raise EventIntegrityError("tool result truth_domain labels disagree")

    def get(self, event_id: str) -> Event | None:
        """Return one event by ID, or ``None`` when absent."""
        row = self._connection.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        return None if row is None else self._row_to_event(row)

    def require(self, event_id: str) -> Event:
        """Return one event or raise :class:`EventNotFoundError`."""
        event = self.get(event_id)
        if event is None:
            raise EventNotFoundError(f"event not found: {event_id}")
        return event

    def list_events(
        self,
        *,
        session_id: str | None = None,
        branch_id: str | None = None,
        event_type: EventType | str | Iterable[EventType | str] | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        parent_event_id: str | None = None,
        actor_kind: str | None = None,
        after: str | dt.datetime | None = None,
        before: str | dt.datetime | None = None,
        limit: int | None = None,
        ascending: bool = True,
    ) -> list[Event]:
        """Query events in deterministic ``(created_at, id)`` order.

        ``after`` and ``before`` accept either an event ID (exclusive cursor)
        or a timezone-aware timestamp.
        """
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("session_id", session_id),
            ("branch_id", branch_id),
            ("correlation_id", correlation_id),
            ("causation_id", causation_id),
            ("parent_event_id", parent_event_id),
            ("actor_kind", actor_kind),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)

        if event_type is not None:
            types = [event_type] if isinstance(event_type, (str, EventType)) else list(event_type)
            if not types:
                return []
            values = [
                item.value if isinstance(item, EventType) else EventType(item).value
                for item in types
            ]
            clauses.append(f"type IN ({','.join('?' for _ in values)})")
            params.extend(values)

        self._add_cursor_clause(clauses, params, "after", after)
        self._add_cursor_clause(clauses, params, "before", before)

        if limit is not None and limit < 1:
            raise ValueError("limit must be positive")
        direction = "ASC" if ascending else "DESC"
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM events{where} ORDER BY created_at {direction}, id {direction}"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [self._row_to_event(row) for row in self._connection.execute(sql, params)]

    def _add_cursor_clause(
        self,
        clauses: list[str],
        params: list[Any],
        direction: str,
        cursor: str | dt.datetime | None,
    ) -> None:
        if cursor is None:
            return
        operator = ">" if direction == "after" else "<"
        if isinstance(cursor, str) and cursor.startswith("evt_"):
            anchor = self.require(cursor)
            clauses.append(f"(created_at {operator} ? OR (created_at = ? AND id {operator} ?))")
            stamp = _iso(anchor.created_at)
            params.extend((stamp, stamp, anchor.id))
            return
        if isinstance(cursor, dt.datetime):
            stamp = _iso(cursor)
        else:
            parsed = dt.datetime.fromisoformat(str(cursor).replace("Z", "+00:00"))
            stamp = _iso(parsed)
        clauses.append(f"created_at {operator} ?")
        params.append(stamp)

    def iter_events(self, **filters: Any) -> Iterator[Event]:
        """Iterate over :meth:`list_events`; useful as a stable replay API."""
        yield from self.list_events(**filters)

    def events(self, **filters: Any) -> list[Event]:
        """Compatibility alias for :meth:`list_events`."""
        return self.list_events(**filters)

    def count_events(self, *, event_type: EventType | str | None = None) -> int:
        """Count all events or events of one validated type."""
        if event_type is None:
            row = self._connection.execute("SELECT COUNT(*) FROM events").fetchone()
        else:
            value = (
                event_type.value
                if isinstance(event_type, EventType)
                else EventType(event_type).value
            )
            row = self._connection.execute(
                "SELECT COUNT(*) FROM events WHERE type = ?", (value,)
            ).fetchone()
        return int(row[0])

    def project(self, events: Sequence[Event]) -> None:
        """Apply default projections to already committed events."""
        from oracle_lab.projections import ProjectionManager

        ProjectionManager(self).project(events)

    def rebuild_projections(self) -> None:
        """Drop all derived rows and replay the authoritative event log."""
        from oracle_lab.projections import ProjectionManager

        ProjectionManager(self).rebuild()

    def verify_integrity(self) -> list[str]:
        """Return human-readable integrity problems without changing data."""
        problems: list[str] = []
        result = self._connection.execute("PRAGMA integrity_check").fetchall()
        problems.extend(str(row[0]) for row in result if str(row[0]).lower() != "ok")
        for row in self._connection.execute(
            """
            SELECT id, schema_version, json_extract(metadata_json, '$.schema_version')
            FROM events
            WHERE schema_version != json_extract(metadata_json, '$.schema_version')
            """
        ):
            problems.append(f"schema version mismatch for {row[0]}")
        for column in ("parent_event_id", "causation_id"):
            for row in self._connection.execute(
                f"""
                SELECT child.id, child.{column}
                FROM events child
                LEFT JOIN events parent ON parent.id = child.{column}
                WHERE child.{column} IS NOT NULL AND parent.id IS NULL
                """
            ):
                problems.append(f"dangling {column} {row[1]} on {row[0]}")
        return problems

    def _row_to_event(self, row: sqlite3.Row) -> Event:
        try:
            metadata = json.loads(str(row["metadata_json"]))
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise EventIntegrityError(f"invalid JSON in event {row['id']}") from error
        if metadata.get("schema_version") != row["schema_version"]:
            raise EventIntegrityError(f"schema version mismatch for event {row['id']}")
        try:
            return Event(
                id=row["id"],
                type=row["type"],
                created_at=row["created_at"],
                session_id=row["session_id"],
                branch_id=row["branch_id"],
                parent_event_id=row["parent_event_id"],
                causation_id=row["causation_id"],
                correlation_id=row["correlation_id"],
                actor=Actor(kind=row["actor_kind"], id=row["actor_id"]),
                payload=payload,
                metadata=metadata,
            )
        except (TypeError, ValueError) as error:
            raise EventIntegrityError(f"invalid event row {row['id']}: {error}") from error

    def close(self) -> None:
        """Close a connection owned by this store."""
        if self._owns_connection:
            self._connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


__all__ = [
    "DatabaseVersionError",
    "DuplicateEventError",
    "EventIntegrityError",
    "EventNotFoundError",
    "EventStore",
    "EventStoreError",
    "MigrationDriftError",
    "SchemaMigrationError",
]
