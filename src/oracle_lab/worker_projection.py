"""Rebuildable projections for untrusted worker runs and candidate patches."""

from __future__ import annotations

import json
import sqlite3

from oracle_lab.events import Event, EventType, thaw_json


class WorkerProjection:
    """Project immutable worker events into query-friendly current state."""

    name = "workers"
    tables = ("worker_runs", "candidate_patches")

    def apply(self, connection: sqlite3.Connection, event: Event) -> None:
        payload = thaw_json(event.payload)
        if event.type is EventType.WORKER_RUN_STARTED:
            connection.execute(
                """
                INSERT INTO worker_runs (
                    run_id, task_event_id, started_event_id, terminal_event_id,
                    adapter_id, status, archive_path, created_at, completed_at
                ) VALUES (?, ?, ?, NULL, ?, 'running', NULL, ?, NULL)
                ON CONFLICT(run_id) DO UPDATE SET
                    task_event_id = excluded.task_event_id,
                    started_event_id = excluded.started_event_id,
                    adapter_id = excluded.adapter_id,
                    status = 'running',
                    archive_path = NULL,
                    completed_at = NULL
                """,
                (
                    payload["run_id"],
                    payload["task_event_id"],
                    event.id,
                    payload.get("adapter_id"),
                    event.created_at.isoformat(),
                ),
            )
            return

        if event.type in {EventType.WORKER_RUN_COMPLETED, EventType.WORKER_RUN_FAILED}:
            status = "completed" if event.type is EventType.WORKER_RUN_COMPLETED else "failed"
            cursor = connection.execute(
                """
                UPDATE worker_runs
                SET terminal_event_id = ?, status = ?, archive_path = ?, completed_at = ?
                WHERE run_id = ? AND task_event_id = ?
                """,
                (
                    event.id,
                    status,
                    payload["archive_path"],
                    event.created_at.isoformat(),
                    payload["run_id"],
                    payload["task_event_id"],
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("terminal worker run has no projected start")
            return

        if event.type is EventType.WORKER_PATCH_PROPOSED:
            status = (
                "imported_historical"
                if event.metadata.get("bundle_import_authority") == "historical_only"
                else "pending_human"
            )
            connection.execute(
                """
                INSERT INTO candidate_patches (
                    patch_event_id, worker_run_id, session_id, branch_id,
                    repository_path, base_commit, patch_sha256,
                    patch_archive_path, changed_paths_json, status,
                    approval_event_id, rejection_event_id, application_event_id,
                    staging_path, validation_status, validation_event_ids_json,
                    last_event_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          NULL, NULL, NULL, NULL, NULL, '[]', ?, ?)
                """,
                (
                    event.id,
                    payload["worker_run_id"],
                    event.session_id,
                    event.branch_id,
                    payload["repository_path"],
                    payload["base_commit"],
                    payload["patch_sha256"],
                    payload["patch_archive_path"],
                    json.dumps(payload.get("changed_paths", []), ensure_ascii=False),
                    status,
                    event.id,
                    event.created_at.isoformat(),
                ),
            )
            return

        if event.type in {EventType.HUMAN_PATCH_APPROVED, EventType.HUMAN_PATCH_REJECTED}:
            approved = event.type is EventType.HUMAN_PATCH_APPROVED
            historical = event.metadata.get("bundle_import_authority") == "historical_only"
            cursor = connection.execute(
                """
                UPDATE candidate_patches
                SET status = ?, approval_event_id = ?, rejection_event_id = ?,
                    last_event_id = ?
                WHERE patch_event_id = ? AND status = ?
                """,
                (
                    "imported_historical" if historical else "approved" if approved else "rejected",
                    event.id if approved else None,
                    None if approved else event.id,
                    event.id,
                    payload["patch_event_id"],
                    "imported_historical" if historical else "pending_human",
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("candidate patch is no longer awaiting human judgment")
            return

        if event.type in {EventType.WORKER_PATCH_APPLIED, EventType.WORKER_PATCH_CONFLICT}:
            applied = event.type is EventType.WORKER_PATCH_APPLIED
            historical = event.metadata.get("bundle_import_authority") == "historical_only"
            cursor = connection.execute(
                """
                UPDATE candidate_patches
                SET status = ?, application_event_id = ?, staging_path = ?,
                    last_event_id = ?
                WHERE patch_event_id = ? AND status = ?
                """,
                (
                    "imported_historical" if historical else "applied" if applied else "conflicted",
                    event.id,
                    payload.get("staging_path"),
                    event.id,
                    payload["patch_event_id"],
                    "imported_historical" if historical else "approved",
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("candidate patch is not in the approved state")
            return

        if event.type in {
            EventType.WORKER_VALIDATION_COMPLETED,
            EventType.WORKER_VALIDATION_FAILED,
        }:
            row = connection.execute(
                """
                SELECT application_event_id, validation_status, validation_event_ids_json
                FROM candidate_patches WHERE patch_event_id = ?
                """,
                (payload["patch_event_id"],),
            ).fetchone()
            if row is None:
                raise ValueError("validation references an unknown candidate patch")
            if row["application_event_id"] != payload.get("application_event_id"):
                raise ValueError("validation references another patch application")
            event_ids = json.loads(row["validation_event_ids_json"])
            if row["validation_status"] is not None or event_ids:
                raise ValueError("patch application already has a validation terminal")
            event_ids.append(event.id)
            cursor = connection.execute(
                """
                UPDATE candidate_patches
                SET validation_status = ?, validation_event_ids_json = ?,
                    last_event_id = ?
                WHERE patch_event_id = ? AND application_event_id = ?
                    AND validation_status IS NULL
                """,
                (
                    "passed" if event.type is EventType.WORKER_VALIDATION_COMPLETED else "failed",
                    json.dumps(event_ids, ensure_ascii=False),
                    event.id,
                    payload["patch_event_id"],
                    payload.get("application_event_id"),
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("patch application validation state changed concurrently")


__all__ = ["WorkerProjection"]
