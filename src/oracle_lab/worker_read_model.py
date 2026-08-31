"""Read-only query views for worker runs and candidate patches."""

from __future__ import annotations

import json
from typing import Any

from oracle_lab.events import EventType
from oracle_lab.store import EventStore


class WorkerReadModelError(RuntimeError):
    """Raised when a worker read query targets the wrong event domain."""


class WorkerReadModel:
    """Build worker status dictionaries without mutating authoritative state."""

    __slots__ = ("_store",)

    def __init__(self, store: EventStore) -> None:
        self._store = store

    def worker_task_status(self, task_event_id: str) -> dict[str, Any]:
        task = self._store.require(task_event_id)
        if task.type is not EventType.WORKER_TASK_REQUESTED:
            raise WorkerReadModelError("event is not a worker task")
        runs = [
            dict(row)
            for row in self._store.connection.execute(
                "SELECT * FROM worker_runs WHERE task_event_id = ? ORDER BY created_at",
                (task.id,),
            )
        ]
        patches = [
            dict(row)
            for row in self._store.connection.execute(
                """
                SELECT p.* FROM candidate_patches p
                JOIN worker_runs r ON r.run_id = p.worker_run_id
                WHERE r.task_event_id = ? ORDER BY p.created_at
                """,
                (task.id,),
            )
        ]
        return {"task": task.to_dict(), "runs": runs, "patches": patches}

    def patch_show(self, patch_event_id: str) -> dict[str, Any]:
        patch = self._store.require(patch_event_id)
        if patch.type is not EventType.WORKER_PATCH_PROPOSED:
            raise WorkerReadModelError("event is not a candidate patch")
        row = self._store.connection.execute(
            "SELECT * FROM candidate_patches WHERE patch_event_id = ?",
            (patch.id,),
        ).fetchone()
        if row is None:
            raise WorkerReadModelError("candidate patch projection is missing")
        state = dict(row)
        for field_name in ("changed_paths_json", "validation_event_ids_json"):
            raw_value = state.get(field_name)
            if isinstance(raw_value, str):
                state[field_name.removesuffix("_json")] = json.loads(raw_value)
        run = self._store.connection.execute(
            "SELECT * FROM worker_runs WHERE run_id = ?",
            (patch.payload["worker_run_id"],),
        ).fetchone()
        return {
            "patch": patch.to_dict(),
            "state": state,
            "worker_run": None if run is None else dict(run),
        }

    def patch_status(self, patch_event_id: str) -> dict[str, Any]:
        return self.patch_show(patch_event_id)


__all__ = ["WorkerReadModel", "WorkerReadModelError"]
