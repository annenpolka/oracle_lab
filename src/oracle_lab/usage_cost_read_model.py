"""Read-only provider-cost queries over projected usage records."""

from __future__ import annotations

from typing import Any

from oracle_lab.store import EventStore


class UsageCostReadModel:
    """Build usage-cost views without owning policy or mutating state."""

    __slots__ = ("_store",)

    def __init__(self, store: EventStore) -> None:
        self._store = store

    def cost_summary(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> dict[str, Any]:
        clauses = []
        parameters: list[Any] = []
        if session_id:
            clauses.append("session_id = ?")
            parameters.append(session_id)
        if model_id:
            clauses.append("model_id = ?")
            parameters.append(model_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        row = self._store.connection.execute(
            f"""
            SELECT COUNT(*) AS records,
                   COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                   COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                   COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
                   COALESCE(SUM(CAST(provider_cost AS REAL)), 0.0) AS provider_cost,
                   COALESCE(SUM(request_count), 0) AS request_count
            FROM usage_records{where}
            """,
            parameters,
        ).fetchone()
        return dict(row)

    def oracle_cost_records(self) -> list[dict[str, Any]]:
        rows = self._store.connection.execute(
            """
            SELECT provider_cost, created_at, session_id
            FROM usage_records
            WHERE kind = 'oracle' AND provider_cost IS NOT NULL
            """
        ).fetchall()
        return [dict(row) for row in rows]


__all__ = ["UsageCostReadModel"]
