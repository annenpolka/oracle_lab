"""Explicit sample groups for reproducible oracle experiments."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict

from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.ids import new_id
from oracle_lab.jsonutil import sha256_json

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


class SamplingParams(BaseModel):
    """Provider-neutral sampling controls retained with every experiment."""

    model_config = ConfigDict(frozen=True, extra="allow")

    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | None = None
    seed: int | None = None


class SampleGroup(BaseModel):
    """A projected group of sibling outputs sharing an exact context hash."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    session_id: str | None = None
    branch_id: str | None = None
    from_event_id: str
    context_hash: str
    provider_id: str
    model_id: str
    sampling: dict[str, Any]
    created_event_id: str
    created_at: dt.datetime


class SampleOutput(BaseModel):
    """A sibling oracle output within a sample group."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    group_id: str
    output_event_id: str
    ordinal: int
    latency_ms: float | None = None
    provider_cost: Decimal | None = None
    host_classifications: dict[str, Any] | None = None


class SampleBatch(BaseModel):
    """Result of one synchronous sampling run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    group: SampleGroup
    outputs: tuple[Event, ...]
    errors: tuple[Event, ...] = ()


class SampleGenerator(Protocol):
    """Provider callback accepted by :meth:`SamplingService.sample`."""

    def generate(
        self,
        *,
        context: Sequence[Mapping[str, Any]],
        provider: str,
        model: str,
        sampling: Mapping[str, Any],
        index: int,
    ) -> str | Mapping[str, Any]: ...


class SamplingProjection:
    """Projection plugin for sample groups and their sibling outputs."""

    name = "sampling"
    tables = ("sample_outputs", "sample_groups")

    def apply(self, connection: sqlite3.Connection, event: Event) -> None:
        payload = event.payload
        if event.type is EventType.ORACLE_SAMPLE_GROUP_CREATED:
            required = (
                "group_id",
                "from_event_id",
                "context_hash",
                "provider_id",
                "model_id",
                "sampling",
            )
            missing = [key for key in required if payload.get(key) is None]
            if missing:
                raise ValueError(f"sample group event missing: {', '.join(missing)}")
            if (
                connection.execute(
                    "SELECT 1 FROM events WHERE id = ?", (payload["from_event_id"],)
                ).fetchone()
                is None
            ):
                raise ValueError(f"sample source event does not exist: {payload['from_event_id']}")
            connection.execute(
                """
                INSERT OR REPLACE INTO sample_groups (
                    id, session_id, branch_id, from_event_id, context_hash,
                    provider_id, model_id, sampling_json, created_event_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["group_id"],
                    event.session_id,
                    event.branch_id,
                    payload["from_event_id"],
                    payload["context_hash"],
                    payload["provider_id"],
                    payload["model_id"],
                    json.dumps(
                        thaw_json(payload["sampling"]),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    event.id,
                    event.created_at.isoformat(),
                ),
            )
            return

        if event.type is EventType.ORACLE_OUTPUT and payload.get("sample_group_id") is not None:
            group_id = str(payload["sample_group_id"])
            group = connection.execute(
                "SELECT context_hash FROM sample_groups WHERE id = ?", (group_id,)
            ).fetchone()
            if group is None:
                raise ValueError(f"sample group not found: {group_id}")
            if payload.get("context_hash") != group["context_hash"]:
                raise ValueError(f"sample output context hash differs from group {group_id}")
            connection.execute(
                """
                INSERT OR REPLACE INTO sample_outputs (
                    group_id, output_event_id, ordinal, latency_ms,
                    provider_cost, classification_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    group_id,
                    event.id,
                    int(payload.get("sample_ordinal", 0)),
                    payload.get("latency_ms"),
                    (
                        None
                        if payload.get("provider_cost") is None
                        else str(payload["provider_cost"])
                    ),
                    (
                        None
                        if payload.get("host_classifications") is None
                        or payload.get("material_origin") == "synthetic_fixture"
                        else json.dumps(
                            thaw_json(payload["host_classifications"]),
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    ),
                ),
            )


class SamplingService:
    """Create groups and issue fresh, non-ranked sibling generations."""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def create_group(
        self,
        *,
        from_event_id: str,
        context: Sequence[Mapping[str, Any]],
        provider_id: str,
        model_id: str,
        sampling: SamplingParams | Mapping[str, Any],
        actor: Actor | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
        correlation_id: str | None = None,
        group_id: str | None = None,
    ) -> SampleGroup:
        """Append ``oracle.sample_group_created`` with the exact context hash."""
        source = self.store.require(from_event_id)
        parameters = (
            sampling.model_dump(mode="json", exclude_none=True)
            if isinstance(sampling, SamplingParams)
            else dict(sampling)
        )
        context_value = [dict(message) for message in context]
        context_hash = sha256_json(context_value)
        identifier = group_id or new_id("smp")
        event = Event(
            type=EventType.ORACLE_SAMPLE_GROUP_CREATED,
            actor=actor or Actor(kind=ActorKind.SYSTEM, id="sampling"),
            session_id=session_id if session_id is not None else source.session_id,
            branch_id=branch_id if branch_id is not None else source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=correlation_id or source.correlation_id or new_id("corr"),
            payload={
                "group_id": identifier,
                "from_event_id": source.id,
                "context_hash": context_hash,
                "provider_id": provider_id,
                "model_id": model_id,
                "sampling": parameters,
            },
        )
        self.store.append(event)
        return self.get_group(identifier)

    def sample(
        self,
        *,
        from_event_id: str,
        context: Sequence[Mapping[str, Any]],
        provider_id: str,
        model_id: str,
        sampling: SamplingParams | Mapping[str, Any],
        n: int,
        generator: SampleGenerator | Callable[..., str | Mapping[str, Any]],
        fixture_origin: Literal["synthetic_fixture"],
        actor: Actor | None = None,
        continue_on_error: bool = False,
    ) -> SampleBatch:
        """Generate explicitly synthetic fixtures for structural tests.

        Genuine and historical oracle sampling must cross ``OracleWorker`` and
        its raw-archive boundary.  This callback API is intentionally unable to
        mint genuine oracle material.
        """
        if n < 1:
            raise ValueError("n must be positive")
        if fixture_origin != "synthetic_fixture":
            raise ValueError("callback sampling is restricted to synthetic_fixture material")
        parameters = (
            sampling.model_dump(mode="json", exclude_none=True)
            if isinstance(sampling, SamplingParams)
            else dict(sampling)
        )
        group = self.create_group(
            from_event_id=from_event_id,
            context=context,
            provider_id=provider_id,
            model_id=model_id,
            sampling=parameters,
            actor=actor,
        )
        outputs: list[Event] = []
        errors: list[Event] = []
        context_value = [dict(message) for message in context]
        call = getattr(generator, "generate", generator)
        for index in range(n):
            request = Event(
                type=EventType.ORACLE_REQUEST,
                actor=Actor(kind=ActorKind.SYSTEM, id="sampling"),
                session_id=group.session_id,
                branch_id=group.branch_id,
                parent_event_id=group.created_event_id,
                causation_id=group.created_event_id,
                correlation_id=self.store.require(group.created_event_id).correlation_id,
                payload={
                    "sample_group_id": group.id,
                    "sample_ordinal": index,
                    "context_hash": group.context_hash,
                    "context": context_value,
                    "provider_id": provider_id,
                    "model_id": model_id,
                    "sampling": parameters,
                    "material_origin": fixture_origin,
                },
                metadata={"schema_version": 1, "material_origin": fixture_origin},
            )
            self.store.append(request)
            started = time.perf_counter()
            try:
                result = call(
                    context=context_value,
                    provider=provider_id,
                    model=model_id,
                    sampling=parameters,
                    index=index,
                )
            except Exception as error:
                elapsed_ms = (time.perf_counter() - started) * 1000
                error_event = Event(
                    type=EventType.ORACLE_ERROR,
                    actor=Actor(kind=ActorKind.SYSTEM, id=provider_id),
                    session_id=group.session_id,
                    branch_id=group.branch_id,
                    parent_event_id=request.id,
                    causation_id=request.id,
                    correlation_id=request.correlation_id,
                    payload={
                        "sample_group_id": group.id,
                        "sample_ordinal": index,
                        "error_type": type(error).__name__,
                        "message": str(error),
                        "material_origin": fixture_origin,
                    },
                    metadata={"schema_version": 1, "material_origin": fixture_origin},
                )
                self.store.append(error_event)
                errors.append(error_event)
                if not continue_on_error:
                    raise
                continue

            elapsed_ms = (time.perf_counter() - started) * 1000
            result_payload = {"content": result} if isinstance(result, str) else dict(result)
            result_payload.update(
                {
                    "sample_group_id": group.id,
                    "sample_ordinal": index,
                    "context_hash": group.context_hash,
                    "provider_id": provider_id,
                    "model_id": model_id,
                    "sampling": parameters,
                    "latency_ms": elapsed_ms,
                    "material_origin": fixture_origin,
                    "archive_path": None,
                    "archive_sha256": None,
                    "model_identity": {
                        "requested_model_profile_id": model_id,
                        "requested_model_slug": None,
                        "requested_provider_id": provider_id,
                        "actual_provider": None,
                        "actual_model_identifier": None,
                        "fallback_occurred": None,
                        "unknown_fields": [
                            "requested_model_slug",
                            "actual_provider",
                            "actual_model_identifier",
                            "fallback_occurred",
                        ],
                    },
                }
            )
            output = Event(
                type=EventType.ORACLE_OUTPUT,
                actor=Actor(kind=ActorKind.MODEL, id=model_id),
                session_id=group.session_id,
                branch_id=group.branch_id,
                parent_event_id=request.id,
                causation_id=request.id,
                correlation_id=request.correlation_id,
                payload=result_payload,
                metadata={"schema_version": 1, "material_origin": fixture_origin},
            )
            self.store.append(output)
            outputs.append(output)
        return SampleBatch(group=group, outputs=tuple(outputs), errors=tuple(errors))

    def get_group(self, group_id: str) -> SampleGroup:
        """Return one projected sample group."""
        row = self.store.connection.execute(
            "SELECT * FROM sample_groups WHERE id = ?", (group_id,)
        ).fetchone()
        if row is None:
            raise LookupError(f"sample group not found: {group_id}")
        return self._row_to_group(row)

    def list_groups(
        self,
        *,
        session_id: str | None = None,
        branch_id: str | None = None,
        context_hash: str | None = None,
    ) -> list[SampleGroup]:
        """List experiment groups in creation order."""
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("session_id", session_id),
            ("branch_id", branch_id),
            ("context_hash", context_hash),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.store.connection.execute(
            f"SELECT * FROM sample_groups{where} ORDER BY created_at, id", params
        ).fetchall()
        return [self._row_to_group(row) for row in rows]

    def outputs(self, group_id: str) -> list[SampleOutput]:
        """Return every sibling in ordinal order; no winner is inferred."""
        rows = self.store.connection.execute(
            "SELECT * FROM sample_outputs WHERE group_id = ? ORDER BY ordinal",
            (group_id,),
        ).fetchall()
        return [
            SampleOutput(
                group_id=row["group_id"],
                output_event_id=row["output_event_id"],
                ordinal=row["ordinal"],
                latency_ms=row["latency_ms"],
                provider_cost=(
                    None if row["provider_cost"] is None else Decimal(str(row["provider_cost"]))
                ),
                host_classifications=(
                    None
                    if row["classification_json"] is None
                    else json.loads(row["classification_json"])
                ),
            )
            for row in rows
        ]

    @staticmethod
    def _row_to_group(row: sqlite3.Row) -> SampleGroup:
        return SampleGroup(
            id=row["id"],
            session_id=row["session_id"],
            branch_id=row["branch_id"],
            from_event_id=row["from_event_id"],
            context_hash=row["context_hash"],
            provider_id=row["provider_id"],
            model_id=row["model_id"],
            sampling=json.loads(row["sampling_json"]),
            created_event_id=row["created_event_id"],
            created_at=dt.datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")),
        )


__all__ = [
    "SampleBatch",
    "SampleGenerator",
    "SampleGroup",
    "SampleOutput",
    "SamplingParams",
    "SamplingProjection",
    "SamplingService",
]
