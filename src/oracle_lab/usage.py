"""Event-backed token, latency, and provider-cost accounting."""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import math
import sqlite3
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from oracle_lab.events import Actor, ActorKind, Event, EventType

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


class UsageKind(StrEnum):
    """Kinds of billable or measurable execution."""

    ORACLE = "oracle"
    HOST = "host"
    TOOL = "tool"

    @property
    def event_type(self) -> EventType:
        return EventType(f"usage.{self.value}")


class UsageRecord(BaseModel):
    """One projected usage event."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    kind: UsageKind
    request_event_id: str | None = None
    session_id: str | None = None
    branch_id: str | None = None
    correlation_id: str | None = None
    provider_id: str | None = None
    model_id: str | None = None
    tool_id: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_tokens: int | None = None
    provider_cost: Decimal | None = None
    latency_ms: float = 0.0
    ttft_ms: float | None = None
    request_count: int = 1
    created_at: dt.datetime


class UsageTotals(BaseModel):
    """An aggregate calculated only from immutable usage events."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_tokens: int = 0
    provider_cost: Decimal = Decimal("0")
    request_count: int = 0
    latency_ms_total: float = 0.0
    ttft_ms_total: float = 0.0
    ttft_count: int = 0
    by_kind: dict[str, int] = Field(default_factory=dict)

    @property
    def average_latency_ms(self) -> float:
        return self.latency_ms_total / self.request_count if self.request_count else 0.0

    @property
    def average_ttft_ms(self) -> float | None:
        return self.ttft_ms_total / self.ttft_count if self.ttft_count else None


class UsageProjection:
    """Projection plugin for ``usage.*`` events."""

    name = "usage"
    tables = ("usage_records",)

    def apply(self, connection: sqlite3.Connection, event: Event) -> None:
        if not event.type.value.startswith("usage."):
            return
        payload = event.payload
        kind = UsageKind(event.type.value.split(".", 1)[1])
        values = {
            "prompt_tokens": _non_negative_int(payload.get("prompt_tokens", 0), "prompt_tokens"),
            "completion_tokens": _non_negative_int(
                payload.get("completion_tokens", 0), "completion_tokens"
            ),
            "reasoning_tokens": _optional_non_negative_int(
                payload.get("reasoning_tokens"), "reasoning_tokens"
            ),
            "latency_ms": _non_negative_float(payload.get("latency_ms", 0), "latency_ms"),
            "ttft_ms": _optional_non_negative_float(payload.get("ttft_ms"), "ttft_ms"),
            "request_count": _non_negative_int(payload.get("request_count", 1), "request_count"),
        }
        provider_cost = payload.get("provider_cost")
        if provider_cost is not None and Decimal(str(provider_cost)) < 0:
            raise ValueError("provider_cost may not be negative")
        connection.execute(
            """
            INSERT OR REPLACE INTO usage_records (
                event_id, kind, request_event_id, session_id, branch_id,
                correlation_id, provider_id, model_id, tool_id,
                prompt_tokens, completion_tokens, reasoning_tokens,
                provider_cost, latency_ms, ttft_ms, request_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                kind.value,
                payload.get("request_event_id") or event.causation_id,
                event.session_id,
                event.branch_id,
                event.correlation_id,
                payload.get("provider_id"),
                payload.get("model_id"),
                payload.get("tool_id"),
                values["prompt_tokens"],
                values["completion_tokens"],
                values["reasoning_tokens"],
                None if provider_cost is None else str(Decimal(str(provider_cost))),
                values["latency_ms"],
                values["ttft_ms"],
                values["request_count"],
                event.created_at.isoformat(),
            ),
        )


class UsageService:
    """Emit usage events and calculate telemetry aggregates.

    Aggregates are observational only; this service exposes no dispatch or
    sampling policy hook, preventing cost from becoming a creativity limiter.
    """

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def record(
        self,
        kind: UsageKind | str,
        *,
        request_event_id: str | None = None,
        actor: Actor | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
        correlation_id: str | None = None,
        provider_id: str | None = None,
        model_id: str | None = None,
        tool_id: str | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        reasoning_tokens: int | None = None,
        provider_cost: Decimal | str | float | None = None,
        latency_ms: float = 0.0,
        ttft_ms: float | None = None,
        request_count: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> UsageRecord:
        """Append one ``usage.*`` event for a single call or attempt."""
        usage_kind = kind if isinstance(kind, UsageKind) else UsageKind(kind)
        request = self.store.require(request_event_id) if request_event_id is not None else None
        prompt_tokens = _non_negative_int(prompt_tokens, "prompt_tokens")
        completion_tokens = _non_negative_int(completion_tokens, "completion_tokens")
        reasoning_tokens = _optional_non_negative_int(reasoning_tokens, "reasoning_tokens")
        latency_ms = _non_negative_float(latency_ms, "latency_ms")
        ttft_ms = _optional_non_negative_float(ttft_ms, "ttft_ms")
        request_count = _non_negative_int(request_count, "request_count")
        cost = None if provider_cost is None else Decimal(str(provider_cost))
        if cost is not None and cost < 0:
            raise ValueError("provider_cost may not be negative")
        event = Event(
            type=usage_kind.event_type,
            actor=actor or Actor(kind=ActorKind.SYSTEM, id="usage"),
            session_id=(
                session_id if session_id is not None else getattr(request, "session_id", None)
            ),
            branch_id=branch_id if branch_id is not None else getattr(request, "branch_id", None),
            causation_id=request_event_id,
            correlation_id=(
                correlation_id
                if correlation_id is not None
                else getattr(request, "correlation_id", None)
            ),
            payload={
                "request_event_id": request_event_id,
                "provider_id": provider_id,
                "model_id": model_id,
                "tool_id": tool_id,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "reasoning_tokens": reasoning_tokens,
                "provider_cost": None if cost is None else str(cost),
                "latency_ms": latency_ms,
                "ttft_ms": ttft_ms,
                "request_count": request_count,
            },
            metadata={"schema_version": 1, **(metadata or {})},
        )
        self.store.append(event)
        return self.get(event.id)

    def record_host_call(
        self,
        *,
        request_event_id: str,
        result: Any | None = None,
        latency_ms: float | None = None,
        provider_id: str | None = None,
        model_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> UsageRecord:
        """Record one host model/agent call using telemetry it exposed.

        Direct host adapters commonly return an ``output`` mapping containing
        OpenAI-style ``usage``.  Coding-agent adapters may expose latency only.
        Unknown values remain zero/``None`` rather than being estimated.
        """

        root = _host_result_mapping(result)
        raw_usage = root.get("usage", {})
        usage = raw_usage if isinstance(raw_usage, Mapping) else {}
        details = usage.get("completion_tokens_details")
        if not isinstance(details, Mapping):
            details = usage.get("output_tokens_details")
        if not isinstance(details, Mapping):
            details = {}
        measured_latency = latency_ms
        if measured_latency is None:
            raw_latency = getattr(result, "elapsed_ms", 0.0)
            measured_latency = _optional_non_negative_number(raw_latency) or 0.0
        cost = _first_non_negative_decimal(
            usage,
            "provider_cost",
            "cost",
            "total_cost",
        )
        if cost is None:
            cost = _first_non_negative_decimal(
                root,
                "provider_cost",
                "cost",
                "total_cost",
            )
        reasoning_tokens = _first_optional_non_negative_integer(usage, "reasoning_tokens")
        if reasoning_tokens is None:
            reasoning_tokens = _first_optional_non_negative_integer(details, "reasoning_tokens")
        return self.record(
            UsageKind.HOST,
            request_event_id=request_event_id,
            provider_id=provider_id
            or _first_non_empty_string(root, "provider_id", "provider", "provider_name"),
            model_id=model_id or _first_non_empty_string(root, "model_id", "model"),
            prompt_tokens=_first_non_negative_integer(usage, "prompt_tokens", "input_tokens"),
            completion_tokens=_first_non_negative_integer(
                usage, "completion_tokens", "output_tokens"
            ),
            reasoning_tokens=reasoning_tokens,
            provider_cost=cost,
            latency_ms=measured_latency,
            ttft_ms=_first_optional_non_negative_number(usage, "ttft_ms", "time_to_first_token_ms")
            or _first_optional_non_negative_number(root, "ttft_ms", "time_to_first_token_ms"),
            metadata=metadata,
        )

    def get(self, event_id: str) -> UsageRecord:
        """Return the projected record for a usage event."""
        row = self.store.connection.execute(
            "SELECT * FROM usage_records WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            raise LookupError(f"usage record not found: {event_id}")
        return self._row_to_record(row)

    def list_records(
        self,
        *,
        kind: UsageKind | str | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
        model_id: str | None = None,
    ) -> list[UsageRecord]:
        """List usage records in event order."""
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("session_id", session_id),
            ("branch_id", branch_id),
            ("model_id", model_id),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind.value if isinstance(kind, UsageKind) else UsageKind(kind).value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.store.connection.execute(
            f"SELECT * FROM usage_records{where} ORDER BY created_at, event_id", params
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def totals(self, **filters: Any) -> UsageTotals:
        """Aggregate branch/session/model telemetry from usage events."""
        records = self.list_records(**filters)
        by_kind: dict[str, int] = {}
        for record in records:
            by_kind[record.kind.value] = by_kind.get(record.kind.value, 0) + record.request_count
        return UsageTotals(
            prompt_tokens=sum(record.prompt_tokens for record in records),
            completion_tokens=sum(record.completion_tokens for record in records),
            reasoning_tokens=sum(record.reasoning_tokens or 0 for record in records),
            provider_cost=sum(
                (record.provider_cost or Decimal("0") for record in records), Decimal("0")
            ),
            request_count=sum(record.request_count for record in records),
            latency_ms_total=sum(record.latency_ms for record in records),
            ttft_ms_total=sum(record.ttft_ms or 0.0 for record in records),
            ttft_count=sum(record.ttft_ms is not None for record in records),
            by_kind=by_kind,
        )

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> UsageRecord:
        return UsageRecord(
            event_id=row["event_id"],
            kind=row["kind"],
            request_event_id=row["request_event_id"],
            session_id=row["session_id"],
            branch_id=row["branch_id"],
            correlation_id=row["correlation_id"],
            provider_id=row["provider_id"],
            model_id=row["model_id"],
            tool_id=row["tool_id"],
            prompt_tokens=row["prompt_tokens"],
            completion_tokens=row["completion_tokens"],
            reasoning_tokens=row["reasoning_tokens"],
            provider_cost=(
                None if row["provider_cost"] is None else Decimal(str(row["provider_cost"]))
            ),
            latency_ms=row["latency_ms"],
            ttft_ms=row["ttft_ms"],
            request_count=row["request_count"],
            created_at=dt.datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")),
        )


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _optional_non_negative_int(value: Any, name: str) -> int | None:
    return None if value is None else _non_negative_int(value, name)


def _non_negative_float(value: Any, name: str) -> float:
    result = float(value)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _optional_non_negative_float(value: Any, name: str) -> float | None:
    return None if value is None else _non_negative_float(value, name)


def _host_result_mapping(result: Any | None) -> Mapping[str, Any]:
    output = getattr(result, "output", None)
    if isinstance(output, Mapping):
        explicit_usage = getattr(result, "usage", None)
        if isinstance(explicit_usage, Mapping) and not isinstance(output.get("usage"), Mapping):
            return {**dict(output), "usage": explicit_usage}
        return output
    explicit_usage = getattr(result, "usage", None)
    if isinstance(explicit_usage, Mapping):
        return {"usage": explicit_usage}
    stdout = getattr(result, "stdout", None)
    if not isinstance(stdout, str) or not stdout.strip():
        return {}
    parsed: list[Any] = []
    with contextlib.suppress(json.JSONDecodeError):
        parsed.append(json.loads(stdout))
    if not parsed:
        for line in stdout.splitlines():
            with contextlib.suppress(json.JSONDecodeError):
                parsed.append(json.loads(line))
    matches: list[Mapping[str, Any]] = []

    def collect(value: Any, depth: int = 0) -> None:
        if depth > 6:
            return
        if isinstance(value, Mapping):
            if isinstance(value.get("usage"), Mapping):
                matches.append(value)
            for nested in value.values():
                if isinstance(nested, (Mapping, list, tuple)):
                    collect(nested, depth + 1)
        elif isinstance(value, (list, tuple)):
            for nested in value:
                collect(nested, depth + 1)

    for value in parsed:
        collect(value)
    return matches[-1] if matches else {}


def _first_non_empty_string(values: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = values.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _first_non_negative_integer(values: Mapping[str, Any], *keys: str) -> int:
    return _first_optional_non_negative_integer(values, *keys) or 0


def _first_optional_non_negative_integer(values: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = values.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int) and value >= 0:
            return value
    return None


def _optional_non_negative_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _first_optional_non_negative_number(values: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        number = _optional_non_negative_number(values.get(key))
        if number is not None:
            return number
    return None


def _first_non_negative_decimal(values: Mapping[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = values.get(key)
        if value is None or isinstance(value, bool):
            continue
        try:
            number = Decimal(str(value))
        except (InvalidOperation, ValueError):
            continue
        if number.is_finite() and number >= 0:
            return number
    return None


__all__ = [
    "UsageKind",
    "UsageProjection",
    "UsageRecord",
    "UsageService",
    "UsageTotals",
]
