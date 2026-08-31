"""Structured logging, correlation propagation, metrics, and optional tracing."""

from __future__ import annotations

import contextlib
import contextvars
import datetime as dt
import logging
import time
from collections.abc import Iterator, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from oracle_lab.events import Event, EventType
from oracle_lab.ids import new_id
from oracle_lab.usage import UsageKind, UsageService

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


class TraceContext(BaseModel):
    """Identifiers propagated across events, jobs, logs, and spans."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    correlation_id: str
    session_id: str | None = None
    branch_id: str | None = None
    event_id: str | None = None


class MetricsSnapshot(BaseModel):
    """Required platform metrics at one instant."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    captured_at: dt.datetime
    events_per_second: float
    jobs_pending: int
    oracle_latency_ms: float
    host_latency_ms: float
    tool_latency_ms: float
    failures: int
    retries: int
    provider_errors: int
    prompt_tokens: int
    completion_tokens: int
    reasoning_tokens: int
    provider_cost: Decimal
    branch_count: int
    sample_group_count: int
    sample_group_size_average: float
    sample_group_size_max: int
    contradiction_count: int
    human_keeps: int
    human_rejects: int


_trace_context: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "oracle_lab_trace_context", default=None
)


def current_trace_context() -> TraceContext | None:
    """Return the context currently bound to this thread/task."""
    return _trace_context.get()


@contextlib.contextmanager
def bind_trace_context(
    *,
    correlation_id: str | None = None,
    session_id: str | None = None,
    branch_id: str | None = None,
    event_id: str | None = None,
) -> Iterator[TraceContext]:
    """Bind correlation identifiers for nested event/job construction."""
    parent = current_trace_context()
    context = TraceContext(
        correlation_id=(
            correlation_id or (None if parent is None else parent.correlation_id) or new_id("corr")
        ),
        session_id=session_id if session_id is not None else getattr(parent, "session_id", None),
        branch_id=branch_id if branch_id is not None else getattr(parent, "branch_id", None),
        event_id=event_id,
    )
    token = _trace_context.set(context)
    try:
        yield context
    finally:
        _trace_context.reset(token)


@contextlib.contextmanager
def bind_event(event: Event) -> Iterator[TraceContext]:
    """Bind the identifiers of one event while handling downstream work."""
    with bind_trace_context(
        correlation_id=event.correlation_id or new_id("corr"),
        session_id=event.session_id,
        branch_id=event.branch_id,
        event_id=event.id,
    ) as context:
        yield context


class ObservabilityService:
    """Read metrics/traces and emit structured platform log records."""

    def __init__(
        self,
        store: EventStore,
        *,
        logger: logging.Logger | None = None,
        tracer: Any | None = None,
    ) -> None:
        self.store = store
        self.logger = logger or logging.getLogger("oracle_lab")
        self.tracer = tracer if tracer is not None else _optional_otel_tracer()

    def log_event(
        self,
        event: Event,
        *,
        level: int = logging.INFO,
        message: str | None = None,
        fields: Mapping[str, Any] | None = None,
    ) -> None:
        """Emit one structured log record with stable correlation fields."""
        data = {
            "event_id": event.id,
            "event_type": event.type.value,
            "correlation_id": event.correlation_id,
            "causation_id": event.causation_id,
            "session_id": event.session_id,
            "branch_id": event.branch_id,
            "actor_kind": event.actor.kind.value,
            "actor_id": event.actor.id,
            **dict(fields or {}),
        }
        self.logger.log(
            level,
            message or event.type.value,
            extra={"oracle_lab": data},
        )

    def correlation_trace(self, correlation_id: str) -> list[Event]:
        """Return the full deterministic event chain for one cycle."""
        return self.store.list_events(correlation_id=correlation_id)

    @contextlib.contextmanager
    def span(self, name: str, *, event: Event | None = None) -> Iterator[Any]:
        """Start an OpenTelemetry span when installed, otherwise a no-op span."""
        attributes: dict[str, Any] = {}
        context = current_trace_context()
        if context is not None:
            attributes.update(
                {
                    "oracle.correlation_id": context.correlation_id,
                    "oracle.session_id": context.session_id or "",
                    "oracle.branch_id": context.branch_id or "",
                }
            )
        if event is not None:
            attributes.update({"oracle.event_id": event.id, "oracle.event_type": event.type.value})
        if self.tracer is None:
            yield None
            return
        with self.tracer.start_as_current_span(name, attributes=attributes) as span:
            yield span

    @contextlib.contextmanager
    def operation(
        self,
        name: str,
        *,
        event: Event | None = None,
        fields: Mapping[str, Any] | None = None,
    ) -> Iterator[Any]:
        """Trace and structurally log one orchestration operation.

        The wrapper is deliberately observational: it never catches, retries,
        reroutes, or otherwise changes the operation's result.
        """

        started = time.monotonic()
        base = self._operation_fields(name, event=event, fields=fields)
        self.logger.info(
            f"{name}.started",
            extra={"oracle_lab": {**base, "status": "started"}},
        )
        span: Any | None = None
        try:
            with self.span(name, event=event) as span:
                yield span
        except Exception as error:
            elapsed_ms = (time.monotonic() - started) * 1000
            if span is not None:
                record_exception = getattr(span, "record_exception", None)
                if callable(record_exception):
                    record_exception(error)
                set_attribute = getattr(span, "set_attribute", None)
                if callable(set_attribute):
                    set_attribute("oracle.status", "error")
                    set_attribute("oracle.elapsed_ms", elapsed_ms)
            self.logger.exception(
                f"{name}.failed",
                extra={
                    "oracle_lab": {
                        **base,
                        "status": "failed",
                        "elapsed_ms": elapsed_ms,
                        "error_type": type(error).__name__,
                    }
                },
            )
            raise
        else:
            elapsed_ms = (time.monotonic() - started) * 1000
            if span is not None:
                set_attribute = getattr(span, "set_attribute", None)
                if callable(set_attribute):
                    set_attribute("oracle.status", "completed")
                    set_attribute("oracle.elapsed_ms", elapsed_ms)
            self.logger.info(
                f"{name}.completed",
                extra={
                    "oracle_lab": {
                        **base,
                        "status": "completed",
                        "elapsed_ms": elapsed_ms,
                    }
                },
            )

    @staticmethod
    def _operation_fields(
        name: str,
        *,
        event: Event | None,
        fields: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        context = current_trace_context()
        correlation_id = (
            event.correlation_id if event is not None else getattr(context, "correlation_id", None)
        )
        session_id = event.session_id if event is not None else getattr(context, "session_id", None)
        branch_id = event.branch_id if event is not None else getattr(context, "branch_id", None)
        return {
            "operation": name,
            "event_id": None if event is None else event.id,
            "event_type": None if event is None else event.type.value,
            "correlation_id": correlation_id,
            "session_id": session_id,
            "branch_id": branch_id,
            **dict(fields or {}),
        }

    def metrics(self, *, window_seconds: float | None = None) -> MetricsSnapshot:
        """Calculate required metrics from events, jobs, and usage projections."""
        now = dt.datetime.now(dt.UTC)
        events = self.store.list_events()
        if window_seconds is not None:
            if window_seconds <= 0:
                raise ValueError("window_seconds must be positive")
            cutoff = now - dt.timedelta(seconds=window_seconds)
            rate_events = [event for event in events if event.created_at >= cutoff]
            elapsed = window_seconds
        elif len(events) > 1:
            elapsed = max(1.0, (events[-1].created_at - events[0].created_at).total_seconds())
            rate_events = events
        else:
            elapsed = 1.0
            rate_events = events

        def count(*types: EventType) -> int:
            selected = set(types)
            return sum(event.type in selected for event in events)

        pending = self.store.connection.execute(
            "SELECT COUNT(*) FROM jobs WHERE status = 'pending'"
        ).fetchone()
        retry_attempts = self.store.connection.execute(
            "SELECT COALESCE(SUM(MAX(attempts - 1, 0)), 0) FROM jobs"
        ).fetchone()
        branches = self.store.connection.execute("SELECT COUNT(*) FROM branches").fetchone()
        groups = self.store.connection.execute("SELECT COUNT(*) FROM sample_groups").fetchone()
        group_sizes = [
            int(row[0])
            for row in self.store.connection.execute(
                """
                SELECT COUNT(o.output_event_id)
                FROM sample_groups g
                LEFT JOIN sample_outputs o ON o.group_id = g.id
                GROUP BY g.id
                """
            ).fetchall()
        ]
        usage = UsageService(self.store)
        totals = usage.totals()
        oracle_latency = usage.totals(kind=UsageKind.ORACLE).average_latency_ms
        host_latency = usage.totals(kind=UsageKind.HOST).average_latency_ms
        tool_latency = usage.totals(kind=UsageKind.TOOL).average_latency_ms
        return MetricsSnapshot(
            captured_at=now,
            events_per_second=len(rate_events) / elapsed,
            jobs_pending=int(pending[0]),
            oracle_latency_ms=oracle_latency,
            host_latency_ms=host_latency,
            tool_latency_ms=tool_latency,
            failures=count(EventType.ORACLE_ERROR, EventType.TOOL_ERROR, EventType.TOOL_TIMEOUT),
            retries=count(EventType.ORACLE_RETRY) + int(retry_attempts[0]),
            provider_errors=count(EventType.ORACLE_ERROR, EventType.ORACLE_PROVIDER_FALLBACK),
            prompt_tokens=totals.prompt_tokens,
            completion_tokens=totals.completion_tokens,
            reasoning_tokens=totals.reasoning_tokens,
            provider_cost=totals.provider_cost,
            branch_count=int(branches[0]),
            sample_group_count=int(groups[0]),
            sample_group_size_average=(sum(group_sizes) / len(group_sizes) if group_sizes else 0.0),
            sample_group_size_max=max(group_sizes, default=0),
            contradiction_count=count(EventType.ANALYSIS_CONTRADICTION_DETECTED),
            human_keeps=count(EventType.HUMAN_KEEP),
            human_rejects=count(EventType.HUMAN_REJECT),
        )


def _optional_otel_tracer() -> Any | None:
    try:
        from opentelemetry import trace
    except ImportError:
        return None
    return trace.get_tracer("oracle_lab")


__all__ = [
    "MetricsSnapshot",
    "ObservabilityService",
    "TraceContext",
    "bind_event",
    "bind_trace_context",
    "current_trace_context",
]
