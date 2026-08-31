"""Oracle request worker: context evidence, provider call, raw archive, events."""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from oracle_lab.archive import ArchiveError, ArchiveRecord, RawResponseArchive
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.ids import new_id
from oracle_lab.providers import (
    OracleGenerateRequest,
    OracleGenerateResponse,
    OracleProvider,
    ProviderError,
    ProviderHTTPError,
    thaw_provider_value,
)
from oracle_lab.session import (
    BuiltContext,
    ContextConstructionError,
    validate_built_context_sources,
)


class EventStoreLike(Protocol):
    def append(self, event: Event) -> Event: ...

    def get(self, event_id: str) -> Event | None: ...

    def append_many(self, events: list[Event] | tuple[Event, ...]) -> tuple[Event, ...]: ...

    def list_events(self, **filters: Any) -> list[Event]: ...


Sleep = Callable[[float], Awaitable[None]]


class OracleWorkerError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OracleRunResult:
    output_event: Event
    context_event: Event | None
    archive: ArchiveRecord | None
    usage_event: Event
    fallback_event: Event | None
    attempts: int
    truncation_event: Event | None = None


def _usage_value(usage: Mapping[str, Any], key: str, default: Any = 0) -> Any:
    value = usage.get(key, default)
    return default if value is None else value


class OracleWorker:
    """The only component that turns a provider response into oracle events."""

    def __init__(
        self,
        provider: OracleProvider,
        archive: RawResponseArchive,
        event_store: EventStoreLike,
        *,
        max_retries: int = 0,
        retry_base_seconds: float = 1.0,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if max_retries < 0 or retry_base_seconds < 0:
            raise ValueError("retry settings must not be negative")
        self.provider = provider
        self.archive = archive
        self.event_store = event_store
        self.max_retries = max_retries
        self.retry_base_seconds = retry_base_seconds
        self.sleep = sleep
        self.last_run: OracleRunResult | None = None

    async def run(
        self,
        request_event: Event,
        request: OracleGenerateRequest,
        context: BuiltContext,
    ) -> Event:
        """Generate and return ``oracle.output``; details remain in ``last_run``.

        The request event must already be authoritative.  The exact provider
        messages must equal a cited BuiltContext; a caller-supplied hash alone
        is never enough to authorize a fresh provider call.
        """

        if request_event.type != EventType.ORACLE_REQUEST:
            raise OracleWorkerError("OracleWorker requires an oracle.request event")
        if self.event_store.get(request_event.id) is None:
            raise OracleWorkerError("oracle.request must be appended before provider execution")
        if context is None:
            raise OracleWorkerError("oracle generation requires a BuiltContext")
        if (
            context.session_id != request_event.session_id
            or context.branch_id != request_event.branch_id
        ):
            raise OracleWorkerError("built context belongs to another session or branch")
        if len(context.source_event_ids) != len(context.messages):
            raise OracleWorkerError("built context must cite one source event per message")
        for source_event_id in context.source_event_ids:
            source_event = self.event_store.get(source_event_id)
            if source_event is None or source_event.session_id != request_event.session_id:
                raise OracleWorkerError(
                    "built context cites a missing event or an event from another session"
                )
        context_evidence_events = self.event_store.list_events(session_id=request_event.session_id)
        if hasattr(self.event_store, "connection"):
            from oracle_lab.branching import BranchService

            context_evidence_events = BranchService(self.event_store).visible_events(
                str(request_event.branch_id),
                until_event_id=request_event.id,
            )
            visible_ids = {event.id for event in context_evidence_events}
            outside_branch = [
                event_id
                for event_id in (
                    *context.source_event_ids,
                    *context.truncated_source_event_ids,
                )
                if event_id not in visible_ids
            ]
            if outside_branch:
                raise OracleWorkerError(
                    "built context cites events outside the request branch history"
                )
        try:
            context_policy = request_event.payload.get("context_policy")
            include_reasoning = (
                context_policy.get("include_reasoning_in_next_turn")
                if isinstance(context_policy, Mapping)
                else None
            )
            validate_built_context_sources(
                context,
                context_evidence_events,
                model_profile_id=request.model_profile_id,
                include_reasoning=(
                    include_reasoning if isinstance(include_reasoning, bool) else None
                ),
            )
        except ContextConstructionError as error:
            raise OracleWorkerError(str(error)) from error
        if context.provider_messages() != [
            thaw_provider_value(message) for message in request.messages
        ]:
            raise OracleWorkerError("provider request messages differ from the recorded context")
        recorded_hash = request_event.payload.get("context_hash")
        if recorded_hash is not None and recorded_hash != context.sha256:
            raise OracleWorkerError("oracle.request context_hash differs from the built context")

        prior_outputs = self.event_store.list_events(
            event_type=EventType.ORACLE_OUTPUT,
            causation_id=request_event.id,
        )
        if prior_outputs:
            # A completed request has one authoritative output. Retrying local
            # post-processing must not generate or bill that request again.
            return prior_outputs[0]

        context_event = self._append_context_event(request_event, context)
        truncation_event = (
            self._append_truncation_event(request_event, context_event, context)
            if context.truncated
            else None
        )
        attempts = 0
        response: OracleGenerateResponse | None = None
        while response is None:
            attempts += 1
            attempt_started = time.monotonic()
            try:
                response = await self.provider.generate(request)
            except ProviderHTTPError as exc:
                retrying = attempts <= self.max_retries
                self._append_attempt_failure(
                    request_event,
                    request,
                    exc,
                    attempts,
                    retrying=retrying,
                    elapsed_ms=exc.elapsed_ms,
                )
                if not retrying:
                    raise
                await self.sleep(self.retry_base_seconds * (2 ** (attempts - 1)))
            except ProviderError as exc:
                retrying = attempts <= self.max_retries
                self._append_attempt_failure(
                    request_event,
                    request,
                    exc,
                    attempts,
                    retrying=retrying,
                    elapsed_ms=(time.monotonic() - attempt_started) * 1000,
                )
                if not retrying:
                    raise
                await self.sleep(self.retry_base_seconds * (2 ** (attempts - 1)))

        output_id = new_id("evt")
        created_at = dt.datetime.now(dt.UTC)
        archive_record: ArchiveRecord | None = None
        if response.material_origin != "synthetic_fixture":
            try:
                archive_record = self.archive.archive_response(
                    event_id=output_id,
                    request=request,
                    response=response,
                    created_at=created_at,
                )
            except ArchiveError as exc:
                # Genuine and historical oracle material is never committed
                # without its authoritative raw bytes.
                self._append_error(request_event, exc, attempts)
                raise OracleWorkerError("provider succeeded but raw archive failed") from exc

        parent_id = (
            truncation_event.id
            if truncation_event is not None
            else context_event.id
            if context_event is not None
            else request_event.id
        )
        fallback_event: Event | None = None
        actual_provider = (
            response.routed_provider_name
            if request.provider_pin
            else response.routed_provider_name or response.provider_name
        )
        fallback_occurred: bool | None = False
        if request.provider_pin and response.routed_provider_name is None:
            fallback_occurred = None
        elif request.provider_pin:
            fallback_occurred = (
                response.routed_provider_name.casefold() != request.provider_pin.casefold()
            )
        if fallback_occurred is True:
            fallback_event = Event.new(
                EventType.ORACLE_PROVIDER_FALLBACK,
                actor=Actor(kind=ActorKind.SYSTEM, id="oracle-worker"),
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                parent_event_id=parent_id,
                causation_id=request_event.id,
                correlation_id=request_event.correlation_id,
                payload={
                    "requested_provider": request.provider_pin,
                    "actual_provider": response.routed_provider_name,
                    "provider_adapter": response.provider_name,
                    "provider_model_id": response.provider_model_id,
                },
            )
            parent_id = fallback_event.id
        request_sampling = {
            key: request_event.payload.get(key)
            for key in (
                "sample_group_id",
                "sample_ordinal",
                "context_hash",
                "sampling",
                "host_classifications",
                "automation_depth",
                "automation_budget_remaining",
                "automation_loop_detector",
                "loop_signature",
            )
            if request_event.payload.get(key) is not None
        }
        request_metadata = thaw_provider_value(request.metadata)
        requested_slug = request_metadata.get("requested_model_slug")
        if requested_slug is None:
            requested_slug = response.generation_settings.get("model")
        context_hash = context.sha256
        model_identity = {
            "requested_model_profile_id": request.model_profile_id,
            "requested_model_slug": requested_slug,
            "model_family": request_metadata.get("model_family"),
            "checkpoint": request_metadata.get("checkpoint"),
            "runtime": request_metadata.get("runtime"),
            "quantization": request_metadata.get("quantization"),
            "requested_provider_id": request_metadata.get("requested_provider_id"),
            "provider_routing": request_metadata.get("provider_routing"),
            "actual_provider": actual_provider,
            "actual_model_identifier": response.provider_model_id,
            "fallback_occurred": fallback_occurred,
        }
        model_identity["unknown_fields"] = sorted(
            key for key, value in model_identity.items() if value is None
        )
        api_response_metadata = {
            "http_status": response.status_code,
            "http_headers": thaw_provider_value(response.headers),
            "provider_request_id": response.request_id,
            "api_revision": response.api_revision,
            "generation_settings": thaw_provider_value(response.generation_settings),
            "provider_adapter": response.provider_name,
            "routed_provider_name": response.routed_provider_name,
        }
        output = Event(
            id=output_id,
            created_at=created_at,
            type=EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id=request.model_profile_id),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=parent_id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "content": response.content,
                "reasoning": response.reasoning,
                "model_profile_id": request.model_profile_id,
                "model": response.provider_model_id or request.model_profile_id,
                "provider": response.provider_name,
                "provider_name": response.provider_name,
                "routed_provider_name": response.routed_provider_name,
                "provider_model_id": response.provider_model_id,
                "sampling": {
                    "temperature": request.temperature,
                    "top_p": request.top_p,
                    "max_tokens": request.max_tokens,
                    "provider_pin": request.provider_pin,
                    "seed": request.seed,
                },
                "effective_sampling": thaw_provider_value(response.generation_settings),
                "finish_reason": response.finish_reason,
                "usage": dict(response.usage),
                "elapsed_ms": response.elapsed_ms,
                "latency_ms": response.elapsed_ms,
                "provider_cost": response.usage.get("cost"),
                "archive_path": (None if archive_record is None else str(archive_record.raw_path)),
                "archive_sha256": None if archive_record is None else archive_record.sha256,
                "archive_size_bytes": (
                    None if archive_record is None else archive_record.size_bytes
                ),
                "provider_request_id": response.request_id,
                "api_revision": response.api_revision,
                "api_response_metadata": api_response_metadata,
                "material_origin": response.material_origin,
                "model_identity": model_identity,
                "context_hash": context_hash,
                **request_sampling,
            },
            metadata={
                "schema_version": 1,
                "material_origin": response.material_origin,
            },
        )
        usage_event = self._make_usage(request_event, request, response, output)
        batch = (
            (output, usage_event)
            if fallback_event is None
            else (
                fallback_event,
                output,
                usage_event,
            )
        )
        self.event_store.append_many(batch)
        self.last_run = OracleRunResult(
            output,
            context_event,
            archive_record,
            usage_event,
            fallback_event,
            attempts,
            truncation_event,
        )
        return output

    def _append_context_event(self, request_event: Event, context: BuiltContext) -> Event:
        event = Event.new(
            EventType.ORACLE_CONTEXT_BUILT,
            actor=Actor(kind=ActorKind.SYSTEM, id="session-context-builder"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=request_event.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload=context.event_payload(),
        )
        return self.event_store.append(event)

    def _append_truncation_event(
        self,
        request_event: Event,
        context_event: Event,
        context: BuiltContext,
    ) -> Event:
        event = Event.new(
            EventType.ORACLE_CONTEXT_TRUNCATED,
            actor=Actor(kind=ActorKind.SYSTEM, id="session-context-builder"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=context_event.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "strategy": context.truncation_strategy,
                "original_message_count": context.original_message_count,
                "retained_message_count": len(context.messages),
                "removed_source_event_ids": list(context.truncated_source_event_ids),
                "retained_source_event_ids": list(context.source_event_ids),
                "context_sha256": context.sha256,
            },
        )
        return self.event_store.append(event)

    def _make_usage(
        self,
        request_event: Event,
        request: OracleGenerateRequest,
        response: OracleGenerateResponse,
        output: Event,
    ) -> Event:
        usage = dict(response.usage)
        completion_details = usage.get("completion_tokens_details")
        details = dict(completion_details) if isinstance(completion_details, Mapping) else {}
        event = Event.new(
            EventType.USAGE_ORACLE,
            actor=Actor(kind=ActorKind.SYSTEM, id="oracle-worker"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=output.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "request_event_id": request_event.id,
                "output_event_id": output.id,
                "provider_id": response.provider_name,
                "model_id": response.provider_model_id or request.model_profile_id,
                "tool_id": None,
                "prompt_tokens": int(_usage_value(usage, "prompt_tokens", 0)),
                "completion_tokens": int(_usage_value(usage, "completion_tokens", 0)),
                "reasoning_tokens": details.get("reasoning_tokens"),
                "provider_cost": usage.get("cost"),
                "latency_ms": max(0.0, response.elapsed_ms),
                "ttft_ms": usage.get("ttft_ms"),
                "request_count": 1,
            },
        )
        return event

    def _append_retry(self, request_event: Event, error: Exception, attempt: int) -> Event:
        event = Event.new(
            EventType.ORACLE_RETRY,
            actor=Actor(kind=ActorKind.SYSTEM, id="oracle-worker"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=request_event.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "attempt": attempt,
                "next_attempt": attempt + 1,
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )
        return self.event_store.append(event)

    def _append_error(self, request_event: Event, error: Exception, attempts: int) -> Event:
        event = Event.new(
            EventType.ORACLE_ERROR,
            actor=Actor(kind=ActorKind.SYSTEM, id="oracle-worker"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=request_event.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "attempts": attempts,
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )
        return self.event_store.append(event)

    def _append_attempt_failure(
        self,
        request_event: Event,
        request: OracleGenerateRequest,
        error: ProviderError,
        attempt: int,
        *,
        retrying: bool,
        elapsed_ms: float,
    ) -> tuple[Event, Event]:
        failure_id = new_id("evt")
        archive_record: ArchiveRecord | None = None
        if isinstance(error, ProviderHTTPError):
            archive_record = self.archive.write(
                event_id=failure_id,
                raw_bytes=error.raw_bytes,
                metadata={
                    "kind": "provider_http_error",
                    "request_event_id": request_event.id,
                    "request_sha256": request.request_hash,
                    "attempt": attempt,
                    "retrying": retrying,
                    "http_status": error.status_code,
                    "http_headers": dict(error.headers),
                    "response_timing_ms": error.elapsed_ms,
                },
            )
        failure = Event(
            id=failure_id,
            type=EventType.ORACLE_RETRY if retrying else EventType.ORACLE_ERROR,
            actor=Actor(kind=ActorKind.SYSTEM, id="oracle-worker"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=request_event.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "attempt": attempt,
                "next_attempt": attempt + 1 if retrying else None,
                "error_type": type(error).__name__,
                "error": str(error),
                "archive_path": str(archive_record.raw_path) if archive_record else None,
                "archive_sha256": archive_record.sha256 if archive_record else None,
                "archive_size_bytes": archive_record.size_bytes if archive_record else None,
            },
        )
        usage = Event.new(
            EventType.USAGE_ORACLE,
            actor=Actor(kind=ActorKind.SYSTEM, id="oracle-worker"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=failure.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "request_event_id": request_event.id,
                "provider_id": type(self.provider).__name__,
                "model_id": request.model_profile_id,
                "tool_id": None,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "reasoning_tokens": None,
                "provider_cost": None,
                "latency_ms": max(0.0, elapsed_ms),
                "ttft_ms": None,
                "request_count": 1,
                "status": "retry" if retrying else "error",
                "error_type": type(error).__name__,
            },
        )
        self.event_store.append_many((failure, usage))
        return failure, usage


__all__ = ["OracleRunResult", "OracleWorker", "OracleWorkerError"]
