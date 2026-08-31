"""Exact orchestration replay and fresh provider/model resampling modes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping, Sequence
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from oracle_lab.archive import RawResponseArchive
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.ids import new_id
from oracle_lab.jsonutil import sha256_json
from oracle_lab.oracle_worker import OracleWorker
from oracle_lab.providers import OracleGenerateRequest, OracleProvider
from oracle_lab.sampling import (
    SampleBatch,
    SamplingParams,
    SamplingService,
)
from oracle_lab.session import BuiltContext, validate_built_context_sources

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


class ReplayMode(StrEnum):
    """Experimentally distinct replay modes from the specification."""

    EXACT = "exact"
    HOST_ANALYSIS = "host_analysis"
    ORACLE_RESAMPLE = "oracle_resample"
    PROVIDER = "provider"
    QUANTIZATION = "quantization"


class ReplayResult(BaseModel):
    """Audit record returned by exact host/orchestration replay."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: ReplayMode
    input_event_ids: tuple[str, ...]
    generated_event_ids: tuple[str, ...] = ()
    replay_event_id: str | None = None
    projections_rebuilt: bool = False


ReplayHandler = Callable[[Event], Event | Iterable[Event] | None]


class ReplayService:
    """Run replay modes without conflating fixed history and fresh calls.

    :meth:`exact` deliberately accepts no provider dependency, so it cannot
    re-query the oracle. Fresh modes require an ``OracleProvider`` whose
    response declares material origin; arbitrary callbacks cannot mint
    genuine oracle material. Siblings remain under a new sample group.
    """

    def __init__(
        self,
        store: EventStore,
        *,
        archive: RawResponseArchive | None = None,
    ) -> None:
        self.store = store
        self.archive = archive

    def exact(
        self,
        *,
        session_id: str | None = None,
        branch_id: str | None = None,
        handler: ReplayHandler | None = None,
        append_generated: bool = False,
        rebuild_projections: bool = True,
        record: bool = False,
        actor: Actor | None = None,
    ) -> ReplayResult:
        """Replay fixed events through host logic without any oracle call."""
        if branch_id is None:
            events = tuple(self.store.list_events(session_id=session_id))
        else:
            from oracle_lab.branching import BranchService

            events = tuple(BranchService(self.store).visible_events(branch_id))
            if session_id is not None and any(event.session_id != session_id for event in events):
                raise ValueError(f"branch {branch_id} does not belong to session {session_id}")
        if rebuild_projections:
            self.store.rebuild_projections()
        generated: list[Event] = []
        if handler is not None:
            for event in events:
                produced = handler(event)
                if produced is None:
                    continue
                produced_events = (produced,) if isinstance(produced, Event) else tuple(produced)
                if append_generated:
                    self.store.append_many(produced_events)
                generated.extend(produced_events)

        replay_event: Event | None = None
        if record:
            if not events:
                raise ValueError("cannot record replay of an empty selection")
            last = events[-1]
            replay_event = Event(
                type=EventType.SESSION_REPLAYED,
                actor=actor or Actor(kind=ActorKind.SYSTEM, id="replay"),
                session_id=last.session_id,
                branch_id=last.branch_id,
                parent_event_id=last.id,
                causation_id=last.id,
                correlation_id=new_id("corr"),
                payload={
                    "mode": ReplayMode.EXACT.value,
                    "input_event_ids": [event.id for event in events],
                    "generated_event_ids": [event.id for event in generated],
                    "oracle_queried": False,
                },
            )
            self.store.append(replay_event)

        return ReplayResult(
            mode=ReplayMode.EXACT,
            input_event_ids=tuple(event.id for event in events),
            generated_event_ids=tuple(event.id for event in generated),
            replay_event_id=None if replay_event is None else replay_event.id,
            projections_rebuilt=rebuild_projections,
        )

    def oracle_resample(
        self,
        *,
        from_event_id: str,
        provider_id: str,
        model_id: str,
        sampling: SamplingParams | Mapping[str, Any],
        n: int,
        provider: OracleProvider,
        actor: Actor | None = None,
    ) -> SampleBatch:
        """Fresh generation from the persisted context visible at a historical tip."""
        if n < 1:
            raise ValueError("n must be positive")
        if self.archive is None:
            raise ValueError("fresh replay requires an immutable RawResponseArchive")
        built_context, source_context_event = self._built_context_from_event(from_event_id)
        context_value = built_context.provider_messages()
        parameters = (
            sampling.model_dump(mode="json", exclude_none=True)
            if isinstance(sampling, SamplingParams)
            else dict(sampling)
        )
        group = SamplingService(self.store).create_group(
            from_event_id=from_event_id,
            context=context_value,
            provider_id=provider_id,
            model_id=model_id,
            sampling=parameters,
            actor=actor,
        )
        source = self.store.require(from_event_id)
        if source.session_id is None or source.branch_id is None:
            raise ValueError("fresh replay source must belong to a session and branch")
        context_hash = built_context.sha256
        include_reasoning = any("reasoning" in message for message in built_context.messages)
        outputs: list[Event] = []
        errors: list[Event] = []
        for index in range(n):
            request_event = self.store.append(
                Event.new(
                    EventType.ORACLE_REQUEST,
                    actor=Actor(kind=ActorKind.SYSTEM, id="replay"),
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    parent_event_id=group.created_event_id,
                    causation_id=group.created_event_id,
                    correlation_id=self.store.require(group.created_event_id).correlation_id,
                    payload={
                        "operation": ReplayMode.ORACLE_RESAMPLE.value,
                        "model_profile_id": model_id,
                        "sample_group_id": group.id,
                        "sample_ordinal": index,
                        "context_hash": context_hash,
                        "provider_id": provider_id,
                        "model_id": model_id,
                        "sampling": parameters,
                        "source_context_event_id": source_context_event.id,
                        "source_event_ids": list(built_context.source_event_ids),
                        "context_policy": {
                            "mode": "reuse_persisted_context",
                            "include_reasoning_in_next_turn": include_reasoning,
                        },
                    },
                )
            )
            request = OracleGenerateRequest(
                model_id,
                context_value,
                temperature=parameters.get("temperature"),
                top_p=parameters.get("top_p"),
                max_tokens=parameters.get("max_tokens"),
                seed=parameters.get("seed"),
                metadata={
                    "replay_mode": ReplayMode.ORACLE_RESAMPLE.value,
                    "sample_group_id": group.id,
                    "sample_ordinal": index,
                    "requested_model_slug": model_id,
                    "requested_provider_id": provider_id,
                    "provider_routing": {
                        "replay_mode": ReplayMode.ORACLE_RESAMPLE.value,
                    },
                    "model_family": parameters.get("model_family"),
                    "checkpoint": parameters.get("checkpoint"),
                    "runtime": parameters.get("runtime"),
                    "quantization": parameters.get("quantization"),
                },
            )
            worker = OracleWorker(provider, self.archive, self.store)
            try:
                output = asyncio.run(worker.run(request_event, request, context=built_context))
            except Exception:
                errors.extend(
                    self.store.list_events(
                        event_type=EventType.ORACLE_ERROR,
                        correlation_id=request_event.correlation_id,
                    )
                )
                raise
            outputs.append(output)
        return SampleBatch(group=group, outputs=tuple(outputs), errors=tuple(errors))

    def provider_replay(
        self,
        *,
        from_event_id: str,
        provider_id: str,
        model_id: str,
        sampling: SamplingParams | Mapping[str, Any],
        provider: OracleProvider,
        n: int = 1,
    ) -> SampleBatch:
        """Use an identical context with an explicitly different provider."""
        return self.oracle_resample(
            from_event_id=from_event_id,
            provider_id=provider_id,
            model_id=model_id,
            sampling=sampling,
            n=n,
            provider=provider,
        )

    def quantization_replay(
        self,
        *,
        from_event_id: str,
        provider_id: str,
        model_id: str,
        quantization: str,
        provider: OracleProvider,
        sampling: SamplingParams | Mapping[str, Any],
        runtime: str = "local",
        n: int = 1,
    ) -> SampleBatch:
        """Run the same context against an explicit quantized model identity."""
        params = (
            sampling.model_dump(mode="json", exclude_none=True)
            if isinstance(sampling, SamplingParams)
            else dict(sampling)
        )
        params.update({"quantization": quantization, "runtime": runtime})
        return self.oracle_resample(
            from_event_id=from_event_id,
            provider_id=provider_id,
            model_id=model_id,
            sampling=params,
            n=n,
            provider=provider,
        )

    def context_from_event(self, from_event_id: str) -> list[dict[str, Any]]:
        """Recover the latest explicit context snapshot visible at an event.

        This does not infer host analysis as messages. It requires a prior
        oracle.context_built payload with a messages list, preserving the
        session-construction boundary from Section 9.
        """
        context, _event = self._built_context_from_event(from_event_id)
        return context.provider_messages()

    def _built_context_from_event(self, from_event_id: str) -> tuple[BuiltContext, Event]:
        """Validate and recover the authoritative context snapshot for replay."""
        source = self.store.require(from_event_id)
        if source.session_id is None or source.branch_id is None:
            raise ValueError("context source must belong to a session and branch")
        from oracle_lab.branching import BranchService

        visible = BranchService(self.store).visible_events(
            source.branch_id, until_event_id=source.id
        )
        context_event = next(
            (event for event in reversed(visible) if event.type is EventType.ORACLE_CONTEXT_BUILT),
            None,
        )
        if context_event is None:
            raise LookupError(f"no oracle.context_built visible at {from_event_id}")

        raw_messages = context_event.payload.get("messages")
        if not isinstance(raw_messages, Sequence) or isinstance(raw_messages, (str, bytes)):
            raise ValueError(f"context event {context_event.id} has no messages list")
        if any(not isinstance(message, Mapping) for message in raw_messages):
            raise ValueError(f"context event {context_event.id} contains a non-object message")
        messages = [dict(message) for message in raw_messages]
        digest = sha256_json(messages)
        if context_event.payload.get("sha256") != digest:
            raise ValueError(f"context event {context_event.id} has a mismatched context hash")

        raw_source_ids = context_event.payload.get("source_event_ids")
        if not isinstance(raw_source_ids, Sequence) or isinstance(raw_source_ids, (str, bytes)):
            raise ValueError(f"context event {context_event.id} has no source_event_ids list")
        if any(not isinstance(event_id, str) or not event_id for event_id in raw_source_ids):
            raise ValueError(f"context event {context_event.id} has an invalid source event ID")
        source_event_ids = tuple(raw_source_ids)
        if len(source_event_ids) != len(messages):
            raise ValueError(
                f"context event {context_event.id} must cite one source event per message"
            )
        visible_ids = {event.id for event in visible}
        missing = [event_id for event_id in source_event_ids if event_id not in visible_ids]
        if missing:
            raise ValueError(
                f"context event {context_event.id} cites events outside visible history: "
                + ", ".join(missing)
            )

        raw_truncated_ids = context_event.payload.get("truncated_source_event_ids", ())
        if not isinstance(raw_truncated_ids, Sequence) or isinstance(
            raw_truncated_ids, (str, bytes)
        ):
            raise ValueError(
                f"context event {context_event.id} has invalid truncated_source_event_ids"
            )
        if any(not isinstance(event_id, str) or not event_id for event_id in raw_truncated_ids):
            raise ValueError(
                f"context event {context_event.id} has an invalid truncated source event ID"
            )
        truncated_source_event_ids = tuple(raw_truncated_ids)
        missing_truncated = [
            event_id for event_id in truncated_source_event_ids if event_id not in visible_ids
        ]
        if missing_truncated:
            raise ValueError(
                f"context event {context_event.id} cites truncated events outside visible history: "
                + ", ".join(missing_truncated)
            )

        original_message_count = context_event.payload.get(
            "original_message_count",
            len(messages) + len(truncated_source_event_ids),
        )
        if (
            not isinstance(original_message_count, int)
            or isinstance(original_message_count, bool)
            or original_message_count < len(messages)
        ):
            raise ValueError(
                f"context event {context_event.id} has an invalid original_message_count"
            )
        truncation_strategy = context_event.payload.get("truncation_strategy")
        if truncation_strategy is not None and not isinstance(truncation_strategy, str):
            raise ValueError(f"context event {context_event.id} has an invalid truncation strategy")

        built_context = BuiltContext(
            messages=tuple(MappingProxyType(message) for message in messages),
            sha256=digest,
            source_event_ids=source_event_ids,
            session_id=source.session_id,
            branch_id=source.branch_id,
            original_message_count=original_message_count,
            truncated_source_event_ids=truncated_source_event_ids,
            truncation_strategy=truncation_strategy,
        )
        request_event = next(
            (
                event
                for event in visible
                if event.id == context_event.causation_id and event.type is EventType.ORACLE_REQUEST
            ),
            None,
        )
        model_profile_id = (
            request_event.payload.get("model_profile_id") if request_event is not None else None
        )
        context_policy = (
            request_event.payload.get("context_policy") if request_event is not None else None
        )
        include_reasoning = (
            context_policy.get("include_reasoning_in_next_turn")
            if isinstance(context_policy, Mapping)
            else None
        )
        context_visible = BranchService(self.store).visible_events(
            str(context_event.branch_id),
            until_event_id=context_event.id,
        )
        validate_built_context_sources(
            built_context,
            context_visible,
            model_profile_id=(model_profile_id if isinstance(model_profile_id, str) else None),
            include_reasoning=(include_reasoning if isinstance(include_reasoning, bool) else None),
        )
        return built_context, context_event


__all__ = ["ReplayHandler", "ReplayMode", "ReplayResult", "ReplayService"]
