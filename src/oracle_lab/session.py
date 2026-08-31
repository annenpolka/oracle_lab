"""Construction of the exact, hash-addressed message sequence seen by R1."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from oracle_lab.jsonutil import sha256_json


class EventLike(Protocol):
    id: str
    type: Any
    session_id: str | None
    branch_id: str | None
    parent_event_id: str | None
    payload: Mapping[str, Any]
    metadata: Mapping[str, Any]


def _event_type(event: EventLike) -> str:
    value = event.type
    return str(getattr(value, "value", value))


def _json_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_copy(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_copy(item) for item in value]
    return value


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class BuiltContext:
    messages: tuple[Mapping[str, Any], ...]
    sha256: str
    source_event_ids: tuple[str, ...]
    session_id: str
    branch_id: str
    original_message_count: int = 0
    truncated_source_event_ids: tuple[str, ...] = ()
    truncation_strategy: str | None = None

    @property
    def truncated(self) -> bool:
        return bool(self.truncated_source_event_ids)

    def provider_messages(self) -> list[dict[str, Any]]:
        return [_json_copy(dict(message)) for message in self.messages]

    def event_payload(self) -> dict[str, Any]:
        payload = {
            "messages": self.provider_messages(),
            "sha256": self.sha256,
            "source_event_ids": list(self.source_event_ids),
            "original_message_count": self.original_message_count or len(self.messages),
            "retained_message_count": len(self.messages),
        }
        if self.truncated:
            payload.update(
                {
                    "truncated": True,
                    "truncated_source_event_ids": list(self.truncated_source_event_ids),
                    "truncation_strategy": self.truncation_strategy,
                }
            )
        return payload


class ContextConstructionError(ValueError):
    pass


def _configuration_system_message(
    event: EventLike,
    *,
    model_profile_id: str | None,
) -> dict[str, Any]:
    if model_profile_id is None:
        raise ContextConstructionError(
            "a configuration-sourced system message requires a model profile ID"
        )
    configuration = event.payload.get("configuration")
    models = configuration.get("models") if isinstance(configuration, Mapping) else None
    profile = models.get(model_profile_id) if isinstance(models, Mapping) else None
    system_prompt = profile.get("system_prompt") if isinstance(profile, Mapping) else None
    if not isinstance(system_prompt, str) or not system_prompt:
        raise ContextConstructionError(
            "configuration snapshot does not contain the cited non-empty system prompt"
        )
    return {"role": "system", "content": system_prompt}


def validate_built_context_sources(
    context: BuiltContext,
    events: Iterable[EventLike],
    *,
    model_profile_id: str | None = None,
    include_reasoning: bool | None = None,
) -> None:
    """Prove that every provider message is exact text from its cited event.

    Replay and sampling may intentionally reuse an older context snapshot, so
    this does not demand that the snapshot equal the latest conversation. It
    does prevent a caller from attaching Host-written or transformed text to
    the ID of an unrelated genuine event.
    """

    if len(context.messages) != len(context.source_event_ids):
        raise ContextConstructionError("built context must cite one source event per message")
    if len(set(context.source_event_ids)) != len(context.source_event_ids):
        raise ContextConstructionError("built context source event IDs must be unique")
    if len(set(context.truncated_source_event_ids)) != len(context.truncated_source_event_ids):
        raise ContextConstructionError("truncated source event IDs must be unique")
    overlap = set(context.source_event_ids) & set(context.truncated_source_event_ids)
    if overlap:
        raise ContextConstructionError("retained and truncated source event IDs must be disjoint")
    expected_original_count = len(context.messages) + len(context.truncated_source_event_ids)
    if context.original_message_count not in {0, expected_original_count}:
        raise ContextConstructionError(
            "original message count must equal retained plus truncated sources"
        )
    if context.truncated_source_event_ids and context.original_message_count == 0:
        raise ContextConstructionError(
            "truncated context requires an explicit original message count"
        )
    materialized = tuple(events)
    by_id = {event.id: event for event in materialized}
    builder = SessionContextBuilder()
    rejected = builder._rejected_ids(materialized)
    permitted_rejected = builder._fork_origins(materialized)
    invalid_rejected = [
        source_event_id
        for source_event_id in (
            *context.source_event_ids,
            *context.truncated_source_event_ids,
        )
        if source_event_id in rejected and source_event_id not in permitted_rejected
    ]
    if invalid_rejected:
        raise ContextConstructionError(
            "built context cites rejected oracle material: " + ", ".join(invalid_rejected)
        )
    for source_event_id in context.truncated_source_event_ids:
        source = by_id.get(source_event_id)
        if source is None:
            raise ContextConstructionError(
                f"built context cites a missing truncated event: {source_event_id}"
            )
        source_type = _event_type(source)
        if source_type == "human.input":
            builder._message_from_payload(source.payload, default_role="user")
        elif source_type == "oracle.output":
            builder._message_from_payload(source.payload, default_role="assistant")
        elif source_type in builder.adapter_event_types:
            builder._message_from_payload(source.payload, default_role="user")
        else:
            raise ContextConstructionError(
                f"event {source_event_id} cannot be a truncated provider-message source"
            )
    for ordinal, (message_value, source_event_id) in enumerate(
        zip(context.messages, context.source_event_ids, strict=True)
    ):
        source = by_id.get(source_event_id)
        if source is None:
            raise ContextConstructionError(
                f"built context cites a missing source event: {source_event_id}"
            )
        actual = _json_copy(dict(message_value))
        source_type = _event_type(source)
        if source_type == "human.input":
            expected = builder._message_from_payload(source.payload, default_role="user")
        elif source_type == "oracle.output":
            expected = builder._message_from_payload(source.payload, default_role="assistant")
            expected.pop("reasoning", None)
            source_reasoning = source.payload.get("reasoning")
            if include_reasoning is True and source_reasoning is not None:
                expected["reasoning"] = _json_copy(source_reasoning)
            elif "reasoning" in actual:
                raise ContextConstructionError(
                    f"context message {ordinal} includes oracle reasoning without "
                    "an explicit recorded intervention"
                )
        elif source_type in builder.adapter_event_types:
            expected = builder._message_from_payload(source.payload, default_role="user")
        elif (
            source_type == "session.checkpointed"
            and source.payload.get("operation") == "configuration.snapshot"
        ):
            expected = _configuration_system_message(
                source,
                model_profile_id=model_profile_id,
            )
        else:
            raise ContextConstructionError(
                f"event {source_event_id} cannot be a provider-message source"
            )
        if actual != expected:
            raise ContextConstructionError(
                f"context message {ordinal} differs from cited event {source_event_id}"
            )


class SessionContextBuilder:
    """Pure projection from an event lineage to provider messages.

    Tool outputs and host analysis are deliberately *not* inferred as visible.
    They enter only through one of the explicit adapter/promotion event types.
    """

    adapter_event_types = frozenset(
        {
            "oracle.context_message",
            "tool.result_adapted",
            "analysis.promoted_to_oracle",
        }
    )

    def build(
        self,
        events: Iterable[EventLike],
        *,
        session_id: str,
        branch_id: str,
        tip_event_id: str | None = None,
        system_prompt: str = "",
        system_prompt_source_event_id: str | None = None,
        include_reasoning: bool = False,
        max_messages: int | None = None,
    ) -> BuiltContext:
        if max_messages is not None and max_messages < 1:
            raise ContextConstructionError("max_messages must be positive")
        materialized = list(events)
        by_id = {event.id: event for event in materialized}
        lineage = self._lineage(
            materialized,
            by_id=by_id,
            session_id=session_id,
            branch_id=branch_id,
            tip_event_id=tip_event_id,
        )
        # Rejection is temporal narrative state, not branch-global state.  A
        # reject appended after ``tip_event_id`` must not retroactively change
        # the exact context reconstructed for that historical tip/replay.
        rejected = self._rejected_ids(lineage)
        permitted_rejected = self._fork_origins(lineage)
        messages: list[dict[str, Any]] = []
        message_source_ids: list[str | None] = []
        if system_prompt:
            if system_prompt_source_event_id is None:
                raise ContextConstructionError("non-empty system_prompt requires a source event ID")
            source_event = by_id.get(system_prompt_source_event_id)
            if source_event is None or source_event.session_id != session_id:
                raise ContextConstructionError(
                    "system_prompt source event is missing or belongs to another session"
                )
            messages.append({"role": "system", "content": system_prompt})
            message_source_ids.append(system_prompt_source_event_id)
        for event in lineage:
            event_type = _event_type(event)
            if event_type == "human.input":
                message = self._message_from_payload(event.payload, default_role="user")
            elif event_type == "oracle.output":
                if event.id in rejected and event.id not in permitted_rejected:
                    continue
                message = self._message_from_payload(event.payload, default_role="assistant")
                if include_reasoning and event.payload.get("reasoning") is not None:
                    message["reasoning"] = _json_copy(event.payload["reasoning"])
            elif event_type in self.adapter_event_types:
                # The event type itself is the explicit adapter/promotion gate.
                message = self._message_from_payload(event.payload, default_role="user")
            else:
                # human.note, raw tool.output, and every analysis event are
                # intentionally invisible regardless of a convenient content key.
                continue
            messages.append(message)
            message_source_ids.append(event.id)
        original_message_count = len(messages)
        truncated_source_ids: list[str] = []
        truncation_strategy: str | None = None
        if max_messages is not None and len(messages) > max_messages:
            if messages[0].get("role") == "system":
                retained_indices = [0, *range(len(messages) - (max_messages - 1), len(messages))]
            else:
                retained_indices = list(range(len(messages) - max_messages, len(messages)))
            retained = set(retained_indices)
            truncated_source_ids = [
                source_id
                for index, source_id in enumerate(message_source_ids)
                if index not in retained and source_id is not None
            ]
            messages = [messages[index] for index in retained_indices]
            message_source_ids = [message_source_ids[index] for index in retained_indices]
            truncation_strategy = "preserve_system_keep_newest"
        source_ids = [source_id for source_id in message_source_ids if source_id is not None]
        digest = sha256_json(messages)
        frozen = tuple(_deep_freeze(message) for message in messages)
        return BuiltContext(
            messages=frozen,
            sha256=digest,
            source_event_ids=tuple(source_ids),
            session_id=session_id,
            branch_id=branch_id,
            original_message_count=original_message_count,
            truncated_source_event_ids=tuple(truncated_source_ids),
            truncation_strategy=truncation_strategy,
        )

    def _lineage(
        self,
        events: Sequence[EventLike],
        *,
        by_id: Mapping[str, EventLike],
        session_id: str,
        branch_id: str,
        tip_event_id: str | None,
    ) -> list[EventLike]:
        candidates = [
            event
            for event in events
            if event.session_id == session_id and event.branch_id == branch_id
        ]
        if tip_event_id is not None:
            try:
                tip = by_id[tip_event_id]
            except KeyError as exc:
                raise ContextConstructionError(f"unknown context tip: {tip_event_id}") from exc
            if tip.session_id != session_id:
                raise ContextConstructionError("context tip belongs to another session")
            if tip.branch_id != branch_id:
                raise ContextConstructionError("context tip belongs to a sibling branch")
        elif narrative_candidates := [
            event for event in candidates if not _event_type(event).startswith("job.")
        ]:
            tip = narrative_candidates[-1]
        else:
            return []

        # Follow narrative lineage across a fork boundary.  This includes the
        # pre-fork visible history while preventing sibling branch leakage.
        chain: list[EventLike] = []
        seen: set[str] = set()
        current: EventLike | None = tip
        while current is not None:
            if current.id in seen:
                raise ContextConstructionError(f"event lineage contains a cycle at {current.id}")
            if current.session_id != session_id:
                raise ContextConstructionError("event lineage crosses session boundary")
            seen.add(current.id)
            chain.append(current)
            parent_id = current.parent_event_id
            parent = by_id.get(parent_id) if parent_id else None
            if parent is not None and parent.branch_id != current.branch_id:
                if _event_type(current) != "session.forked":
                    raise ContextConstructionError(
                        "branch lineage crossed without an explicit session.forked event"
                    )
                fork_origin = self._target_id(current.payload)
                if fork_origin is not None and fork_origin != parent.id:
                    raise ContextConstructionError("session.forked origin disagrees with lineage")
            current = parent
        chain.reverse()

        # Some imported logs omit narrative parent links.  For a non-forked
        # branch, retain their supplied authoritative order instead of silently
        # producing an empty/one-event context.
        if len(chain) == 1 and len(candidates) > 1 and not tip.parent_event_id:
            return candidates[: candidates.index(tip) + 1]
        return chain

    @staticmethod
    def _message_from_payload(payload: Mapping[str, Any], *, default_role: str) -> dict[str, Any]:
        nested = payload.get("message")
        if isinstance(nested, Mapping):
            message = _json_copy(dict(nested))
        else:
            content = payload.get("content", payload.get("text"))
            if content is None:
                raise ContextConstructionError("R1-visible event has no message content")
            message = {"role": payload.get("role", default_role), "content": _json_copy(content)}
        role = message.get("role")
        if not isinstance(role, str) or not role:
            raise ContextConstructionError("R1-visible message has an invalid role")
        if "content" not in message:
            raise ContextConstructionError("R1-visible message has no content")
        return message

    @staticmethod
    def _target_id(payload: Mapping[str, Any]) -> str | None:
        for key in ("event_id", "target_event_id", "source_event_id", "from_event_id"):
            value = payload.get(key)
            if isinstance(value, str):
                return value
        return None

    def _rejected_ids(self, lineage: Sequence[EventLike]) -> set[str]:
        return {
            target
            for event in lineage
            if _event_type(event) == "human.reject"
            if (target := self._target_id(event.payload)) is not None
        }

    def _fork_origins(self, lineage: Sequence[EventLike]) -> set[str]:
        return {
            target
            for event in lineage
            if _event_type(event) == "session.forked"
            if (target := self._target_id(event.payload)) is not None
        }


ContextBuilder = SessionContextBuilder

__all__ = [
    "BuiltContext",
    "ContextBuilder",
    "ContextConstructionError",
    "SessionContextBuilder",
    "validate_built_context_sources",
]
