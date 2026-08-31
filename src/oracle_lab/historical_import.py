"""Lossless import of historical conversational message logs.

The importer deliberately translates only conversational structure.  It does
not run Host analysis, guess provider/model settings, or turn missing metadata
into current defaults.  Imported messages become an immutable parent chain;
assistant messages are explicitly marked ``historical_fixture`` material and
receive context snapshots that can be used as exact replay fixtures.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.ids import new_id
from oracle_lab.jsonutil import sha256_bytes, sha256_json, sha256_text

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


MATERIAL_ORIGIN = "historical_fixture"
_SENSITIVE_KEY_RE = re.compile(
    r"(?:authorization|cookie|api[-_]?key|access[-_]?token|password|secret)",
    re.IGNORECASE,
)
_ROLE_ALIASES = {
    "assistant": "assistant",
    "model": "assistant",
    "oracle": "assistant",
    "r1": "assistant",
    "human": "user",
    "user": "user",
    "system": "system",
    "tool": "tool",
    "function": "tool",
}
_EVENT_ROLE = {
    "human.input": "user",
    "oracle.output": "assistant",
    "oracle.context_message": "user",
    "tool.result_adapted": "tool",
    "analysis.promoted_to_oracle": "user",
}
_MESSAGE_KEYS = ("name", "tool_call_id", "tool_calls")
_TIMESTAMP_KEYS = ("created_at", "timestamp", "create_time", "time")
_SAMPLING_KEYS = ("temperature", "top_p", "max_tokens", "seed", "provider_pin")


class HistoricalImportError(ValueError):
    """Raised when a source cannot be imported without guessing its meaning."""


@dataclass(frozen=True, slots=True)
class HistoricalMessage:
    """One parsed source message before it becomes an event."""

    role: str
    original_role: str
    content: str
    message: Mapping[str, Any]
    source_index: int
    original_timestamp: str | int | float | None
    created_at: dt.datetime
    timestamp_status: str
    source_event_id: str | None
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ParsedHistoricalLog:
    messages: tuple[HistoricalMessage, ...]
    title: str | None
    provider: str | None
    requested_model_slug: str | None
    actual_model_identifier: str | None
    provider_routing: Mapping[str, Any] | None
    fallback_occurred: bool | None
    sampling: Mapping[str, Any] | None
    system_prompt: str | None
    api_response_metadata: Mapping[str, Any] | None
    unknown_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HistoricalImportResult:
    """Stable identifiers returned after an atomic historical import."""

    session_id: str
    branch_id: str
    root_event_id: str
    context_event_id: str
    import_event_id: str
    message_event_ids: tuple[str, ...]
    assistant_event_ids: tuple[str, ...]
    source_file: Mapping[str, Any]
    unknown_fields: tuple[str, ...]


def _json_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_copy(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_copy(item) for item in value]
    return value


def _redact_sensitive(value: Any) -> Any:
    """Copy imported metadata while never retaining credential-like values."""
    if isinstance(value, Mapping):
        return {
            str(key): (
                "[redacted]" if _SENSITIVE_KEY_RE.search(str(key)) else _redact_sensitive(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_redact_sensitive(item) for item in value]
    return value


def _source_descriptor(path: Path, raw: bytes) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    try:
        relative = resolved.relative_to(Path.home().resolve())
        display_path = f"~/{relative.as_posix()}"
    except ValueError:
        display_path = str(resolved)
    return {
        "path": display_path,
        "path_sha256": sha256_text(str(resolved)),
        "sha256": sha256_bytes(raw),
        "size_bytes": len(raw),
        "format": "jsonl" if path.suffix.casefold() == ".jsonl" else "json",
    }


def _parse_document(raw: bytes) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise HistoricalImportError("historical log must be UTF-8") from error
    if not text.strip():
        raise HistoricalImportError("historical log is empty")
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        return _parse_jsonl(text)
    return _records_from_document(document)


def _parse_jsonl(text: str) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    records: list[Mapping[str, Any]] = []
    session_metadata: dict[str, Any] = {}
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise HistoricalImportError(
                f"invalid JSONL record at line {line_number}: {error.msg}"
            ) from error
        if not isinstance(value, Mapping):
            raise HistoricalImportError(f"JSONL record at line {line_number} must be an object")
        if _is_metadata_record(value):
            _merge_known_metadata(session_metadata, value)
            continue
        nested, metadata = _records_from_document(value)
        records.extend(nested)
        _merge_known_metadata(session_metadata, metadata)
    if not records:
        raise HistoricalImportError("historical JSONL contains no messages")
    return records, session_metadata


def _records_from_document(value: Any) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        records = list(value)
        if not all(isinstance(item, Mapping) for item in records):
            raise HistoricalImportError("historical message arrays must contain objects")
        return [dict(item) for item in records], {}
    if not isinstance(value, Mapping):
        raise HistoricalImportError("historical JSON must be an object or message array")
    metadata = _known_metadata(value)
    for key in ("messages", "conversation", "conversations"):
        nested = value.get(key)
        if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
            if not all(isinstance(item, Mapping) for item in nested):
                raise HistoricalImportError(f"{key} must contain message objects")
            return [dict(item) for item in nested], metadata
    if _looks_like_message(value):
        return [dict(value)], metadata
    raise HistoricalImportError("historical JSON must contain messages/conversation/conversations")


def _looks_like_message(value: Mapping[str, Any]) -> bool:
    return (
        isinstance(value.get("role"), str)
        or isinstance(value.get("from"), str)
        or str(value.get("type", "")) in _EVENT_ROLE
    )


def _is_metadata_record(value: Mapping[str, Any]) -> bool:
    return str(value.get("type", "")).casefold() in {
        "metadata",
        "session",
    } and not _looks_like_message(value)


def _known_metadata(value: Mapping[str, Any]) -> dict[str, Any]:
    nested = value.get("metadata")
    sources = [value]
    if isinstance(nested, Mapping):
        sources.append(nested)
    payload = value.get("payload")
    if isinstance(payload, Mapping):
        sources.append(payload)
        message = payload.get("message")
        if isinstance(message, Mapping):
            sources.append(message)
    result: dict[str, Any] = {}
    for source in sources:
        for key in (
            "title",
            "provider",
            "provider_name",
            "actual_provider",
            "requested_model_slug",
            "model_slug",
            "actual_model_identifier",
            "provider_model_id",
            "model",
            "provider_routing",
            "fallback_occurred",
            "sampling",
            "system_prompt",
            "api_response_metadata",
            "reasoning",
            "finish_reason",
            "usage",
            "truth_domain",
        ):
            if key in source and source[key] is not None:
                result[key] = _redact_sensitive(source[key])
        for key in _SAMPLING_KEYS:
            if key in source and source[key] is not None:
                result[key] = source[key]
    return result


def _merge_known_metadata(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    for key, value in _known_metadata(source).items():
        target.setdefault(key, value)


def _first_string(value: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


def _optional_mapping(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    return _redact_sensitive(dict(value))


def _metadata_profile(value: Mapping[str, Any]) -> dict[str, Any]:
    provider = _first_string(value, "actual_provider", "provider", "provider_name")
    requested_model_slug = _first_string(value, "requested_model_slug", "model_slug")
    actual_model_identifier = _first_string(
        value, "actual_model_identifier", "provider_model_id", "model"
    )
    routing = _optional_mapping(value.get("provider_routing"))
    fallback = value.get("fallback_occurred")
    fallback_occurred = fallback if isinstance(fallback, bool) else None
    sampling = _optional_mapping(value.get("sampling"))
    if sampling is None:
        supplied_sampling = {
            key: value[key] for key in _SAMPLING_KEYS if value.get(key) is not None
        }
        sampling = supplied_sampling or None
    system_prompt = value.get("system_prompt")
    if system_prompt is not None and not isinstance(system_prompt, str):
        raise HistoricalImportError("system_prompt must be text or null")
    api_metadata = _optional_mapping(value.get("api_response_metadata"))
    unknown_fields = tuple(
        key
        for key, candidate in (
            ("provider", provider),
            ("requested_model_slug", requested_model_slug),
            ("actual_model_identifier", actual_model_identifier),
            ("provider_routing", routing),
            ("fallback_occurred", fallback_occurred),
            ("sampling", sampling),
            ("system_prompt", system_prompt),
            ("api_response_metadata", api_metadata),
        )
        if candidate is None
    )
    return {
        "provider": provider,
        "requested_model_slug": requested_model_slug,
        "actual_model_identifier": actual_model_identifier,
        "provider_routing": routing,
        "fallback_occurred": fallback_occurred,
        "sampling": sampling,
        "system_prompt": system_prompt,
        "api_response_metadata": api_metadata,
        "unknown_fields": unknown_fields,
    }


def _timestamp(
    record: Mapping[str, Any],
) -> tuple[str | int | float | None, dt.datetime | None, str]:
    raw: Any = None
    sources: list[Mapping[str, Any]] = [record]
    payload = record.get("payload")
    metadata = record.get("metadata")
    if isinstance(payload, Mapping):
        sources.append(payload)
    if isinstance(metadata, Mapping):
        sources.append(metadata)
    for source in sources:
        for key in _TIMESTAMP_KEYS:
            if key in source:
                raw = source[key]
                break
        if raw is not None:
            break
    if raw is None:
        return None, None, "unknown"
    if isinstance(raw, bool):
        return str(raw).lower(), None, "invalid"
    if isinstance(raw, (int, float)):
        if not math.isfinite(float(raw)):
            return raw, None, "invalid"
        try:
            seconds = float(raw) / 1000 if abs(float(raw)) >= 10_000_000_000 else float(raw)
            return raw, dt.datetime.fromtimestamp(seconds, tz=dt.UTC), "known"
        except (OverflowError, OSError, ValueError):
            return raw, None, "invalid"
    if not isinstance(raw, str):
        return str(raw), None, "invalid"
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw, None, "invalid"
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return raw, None, "unknown_timezone"
    return raw, parsed, "known"


def _message_content(record: Mapping[str, Any]) -> str:
    payload = record.get("payload")
    candidates = [record]
    if isinstance(payload, Mapping):
        message = payload.get("message")
        if isinstance(message, Mapping):
            candidates.insert(0, message)
        candidates.append(payload)
    for source in candidates:
        for key in ("content", "text", "value", "output"):
            value = source.get(key)
            if isinstance(value, str):
                return value
    raise HistoricalImportError("each historical message requires exact string content")


def _message_role(record: Mapping[str, Any]) -> tuple[str, str]:
    payload = record.get("payload")
    nested_message = payload.get("message") if isinstance(payload, Mapping) else None
    candidates = [
        nested_message.get("role") if isinstance(nested_message, Mapping) else None,
        payload.get("role") if isinstance(payload, Mapping) else None,
        record.get("role"),
        record.get("from"),
    ]
    original = next((item for item in candidates if isinstance(item, str) and item), None)
    if original is None:
        original = _EVENT_ROLE.get(str(record.get("type", "")))
    if original is None:
        raise HistoricalImportError("each historical message requires a role")
    role = _ROLE_ALIASES.get(original.casefold())
    if role is None:
        raise HistoricalImportError(f"unsupported historical message role: {original}")
    return role, original


def _visible_message(record: Mapping[str, Any], *, role: str, content: str) -> dict[str, Any]:
    payload = record.get("payload")
    nested = payload.get("message") if isinstance(payload, Mapping) else None
    sources = [source for source in (nested, payload, record) if isinstance(source, Mapping)]
    message: dict[str, Any] = {"role": role, "content": content}
    for key in _MESSAGE_KEYS:
        for source in sources:
            if key in source:
                message[key] = _redact_sensitive(source[key])
                break
    return message


def _parse_historical_raw(source_path: Path, raw: bytes) -> ParsedHistoricalLog:
    records, raw_metadata = _parse_document(raw)
    profile = _metadata_profile(raw_metadata)
    imported_at = dt.datetime.now(dt.UTC)
    prepared: list[HistoricalMessage] = []
    for index, record in enumerate(records):
        role, original_role = _message_role(record)
        content = _message_content(record)
        original_timestamp, parsed_time, status = _timestamp(record)
        created_at = parsed_time or (imported_at + dt.timedelta(microseconds=index))
        record_metadata = _known_metadata(record)
        truth_domain = record_metadata.get("truth_domain")
        if (
            role == "tool"
            and truth_domain is not None
            and truth_domain
            not in {
                "real",
                "sandbox",
                "virtual",
                "retrieved",
                "synthetic",
            }
        ):
            raise HistoricalImportError(
                f"historical tool message {index} has an invalid truth_domain"
            )
        source_event_id = record.get("id") or record.get("event_id")
        prepared.append(
            HistoricalMessage(
                role=role,
                original_role=original_role,
                content=content,
                message=_visible_message(record, role=role, content=content),
                source_index=index,
                original_timestamp=original_timestamp,
                created_at=created_at,
                timestamp_status=status,
                source_event_id=(source_event_id if isinstance(source_event_id, str) else None),
                metadata=record_metadata,
            )
        )
    if not prepared:
        raise HistoricalImportError("historical log contains no messages")

    explicit_system = profile["system_prompt"]
    system_messages = [message for message in prepared if message.role == "system"]
    if isinstance(explicit_system, str):
        if system_messages and system_messages[0].content != explicit_system:
            raise HistoricalImportError(
                "top-level system_prompt differs from the imported system message"
            )
        if not system_messages:
            first_time = prepared[0].created_at
            prepared.insert(
                0,
                HistoricalMessage(
                    role="system",
                    original_role="system_prompt",
                    content=explicit_system,
                    message={"role": "system", "content": explicit_system},
                    source_index=-1,
                    original_timestamp=None,
                    created_at=first_time,
                    timestamp_status="not_separately_supplied",
                    source_event_id=None,
                    metadata={},
                ),
            )
    return ParsedHistoricalLog(
        messages=tuple(prepared),
        title=_first_string(raw_metadata, "title"),
        provider=profile["provider"],
        requested_model_slug=profile["requested_model_slug"],
        actual_model_identifier=profile["actual_model_identifier"],
        provider_routing=profile["provider_routing"],
        fallback_occurred=profile["fallback_occurred"],
        sampling=profile["sampling"],
        system_prompt=profile["system_prompt"],
        api_response_metadata=profile["api_response_metadata"],
        unknown_fields=profile["unknown_fields"],
    )


def parse_historical_log(path: str | Path) -> ParsedHistoricalLog:
    """Parse supported JSON/JSONL logs without mutating or normalizing text."""
    source_path = Path(path).expanduser()
    try:
        raw = source_path.read_bytes()
    except OSError as error:
        raise HistoricalImportError(f"cannot read historical log: {source_path}") from error
    return _parse_historical_raw(source_path, raw)


def _before(value: dt.datetime) -> dt.datetime:
    try:
        return value - dt.timedelta(microseconds=1)
    except OverflowError:
        return value


def _after(value: dt.datetime) -> dt.datetime:
    try:
        return value + dt.timedelta(microseconds=1)
    except OverflowError:
        return value


class HistoricalSessionImporter:
    """Convert a parsed historical log into one atomic event ancestry."""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def import_file(
        self,
        path: str | Path,
        *,
        title: str | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
    ) -> HistoricalImportResult:
        source_path = Path(path).expanduser()
        try:
            raw = source_path.read_bytes()
        except OSError as error:
            raise HistoricalImportError(f"cannot read historical log: {source_path}") from error
        parsed = _parse_historical_raw(source_path, raw)
        session_identifier = session_id or new_id("ses")
        branch_identifier = branch_id or new_id("br")
        if self.store.connection.execute(
            "SELECT 1 FROM sessions WHERE id = ?", (session_identifier,)
        ).fetchone():
            raise HistoricalImportError(f"session already exists: {session_identifier}")
        if self.store.connection.execute(
            "SELECT 1 FROM branches WHERE id = ?", (branch_identifier,)
        ).fetchone():
            raise HistoricalImportError(f"branch already exists: {branch_identifier}")

        source = _source_descriptor(source_path, raw)
        correlation_id = new_id("cor")
        earliest = min(message.created_at for message in parsed.messages)
        root = Event.new(
            EventType.HUMAN_CHECKPOINT,
            actor=Actor(kind=ActorKind.SYSTEM, id="historical-importer"),
            created_at=_before(earliest),
            session_id=session_identifier,
            branch_id=branch_identifier,
            correlation_id=correlation_id,
            payload={
                "operation": "session.created",
                "title": title or parsed.title or f"Imported {source_path.stem}",
                "model_profile_id": None,
                "branch_id": branch_identifier,
                "branch_title": "imported-main",
                "material_origin": MATERIAL_ORIGIN,
                "historical_fixture": True,
                "source_file": source,
                "historical_identity": self._historical_identity(parsed),
            },
            metadata=self._event_metadata(source, source_index=None),
        )
        events: list[Event] = [root]
        parent = root
        visible_messages: list[dict[str, Any]] = []
        visible_source_ids: list[str] = []
        message_event_ids: list[str] = []
        assistant_event_ids: list[str] = []

        for message in parsed.messages:
            if message.role == "assistant":
                request = Event.new(
                    EventType.ORACLE_REQUEST,
                    actor=Actor(kind=ActorKind.SYSTEM, id="historical-importer"),
                    created_at=message.created_at,
                    session_id=session_identifier,
                    branch_id=branch_identifier,
                    parent_event_id=parent.id,
                    causation_id=parent.id,
                    correlation_id=correlation_id,
                    payload={
                        "operation": "historical_import",
                        "model_profile_id": None,
                        "provider_id": self._message_value(message, parsed, "provider"),
                        "requested_model_slug": self._message_value(
                            message, parsed, "requested_model_slug"
                        ),
                        "sampling": self._message_value(message, parsed, "sampling"),
                        "context_hash": sha256_json(visible_messages),
                        "source_event_ids": list(visible_source_ids),
                        "material_origin": MATERIAL_ORIGIN,
                    },
                    metadata=self._event_metadata(source, source_index=message.source_index),
                )
                context = Event.new(
                    EventType.ORACLE_CONTEXT_BUILT,
                    actor=Actor(kind=ActorKind.SYSTEM, id="historical-importer"),
                    created_at=message.created_at,
                    session_id=session_identifier,
                    branch_id=branch_identifier,
                    parent_event_id=request.id,
                    causation_id=request.id,
                    correlation_id=correlation_id,
                    payload={
                        "messages": _json_copy(visible_messages),
                        "sha256": sha256_json(visible_messages),
                        "source_event_ids": list(visible_source_ids),
                        "material_origin": MATERIAL_ORIGIN,
                        "operation": "historical_import.context",
                    },
                    metadata=self._event_metadata(source, source_index=message.source_index),
                )
                output = self._assistant_event(
                    message,
                    parsed=parsed,
                    source=source,
                    session_id=session_identifier,
                    branch_id=branch_identifier,
                    parent_event_id=context.id,
                    request_event_id=request.id,
                    correlation_id=correlation_id,
                    context_hash=str(context.payload["sha256"]),
                )
                events.extend((request, context, output))
                parent = output
                message_event = output
                assistant_event_ids.append(output.id)
            else:
                message_event = self._visible_import_event(
                    message,
                    source=source,
                    session_id=session_identifier,
                    branch_id=branch_identifier,
                    parent_event_id=parent.id,
                    correlation_id=correlation_id,
                )
                events.append(message_event)
                parent = message_event
            visible_messages.append(_json_copy(message.message))
            visible_source_ids.append(message_event.id)
            message_event_ids.append(message_event.id)

        final_time = _after(
            max(dt.datetime.now(dt.UTC), *(item.created_at for item in parsed.messages))
        )
        final_context = Event.new(
            EventType.ORACLE_CONTEXT_BUILT,
            actor=Actor(kind=ActorKind.SYSTEM, id="historical-importer"),
            created_at=final_time,
            session_id=session_identifier,
            branch_id=branch_identifier,
            parent_event_id=parent.id,
            causation_id=parent.id,
            correlation_id=correlation_id,
            payload={
                "messages": _json_copy(visible_messages),
                "sha256": sha256_json(visible_messages),
                "source_event_ids": list(visible_source_ids),
                "material_origin": MATERIAL_ORIGIN,
                "operation": "historical_import.final_context",
            },
            metadata=self._event_metadata(source, source_index=None),
        )
        completed = Event.new(
            EventType.SESSION_IMPORTED,
            actor=Actor(kind=ActorKind.SYSTEM, id="historical-importer"),
            created_at=_after(final_time),
            session_id=session_identifier,
            branch_id=branch_identifier,
            parent_event_id=final_context.id,
            causation_id=root.id,
            correlation_id=correlation_id,
            payload={
                "source_file": source,
                "material_origin": MATERIAL_ORIGIN,
                "historical_fixture": True,
                "root_event_id": root.id,
                "tip_event_id": parent.id,
                "context_event_id": final_context.id,
                "message_event_ids": message_event_ids,
                "assistant_event_ids": assistant_event_ids,
                "message_count": len(message_event_ids),
                "unknown_fields": list(parsed.unknown_fields),
            },
            metadata=self._event_metadata(source, source_index=None),
        )
        events.extend((final_context, completed))
        self.store.append_many(events)
        return HistoricalImportResult(
            session_id=session_identifier,
            branch_id=branch_identifier,
            root_event_id=root.id,
            context_event_id=final_context.id,
            import_event_id=completed.id,
            message_event_ids=tuple(message_event_ids),
            assistant_event_ids=tuple(assistant_event_ids),
            source_file=source,
            unknown_fields=parsed.unknown_fields,
        )

    @staticmethod
    def _historical_identity(parsed: ParsedHistoricalLog) -> dict[str, Any]:
        return {
            "requested_model_slug": parsed.requested_model_slug,
            "actual_provider": parsed.provider,
            "actual_model_identifier": parsed.actual_model_identifier,
            "provider_routing": _json_copy(parsed.provider_routing),
            "fallback_occurred": parsed.fallback_occurred,
            "sampling": _json_copy(parsed.sampling),
            "system_prompt": parsed.system_prompt,
            "api_response_metadata": _json_copy(parsed.api_response_metadata),
            "unknown_fields": list(parsed.unknown_fields),
        }

    @staticmethod
    def _event_metadata(source: Mapping[str, Any], *, source_index: int | None) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "material_origin": MATERIAL_ORIGIN,
            "historical_fixture": True,
            "import_source": {
                "path": source["path"],
                "path_sha256": source["path_sha256"],
                "sha256": source["sha256"],
                "source_index": source_index,
            },
        }

    @staticmethod
    def _message_value(message: HistoricalMessage, parsed: ParsedHistoricalLog, name: str) -> Any:
        profile = _metadata_profile(message.metadata)
        value = profile.get(name)
        return value if value is not None else getattr(parsed, name)

    def _assistant_event(
        self,
        message: HistoricalMessage,
        *,
        parsed: ParsedHistoricalLog,
        source: Mapping[str, Any],
        session_id: str,
        branch_id: str,
        parent_event_id: str,
        request_event_id: str,
        correlation_id: str,
        context_hash: str,
    ) -> Event:
        provider = self._message_value(message, parsed, "provider")
        requested_slug = self._message_value(message, parsed, "requested_model_slug")
        actual_model = self._message_value(message, parsed, "actual_model_identifier")
        routing = self._message_value(message, parsed, "provider_routing")
        fallback = self._message_value(message, parsed, "fallback_occurred")
        sampling = self._message_value(message, parsed, "sampling")
        api_metadata = self._message_value(message, parsed, "api_response_metadata")
        unknown = [
            key
            for key, value in (
                ("requested_model_slug", requested_slug),
                ("actual_provider", provider),
                ("actual_model_identifier", actual_model),
                ("provider_routing", routing),
                ("fallback_occurred", fallback),
                ("sampling", sampling),
                ("api_response_metadata", api_metadata),
            )
            if value is None
        ]
        model_identity = {
            "requested_model_profile_id": None,
            "requested_model_slug": requested_slug,
            "model_family": None,
            "checkpoint": None,
            "runtime": None,
            "quantization": None,
            "requested_provider_id": None,
            "provider_routing": _json_copy(routing),
            "actual_provider": provider,
            "actual_model_identifier": actual_model,
            "fallback_occurred": fallback,
            "unknown_fields": unknown,
        }
        return Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id=None),
            created_at=message.created_at,
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent_event_id,
            causation_id=request_event_id,
            correlation_id=correlation_id,
            payload={
                "role": "assistant",
                "content": message.content,
                "message": _json_copy(message.message),
                "reasoning": message.metadata.get("reasoning"),
                "model_profile_id": None,
                "model": actual_model,
                "provider": provider,
                "provider_name": provider,
                "provider_model_id": actual_model,
                "sampling": _json_copy(sampling),
                "provider_routing": _json_copy(routing),
                "fallback_occurred": fallback,
                "finish_reason": message.metadata.get("finish_reason"),
                "usage": _json_copy(message.metadata.get("usage", {})),
                "api_response_metadata": _json_copy(api_metadata),
                "archive_path": None,
                "archive_sha256": None,
                "source_file": _json_copy(source),
                "original_event_id": message.source_event_id,
                "source_index": message.source_index,
                "original_role": message.original_role,
                "original_timestamp": message.original_timestamp,
                "timestamp_status": message.timestamp_status,
                "raw_sha256": sha256_text(message.content),
                "material_origin": MATERIAL_ORIGIN,
                "historical_fixture": True,
                "model_identity": model_identity,
                "context_hash": context_hash,
            },
            metadata=self._event_metadata(source, source_index=message.source_index),
        )

    def _visible_import_event(
        self,
        message: HistoricalMessage,
        *,
        source: Mapping[str, Any],
        session_id: str,
        branch_id: str,
        parent_event_id: str,
        correlation_id: str,
    ) -> Event:
        if message.role == "user":
            event_type = EventType.HUMAN_INPUT
            actor = Actor(kind=ActorKind.HUMAN, id="historical-import")
        elif message.role == "system":
            event_type = EventType.ORACLE_CONTEXT_MESSAGE
            actor = Actor(kind=ActorKind.SYSTEM, id="historical-import")
        else:
            event_type = EventType.ORACLE_CONTEXT_MESSAGE
            actor = Actor(kind=ActorKind.TOOL, id="historical-import")
        truth_domain = message.metadata.get("truth_domain") if message.role == "tool" else None
        truth_domain_status = None
        truth_domain_unknown_reason = None
        if message.role == "tool":
            if truth_domain is None:
                truth_domain_status = "unknown_historical"
                truth_domain_unknown_reason = "not_present_in_source"
            else:
                truth_domain_status = "known_historical"
        return Event.new(
            event_type,
            actor=actor,
            created_at=message.created_at,
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent_event_id,
            causation_id=parent_event_id,
            correlation_id=correlation_id,
            payload={
                "role": message.role,
                "content": message.content,
                "text": message.content,
                "message": _json_copy(message.message),
                "source_file": _json_copy(source),
                "original_event_id": message.source_event_id,
                "source_index": message.source_index,
                "original_role": message.original_role,
                "original_timestamp": message.original_timestamp,
                "timestamp_status": message.timestamp_status,
                "raw_sha256": sha256_text(message.content),
                "truth_domain": truth_domain,
                "truth_domain_status": truth_domain_status,
                "truth_domain_unknown_reason": truth_domain_unknown_reason,
                "material_origin": MATERIAL_ORIGIN,
                "historical_fixture": True,
            },
            metadata=self._event_metadata(source, source_index=message.source_index),
        )


__all__ = [
    "MATERIAL_ORIGIN",
    "HistoricalImportError",
    "HistoricalImportResult",
    "HistoricalMessage",
    "HistoricalSessionImporter",
    "ParsedHistoricalLog",
    "parse_historical_log",
]
