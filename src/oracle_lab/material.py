"""Material-origin labels and transitive synthetic-fixture isolation."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import Any

from oracle_lab.events import ActorKind, Event


class MaterialOrigin(StrEnum):
    ORACLE_GENERATED = "oracle_generated"
    HISTORICAL_FIXTURE = "historical_fixture"
    SYNTHETIC_FIXTURE = "synthetic_fixture"
    UNKNOWN = "unknown"


EventGetter = Callable[[str], Event | None]


def explicit_material_origin(event: Event) -> MaterialOrigin:
    """Read only an event's explicit label; never guess provenance."""
    for values in (event.payload, event.metadata):
        raw = values.get("material_origin")
        if raw is not None:
            try:
                return MaterialOrigin(str(raw))
            except ValueError:
                return MaterialOrigin.UNKNOWN
        if values.get("synthetic_fixture") is True:
            return MaterialOrigin.SYNTHETIC_FIXTURE
        if values.get("historical_fixture") is True:
            return MaterialOrigin.HISTORICAL_FIXTURE
    return MaterialOrigin.UNKNOWN


def source_event_ids(event: Event) -> tuple[str, ...]:
    """Return explicit/envelope ancestry used to classify derived material."""
    identifiers: list[str] = []
    for identifier in (event.causation_id, event.parent_event_id):
        if isinstance(identifier, str):
            identifiers.append(identifier)
    for key in (
        "source_event_id",
        "target_event_id",
        "event_id",
        "verification_source_event_id",
        "approver_event_id",
    ):
        value = event.payload.get(key)
        if isinstance(value, str):
            identifiers.append(value)
    values = event.payload.get("source_event_ids", ())
    if isinstance(values, (list, tuple)):
        identifiers.extend(str(value) for value in values if isinstance(value, str))
    return tuple(dict.fromkeys(identifier for identifier in identifiers if identifier != event.id))


def material_origins(
    event: Event,
    getter: EventGetter,
    *,
    max_events: int = 10_000,
) -> frozenset[MaterialOrigin]:
    """Trace explicit origin labels through all cited event ancestry."""
    queue = deque([event])
    seen: set[str] = set()
    origins: set[MaterialOrigin] = set()
    while queue:
        current = queue.popleft()
        if current.id in seen:
            continue
        seen.add(current.id)
        if len(seen) > max_events:
            raise ValueError("material-origin ancestry exceeds safety limit")
        explicit = explicit_material_origin(current)
        if explicit is not MaterialOrigin.UNKNOWN:
            origins.add(explicit)
        for identifier in source_event_ids(current):
            source = getter(identifier)
            if source is not None and source.id not in seen:
                queue.append(source)
    return frozenset(origins or {MaterialOrigin.UNKNOWN})


def is_synthetic_lineage(event: Event, getter: EventGetter) -> bool:
    return MaterialOrigin.SYNTHETIC_FIXTURE in material_origins(event, getter)


def is_explicit_worker_artifact(event: Event) -> bool:
    """Identify auditable worker material without inferring Oracle provenance."""

    return (
        event.type.value.startswith("worker.")
        or event.actor.kind is ActorKind.WORKER
        or event.payload.get("artifact_origin") == "worker_generated"
        or event.metadata.get("artifact_origin") == "worker_generated"
    )


def is_worker_lineage(
    event: Event,
    getter: EventGetter,
    *,
    max_events: int = 10_000,
) -> bool:
    """Return true when an event is or transitively cites worker-generated material."""

    queue = deque([event])
    seen: set[str] = set()
    while queue:
        current = queue.popleft()
        if current.id in seen:
            continue
        seen.add(current.id)
        if len(seen) > max_events:
            raise ValueError("worker-artifact ancestry exceeds safety limit")
        if is_explicit_worker_artifact(current):
            return True
        for identifier in source_event_ids(current):
            source = getter(identifier)
            if source is not None and source.id not in seen:
                queue.append(source)
    return False


def mapping_is_explicit_synthetic(value: Mapping[str, Any]) -> bool:
    payload = value.get("payload")
    metadata = value.get("metadata")
    containers = [item for item in (payload, metadata) if isinstance(item, Mapping)]
    return any(
        item.get("material_origin") == MaterialOrigin.SYNTHETIC_FIXTURE.value
        or item.get("synthetic_fixture") is True
        for item in containers
    )


def mapping_is_explicit_worker_artifact(value: Mapping[str, Any]) -> bool:
    """Mapping counterpart used by lossless export filters."""

    payload = value.get("payload")
    metadata = value.get("metadata")
    actor = value.get("actor")
    containers = [item for item in (payload, metadata) if isinstance(item, Mapping)]
    return (
        str(value.get("type", "")).startswith("worker.")
        or (isinstance(actor, Mapping) and actor.get("kind") == ActorKind.WORKER.value)
        or any(item.get("artifact_origin") == "worker_generated" for item in containers)
    )


__all__ = [
    "MaterialOrigin",
    "explicit_material_origin",
    "is_explicit_worker_artifact",
    "is_synthetic_lineage",
    "is_worker_lineage",
    "mapping_is_explicit_synthetic",
    "mapping_is_explicit_worker_artifact",
    "material_origins",
    "source_event_ids",
]
