"""Event-backed provenance graph and origin queries."""

from __future__ import annotations

import sqlite3
from collections import deque
from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from oracle_lab.events import Actor, ActorKind, Event, EventType

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


class ProvenanceRelation(StrEnum):
    """Well-known relationships between a derived record and source event."""

    DERIVED_FROM = "derived_from"
    INTRODUCED = "introduced"
    REUSED = "reused"
    REFERENCED = "referenced"
    ESTABLISHED_BY_TOOL = "established_by_tool"
    PROMPTED_BY_HUMAN = "prompted_by_human"
    SYNTHESIZED = "synthesized"
    STARRED = "starred"
    KEPT = "kept"
    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    CAUSED_BY = "caused_by"


class ProvenanceEdge(BaseModel):
    """One immutable edge in the rebuildable provenance projection."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: int
    derived_kind: str
    derived_id: str
    source_event_id: str
    relation: str
    created_event_id: str
    branch_id: str | None = None


class ProvenanceOrigin(BaseModel):
    """A source event together with its human/model/tool origin."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    event: Event
    relation: str
    depth: int


def _derived_identity(event: Event) -> tuple[str, str]:
    payload = event.payload
    if payload.get("derived_kind") and payload.get("derived_id"):
        return str(payload["derived_kind"]), str(payload["derived_id"])
    if event.type is EventType.VIRTUAL_FILE_CREATED:
        node = payload.get("node")
        if isinstance(node, Mapping) and node.get("path") is not None:
            return "virtual_file", str(node["path"])
    if event.type is EventType.VIRTUAL_PROCESS_CREATED:
        process = payload.get("process")
        if isinstance(process, Mapping) and process.get("pid") is not None:
            return "virtual_process", str(process["pid"])
    if event.type is EventType.VIRTUAL_PROCESS_SIGNAL_RECEIVED and payload.get("pid") is not None:
        return "virtual_process", str(payload["pid"])
    for key, kind in (
        ("claim_id", "claim"),
        ("entity_id", "entity"),
        ("motif_id", "motif"),
        ("group_id", "sample_group"),
        ("sample_group_id", "sample_output"),
        ("path", "virtual_file"),
        ("process_id", "virtual_process"),
    ):
        value = payload.get(key)
        if value is not None:
            identifier = event.id if key == "sample_group_id" else str(value)
            return kind, identifier
    return "event", event.id


def _derived_identities(event: Event) -> list[tuple[str, str]]:
    if event.type is EventType.ANALYSIS_ENTITY_DETECTED:
        identifier = event.payload.get("entity_id")
        return [
            (
                "entity",
                str(identifier or f"ent_{event.id.removeprefix('evt_')}"),
            )
        ]
    if event.type is not EventType.ANALYSIS_CLAIM_DETECTED:
        return [_derived_identity(event)]
    values = event.payload.get("claims")
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        identifier = event.payload.get("claim_id")
        return [
            (
                "claim",
                str(identifier or f"clm_{event.id.removeprefix('evt_')}_000"),
            )
        ]
    identities: list[tuple[str, str]] = []
    for index, value in enumerate(values):
        if isinstance(value, Mapping):
            identifier = value.get("id") or value.get("claim_id")
        else:
            identifier = None
        if identifier is None and len(values) == 1:
            identifier = event.payload.get("claim_id")
        identities.append(
            (
                "claim",
                str(identifier or f"clm_{event.id.removeprefix('evt_')}_{index:03d}"),
            )
        )
    return identities or [_derived_identity(event)]


def _source_pairs(event: Event) -> list[tuple[str, str]]:
    """Extract source IDs and relations from one event payload."""
    payload = event.payload
    explicit_relation = str(payload.get("provenance_relation", "derived_from"))
    pairs: list[tuple[str, str]] = []

    single = payload.get("source_event_id")
    if isinstance(single, str):
        pairs.append((single, explicit_relation))
    for key, relation in (
        ("source_event_ids", explicit_relation),
        ("provenance", explicit_relation),
        ("evidence_event_ids", "supports"),
    ):
        values = payload.get(key, ())
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
            pairs.extend((str(value), relation) for value in values)

    target = payload.get("target_event_id") or payload.get("event_id")
    relation_by_type = {
        EventType.HUMAN_STAR: ProvenanceRelation.STARRED.value,
        EventType.HUMAN_KEEP: ProvenanceRelation.KEPT.value,
        EventType.ANALYSIS_CONTRADICTION_DETECTED: ProvenanceRelation.CONTRADICTS.value,
    }
    if isinstance(target, str) and event.type in relation_by_type:
        pairs.append((target, relation_by_type[event.type]))

    derived_prefixes = ("analysis.", "claim.", "entity.", "relation.", "virtual_")
    if event.causation_id is not None and event.type.value.startswith(derived_prefixes):
        pairs.append((event.causation_id, ProvenanceRelation.CAUSED_BY.value))

    # Stable de-duplication preserves explicit relation choices.
    return list(dict.fromkeys(pair for pair in pairs if pair[0] != event.id))


class ProvenanceProjection:
    """Projection plugin that derives provenance edges from event payloads."""

    name = "provenance"
    tables = ("provenance_edges",)

    def apply(self, connection: sqlite3.Connection, event: Event) -> None:
        sources = _source_pairs(event)
        for derived_kind, derived_id in _derived_identities(event):
            for source_event_id, relation in sources:
                exists = connection.execute(
                    "SELECT 1 FROM events WHERE id = ?", (source_event_id,)
                ).fetchone()
                if exists is None:
                    raise ValueError(
                        f"provenance source {source_event_id!r} does not exist for {event.id}"
                    )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO provenance_edges (
                        derived_kind, derived_id, source_event_id, relation,
                        created_event_id, branch_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        derived_kind,
                        derived_id,
                        source_event_id,
                        relation,
                        event.id,
                        event.branch_id,
                    ),
                )


class ProvenanceService:
    """Record and query explicit source relationships.

    :meth:`link` appends a ``relation.created`` event rather than mutating the
    projection directly.  The resulting graph therefore survives a complete
    projection rebuild.
    """

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def link(
        self,
        derived_kind: str,
        derived_id: str,
        source_event_id: str,
        *,
        relation: ProvenanceRelation | str = ProvenanceRelation.DERIVED_FROM,
        actor: Actor | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
        correlation_id: str | None = None,
    ) -> ProvenanceEdge:
        """Append one provenance relation and return its projected edge."""
        source = self.store.require(source_event_id)
        relation_value = relation.value if isinstance(relation, ProvenanceRelation) else relation
        event = Event(
            type=EventType.RELATION_CREATED,
            actor=actor or Actor(kind=ActorKind.SYSTEM, id="provenance"),
            session_id=session_id if session_id is not None else source.session_id,
            branch_id=branch_id if branch_id is not None else source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=correlation_id or source.correlation_id,
            payload={
                "relation_type": "provenance",
                "derived_kind": derived_kind,
                "derived_id": derived_id,
                "source_event_id": source.id,
                "provenance_relation": relation_value,
            },
        )
        self.store.append(event)
        row = self.store.connection.execute(
            "SELECT * FROM provenance_edges WHERE created_event_id = ? ORDER BY id DESC LIMIT 1",
            (event.id,),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"provenance projection did not apply for {event.id}")
        return self._row_to_edge(row)

    def edges_for(self, derived_kind: str, derived_id: str) -> list[ProvenanceEdge]:
        """Return all direct source edges for a derived record."""
        rows = self.store.connection.execute(
            """
            SELECT * FROM provenance_edges
            WHERE derived_kind = ? AND derived_id = ?
            ORDER BY id ASC
            """,
            (derived_kind, derived_id),
        ).fetchall()
        return [self._row_to_edge(row) for row in rows]

    def edges_from_event(self, source_event_id: str) -> list[ProvenanceEdge]:
        """Return every derived record that cites a source event."""
        rows = self.store.connection.execute(
            "SELECT * FROM provenance_edges WHERE source_event_id = ? ORDER BY id ASC",
            (source_event_id,),
        ).fetchall()
        return [self._row_to_edge(row) for row in rows]

    def edges_for_event(self, event_id: str) -> list[ProvenanceEdge]:
        """Return direct source edges declared for or created by an event.

        Analysis and virtual-world events usually project an artifact identity
        such as ``claim`` or ``virtual_file`` rather than ``event``.  Looking up
        only ``("event", event_id)`` therefore loses their explicit
        ``source_event_ids``.  ``created_event_id`` is the durable bridge back
        to the event that emitted those edges.
        """
        self.store.require(event_id)
        rows = self.store.connection.execute(
            """
            SELECT * FROM provenance_edges
            WHERE (derived_kind = 'event' AND derived_id = ?)
               OR created_event_id = ?
            ORDER BY id ASC
            """,
            (event_id, event_id),
        ).fetchall()
        return [self._row_to_edge(row) for row in rows]

    def trace(
        self, derived_kind: str, derived_id: str, *, include_event_lineage: bool = True
    ) -> list[ProvenanceOrigin]:
        """Breadth-first trace from a derived record to all source events."""
        seeds = (
            (edge.source_event_id, edge.relation, 1)
            for edge in self.edges_for(derived_kind, derived_id)
        )
        return self._trace_event_queue(seeds, include_event_lineage=include_event_lineage)

    def trace_event(
        self, event_id: str, *, include_event_lineage: bool = True
    ) -> list[ProvenanceOrigin]:
        """Breadth-first trace of an event's explicit sources and lineage."""
        event = self.store.require(event_id)
        seeds: list[tuple[str, str, int]] = [
            (edge.source_event_id, edge.relation, 1) for edge in self.edges_for_event(event.id)
        ]
        if include_event_lineage:
            seeds.extend(self._envelope_sources(event, depth=1))
        return self._trace_event_queue(seeds, include_event_lineage=include_event_lineage)

    def _trace_event_queue(
        self,
        seeds: Sequence[tuple[str, str, int]],
        *,
        include_event_lineage: bool,
    ) -> list[ProvenanceOrigin]:
        """Walk source events once, retaining the shortest discovered path."""
        queue = deque(seeds)
        seen_events: set[str] = set()
        origins: list[ProvenanceOrigin] = []
        while queue:
            event_id, relation, depth = queue.popleft()
            if event_id in seen_events:
                continue
            event = self.store.require(event_id)
            seen_events.add(event.id)
            origins.append(ProvenanceOrigin(event=event, relation=relation, depth=depth))

            links = [
                (edge.source_event_id, edge.relation, depth + 1)
                for edge in self.edges_for_event(event.id)
            ]
            if include_event_lineage:
                links.extend(self._envelope_sources(event, depth=depth + 1))
            queue.extend(links)
        return origins

    @staticmethod
    def _envelope_sources(event: Event, *, depth: int) -> list[tuple[str, str, int]]:
        """Return distinct causal and narrative parents in precedence order."""
        sources: list[tuple[str, str, int]] = []
        seen: set[str] = set()
        for relation, source_id in (
            (ProvenanceRelation.CAUSED_BY.value, event.causation_id),
            ("parent", event.parent_event_id),
        ):
            if source_id is None or source_id == event.id or source_id in seen:
                continue
            sources.append((source_id, relation, depth))
            seen.add(source_id)
        return sources

    def actor_origins(self, derived_kind: str, derived_id: str) -> frozenset[str]:
        """Return actor kinds responsible for a derived record and its sources."""
        actors = {origin.event.actor.kind.value for origin in self.trace(derived_kind, derived_id)}
        actors.update(
            self.store.require(edge.created_event_id).actor.kind.value
            for edge in self.edges_for(derived_kind, derived_id)
        )
        return frozenset(actors)

    def actor_origins_for_event(self, event_id: str) -> frozenset[str]:
        """Return actor kinds represented in an event provenance trace."""
        event = self.store.require(event_id)
        actors = {event.actor.kind.value}
        actors.update(origin.event.actor.kind.value for origin in self.trace_event(event_id))
        actors.update(
            self.store.require(edge.created_event_id).actor.kind.value
            for edge in self.edges_for_event(event_id)
        )
        return frozenset(actors)

    def validate(self) -> list[str]:
        """Return dangling-source or event-reference problems."""
        problems: list[str] = []
        rows = self.store.connection.execute(
            """
            SELECT p.id, p.source_event_id, p.created_event_id
            FROM provenance_edges p
            LEFT JOIN events source ON source.id = p.source_event_id
            LEFT JOIN events created ON created.id = p.created_event_id
            WHERE source.id IS NULL OR created.id IS NULL
            """
        ).fetchall()
        for row in rows:
            problems.append(
                f"provenance edge {row['id']} has missing source/creator "
                f"({row['source_event_id']}, {row['created_event_id']})"
            )
        return problems

    @staticmethod
    def _row_to_edge(row: sqlite3.Row) -> ProvenanceEdge:
        return ProvenanceEdge(
            id=row["id"],
            derived_kind=row["derived_kind"],
            derived_id=row["derived_id"],
            source_event_id=row["source_event_id"],
            relation=row["relation"],
            created_event_id=row["created_event_id"],
            branch_id=row["branch_id"],
        )


__all__ = [
    "ProvenanceEdge",
    "ProvenanceOrigin",
    "ProvenanceProjection",
    "ProvenanceRelation",
    "ProvenanceService",
]
