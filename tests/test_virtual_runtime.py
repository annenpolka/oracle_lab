from __future__ import annotations

import json

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.projections import VirtualStateService
from oracle_lab.store import EventStore
from oracle_lab.virtual import (
    EventBackedVirtualWorld,
    SourceEvidence,
    VirtualNodeKind,
    VirtualNotFoundError,
    VirtualWorldError,
    VirtualWorldRuntime,
)


def _source(store: EventStore, text: str, parent: Event | None = None) -> Event:
    event = Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="tester"),
        session_id="ses_virtual",
        branch_id="br_main",
        parent_event_id=None if parent is None else parent.id,
        causation_id=None if parent is None else parent.id,
        payload={"content": text},
    )
    return store.append(event)


def test_virtual_state_is_provenance_backed_and_survives_hydration() -> None:
    store = EventStore()
    introduced = _source(store, "There is a /dev/void device and reality_monitor process.")
    service = VirtualStateService(store)
    sink = service.mutation_sink(
        session_id="ses_virtual",
        branch_id="br_main",
        actor=Actor(kind=ActorKind.TOOL, id="virtual-runtime"),
    )
    runtime = VirtualWorldRuntime(mutation_sink=sink)
    evidence = SourceEvidence((introduced.id,), "explicit")

    runtime.fs.create(
        "/dev/void",
        evidence=evidence,
        kind=VirtualNodeKind.CHARACTER_DEVICE,
        content="observer interface",
        unresolved_fields=("major", "minor"),
    )
    runtime.commands.register(
        "reality_monitor",
        "0.31",
        ("--target", "--precision"),
        evidence=evidence,
    )
    process = runtime.processes.create(
        "reality_monitor",
        ("--target", "/dev/void"),
        evidence=evidence,
        event_callbacks={"TERM": "observer.stopped"},
    )
    runtime.signal(process.pid, "TERM", evidence=evidence)

    restarted = service.hydrate("br_main", mutation_sink=sink)
    assert restarted.fs.cat("/dev/void") == "observer interface"
    assert restarted.fs.stat("/dev/void")["provenance"] == [introduced.id]
    assert restarted.commands.require("reality_monitor").version == "0.31"
    assert restarted.processes.require(process.pid).state == "terminated"
    assert restarted.processes.require(process.pid).signals == ["TERM"]

    assert store.list_events(event_type=EventType.VIRTUAL_FILE_CREATED)
    assert store.list_events(event_type=EventType.VIRTUAL_PROCESS_CREATED)
    assert store.list_events(event_type=EventType.VIRTUAL_PROCESS_SIGNAL_RECEIVED)
    assert service.snapshot("br_other") == {"nodes": []}


def test_reads_never_invent_missing_entities_and_synthesis_emits_event() -> None:
    store = EventStore()
    introduced = _source(store, "/dev/void exists but its major number is unknown.")
    service = VirtualStateService(store)
    sink = service.mutation_sink(session_id="ses_virtual", branch_id="br_main")
    runtime = VirtualWorldRuntime(mutation_sink=sink)
    runtime.fs.create(
        "/dev/void",
        evidence=SourceEvidence((introduced.id,), "implied"),
        kind=VirtualNodeKind.CHARACTER_DEVICE,
        unresolved_fields=("major",),
    )

    before = len(store.list_events())
    with pytest.raises(VirtualNotFoundError):
        runtime.fs.cat("/not/invented")
    assert len(store.list_events()) == before

    query = _source(store, "stat /dev/void", parent=store.list_events()[-1])
    runtime.fs.synthesize_detail(
        "/dev/void",
        "major",
        42,
        evidence=SourceEvidence((introduced.id, query.id), "synthesized"),
    )
    update = store.list_events(event_type=EventType.VIRTUAL_FILE_UPDATED)[-1]
    assert update.payload["source_event_ids"] == (introduced.id, query.id)
    assert service.hydrate("br_main").fs.stat("/dev/void")["properties"]["major"] == 42


def test_virtual_filesystem_command_semantics_and_versions() -> None:
    events = []
    runtime = VirtualWorldRuntime(mutation_sink=events.append)
    first = SourceEvidence(("evt_source_1",), "explicit")
    second = SourceEvidence(("evt_source_2",), "explicit")
    node = runtime.fs.create("/logs/day.txt", content="alpha\nbeta\n", evidence=first)
    runtime.fs.update_content("/logs/day.txt", "alpha\ngamma\n", evidence=second)

    assert runtime.fs.cat("/logs/day.txt") == "alpha\ngamma\n"
    assert runtime.fs.ls("/logs") == ["day.txt"]
    assert runtime.fs.find("/", "*.txt") == ["/logs/day.txt"]
    assert runtime.fs.grep("gamma", ["/logs/day.txt"]) == ["/logs/day.txt:2:gamma"]
    assert runtime.fs.stat("/logs/day.txt")["inode"] == node.inode
    assert runtime.fs.stat("/logs/day.txt")["content_versions"][-1]["version"] == 2
    assert len(events) >= 3  # implicit directory + file + update, all provenance events


def test_virtual_clock_query_never_invents_time_or_state() -> None:
    mutations = []
    runtime = VirtualWorldRuntime(mutation_sink=mutations.append)

    with pytest.raises(VirtualNotFoundError, match="virtual clock does not exist"):
        runtime.execute(
            "clock query observer",
            evidence=SourceEvidence(("evt_query",), "explicit"),
        )

    assert runtime.clocks.clocks == {}
    assert mutations == []


def test_virtual_clock_persists_unknowns_revisions_and_unresolved_contradiction() -> None:
    store = EventStore()
    created_by = _source(store, "clock create observer")
    first_set = _source(store, "clock set observer 10 pulse", parent=created_by)
    advanced_by = _source(store, "clock advance observer 2 pulse", parent=first_set)
    conflicting_set = _source(store, "clock set observer 7 pulse", parent=advanced_by)
    service = VirtualStateService(store)
    runtime = VirtualWorldRuntime(
        mutation_sink=service.mutation_sink(
            session_id="ses_virtual",
            branch_id="br_main",
            actor=Actor(kind=ActorKind.HOST, id="virtual-materializer"),
        )
    )

    clock = runtime.clocks.create(
        "observer",
        evidence=SourceEvidence((created_by.id,), "synthesized"),
    )
    assert clock.current_revision is None
    assert clock.unresolved_fields == {"unit", "value"}
    assert "created_at" not in clock.to_dict()
    runtime.clocks.set(
        "observer",
        "10",
        "pulse",
        evidence=SourceEvidence((first_set.id,), "explicit"),
    )
    runtime.clocks.advance(
        "observer",
        "2",
        "pulse",
        evidence=SourceEvidence((advanced_by.id,), "explicit"),
    )
    before_mismatch = len(store.list_events())
    with pytest.raises(VirtualWorldError, match="unit conversion is never inferred"):
        runtime.clocks.advance(
            "observer",
            "1",
            "second",
            evidence=SourceEvidence((advanced_by.id,), "explicit"),
        )
    assert len(store.list_events()) == before_mismatch
    runtime.clocks.set(
        "observer",
        "7",
        "pulse",
        evidence=SourceEvidence((conflicting_set.id,), "explicit"),
    )

    contradiction = store.list_events(event_type=EventType.VIRTUAL_CLOCK_CONTRADICTION_DETECTED)[-1]
    assert contradiction.payload["status"] == "unresolved"
    assert contradiction.payload["truth_domain"] == "virtual"
    assert contradiction.metadata["truth_domain"] == "virtual"
    assert set(contradiction.payload["source_event_ids"]) == {
        advanced_by.id,
        conflicting_set.id,
    }

    hydrated = service.hydrate("br_main")
    persisted = hydrated.clocks.require("observer")
    assert [item.value for item in persisted.revisions] == ["10", "12", "7"]
    assert persisted.contradictions[-1].prior_revision == 2
    assert persisted.contradictions[-1].conflicting_revision == 3

    store.rebuild_projections()
    rebuilt = service.hydrate("br_main").clocks.require("observer")
    assert rebuilt.to_dict() == persisted.to_dict()

    replayed = EventBackedVirtualWorld(
        store,
        session_id="ses_virtual",
        branch_id="br_main",
    )
    query = json.loads(
        replayed.execute(
            "clock query observer",
            evidence=SourceEvidence((conflicting_set.id,), "explicit"),
        )
    )
    assert query["value"] == "7"
    assert len(query["revisions"]) == 3
    assert query["contradictions"][-1]["status"] == "unresolved"
