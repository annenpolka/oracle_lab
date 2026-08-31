from __future__ import annotations

from oracle_lab.branching import BranchService
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.projections import ClaimStatus, VirtualStateService
from oracle_lab.store import EventStore
from oracle_lab.virtual import SourceEvidence, VirtualNodeKind, VirtualWorldRuntime


def _append_claim_analysis(store: EventStore, source: Event) -> Event:
    return store.append(
        Event(
            type="analysis.claim_detected",
            actor=Actor(kind="host", id="extractor"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload={
                "claims": [
                    {
                        "subject": "pain_phase",
                        "predicate": "equals",
                        "object": 34.7,
                        "raw": "pain phase = 34.7°",
                    },
                    "hope_filter = null",
                ],
                "source_event_ids": [source.id],
                "confidence": 0.9,
                "rationale": "fixture extraction",
            },
        )
    )


def test_claim_array_lifecycle_is_non_boolean_and_human_gated() -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    output = store.append(
        Event(
            type="human.input",
            actor=Actor(kind="human", id="researcher"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=session.root_event_id,
            causation_id=session.root_event_id,
            payload={"content": "pain phase = 34.7°\nhope_filter = null"},
        )
    )
    analysis = _append_claim_analysis(store, output)
    claims = store.connection.execute("SELECT * FROM claims ORDER BY id").fetchall()
    first_id = claims[0]["id"]
    assert len(claims) == 2
    assert {row["status"] for row in claims} == {ClaimStatus.RAW_CLAIM.value}

    candidate = store.append(
        Event(
            type="analysis.canon_candidate",
            actor=Actor(kind="host"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=analysis.id,
            causation_id=analysis.id,
            payload={"claim_id": first_id, "source_event_ids": [analysis.id]},
        )
    )
    assert (
        store.connection.execute(
            "SELECT status FROM branch_claim_states WHERE claim_id = ?", (first_id,)
        ).fetchone()[0]
        == ClaimStatus.RAW_CLAIM.value
    )

    store.append(
        Event(
            type="human.keep",
            actor=Actor(kind="human"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=candidate.id,
            causation_id=candidate.id,
            payload={"claim_id": first_id, "target_event_id": output.id},
        )
    )
    assert (
        store.connection.execute(
            "SELECT status FROM branch_claim_states WHERE claim_id = ?", (first_id,)
        ).fetchone()[0]
        == ClaimStatus.CANONICAL.value
    )
    assert store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 2


def test_projection_rebuild_restores_claims_virtual_world_and_provenance() -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    source = store.require(session.root_event_id)
    analysis = _append_claim_analysis(store, source)

    virtual = VirtualStateService(store)
    runtime = VirtualWorldRuntime(
        mutation_sink=virtual.mutation_sink(
            session_id=session.id, branch_id=session.current_branch_id
        )
    )
    evidence = SourceEvidence((source.id,), "explicit")
    runtime.fs.create(
        "/dev/void",
        evidence=evidence,
        kind=VirtualNodeKind.CHARACTER_DEVICE,
        content="consciousness",
    )
    before_claims = [
        tuple(row)
        for row in store.connection.execute(
            "SELECT id, source_event_id, raw_text, status FROM claims ORDER BY id"
        )
    ]
    before_edges = [
        tuple(row)
        for row in store.connection.execute(
            """
            SELECT derived_kind, derived_id, source_event_id, relation
            FROM provenance_edges ORDER BY id
            """
        )
    ]

    store.rebuild_projections()

    after_claims = [
        tuple(row)
        for row in store.connection.execute(
            "SELECT id, source_event_id, raw_text, status FROM claims ORDER BY id"
        )
    ]
    after_edges = [
        tuple(row)
        for row in store.connection.execute(
            """
            SELECT derived_kind, derived_id, source_event_id, relation
            FROM provenance_edges ORDER BY id
            """
        )
    ]
    assert after_claims == before_claims
    assert after_edges == before_edges
    assert virtual.hydrate(session.current_branch_id).fs.cat("/dev/void") == "consciousness"
    assert store.require(analysis.id).payload["claims"][0]["raw"] == "pain phase = 34.7°"


def test_virtual_clock_fork_inherits_then_diverges_without_cross_branch_repair() -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session(title="clock fork")
    root = store.require(session.root_event_id)
    virtual = VirtualStateService(store)
    main_runtime = VirtualWorldRuntime(
        mutation_sink=virtual.mutation_sink(
            session_id=session.id,
            branch_id=session.current_branch_id,
        )
    )
    evidence = SourceEvidence((root.id,), "explicit")
    main_runtime.clocks.create("observer", evidence=evidence)
    main_runtime.clocks.set("observer", "10", "pulse", evidence=evidence)
    fork_point = store.list_events(event_type=EventType.VIRTUAL_CLOCK_SET)[0]

    child = branches.fork(fork_point.id, title="advance clock")
    child_clock = virtual.hydrate(child.id).clocks.require("observer")
    assert child_clock.current_revision is not None
    assert child_clock.current_revision.value == "10"
    child_runtime = virtual.hydrate(
        child.id,
        mutation_sink=virtual.mutation_sink(session_id=session.id, branch_id=child.id),
    )
    child_runtime.clocks.advance("observer", "2", "pulse", evidence=evidence)

    assert (
        virtual.hydrate(session.current_branch_id).clocks.require("observer").current_revision.value
        == "10"
    )
    assert virtual.hydrate(child.id).clocks.require("observer").current_revision.value == "12"

    store.rebuild_projections()
    assert (
        virtual.hydrate(session.current_branch_id).clocks.require("observer").current_revision.value
        == "10"
    )
    assert virtual.hydrate(child.id).clocks.require("observer").current_revision.value == "12"


def test_synthetic_approval_reference_taints_research_projection_transitively() -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    root = store.require(session.root_event_id)
    synthetic = store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="synthetic-fixture"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={"content": "fixture only"},
        )
    )

    derived = store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="untrusted-derived-fixture"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={
                "claims": [{"raw": "must remain synthetic"}],
                "source_event_ids": [root.id],
                "approver_event_id": synthetic.id,
            },
        )
    )

    assert store.get(derived.id) is not None
    assert store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0

    store.rebuild_projections()

    assert store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
