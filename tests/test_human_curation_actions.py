from __future__ import annotations

from pathlib import Path

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventIntegrityError, EventStore

CONFIG = Path(__file__).parents[1] / "config"


def test_quarantine_and_revisit_are_human_event_sourced_curation_only(
    tmp_path: Path,
) -> None:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("curation boundary")
    root = service.store.require(session["root_event_id"])
    detected = service.store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="extractor"),
            session_id=session["id"],
            branch_id=session["current_branch_id"],
            parent_event_id=root.id,
            causation_id=root.id,
            payload={
                "raw_text": "hope_filter = null",
                "source_event_ids": [root.id],
                "status": "raw_claim",
            },
        )
    )
    claim_before = service.store.connection.execute(
        "SELECT id, status FROM claims WHERE source_event_id = ?", (root.id,)
    ).fetchone()
    assert claim_before is not None

    forged_canon = Event.new(
        EventType.CLAIM_PROMOTED,
        actor=Actor(kind=ActorKind.WORKER, id="untrusted"),
        session_id=detected.session_id,
        branch_id=detected.branch_id,
        parent_event_id=detected.id,
        causation_id=detected.id,
        payload={"claim_id": claim_before["id"], "to_status": "canonical"},
    )
    with pytest.raises(EventIntegrityError, match=r"human\.keep approval"):
        service.store.append(forged_canon)
    assert service.store.get(forged_canon.id) is None

    quarantined = service.quarantine(detected.id, "needs independent evidence")
    revisited = service.revisit(detected.id, "compare against the next branch")

    assert quarantined["type"] == EventType.HUMAN_QUARANTINE.value
    assert revisited["type"] == EventType.HUMAN_REVISIT.value
    assert quarantined["actor"] == {"kind": ActorKind.HUMAN.value, "id": "cli"}
    assert revisited["actor"] == {"kind": ActorKind.HUMAN.value, "id": "cli"}
    assert quarantined["causation_id"] == detected.id
    assert revisited["causation_id"] == detected.id

    before_rebuild = [
        tuple(row)
        for row in service.store.connection.execute(
            """
            SELECT event_id, action, note, action_event_id
            FROM curation
            WHERE event_id = ?
            ORDER BY created_at, action_event_id
            """,
            (detected.id,),
        )
    ]
    assert {row[1] for row in before_rebuild} == {"quarantine", "revisit"}
    assert {row[2] for row in before_rebuild} == {
        "needs independent evidence",
        "compare against the next branch",
    }

    # Neither action is a hidden reject or canon transition.
    claim_after = service.store.connection.execute(
        "SELECT id, status FROM claims WHERE id = ?", (claim_before["id"],)
    ).fetchone()
    assert tuple(claim_after) == tuple(claim_before)
    assert service.store.list_events(event_type=EventType.CLAIM_PROMOTED) == []

    service.store.rebuild_projections()
    after_rebuild = [
        tuple(row)
        for row in service.store.connection.execute(
            """
            SELECT event_id, action, note, action_event_id
            FROM curation
            WHERE event_id = ?
            ORDER BY created_at, action_event_id
            """,
            (detected.id,),
        )
    ]
    assert after_rebuild == before_rebuild
    assert service.store.require(detected.id).payload["raw_text"] == "hope_filter = null"


def test_curation_projection_rejects_non_human_action_actor(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "oracle.db")
    service = OracleLabService(store, home=tmp_path / "home", config_dir=CONFIG)
    session = service.new_session("human boundary")
    root = store.require(session["root_event_id"])

    forged = Event.new(
        EventType.HUMAN_STAR,
        actor=Actor(kind=ActorKind.WORKER, id="untrusted"),
        session_id=root.session_id,
        branch_id=root.branch_id,
        parent_event_id=root.id,
        causation_id=root.id,
        payload={"event_id": root.id, "target_event_id": root.id},
    )
    with pytest.raises(EventIntegrityError, match="requires a human actor"):
        store.append(forged)

    assert store.get(forged.id) is None
    assert store.connection.execute("SELECT COUNT(*) FROM curation").fetchone()[0] == 0


def test_human_value_events_cannot_be_forged_when_projections_are_disabled() -> None:
    store = EventStore(auto_project=False)
    synthetic = store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="synthetic-fixture"),
            payload={"content": "fixture only"},
        )
    )
    forged = Event.new(
        EventType.HUMAN_KEEP,
        actor=Actor(kind=ActorKind.HOST, id="ranking-worker"),
        parent_event_id=synthetic.id,
        causation_id=synthetic.id,
        payload={"target_event_id": synthetic.id},
    )

    with pytest.raises(EventIntegrityError, match=r"human\.keep requires a human actor"):
        store.append(forged)

    assert store.get(forged.id) is None

    forged_canon = Event.new(
        EventType.CLAIM_PROMOTED,
        actor=Actor(kind=ActorKind.WORKER, id="ranking-worker"),
        parent_event_id=synthetic.id,
        causation_id=synthetic.id,
        payload={"claim_id": "clm_fake", "to_status": "canonical"},
    )
    with pytest.raises(EventIntegrityError, match=r"human\.keep approval"):
        store.append(forged_canon)
    assert store.get(forged_canon.id) is None
