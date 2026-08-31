from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.store import EventIntegrityError, EventStore


def _source(
    store: EventStore,
    *,
    session_id: str = "ses_canon",
    branch_id: str = "br_main",
) -> Event:
    return store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session_id,
            branch_id=branch_id,
            payload={"content": "確認しろ。"},
        )
    )


def _candidate(
    store: EventStore,
    source: Event,
    *,
    claim_id: str = "clm_canon",
    payload_extra: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> Event:
    return store.append(
        Event.new(
            EventType.ANALYSIS_CANON_CANDIDATE,
            actor=Actor(kind=ActorKind.HOST, id="canon-review"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload={
                "claim_id": claim_id,
                "source_event_ids": [source.id],
                **dict(payload_extra or {}),
            },
            metadata=metadata,
        )
    )


def _approval(
    store: EventStore,
    candidate: Event,
    *,
    claim_id: str | None = "clm_canon",
    candidate_event_id: str | None = None,
    include_candidate_event_id: bool = True,
    target_event_id: str | None = None,
    event_id: str | None = None,
    session_id: str | None = None,
    branch_id: str | None = None,
    parent_event_id: str | None = None,
    causation_id: str | None = None,
) -> Event:
    payload: dict[str, Any] = {
        "event_id": candidate.id if event_id is None else event_id,
        "target_event_id": candidate.id if target_event_id is None else target_event_id,
    }
    if include_candidate_event_id:
        payload["candidate_event_id"] = (
            candidate.id if candidate_event_id is None else candidate_event_id
        )
    if claim_id is not None:
        payload["claim_id"] = claim_id
    return store.append(
        Event.new(
            EventType.HUMAN_KEEP,
            actor=Actor(kind=ActorKind.HUMAN, id="curator"),
            session_id=candidate.session_id if session_id is None else session_id,
            branch_id=candidate.branch_id if branch_id is None else branch_id,
            parent_event_id=candidate.id if parent_event_id is None else parent_event_id,
            causation_id=candidate.id if causation_id is None else causation_id,
            correlation_id=candidate.correlation_id,
            payload=payload,
        )
    )


def _promotion(
    candidate: Event,
    approval: Event | None,
    *,
    actor_kind: ActorKind = ActorKind.SYSTEM,
    claim_id: str = "clm_canon",
    candidate_event_id: str | None = None,
    session_id: str | None = None,
    branch_id: str | None = None,
    parent_event_id: str | None = None,
    causation_id: str | None = None,
) -> Event:
    payload: dict[str, Any] = {
        "claim_id": claim_id,
        "to_status": "canonical",
        "candidate_event_id": candidate.id if candidate_event_id is None else candidate_event_id,
        "source_event_id": candidate.id,
        "source_event_ids": [candidate.id],
    }
    if approval is not None:
        payload["approver_event_id"] = approval.id
    return Event.new(
        EventType.CLAIM_PROMOTED,
        actor=Actor(kind=actor_kind, id="promotion-path"),
        session_id=candidate.session_id if session_id is None else session_id,
        branch_id=candidate.branch_id if branch_id is None else branch_id,
        parent_event_id=candidate.id if parent_event_id is None else parent_event_id,
        causation_id=candidate.id if causation_id is None else causation_id,
        correlation_id=candidate.correlation_id,
        payload=payload,
    )


@pytest.mark.parametrize("actor_kind", [ActorKind.SYSTEM, ActorKind.HUMAN])
def test_store_accepts_only_fully_bound_human_approved_canon_promotion(
    actor_kind: ActorKind,
) -> None:
    store = EventStore(auto_project=False)
    candidate = _candidate(store, _source(store))
    approval = _approval(store, candidate)
    promotion = _promotion(candidate, approval, actor_kind=actor_kind)

    stored = store.append(promotion)

    assert stored.id == promotion.id
    assert stored.payload["approver_event_id"] == approval.id
    assert stored.payload["candidate_event_id"] == candidate.id


def test_store_rejects_human_direct_canon_promotion_without_approval() -> None:
    store = EventStore(auto_project=False)
    candidate = _candidate(store, _source(store))
    promotion = _promotion(candidate, None, actor_kind=ActorKind.HUMAN)

    with pytest.raises(EventIntegrityError, match=r"human\.keep approval"):
        store.append(promotion)

    assert store.get(promotion.id) is None


@pytest.mark.parametrize(
    ("approval_claim", "message"),
    [
        (None, "another claim"),
        ("clm_other", "another claim"),
    ],
)
def test_store_rejects_claimless_or_other_claim_keep_as_canon_authority(
    approval_claim: str | None,
    message: str,
) -> None:
    store = EventStore(auto_project=False)
    candidate = _candidate(store, _source(store))
    approval = _approval(store, candidate, claim_id=approval_claim)
    promotion = _promotion(candidate, approval)

    with pytest.raises(EventIntegrityError, match=message):
        store.append(promotion)

    assert store.get(promotion.id) is None


def test_store_rejects_approval_for_another_candidate() -> None:
    store = EventStore(auto_project=False)
    source = _source(store)
    approved_candidate = _candidate(store, source)
    other_candidate = _candidate(store, source)
    approval = _approval(store, approved_candidate)
    promotion = _promotion(other_candidate, approval)

    with pytest.raises(EventIntegrityError, match="another candidate"):
        store.append(promotion)

    assert store.get(promotion.id) is None


def test_store_rejects_candidate_for_another_claim() -> None:
    store = EventStore(auto_project=False)
    candidate = _candidate(store, _source(store), claim_id="clm_candidate_other")
    approval = _approval(store, candidate, claim_id="clm_canon")
    promotion = _promotion(candidate, approval, claim_id="clm_canon")

    with pytest.raises(EventIntegrityError, match="candidate references another claim"):
        store.append(promotion)

    assert store.get(promotion.id) is None


def test_store_rejects_keep_without_an_explicit_candidate_identity() -> None:
    store = EventStore(auto_project=False)
    candidate = _candidate(store, _source(store))
    approval = _approval(store, candidate, include_candidate_event_id=False)
    promotion = _promotion(candidate, approval)

    with pytest.raises(EventIntegrityError, match="existing canon candidate"):
        store.append(promotion)

    assert store.get(promotion.id) is None


@pytest.mark.parametrize(
    ("approval_changes", "promotion_changes", "message"),
    [
        ({"branch_id": "br_other"}, {}, "share a branch"),
        ({}, {"session_id": "ses_other"}, "share a branch"),
        ({"target_event_id": "source"}, {}, "does not target"),
        ({"causation_id": "source"}, {}, "does not target"),
        ({}, {"causation_id": "approval"}, "causal source"),
    ],
)
def test_store_rejects_cross_context_or_broken_causal_canon_binding(
    approval_changes: dict[str, str],
    promotion_changes: dict[str, str],
    message: str,
) -> None:
    store = EventStore(auto_project=False)
    source = _source(store)
    candidate = _candidate(store, source)
    resolved_approval = {
        key: source.id if value == "source" else value for key, value in approval_changes.items()
    }
    approval = _approval(store, candidate, **resolved_approval)
    resolved_promotion = {
        key: approval.id if value == "approval" else value
        for key, value in promotion_changes.items()
    }
    promotion = _promotion(candidate, approval, **resolved_promotion)

    with pytest.raises(EventIntegrityError, match=message):
        store.append(promotion)

    assert store.get(promotion.id) is None


@pytest.mark.parametrize("origin", ["synthetic_fixture", "worker_generated"])
def test_store_rejects_tainted_candidate_before_it_can_be_human_approved(origin: str) -> None:
    store = EventStore(auto_project=False)
    source = _source(store)
    candidate = _candidate(
        store,
        source,
        payload_extra=(
            {"material_origin": origin}
            if origin == "synthetic_fixture"
            else {"artifact_origin": origin}
        ),
        metadata=(
            {"schema_version": 1, "material_origin": origin}
            if origin == "synthetic_fixture"
            else {"schema_version": 1, "artifact_origin": origin}
        ),
    )

    message = "synthetic fixture lineage" if origin == "synthetic_fixture" else "worker-generated"
    with pytest.raises(EventIntegrityError, match=message):
        _approval(store, candidate)

    assert store.list_events(event_type=EventType.HUMAN_KEEP) == []
