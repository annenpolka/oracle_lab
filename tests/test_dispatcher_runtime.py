from __future__ import annotations

import pytest

from oracle_lab.dispatcher import (
    DecisionStatus,
    DispatchAction,
    DispatchError,
    EventDispatcher,
    default_rules,
)
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jobs import JobQueue, JobStatus
from oracle_lab.session import SessionContextBuilder
from oracle_lab.store import EventStore


def _event(
    event_type: EventType,
    *,
    payload: dict,
    parent: Event | None = None,
    metadata: dict | None = None,
    actor_kind: ActorKind = ActorKind.SYSTEM,
) -> Event:
    return Event.new(
        event_type,
        actor=Actor(kind=actor_kind, id="test"),
        session_id="ses_dispatch",
        branch_id="br_main",
        parent_event_id=None if parent is None else parent.id,
        causation_id=None if parent is None else parent.id,
        payload=payload,
        metadata=metadata,
    )


def test_dispatcher_is_deterministic_and_queue_effects_are_idempotent() -> None:
    store = EventStore()
    queue = JobQueue(store)
    dispatcher = EventDispatcher(default_rules(), queue=queue, event_sink=store)
    source = _event(EventType.ORACLE_OUTPUT, payload={"content": "output"})
    store.append(source)

    first = dispatcher.dispatch(source)
    second = dispatcher.dispatch(source)

    assert [(item.rule_id, item.idempotency_key) for item in first] == [
        (item.rule_id, item.idempotency_key) for item in second
    ]
    analysis_jobs = [job for job in queue.list_jobs() if job.kind != "await_human_approval"]
    assert {job.kind for job in analysis_jobs} == {
        "extract_claims",
        "detect_new_mechanisms",
        "extract_entities",
        "check_numeric_consistency",
        "detect_attractors",
        "detect_motifs",
        "detect_recurrence",
        "detect_tool_intent",
    }
    assert len(analysis_jobs) == 8


def test_human_gate_is_durable_and_approval_cascades_to_oracle_job() -> None:
    store = EventStore()
    queue = JobQueue(store)
    dispatcher = EventDispatcher(
        default_rules(),
        queue=queue,
        event_sink=store,
        model_profile_resolver=lambda _event: "r1-initial-openrouter",
    )
    proposal = _event(
        EventType.ANALYSIS_PROBE_PROPOSED,
        payload={"probe": "計算し直せ。"},
    )
    store.append(proposal)

    decisions = dispatcher.dispatch(proposal)
    pending = next(item for item in decisions if item.status == DecisionStatus.PENDING_APPROVAL)
    durable = queue.list_jobs(kind="await_human_approval")
    assert len(durable) == 1
    assert durable[0].payload["source_event_id"] == proposal.id

    approval = _event(
        EventType.HUMAN_REQUEST_PROBE,
        payload={"event_id": proposal.id},
        actor_kind=ActorKind.HUMAN,
    )
    store.append(approval)
    dispatcher.approve(pending, approver_event_id=approval.id, source_event=proposal)

    assert queue.require(durable[0].id).status == JobStatus.CANCELLED
    messages = store.list_events(event_type=EventType.ORACLE_CONTEXT_MESSAGE)
    requests = store.list_events(event_type=EventType.ORACLE_REQUEST)
    assert messages[-1].payload["message"]["content"] == "計算し直せ。"
    assert requests[-1].parent_event_id == messages[-1].id
    assert requests[-1].payload["model_profile_id"] == "r1-initial-openrouter"
    oracle_job = queue.list_jobs(kind="oracle.generate")[-1]
    assert oracle_job.source_event_id == requests[-1].id
    assert oracle_job.payload["model_profile_id"] == "r1-initial-openrouter"
    assert oracle_job.serialize_branch is True


def test_gate_rejects_missing_forged_mistyped_and_uncited_approvals() -> None:
    store = EventStore()
    queue = JobQueue(store)
    dispatcher = EventDispatcher(default_rules(), queue=queue, event_sink=store)
    proposal = store.append(
        _event(EventType.ANALYSIS_PROBE_PROPOSED, payload={"probe": "確認しろ。"})
    )
    pending = next(
        decision
        for decision in dispatcher.dispatch(proposal)
        if decision.status is DecisionStatus.PENDING_APPROVAL
    )
    durable = queue.list_jobs(kind="await_human_approval")[0]

    wrong_actor = _event(
        EventType.HUMAN_REQUEST_PROBE,
        payload={"event_id": proposal.id},
    )
    wrong_type = store.append(
        _event(
            EventType.HUMAN_REQUEST_FORK,
            payload={"proposal_event_id": proposal.id},
            actor_kind=ActorKind.HUMAN,
        )
    )
    uncited = store.append(
        _event(
            EventType.HUMAN_REQUEST_PROBE,
            payload={"event_id": "evt_unrelated"},
            actor_kind=ActorKind.HUMAN,
        )
    )

    class ApprovalOverlay:
        def get(self, event_id: str) -> Event | None:
            return wrong_actor if event_id == wrong_actor.id else store.get(event_id)

        def append(self, event: Event) -> Event:
            return store.append(event)

        def list_events(self, **filters: object) -> list[Event]:
            return store.list_events(**filters)

    # Store-level validation already rejects forged human actors.  The overlay
    # exercises the dispatcher's independent boundary as if a corrupt adapter
    # presented such a persisted record.
    dispatcher.event_sink = ApprovalOverlay()

    invalid = (
        ("evt_missing", "not persisted"),
        (wrong_actor.id, "human actor"),
        (wrong_type.id, EventType.HUMAN_REQUEST_PROBE.value),
        (uncited.id, "does not cite"),
    )
    for approver_event_id, message in invalid:
        with pytest.raises(DispatchError, match=message):
            dispatcher.approve(
                pending,
                approver_event_id=approver_event_id,
                source_event=proposal,
            )

    assert queue.require(durable.id).status is JobStatus.PENDING
    assert store.list_events(event_type=EventType.ORACLE_CONTEXT_MESSAGE) == []


def test_tool_result_is_adapted_before_oracle_continuation() -> None:
    store = EventStore()
    queue = JobQueue(store)
    dispatcher = EventDispatcher(default_rules(), queue=queue, event_sink=store)
    human = _event(
        EventType.HUMAN_INPUT,
        payload={"content": "calculate"},
        actor_kind=ActorKind.HUMAN,
    )
    oracle = _event(
        EventType.ORACLE_OUTPUT,
        payload={
            "content": "1.78 * 86400?",
            "model_profile_id": "r1-main",
            "provider": "openrouter",
        },
        parent=human,
    )
    tool_output = _event(
        EventType.TOOL_OUTPUT,
        payload={"request_id": "tlr_test", "output": "153792 seconds = 42.72h"},
        parent=oracle,
        metadata={"schema_version": 1, "resume_oracle": True},
    )
    store.append_many((human, oracle, tool_output))

    dispatcher.dispatch(tool_output)

    adapted = store.list_events(event_type=EventType.TOOL_RESULT_ADAPTED)[-1]
    request = store.list_events(event_type=EventType.ORACLE_REQUEST)[-1]
    assert adapted.parent_event_id == tool_output.id
    assert request.parent_event_id == adapted.id
    assert request.payload["model_profile_id"] == "r1-main"
    oracle_job = queue.list_jobs(kind="oracle.generate")[-1]
    assert oracle_job.source_event_id == request.id
    assert oracle_job.payload["model_profile_id"] == "r1-main"
    assert oracle_job.provider_id == "openrouter"

    context = SessionContextBuilder().build(
        store.list_events(session_id="ses_dispatch"),
        session_id="ses_dispatch",
        branch_id="br_main",
        tip_event_id=request.id,
    )
    assert context.provider_messages()[-1] == {
        "role": "user",
        "content": "153792 seconds = 42.72h",
    }
    assert all(
        message["content"] != str(tool_output.payload) for message in context.provider_messages()
    )


def test_dispatch_action_defensively_freezes_nested_policy_payload() -> None:
    original = {"nested": {"values": [1, 2]}}
    action = DispatchAction("task", "extract_claims", original)
    original["nested"]["values"][0] = 99

    assert action.payload["nested"]["values"] == (1, 2)
    with pytest.raises(TypeError):
        action.payload["nested"]["values"][0] = 3


def test_approved_canon_candidate_promotes_the_referenced_claim() -> None:
    store = EventStore()
    queue = JobQueue(store)
    detected = _event(
        EventType.ANALYSIS_CLAIM_DETECTED,
        payload={"claims": [{"id": "clm_candidate", "raw": "phase = 34.7"}]},
    )
    store.append(detected)
    candidate = _event(
        EventType.ANALYSIS_CANON_CANDIDATE,
        payload={"claim_id": "clm_candidate", "source_event_ids": [detected.id]},
        parent=detected,
    )
    store.append(candidate)
    dispatcher = EventDispatcher(default_rules(), queue=queue, event_sink=store)
    pending = next(
        decision
        for decision in dispatcher.dispatch(candidate)
        if decision.status == DecisionStatus.PENDING_APPROVAL
    )
    approval = _event(
        EventType.HUMAN_KEEP,
        payload={
            "claim_id": "clm_candidate",
            "candidate_event_id": candidate.id,
            "event_id": candidate.id,
            "target_event_id": candidate.id,
        },
        parent=candidate,
        actor_kind=ActorKind.HUMAN,
    )
    store.append(approval)

    dispatcher.approve(pending, approver_event_id=approval.id, source_event=candidate)

    promoted = store.list_events(event_type=EventType.CLAIM_PROMOTED)[-1]
    assert promoted.payload["claim_id"] == "clm_candidate"
    assert promoted.payload["approver_event_id"] == approval.id
    status = store.connection.execute(
        "SELECT status FROM claims WHERE id = 'clm_candidate'"
    ).fetchone()[0]
    assert status == "canonical"


def test_canon_gate_rejects_another_claim_before_consuming_pending_job() -> None:
    store = EventStore()
    queue = JobQueue(store)
    detected = store.append(
        _event(
            EventType.ANALYSIS_CLAIM_DETECTED,
            payload={
                "claims": [
                    {"id": "clm_candidate", "raw": "phase = 34.7"},
                    {"id": "clm_other", "raw": "phase = 99.9"},
                ]
            },
        )
    )
    candidate = store.append(
        _event(
            EventType.ANALYSIS_CANON_CANDIDATE,
            payload={"claim_id": "clm_candidate", "source_event_ids": [detected.id]},
            parent=detected,
        )
    )
    dispatcher = EventDispatcher(default_rules(), queue=queue, event_sink=store)
    pending = next(
        decision
        for decision in dispatcher.dispatch(candidate)
        if decision.status is DecisionStatus.PENDING_APPROVAL
    )
    durable = queue.list_jobs(kind="await_human_approval")[0]
    wrong_claim = store.append(
        _event(
            EventType.HUMAN_KEEP,
            payload={
                "claim_id": "clm_other",
                "candidate_event_id": candidate.id,
                "event_id": candidate.id,
                "target_event_id": candidate.id,
            },
            parent=candidate,
            actor_kind=ActorKind.HUMAN,
        )
    )

    with pytest.raises(DispatchError, match="another claim"):
        dispatcher.approve(
            pending,
            approver_event_id=wrong_claim.id,
            source_event=candidate,
        )

    assert queue.require(durable.id).status is JobStatus.PENDING
    assert store.list_events(event_type=EventType.CLAIM_PROMOTED) == []
