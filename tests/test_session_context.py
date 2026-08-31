from __future__ import annotations

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.session import ContextConstructionError, SessionContextBuilder

ACTOR = Actor(kind=ActorKind.HUMAN, id="tester")


def _event(
    event_type: EventType,
    payload: dict,
    *,
    parent: Event | None = None,
    branch: str = "br_main",
    actor: Actor = ACTOR,
) -> Event:
    return Event.new(
        event_type,
        actor=actor,
        session_id="ses_1",
        branch_id=branch,
        parent_event_id=None if parent is None else parent.id,
        causation_id=None if parent is None else parent.id,
        payload=payload,
    )


def test_context_applies_visibility_rejection_and_reasoning_rules() -> None:
    human = _event(EventType.HUMAN_INPUT, {"content": "question"})
    note = _event(EventType.HUMAN_NOTE, {"content": "private note"}, parent=human)
    output = _event(
        EventType.ORACLE_OUTPUT,
        {"content": "rejected answer", "reasoning": {"secret": [1, 2]}},
        parent=note,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
    )
    reject = _event(EventType.HUMAN_REJECT, {"target_event_id": output.id}, parent=output)
    analysis = _event(
        EventType.ANALYSIS_PROBE_PROPOSED,
        {"content": "host-only"},
        parent=reject,
        actor=Actor(kind=ActorKind.HOST, id="host"),
    )
    tool = _event(
        EventType.TOOL_OUTPUT,
        {"output": "raw tool output"},
        parent=analysis,
        actor=Actor(kind=ActorKind.TOOL, id="calculator"),
    )
    adapted = _event(
        EventType.TOOL_RESULT_ADAPTED,
        {"message": {"role": "user", "content": {"result": [42]}}},
        parent=tool,
        actor=Actor(kind=ActorKind.SYSTEM, id="dispatcher"),
    )

    context = SessionContextBuilder().build(
        [human, note, output, reject, analysis, tool, adapted],
        session_id="ses_1",
        branch_id="br_main",
        tip_event_id=adapted.id,
    )

    assert context.provider_messages() == [
        {"role": "user", "content": "question"},
        {"role": "user", "content": {"result": [42]}},
    ]
    assert context.source_event_ids == (human.id, adapted.id)
    with pytest.raises(TypeError):
        context.messages[1]["content"]["result"][0] = 0


def test_fork_from_rejected_output_explicitly_restores_it() -> None:
    human = _event(EventType.HUMAN_INPUT, {"content": "question"})
    output = _event(
        EventType.ORACLE_OUTPUT,
        {"content": "interesting rejection"},
        parent=human,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
    )
    reject = _event(EventType.HUMAN_REJECT, {"target_event_id": output.id}, parent=output)
    fork = _event(
        EventType.SESSION_FORKED,
        {"from_event_id": output.id},
        parent=output,
        branch="br_child",
        actor=Actor(kind=ActorKind.SYSTEM, id="forker"),
    )
    child = _event(
        EventType.HUMAN_INPUT,
        {"content": "continue here"},
        parent=fork,
        branch="br_child",
    )

    context = SessionContextBuilder().build(
        [human, output, reject, fork, child],
        session_id="ses_1",
        branch_id="br_child",
        tip_event_id=child.id,
    )

    assert [message["content"] for message in context.provider_messages()] == [
        "question",
        "interesting rejection",
        "continue here",
    ]


def test_reject_after_historical_tip_does_not_change_replayed_context() -> None:
    human = _event(EventType.HUMAN_INPUT, {"content": "question"})
    output = _event(
        EventType.ORACLE_OUTPUT,
        {"content": "answer at this tip"},
        parent=human,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
    )
    reject = _event(EventType.HUMAN_REJECT, {"target_event_id": output.id}, parent=output)
    builder = SessionContextBuilder()

    before_reject = builder.build(
        [human, output, reject],
        session_id="ses_1",
        branch_id="br_main",
        tip_event_id=output.id,
    )
    after_reject = builder.build(
        [human, output, reject],
        session_id="ses_1",
        branch_id="br_main",
        tip_event_id=reject.id,
    )

    assert [message["content"] for message in before_reject.provider_messages()] == [
        "question",
        "answer at this tip",
    ]
    assert [message["content"] for message in after_reject.provider_messages()] == ["question"]


def test_sibling_branch_tip_is_rejected_and_hash_is_stable() -> None:
    root = _event(EventType.HUMAN_INPUT, {"content": "root"})
    sibling = _event(EventType.HUMAN_INPUT, {"content": "sibling"}, parent=root, branch="br_b")
    builder = SessionContextBuilder()

    with pytest.raises(ContextConstructionError, match="sibling branch"):
        builder.build(
            [root, sibling],
            session_id="ses_1",
            branch_id="br_a",
            tip_event_id=sibling.id,
        )

    first = builder.build([root], session_id="ses_1", branch_id="br_main")
    second = builder.build([root], session_id="ses_1", branch_id="br_main")
    changed = _event(EventType.HUMAN_INPUT, {"content": "root!"})
    third = builder.build([changed], session_id="ses_1", branch_id="br_main")
    assert first.sha256 == second.sha256
    assert first.sha256 != third.sha256


def test_context_truncation_preserves_system_and_records_removed_sources() -> None:
    configuration = _event(
        EventType.SESSION_CHECKPOINTED,
        {"operation": "configuration.snapshot"},
    )
    first = _event(
        EventType.HUMAN_INPUT,
        {"content": "  first\n"},
        parent=configuration,
    )
    answer = _event(
        EventType.ORACLE_OUTPUT,
        {"content": "answer"},
        parent=first,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
    )
    latest = _event(EventType.HUMAN_INPUT, {"content": " latest "}, parent=answer)

    context = SessionContextBuilder().build(
        [configuration, first, answer, latest],
        session_id="ses_1",
        branch_id="br_main",
        tip_event_id=latest.id,
        system_prompt="system",
        system_prompt_source_event_id=configuration.id,
        max_messages=2,
    )

    assert context.provider_messages() == [
        {"role": "system", "content": "system"},
        {"role": "user", "content": " latest "},
    ]
    assert context.source_event_ids == (configuration.id, latest.id)
    assert context.truncated_source_event_ids == (first.id, answer.id)
    assert context.original_message_count == 4
    assert context.truncation_strategy == "preserve_system_keep_newest"
    assert context.event_payload()["truncated"] is True
