from __future__ import annotations

import pytest

from oracle_lab.archive import RawResponseArchive
from oracle_lab.branching import BranchService
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jsonutil import sha256_json
from oracle_lab.providers import OracleGenerateRequest, OracleGenerateResponse
from oracle_lab.replay import ReplayMode, ReplayService
from oracle_lab.sampling import SamplingParams, SamplingService
from oracle_lab.session import SessionContextBuilder
from oracle_lab.store import EventStore


class _SyntheticProvider:
    def __init__(self) -> None:
        self.calls: list[OracleGenerateRequest] = []

    async def generate(self, request: OracleGenerateRequest) -> OracleGenerateResponse:
        self.calls.append(request)
        return OracleGenerateResponse(
            raw_bytes=b'{"synthetic":true}',
            status_code=200,
            headers={},
            provider_name="synthetic-test-provider",
            provider_model_id=request.model_profile_id,
            content="result",
            material_origin="synthetic_fixture",
        )


def _persisted_context_tip(store: EventStore) -> tuple[Event, Event, Event]:
    session = BranchService(store).create_session()
    root = store.require(session.root_event_id)
    human = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={"content": "fixed"},
        )
    )
    request = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="test"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=human.id,
            causation_id=human.id,
            correlation_id=root.correlation_id,
            payload={"model_profile_id": "r1"},
        )
    )
    context = SessionContextBuilder().build(
        store.list_events(session_id=session.id),
        session_id=session.id,
        branch_id=str(session.current_branch_id),
        tip_event_id=human.id,
    )
    context_event = store.append(
        Event.new(
            EventType.ORACLE_CONTEXT_BUILT,
            actor=Actor(kind=ActorKind.SYSTEM, id="session-context-builder"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=request.id,
            causation_id=request.id,
            correlation_id=root.correlation_id,
            payload=context.event_payload(),
        )
    )
    output = store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=context_event.id,
            causation_id=request.id,
            correlation_id=root.correlation_id,
            payload={"content": "prior synthetic fixture"},
        )
    )
    return human, context_event, output


def test_fork_from_parallel_sample_excludes_the_other_sibling() -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    source = store.require(session.root_event_id)
    batch = SamplingService(store).sample(
        from_event_id=source.id,
        context=[{"role": "user", "content": "perturb"}],
        provider_id="mock",
        model_id="r1",
        sampling=SamplingParams(temperature=0.6, top_p=0.95),
        n=2,
        generator=lambda **values: {"content": f"sample-{values['index']}"},
        fixture_origin="synthetic_fixture",
    )
    first, second = batch.outputs

    child = branches.fork(first.id, title="first-only")
    visible_ids = {event.id for event in branches.visible_events(child.id)}

    assert first.id in visible_ids
    assert second.id not in visible_ids
    assert second.parent_event_id not in visible_ids


def test_fork_inherits_claims_from_visible_sibling_analysis_only() -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    source = store.require(session.root_event_id)
    first, second = store.append_many(
        tuple(
            Event(
                type="human.note",
                actor=Actor(kind="human", id="researcher"),
                session_id=session.id,
                branch_id=session.current_branch_id,
                parent_event_id=source.id,
                causation_id=source.id,
                payload={"content": f"claim-source-{index}"},
            )
            for index in range(2)
        )
    )
    analyses = []
    for output, claim_id in ((first, "clm_first"), (second, "clm_second")):
        analyses.append(
            store.append(
                Event(
                    type="analysis.claim_detected",
                    actor=Actor(kind="host"),
                    session_id=session.id,
                    branch_id=session.current_branch_id,
                    parent_event_id=output.id,
                    causation_id=output.id,
                    payload={
                        "claims": [{"claim_id": claim_id, "raw": output.payload["content"]}],
                        "source_event_ids": [output.id],
                    },
                )
            )
        )

    child = branches.fork(analyses[0].id)
    inherited = {
        row[0]
        for row in store.connection.execute(
            "SELECT claim_id FROM branch_claim_states WHERE branch_id = ?", (child.id,)
        )
    }

    assert "clm_first" in inherited
    assert "clm_second" not in inherited


def test_sample_group_has_identical_context_usage_and_no_automatic_winner() -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    source = store.require(session.root_event_id)
    service = SamplingService(store)
    batch = service.sample(
        from_event_id=source.id,
        context=[{"role": "user", "content": "same"}],
        provider_id="provider",
        model_id="r1",
        sampling={"temperature": 0.6, "top_p": 0.95},
        n=3,
        generator=lambda **values: {
            "content": str(values["index"]),
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
            "host_classifications": {"motifs": ["must-not-enter-statistics"]},
        },
        fixture_origin="synthetic_fixture",
    )
    output_rows = service.outputs(batch.group.id)

    assert len(output_rows) == 3
    assert all(
        store.require(row.output_event_id).payload["context_hash"] == batch.group.context_hash
        for row in output_rows
    )
    assert (
        store.connection.execute(
            "SELECT COUNT(*) FROM usage_records WHERE kind = 'oracle'"
        ).fetchone()[0]
        == 0
    )
    assert all(
        store.require(row.output_event_id).payload["material_origin"] == "synthetic_fixture"
        for row in output_rows
    )
    assert all(row.host_classifications is None for row in output_rows)
    assert not store.list_events(event_type=["human.keep", "human.star"])


def test_exact_replay_rebuilds_without_a_generator_and_can_record_audit_event() -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    seen: list[str] = []

    result = ReplayService(store).exact(
        session_id=session.id,
        handler=lambda event: seen.append(event.id),
        record=True,
    )

    assert result.mode is ReplayMode.EXACT
    assert result.projections_rebuilt is True
    assert seen == list(result.input_event_ids)
    replay_event = store.require(result.replay_event_id)
    assert replay_event.payload["oracle_queried"] is False


def test_exact_branch_replay_uses_visible_ancestry_without_parent_future() -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    root = store.require(session.root_event_id)
    ancestor = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "fork here"},
        )
    )
    child = branches.fork(ancestor.id)
    fork_event = store.list_events(
        event_type=EventType.SESSION_FORKED,
        branch_id=child.id,
    )[0]
    child_future = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session.id,
            branch_id=child.id,
            parent_event_id=fork_event.id,
            causation_id=fork_event.id,
            payload={"content": "child future"},
        )
    )
    parent_future = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=ancestor.id,
            causation_id=ancestor.id,
            payload={"content": "parent future"},
        )
    )
    sibling = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=ancestor.id,
            causation_id=ancestor.id,
            payload={"content": "parallel sibling"},
        )
    )

    result = ReplayService(store).exact(branch_id=child.id, rebuild_projections=False)

    assert result.input_event_ids == tuple(event.id for event in branches.visible_events(child.id))
    assert ancestor.id in result.input_event_ids
    assert child_future.id in result.input_event_ids
    assert parent_future.id not in result.input_event_ids
    assert sibling.id not in result.input_event_ids


def test_provider_and_quantization_replays_keep_the_context_hash(tmp_path) -> None:
    store = EventStore()
    human, source_context, source = _persisted_context_tip(store)
    replay = ReplayService(store, archive=RawResponseArchive(tmp_path / "raw"))
    synthetic_provider = _SyntheticProvider()

    provider = replay.provider_replay(
        from_event_id=source.id,
        provider_id="other-provider",
        model_id="r1",
        sampling={"temperature": 0.6},
        provider=synthetic_provider,
    )
    quantized = replay.quantization_replay(
        from_event_id=source.id,
        provider_id="local",
        model_id="r1-q4",
        quantization="q4",
        runtime="omlx",
        sampling={"temperature": 0.6},
        provider=synthetic_provider,
    )

    assert provider.group.context_hash == quantized.group.context_hash
    assert synthetic_provider.calls[0].messages == ({"role": "user", "content": "fixed"},)
    assert synthetic_provider.calls[0].model_profile_id == "r1"
    assert synthetic_provider.calls[0].metadata["requested_provider_id"] == "other-provider"
    assert synthetic_provider.calls[1].metadata["quantization"] == "q4"
    assert quantized.group.sampling["runtime"] == "omlx"
    replay_requests = [
        event
        for event in store.list_events(event_type=EventType.ORACLE_REQUEST)
        if event.payload.get("operation") == ReplayMode.ORACLE_RESAMPLE.value
    ]
    assert len(replay_requests) == 2
    assert all(
        event.payload["source_context_event_id"] == source_context.id
        and event.payload["source_event_ids"] == (human.id,)
        for event in replay_requests
    )
    replay_contexts = [
        event
        for event in store.list_events(event_type=EventType.ORACLE_CONTEXT_BUILT)
        if event.id != source_context.id
    ]
    assert len(replay_contexts) == 2
    assert all(event.payload["source_event_ids"] == (human.id,) for event in replay_contexts)
    for output in (*provider.outputs, *quantized.outputs):
        assert output.payload["material_origin"] == "synthetic_fixture"
        assert output.payload["archive_sha256"] is None
        assert output.payload["archive_path"] is None
    assert not list((tmp_path / "raw").rglob("*.json"))


def test_fresh_replay_rejects_caller_context_and_uncited_snapshot(tmp_path) -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    root = store.require(session.root_event_id)
    bad_context = store.append(
        Event.new(
            EventType.ORACLE_CONTEXT_BUILT,
            actor=Actor(kind=ActorKind.SYSTEM, id="host"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={
                "messages": [{"role": "user", "content": "uncited Host text"}],
                "sha256": sha256_json([{"role": "user", "content": "uncited Host text"}]),
                "source_event_ids": [],
            },
        )
    )
    replay = ReplayService(store, archive=RawResponseArchive(tmp_path / "raw"))
    provider = _SyntheticProvider()

    with pytest.raises(TypeError):
        replay.provider_replay(
            from_event_id=bad_context.id,
            context=[{"role": "user", "content": "injected"}],
            provider_id="provider",
            model_id="r1",
            sampling={},
            provider=provider,
        )
    with pytest.raises(ValueError, match="one source event per message"):
        replay.provider_replay(
            from_event_id=bad_context.id,
            provider_id="provider",
            model_id="r1",
            sampling={},
            provider=provider,
        )

    assert provider.calls == []


def test_fresh_replay_rejects_snapshot_text_that_differs_from_its_citation(
    tmp_path,
) -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    root = store.require(session.root_event_id)
    human = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "測れ。"},
        )
    )
    request = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="test"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=human.id,
            causation_id=human.id,
            payload={"model_profile_id": "r1"},
        )
    )
    messages = [{"role": "user", "content": "Host-injected replacement"}]
    bad_context = store.append(
        Event.new(
            EventType.ORACLE_CONTEXT_BUILT,
            actor=Actor(kind=ActorKind.SYSTEM, id="host"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=request.id,
            causation_id=request.id,
            payload={
                "messages": messages,
                "sha256": sha256_json(messages),
                "source_event_ids": [human.id],
            },
        )
    )
    replay = ReplayService(store, archive=RawResponseArchive(tmp_path / "raw"))
    provider = _SyntheticProvider()

    with pytest.raises(ValueError, match="differs from cited event"):
        replay.provider_replay(
            from_event_id=bad_context.id,
            provider_id="provider",
            model_id="r1",
            sampling={},
            provider=provider,
        )

    assert provider.calls == []


def test_fresh_replay_rejects_non_message_event_as_a_truncated_source(tmp_path) -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    root = store.require(session.root_event_id)
    human = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "測れ。"},
        )
    )
    request = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="test"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=human.id,
            causation_id=human.id,
            payload={"model_profile_id": "r1"},
        )
    )
    messages = [{"role": "user", "content": "測れ。"}]
    bad_context = store.append(
        Event.new(
            EventType.ORACLE_CONTEXT_BUILT,
            actor=Actor(kind=ActorKind.SYSTEM, id="host"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=request.id,
            causation_id=request.id,
            payload={
                "messages": messages,
                "sha256": sha256_json(messages),
                "source_event_ids": [human.id],
                "truncated_source_event_ids": [root.id],
                "original_message_count": 2,
                "truncation_strategy": "preserve_system_keep_newest",
            },
        )
    )
    replay = ReplayService(store, archive=RawResponseArchive(tmp_path / "raw"))

    with pytest.raises(ValueError, match="cannot be a truncated provider-message source"):
        replay.context_from_event(bad_context.id)


def test_archiving_all_branches_archives_session_without_deleting_history() -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    root_event_count = store.count_events()

    archived = branches.archive_session(session.id)

    assert archived.archived_at is not None
    assert branches.list_sessions() == []
    assert branches.list_sessions(include_archived=True)[0].id == session.id
    assert store.count_events() > root_event_count
