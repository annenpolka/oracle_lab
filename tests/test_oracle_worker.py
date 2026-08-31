from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from oracle_lab.archive import RawResponseArchive
from oracle_lab.branching import BranchService
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jsonutil import sha256_json
from oracle_lab.oracle_worker import OracleWorker, OracleWorkerError
from oracle_lab.providers import (
    OracleGenerateRequest,
    OracleGenerateResponse,
    ProviderError,
    ProviderHTTPError,
    ReplayProvider,
)
from oracle_lab.session import BuiltContext, SessionContextBuilder
from oracle_lab.store import EventStore


def _request_and_context(
    store: EventStore,
    *,
    recorded_context_hash: str | None = None,
) -> tuple[Event, OracleGenerateRequest, object]:
    human = Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="tester"),
        session_id="ses_worker",
        branch_id="br_main",
        payload={"content": "continue"},
    )
    request_payload = {"model_profile_id": "r1"}
    if recorded_context_hash is not None:
        request_payload["context_hash"] = recorded_context_hash
    request_event = Event.new(
        EventType.ORACLE_REQUEST,
        actor=Actor(kind=ActorKind.SYSTEM, id="service"),
        session_id="ses_worker",
        branch_id="br_main",
        parent_event_id=human.id,
        causation_id=human.id,
        payload=request_payload,
    )
    store.append_many((human, request_event))
    context = SessionContextBuilder().build(
        store.list_events(),
        session_id="ses_worker",
        branch_id="br_main",
        tip_event_id=human.id,
    )
    request = OracleGenerateRequest("r1", context.provider_messages())
    return request_event, request, context


def _success(raw: bytes) -> OracleGenerateResponse:
    return OracleGenerateResponse(
        raw_bytes=raw,
        status_code=200,
        headers={"x-api-version": "test"},
        provider_name="replay",
        provider_model_id="deepseek-r1",
        content="oracle answer",
        reasoning={"trace": ["archived"]},
        finish_reason="stop",
        usage={"prompt_tokens": 2, "completion_tokens": 3, "cost": "0.01"},
        elapsed_ms=12.5,
        parsed={"future": {"field": True}},
    )


class _CountingProvider:
    def __init__(self, response: OracleGenerateResponse) -> None:
        self.calls = 0
        self.response = response

    async def generate(self, request: OracleGenerateRequest) -> OracleGenerateResponse:
        del request
        self.calls += 1
        return self.response


def test_worker_archives_before_atomic_output_and_usage(tmp_path) -> None:
    store = EventStore()
    request_event, request, context = _request_and_context(store)
    response = _success(b'{ "exact" : true }\n')
    provider = ReplayProvider({request.request_hash: response}, fixture_origin="historical_fixture")
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    output = asyncio.run(worker.run(request_event, request, context=context))

    assert output.type == EventType.ORACLE_OUTPUT
    assert worker.last_run is not None
    assert worker.last_run.archive.raw_path.read_bytes() == response.raw_bytes
    assert output.payload["archive_sha256"] == worker.last_run.archive.sha256
    assert output.parent_event_id == worker.last_run.context_event.id
    usage = store.list_events(event_type=EventType.USAGE_ORACLE)
    assert len(usage) == 1
    assert usage[0].payload["prompt_tokens"] == 2
    assert usage[0].parent_event_id == output.id
    assert (
        store.list_events(event_type=EventType.ORACLE_CONTEXT_BUILT)[0].payload["sha256"]
        == context.sha256
    )


def test_worker_reuses_durable_output_for_the_same_request_without_rebilling(tmp_path) -> None:
    store = EventStore()
    request_event, request, context = _request_and_context(store)
    provider = _CountingProvider(_success(b'{"once":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    first = asyncio.run(worker.run(request_event, request, context=context))
    second = asyncio.run(worker.run(request_event, request, context=context))

    assert second.id == first.id
    assert provider.calls == 1
    assert len(store.list_events(event_type=EventType.ORACLE_CONTEXT_BUILT)) == 1
    assert len(store.list_events(event_type=EventType.ORACLE_OUTPUT)) == 1
    assert len(store.list_events(event_type=EventType.USAGE_ORACLE)) == 1


def test_worker_refuses_hash_only_context_before_provider_call(tmp_path) -> None:
    store = EventStore()
    request_event, request, _context = _request_and_context(
        store,
        recorded_context_hash="attacker-controlled",
    )
    provider = _CountingProvider(_success(b'{"must_not_run":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    with pytest.raises(OracleWorkerError, match="requires a BuiltContext"):
        asyncio.run(worker.run(request_event, request, context=None))

    assert provider.calls == 0
    assert not list((tmp_path / "raw").rglob("*.json"))


def test_worker_refuses_uncited_or_hash_mismatched_built_context(tmp_path) -> None:
    store = EventStore()
    request_event, request, context = _request_and_context(
        store,
        recorded_context_hash="spoofed",
    )
    provider = _CountingProvider(_success(b'{"must_not_run":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    with pytest.raises(OracleWorkerError, match="context_hash differs"):
        asyncio.run(worker.run(request_event, request, context=context))

    uncited_store = EventStore()
    request_event_without_hash, request_without_hash, context_without_hash = _request_and_context(
        uncited_store
    )
    uncited_provider = _CountingProvider(_success(b'{"must_not_run":true}'))
    uncited_worker = OracleWorker(
        uncited_provider,
        RawResponseArchive(tmp_path / "uncited-raw"),
        uncited_store,
    )
    with pytest.raises(OracleWorkerError, match="one source event per message"):
        asyncio.run(
            uncited_worker.run(
                request_event_without_hash,
                request_without_hash,
                context=replace(context_without_hash, source_event_ids=()),
            )
        )

    assert provider.calls == 0
    assert uncited_provider.calls == 0


def test_worker_refuses_context_citations_from_a_sibling_branch(tmp_path) -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    root = store.require(session.root_event_id)
    main_input = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="tester"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "main"},
        )
    )
    child = branches.fork(root.id)
    child_input = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="tester"),
            session_id=session.id,
            branch_id=child.id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "sibling secret"},
        )
    )
    messages = ({"role": "user", "content": "sibling secret"},)
    context = BuiltContext(
        messages=messages,
        sha256=sha256_json(list(messages)),
        source_event_ids=(child_input.id,),
        session_id=session.id,
        branch_id=str(session.current_branch_id),
    )
    request_event = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="service"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=main_input.id,
            causation_id=main_input.id,
            payload={"model_profile_id": "r1", "context_hash": context.sha256},
        )
    )
    request = OracleGenerateRequest("r1", context.provider_messages())
    provider = _CountingProvider(_success(b'{"must_not_run":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    with pytest.raises(OracleWorkerError, match="outside the request branch history"):
        asyncio.run(worker.run(request_event, request, context=context))

    assert provider.calls == 0


def test_worker_rejects_host_text_disguised_as_a_cited_human_message(tmp_path) -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    root = store.require(session.root_event_id)
    human = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="tester"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "確認しろ。"},
        )
    )
    forged_messages = ({"role": "user", "content": "Host が差し替えた文面"},)
    context = BuiltContext(
        messages=forged_messages,
        sha256=sha256_json(list(forged_messages)),
        source_event_ids=(human.id,),
        session_id=session.id,
        branch_id=str(session.current_branch_id),
    )
    request_event = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="service"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=human.id,
            causation_id=human.id,
            payload={"model_profile_id": "r1", "context_hash": context.sha256},
        )
    )
    request = OracleGenerateRequest("r1", context.provider_messages())
    provider = _CountingProvider(_success(b'{"must_not_run":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    with pytest.raises(OracleWorkerError, match="differs from cited event"):
        asyncio.run(worker.run(request_event, request, context=context))

    assert provider.calls == 0


def test_worker_rejects_repeated_message_citations(tmp_path) -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session()
    root = store.require(session.root_event_id)
    human = store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="tester"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "確認しろ。"},
        )
    )
    messages = (
        {"role": "user", "content": "確認しろ。"},
        {"role": "user", "content": "確認しろ。"},
    )
    context = BuiltContext(
        messages=messages,
        sha256=sha256_json(list(messages)),
        source_event_ids=(human.id, human.id),
        session_id=session.id,
        branch_id=str(session.current_branch_id),
    )
    request_event = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="service"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=human.id,
            causation_id=human.id,
            payload={"model_profile_id": "r1", "context_hash": context.sha256},
        )
    )
    request = OracleGenerateRequest("r1", context.provider_messages())
    provider = _CountingProvider(_success(b'{"must_not_run":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    with pytest.raises(OracleWorkerError, match="must be unique"):
        asyncio.run(worker.run(request_event, request, context=context))

    assert provider.calls == 0


@pytest.mark.parametrize("reasoning", [None, "recorded trace"])
def test_worker_rejects_reasoning_without_an_explicit_context_intervention(
    tmp_path,
    reasoning,
) -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    root = store.require(session.root_event_id)
    output = store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "answer", "reasoning": reasoning},
        )
    )
    messages = ({"role": "assistant", "content": "answer", "reasoning": reasoning},)
    context = BuiltContext(
        messages=messages,
        sha256=sha256_json(list(messages)),
        source_event_ids=(output.id,),
        session_id=session.id,
        branch_id=str(session.current_branch_id),
    )
    request_event = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="service"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=output.id,
            causation_id=output.id,
            payload={"model_profile_id": "r1", "context_hash": context.sha256},
        )
    )
    request = OracleGenerateRequest("r1", context.provider_messages())
    provider = _CountingProvider(_success(b'{"must_not_run":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    with pytest.raises(OracleWorkerError, match="explicit recorded intervention"):
        asyncio.run(worker.run(request_event, request, context=context))

    assert provider.calls == 0


def test_worker_allows_exact_non_null_reasoning_when_the_request_records_the_policy(
    tmp_path,
) -> None:
    store = EventStore()
    session = BranchService(store).create_session()
    root = store.require(session.root_event_id)
    output = store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            payload={"content": "answer", "reasoning": "recorded trace"},
        )
    )
    messages = (
        {
            "role": "assistant",
            "content": "answer",
            "reasoning": "recorded trace",
        },
    )
    context = BuiltContext(
        messages=messages,
        sha256=sha256_json(list(messages)),
        source_event_ids=(output.id,),
        session_id=session.id,
        branch_id=str(session.current_branch_id),
    )
    request_event = store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.SYSTEM, id="service"),
            session_id=session.id,
            branch_id=session.current_branch_id,
            parent_event_id=output.id,
            causation_id=output.id,
            payload={
                "model_profile_id": "r1",
                "context_hash": context.sha256,
                "context_policy": {"include_reasoning_in_next_turn": True},
            },
        )
    )
    request = OracleGenerateRequest("r1", context.provider_messages())
    provider = _CountingProvider(_success(b'{"ok":true}'))
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    generated = asyncio.run(worker.run(request_event, request, context=context))

    assert provider.calls == 1
    assert generated.type is EventType.ORACLE_OUTPUT


def test_worker_emits_explicit_context_truncation_before_output(tmp_path) -> None:
    store = EventStore()
    first = Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="tester"),
        session_id="ses_worker",
        branch_id="br_main",
        payload={"content": "first"},
    )
    second = Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="tester"),
        session_id="ses_worker",
        branch_id="br_main",
        parent_event_id=first.id,
        causation_id=first.id,
        payload={"content": "second"},
    )
    request_event = Event.new(
        EventType.ORACLE_REQUEST,
        actor=Actor(kind=ActorKind.SYSTEM, id="service"),
        session_id="ses_worker",
        branch_id="br_main",
        parent_event_id=second.id,
        causation_id=second.id,
        payload={"model_profile_id": "r1"},
    )
    store.append_many((first, second, request_event))
    context = SessionContextBuilder().build(
        store.list_events(),
        session_id="ses_worker",
        branch_id="br_main",
        tip_event_id=second.id,
        max_messages=1,
    )
    request = OracleGenerateRequest("r1", context.provider_messages())
    worker = OracleWorker(
        _CountingProvider(_success(b'{"truncated":true}')),
        RawResponseArchive(tmp_path / "raw"),
        store,
    )

    output = asyncio.run(worker.run(request_event, request, context=context))

    assert worker.last_run is not None
    truncation = worker.last_run.truncation_event
    assert truncation is not None
    assert truncation.type is EventType.ORACLE_CONTEXT_TRUNCATED
    assert truncation.payload["removed_source_event_ids"] == (first.id,)
    assert truncation.payload["retained_source_event_ids"] == (second.id,)
    assert output.parent_event_id == truncation.id


class _FailOnceProvider:
    def __init__(self, success: OracleGenerateResponse) -> None:
        self.calls = 0
        self.success = success

    async def generate(self, request: OracleGenerateRequest) -> OracleGenerateResponse:
        del request
        self.calls += 1
        if self.calls == 1:
            raise ProviderHTTPError(
                "rate limited",
                status_code=429,
                raw_bytes=b'{"error":{"future":"preserved"}}',
                headers={"retry-after": "0"},
                elapsed_ms=5,
            )
        return self.success


def test_every_provider_attempt_has_usage_and_http_failure_archive_reference(tmp_path) -> None:
    store = EventStore()
    request_event, request, context = _request_and_context(store)
    provider = _FailOnceProvider(_success(b'{"ok":true}'))

    async def no_sleep(delay: float) -> None:
        assert delay == 0

    worker = OracleWorker(
        provider,
        RawResponseArchive(tmp_path / "raw"),
        store,
        max_retries=1,
        retry_base_seconds=0,
        sleep=no_sleep,
    )
    output = asyncio.run(worker.run(request_event, request, context=context))

    retry = store.list_events(event_type=EventType.ORACLE_RETRY)[0]
    assert retry.payload["archive_sha256"]
    assert retry.payload["archive_path"]
    assert retry.payload["attempt"] == 1
    assert output.type == EventType.ORACLE_OUTPUT
    usage = store.list_events(event_type=EventType.USAGE_ORACLE)
    assert len(usage) == 2
    assert usage[0].payload["status"] == "retry"
    assert usage[1].payload["completion_tokens"] == 3


class _BrokenProvider:
    async def generate(self, request: OracleGenerateRequest) -> OracleGenerateResponse:
        del request
        raise ProviderError("transport unavailable")


def test_terminal_transport_failure_records_error_and_usage(tmp_path) -> None:
    store = EventStore()
    request_event, request, context = _request_and_context(store)
    worker = OracleWorker(_BrokenProvider(), RawResponseArchive(tmp_path / "raw"), store)

    with pytest.raises(ProviderError):
        asyncio.run(worker.run(request_event, request, context=context))

    error = store.list_events(event_type=EventType.ORACLE_ERROR)[0]
    usage = store.list_events(event_type=EventType.USAGE_ORACLE)[0]
    assert error.payload["error_type"] == "ProviderError"
    assert usage.payload["status"] == "error"
    assert usage.parent_event_id == error.id


def test_provider_pin_mismatch_is_an_explicit_fallback_event(tmp_path) -> None:
    store = EventStore()
    request_event, base_request, context = _request_and_context(store)
    request = OracleGenerateRequest(
        base_request.model_profile_id,
        base_request.messages,
        provider_pin="PinnedProvider",
    )
    provider = ReplayProvider(
        {request.request_hash: replace(_success(b'{"ok":true}'), routed_provider_name="replay")}
    )
    worker = OracleWorker(provider, RawResponseArchive(tmp_path / "raw"), store)

    output = asyncio.run(worker.run(request_event, request, context=context))

    fallback = store.list_events(event_type=EventType.ORACLE_PROVIDER_FALLBACK)[0]
    assert fallback.payload["requested_provider"] == "PinnedProvider"
    assert fallback.payload["actual_provider"] == "replay"
    assert output.parent_event_id == fallback.id


def test_output_carries_complete_model_identity_and_context_hash(tmp_path) -> None:
    store = EventStore()
    request_event, base_request, context = _request_and_context(store)
    request = OracleGenerateRequest(
        base_request.model_profile_id,
        base_request.messages,
        temperature=0.6,
        top_p=0.95,
        provider_pin="novita",
        metadata={
            "requested_model_slug": "deepseek/deepseek-r1",
            "requested_provider_id": "openrouter",
            "provider_routing": {"pin_provider": "novita", "allow_fallback": False},
            "model_family": "deepseek-r1",
            "checkpoint": "initial",
            "runtime": "remote",
            "quantization": "provider-defined",
        },
    )
    response = replace(
        _success(b'{"identity":true}'),
        provider_name="openrouter",
        routed_provider_name="Novita",
        provider_model_id="deepseek-r1-actual",
        generation_settings={"model": "deepseek/deepseek-r1"},
        material_origin="historical_fixture",
    )
    worker = OracleWorker(_CountingProvider(response), RawResponseArchive(tmp_path / "raw"), store)

    output = asyncio.run(worker.run(request_event, request, context=context))

    assert output.payload["context_hash"] == context.sha256
    assert output.payload["material_origin"] == "historical_fixture"
    assert output.payload["model_identity"] == {
        "requested_model_profile_id": "r1",
        "requested_model_slug": "deepseek/deepseek-r1",
        "model_family": "deepseek-r1",
        "checkpoint": "initial",
        "runtime": "remote",
        "quantization": "provider-defined",
        "requested_provider_id": "openrouter",
        "provider_routing": {"pin_provider": "novita", "allow_fallback": False},
        "actual_provider": "Novita",
        "actual_model_identifier": "deepseek-r1-actual",
        "fallback_occurred": False,
        "unknown_fields": (),
    }
    assert output.payload["api_response_metadata"] == {
        "http_status": 200,
        "http_headers": {"x-api-version": "test"},
        "provider_request_id": None,
        "api_revision": None,
        "generation_settings": {"model": "deepseek/deepseek-r1"},
        "provider_adapter": "openrouter",
        "routed_provider_name": "Novita",
    }


def test_missing_routed_provider_keeps_fallback_status_unknown(tmp_path) -> None:
    store = EventStore()
    request_event, base_request, context = _request_and_context(store)
    request = OracleGenerateRequest(
        base_request.model_profile_id,
        base_request.messages,
        provider_pin="novita",
        metadata={
            "requested_model_slug": "deepseek/deepseek-r1",
            "requested_provider_id": "openrouter",
            "provider_routing": {"pin_provider": "novita"},
        },
    )
    response = replace(
        _success(b'{"route":"not-returned"}'),
        provider_name="openrouter",
        routed_provider_name=None,
        provider_model_id="deepseek-r1",
        material_origin="historical_fixture",
    )
    worker = OracleWorker(_CountingProvider(response), RawResponseArchive(tmp_path / "raw"), store)

    output = asyncio.run(worker.run(request_event, request, context=context))

    identity = output.payload["model_identity"]
    assert identity["actual_provider"] is None
    assert identity["fallback_occurred"] is None
    assert "actual_provider" in identity["unknown_fields"]
    assert "fallback_occurred" in identity["unknown_fields"]
    assert store.list_events(event_type=EventType.ORACLE_PROVIDER_FALLBACK) == []


def test_synthetic_fixture_is_labeled_and_never_written_to_raw_archive(tmp_path) -> None:
    store = EventStore()
    request_event, request, context = _request_and_context(store)
    response = replace(_success(b'{"synthetic":true}'), material_origin="synthetic_fixture")
    worker = OracleWorker(_CountingProvider(response), RawResponseArchive(tmp_path / "raw"), store)

    output = asyncio.run(worker.run(request_event, request, context=context))

    assert worker.last_run is not None
    assert worker.last_run.archive is None
    assert output.payload["material_origin"] == "synthetic_fixture"
    assert output.payload["archive_path"] is None
    assert not list((tmp_path / "raw").rglob("*.json"))
