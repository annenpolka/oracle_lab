from __future__ import annotations

import dataclasses
import json
import shutil
from pathlib import Path

import httpx
import pytest

from oracle_lab.agent_adapters import DirectAPIHost, HostWorkerRouter, build_worker_router
from oracle_lab.config import ConfigError, load_runtime_config
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jsonutil import canonical_json
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventStore
from oracle_lab.worker_archive import WorkerRunArchive

CONFIG = Path(__file__).parents[1] / "config"


class _CountingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        self.chunks = chunks
        self.yielded = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk


def _direct_config(
    root: Path,
    *,
    temperature: float = 0.0,
    allow_fallback: bool = False,
    max_output_bytes: int = 1_048_576,
) -> Path:
    root.mkdir()
    for name in ("models.toml", "providers.toml", "policies.toml", "tools.toml"):
        shutil.copy2(CONFIG / name, root / name)
    (root / "agents.toml").write_text(
        f"""
[router]
enabled = true
prefer_coding_agent = "codex"

[workers.direct]
enabled = true
adapter = "direct"
executable = "direct-api"
model = "host-analysis-v1"
timeout_seconds = 10
max_output_bytes = {max_output_bytes}
sandbox_profile = "api-only"
allowed_environment_names = []
max_retries = 0
validation_commands = []
host_provider_kind = "openai_compatible"
host_provider_id = "host-frontier"
host_base_url = "https://host.example.invalid/v1"
host_api_key_env = "TEST_HOST_API_KEY"
host_temperature = {temperature}
host_top_p = 0.8
host_max_tokens = 512
host_allow_fallback = {str(allow_fallback).lower()}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return root


def _service(
    tmp_path: Path,
    *,
    client: httpx.AsyncClient,
    allow_fallback: bool = False,
    max_output_bytes: int = 1_048_576,
) -> OracleLabService:
    config_dir = _direct_config(
        tmp_path / "config",
        allow_fallback=allow_fallback,
        max_output_bytes=max_output_bytes,
    )
    runtime = load_runtime_config(config_dir)
    router = build_worker_router(
        runtime.agents,
        workspace_root=tmp_path / "unused-worker-root",
        direct_http_client=client,
    )
    assert router is not None
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=config_dir,
        host_worker_router=router,
    )
    service._config = runtime
    return service


def _oracle_source(service: OracleLabService) -> Event:
    session = service.new_session("standard Direct Host")
    parent = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    source = service.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            session_id=session["id"],
            branch_id=session["current_branch_id"],
            parent_event_id=parent.id,
            causation_id=parent.id,
            correlation_id=parent.correlation_id,
            payload={
                "content": "TIME_DILATION_FACTOR=1.78",
                "model_profile_id": "r1-initial-openrouter",
                "provider": "openrouter",
            },
        )
    )
    service._enqueue_host_analysis_jobs(source)
    return source


def test_default_direct_host_uses_separate_config_and_archives_exact_provider_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "must-never-enter-events-or-archives"
    monkeypatch.setenv("TEST_HOST_API_KEY", secret)
    calls: list[httpx.Request] = []
    responses: list[bytes] = []
    assistant_output = {
        "events": [
            {
                "type": "analysis.claim_detected",
                "payload": {
                    "raw_text": "TIME_DILATION_FACTOR=1.78",
                    "status": "raw_claim",
                },
                "source_event_ids": [],
            }
        ]
    }
    provider_envelope = {
        "id": "host-request-1",
        "model": "host-analysis-returned-v2",
        "provider": "host-route-b",
        "choices": [
            {
                "message": {"role": "assistant", "content": json.dumps(assistant_output)},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 101,
            "completion_tokens": 17,
            "reasoning_tokens": 3,
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        body = json.loads(request.content)
        source_id = body["messages"][0]["content"].split("You are processing event ", 1)[1][:30]
        assistant_output["events"][0]["source_event_ids"] = [source_id]
        envelope = {**provider_envelope}
        envelope["choices"] = [
            {
                "message": {"role": "assistant", "content": json.dumps(assistant_output)},
                "finish_reason": "stop",
            }
        ]
        exact = json.dumps(envelope, separators=(",", ":")).encode()
        responses.append(exact)
        assert request.headers["authorization"] == f"Bearer {secret}"
        assert request.headers["idempotency-key"].startswith("host-direct:evt_")
        assert body["model"] == "host-analysis-v1"
        assert body["provider"] == {
            "order": ["host-frontier"],
            "allow_fallbacks": True,
        }
        assert body["temperature"] == 0.0
        assert body["top_p"] == 0.8
        assert body["max_tokens"] == 512
        assert body["messages"][0]["content"].startswith(
            "HOST_PROMPT_CONTRACT=oracle-lab-direct-host-v1\n"
        )
        return httpx.Response(
            200,
            content=exact,
            headers={"x-request-id": "host-request-1", "set-cookie": "private-cookie"},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = _service(tmp_path, client=client, allow_fallback=True)
    _oracle_source(service)

    result = service.run_automation(max_jobs=1)

    assert result["processed"][0]["status"] == "completed"
    assert len(calls) == 1
    task = service.store.list_events(event_type=EventType.WORKER_TASK_REQUESTED)[-1]
    assert task.payload["worker_execution_profile"]["model"] == "host-analysis-v1"
    assert task.payload["worker_execution_profile"]["host_provider_id"] == "host-frontier"
    assert task.payload["worker_routing"]["route_class"] == "direct_host"
    assert task.payload["worker_execution_profile"]["host_api_key_env"] == "TEST_HOST_API_KEY"
    terminal = service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)[-1]
    assert terminal.actor == Actor(kind=ActorKind.HOST, id="direct-api-host")
    assert terminal.payload["artifact_origin"] == "host_generated"
    assert terminal.metadata["artifact_origin"] == "host_generated"
    identity = terminal.payload["host_identity"]
    assert identity["requested_provider_id"] == "host-frontier"
    assert identity["requested_model"] == "host-analysis-v1"
    assert identity["actual_provider"] == "host-route-b"
    assert identity["returned_model"] == "host-analysis-returned-v2"
    assert identity["routing_settings"]["fallback_status"] is True
    assert identity["routing_settings"]["allow_fallback"] is True
    assert identity["sampling_settings"] == {
        "model": "host-analysis-v1",
        "temperature": 0.0,
        "top_p": 0.8,
        "max_tokens": 512,
    }
    assert identity["api_response_metadata"]["headers"]["set-cookie"] == "[redacted]"
    assert identity["usage"]["prompt_tokens"] == 101
    started = service.store.list_events(event_type=EventType.WORKER_RUN_STARTED)[-1]
    snapshot = WorkerRunArchive(service.archive_root / "workers").load(
        run_id=str(terminal.payload["run_id"]),
        archived_at=started.created_at,
    )
    assert snapshot.stdout == responses[0]
    assert snapshot.metadata["artifact_origin"] == "host_generated"
    assert snapshot.task["direct_host_response"]["actual_provider"] == "host-route-b"
    assert snapshot.prompt.startswith("HOST_PROMPT_CONTRACT=oracle-lab-direct-host-v1\n")
    usage = service.store.list_events(event_type=EventType.USAGE_HOST)[-1]
    assert usage.payload["provider_id"] == "host-frontier"
    assert usage.payload["model_id"] == "host-analysis-returned-v2"
    assert usage.payload["prompt_tokens"] == 101
    all_event_bytes = canonical_json(
        [event.to_dict() for event in service.store.list_events()]
    ).encode()
    assert secret.encode() not in all_event_bytes
    assert all(
        secret.encode() not in artifact.path.read_bytes() for artifact in snapshot.record.artifacts
    )

    # A completed durable task is reconstructed without issuing another HTTP request.
    job = service._job_queue().list_jobs(kind="extract_claims")[0]
    replayed = service._execute_host_worker_job(job)
    assert replayed
    assert len(calls) == 1
    assert len(service.store.list_events(event_type=EventType.USAGE_HOST)) == 1
    service.close()


def test_direct_host_config_drift_fails_before_transport(
    tmp_path: Path,
) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise AssertionError("config drift must fail before transport")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = _service(tmp_path, client=client)
    source = _oracle_source(service)
    job = service._job_queue().list_jobs(kind="extract_claims")[0]
    routed_task_type, worker = service.host_worker_router.route(job.kind)
    service._ensure_worker_task_event(
        job=job,
        source=source,
        goal="Extract verifiable claims only.",
        routed_task_type=routed_task_type,
        worker=worker,
    )
    drifted_profile = dataclasses.replace(worker.profile, host_temperature=1.0)
    service.host_worker_router = HostWorkerRouter(
        direct=DirectAPIHost(worker.call, profile=drifted_profile)
    )

    with pytest.raises(ServiceError, match="has drifted"):
        service._execute_host_worker_job(job)
    assert calls == 0
    assert not service.store.list_events(event_type=EventType.WORKER_RUN_STARTED)
    service.close()


def test_enabled_direct_host_requires_explicit_non_oracle_identity(tmp_path: Path) -> None:
    config_dir = _direct_config(tmp_path / "config")
    text = (config_dir / "agents.toml").read_text(encoding="utf-8")
    (config_dir / "agents.toml").write_text(
        text.replace('model = "host-analysis-v1"', 'model = ""'),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="explicit Host settings"):
        load_runtime_config(config_dir)


def test_direct_host_config_rejects_credentials_embedded_in_archived_url(
    tmp_path: Path,
) -> None:
    config_dir = _direct_config(tmp_path / "config")
    text = (config_dir / "agents.toml").read_text(encoding="utf-8")
    (config_dir / "agents.toml").write_text(
        text.replace(
            'host_base_url = "https://host.example.invalid/v1"',
            'host_base_url = "https://user:secret@host.example.invalid/v1"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="must not contain credentials"):
        load_runtime_config(config_dir)


def test_direct_host_http_failure_archives_exact_response_before_terminal(
    tmp_path: Path,
) -> None:
    raw = b'{"error":{"code":"rate_limited","message":"retry later"}}'

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, content=raw, headers={"x-request-id": "failed-host-1"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = _service(tmp_path, client=client)
    _oracle_source(service)
    job = service._job_queue().list_jobs(kind="extract_claims")[0]

    with pytest.raises(ServiceError, match="Direct Host call failed"):
        service._execute_host_worker_job(job)

    started = service.store.list_events(event_type=EventType.WORKER_RUN_STARTED)[-1]
    failed = service.store.list_events(event_type=EventType.WORKER_RUN_FAILED)[-1]
    assert failed.actor == Actor(kind=ActorKind.HOST, id="direct-api-host")
    assert failed.payload["artifact_origin"] == "host_generated"
    snapshot = WorkerRunArchive(service.archive_root / "workers").load(
        run_id=str(failed.payload["run_id"]),
        archived_at=started.created_at,
    )
    assert snapshot.stdout == raw
    assert snapshot.task["direct_host_response"]["api_response_metadata"] == {
        "api_revision": None,
        "headers": {
            "content-length": str(len(raw)),
            "x-request-id": "failed-host-1",
        },
        "request_id": "failed-host-1",
        "status_code": 429,
    }
    assert snapshot.metadata["artifact_origin"] == "host_generated"
    usage = service.store.list_events(event_type=EventType.USAGE_HOST)[-1]
    assert usage.payload["provider_id"] == "host-frontier"
    assert usage.payload["model_id"] == "host-analysis-v1"
    service.close()


def test_direct_host_recovers_post_archive_crash_without_second_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    raw = json.dumps(
        {
            "id": "host-once",
            "model": "host-analysis-v1",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": json.dumps({"events": []}),
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 4, "completion_tokens": 2},
        },
        separators=(",", ":"),
    ).encode()

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=raw)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = _service(tmp_path, client=client)
    _oracle_source(service)
    job = service._job_queue().list_jobs(kind="extract_claims")[0]
    original_write = WorkerRunArchive.write

    class InjectedCrash(RuntimeError):
        pass

    def write_then_crash(self: WorkerRunArchive, *args, **kwargs):
        original_write(self, *args, **kwargs)
        raise InjectedCrash("after archive before terminal")

    monkeypatch.setattr(WorkerRunArchive, "write", write_then_crash)
    with pytest.raises(InjectedCrash):
        service._execute_host_worker_job(job)
    monkeypatch.setattr(WorkerRunArchive, "write", original_write)

    recovered = service._execute_host_worker_job(job)

    assert recovered == ()
    assert calls == 1
    terminal = service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)[-1]
    assert terminal.payload["host_identity"]["recovered_verified_orphan"] is True
    assert len(service.store.list_events(event_type=EventType.USAGE_HOST)) == 1
    service.close()


@pytest.mark.parametrize("status_code", [200, 429])
def test_direct_host_streams_success_and_error_responses_to_hard_output_limit(
    tmp_path: Path,
    status_code: int,
) -> None:
    max_output_bytes = 64
    raw = b"A" * 32 + b"B" * 33 + b"must-not-be-read" * 100
    stream = _CountingAsyncStream((raw[:32], raw[32:65], raw[65:]))

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["provider"] == {
            "order": ["host-frontier"],
            "allow_fallbacks": False,
        }
        return httpx.Response(status_code, stream=stream)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = _service(
        tmp_path,
        client=client,
        max_output_bytes=max_output_bytes,
    )
    _oracle_source(service)
    job = service._job_queue().list_jobs(kind="extract_claims")[0]

    with pytest.raises(ServiceError, match="exceeded 64 bytes"):
        service._execute_host_worker_job(job)

    assert stream.yielded == 2
    failed = service.store.list_events(event_type=EventType.WORKER_RUN_FAILED)[-1]
    assert failed.payload["failure_type"] == "HostProviderError"
    assert failed.payload["output_limited"] is True
    assert failed.payload["reasons"] == ("output_limit",)
    started = service.store.list_events(event_type=EventType.WORKER_RUN_STARTED)[-1]
    snapshot = WorkerRunArchive(service.archive_root / "workers").load(
        run_id=str(failed.payload["run_id"]),
        archived_at=started.created_at,
    )
    assert snapshot.stdout == raw[:max_output_bytes]
    assert len(snapshot.stdout) == max_output_bytes
    assert snapshot.metadata["execution"]["status"]["value"] == "output_limit"
    assert snapshot.metadata["execution"]["output_limited"]["value"] is True
    response = snapshot.task["direct_host_response"]
    assert response["api_response_metadata"]["raw_response_disposition"] == "bounded_prefix"
    assert response["api_response_metadata"]["captured_bytes"] == max_output_bytes
    assert response["api_response_metadata"]["max_output_bytes"] == max_output_bytes
    assert not service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)
    service.close()


def test_direct_host_rejects_observed_provider_fallback_when_disabled(
    tmp_path: Path,
) -> None:
    raw = json.dumps(
        {
            "model": "host-analysis-returned-v2",
            "provider": "host-route-b",
            "routing": {"fallback": True, "selected_provider": "host-route-b"},
            "choices": [
                {
                    "message": {"content": json.dumps({"events": []})},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 3, "completion_tokens": 1},
        },
        separators=(",", ":"),
    ).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content)["provider"] == {
            "order": ["host-frontier"],
            "allow_fallbacks": False,
        }
        return httpx.Response(200, content=raw)

    service = _service(
        tmp_path,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    _oracle_source(service)
    job = service._job_queue().list_jobs(kind="extract_claims")[0]

    with pytest.raises(ServiceError, match="fallback routing while fallback is disabled"):
        service._execute_host_worker_job(job)

    failed = service.store.list_events(event_type=EventType.WORKER_RUN_FAILED)[-1]
    identity = failed.payload["host_identity"]
    assert identity["actual_provider"] == "host-route-b"
    assert identity["returned_model"] == "host-analysis-returned-v2"
    assert identity["routing_settings"]["fallback_status"] is True
    assert identity["routing_settings"]["allow_fallback"] is False
    started = service.store.list_events(event_type=EventType.WORKER_RUN_STARTED)[-1]
    snapshot = WorkerRunArchive(service.archive_root / "workers").load(
        run_id=str(failed.payload["run_id"]),
        archived_at=started.created_at,
    )
    assert snapshot.stdout == raw
    assert snapshot.task["direct_host_response"]["actual_provider"] == "host-route-b"
    assert not service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)
    service.close()


def test_direct_host_preserves_unknown_fallback_when_provider_is_not_reported(
    tmp_path: Path,
) -> None:
    raw = json.dumps(
        {
            "model": "host-analysis-v1",
            "choices": [{"message": {"content": json.dumps({"events": []})}}],
        },
        separators=(",", ":"),
    ).encode()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=raw)

    service = _service(
        tmp_path,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    _oracle_source(service)
    result = service.run_automation(max_jobs=1)

    assert result["processed"][0]["status"] == "completed"
    terminal = service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)[-1]
    assert terminal.payload["host_identity"]["actual_provider"] is None
    assert terminal.payload["host_identity"]["routing_settings"]["fallback_status"] is None
    service.close()


def test_direct_host_redacts_known_credential_values_from_all_response_headers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "direct-host-secret-value"
    monkeypatch.setenv("TEST_HOST_API_KEY", secret)
    raw = json.dumps(
        {
            "model": "host-analysis-v1",
            "provider": "host-frontier",
            "choices": [{"message": {"content": json.dumps({"events": []})}}],
        },
        separators=(",", ":"),
    ).encode()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=raw,
            headers={
                "x-request-id": secret,
                "x-debug-trace": f"before-{secret}-after",
                "x-api-version": f"revision-{secret}",
            },
        )

    service = _service(
        tmp_path,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    _oracle_source(service)
    service.run_automation(max_jobs=1)

    terminal = service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)[-1]
    metadata = terminal.payload["host_identity"]["api_response_metadata"]
    assert metadata["request_id"] == "[redacted]"
    assert metadata["api_revision"] == "revision-[redacted]"
    assert metadata["headers"]["x-debug-trace"] == "before-[redacted]-after"
    all_event_bytes = canonical_json(
        [event.to_dict() for event in service.store.list_events()]
    ).encode()
    assert secret.encode() not in all_event_bytes
    started = service.store.list_events(event_type=EventType.WORKER_RUN_STARTED)[-1]
    snapshot = WorkerRunArchive(service.archive_root / "workers").load(
        run_id=str(terminal.payload["run_id"]),
        archived_at=started.created_at,
    )
    assert all(
        secret.encode() not in artifact.path.read_bytes() for artifact in snapshot.record.artifacts
    )
    service.close()


def test_direct_host_quarantines_response_body_containing_configured_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "credential-reflected-in-body"
    monkeypatch.setenv("TEST_HOST_API_KEY", secret)
    raw = json.dumps({"error": {"message": f"provider echoed {secret}"}}).encode()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            content=raw,
            headers={"x-debug-trace": f"also-{secret}"},
        )

    service = _service(
        tmp_path,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    _oracle_source(service)
    job = service._job_queue().list_jobs(kind="extract_claims")[0]

    with pytest.raises(ServiceError, match="contained a configured credential") as captured:
        service._execute_host_worker_job(job)
    assert secret not in str(captured.value)

    failed = service.store.list_events(event_type=EventType.WORKER_RUN_FAILED)[-1]
    assert failed.payload["reasons"] == ("credential_response_quarantined",)
    metadata = failed.payload["host_identity"]["api_response_metadata"]
    assert metadata["raw_response_disposition"] == "quarantined_credential"
    assert metadata["captured_bytes"] == 0
    assert metadata["headers"]["x-debug-trace"] == "also-[redacted]"
    started = service.store.list_events(event_type=EventType.WORKER_RUN_STARTED)[-1]
    snapshot = WorkerRunArchive(service.archive_root / "workers").load(
        run_id=str(failed.payload["run_id"]),
        archived_at=started.created_at,
    )
    assert snapshot.stdout == b""
    assert snapshot.metadata["execution"]["status"]["value"] == "credential_quarantined"
    all_event_bytes = canonical_json(
        [event.to_dict() for event in service.store.list_events()]
    ).encode()
    assert secret.encode() not in all_event_bytes
    assert all(
        secret.encode() not in artifact.path.read_bytes() for artifact in snapshot.record.artifacts
    )
    service.close()


def test_direct_host_output_limit_orphan_recovers_without_second_http_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    raw = b"X" * 65

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=raw)

    service = _service(
        tmp_path,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        max_output_bytes=64,
    )
    _oracle_source(service)
    job = service._job_queue().list_jobs(kind="extract_claims")[0]
    original_write = WorkerRunArchive.write

    class InjectedCrash(RuntimeError):
        pass

    def write_then_crash(self: WorkerRunArchive, *args, **kwargs):
        original_write(self, *args, **kwargs)
        raise InjectedCrash("after bounded failure archive")

    monkeypatch.setattr(WorkerRunArchive, "write", write_then_crash)
    with pytest.raises(InjectedCrash):
        service._execute_host_worker_job(job)
    monkeypatch.setattr(WorkerRunArchive, "write", original_write)

    with pytest.raises(ServiceError, match="recovered an archived Direct Host failure"):
        service._execute_host_worker_job(job)

    assert calls == 1
    failed = service.store.list_events(event_type=EventType.WORKER_RUN_FAILED)[-1]
    assert failed.payload["recovered_verified_orphan"] is True
    assert failed.payload["output_limited"] is True
    assert failed.payload["reasons"] == ("output_limit",)
    service.close()


def test_direct_host_bundle_round_trip_preserves_archive_and_identity_without_recall(
    tmp_path: Path,
) -> None:
    raw = json.dumps(
        {
            "model": "host-analysis-v1",
            "provider": "host-frontier",
            "choices": [{"message": {"content": json.dumps({"events": []})}}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
        },
        separators=(",", ":"),
    ).encode()
    source_calls = 0

    def source_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal source_calls
        source_calls += 1
        return httpx.Response(200, content=raw)

    source_service = _service(
        tmp_path,
        client=httpx.AsyncClient(transport=httpx.MockTransport(source_handler)),
    )
    historical_path = tmp_path / "historical-direct-host.json"
    historical_path.write_text(
        json.dumps(
            {
                "messages": [
                    {"role": "user", "content": "確認しろ。"},
                    {"role": "assistant", "content": "OBSERVATION=1"},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    imported = source_service.import_session(historical_path, title="Direct Host bundle")
    historical_output = source_service.store.require(imported["assistant_event_ids"][0])
    source_service._enqueue_host_analysis_jobs(historical_output)
    source_service.run_automation(max_jobs=1)
    terminal = source_service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)[-1]
    bundle = tmp_path / "direct-host-bundle"
    source_service.export("bundle", bundle, session_id=historical_output.session_id)
    assert source_calls == 1

    restored_calls = 0

    def forbidden_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal restored_calls
        restored_calls += 1
        raise AssertionError("bundle import must not call the Direct Host provider")

    restored_root = tmp_path / "restored"
    restored_root.mkdir()
    restored = _service(
        restored_root,
        client=httpx.AsyncClient(transport=httpx.MockTransport(forbidden_handler)),
    )
    restored.import_bundle(bundle)

    restored_terminal = restored.store.require(terminal.id)
    restored_started = next(
        event
        for event in restored.store.list_events(event_type=EventType.WORKER_RUN_STARTED)
        if event.payload.get("run_id") == restored_terminal.payload.get("run_id")
    )
    snapshot = WorkerRunArchive(restored.archive_root / "workers").load(
        run_id=str(restored_terminal.payload["run_id"]),
        archived_at=restored_started.created_at,
    )
    assert snapshot.stdout == raw
    assert snapshot.task["direct_host_response"]["requested_provider_id"] == "host-frontier"
    assert restored_terminal.payload["host_identity"]["actual_provider"] == "host-frontier"
    restored_task = restored.store.require(str(restored_terminal.payload["task_event_id"]))
    assert restored_task.metadata["bundle_import_authority"] == "historical_only"
    assert restored.store.verify_integrity() == []
    assert restored.run_automation(max_jobs=1)["processed"] == []
    assert restored_calls == 0
    source_service.close()
    restored.close()
