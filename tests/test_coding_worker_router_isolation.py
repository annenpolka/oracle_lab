from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Any

import pytest

import oracle_lab.agent_adapters as agent_adapters
from oracle_lab.agent_adapters import (
    AgentAdapterError,
    CodexAdapter,
    OpenCodeAdapter,
    WorkerTask,
    build_worker_router,
)
from oracle_lab.coding_isolation import (
    REQUIRED_ISOLATION_CAPABILITIES,
    CodingIsolationError,
    IsolationAttestation,
    IsolationRunRequest,
    IsolationRunResult,
    receipt_sha256,
)
from oracle_lab.config import AgentRuntimeConfig, AgentWorkerConfig, ConfigError
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jsonutil import canonical_json
from oracle_lab.workspace_archive import WORKSPACE_ARCHIVE_MAGIC


def _attestation(
    *,
    policy_sha256: str = "2" * 64,
    capabilities: tuple[str, ...] | None = None,
) -> IsolationAttestation:
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "status": "passed",
        "observed_at": "2026-08-31T00:00:00+00:00",
        "checks": [
            {
                "id": capability,
                "status": "passed",
                "evidence": {"fixture_observation": capability},
            }
            for capability in sorted(
                REQUIRED_ISOLATION_CAPABILITIES if capabilities is None else capabilities
            )
        ],
    }
    return IsolationAttestation(
        backend="fixture-microvm",
        broker_executable_path="/usr/local/bin/sbx",
        broker_executable_sha256="1" * 64,
        client_version="fixture-client-1",
        server_version="fixture-server-1",
        template_reference="fixture/coding-worker@sha256:" + "a" * 64,
        template_identity="sha256:" + "a" * 64,
        policy_sha256=policy_sha256,
        conformance_suite_version="fixture-suite-1",
        conformance_receipt_sha256=receipt_sha256(receipt),
        capabilities=(
            tuple(REQUIRED_ISOLATION_CAPABILITIES) if capabilities is None else capabilities
        ),
        receipt=receipt,
    )


class _FakeBinding:
    def __init__(self, attestation: IsolationAttestation) -> None:
        self.attestation = attestation
        self.returned_attestation = attestation
        self.requests: list[IsolationRunRequest] = []

    def run(self, request: IsolationRunRequest) -> IsolationRunResult:
        self.requests.append(request)
        workspace_export = WORKSPACE_ARCHIVE_MAGIC + (0).to_bytes(8, "big")
        return IsolationRunResult(
            exit_code=0,
            stdout=b"",
            stderr=b"",
            timed_out=False,
            output_limited=False,
            actual_command=(f"/guest/bin/{request.adapter}", *request.command[1:]),
            guest_executable_path=f"/guest/bin/{request.adapter}",
            guest_executable_version=f"{request.adapter} fixture-1",
            guest_executable_version_status="reported",
            sandbox_id=f"sandbox-{request.adapter}-fixture",
            workspace_export=workspace_export,
            workspace_export_sha256=hashlib.sha256(workspace_export).hexdigest(),
            workspace_export_bytes=len(workspace_export),
            workspace_export_entries=0,
            cleanup_confirmed=True,
            attestation=self.returned_attestation,
        )


class _FakeBroker:
    def __init__(
        self,
        binding: _FakeBinding | None = None,
        *,
        error: OSError | CodingIsolationError | None = None,
        missing_capability: str | None = None,
    ) -> None:
        self.binding = binding
        self.error = error
        self.missing_capability = missing_capability
        self.bound_profiles: list[Any] = []

    def bind(self, profile: Any) -> _FakeBinding:
        self.bound_profiles.append(profile)
        if self.error is not None:
            raise self.error
        if self.missing_capability is not None:
            capabilities = tuple(REQUIRED_ISOLATION_CAPABILITIES - {self.missing_capability})
            return _FakeBinding(_attestation(capabilities=capabilities))
        assert self.binding is not None
        return self.binding


def _worker(
    adapter: str,
    *,
    sandbox_profile: str = "external-broker",
    template_reference: str = "fixture/coding-worker@sha256:" + "a" * 64,
    allowed_hosts: tuple[str, ...] = ("api.example.invalid",),
) -> AgentWorkerConfig:
    return AgentWorkerConfig(
        id=adapter,
        enabled=True,
        adapter=adapter,  # type: ignore[arg-type]
        executable=adapter,
        model=None,
        timeout_seconds=30,
        max_output_bytes=4096,
        sandbox_profile=sandbox_profile,
        allowed_environment_names=("TERM",),
        isolation_template_reference=template_reference,
        isolation_allowed_hosts=allowed_hosts,
        max_workspace_export_bytes=8192,
        max_workspace_entries=64,
    )


def _runtime(worker: AgentWorkerConfig) -> AgentRuntimeConfig:
    return AgentRuntimeConfig(
        enabled=True,
        prefer_coding_agent=worker.adapter,  # type: ignore[arg-type]
        workers={worker.id: worker},
        isolation_backend="docker-sbx-microvm",
        isolation_broker_executable="sbx",
    )


def _source() -> Event:
    return Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="test"),
        session_id="ses_isolated_router",
        branch_id="br_isolated_router",
        payload={"content": "Inspect only."},
    )


@pytest.mark.parametrize(
    ("adapter_name", "adapter_type"),
    [("codex", CodexAdapter), ("opencode", OpenCodeAdapter)],
)
def test_conforming_broker_activates_coding_adapter_without_host_subprocess(
    adapter_name: str,
    adapter_type: type[CodexAdapter] | type[OpenCodeAdapter],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_host_process(*_args: object, **_kwargs: object) -> subprocess.Popen[bytes]:
        raise AssertionError("brokered coding adapter attempted a Host subprocess")

    def forbidden_host_lookup(*_args: object, **_kwargs: object) -> str | None:
        raise AssertionError("brokered coding adapter attempted a Host executable lookup")

    monkeypatch.setattr(subprocess, "Popen", forbidden_host_process)
    monkeypatch.setattr(agent_adapters.shutil, "which", forbidden_host_lookup)
    attestation = _attestation()
    binding = _FakeBinding(attestation)
    broker = _FakeBroker(binding)

    router = build_worker_router(
        _runtime(_worker(adapter_name)),
        workspace_root=tmp_path / "workers",
        coding_worker_broker=broker,
    )

    assert router is not None
    routed_kind, adapter = router.route("investigation")
    assert routed_kind == "investigation"
    assert isinstance(adapter, adapter_type)
    assert adapter.profile is not None
    assert adapter.profile.redacted_snapshot()["isolation_attestation"] == attestation.to_dict()
    result = adapter.run(WorkerTask(_source(), "Inspect only.", task_kind="investigation"))

    assert len(broker.bound_profiles) == 1
    assert len(binding.requests) == 1
    assert binding.requests[0].adapter == adapter_name
    assert binding.requests[0].workspace.is_absolute()
    assert binding.requests[0].workspace.is_relative_to((tmp_path / "workers").resolve())
    assert result.executable_path == f"/guest/bin/{adapter_name}"
    assert canonical_json(result.isolation_attestation) == canonical_json(attestation.to_dict())
    assert result.isolation_cleanup_confirmed is True


def test_bound_profile_deeply_freezes_isolation_attestation(tmp_path: Path) -> None:
    binding = _FakeBinding(_attestation())
    router = build_worker_router(
        _runtime(_worker("codex")),
        workspace_root=tmp_path / "workers",
        coding_worker_broker=_FakeBroker(binding),
    )

    assert router is not None
    adapter = router.codex
    assert adapter is not None
    assert adapter.profile is not None
    frozen = adapter.profile.isolation_attestation
    assert frozen is not None
    with pytest.raises(TypeError):
        frozen["backend"] = "mutated"  # type: ignore[index]
    with pytest.raises(TypeError):
        frozen["receipt"]["checks"][0]["status"] = "failed"  # type: ignore[index]


@pytest.mark.parametrize(
    "error",
    [
        CodingIsolationError("conformance receipt unavailable"),
        OSError("broker socket unavailable"),
    ],
    ids=("contract-failure", "operating-system-failure"),
)
def test_broker_bind_failure_is_fail_closed(
    error: OSError | CodingIsolationError,
    tmp_path: Path,
) -> None:
    with pytest.raises(AgentAdapterError, match="worker isolation binding failed"):
        build_worker_router(
            _runtime(_worker("codex")),
            workspace_root=tmp_path / "workers",
            coding_worker_broker=_FakeBroker(error=error),
        )


def test_broker_missing_required_capability_is_fail_closed(tmp_path: Path) -> None:
    missing = "host_git_control_unavailable"

    with pytest.raises(
        AgentAdapterError,
        match=rf"missing required capabilities: {missing}",
    ):
        build_worker_router(
            _runtime(_worker("codex")),
            workspace_root=tmp_path / "workers",
            coding_worker_broker=_FakeBroker(missing_capability=missing),
        )


def test_run_rejects_attestation_drift_before_accepting_output(tmp_path: Path) -> None:
    binding = _FakeBinding(_attestation())
    router = build_worker_router(
        _runtime(_worker("codex")),
        workspace_root=tmp_path / "workers",
        coding_worker_broker=_FakeBroker(binding),
    )
    assert router is not None
    adapter = router.codex
    assert adapter is not None
    binding.returned_attestation = _attestation(policy_sha256="4" * 64)

    with pytest.raises(AgentAdapterError, match="isolation attestation drifted"):
        adapter.run(WorkerTask(_source(), "Inspect only.", task_kind="investigation"))

    assert len(binding.requests) == 1


@pytest.mark.parametrize(
    ("adapter_name", "sandbox_profile"),
    [("codex", "workspace-write"), ("opencode", "external-sandbox-wrapper")],
)
def test_broker_requires_external_broker_sandbox_profile(
    adapter_name: str,
    sandbox_profile: str,
    tmp_path: Path,
) -> None:
    broker = _FakeBroker(_FakeBinding(_attestation()))

    with pytest.raises(
        AgentAdapterError,
        match=rf"brokered {adapter_name} worker requires sandbox_profile=external-broker",
    ):
        build_worker_router(
            _runtime(_worker(adapter_name, sandbox_profile=sandbox_profile)),
            workspace_root=tmp_path / "workers",
            coding_worker_broker=broker,
        )

    assert broker.bound_profiles == []


def _worker_from_mapping(**overrides: Any) -> AgentWorkerConfig:
    values: dict[str, Any] = {
        "enabled": True,
        "adapter": "codex",
        "executable": "codex",
        "sandbox_profile": "external-broker",
        "allowed_environment_names": [],
        "validation_commands": [],
        "isolation_template_reference": "fixture/coding-worker@sha256:" + "a" * 64,
        "isolation_allowed_hosts": ["api.example.invalid"],
    }
    values.update(overrides)
    return AgentWorkerConfig.from_mapping("codex", values)


@pytest.mark.parametrize(
    "reference",
    [
        "fixture/coding-worker:latest",
        "fixture/coding-worker@sha256:x",
        "fixture/coding-worker@sha256:" + "a" * 63,
        "fixture/coding-worker@sha256:" + "A" * 64,
        "fixture/coding-worker@sha256:" + "g" * 64,
        "fixture/coding-worker@sha256:" + "a" * 65,
    ],
)
def test_isolation_template_reference_requires_exact_lowercase_sha256_digest(
    reference: str,
) -> None:
    with pytest.raises(ConfigError, match=r"must pin .*sha256 digest"):
        _worker_from_mapping(isolation_template_reference=reference)


def test_isolation_template_reference_accepts_exact_sha256_digest() -> None:
    reference = "registry.example.invalid/oracle/codex@sha256:" + "0f" * 32

    worker = _worker_from_mapping(isolation_template_reference=reference)

    assert worker.isolation_template_reference == reference


def test_enabled_external_broker_requires_template_reference() -> None:
    with pytest.raises(ConfigError, match="requires a pinned isolation_template_reference"):
        _worker_from_mapping(isolation_template_reference="")


@pytest.mark.parametrize("name", ["HOME", "PATH", "CODEX_HOME", "OPENAI_API_KEY"])
def test_external_broker_rejects_host_state_and_credential_environment_names(
    name: str,
) -> None:
    with pytest.raises(ConfigError, match="environment contains unsafe names"):
        _worker_from_mapping(allowed_environment_names=[name])


@pytest.mark.parametrize(
    "host",
    [
        "*.example.invalid",
        "10.0.0.0/8",
        "api.example.invalid:443",
        "-api.example.invalid",
        "api-.example.invalid",
        "api_name.example.invalid",
        "api.\u4f8b\u3048.invalid",
    ],
)
def test_isolation_allowed_hosts_rejects_nonexact_dns_names(host: str) -> None:
    with pytest.raises(ConfigError, match="must be exact DNS names"):
        _worker_from_mapping(isolation_allowed_hosts=[host])


def test_isolation_allowed_hosts_are_canonical_and_unique() -> None:
    worker = _worker_from_mapping(
        isolation_allowed_hosts=["API.Example.Invalid.", "events.example.invalid"]
    )

    assert worker.isolation_allowed_hosts == (
        "api.example.invalid",
        "events.example.invalid",
    )
    with pytest.raises(ConfigError, match="must be unique"):
        _worker_from_mapping(
            isolation_allowed_hosts=["API.Example.Invalid", "api.example.invalid."]
        )
