from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import oracle_lab.agent_adapters as agent_adapters
import oracle_lab.config as config_module
import oracle_lab.docker_sbx_isolation as docker_sbx_isolation
import oracle_lab.services as services
from oracle_lab.agent_adapters import AgentAdapterError
from oracle_lab.coding_isolation import CodingIsolationError
from oracle_lab.config import AgentRuntimeConfig, AgentWorkerConfig
from oracle_lab.services import OracleLabService

_TEMPLATE_REFERENCE = "fixture/coding-worker@sha256:" + "a" * 64


def _worker(executable: Path) -> AgentWorkerConfig:
    return AgentWorkerConfig.from_mapping(
        "codex",
        {
            "enabled": True,
            "adapter": "codex",
            "executable": str(executable),
            "sandbox_profile": "external-broker",
            "allowed_environment_names": [],
            "validation_commands": [],
            "isolation_template_reference": _TEMPLATE_REFERENCE,
            "isolation_allowed_hosts": ["api.example.invalid"],
        },
    )


def _configure_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    agents: Any,
) -> tuple[Path, Any]:
    home = (tmp_path / "home").resolve()
    runtime_config = SimpleNamespace(agents=agents)
    monkeypatch.setenv("ORACLE_LAB_HOME", str(home))
    monkeypatch.setenv("ORACLE_LAB_CONFIG", str(tmp_path / "config"))
    monkeypatch.delenv("ORACLE_LAB_DB", raising=False)
    monkeypatch.setattr(services, "_git_worktree_root", lambda _path: None)
    monkeypatch.setattr(config_module, "load_runtime_config", lambda _path: runtime_config)
    return home, runtime_config


def test_default_wires_isolation_broker_into_worker_router(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agents = object()
    home, runtime_config = _configure_default(tmp_path, monkeypatch, agents)
    broker = object()
    router = object()
    calls: list[tuple[str, object, Path, Path | object | None]] = []

    def fake_broker_factory(
        config: object,
        *,
        state_root: str | Path,
        workspace_root: str | Path | None = None,
    ) -> object:
        calls.append(
            (
                "broker",
                config,
                Path(state_root),
                None if workspace_root is None else Path(workspace_root),
            )
        )
        return broker

    def fake_router_factory(
        config: object,
        *,
        workspace_root: str | Path,
        direct: object | None = None,
        direct_http_client: object | None = None,
        coding_worker_broker: object | None = None,
    ) -> object:
        assert direct is None
        assert direct_http_client is None
        calls.append(("router", config, Path(workspace_root), coding_worker_broker))
        return router

    monkeypatch.setattr(
        docker_sbx_isolation,
        "build_coding_worker_isolation_broker",
        fake_broker_factory,
    )
    monkeypatch.setattr(agent_adapters, "build_worker_router", fake_router_factory)

    service = OracleLabService.default()
    try:
        assert service.host_worker_router is router
        assert service.runtime_config is runtime_config
        assert calls == [
            (
                "broker",
                agents,
                home / "coding-isolation",
                home / "worker-workspaces",
            ),
            ("router", agents, home / "worker-workspaces", broker),
        ]
    finally:
        service.close()


def test_default_does_not_probe_sbx_when_agents_are_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agents = AgentRuntimeConfig(
        enabled=False,
        isolation_backend="docker-sbx-microvm",
        isolation_broker_executable="sbx",
    )
    home, _runtime_config = _configure_default(tmp_path, monkeypatch, agents)

    def forbidden_run(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("disabled agents attempted to probe sbx")

    monkeypatch.setattr(docker_sbx_isolation.SubprocessCommandRunner, "run", forbidden_run)

    service = OracleLabService.default()
    try:
        assert service.host_worker_router is None
        assert not (home / "coding-isolation").exists()
    finally:
        service.close()


def test_default_fails_closed_when_isolation_binding_cannot_attest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "model-was-invoked"
    executable = tmp_path / "codex"
    executable.write_text(f"#!/bin/sh\ntouch {marker}\n", encoding="utf-8")
    executable.chmod(0o700)
    worker = _worker(executable)
    agents = AgentRuntimeConfig(
        enabled=True,
        prefer_coding_agent="codex",
        workers={worker.id: worker},
        isolation_backend="docker-sbx-microvm",
        isolation_broker_executable="sbx",
    )
    home, _runtime_config = _configure_default(tmp_path, monkeypatch, agents)

    def reject_binding(
        _self: docker_sbx_isolation.DockerSbxIsolationBroker,
        _profile: object,
    ) -> object:
        raise CodingIsolationError("conformance evidence unavailable")

    monkeypatch.setattr(
        docker_sbx_isolation.DockerSbxIsolationBroker,
        "bind",
        reject_binding,
    )

    with pytest.raises(AgentAdapterError, match="worker isolation binding failed"):
        OracleLabService.default()

    assert not marker.exists()
    assert not (home / "oracle.db").exists()
