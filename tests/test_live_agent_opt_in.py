from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

_OPT_IN_ENV = "ORACLE_LAB_RUN_LIVE_AGENT_TESTS"
_AGENT_ENV = "ORACLE_LAB_LIVE_AGENT"
_TEMPLATE_ENV = "ORACLE_LAB_LIVE_SBX_TEMPLATE"
_HOSTS_ENV = "ORACLE_LAB_LIVE_ALLOWED_HOSTS"
_SBX_ENV = "ORACLE_LAB_LIVE_SBX_EXECUTABLE"
_SUPPORTED_AGENTS = frozenset({"codex", "opencode"})
_PINNED_TEMPLATE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")


def _require_live_agent_opt_in() -> str:
    if os.environ.get(_OPT_IN_ENV) != "1":
        pytest.skip(f"set {_OPT_IN_ENV}=1 to opt in before any live-agent subprocess")
    agent = os.environ.get(_AGENT_ENV, "").strip().lower()
    if agent not in _SUPPORTED_AGENTS:
        pytest.skip(f"set {_AGENT_ENV} to one of: {', '.join(sorted(_SUPPORTED_AGENTS))}")
    return agent


def _require_live_isolation_configuration() -> tuple[str, tuple[str, ...]]:
    template = os.environ.get(_TEMPLATE_ENV, "").strip()
    if not template:
        pytest.skip(f"set {_TEMPLATE_ENV} to a digest-pinned sbx template")
    if _PINNED_TEMPLATE.fullmatch(template) is None:
        pytest.fail(f"{_TEMPLATE_ENV} must use repository@sha256:<64 lowercase hex>")
    hosts = tuple(
        host.strip().lower().rstrip(".")
        for host in os.environ.get(_HOSTS_ENV, "").split(",")
        if host.strip()
    )
    if not hosts:
        pytest.skip(f"set {_HOSTS_ENV} to the exact comma-separated agent API hosts")
    return template, hosts


def _run_live_agent_smoke(fixture_root: Path) -> None:
    agent = _require_live_agent_opt_in()
    template, allowed_hosts = _require_live_isolation_configuration()
    from oracle_lab.agent_adapters import (
        WorkerTask,
        build_worker_router,
    )
    from oracle_lab.config import AgentRuntimeConfig, AgentWorkerConfig
    from oracle_lab.docker_sbx_isolation import build_coding_worker_isolation_broker
    from oracle_lab.events import Actor, ActorKind, Event, EventType

    sbx_executable = os.environ.get(_SBX_ENV, "sbx").strip()
    if not sbx_executable:
        pytest.fail(f"{_SBX_ENV} must not be blank")
    resolved_sbx = (
        str(Path(sbx_executable).expanduser())
        if Path(sbx_executable).is_absolute()
        else shutil.which(sbx_executable)
    )
    if resolved_sbx is None or not Path(resolved_sbx).is_file():
        pytest.skip(f"configured live sbx executable is unavailable: {sbx_executable}")

    worker = AgentWorkerConfig(
        id=f"live-{agent}",
        enabled=True,
        adapter=agent,
        executable=agent,
        model=os.environ.get("ORACLE_LAB_LIVE_AGENT_MODEL") or None,
        timeout_seconds=300,
        max_output_bytes=4 * 1024 * 1024,
        sandbox_profile="external-broker",
        allowed_environment_names=("TERM",),
        isolation_template_reference=template,
        isolation_allowed_hosts=allowed_hosts,
        max_workspace_export_bytes=64 * 1024 * 1024,
        max_workspace_entries=100_000,
    )
    runtime = AgentRuntimeConfig(
        enabled=True,
        prefer_coding_agent=agent,
        workers={worker.id: worker},
        isolation_backend="docker-sbx-microvm",
        isolation_broker_executable=resolved_sbx,
    )
    broker = build_coding_worker_isolation_broker(
        runtime,
        state_root=fixture_root / "isolation-state",
        workspace_root=fixture_root / "isolated-workspaces",
    )
    assert broker is not None
    router = build_worker_router(
        runtime,
        workspace_root=fixture_root / "isolated-workspaces",
        coding_worker_broker=broker,
    )
    assert router is not None
    _, adapter = router.route("repository_edit")

    repository = fixture_root / "fixture-repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(
        ["git", "config", "user.email", "live-smoke@example.invalid"],
        cwd=repository,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Oracle Lab Live Smoke"],
        cwd=repository,
        check=True,
    )
    (repository / "README.md").write_text("fixture repository\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture base"], cwd=repository, check=True)
    base_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    source = Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="live-smoke-operator"),
        payload={"content": "live coding-agent fixture smoke"},
    )
    result = adapter.run(
        WorkerTask(
            source,
            "Create live_agent_result.txt containing exactly: isolated live smoke passed",
            task_kind="repository_edit",
            repository=str(repository),
            base_commit=base_commit,
        )
    )

    assert result.succeeded
    assert result.base_commit == base_commit
    assert result.patch_bytes
    assert "live_agent_result.txt" in result.changed_paths
    assert not (repository / "live_agent_result.txt").exists()


def test_live_agent_gate_skips_before_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(_OPT_IN_ENV, raising=False)
    monkeypatch.delenv(_AGENT_ENV, raising=False)

    def forbidden_subprocess(
        *_args: object, **_kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        raise AssertionError("live-agent subprocess started without explicit opt-in")

    monkeypatch.setattr(subprocess, "run", forbidden_subprocess)

    with pytest.raises(pytest.skip.Exception, match="before any live-agent subprocess"):
        _run_live_agent_smoke(tmp_path)


def test_live_agent_opt_in_requires_pinned_isolation_before_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_OPT_IN_ENV, "1")
    monkeypatch.setenv(_AGENT_ENV, "codex")
    monkeypatch.delenv(_TEMPLATE_ENV, raising=False)

    def forbidden_subprocess(
        *_args: object, **_kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        raise AssertionError("subprocess started before pinned isolation configuration")

    monkeypatch.setattr(subprocess, "run", forbidden_subprocess)

    with pytest.raises(pytest.skip.Exception, match=_TEMPLATE_ENV):
        _run_live_agent_smoke(tmp_path)


def test_live_agent_opt_in_requires_exact_hosts_before_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_OPT_IN_ENV, "1")
    monkeypatch.setenv(_AGENT_ENV, "codex")
    monkeypatch.setenv(_TEMPLATE_ENV, "fixture.invalid/codex@sha256:" + "a" * 64)
    monkeypatch.delenv(_HOSTS_ENV, raising=False)

    def forbidden_subprocess(
        *_args: object, **_kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        raise AssertionError("subprocess started before exact network policy configuration")

    monkeypatch.setattr(subprocess, "run", forbidden_subprocess)

    with pytest.raises(pytest.skip.Exception, match=_HOSTS_ENV):
        _run_live_agent_smoke(tmp_path)


def test_live_agent_full_gate_still_fails_closed_before_sbx_or_model(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sbx = tmp_path / "sbx"
    sbx.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
    sbx.chmod(0o700)
    monkeypatch.setenv(_OPT_IN_ENV, "1")
    monkeypatch.setenv(_AGENT_ENV, "codex")
    monkeypatch.setenv(_TEMPLATE_ENV, "fixture.invalid/codex@sha256:" + "a" * 64)
    monkeypatch.setenv(_HOSTS_ENV, "api.example.invalid")
    monkeypatch.setenv(_SBX_ENV, str(sbx))

    def forbidden_subprocess(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("sbx or model subprocess started without production evidence")

    monkeypatch.setattr(subprocess, "run", forbidden_subprocess)
    monkeypatch.setattr(subprocess, "Popen", forbidden_subprocess)

    with pytest.raises(
        RuntimeError,
        match="production Docker sbx attestation is unavailable",
    ):
        _run_live_agent_smoke(tmp_path)


@pytest.mark.live_agent
def test_live_agent_fixture_repository_smoke(tmp_path: Path) -> None:
    _run_live_agent_smoke(tmp_path)
