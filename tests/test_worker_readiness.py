from __future__ import annotations

import hashlib
import json
import stat
import subprocess
from pathlib import Path
from typing import Any

import pytest

from oracle_lab.docker_sbx_isolation import DockerSbxIsolationBroker
from oracle_lab.services import OracleLabService
from oracle_lab.worker_readiness import (
    PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS,
    WorkerReadinessReport,
    inspect_worker_readiness,
)

_DIGEST = "a" * 64
_EXPECTED_BLOCKERS = (
    "workspace_quiescence",
    "guest_git_control_integrity",
    "data_plane_network_and_credential_enforcement",
    "actual_template_instance_identity",
    "sandbox_ownership",
    "profile_workspace_binding",
)


def _write_candidate_config(
    path: Path,
    broker: Path,
    *,
    template: str = f"fixture.invalid/codex@sha256:{_DIGEST}",
    hosts: tuple[str, ...] = ("api.example.invalid",),
) -> None:
    encoded_hosts = ", ".join(json.dumps(host) for host in hosts)
    path.write_text(
        "\n".join(
            (
                "[router]",
                "enabled = true",
                'prefer_coding_agent = "codex"',
                'isolation_backend = "docker-sbx-microvm"',
                f"isolation_broker_executable = {json.dumps(str(broker))}",
                "",
                "[workers.codex]",
                "enabled = true",
                'adapter = "codex"',
                'executable = "codex"',
                'sandbox_profile = "external-broker"',
                'allowed_environment_names = ["TERM"]',
                "validation_commands = []",
                f"isolation_template_reference = {json.dumps(template)}",
                f"isolation_allowed_hosts = [{encoded_hosts}]",
                "",
            )
        ),
        encoding="utf-8",
    )


def _make_executable(path: Path, content: bytes = b"fixed sbx diagnostic fixture\n") -> None:
    path.write_bytes(content)
    path.chmod(0o700)


def _checks(report: WorkerReadinessReport) -> dict[str, Any]:
    return {check.id: check for check in report.checks}


def _tree_snapshot(root: Path) -> dict[str, tuple[str, int, bytes | str]]:
    result: dict[str, tuple[str, int, bytes | str]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        details = path.lstat()
        mode = stat.S_IMODE(details.st_mode)
        if path.is_symlink():
            result[relative] = ("symlink", mode, str(path.readlink()))
        elif path.is_file():
            result[relative] = ("file", mode, path.read_bytes())
        else:
            result[relative] = ("directory", mode, b"")
    return result


def test_production_blocker_registry_is_exact_and_always_blocks_readiness(
    tmp_path: Path,
) -> None:
    broker = tmp_path / "sbx"
    config = tmp_path / "agents.toml"
    _make_executable(broker)
    _write_candidate_config(config, broker)

    report = inspect_worker_readiness(config)
    document = report.to_dict()

    assert PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS == _EXPECTED_BLOCKERS
    assert report.production_evidence_blockers == _EXPECTED_BLOCKERS
    assert document["production_evidence_blockers"] == list(_EXPECTED_BLOCKERS)
    assert report.status == "blocked"
    assert report.ready is False
    assert report.safe_to_start_worker is False
    assert document["ready"] is False
    assert document["safe_to_start_worker"] is False
    assert _checks(report)["production_isolation_evidence"].reason_id == (
        "production_isolation_evidence_unproven"
    )


def test_complete_static_prerequisites_hash_regular_broker_without_running_any_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker_bytes = b"#!/bin/sh\nexit 99\n"
    broker = tmp_path / "sbx"
    config = tmp_path / "agents.toml"
    _make_executable(broker, broker_bytes)
    _write_candidate_config(config, broker)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("static readiness attempted an executable operation")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(DockerSbxIsolationBroker, "bind", forbidden)
    monkeypatch.setattr(OracleLabService, "default", forbidden)
    before = _tree_snapshot(tmp_path)

    report = inspect_worker_readiness(config)

    assert _tree_snapshot(tmp_path) == before
    checks = _checks(report)
    assert tuple(check.id for check in report.checks) == (
        "agents_config",
        "router_enabled",
        "enabled_coding_workers",
        "coding_worker_selection",
        "isolation_backend",
        "worker_sandbox_profiles",
        "worker_template_references",
        "worker_network_allowlists",
        "worker_environment_allowlists",
        "broker_executable",
        "production_isolation_evidence",
    )
    assert all(
        checks[check_id].status == "passed"
        for check_id in (
            "agents_config",
            "router_enabled",
            "enabled_coding_workers",
            "coding_worker_selection",
            "isolation_backend",
            "worker_sandbox_profiles",
            "worker_template_references",
            "worker_network_allowlists",
            "worker_environment_allowlists",
            "broker_executable",
        )
    )
    assert checks["broker_executable"].evidence == {
        "resolved_path": str(broker.resolve()),
        "sha256": hashlib.sha256(broker_bytes).hexdigest(),
        "truth_domain": "real",
    }
    assert checks["production_isolation_evidence"].status == "blocked"
    assert report.status == "blocked"


def test_disabled_config_is_blocked_without_resolving_or_hashing_broker(tmp_path: Path) -> None:
    config = tmp_path / "agents.toml"
    config.write_text(
        "\n".join(
            (
                "[router]",
                "enabled = false",
                'isolation_backend = "disabled"',
                'isolation_broker_executable = "must-not-resolve"',
                "",
            )
        ),
        encoding="utf-8",
    )

    def forbidden_resolver(_configured: str) -> str | None:
        raise AssertionError("disabled readiness attempted executable resolution")

    report = inspect_worker_readiness(config, executable_resolver=forbidden_resolver)
    checks = _checks(report)

    assert report.status == "blocked"
    assert checks["router_enabled"].reason_id == "router_disabled"
    assert checks["enabled_coding_workers"].reason_id == "no_enabled_coding_worker"
    assert checks["isolation_backend"].reason_id == "production_isolation_backend_disabled"
    assert checks["broker_executable"].reason_id == "production_isolation_backend_disabled"


def test_broker_executable_symlink_is_bound_to_resolved_regular_file(tmp_path: Path) -> None:
    target = tmp_path / "sbx-real"
    symlink = tmp_path / "sbx"
    config = tmp_path / "agents.toml"
    _make_executable(target)
    symlink.symlink_to(target)
    _write_candidate_config(config, symlink)

    report = inspect_worker_readiness(config)
    check = _checks(report)["broker_executable"]

    assert report.status == "blocked"
    assert check.status == "passed"
    assert check.reason_id is None
    assert check.evidence == {
        "resolved_path": str(target.resolve()),
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        "truth_domain": "real",
    }


def test_broker_hash_read_error_has_a_stable_secret_free_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = tmp_path / "sbx"
    config = tmp_path / "agents.toml"
    _make_executable(broker)
    _write_candidate_config(config, broker)

    def unreadable(_descriptor: int, _size: int) -> bytes:
        raise OSError("sensitive filesystem detail")

    monkeypatch.setattr("oracle_lab.worker_readiness.os.read", unreadable)

    report = inspect_worker_readiness(config)
    check = _checks(report)["broker_executable"]
    serialized = json.dumps(report.to_dict(), sort_keys=True)

    assert report.status == "failed"
    assert check.status == "failed"
    assert check.reason_id == "broker_executable_unreadable"
    assert "sensitive filesystem detail" not in serialized


def test_static_validation_catches_template_and_network_gaps(tmp_path: Path) -> None:
    broker = tmp_path / "sbx"
    config = tmp_path / "agents.toml"
    _make_executable(broker)
    # AgentWorkerConfig accepts the digest suffix; readiness additionally requires
    # a non-empty repository component and an explicit non-empty host list.
    _write_candidate_config(config, broker, template=f"@sha256:{_DIGEST}", hosts=())

    report = inspect_worker_readiness(config)
    checks = _checks(report)

    assert report.status == "failed"
    assert checks["worker_template_references"].reason_id == ("worker_template_not_digest_pinned")
    assert checks["worker_network_allowlists"].reason_id == (
        "worker_exact_network_allowlist_missing"
    )
    assert checks["broker_executable"].status == "passed"


@pytest.mark.parametrize("exists", [False, True], ids=("missing", "invalid"))
def test_unavailable_config_has_stable_checks_and_never_exposes_secret_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exists: bool,
) -> None:
    secret = "operator-secret-must-not-appear"
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    config = tmp_path / "agents.toml"
    if exists:
        config.write_text(f"invalid = {json.dumps(secret)} trailing-token\n", encoding="utf-8")

    report = inspect_worker_readiness(config)
    serialized = json.dumps(report.to_dict(), sort_keys=True)

    assert report.status == "failed"
    assert report.ready is False
    assert report.safe_to_start_worker is False
    assert secret not in serialized
    assert tuple(check.id for check in report.checks) == (
        "agents_config",
        "router_enabled",
        "enabled_coding_workers",
        "coding_worker_selection",
        "isolation_backend",
        "worker_sandbox_profiles",
        "worker_template_references",
        "worker_network_allowlists",
        "worker_environment_allowlists",
        "broker_executable",
        "production_isolation_evidence",
    )
    expected_reason = "agents_config_invalid" if exists else "agents_config_unavailable"
    assert _checks(report)["agents_config"].reason_id == expected_reason
    assert _checks(report)["production_isolation_evidence"].evidence["blocker_ids"] == (
        _EXPECTED_BLOCKERS
    )


def test_valid_toml_with_secret_wrong_scalar_is_collapsed_to_safe_config_error(
    tmp_path: Path,
) -> None:
    secret = "operator-secret-must-not-appear"
    config = tmp_path / "agents.toml"
    config.write_text(
        "\n".join(
            (
                "[router]",
                "enabled = true",
                "",
                "[workers.codex]",
                "enabled = true",
                'adapter = "codex"',
                f"timeout_seconds = {json.dumps(secret)}",
            )
        ),
        encoding="utf-8",
    )

    report = inspect_worker_readiness(config)
    serialized = json.dumps(report.to_dict(), sort_keys=True)

    assert report.status == "failed"
    assert _checks(report)["agents_config"].reason_id == "agents_config_invalid"
    assert secret not in serialized
