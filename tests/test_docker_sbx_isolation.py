from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

import oracle_lab.docker_sbx_isolation as sbx_module
from oracle_lab.agent_adapters import WorkerExecutionProfile
from oracle_lab.coding_isolation import (
    REQUIRED_ISOLATION_CAPABILITIES,
    CodingIsolationError,
    IsolationRunFailed,
    IsolationRunRequest,
)
from oracle_lab.docker_sbx_isolation import (
    _GUEST_CONFORMANCE_PROBE_SCRIPT,
    _GUEST_WORKSPACE_EXPORT_SCRIPT,
    CommandResult,
    DockerSbxIsolationBroker,
    _parse_version,
    build_coding_worker_isolation_broker,
)
from oracle_lab.git_control import fingerprint_git_control
from oracle_lab.workspace_archive import WorkspaceArchiveLimits, build_workspace_export

_DIGEST = "a" * 64
_TEMPLATE = f"docker.io/oracle-lab/synthetic-fixture@sha256:{_DIGEST}"
_VERSION = (
    b"Client Version:  v0.39.0 1111111111111111111111111111111111111111\n"
    b"Server Version:  v0.39.0 2222222222222222222222222222222222222222\n"
)


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _empty_export() -> bytes:
    return b"ORACLELAB-WORKSPACE-V1\x00" + (0).to_bytes(8, "big")


def _policy_check(target: str, *, allowed: bool, sandbox: str | None) -> bytes:
    value: dict[str, object] = {
        "allowed": allowed,
        "action": "net:connect:tcp",
        "context": "global" if sandbox is None else f"sandbox:{sandbox}",
        "governance": {
            "active": False,
            "organization": "",
            "organization_unavailable": False,
            "last_synced_status": "",
            "last_synced_message": "",
        },
        "resource_type": "net:domain",
        "resource_value": f"{target}:443",
        "target": target,
        "type": "network",
    }
    if allowed:
        value["origin"] = "local"
        value["rule"] = "oracle-lab-synthetic-fixture"
    else:
        value["deny_kind"] = "implicit"
        value["reason"] = "No matching allow rule (default deny)"
    return _json_bytes(value)


class _SyntheticSbxRunner:
    """Deterministic protocol double; it never invokes sbx or a model."""

    evidence_origin = "synthetic_fixture"

    def __init__(self, *, executable: Path, exports: Sequence[bytes]) -> None:
        self.executable = str(executable)
        self.exports = list(exports)
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str]] = []
        self.sandboxes: set[str] = set()
        self.hosts: dict[str, set[str]] = {}

    @staticmethod
    def _sandbox_rule(sandbox: str, host: str, index: int) -> dict[str, object]:
        return {
            "id": f"rule-{index}",
            "name": f"allow-{index}",
            "policy_id": f"policy-{sandbox}",
            "scope": "sandbox",
            "applies_to": sandbox,
            "resource_type": "network",
            "decision": "allow",
            "resources": [host],
            "origin": "local",
            "layer": "local",
            "status": "active",
            "editable": True,
        }

    def run(
        self,
        argv: Sequence[str],
        *,
        input_bytes: bytes,
        environment: Mapping[str, str],
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> CommandResult:
        del input_bytes, timeout_seconds, max_output_bytes
        command = tuple(argv)
        self.calls.append(command)
        self.environments.append(dict(environment))
        assert command[0] == self.executable
        args = command[1:]
        stdout = b""

        if args == ("version",):
            stdout = _VERSION
        elif args == ("template", "ls", "--json"):
            stdout = _json_bytes(
                {
                    "images": [
                        {
                            "id": "synthetic-image-id",
                            "repository": "docker.io/oracle-lab/synthetic-fixture",
                            "tag": "",
                            "flavor": "shell",
                            "created_at": "2026-08-31T00:00:00Z",
                            "size": 123,
                            "digest": f"sha256:{_DIGEST}",
                        }
                    ]
                }
            )
        elif args == (
            "policy",
            "ls",
            "--type",
            "network",
            "--include-inactive",
            "--json",
        ):
            stdout = b'{"rules":[]}'
        elif args == ("ls", "--quiet"):
            stdout = ("\n".join(sorted(self.sandboxes)) + ("\n" if self.sandboxes else "")).encode()
        elif args[:2] == ("create", "shell"):
            sandbox = args[args.index("--name") + 1]
            self.sandboxes.add(sandbox)
            self.hosts[sandbox] = set()
        elif args[:3] == ("policy", "allow", "network"):
            sandbox = args[args.index("--sandbox") + 1]
            self.hosts[sandbox].add(args[-1])
        elif args[:2] == ("policy", "ls") and len(args) > 2 and not args[2].startswith("--"):
            sandbox = args[2]
            rules = [
                self._sandbox_rule(sandbox, host, index)
                for index, host in enumerate(sorted(self.hosts[sandbox]), start=1)
            ]
            stdout = _json_bytes({"rules": rules})
        elif args[:3] == ("policy", "check", "network"):
            sandbox = args[args.index("--sandbox") + 1] if "--sandbox" in args else None
            target = args[-1]
            stdout = _policy_check(
                target,
                allowed=sandbox is not None and target in self.hosts[sandbox],
                sandbox=sandbox,
            )
        elif args[:2] == ("policy", "log"):
            stdout = b'{"allowed_hosts":[],"blocked_hosts":[]}'
        elif args[:1] == ("exec",) and _GUEST_CONFORMANCE_PROBE_SCRIPT in args:
            stdout = _json_bytes(
                {
                    "schema_version": 1,
                    "linux_guest": True,
                    "host_marker_visible": False,
                    "host_home_visible": False,
                    "host_process_sentinel_visible": False,
                    "credential_sentinel_visible": False,
                    "sandbox_docker_socket_visible": True,
                    "host_docker_endpoint_visible": False,
                    "ssh_auth_sock_visible": False,
                    "unsafe_credential_environment": False,
                    "shared_agent_state_mount_visible": False,
                    "guest_home": "/home/agent",
                    "guest_write_complete": True,
                    "detached_descendant_pid": 73,
                }
            )
        elif args[:1] == ("exec",) and _GUEST_WORKSPACE_EXPORT_SCRIPT in args:
            stdout = self.exports.pop(0)
        elif args[:1] == ("exec",):
            stdout = b"synthetic worker output"
        elif args[:2] == ("rm", "--force"):
            sandbox = args[2]
            self.sandboxes.discard(sandbox)
            self.hosts.pop(sandbox, None)
        else:  # pragma: no cover - a new production argv must get a fixture first
            raise AssertionError(f"unexpected sbx argv: {command!r}")

        return CommandResult(command, 0, stdout, b"")


@dataclass(frozen=True)
class _RouterConfig:
    isolation_backend: str
    isolation_broker_executable: str = "sbx"


def _executable(tmp_path: Path) -> Path:
    executable = tmp_path / "sbx-real-binary"
    executable.write_bytes(b"#!/bin/sh\nexit 0\n")
    executable.chmod(0o700)
    return executable


def _profile() -> WorkerExecutionProfile:
    return WorkerExecutionProfile(
        id="codex",
        adapter="codex",
        executable="codex",
        timeout_seconds=30,
        max_output_bytes=4096,
        sandbox_profile="external-broker",
        allowed_environment_names=("TERM",),
        isolation_template_reference=_TEMPLATE,
        isolation_allowed_hosts=("api.openai.com",),
        max_workspace_export_bytes=1024 * 1024,
        max_workspace_entries=1024,
    )


def _codex_command(profile: WorkerExecutionProfile) -> tuple[str, ...]:
    command = [
        profile.executable,
        "exec",
        "--json",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--dangerously-bypass-approvals-and-sandbox",
        "--color",
        "never",
    ]
    if profile.model is not None:
        command.extend(("--model", profile.model))
    command.append("-")
    return tuple(command)


def _synthetic_broker(
    tmp_path: Path,
    *,
    executable: Path,
    runner: _SyntheticSbxRunner,
) -> DockerSbxIsolationBroker:
    workspace_root = tmp_path / "workspaces"
    workspace_root.mkdir(exist_ok=True)
    return DockerSbxIsolationBroker(
        executable=str(executable),
        state_root=tmp_path / "state",
        workspace_root=workspace_root,
        runner=runner,
        clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
        synthetic_fixture=True,
    )


def _git_repository(path: Path) -> None:
    path.mkdir()
    (path / "target.txt").write_text("before\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "fixture@example.invalid"],
        cwd=path,
        check=True,
    )
    subprocess.run(["git", "add", "target.txt"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=path, check=True)


def _request(
    profile: WorkerExecutionProfile,
    workspace: Path,
    *,
    input_bytes: bytes = b"synthetic fixture input",
) -> IsolationRunRequest:
    return IsolationRunRequest(
        adapter=profile.adapter,
        workspace=workspace.resolve(),
        command=_codex_command(profile),
        input_bytes=input_bytes,
        environment={"TERM": "dumb"},
        timeout_seconds=profile.timeout_seconds,
        max_output_bytes=profile.max_output_bytes,
        max_workspace_export_bytes=profile.max_workspace_export_bytes,
        max_workspace_entries=profile.max_workspace_entries,
    )


def test_factory_is_side_effect_free_when_disabled(tmp_path: Path) -> None:
    state_root = tmp_path / "must-not-exist"

    result = build_coding_worker_isolation_broker(
        _RouterConfig("disabled"),
        state_root=state_root,
    )

    assert result is None
    assert not state_root.exists()


def test_factory_rejects_an_unknown_backend_without_starting_a_process(tmp_path: Path) -> None:
    with pytest.raises(CodingIsolationError, match="unsupported coding-worker"):
        build_coding_worker_isolation_broker(
            _RouterConfig("unknown"),
            state_root=tmp_path / "must-not-exist",
        )


def test_production_bind_fails_before_any_sbx_process_or_attestation(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspaces"
    workspace_root.mkdir()
    state_root = tmp_path / "state"
    broker = DockerSbxIsolationBroker(
        executable=str(tmp_path / "missing-sbx"),
        state_root=state_root,
        workspace_root=workspace_root,
    )

    with pytest.raises(
        CodingIsolationError, match="production Docker sbx attestation is unavailable"
    ):
        broker.bind(_profile())

    assert not state_root.exists()


def test_synthetic_mode_requires_an_explicitly_marked_runner(tmp_path: Path) -> None:
    class UnmarkedRunner:
        def run(self, *_args: object, **_kwargs: object) -> CommandResult:
            raise AssertionError("unmarked runner must never run")

    with pytest.raises(CodingIsolationError, match="explicitly marked synthetic runner"):
        DockerSbxIsolationBroker(
            executable="sbx",
            state_root=tmp_path / "state",
            workspace_root=tmp_path / "workspaces",
            runner=UnmarkedRunner(),  # type: ignore[arg-type]
            synthetic_fixture=True,
        )


@pytest.mark.parametrize(
    "raw",
    [
        b"Client Version:  v0.38.9 1111111\nServer Version:  v0.39.0 2222222\n",
        b"sbx version: v0.39.0 1111111\n",
        b"Client Version: v0.39.0 1111111\nServer Version: v0.39.0 2222222\n",
    ],
)
def test_version_gate_rejects_old_or_unrecognized_output(raw: bytes) -> None:
    with pytest.raises(CodingIsolationError):
        _parse_version(raw)


def test_bind_refuses_current_template_inventory_without_registry_digest(
    tmp_path: Path,
) -> None:
    executable = _executable(tmp_path)

    class NoDigestRunner(_SyntheticSbxRunner):
        def run(self, argv: Sequence[str], **options: object) -> CommandResult:
            result = super().run(argv, **options)  # type: ignore[arg-type]
            if tuple(argv[1:]) == ("template", "ls", "--json"):
                value = json.loads(result.stdout)
                del value["images"][0]["digest"]
                return CommandResult(tuple(argv), 0, _json_bytes(value), b"")
            return result

    broker = _synthetic_broker(
        tmp_path,
        executable=executable,
        runner=NoDigestRunner(executable=executable, exports=[]),
    )

    with pytest.raises(CodingIsolationError, match="does not expose an attestable registry digest"):
        broker.bind(_profile())


def test_fake_runner_exercises_attestation_export_import_and_cleanup(tmp_path: Path) -> None:
    executable = _executable(tmp_path)
    edited = tmp_path / "edited"
    edited.mkdir()
    (edited / "target.txt").write_text("after\n", encoding="utf-8")
    limits = WorkspaceArchiveLimits(1024 * 1024, 1024, 1024 * 1024)
    edited_export = build_workspace_export(edited, limits).data
    runner = _SyntheticSbxRunner(
        executable=executable,
        exports=[_empty_export(), edited_export],
    )
    broker = _synthetic_broker(
        tmp_path,
        executable=executable,
        runner=runner,
    )

    profile = _profile()
    binding = broker.bind(profile)
    repository = tmp_path / "workspaces" / "repository"
    _git_repository(repository)
    git_before = fingerprint_git_control(repository / ".git")
    request = IsolationRunRequest(
        adapter="codex",
        workspace=repository.resolve(),
        command=_codex_command(profile),
        input_bytes=b"synthetic fixture input",
        environment={"TERM": "dumb"},
        timeout_seconds=30,
        max_output_bytes=4096,
        max_workspace_export_bytes=1024 * 1024,
        max_workspace_entries=1024,
    )

    result = binding.run(request)

    assert result.stdout == b"synthetic worker output"
    assert result.actual_command == request.command
    assert result.workspace_export == edited_export
    assert result.workspace_export_sha256 == hashlib.sha256(edited_export).hexdigest()
    assert result.cleanup_confirmed is True
    # The binding returns opaque bytes only.  The adapter is the sole importer.
    assert (repository / "target.txt").read_text(encoding="utf-8") == "before\n"
    assert fingerprint_git_control(repository / ".git") == git_before
    assert runner.sandboxes == set()
    assert result.attestation.backend == "docker-sbx-synthetic-fixture"
    assert result.attestation.receipt["evidence_origin"] == "synthetic_fixture"
    assert set(result.attestation.capabilities) == REQUIRED_ISOLATION_CAPABILITIES
    assert {check["id"] for check in result.attestation.receipt["checks"]} == set(
        REQUIRED_ISOLATION_CAPABILITIES
    )
    assert all(check["evidence"] for check in result.attestation.receipt["checks"])
    assert all(
        check["evidence"]["evidence_origin"] == "synthetic_fixture"
        for check in result.attestation.receipt["checks"]
    )
    assert all("cp" not in call[1:2] for call in runner.calls)
    creates = [call for call in runner.calls if call[1:3] == ("create", "shell")]
    assert creates
    for call in creates:
        assert call[3].startswith(str(tmp_path / "state"))
        assert call[4:6] == ("--clone", "--name")
        assert "--no-share-skills" in call
    assert any(
        len(call) > 5 and call[1:4] == ("exec", "-i", "-w") and "codex" in call
        for call in runner.calls
    )
    assert any(
        environment.get("ORACLE_LAB_CREDENTIAL_PROBE") for environment in runner.environments
    )


def test_binding_rejects_argv_limit_and_workspace_drift_before_create(tmp_path: Path) -> None:
    executable = _executable(tmp_path)
    runner = _SyntheticSbxRunner(executable=executable, exports=[_empty_export()])
    broker = _synthetic_broker(tmp_path, executable=executable, runner=runner)
    profile = replace(_profile(), model="bound-model")
    binding = broker.bind(profile)
    workspace = tmp_path / "workspaces" / "task"
    workspace.mkdir()
    valid = _request(profile, workspace)
    create_count = sum(call[1:3] == ("create", "shell") for call in runner.calls)

    with pytest.raises(CodingIsolationError, match="argv is not canonical"):
        binding.run(replace(valid, command=(profile.executable, "exec", "--json", "-")))
    with pytest.raises(CodingIsolationError, match="limits differ"):
        binding.run(replace(valid, max_output_bytes=profile.max_output_bytes + 1))
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(CodingIsolationError, match="outside the bound workspace_root"):
        binding.run(replace(valid, workspace=outside.resolve()))

    assert sum(call[1:3] == ("create", "shell") for call in runner.calls) == create_count


def test_opencode_argv_binds_model_and_exact_prompt(tmp_path: Path) -> None:
    executable = _executable(tmp_path)
    runner = _SyntheticSbxRunner(
        executable=executable,
        exports=[_empty_export(), _empty_export()],
    )
    broker = _synthetic_broker(tmp_path, executable=executable, runner=runner)
    profile = replace(
        _profile(),
        id="opencode",
        adapter="opencode",
        executable="opencode",
        model="bound-model",
    )
    binding = broker.bind(profile)
    workspace = tmp_path / "workspaces" / "opencode-task"
    workspace.mkdir()
    prompt = "exact prompt\n--not-an-option"
    command = (
        "opencode",
        "run",
        "--format",
        "json",
        "--model",
        "bound-model",
        prompt,
    )
    request = IsolationRunRequest(
        adapter="opencode",
        workspace=workspace.resolve(),
        command=command,
        input_bytes=prompt.encode(),
        environment={"TERM": "dumb"},
        timeout_seconds=profile.timeout_seconds,
        max_output_bytes=profile.max_output_bytes,
        max_workspace_export_bytes=profile.max_workspace_export_bytes,
        max_workspace_entries=profile.max_workspace_entries,
    )

    result = binding.run(request)

    assert result.actual_command == command
    assert runner.sandboxes == set()


def test_synthetic_bind_rejects_overlapping_workspace_and_state_roots(
    tmp_path: Path,
) -> None:
    executable = _executable(tmp_path)
    workspace_root = tmp_path / "workspaces"
    workspace_root.mkdir()
    runner = _SyntheticSbxRunner(executable=executable, exports=[])
    broker = DockerSbxIsolationBroker(
        executable=str(executable),
        state_root=workspace_root / "state",
        workspace_root=workspace_root,
        runner=runner,
        synthetic_fixture=True,
    )

    with pytest.raises(CodingIsolationError, match="must be disjoint"):
        broker.bind(_profile())

    assert runner.calls == []


@pytest.mark.parametrize(
    ("timed_out", "output_limited", "expected_exit"),
    [
        (True, False, None),
        (False, True, 137),
        (False, False, 17),
    ],
)
def test_worker_failure_preserves_bounded_output_without_export_after_cleanup(
    tmp_path: Path,
    timed_out: bool,
    output_limited: bool,
    expected_exit: int | None,
) -> None:
    executable = _executable(tmp_path)

    class LimitedRunner(_SyntheticSbxRunner):
        def run(self, argv: Sequence[str], **options: object) -> CommandResult:
            result = super().run(argv, **options)  # type: ignore[arg-type]
            if tuple(argv[1:2]) == ("exec",) and "codex" in argv:
                return CommandResult(
                    tuple(argv),
                    expected_exit,
                    b"bounded-prefix\x00\xff",
                    b"bounded-error\x80",
                    timed_out=timed_out,
                    output_limited=output_limited,
                )
            return result

    runner = LimitedRunner(
        executable=executable,
        exports=[_empty_export(), _empty_export()],
    )
    broker = _synthetic_broker(
        tmp_path,
        executable=executable,
        runner=runner,
    )
    profile = _profile()
    binding = broker.bind(profile)
    workspace = tmp_path / "workspaces" / "workspace"
    workspace.mkdir()
    request = IsolationRunRequest(
        adapter="codex",
        workspace=workspace.resolve(),
        command=_codex_command(profile),
        input_bytes=b"synthetic fixture input",
        environment={"TERM": "dumb"},
        timeout_seconds=profile.timeout_seconds,
        max_output_bytes=profile.max_output_bytes,
        max_workspace_export_bytes=profile.max_workspace_export_bytes,
        max_workspace_entries=profile.max_workspace_entries,
    )

    with pytest.raises(IsolationRunFailed) as captured:
        binding.run(request)

    failure = captured.value
    assert failure.exit_code == expected_exit
    assert failure.stdout == b"bounded-prefix\x00\xff"
    assert failure.stderr == b"bounded-error\x80"
    assert failure.timed_out is timed_out
    assert failure.output_limited is output_limited
    assert failure.actual_command == request.command
    assert failure.sandbox_id.startswith("oracle-lab-run-")
    assert failure.cleanup_confirmed is True
    assert failure.attestation is binding.attestation
    assert not hasattr(failure, "workspace_export")
    assert runner.exports == [_empty_export()]
    assert runner.sandboxes == set()


def test_failed_worker_cleanup_failure_remains_a_hard_boundary_failure(
    tmp_path: Path,
) -> None:
    executable = _executable(tmp_path)

    class CleanupFailureRunner(_SyntheticSbxRunner):
        def run(self, argv: Sequence[str], **options: object) -> CommandResult:
            result = super().run(argv, **options)  # type: ignore[arg-type]
            if tuple(argv[1:2]) == ("exec",) and "codex" in argv:
                return CommandResult(tuple(argv), 17, b"bounded", b"failure")
            if tuple(argv[1:3]) == ("rm", "--force"):
                sandbox = str(argv[3])
                if sandbox.startswith("oracle-lab-run-"):
                    self.sandboxes.add(sandbox)
                    self.hosts[sandbox] = set()
            return result

    runner = CleanupFailureRunner(
        executable=executable,
        exports=[_empty_export(), _empty_export()],
    )
    broker = _synthetic_broker(tmp_path, executable=executable, runner=runner)
    profile = _profile()
    binding = broker.bind(profile)
    workspace = tmp_path / "workspaces" / "cleanup-failure"
    workspace.mkdir()

    with pytest.raises(CodingIsolationError, match="cleanup was not confirmed") as captured:
        binding.run(_request(profile, workspace))

    assert not isinstance(captured.value, IsolationRunFailed)
    assert runner.exports == [_empty_export()]


def test_post_cleanup_runtime_identity_drift_rejects_structured_worker_failure(
    tmp_path: Path,
) -> None:
    executable = _executable(tmp_path)

    class RuntimeDriftRunner(_SyntheticSbxRunner):
        worker_finished = False

        def run(self, argv: Sequence[str], **options: object) -> CommandResult:
            result = super().run(argv, **options)  # type: ignore[arg-type]
            arguments = tuple(argv[1:])
            if arguments[:1] == ("exec",) and "codex" in argv:
                self.worker_finished = True
                return CommandResult(tuple(argv), 17, b"bounded", b"failure")
            if arguments == ("version",) and self.worker_finished:
                drifted = _VERSION.replace(b"1111111111", b"3333333333", 1)
                return CommandResult(tuple(argv), 0, drifted, b"")
            return result

    runner = RuntimeDriftRunner(
        executable=executable,
        exports=[_empty_export(), _empty_export()],
    )
    broker = _synthetic_broker(tmp_path, executable=executable, runner=runner)
    profile = _profile()
    binding = broker.bind(profile)
    workspace = tmp_path / "workspaces" / "runtime-drift"
    workspace.mkdir()

    with pytest.raises(CodingIsolationError, match="runtime identity drifted") as captured:
        binding.run(_request(profile, workspace))

    assert not isinstance(captured.value, IsolationRunFailed)
    assert runner.exports == [_empty_export()]
    assert runner.sandboxes == set()


def test_preexisting_generated_name_is_never_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = _executable(tmp_path)
    fixed = "0" * 32
    monkeypatch.setattr(sbx_module.uuid, "uuid4", lambda: type("U", (), {"hex": fixed})())
    runner = _SyntheticSbxRunner(executable=executable, exports=[])
    preexisting = f"oracle-lab-probe-{fixed}"
    runner.sandboxes.add(preexisting)
    runner.hosts[preexisting] = set()
    broker = _synthetic_broker(
        tmp_path,
        executable=executable,
        runner=runner,
    )

    with pytest.raises(CodingIsolationError, match="already exists"):
        broker.bind(_profile())

    assert preexisting in runner.sandboxes
    assert not any(call[1:2] == ("rm",) for call in runner.calls)


def test_partial_create_failure_is_leaked_fail_closed_not_deleted(
    tmp_path: Path,
) -> None:
    executable = _executable(tmp_path)

    class PartialCreateRunner(_SyntheticSbxRunner):
        def run(self, argv: Sequence[str], **options: object) -> CommandResult:
            result = super().run(argv, **options)  # type: ignore[arg-type]
            if tuple(argv[1:3]) == ("create", "shell"):
                return CommandResult(tuple(argv), 1, b"", b"synthetic create failure")
            return result

    runner = PartialCreateRunner(executable=executable, exports=[])
    broker = _synthetic_broker(tmp_path, executable=executable, runner=runner)

    with pytest.raises(CodingIsolationError, match=r"create shell .* failed"):
        broker.bind(_profile())

    assert len(runner.sandboxes) == 1
    assert not any(call[1:2] == ("rm",) for call in runner.calls)


def test_control_failure_never_copies_raw_stderr_into_the_exception(tmp_path: Path) -> None:
    executable = _executable(tmp_path)

    class FailedVersionRunner(_SyntheticSbxRunner):
        def run(self, argv: Sequence[str], **options: object) -> CommandResult:
            if tuple(argv[1:]) == ("version",):
                return CommandResult(tuple(argv), 1, b"", b"credential-shaped-secret")
            return super().run(argv, **options)  # type: ignore[arg-type]

    broker = _synthetic_broker(
        tmp_path,
        executable=executable,
        runner=FailedVersionRunner(executable=executable, exports=[]),
    )

    with pytest.raises(CodingIsolationError) as captured:
        broker.bind(_profile())

    assert "credential-shaped-secret" not in str(captured.value)
