from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest

from oracle_lab import agent_adapters
from oracle_lab.agent_adapters import (
    AgentAdapterError,
    CodexAdapter,
    WorkerExecutionProfile,
    WorkerTask,
)
from oracle_lab.coding_isolation import (
    REQUIRED_ISOLATION_CAPABILITIES,
    IsolationAttestation,
    IsolationRunFailed,
    IsolationRunRequest,
    IsolationRunResult,
    receipt_sha256,
)
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.workspace_archive import (
    ValidatedWorkspaceExport,
    WorkspaceArchiveLimits,
    build_workspace_export,
)


def _git(repository: Path, *arguments: str) -> bytes:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    return completed.stdout


def _repository(root: Path) -> Path:
    repository = root / "source"
    repository.mkdir()
    _git(repository, "init", "-q")
    _git(repository, "config", "user.name", "Workspace Archive Test")
    _git(repository, "config", "user.email", "workspace@example.invalid")
    (repository / "tracked.txt").write_text("base\n", encoding="utf-8")
    _git(repository, "add", "tracked.txt")
    _git(repository, "commit", "-qm", "base")
    return repository


def _source_event() -> Event:
    return Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="workspace-archive-test"),
        session_id="ses_workspace_export",
        branch_id="br_workspace_export",
        payload={"content": "Create the requested candidate patch."},
    )


def _attestation() -> IsolationAttestation:
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
            for capability in sorted(REQUIRED_ISOLATION_CAPABILITIES)
        ],
    }
    return IsolationAttestation(
        backend="fixture-microvm",
        broker_executable_path="/usr/local/bin/oracle-fixture-broker",
        broker_executable_sha256="1" * 64,
        client_version="fixture-client-1",
        server_version="fixture-server-1",
        template_reference="fixture-template@sha256:" + "a" * 64,
        template_identity="sha256:" + "a" * 64,
        policy_sha256="2" * 64,
        conformance_suite_version="fixture-suite-1",
        conformance_receipt_sha256=receipt_sha256(receipt),
        capabilities=tuple(REQUIRED_ISOLATION_CAPABILITIES),
        receipt=receipt,
    )


class _ArchiveBinding:
    def __init__(
        self,
        attestation: IsolationAttestation,
        events: list[str],
        *,
        mismatch: str | None = None,
    ) -> None:
        self.attestation = attestation
        self.events = events
        self.mismatch = mismatch
        self.requests: list[IsolationRunRequest] = []

    def run(self, request: IsolationRunRequest) -> IsolationRunResult:
        self.requests.append(request)
        self.events.append("run_started")
        with tempfile.TemporaryDirectory(prefix="oracle-fixture-guest-") as raw_guest:
            guest = Path(raw_guest) / "workspace"
            shutil.copytree(
                request.workspace,
                guest,
                symlinks=True,
                ignore=shutil.ignore_patterns(".git"),
            )
            (guest / "tracked.txt").write_text("changed in microvm\n", encoding="utf-8")
            generated = guest / "generated.bin"
            generated.write_bytes(b"\x00microvm\xff")
            generated.chmod(0o755)
            exported = build_workspace_export(
                guest,
                WorkspaceArchiveLimits(
                    max_raw_bytes=request.max_workspace_export_bytes,
                    max_entries=request.max_workspace_entries,
                    max_regular_payload_bytes=request.max_workspace_export_bytes,
                ),
            )
        self.events.append("opaque_export_ready")
        cleanup_confirmed = self.mismatch != "cleanup"
        self.events.append("cleanup_confirmed" if cleanup_confirmed else "cleanup_failed")
        export_sha256 = exported.sha256
        export_entries = exported.entry_count
        if self.mismatch == "hash":
            export_sha256 = "f" * 64
        elif self.mismatch == "count":
            export_entries += 1
        return IsolationRunResult(
            exit_code=0,
            stdout=b"fixture stdout",
            stderr=b"",
            timed_out=False,
            output_limited=False,
            actual_command=request.command,
            guest_executable_path="/guest/bin/codex",
            guest_executable_version="fixture-codex-1",
            guest_executable_version_status="reported",
            sandbox_id="sandbox-fixture-workspace",
            workspace_export=exported.data,
            workspace_export_sha256=export_sha256,
            workspace_export_bytes=exported.size_bytes,
            workspace_export_entries=export_entries,
            cleanup_confirmed=cleanup_confirmed,
            attestation=self.attestation,
        )


class _FailedBinding:
    def __init__(self, attestation: IsolationAttestation, events: list[str]) -> None:
        self.attestation = attestation
        self.events = events

    def run(self, request: IsolationRunRequest) -> IsolationRunResult:
        self.events.append("failed_output_captured")
        self.events.append("cleanup_confirmed")
        raise IsolationRunFailed(
            exit_code=137,
            stdout=b"bounded failure stdout\x00\xff",
            stderr=b"bounded failure stderr\x80",
            timed_out=False,
            output_limited=True,
            actual_command=request.command,
            guest_executable_path="/guest/bin/codex",
            guest_executable_version="fixture-codex-1",
            guest_executable_version_status="reported",
            sandbox_id="sandbox-failed-workspace",
            cleanup_confirmed=True,
            attestation=self.attestation,
            max_output_bytes=request.max_output_bytes,
        )


def _adapter(
    tmp_path: Path,
    binding: _ArchiveBinding | _FailedBinding,
) -> CodexAdapter:
    profile = WorkerExecutionProfile(
        id="codex-fixture",
        adapter="codex",
        executable="codex",
        timeout_seconds=30,
        max_output_bytes=4096,
        sandbox_profile="external-broker",
        max_workspace_export_bytes=1024 * 1024,
        max_workspace_entries=100,
    ).with_isolation_attestation(binding.attestation.to_dict())
    return CodexAdapter(
        profile=profile,
        isolation_binding=binding,
        repository_workspace_root=tmp_path / "repository-workspaces",
        environment={},
    )


def _task(repository: Path) -> WorkerTask:
    return WorkerTask(
        _source_event(),
        "Change the tracked file and add the generated binary.",
        task_kind="repository_edit",
        repository=str(repository),
    )


def test_brokered_repository_edit_builds_patch_from_post_cleanup_export(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _repository(tmp_path)
    source_head = _git(repository, "rev-parse", "HEAD").strip()
    source_status = _git(repository, "status", "--porcelain=v1", "-z")
    events: list[str] = []
    binding = _ArchiveBinding(_attestation(), events)
    adapter = _adapter(tmp_path, binding)
    original_materialize = agent_adapters.materialize_workspace_export

    def observed_materialize(
        export: ValidatedWorkspaceExport,
        destination: str | Path,
    ) -> Path:
        assert events[-1] == "cleanup_confirmed"
        events.append("materialize")
        return original_materialize(export, destination)

    monkeypatch.setattr(
        agent_adapters,
        "materialize_workspace_export",
        observed_materialize,
    )

    result = adapter.run(_task(repository))

    assert result.succeeded
    assert set(result.changed_paths) == {"generated.bin", "tracked.txt"}
    assert b"changed in microvm" in result.patch_bytes
    assert b"GIT binary patch" in result.patch_bytes
    assert result.changed_modes["generated.bin"] == "100755"
    assert result.source_worktree_unchanged is True
    assert result.isolation_cleanup_confirmed is True
    assert result.workspace_export_sha256 is not None
    assert result.workspace_export_bytes is not None
    assert result.workspace_export_entries == 2
    assert events == [
        "run_started",
        "opaque_export_ready",
        "cleanup_confirmed",
        "materialize",
    ]
    assert (repository / "tracked.txt").read_text(encoding="utf-8") == "base\n"
    assert not (repository / "generated.bin").exists()
    assert _git(repository, "rev-parse", "HEAD").strip() == source_head
    assert _git(repository, "status", "--porcelain=v1", "-z") == source_status


@pytest.mark.parametrize("mismatch", ["count", "hash", "cleanup"])
def test_brokered_export_identity_or_cleanup_failure_stops_before_materialization_and_git_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mismatch: str,
) -> None:
    repository = _repository(tmp_path)
    events: list[str] = []
    binding = _ArchiveBinding(_attestation(), events, mismatch=mismatch)
    adapter = _adapter(tmp_path, binding)

    def forbidden_materialize(*_args: object, **_kwargs: object) -> Path:
        raise AssertionError("workspace materialized before export identity and cleanup passed")

    def forbidden_capture(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("Host Git capture ran before export identity and cleanup passed")

    monkeypatch.setattr(
        agent_adapters,
        "materialize_workspace_export",
        forbidden_materialize,
    )
    monkeypatch.setattr(adapter, "_capture_repository_patch", forbidden_capture)

    with pytest.raises(AgentAdapterError):
        adapter.run(_task(repository))

    assert "materialize" not in events
    assert (repository / "tracked.txt").read_text(encoding="utf-8") == "base\n"
    assert not (repository / "generated.bin").exists()


def test_brokered_failed_run_returns_archivable_raw_output_without_candidate_patch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _repository(tmp_path)
    source_head = _git(repository, "rev-parse", "HEAD").strip()
    source_status = _git(repository, "status", "--porcelain=v1", "-z")
    events: list[str] = []
    binding = _FailedBinding(_attestation(), events)
    adapter = _adapter(tmp_path, binding)

    def forbidden_materialize(*_args: object, **_kwargs: object) -> Path:
        raise AssertionError("failed worker workspace must never be materialized")

    def forbidden_capture(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("failed worker workspace must never become a candidate patch")

    monkeypatch.setattr(agent_adapters, "materialize_workspace_export", forbidden_materialize)
    monkeypatch.setattr(adapter, "_capture_repository_patch", forbidden_capture)

    result = adapter.run(_task(repository))

    assert not result.succeeded
    assert result.exit_code == 137
    assert result.output_limited is True
    assert result.stdout_bytes == b"bounded failure stdout\x00\xff"
    assert result.stderr_bytes == b"bounded failure stderr\x80"
    assert result.patch_bytes == b""
    assert result.changed_paths == ()
    assert result.isolation_sandbox_id == "sandbox-failed-workspace"
    assert result.isolation_cleanup_confirmed is True
    assert result.workspace_export_sha256 is None
    assert result.workspace_export_bytes is None
    assert result.workspace_export_entries is None
    assert result.source_worktree_unchanged is True
    assert events == ["failed_output_captured", "cleanup_confirmed"]
    assert (repository / "tracked.txt").read_text(encoding="utf-8") == "base\n"
    assert _git(repository, "rev-parse", "HEAD").strip() == source_head
    assert _git(repository, "status", "--porcelain=v1", "-z") == source_status
