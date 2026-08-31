from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from oracle_lab.agent_adapters import (
    AgentAdapterError,
    BaseAgentAdapter,
    DedicatedWorkspace,
    DirectAPIHost,
    HostWorkerRouter,
    WorkerExecutionProfile,
)
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.git_control import fingerprint_git_control
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventIntegrityError, EventStore
from oracle_lab.worker_read_model import WorkerReadModel

CONFIG = Path(__file__).parents[1] / "config"


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


def _repository(path: Path) -> Path:
    path.mkdir()
    _git(path, "init", "-q")
    _git(path, "config", "user.email", "tests@example.invalid")
    _git(path, "config", "user.name", "Oracle Lab Tests")
    (path / "target.txt").write_text("before\n", encoding="utf-8")
    _git(path, "add", "target.txt")
    _git(path, "commit", "-qm", "base")
    return path


def _inject_fake_codex(
    service: OracleLabService,
    executable: Path,
    *,
    workspace_root: Path,
    validation_commands: tuple[str, ...] = (),
) -> BaseAgentAdapter:
    """Install a deterministic test adapter without crossing the config gate."""

    profile = WorkerExecutionProfile(
        id="fake-codex",
        adapter="codex",
        executable=str(executable),
        timeout_seconds=10,
        max_output_bytes=4096,
        sandbox_profile="workspace-write",
        allowed_environment_names=("PATH",),
        max_retries=0,
        validation_commands=validation_commands,
    )
    adapter = BaseAgentAdapter(
        executable=str(executable),
        command_builder=lambda _prompt: (str(executable),),
        workspace_factory=DedicatedWorkspace(workspace_root / "analysis"),
        environment={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        profile=profile,
        repository_workspace_root=workspace_root / "repository-workspaces",
    )
    service.host_worker_router = HostWorkerRouter(
        codex=adapter,
        prefer_coding_agent="codex",
    )
    return adapter


def _source_fingerprint(repository: Path) -> tuple[bytes | str, ...]:
    index_path = Path(
        _git(
            repository,
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "index",
        )
        .decode()
        .strip()
    )
    return (
        _git(repository, "rev-parse", "--verify", "HEAD^{commit}"),
        _git(repository, "status", "--porcelain=v1", "-z", "--untracked-files=all"),
        _git(repository, "ls-files", "--stage", "-z"),
        _git(repository, "ls-files", "-v", "-z"),
        index_path.read_bytes(),
        BaseAgentAdapter._hash_source_tree(repository),
        fingerprint_git_control(repository / ".git"),
    )


def test_service_automation_routes_analysis_job_to_direct_api_host(tmp_path: Path) -> None:
    calls: list[tuple[str, dict]] = []

    def fake_frontier_host(task_type: str, payload: dict) -> dict:
        calls.append((task_type, payload))
        source_id = payload["source_event_id"]
        return {
            "events": [
                {
                    "type": "analysis.claim_detected",
                    "payload": {
                        "raw_text": "TIME_DILATION_FACTOR=1.78",
                        "status": "raw_claim",
                    },
                    "source_event_ids": [source_id],
                }
            ]
        }

    router = HostWorkerRouter(direct=DirectAPIHost(fake_frontier_host))
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        host_worker_router=router,
    )
    session = service.new_session("direct host")
    parent = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    output = service.store.append(
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
    service._enqueue_host_analysis_jobs(output)

    result = service.run_automation(max_jobs=1)

    assert result["processed"][0]["status"] == "completed"
    assert calls[0][0] == "claim_extraction"
    assert calls[0][1]["source_event_id"] == output.id
    claim = service.store.list_events(event_type=EventType.ANALYSIS_CLAIM_DETECTED)[-1]
    assert claim.actor == Actor(kind=ActorKind.HOST, id="direct-api-host")
    assert claim.parent_event_id == output.id
    assert claim.payload["source_event_ids"] == (output.id,)
    assert service._job_queue().list_jobs(kind="compare_claim_history")


def test_service_automation_routes_repository_job_to_isolated_adapter(
    tmp_path: Path, monkeypatch
) -> None:
    repository = tmp_path / "source-repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(
        ["git", "config", "user.email", "tests@example.invalid"],
        cwd=repository,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Oracle Lab Tests"],
        cwd=repository,
        check=True,
    )
    (repository / "target.txt").write_text("before\n", encoding="utf-8")
    subprocess.run(["git", "add", "target.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repository, check=True)
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("coding host")
    source = service.store.require(session["root_event_id"])
    executable = tmp_path / "fake-coding-agent"
    executable.write_text(
        "#!/bin/sh\n"
        'if [ "${1:-}" = "--version" ]; then printf \'fake-agent 1.0\\n\'; exit 0; fi\n'
        "cat >/dev/null\n"
        "printf 'after\\n' > target.txt\n"
        "printf 'worker stdout is audit material only\\n'\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    profile = WorkerExecutionProfile(
        id="fake-codex",
        adapter="codex",
        executable=executable.name,
        timeout_seconds=10,
        max_output_bytes=4096,
        sandbox_profile="workspace-write",
        allowed_environment_names=("PATH",),
        max_retries=0,
        validation_commands=("python -c \"print('validated')\"",),
    )
    adapter = BaseAgentAdapter(
        executable=executable.name,
        command_builder=lambda _prompt: (executable.name,),
        workspace_factory=DedicatedWorkspace(tmp_path / "workspaces"),
        environment={"PATH": f"{tmp_path}:{os.environ.get('PATH', '')}"},
        profile=profile,
        repository_workspace_root=tmp_path / "repository-workspaces",
    )
    service.host_worker_router = HostWorkerRouter(
        codex=adapter,
        prefer_coding_agent="codex",
    )
    enqueued = service.enqueue_repository_edit(
        source.id,
        "Change target.txt to after and do not commit.",
        repository=repository,
    )

    result = service.run_automation(until_human=True, max_jobs=2)

    assert result["processed"][0]["status"] == "completed"
    assert result["stopped"] == "human_judgment"
    assert (repository / "target.txt").read_text(encoding="utf-8") == "before\n"
    assert not service.store.list_events(event_type=EventType.ANALYSIS_SESSION_SUMMARY_UPDATED)
    patch = service.store.list_events(event_type=EventType.WORKER_PATCH_PROPOSED)[-1]
    assert patch.payload["task_event_id"] == enqueued["task_event"]["id"]
    with pytest.raises(EventIntegrityError, match="oracle curation"):
        service.store.append(
            Event.new(
                EventType.HUMAN_KEEP,
                actor=Actor(kind=ActorKind.HUMAN, id="operator"),
                session_id=patch.session_id,
                branch_id=patch.branch_id,
                parent_event_id=patch.id,
                causation_id=patch.id,
                payload={"event_id": patch.id, "target_event_id": patch.id},
            )
        )
    with pytest.raises(EventIntegrityError, match="requires a human actor"):
        service.store.append(
            Event.new(
                EventType.HUMAN_PATCH_APPROVED,
                actor=Actor(kind=ActorKind.WORKER, id="agent"),
                session_id=patch.session_id,
                branch_id=patch.branch_id,
                parent_event_id=patch.id,
                causation_id=patch.id,
                payload={
                    "patch_event_id": patch.id,
                    "patch_sha256": patch.payload["patch_sha256"],
                    "base_commit": patch.payload["base_commit"],
                },
            )
        )
    approval = service.approve_patch(patch.id)
    stale_staging = service._safe_staging_path(patch, repository)
    stale_staging.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "--detach",
            str(stale_staging),
            str(patch.payload["base_commit"]),
        ],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    (stale_staging / "partial-worker-state.txt").write_text(
        "must be discarded\n",
        encoding="utf-8",
    )

    applied = service.run_automation(max_jobs=1)

    assert applied["processed"][0]["status"] == "completed"
    state = service.patch_status(patch.id)["state"]
    staging = Path(state["staging_path"])
    assert state["status"] == "applied"
    assert (staging / "target.txt").read_text(encoding="utf-8") == "after\n"
    assert not (staging / "partial-worker-state.txt").exists()
    assert (repository / "target.txt").read_text(encoding="utf-8") == "before\n"

    from oracle_lab.tooling import DockerShellSandbox, ToolResult, ToolStatus

    monkeypatch.setattr(
        DockerShellSandbox,
        "run",
        lambda *_args, **_kwargs: ToolResult(
            "validation_fixture",
            ToolStatus.OK,
            output="validated\n",
            exit_code=0,
            raw_stdout=b"\xffvalidated\n",
            raw_stderr=b"\x00audit\n",
        ),
    )
    validated = service.run_automation(max_jobs=1)

    assert validated["processed"][0]["status"] == "completed"
    final_state = service.patch_status(patch.id)["state"]
    assert final_state["validation_status"] == "passed"
    validation = service.store.list_events(event_type=EventType.WORKER_VALIDATION_COMPLETED)[-1]
    assert validation.payload["truth_domain"] == "sandbox"
    assert validation.payload["approval_event_id"] == approval["approval_event"]["id"]
    assert validation.payload["status"] == "ok"
    assert validation.payload["error"] is None
    assert Path(validation.payload["archive_manifest"]["stdout.bin"]["path"]).read_bytes() == (
        b"\xffvalidated\n"
    )
    service.store.rebuild_projections()
    rebuilt = service.patch_status(patch.id)["state"]
    assert rebuilt["status"] == "applied"
    assert rebuilt["validation_status"] == "passed"

    read_model = WorkerReadModel(service.store)
    changes_before = service.store.connection.total_changes
    events_before = tuple(event.id for event in service.store.list_events())
    task_status = service.worker_task_status(enqueued["task_event"]["id"])
    patch_show = service.patch_show(patch.id)

    assert task_status == read_model.worker_task_status(enqueued["task_event"]["id"])
    assert set(task_status) == {"task", "runs", "patches"}
    assert patch_show == read_model.patch_show(patch.id)
    assert patch_show == read_model.patch_status(patch.id)
    assert patch_show == service.patch_status(patch.id)
    assert set(patch_show) == {"patch", "state", "worker_run"}
    assert patch_show["state"]["changed_paths"] == list(patch.payload["changed_paths"])
    assert patch_show["state"]["validation_event_ids"] == [validation.id]
    assert tuple(event.id for event in service.store.list_events()) == events_before
    assert service.store.connection.total_changes == changes_before


def test_default_service_rejects_enabled_coding_worker_before_subprocess(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = tmp_path / "config"
    config.mkdir()
    started = tmp_path / "unisolated-worker-started"
    executable = tmp_path / "unisolated-codex"
    executable.write_text(
        f"#!/bin/sh\ntouch {started}\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    for name in ("models.toml", "providers.toml", "policies.toml", "tools.toml"):
        shutil.copy2(CONFIG / name, config / name)
    (config / "agents.toml").write_text(
        f"""
[router]
enabled = true
prefer_coding_agent = "codex"

[workers.codex]
enabled = true
adapter = "codex"
executable = "{executable}"
timeout_seconds = 10
max_output_bytes = 1024
sandbox_profile = "workspace-write"
allowed_environment_names = ["PATH"]
validation_commands = []
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ORACLE_LAB_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("ORACLE_LAB_CONFIG", str(config))

    with pytest.raises(AgentAdapterError, match="OS-level coding-worker isolation"):
        OracleLabService.default()
    assert not started.exists()


def test_default_full_worker_flow_keeps_all_host_state_outside_source(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = _repository(tmp_path / "source")
    executable = tmp_path / "fake-codex"
    executable.write_text(
        "#!/bin/sh\n"
        'if [ "${1:-}" = "--version" ]; then printf \'fake-codex 1.0\\n\'; exit 0; fi\n'
        "cat >/dev/null\n"
        "printf 'after\\n' > target.txt\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    data_home = tmp_path / "data"
    for name in (
        "ORACLE_LAB_HOME",
        "ORACLE_LAB_DB",
        "ORACLE_LAB_ARCHIVE",
        "ORACLE_LAB_RENDERING",
        "ORACLE_LAB_STAGING",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(data_home))
    monkeypatch.setenv("ORACLE_LAB_CONFIG", str(CONFIG))
    monkeypatch.chdir(repository)
    before = _source_fingerprint(repository)

    service = OracleLabService.default()
    try:
        adapter = _inject_fake_codex(
            service,
            executable,
            workspace_root=service.home / "injected-test-worker",
        )
        assert service.home == (data_home / "oracle-lab").resolve()
        assert not (repository / ".oracle_lab").exists()
        session = service.new_session("default path containment")
        service.enqueue_repository_edit(
            str(session["root_event_id"]),
            "Change target.txt to after and do not commit.",
            repository=repository,
        )

        generated = service.run_automation(until_human=True, max_jobs=2)

        assert generated["stopped"] == "human_judgment"
        patch = service.store.list_events(event_type=EventType.WORKER_PATCH_PROPOSED)[-1]
        service.approve_patch(patch.id)
        applied = service.run_automation(max_jobs=1)
        assert applied["processed"][0]["status"] == "completed"
        state = service.patch_status(patch.id)["state"]
        staging = Path(str(state["staging_path"])).resolve()
        terminal = service.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)[-1]
        archive = Path(str(terminal.payload["archive_path"])).resolve()
        assert adapter.repository_workspace_root is not None
        worker_root = adapter.repository_workspace_root.resolve()

        assert (staging / "target.txt").read_text(encoding="utf-8") == "after\n"
        assert (staging / ".git").is_dir()
        assert _git(staging, "remote") == b""
        assert not (staging / ".git" / "objects" / "info" / "alternates").exists()
        assert (repository / "target.txt").read_text(encoding="utf-8") == "before\n"
        assert not any(
            path.is_relative_to(repository)
            for path in (service.home.resolve(), archive, worker_root, staging)
        )
        assert not (repository / ".oracle_lab").exists()
        assert _source_fingerprint(repository) == before
    finally:
        service.close()


def test_default_rejects_explicit_home_inside_current_worktree_before_creation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = _repository(tmp_path / "source")
    unsafe_home = repository / ".oracle_lab"
    monkeypatch.chdir(repository)
    monkeypatch.setenv("ORACLE_LAB_HOME", str(unsafe_home))
    monkeypatch.delenv("ORACLE_LAB_DB", raising=False)

    with pytest.raises(ServiceError, match="HOME must be outside"):
        OracleLabService.default()

    assert not unsafe_home.exists()


def test_repository_enqueue_rejects_archive_inside_target_worktree(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = _repository(tmp_path / "source")
    executable = tmp_path / "fake-codex"
    executable.write_text(
        '#!/bin/sh\nif [ "${1:-}" = "--version" ]; then exit 0; fi\ntouch worker-was-started\n',
        encoding="utf-8",
    )
    executable.chmod(0o700)
    unsafe_archive = repository / "nested" / "worker-archive"
    monkeypatch.chdir(repository)
    monkeypatch.delenv("ORACLE_LAB_HOME", raising=False)
    monkeypatch.delenv("ORACLE_LAB_DB", raising=False)
    monkeypatch.delenv("ORACLE_LAB_STAGING", raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("ORACLE_LAB_ARCHIVE", str(unsafe_archive))
    monkeypatch.setenv("ORACLE_LAB_CONFIG", str(CONFIG))
    service = OracleLabService.default()
    try:
        _inject_fake_codex(
            service,
            executable,
            workspace_root=service.home / "injected-test-worker",
        )
        session = service.new_session("unsafe archive")

        with pytest.raises(ServiceError, match="archive root must be outside"):
            service.enqueue_repository_edit(
                str(session["root_event_id"]),
                "Change the file.",
                repository=repository,
            )

        assert not unsafe_archive.exists()
        assert not service.store.list_events(event_type=EventType.WORKER_TASK_REQUESTED)
    finally:
        service.close()


def test_repository_enqueue_rejects_staging_below_different_current_worktree(
    tmp_path: Path,
    monkeypatch,
) -> None:
    current_repository = _repository(tmp_path / "current")
    target_repository = _repository(tmp_path / "target")
    executable = tmp_path / "fake-codex"
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o700)
    unsafe_staging = current_repository / "nested" / "staging"
    monkeypatch.chdir(current_repository)
    monkeypatch.delenv("ORACLE_LAB_HOME", raising=False)
    monkeypatch.delenv("ORACLE_LAB_DB", raising=False)
    monkeypatch.delenv("ORACLE_LAB_ARCHIVE", raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("ORACLE_LAB_STAGING", str(unsafe_staging))
    monkeypatch.setenv("ORACLE_LAB_CONFIG", str(CONFIG))
    service = OracleLabService.default()
    try:
        _inject_fake_codex(
            service,
            executable,
            workspace_root=service.home / "injected-test-worker",
        )
        session = service.new_session("unsafe staging")

        with pytest.raises(ServiceError, match="staging root must be outside"):
            service.enqueue_repository_edit(
                str(session["root_event_id"]),
                "Change the file.",
                repository=target_repository,
            )

        assert not unsafe_staging.exists()
        assert not service.store.list_events(event_type=EventType.WORKER_TASK_REQUESTED)
    finally:
        service.close()
