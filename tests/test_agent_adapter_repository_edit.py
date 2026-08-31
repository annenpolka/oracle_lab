from __future__ import annotations

import os
import stat
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from oracle_lab.agent_adapters import (
    AgentAdapterError,
    BaseAgentAdapter,
    DedicatedWorkspace,
    StructuredWorkerEvent,
    WorkerTask,
    build_worker_router,
    ingest_structured_events,
)
from oracle_lab.branching import BranchService
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.git_control import fingerprint_git_control
from oracle_lab.store import EventStore


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
    _git(repository, "config", "user.name", "Oracle Lab Test")
    _git(repository, "config", "user.email", "oracle-lab@example.invalid")
    (repository / "tracked.txt").write_text("base\n", encoding="utf-8")
    _git(repository, "add", "tracked.txt")
    _git(repository, "commit", "-qm", "base")
    return repository


def _source_event() -> Event:
    return Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="test"),
        session_id="ses_worker",
        branch_id="br_worker",
        payload={"content": "Implement only the requested change."},
    )


def _worker(path: Path, body: str) -> Path:
    path.write_text(
        "#!/bin/sh\n"
        'if [ "${1-}" = "--version" ]; then\n'
        "  printf 'fixture-worker 1.0\\n'\n"
        "  exit 0\n"
        "fi\n"
        f"{body}\n",
        encoding="utf-8",
    )
    path.chmod(0o700)
    return path


def _adapter(
    executable: Path,
    root: Path,
    *,
    timeout_seconds: float = 2,
    max_output_bytes: int = 1024 * 1024,
    arguments: tuple[str, ...] = (),
) -> BaseAgentAdapter:
    return BaseAgentAdapter(
        executable=str(executable),
        command_builder=lambda _prompt: (str(executable), *arguments),
        workspace_factory=DedicatedWorkspace(root / "analysis"),
        repository_workspace_root=root / "repository-worktrees",
        timeout_seconds=timeout_seconds,
        max_output_bytes=max_output_bytes,
        environment={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
    )


def _task(repository: Path) -> WorkerTask:
    return WorkerTask(
        _source_event(),
        "Create the requested candidate patch only.",
        task_kind="repository_edit",
        repository=str(repository),
    )


def _source_files(repository: Path) -> dict[str, tuple[int, bytes]]:
    snapshot: dict[str, tuple[int, bytes]] = {}
    for path in sorted(repository.rglob("*")):
        relative = path.relative_to(repository)
        if relative.parts and relative.parts[0] == ".git":
            continue
        if path.is_symlink():
            snapshot[str(relative)] = (
                stat.S_IMODE(path.lstat().st_mode),
                os.fsencode(os.readlink(path)),
            )
        elif path.is_file():
            snapshot[str(relative)] = (
                stat.S_IMODE(path.stat().st_mode),
                path.read_bytes(),
            )
    return snapshot


def _source_git_attack_state(repository: Path) -> str:
    """Hash source config, hooks, refs, objects, and other executable Git state."""

    return fingerprint_git_control(repository / ".git")


def _profile(**overrides):
    values = {
        "id": "codex",
        "enabled": True,
        "adapter": "codex",
        "executable": "codex",
        "model": None,
        "timeout_seconds": 30,
        "max_output_bytes": 4096,
        "sandbox_profile": "workspace-write",
        "allowed_environment_names": ("PATH",),
        "fallback_adapter": None,
        "max_retries": 0,
        "validation_commands": (),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_repository_edit_prompt_uses_filesystem_patch_only(tmp_path: Path) -> None:
    source = _source_event()
    repository_prompt = WorkerTask(
        source,
        "Edit one file.",
        task_kind="repository_edit",
        repository=str(tmp_path),
    ).render()
    analysis_prompt = WorkerTask(source, "Classify only.").render()

    assert "only candidate artifact is the filesystem patch" in repository_prompt
    assert "stdout and stderr are audit streams only" in repository_prompt
    assert '"events"' not in repository_prompt
    assert '"events"' in analysis_prompt


def test_repository_edit_captures_patch_without_touching_dirty_source(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    (repository / "tracked.txt").write_text("dirty source stays here\n", encoding="utf-8")
    (repository / "source-only.bin").write_bytes(b"\x00source\xff")
    before = _source_files(repository)
    before_status = _git(repository, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    before_head = _git(repository, "rev-parse", "HEAD").strip()
    executable = _worker(
        tmp_path / "patch-worker",
        r"""
cat >/dev/null
printf 'new untracked file\n' > untracked.txt
printf '\000\001\002\377binary\000' > blob.bin
printf '#!/bin/sh\necho candidate\n' > candidate.sh
chmod 755 candidate.sh
printf 'repository stdout is audit-only\n'
""",
    )
    adapter = _adapter(executable, tmp_path / "worker-root")

    result = adapter.run(_task(repository))

    assert result.succeeded
    assert result.events == ()
    assert result.executable_version == "fixture-worker 1.0"
    assert result.base_commit == before_head.decode()
    assert result.workspace_head == result.base_commit
    assert result.source_head_before == result.source_head_after == result.base_commit
    assert result.source_status_before_sha256 == result.source_status_after_sha256
    assert result.source_index_before_sha256 == result.source_index_after_sha256
    assert result.source_snapshot_before_sha256 == result.source_snapshot_after_sha256
    assert result.source_worktree_unchanged is True
    assert _source_files(repository) == before
    assert (
        _git(repository, "status", "--porcelain=v1", "-z", "--untracked-files=all") == before_status
    )
    assert _git(repository, "rev-parse", "HEAD").strip() == before_head
    assert set(result.changed_paths) == {"blob.bin", "candidate.sh", "untracked.txt"}
    assert result.precondition_sha256 == {
        "blob.bin": None,
        "candidate.sh": None,
        "untracked.txt": None,
    }
    assert result.changed_modes["candidate.sh"] == "100755"
    assert b"GIT binary patch" in result.patch_bytes
    assert b"new file mode 100755" in result.patch_bytes
    assert not Path(result.workspace).exists()
    assert (tmp_path / "worker-root/repository-worktrees").is_dir()


def test_repository_edit_detects_source_index_only_mutation(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    executable = _worker(
        tmp_path / "index-mutating-worker",
        r"""
cat >/dev/null
git -C "$1" update-index --assume-unchanged tracked.txt
""",
    )
    adapter = _adapter(
        executable,
        tmp_path / "worker-root",
        arguments=(str(repository),),
    )

    result = adapter.run(_task(repository))

    assert result.source_status_before_sha256 == result.source_status_after_sha256
    assert result.source_snapshot_before_sha256 == result.source_snapshot_after_sha256
    assert result.source_index_before_sha256 != result.source_index_after_sha256
    assert result.source_worktree_unchanged is False
    assert result.succeeded is False


def test_repository_edit_detects_empty_directory_added_to_source(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    executable = _worker(
        tmp_path / "directory-mutating-worker",
        r"""
cat >/dev/null
mkdir "$1/worker-created-empty-directory"
""",
    )
    adapter = _adapter(
        executable,
        tmp_path / "worker-root",
        arguments=(str(repository),),
    )

    result = adapter.run(_task(repository))

    assert result.source_status_before_sha256 == result.source_status_after_sha256
    assert result.source_index_before_sha256 == result.source_index_after_sha256
    assert result.source_snapshot_before_sha256 != result.source_snapshot_after_sha256
    assert result.source_worktree_unchanged is False
    assert result.succeeded is False


def test_repository_edit_detects_worker_commit_and_never_ingests_stdout(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    executable = _worker(
        tmp_path / "commit-worker",
        r"""
cat >/dev/null
printf 'committed candidate\n' > committed.txt
git add committed.txt
git -c user.name=Worker -c user.email=worker@example.invalid \
  -c commit.gpgsign=false commit -qm worker-commit
printf '{"events":[{"type":"analysis.session_summary_updated","payload":{},"source_event_ids":[]}]}'
""",
    )

    result = _adapter(executable, tmp_path / "worker-root").run(_task(repository))

    assert result.worker_committed is True
    assert result.workspace_head != result.base_commit
    assert result.succeeded is False
    assert result.events == ()
    assert b"committed.txt" in result.patch_bytes
    assert result.source_worktree_unchanged is True


def test_repository_edit_never_runs_source_post_checkout_hook_or_clean_filter(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    (repository / ".gitattributes").write_text(
        "*.payload filter=attack diff=attack\n",
        encoding="utf-8",
    )
    (repository / "tracked.payload").write_text("base payload\n", encoding="utf-8")
    _git(repository, "add", ".gitattributes", "tracked.payload")
    _git(repository, "commit", "-qm", "add attack fixture")

    hook_marker = tmp_path / "source-hook-ran"
    filter_marker = tmp_path / "source-filter-ran"
    hook = repository / ".git" / "hooks" / "post-checkout"
    hook.write_text(
        f"#!/bin/sh\nprintf ran > {hook_marker}\n",
        encoding="utf-8",
    )
    hook.chmod(0o700)
    filter_program = tmp_path / "source-clean-filter"
    filter_program.write_text(
        f"#!/bin/sh\nprintf ran > {filter_marker}\ncat\n",
        encoding="utf-8",
    )
    filter_program.chmod(0o700)
    _git(repository, "config", "filter.attack.clean", str(filter_program))
    _git(repository, "config", "filter.attack.required", "true")
    _git(repository, "config", "diff.external", str(filter_program))
    _git(repository, "config", "diff.attack.textconv", str(filter_program))
    git_before = _source_git_attack_state(repository)

    executable = _worker(
        tmp_path / "safe-patch-worker",
        """
cat >/dev/null
printf 'changed payload\n' > tracked.payload
""",
    )
    result = _adapter(executable, tmp_path / "worker-root").run(_task(repository))

    assert result.succeeded
    assert b"changed payload" in result.patch_bytes
    assert not hook_marker.exists()
    assert not filter_marker.exists()
    assert _source_git_attack_state(repository) == git_before
    assert result.source_git_control_before_sha256 == result.source_git_control_after_sha256


def test_repository_edit_host_git_scrubs_inherited_control_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _repository(tmp_path)
    (repository / ".gitattributes").write_text(
        "*.payload filter=attack diff=attack\n",
        encoding="utf-8",
    )
    (repository / "tracked.payload").write_text("base payload\n", encoding="utf-8")
    _git(repository, "add", ".gitattributes", "tracked.payload")
    _git(repository, "commit", "-qm", "environment attack fixture")
    marker = tmp_path / "inherited-git-extension-ran"
    hooks = tmp_path / "global-hooks"
    hooks.mkdir()
    post_checkout = hooks / "post-checkout"
    post_checkout.write_text(
        f"#!/bin/sh\nprintf ran > {marker}\n",
        encoding="utf-8",
    )
    post_checkout.chmod(0o700)
    clean_filter = tmp_path / "global-clean-filter"
    clean_filter.write_text(
        f"#!/bin/sh\nprintf ran > {marker}\ncat\n",
        encoding="utf-8",
    )
    clean_filter.chmod(0o700)
    hostile_global = tmp_path / "hostile-gitconfig"
    hostile_global.write_text(
        "\n".join(
            (
                "[core]",
                f"\thooksPath = {hooks}",
                '[filter "attack"]',
                f"\tclean = {clean_filter}",
                "\trequired = true",
                "[diff]",
                f"\texternal = {clean_filter}",
                '[diff "attack"]',
                f"\ttextconv = {clean_filter}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(hostile_global))
    monkeypatch.setenv("GIT_DIR", str(repository / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(repository))
    monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / "hostile-index"))

    executable = _worker(
        tmp_path / "environment-safe-worker",
        """
cat >/dev/null
printf 'changed payload\n' > tracked.payload
""",
    )
    result = _adapter(executable, tmp_path / "worker-root").run(_task(repository))

    assert result.succeeded
    assert b"changed payload" in result.patch_bytes
    assert not marker.exists()


def test_repository_edit_rejects_worker_clone_config_and_hook_tampering(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    marker = tmp_path / "worker-git-extension-ran"
    git_before = _source_git_attack_state(repository)
    executable = _worker(
        tmp_path / "git-config-worker",
        r"""
cat >/dev/null
printf 'candidate still captured\n' > candidate.txt
printf '*.txt filter=attack\n' > .gitattributes
printf '\n[filter "attack"]\n\tclean = sh -c "printf ran > $1" - '"$1"'\n' >> .git/config
mkdir -p .git/hooks
printf '#!/bin/sh\nprintf ran > %s\n' "$1" > .git/hooks/post-checkout
chmod 700 .git/hooks/post-checkout
""",
    )

    result = _adapter(
        executable,
        tmp_path / "worker-root",
        arguments=(str(marker),),
    ).run(_task(repository))

    assert result.worker_git_control_tampered is True
    assert result.worker_committed is False
    assert result.succeeded is False
    assert b"candidate.txt" in result.patch_bytes
    assert not marker.exists()
    assert _source_git_attack_state(repository) == git_before


def test_repository_edit_rejects_worker_clone_refs_and_objects_tampering(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    git_before = _source_git_attack_state(repository)
    executable = _worker(
        tmp_path / "git-ref-worker",
        r"""
cat >/dev/null
printf 'ordinary edit\n' > ordinary.txt
git update-ref refs/heads/worker-controlled HEAD
printf 'worker-only-object\n' | git hash-object -w --stdin >/dev/null
""",
    )

    result = _adapter(executable, tmp_path / "worker-root").run(_task(repository))

    assert result.worker_git_control_tampered is True
    assert result.succeeded is False
    assert b"ordinary.txt" in result.patch_bytes
    assert _source_git_attack_state(repository) == git_before
    assert not (repository / ".git" / "refs" / "heads" / "worker-controlled").exists()


def test_repository_edit_nonzero_exit_preserves_exact_stream_bytes_and_patch(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    executable = _worker(
        tmp_path / "failing-worker",
        r"""
cat >/dev/null
printf 'failed candidate\n' > failed.txt
printf 'out\377\000'
printf 'err\200\n' >&2
exit 7
""",
    )

    result = _adapter(executable, tmp_path / "worker-root").run(_task(repository))

    assert result.exit_code == 7
    assert result.succeeded is False
    assert result.stdout_bytes == b"out\xff\x00"
    assert result.stderr_bytes == b"err\x80\n"
    assert b"failed.txt" in result.patch_bytes
    assert result.source_worktree_unchanged is True


def test_repository_edit_timeout_kills_worker_process_group(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    marker = tmp_path / "timeout-child-survived"
    child_started = tmp_path / "timeout-child-started"
    executable = _worker(
        tmp_path / "timeout-worker",
        r"""
marker=$1
printf 'started' > "$2"
( sleep 1.3; printf 'survived' > "$marker" ) &
printf 'partial-timeout'
sleep 10
""",
    )
    adapter = _adapter(
        executable,
        tmp_path / "worker-root",
        timeout_seconds=0.8,
        arguments=(str(marker), str(child_started)),
    )

    result = adapter.run(_task(repository))
    time.sleep(1.5)

    assert result.timed_out is True
    assert result.output_limited is False
    assert result.exit_code is None
    assert b"partial-timeout".startswith(result.stdout_bytes)
    assert child_started.read_bytes() == b"started"
    assert not marker.exists()
    assert result.source_worktree_unchanged is True
    assert not Path(result.workspace).exists()


def test_repository_edit_output_limit_is_enforced_while_running_and_kills_children(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    marker = tmp_path / "overflow-child-survived"
    child_started = tmp_path / "overflow-child-started"
    executable = _worker(
        tmp_path / "overflow-worker",
        r"""
marker=$1
printf 'started' > "$2"
( sleep 0.35; printf 'survived' > "$marker" ) &
dd if=/dev/zero bs=4096 count=1024 2>/dev/null
sleep 10
""",
    )
    adapter = _adapter(
        executable,
        tmp_path / "worker-root",
        timeout_seconds=5,
        max_output_bytes=257,
        arguments=(str(marker), str(child_started)),
    )

    started = time.monotonic()
    result = adapter.run(_task(repository))
    elapsed = time.monotonic() - started
    time.sleep(0.5)

    assert result.output_limited is True
    assert result.timed_out is False
    assert len(result.stdout_bytes) + len(result.stderr_bytes) == 257
    assert result.stdout_bytes == b"\x00" * 257
    assert elapsed < 5
    assert child_started.read_bytes() == b"started"
    assert not marker.exists()
    assert result.source_worktree_unchanged is True
    assert not Path(result.workspace).exists()


def test_production_router_rejects_unisolated_codex_before_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = SimpleNamespace(
        enabled=True,
        prefer_coding_agent="codex",
        workers={"codex": _profile()},
    )

    def forbidden_popen(*_args: object, **_kwargs: object) -> subprocess.Popen[bytes]:
        raise AssertionError("coding-worker subprocess started without OS isolation")

    monkeypatch.setattr(subprocess, "Popen", forbidden_popen)

    with pytest.raises(AgentAdapterError, match="OS-level coding-worker isolation"):
        build_worker_router(config, workspace_root=tmp_path / "workers")


def test_router_rejects_multiple_enabled_profiles_for_one_adapter(tmp_path: Path) -> None:
    config = SimpleNamespace(
        enabled=True,
        prefer_coding_agent="codex",
        workers={
            "codex-primary": _profile(id="codex-primary"),
            "codex-secondary": _profile(id="codex-secondary"),
        },
    )

    with pytest.raises(AgentAdapterError, match="multiple enabled worker profiles"):
        build_worker_router(config, workspace_root=tmp_path / "workers")


def test_opencode_requires_real_absolute_external_wrapper(tmp_path: Path) -> None:
    direct = _profile(
        id="opencode",
        adapter="opencode",
        executable="opencode",
        sandbox_profile="external-sandbox-wrapper",
    )
    config = SimpleNamespace(
        enabled=True,
        prefer_coding_agent="opencode",
        workers={"opencode": direct},
    )
    with pytest.raises(AgentAdapterError, match="absolute path"):
        build_worker_router(config, workspace_root=tmp_path / "workers")

    started = tmp_path / "opencode-wrapper-started"
    wrapper = _worker(
        tmp_path / "sandboxed-opencode-wrapper",
        f"touch {started}\nexit 0",
    )
    wrapped = _profile(
        id="opencode",
        adapter="opencode",
        executable=str(wrapper),
        sandbox_profile="external-sandbox-wrapper",
    )
    config.workers = {"opencode": wrapped}
    with pytest.raises(AgentAdapterError, match="wrapper paths are not isolation attestations"):
        build_worker_router(config, workspace_root=tmp_path / "workers")
    assert not started.exists()

    symlink = tmp_path / "wrapper-symlink"
    symlink.symlink_to(wrapper)
    config.workers = {
        "opencode": _profile(
            id="opencode",
            adapter="opencode",
            executable=str(symlink),
            sandbox_profile="external-sandbox-wrapper",
        )
    }
    with pytest.raises(AgentAdapterError, match="not a symlink"):
        build_worker_router(config, workspace_root=tmp_path / "workers")


def test_worker_citations_reject_sibling_and_future_events() -> None:
    store = EventStore()
    branches = BranchService(store)
    session = branches.create_session(title="citation visibility")
    assert session.current_branch_id is not None
    source = branches.checkpoint(session.current_branch_id, title="assigned source")
    future = branches.checkpoint(session.current_branch_id, title="future event")
    sibling_branch = branches.fork(source.id, title="sibling")
    sibling = branches.checkpoint(sibling_branch.id, title="sibling event")

    for invisible in (future, sibling):
        proposal = StructuredWorkerEvent(
            EventType.ANALYSIS_SESSION_SUMMARY_UPDATED,
            {"operation": "forged citation"},
            (source.id, invisible.id),
        )
        with pytest.raises(AgentAdapterError, match="not visible"):
            ingest_structured_events(
                (proposal,),
                source=source,
                store=store,
                actor_kind=ActorKind.WORKER,
                actor_id="fixture-worker",
            )

    assert store.list_events(event_type=EventType.ANALYSIS_SESSION_SUMMARY_UPDATED) == []
