from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from oracle_lab.agent_adapters import (
    BaseAgentAdapter,
    DedicatedWorkspace,
    HostWorkerRouter,
    WorkerExecutionProfile,
)
from oracle_lab.events import ActorKind, Event, EventType
from oracle_lab.jobs import JobQueue, JobQueueError, JobStatus
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"


@dataclass(slots=True)
class _WorkerHarness:
    service: OracleLabService
    repository: Path
    source_event_id: str


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _worker_harness(
    tmp_path: Path,
    *,
    worker_exit_code: int = 0,
    max_retries: int = 0,
    validation_commands: tuple[str, ...] = ("python -c \"print('validated')\"",),
) -> _WorkerHarness:
    repository = tmp_path / "source-repository"
    repository.mkdir()
    _git(repository, "init", "-q")
    _git(repository, "config", "user.email", "tests@example.invalid")
    _git(repository, "config", "user.name", "Oracle Lab Tests")
    (repository / "target.txt").write_text("before\n", encoding="utf-8")
    _git(repository, "add", "target.txt")
    _git(repository, "commit", "-qm", "base")

    executable = tmp_path / "fake-coding-agent"
    executable.write_text(
        "#!/bin/sh\n"
        'if [ "${1:-}" = "--version" ]; then\n'
        "  printf 'fake-agent 1.0\\n'\n"
        "  exit 0\n"
        "fi\n"
        "cat >/dev/null\n"
        "printf 'stable worker stderr\\n' >&2\n"
        + (
            f"exit {worker_exit_code}\n"
            if worker_exit_code
            else "printf 'after\\n' > target.txt\nprintf 'worker audit stream\\n'\n"
        ),
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
        max_retries=max_retries,
        validation_commands=validation_commands,
    )
    adapter = BaseAgentAdapter(
        executable=executable.name,
        command_builder=lambda _prompt: (executable.name,),
        workspace_factory=DedicatedWorkspace(tmp_path / "worker-workspaces"),
        environment={"PATH": f"{tmp_path}:{os.environ.get('PATH', '')}"},
        profile=profile,
        repository_workspace_root=tmp_path / "repository-workspaces",
    )
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        host_worker_router=HostWorkerRouter(
            codex=adapter,
            prefer_coding_agent="codex",
        ),
    )
    session = service.new_session("worker transaction fixture")
    return _WorkerHarness(
        service=service,
        repository=repository,
        source_event_id=str(session["root_event_id"]),
    )


def _events(service: OracleLabService, event_type: EventType) -> list[Event]:
    return service.store.list_events(event_type=event_type)


def _jobs(service: OracleLabService, kind: str):
    return [job for job in service._job_queue().list_jobs() if job.kind == kind]


def _candidate_patch(harness: _WorkerHarness) -> Event:
    harness.service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    result = harness.service.run_automation(until_human=True, max_jobs=2)
    assert result["stopped"] == "human_judgment"
    patches = _events(harness.service, EventType.WORKER_PATCH_PROPOSED)
    assert len(patches) == 1
    return patches[0]


def test_patch_approval_and_apply_enqueue_are_one_retryable_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    patch = _candidate_patch(harness)
    original_enqueue = JobQueue.enqueue

    def fail_apply_enqueue(self: JobQueue, kind: str, *args: Any, **kwargs: Any):
        if kind == "worker.patch.apply":
            raise JobQueueError("injected apply enqueue failure")
        return original_enqueue(self, kind, *args, **kwargs)

    with monkeypatch.context() as injected:
        injected.setattr(JobQueue, "enqueue", fail_apply_enqueue)
        with pytest.raises(JobQueueError, match="injected apply enqueue failure"):
            service.approve_patch(patch.id)

    # The Human decision and its durable work item are one control-plane fact.
    # A failed enqueue must not strand an approval that can no longer be retried.
    assert _events(service, EventType.HUMAN_PATCH_APPROVED) == []
    assert _jobs(service, "worker.patch.apply") == []
    state = service.store.connection.execute(
        "SELECT status FROM candidate_patches WHERE patch_event_id = ?", (patch.id,)
    ).fetchone()
    assert state is not None
    assert state["status"] == "pending_human"

    retried = service.approve_patch(patch.id)
    approvals = _events(service, EventType.HUMAN_PATCH_APPROVED)
    apply_jobs = _jobs(service, "worker.patch.apply")
    assert len(approvals) == 1
    assert len(apply_jobs) == 1
    assert retried["approval_event"]["id"] == approvals[0].id
    assert retried["job"]["id"] == apply_jobs[0].id
    assert apply_jobs[0].source_event_id == approvals[0].id


def test_applied_event_and_validation_enqueue_are_atomic_and_reconcilable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    patch = _candidate_patch(harness)
    approval = service.approve_patch(patch.id)
    apply_job = service._job_queue().require(approval["job"]["id"])
    original_enqueue = JobQueue.enqueue

    def fail_validation_enqueue(self: JobQueue, kind: str, *args: Any, **kwargs: Any):
        if kind == "worker.patch.validate":
            raise JobQueueError("injected validation enqueue failure")
        return original_enqueue(self, kind, *args, **kwargs)

    with monkeypatch.context() as injected:
        injected.setattr(JobQueue, "enqueue", fail_validation_enqueue)
        with pytest.raises(JobQueueError, match="injected validation enqueue failure"):
            service._execute_patch_application_job(apply_job)

    # The filesystem patch may already exist in staging, but the authoritative
    # event must not become visible without its required validation work item.
    assert _events(service, EventType.WORKER_PATCH_APPLIED) == []
    assert _jobs(service, "worker.patch.validate") == []

    resumed = service._execute_patch_application_job(apply_job)
    applications = _events(service, EventType.WORKER_PATCH_APPLIED)
    validation_jobs = _jobs(service, "worker.patch.validate")
    assert len(resumed) == 1
    assert len(applications) == 1
    assert len(validation_jobs) == 1
    assert resumed[0].id == applications[0].id
    assert validation_jobs[0].source_event_id == applications[0].id
    assert validation_jobs[0].payload["application_event_id"] == applications[0].id
    assert validation_jobs[0].payload["patch_event_id"] == patch.id

    # Further reconciliation is idempotent: no duplicate application or job.
    replayed = service._execute_patch_application_job(apply_job)
    assert [event.id for event in replayed] == [applications[0].id]
    assert len(_events(service, EventType.WORKER_PATCH_APPLIED)) == 1
    assert len(_jobs(service, "worker.patch.validate")) == 1


def test_explicit_dead_letter_retry_has_new_human_task_job_and_run_identity(
    tmp_path: Path,
) -> None:
    harness = _worker_harness(
        tmp_path,
        worker_exit_code=17,
        max_retries=0,
        validation_commands=(),
    )
    service = harness.service
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Attempt the deterministic failing edit.",
        repository=harness.repository,
    )
    first_result = service.run_automation(max_jobs=1)
    assert first_result["processed"][0]["status"] == "failed"

    original_job = service._job_queue().require(enqueued["job"]["id"])
    assert original_job.status is JobStatus.DEAD_LETTER
    original_task = service.store.require(enqueued["task_event"]["id"])
    original_failures = _events(service, EventType.WORKER_RUN_FAILED)
    assert len(original_failures) == 1
    original_terminal = original_failures[0]

    first_retry = service.retry_jobs(original_job.id)
    second_retry = service.retry_jobs(original_job.id)
    assert len(first_retry) == 1
    assert second_retry == first_retry
    retry_job = service._job_queue().require(first_retry[0]["id"])
    assert retry_job.id != original_job.id
    assert retry_job.payload["retry_of_job_id"] == original_job.id

    tasks = _events(service, EventType.WORKER_TASK_REQUESTED)
    assert len(tasks) == 2
    retry_task = next(task for task in tasks if task.id != original_task.id)
    assert retry_task.actor.kind is ActorKind.HUMAN
    assert retry_task.parent_event_id == original_terminal.id
    assert retry_task.causation_id == original_terminal.id
    assert retry_task.correlation_id == original_task.correlation_id
    assert retry_task.payload["job_id"] == retry_job.id
    assert retry_task.payload["retry_of_job_id"] == original_job.id
    assert retry_task.payload["retry_of_task_event_id"] == original_task.id
    assert retry_task.payload["previous_terminal_event_id"] == original_terminal.id
    assert retry_job.source_event_id == retry_task.id
    assert retry_job.payload["task_event_id"] == retry_task.id

    retry_result = service.run_automation(max_jobs=1)
    assert retry_result["processed"][0]["status"] == "failed"
    failures = _events(service, EventType.WORKER_RUN_FAILED)
    assert len(failures) == 2
    retry_terminal = next(event for event in failures if event.id != original_terminal.id)
    assert retry_terminal.payload["task_event_id"] == retry_task.id
    assert retry_terminal.payload["job_id"] == retry_job.id
    assert retry_terminal.payload["run_id"] != original_terminal.payload["run_id"]

    # Repeating the explicit retry command for the same dead letter discovers
    # the existing retry instead of adding a third task or job.
    third_retry = service.retry_jobs(original_job.id)
    assert [item["id"] for item in third_retry] == [retry_job.id]
    assert third_retry[0]["status"] == JobStatus.DEAD_LETTER.value
    assert len(_events(service, EventType.WORKER_TASK_REQUESTED)) == 2
    assert len(_jobs(service, "repository_edit")) == 2
