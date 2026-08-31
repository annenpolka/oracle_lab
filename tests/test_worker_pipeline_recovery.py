from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from oracle_lab.agent_adapters import (
    BaseAgentAdapter,
    DedicatedWorkspace,
    HostWorkerRouter,
    WorkerExecutionProfile,
    WorkerTask,
    parse_structured_events,
    prepare_structured_events,
)
from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.jobs import JobQueue, JobStatus
from oracle_lab.services import (
    NonRetryableWorkerError,
    OracleLabService,
    ServiceError,
)
from oracle_lab.store import EventIntegrityError, EventStore
from oracle_lab.tooling import DockerShellSandbox, ToolResult, ToolStatus
from oracle_lab.validation_archive import SandboxValidationArchive
from oracle_lab.worker_archive import WorkerRunArchive
from oracle_lab.worker_projection import WorkerProjection

CONFIG = Path(__file__).parents[1] / "config"


class _InjectedCrash(BaseException):
    """Simulate process loss without entering normal Exception recovery."""


@dataclass(slots=True)
class _WorkerHarness:
    service: OracleLabService
    repository: Path
    source_event_id: str
    adapter: BaseAgentAdapter
    run_calls: list[str]


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
    run_calls: list[str] = []
    original_run = adapter.run

    def counted_run(task: Any):
        run_calls.append(task.task_kind)
        return original_run(task)

    adapter.run = counted_run  # type: ignore[method-assign]
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        host_worker_router=HostWorkerRouter(
            codex=adapter,
            prefer_coding_agent="codex",
        ),
    )
    session = service.new_session("worker recovery fixture")
    return _WorkerHarness(
        service=service,
        repository=repository,
        source_event_id=str(session["root_event_id"]),
        adapter=adapter,
        run_calls=run_calls,
    )


def _events(service: OracleLabService, event_type: EventType):
    return service.store.list_events(event_type=event_type)


def _expire_oracle_cli_lease(service: OracleLabService, job_id: str) -> None:
    queue = service._job_queue()
    worker_id = queue.require(job_id).worker_id
    assert worker_id is not None and worker_id.startswith("oracle-cli:")
    queue.heartbeat(
        job_id,
        worker_id,
        lease_seconds=1,
        now=dt.datetime.now(dt.UTC) - dt.timedelta(seconds=2),
    )
    assert [job.id for job in queue.expired_leases()] == [job_id]


def test_worker_archive_orphan_resumes_same_lease_without_restarting_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    queue = service._job_queue()
    leased = queue.lease_one("crash-fixture", kinds=("repository_edit",))
    assert leased is not None
    assert leased.id == enqueued["job"]["id"]

    original_write = WorkerRunArchive.write
    archived_records = []

    def write_then_crash(self: WorkerRunArchive, *args: Any, **kwargs: Any):
        record = original_write(self, *args, **kwargs)
        archived_records.append(record)
        raise _InjectedCrash("after worker archive, before terminal event")

    monkeypatch.setattr(WorkerRunArchive, "write", write_then_crash)
    with pytest.raises(_InjectedCrash, match="before terminal event"):
        service._execute_host_worker_job(leased)

    assert len(archived_records) == 1
    assert archived_records[0].directory.is_dir()
    assert harness.run_calls == ["repository_edit"]
    assert len(_events(service, EventType.WORKER_RUN_STARTED)) == 1
    assert not _events(service, EventType.WORKER_RUN_COMPLETED)
    assert not _events(service, EventType.WORKER_RUN_FAILED)
    assert not _events(service, EventType.WORKER_PATCH_PROPOSED)

    monkeypatch.setattr(WorkerRunArchive, "write", original_write)
    resumed = service._execute_host_worker_job(leased)
    queue.complete(leased.id, worker_id="crash-fixture")

    assert harness.run_calls == ["repository_edit"]
    assert len(resumed) == 1
    assert resumed[0].type is EventType.WORKER_PATCH_PROPOSED
    completed = _events(service, EventType.WORKER_RUN_COMPLETED)
    proposals = _events(service, EventType.WORKER_PATCH_PROPOSED)
    assert len(completed) == 1
    assert len(proposals) == 1
    assert completed[0].payload["worker_identity"]["recovered_verified_orphan"] is True
    assert proposals[0].payload["patch_sha256"] == archived_records[0].patch.sha256
    assert (harness.repository / "target.txt").read_text(encoding="utf-8") == "before\n"


def test_worker_orphan_rejects_drifted_expected_argv_without_reinvocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    leased = service._job_queue().lease_one("argv-drift", kinds=("repository_edit",))
    assert leased is not None
    original_write = WorkerRunArchive.write

    def write_then_crash(self: WorkerRunArchive, *args: Any, **kwargs: Any):
        original_write(self, *args, **kwargs)
        raise _InjectedCrash("archive complete")

    monkeypatch.setattr(WorkerRunArchive, "write", write_then_crash)
    with pytest.raises(_InjectedCrash, match="archive complete"):
        service._execute_host_worker_job(leased)
    monkeypatch.setattr(WorkerRunArchive, "write", original_write)
    harness.adapter.command_builder = lambda _prompt: (
        harness.adapter.profile.executable,
        "--drifted-argv",
    )

    with pytest.raises(ServiceError, match="prompt, argv, or adapter identity"):
        service._execute_host_worker_job(leased)

    assert enqueued["job"]["id"] == leased.id
    assert harness.run_calls == ["repository_edit"]


def test_repository_task_freezes_profile_and_routing_and_rejects_config_drift(
    tmp_path: Path,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    task = enqueued["task_event"]["payload"]
    job = enqueued["job"]["payload"]
    expected_profile = harness.adapter.profile.redacted_snapshot()
    assert task["worker_execution_profile"] == expected_profile
    assert job["worker_execution_profile"] == expected_profile
    assert task["worker_routing"] == job["worker_routing"]
    assert task["worker_routing"]["selected_profile_id"] == expected_profile["id"]
    assert task["worker_execution_profile"]["allowed_environment_names"] == ["PATH"]
    harness.adapter.profile = replace(
        harness.adapter.profile,
        timeout_seconds=harness.adapter.profile.timeout_seconds + 1,
    )

    result = service.run_automation(max_jobs=1)

    assert result["processed"][0]["status"] == "failed"
    assert "profile or routing has drifted" in result["processed"][0]["error"]
    assert harness.run_calls == []
    assert not _events(service, EventType.WORKER_RUN_STARTED)


def test_worker_loop_detector_uses_algorithm_label_and_cited_ancestry(tmp_path: Path) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    task = service.store.require(str(enqueued["task_event"]["id"]))
    assert task.payload["automation_loop_detector"] == "sha256-equivalent-event-v1"
    descendant = service.store.append(
        Event.new(
            EventType.HUMAN_CHECKPOINT,
            actor=Actor(kind=ActorKind.HUMAN, id="loop-fixture"),
            session_id=task.session_id,
            branch_id=task.branch_id,
            parent_event_id=task.id,
            causation_id=task.id,
            correlation_id=task.correlation_id,
            payload={"operation": "loop-fixture", "source_event_ids": [task.id]},
        )
    )
    seed = {
        "task_kind": "repository_edit",
        "source_event_id": harness.source_event_id,
        "goal": task.payload["goal"],
        "repository_path": task.payload["repository_path"],
        "base_commit": task.payload["base_commit"],
        "worker_profile_id": task.payload["worker_profile_id"],
    }

    with pytest.raises(ServiceError, match="repeats an equivalent automation event"):
        service._worker_automation_fields(descendant, signature_seed=seed)


def test_conflicting_target_precondition_is_rejected_before_human_gate(tmp_path: Path) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    original_run = harness.adapter.run

    def dirty_target_after_capture(task: WorkerTask):
        result = original_run(task)
        (harness.repository / "target.txt").write_text("human-dirty\n", encoding="utf-8")
        return result

    harness.adapter.run = dirty_target_after_capture  # type: ignore[method-assign]
    service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )

    result = service.run_automation(max_jobs=1)

    assert result["processed"][0]["status"] == "completed"
    assert not _events(service, EventType.WORKER_PATCH_PROPOSED)
    rejection = _events(service, EventType.WORKER_PATCH_SECURITY_REJECTED)[0]
    assert any(
        reason.startswith("target_precondition_failed:") for reason in rejection.payload["reasons"]
    )


def test_store_rejects_patch_identity_forged_away_from_task_and_run(tmp_path: Path) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    service.run_automation(max_jobs=1)
    patch = _events(service, EventType.WORKER_PATCH_PROPOSED)[0]
    mutations: tuple[tuple[str, Any], ...] = (
        ("task_event_id", harness.source_event_id),
        ("repository_path", str(harness.repository / "different")),
        ("base_commit", "0" * 40),
        ("source_event_ids", [patch.id]),
        ("patch_size_bytes", int(patch.payload["patch_size_bytes"]) + 1),
    )

    for key, value in mutations:
        payload = thaw_json(patch.payload)
        payload[key] = value
        forged = Event.new(
            EventType.WORKER_PATCH_PROPOSED,
            actor=patch.actor,
            session_id=patch.session_id,
            branch_id=patch.branch_id,
            parent_event_id=patch.parent_event_id,
            causation_id=patch.causation_id,
            correlation_id=patch.correlation_id,
            payload=payload,
            metadata=thaw_json(patch.metadata),
        )
        with pytest.raises(EventIntegrityError):
            service.store.append(forged)


def test_expired_final_worker_lease_gets_one_same_job_archive_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path, max_retries=0)
    service = harness.service
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    job_id = str(enqueued["job"]["id"])
    original_write = WorkerRunArchive.write

    def write_then_crash(self: WorkerRunArchive, *args: Any, **kwargs: Any):
        record = original_write(self, *args, **kwargs)
        raise _InjectedCrash(f"archived {record.run_id} before queue ack")

    monkeypatch.setattr(WorkerRunArchive, "write", write_then_crash)
    with pytest.raises(_InjectedCrash, match="before queue ack"):
        service.run_automation(max_jobs=1)

    first_lease = service._job_queue().require(job_id)
    assert first_lease.status is JobStatus.LEASED
    assert first_lease.attempts == first_lease.max_attempts == 1
    assert harness.run_calls == ["repository_edit"]
    _expire_oracle_cli_lease(service, job_id)
    monkeypatch.setattr(WorkerRunArchive, "write", original_write)

    recovered = service.run_automation(max_jobs=1)

    assert recovered["processed"][0]["job_id"] == job_id
    assert recovered["processed"][0]["status"] == "completed"
    completed = service._job_queue().require(job_id)
    assert completed.status is JobStatus.COMPLETED
    assert completed.attempts == 2
    assert completed.max_attempts == 1
    assert harness.run_calls == ["repository_edit"]
    assert len(_events(service, EventType.WORKER_TASK_REQUESTED)) == 1
    assert len(_events(service, EventType.WORKER_RUN_STARTED)) == 1
    assert len(_events(service, EventType.WORKER_RUN_COMPLETED)) == 1
    assert len(_events(service, EventType.WORKER_PATCH_PROPOSED)) == 1
    recovery_leases = [
        event
        for event in _events(service, EventType.JOB_LEASED)
        if event.payload["id"] == job_id
        and event.metadata.get("lease_expiry_recovery", {}).get("ordinal") == 1
    ]
    assert len(recovery_leases) == 1
    assert recovery_leases[0].metadata["lease_expiry_recovery"]["recovery_only"] is True


def test_worker_recovery_only_lease_never_reinvokes_after_archive_disappears(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path, max_retries=0)
    service = harness.service
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    job_id = str(enqueued["job"]["id"])
    original_write = WorkerRunArchive.write
    archives = []

    def write_then_crash(self: WorkerRunArchive, *args: Any, **kwargs: Any):
        record = original_write(self, *args, **kwargs)
        archives.append(record)
        raise _InjectedCrash("archive complete before worker crash")

    monkeypatch.setattr(WorkerRunArchive, "write", write_then_crash)
    with pytest.raises(_InjectedCrash, match="worker crash"):
        service.run_automation(max_jobs=1)
    _expire_oracle_cli_lease(service, job_id)
    monkeypatch.setattr(WorkerRunArchive, "write", original_write)
    original_lease_one = JobQueue.lease_one
    removed_archive = archives[0].directory.with_name(archives[0].directory.name + "-removed")

    def lease_then_remove_archive(self: JobQueue, *args: Any, **kwargs: Any):
        leased = original_lease_one(self, *args, **kwargs)
        if leased is not None and self.is_archive_recovery_lease(leased.id):
            archives[0].directory.rename(removed_archive)
        return leased

    monkeypatch.setattr(JobQueue, "lease_one", lease_then_remove_archive)

    recovered = service.run_automation(max_jobs=1)

    assert recovered["processed"][0]["job_id"] == job_id
    assert recovered["processed"][0]["status"] == "failed"
    assert "no recoverable terminal state" in recovered["processed"][0]["error"]
    assert service._job_queue().require(job_id).status is JobStatus.DEAD_LETTER
    assert harness.run_calls == ["repository_edit"]
    assert len(_events(service, EventType.WORKER_RUN_STARTED)) == 1


def test_nonrepository_worker_events_and_terminal_are_atomic_and_repair_valid_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    source = service.store.require(harness.source_event_id)
    envelope = json.dumps(
        {
            "events": [
                {
                    "type": "analysis.session_summary_updated",
                    "payload": {"operation": "first archived proposal"},
                    "source_event_ids": [source.id],
                },
                {
                    "type": "analysis.session_summary_updated",
                    "payload": {"operation": "second archived proposal"},
                    "source_event_ids": [source.id],
                },
            ]
        },
        separators=(",", ":"),
    )
    executable = tmp_path / "fake-coding-agent"
    executable.write_text(
        "#!/bin/sh\n"
        'if [ "${1:-}" = "--version" ]; then printf "fake-agent 1.0\\n"; exit 0; fi\n'
        "cat >/dev/null\n"
        f"printf '%s\\n' '{envelope}'\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    queue = service._job_queue()
    queued = queue.enqueue(
        "detect_motifs",
        {"source_event_id": source.id, "goal": "emit two cited analysis events"},
        source_event_id=source.id,
        session_id=source.session_id,
        branch_id=source.branch_id,
        max_attempts=1,
    )
    leased = queue.lease_one("atomic-worker", kinds=("detect_motifs",))
    assert leased is not None and leased.id == queued.id
    task = WorkerTask(source, "Emit two cited analysis events.")

    original_validate = service.store._validate_worker_event

    def reject_terminal(event):
        if event.type is EventType.WORKER_RUN_COMPLETED:
            raise EventIntegrityError("injected terminal rejection")
        original_validate(event)

    monkeypatch.setattr(service.store, "_validate_worker_event", reject_terminal)
    with pytest.raises(EventIntegrityError, match="terminal rejection"):
        service._execute_coding_worker(
            job=leased,
            source=source,
            task=task,
            routed_task_type="classification",
            worker=harness.adapter,
        )

    assert not _events(service, EventType.ANALYSIS_SESSION_SUMMARY_UPDATED)
    assert not _events(service, EventType.WORKER_RUN_COMPLETED)
    assert harness.run_calls == ["analysis"]

    started = _events(service, EventType.WORKER_RUN_STARTED)[0]
    run_id = str(started.payload["run_id"])
    snapshot = WorkerRunArchive(service.archive_root / "workers").load(
        run_id=run_id,
        archived_at=started.created_at,
    )
    proposals = parse_structured_events(
        snapshot.stdout.decode("utf-8", "replace"),
        expected_source_event_id=source.id,
    )
    prepared = prepare_structured_events(
        proposals,
        source=source,
        store=service.store,
        actor_kind=started.actor.kind,
        actor_id=started.actor.id,
        worker_run_id=run_id,
    )
    service.store.append(prepared[0])
    monkeypatch.setattr(service.store, "_validate_worker_event", original_validate)

    recovered = service._execute_coding_worker(
        job=leased,
        source=source,
        task=task,
        routed_task_type="classification",
        worker=harness.adapter,
    )

    assert harness.run_calls == ["analysis"]
    assert [event.payload["operation"] for event in recovered] == [
        "first archived proposal",
        "second archived proposal",
    ]
    completed = _events(service, EventType.WORKER_RUN_COMPLETED)
    assert len(completed) == 1
    assert list(completed[0].payload["produced_event_ids"]) == [event.id for event in recovered]


def test_validation_archive_orphan_resumes_without_rerunning_docker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    generated = service.run_automation(until_human=True, max_jobs=2)
    assert generated["stopped"] == "human_judgment"
    patch = _events(service, EventType.WORKER_PATCH_PROPOSED)[0]
    approval = service.approve_patch(patch.id)
    applied = service.run_automation(max_jobs=1)
    assert applied["processed"][0]["status"] == "completed"
    application = _events(service, EventType.WORKER_PATCH_APPLIED)[0]

    queue = service._job_queue()
    leased = queue.lease_one("validation-crash-fixture", kinds=("worker.patch.validate",))
    assert leased is not None
    frozen_sandbox = thaw_json(leased.payload)["validation_sandbox"]
    service._config = replace(
        service.runtime_config,
        tools=replace(
            service.runtime_config.tools,
            sandbox=replace(
                service.runtime_config.tools.sandbox,
                image="python:config-drift",
                memory_mb=512,
            ),
        ),
    )
    docker_calls: list[str] = []

    def fake_docker_run(
        _sandbox: DockerShellSandbox,
        script: str,
        **_kwargs: Any,
    ) -> ToolResult:
        docker_calls.append(script)
        assert _sandbox.config.image == frozen_sandbox["image_requested"]
        assert _sandbox.config.memory_mb == frozen_sandbox["memory_mb"]
        return ToolResult(
            request_id="validation_fixture",
            status=ToolStatus.OK,
            output="validated\n",
            exit_code=0,
            metadata={
                "sandbox_image_requested": frozen_sandbox["image_requested"],
                "sandbox_image_actual": "sha256:" + "a" * 64,
            },
            raw_stdout=b"validated\x00\xff\n",
            raw_stderr=b"audit\x80\n",
        )

    monkeypatch.setattr(DockerShellSandbox, "run", fake_docker_run)
    original_write = SandboxValidationArchive.write
    archived_records = []

    def write_then_crash(self: SandboxValidationArchive, *args: Any, **kwargs: Any):
        record = original_write(self, *args, **kwargs)
        archived_records.append(record)
        raise _InjectedCrash("after validation archive, before validation event")

    monkeypatch.setattr(SandboxValidationArchive, "write", write_then_crash)
    with pytest.raises(_InjectedCrash, match="before validation event"):
        service._execute_patch_validation_job(leased)

    assert len(docker_calls) == 1
    assert len(archived_records) == 1
    assert archived_records[0].directory.is_dir()
    assert not _events(service, EventType.WORKER_VALIDATION_COMPLETED)
    assert not _events(service, EventType.WORKER_VALIDATION_FAILED)

    monkeypatch.setattr(SandboxValidationArchive, "write", original_write)
    resumed = service._execute_patch_validation_job(leased)
    queue.complete(leased.id, worker_id="validation-crash-fixture")

    assert len(docker_calls) == 1
    assert len(resumed) == 1
    assert resumed[0].type is EventType.WORKER_VALIDATION_COMPLETED
    validations = _events(service, EventType.WORKER_VALIDATION_COMPLETED)
    assert len(validations) == 1
    assert validations[0].payload["approval_event_id"] == approval["approval_event"]["id"]
    assert validations[0].payload["application_event_id"] == application.id
    assert validations[0].payload["status"] == "ok"
    assert validations[0].payload["error"] is None
    assert validations[0].payload["truth_domain"] == "sandbox"
    assert validations[0].payload["sandbox_config"] == frozen_sandbox
    assert validations[0].payload["sandbox_image_identity"] == {
        "requested": {
            "status": "known",
            "value": frozen_sandbox["image_requested"],
        },
        "actual": {"status": "known", "value": "sha256:" + "a" * 64},
    }
    archived = SandboxValidationArchive(service.archive_root / "validations").load(
        run_id=leased.id,
        validation_id=f"patch-{patch.id}",
        archived_at=application.created_at,
    )
    assert archived.task["sandbox_config"] == frozen_sandbox
    assert (
        archived.task["sandbox_image_identity"] == validations[0].payload["sandbox_image_identity"]
    )
    assert Path(validations[0].payload["archive_path"]) == archived_records[0].directory


def test_expired_final_validation_lease_recovers_archive_without_rerunning_docker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    service.run_automation(until_human=True, max_jobs=2)
    patch = _events(service, EventType.WORKER_PATCH_PROPOSED)[0]
    service.approve_patch(patch.id)
    service.run_automation(max_jobs=1)
    validation_job = service._job_queue().list_jobs(kind="worker.patch.validate")[0]
    assert validation_job.max_attempts == 1
    docker_calls: list[str] = []

    def fake_docker_run(
        _sandbox: DockerShellSandbox,
        script: str,
        **_kwargs: Any,
    ) -> ToolResult:
        docker_calls.append(script)
        return ToolResult(
            request_id="expired_validation_fixture",
            status=ToolStatus.OK,
            output="validated\n",
            exit_code=0,
            raw_stdout=b"validated\n",
            raw_stderr=b"",
        )

    monkeypatch.setattr(DockerShellSandbox, "run", fake_docker_run)
    original_write = SandboxValidationArchive.write

    def write_then_crash(self: SandboxValidationArchive, *args: Any, **kwargs: Any):
        record = original_write(self, *args, **kwargs)
        raise _InjectedCrash(f"archived {record.validation_id} before queue ack")

    monkeypatch.setattr(SandboxValidationArchive, "write", write_then_crash)
    with pytest.raises(_InjectedCrash, match="before queue ack"):
        service.run_automation(max_jobs=1)

    expired = service._job_queue().require(validation_job.id)
    assert expired.status is JobStatus.LEASED
    assert expired.attempts == expired.max_attempts == 1
    assert len(docker_calls) == 1
    _expire_oracle_cli_lease(service, validation_job.id)
    monkeypatch.setattr(SandboxValidationArchive, "write", original_write)

    recovered = service.run_automation(max_jobs=1)

    assert recovered["processed"][0]["job_id"] == validation_job.id
    assert recovered["processed"][0]["status"] == "completed"
    completed = service._job_queue().require(validation_job.id)
    assert completed.status is JobStatus.COMPLETED
    assert completed.attempts == 2
    assert len(docker_calls) == 1
    assert len(_events(service, EventType.WORKER_VALIDATION_COMPLETED)) == 1
    recovery_leases = [
        event
        for event in _events(service, EventType.JOB_LEASED)
        if event.payload["id"] == validation_job.id
        and event.metadata.get("lease_expiry_recovery", {}).get("ordinal") == 1
    ]
    assert len(recovery_leases) == 1


def test_repeated_worker_failure_is_non_retryable_at_configured_retry_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path, worker_exit_code=17, max_retries=1)
    service = harness.service
    queue = JobQueue(
        service.store,
        backoff_base_seconds=0,
        max_backoff_seconds=0,
    )
    monkeypatch.setattr(service, "_job_queue", lambda: queue)
    enqueued = service.enqueue_repository_edit(
        harness.source_event_id,
        "Attempt the same deterministic failing edit.",
        repository=harness.repository,
    )

    job_id = enqueued["job"]["id"]
    assert enqueued["job"]["max_attempts"] == 2
    first_lease = queue.lease_one("retry-fixture", kinds=("repository_edit",))
    assert first_lease is not None
    with pytest.raises(ServiceError) as first_failure:
        service._execute_host_worker_job(first_lease)
    assert type(first_failure.value) is ServiceError
    first_state = queue.fail(
        job_id,
        str(first_failure.value),
        worker_id="retry-fixture",
        retryable=True,
    )
    assert first_state.status is JobStatus.PENDING

    second_lease = queue.lease_one("retry-fixture", kinds=("repository_edit",))
    assert second_lease is not None
    with pytest.raises(NonRetryableWorkerError) as repeated_failure:
        service._execute_host_worker_job(second_lease)
    final_state = queue.fail(
        job_id,
        str(repeated_failure.value),
        worker_id="retry-fixture",
        retryable=False,
    )

    failures = _events(service, EventType.WORKER_RUN_FAILED)
    assert harness.run_calls == ["repository_edit", "repository_edit"]
    assert len(failures) == 2
    assert failures[0].payload["failure_signature"] == failures[1].payload["failure_signature"]
    assert failures[0].payload["repeated_equivalent_failure"] is False
    assert failures[1].payload["repeated_equivalent_failure"] is True
    assert final_state.status is JobStatus.DEAD_LETTER
    assert final_state.attempts == 2
    assert final_state.max_attempts == 2


def test_validation_archives_exact_prestart_tool_failure_and_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    assert service.run_automation(until_human=True, max_jobs=2)["stopped"] == "human_judgment"
    patch = _events(service, EventType.WORKER_PATCH_PROPOSED)[0]
    approval = service.approve_patch(patch.id)["approval_event"]
    assert service.run_automation(max_jobs=1)["processed"][0]["status"] == "completed"

    queue = service._job_queue()
    leased = queue.lease_one("validation-prestart", kinds=("worker.patch.validate",))
    assert leased is not None
    assert leased.payload["approval_event_id"] == approval["id"]

    monkeypatch.setattr(
        DockerShellSandbox,
        "run",
        lambda *_args, **_kwargs: ToolResult(
            request_id="validation_prestart",
            status=ToolStatus.ERROR,
            error="docker daemon unavailable before container start",
            raw_stdout=b"",
            raw_stderr=b"",
        ),
    )

    (failed,) = service._execute_patch_validation_job(leased)

    assert failed.type is EventType.WORKER_VALIDATION_FAILED
    assert failed.payload["approval_event_id"] == approval["id"]
    assert failed.payload["status"] == "error"
    assert failed.payload["error"] == "docker daemon unavailable before container start"
    archive = SandboxValidationArchive(service.archive_root / "validations").load(
        run_id=leased.id,
        validation_id=f"patch-{patch.id}",
        archived_at=_events(service, EventType.WORKER_PATCH_APPLIED)[0].created_at,
    )
    assert archive.task["approval_event_id"] == approval["id"]
    assert archive.metadata["execution"]["status"] == {
        "status": "known",
        "value": "error",
    }
    assert archive.metadata["execution"]["error"] == {
        "status": "known",
        "value": "docker daemon unavailable before container start",
    }
    assert archive.stdout == b""
    assert archive.stderr == b""


def test_validation_reads_frozen_tree_when_index_changes_after_tree_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    service.run_automation(until_human=True, max_jobs=2)
    patch = _events(service, EventType.WORKER_PATCH_PROPOSED)[0]
    service.approve_patch(patch.id)
    service.run_automation(max_jobs=1)
    application = _events(service, EventType.WORKER_PATCH_APPLIED)[0]
    staging = Path(str(application.payload["staging_path"]))
    leased = service._job_queue().lease_one(
        "index-swap-fixture",
        kinds=("worker.patch.validate",),
    )
    assert leased is not None

    original_require = service._require_git_bytes
    swapped = False

    def swap_index_after_write_tree(
        repository: Path,
        *arguments: str,
        input_bytes: bytes | None = None,
    ) -> bytes:
        nonlocal swapped
        result = original_require(repository, *arguments, input_bytes=input_bytes)
        if arguments == ("write-tree",) and repository == staging and not swapped:
            (staging / "target.txt").write_bytes(b"raced-index\n")
            _git(staging, "add", "target.txt")
            swapped = True
        return result

    captured_files: dict[str, bytes] = {}

    def capture_docker_input(
        _sandbox: DockerShellSandbox,
        _script: str,
        **kwargs: Any,
    ) -> ToolResult:
        captured_files.update(kwargs["files"])
        return ToolResult(
            request_id="index_swap_validation",
            status=ToolStatus.OK,
            exit_code=0,
            raw_stdout=b"validated\n",
            raw_stderr=b"",
        )

    monkeypatch.setattr(service, "_require_git_bytes", swap_index_after_write_tree)
    monkeypatch.setattr(DockerShellSandbox, "run", capture_docker_input)

    (validation,) = service._execute_patch_validation_job(leased)

    assert swapped is True
    assert validation.type is EventType.WORKER_VALIDATION_COMPLETED
    assert validation.payload["target_tree"] == application.payload["target_tree"]
    assert captured_files["target.txt"] == b"after\n"
    assert (staging / "target.txt").read_bytes() == b"raced-index\n"


def test_store_validation_terminal_matches_archive_and_is_unique_per_application(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    service.enqueue_repository_edit(
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
        repository=harness.repository,
    )
    service.run_automation(until_human=True, max_jobs=2)
    patch = _events(service, EventType.WORKER_PATCH_PROPOSED)[0]
    service.approve_patch(patch.id)
    service.run_automation(max_jobs=1)
    leased = service._job_queue().lease_one(
        "validation-integrity-fixture",
        kinds=("worker.patch.validate",),
    )
    assert leased is not None

    monkeypatch.setattr(
        DockerShellSandbox,
        "run",
        lambda *_args, **_kwargs: ToolResult(
            request_id="validation_integrity",
            status=ToolStatus.OK,
            exit_code=0,
            raw_stdout=b"validated\n",
            raw_stderr=b"",
        ),
    )
    original_append = service.store.append
    captured: list[Event] = []

    def capture_terminal(event: Event | dict[str, Any]) -> Event:
        candidate = event if isinstance(event, Event) else Event.from_dict(event)
        if candidate.type in {
            EventType.WORKER_VALIDATION_COMPLETED,
            EventType.WORKER_VALIDATION_FAILED,
        }:
            captured.append(candidate)
            raise _InjectedCrash("after validation archive, before terminal append")
        return original_append(candidate)

    monkeypatch.setattr(service.store, "append", capture_terminal)
    with pytest.raises(_InjectedCrash, match="before terminal append"):
        service._execute_patch_validation_job(leased)
    monkeypatch.setattr(service.store, "append", original_append)
    assert len(captured) == 1
    terminal = captured[0]

    mutations: tuple[tuple[str, Any], ...] = (
        ("status", "error"),
        ("error", "forged"),
        ("exit_code", 9),
        ("timed_out", True),
        ("output_limited", True),
        ("target_tree", "0" * 40),
        ("commands", ["false"]),
        ("patch_sha256", "0" * 64),
        ("base_commit", "0" * 40),
    )
    for field_name, value in mutations:
        payload = thaw_json(terminal.payload)
        payload[field_name] = value
        forged = Event.new(
            terminal.type,
            actor=terminal.actor,
            session_id=terminal.session_id,
            branch_id=terminal.branch_id,
            parent_event_id=terminal.parent_event_id,
            causation_id=terminal.causation_id,
            correlation_id=terminal.correlation_id,
            payload=payload,
            metadata=thaw_json(terminal.metadata),
        )
        with pytest.raises(EventIntegrityError):
            service.store.append(forged)

    service.store.append(terminal)
    contradictory_payload = thaw_json(terminal.payload)
    contradictory_payload.update(
        {
            "status": "error",
            "error": "contradictory terminal",
            "exit_code": 1,
        }
    )
    contradictory = Event.new(
        EventType.WORKER_VALIDATION_FAILED,
        actor=terminal.actor,
        session_id=terminal.session_id,
        branch_id=terminal.branch_id,
        parent_event_id=terminal.parent_event_id,
        causation_id=terminal.causation_id,
        correlation_id=terminal.correlation_id,
        payload=contradictory_payload,
        metadata=thaw_json(terminal.metadata),
    )
    with pytest.raises(EventIntegrityError, match="already has a validation terminal"):
        service.store.append(contradictory)
    with pytest.raises(ValueError, match="already has a validation terminal"):
        WorkerProjection().apply(service.store.connection, contradictory)

    projected = service.store.connection.execute(
        """
        SELECT validation_status, validation_event_ids_json
        FROM candidate_patches WHERE patch_event_id = ?
        """,
        (patch.id,),
    ).fetchone()
    assert projected["validation_status"] == "passed"
    assert json.loads(projected["validation_event_ids_json"]) == [terminal.id]


def test_repository_enqueue_and_replay_are_idempotent_through_patch_proposal(
    tmp_path: Path,
) -> None:
    harness = _worker_harness(tmp_path)
    service = harness.service
    arguments = (
        harness.source_event_id,
        "Change target.txt to after and do not commit.",
    )
    first = service.enqueue_repository_edit(*arguments, repository=harness.repository)
    second = service.enqueue_repository_edit(*arguments, repository=harness.repository)

    assert second["task_event"]["id"] == first["task_event"]["id"]
    assert second["job"]["id"] == first["job"]["id"]
    generated = service.run_automation(until_human=True, max_jobs=2)
    assert generated["stopped"] == "human_judgment"
    patch = _events(service, EventType.WORKER_PATCH_PROPOSED)[0]

    third = service.enqueue_repository_edit(*arguments, repository=harness.repository)
    fourth = service.enqueue_repository_edit(*arguments, repository=harness.repository)
    assert {item["task_event"]["id"] for item in (first, second, third, fourth)} == {
        first["task_event"]["id"]
    }
    assert {item["job"]["id"] for item in (first, second, third, fourth)} == {first["job"]["id"]}

    completed_job = service._job_queue().require(first["job"]["id"])
    replayed_once = service._execute_host_worker_job(completed_job)
    replayed_twice = service._execute_host_worker_job(completed_job)

    assert [event.id for event in replayed_once] == [patch.id]
    assert [event.id for event in replayed_twice] == [patch.id]
    assert harness.run_calls == ["repository_edit"]
    assert len(_events(service, EventType.WORKER_TASK_REQUESTED)) == 1
    assert len(_events(service, EventType.JOB_ENQUEUED)) == 1
    assert len(_events(service, EventType.WORKER_RUN_STARTED)) == 1
    assert len(_events(service, EventType.WORKER_RUN_COMPLETED)) == 1
    assert len(_events(service, EventType.WORKER_PATCH_PROPOSED)) == 1


def test_concurrent_repository_enqueue_atomically_reuses_one_task_and_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _worker_harness(tmp_path)
    first_service = harness.service
    second_service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        host_worker_router=HostWorkerRouter(
            codex=harness.adapter,
            prefer_coding_agent="codex",
        ),
    )
    original_list_events = EventStore.list_events
    stale_read_barrier = threading.Barrier(2)

    def synchronize_nontransactional_task_lookup(store: EventStore, *args: Any, **kwargs: Any):
        events = original_list_events(store, *args, **kwargs)
        if (
            kwargs.get("event_type") is EventType.WORKER_TASK_REQUESTED
            and not store.connection.in_transaction
        ):
            stale_read_barrier.wait(timeout=5)
        return events

    monkeypatch.setattr(EventStore, "list_events", synchronize_nontransactional_task_lookup)
    start = threading.Barrier(2)

    def enqueue(service: OracleLabService) -> dict[str, Any]:
        start.wait(timeout=5)
        return service.enqueue_repository_edit(
            harness.source_event_id,
            "Change target.txt to after and do not commit.",
            repository=harness.repository,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(enqueue, service) for service in (first_service, second_service)]
        results = [future.result(timeout=15) for future in futures]
    monkeypatch.setattr(EventStore, "list_events", original_list_events)

    task_ids = {str(result["task_event"]["id"]) for result in results}
    job_ids = {str(result["job"]["id"]) for result in results}
    assert len(task_ids) == 1
    assert len(job_ids) == 1
    assert all(
        result["task_event"]["payload"]["job_id"] == result["job"]["id"] for result in results
    )
    assert len(_events(first_service, EventType.WORKER_TASK_REQUESTED)) == 1
    assert len(_events(first_service, EventType.JOB_ENQUEUED)) == 1
    assert len(first_service._job_queue().list_jobs(kind="repository_edit")) == 1
