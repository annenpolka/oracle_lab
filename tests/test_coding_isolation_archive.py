from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

from oracle_lab.agent_adapters import AgentRunResult, WorkerExecutionProfile, WorkerTask
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.worker_archive import WorkerArchiveRecord

CONFIG = Path(__file__).parents[1] / "config"
_STARTED_AT = dt.datetime(2026, 8, 31, 3, 0, tzinfo=dt.UTC)
_FINISHED_AT = dt.datetime(2026, 8, 31, 3, 1, tzinfo=dt.UTC)


def _write_isolated_archive(
    tmp_path: Path,
    *,
    failed: bool = False,
) -> tuple[WorkerArchiveRecord, dict, bytes]:
    credential_value = b"credential-value-must-never-be-archived"
    attestation = {
        "schema_version": 1,
        "backend": "docker-sbx-microvm",
        "broker_executable_path": "/usr/local/bin/sbx",
        "broker_executable_sha256": "1" * 64,
        "client_version": "fixture-client-1",
        "server_version": "fixture-server-1",
        "template_reference": "oracle-worker@sha256:" + "2" * 64,
        "template_identity": "sha256:" + "2" * 64,
        "policy_sha256": "3" * 64,
        "conformance_suite_version": "fixture-suite-1",
        "conformance_receipt_sha256": "4" * 64,
        "capabilities": [
            "credential_proxy_values_unavailable",
            "microvm_or_equivalent_os_boundary",
        ],
        "receipt": {
            "status": "passed",
            "checks": [
                {
                    "id": "credential_proxy_values_unavailable",
                    "status": "passed",
                }
            ],
        },
    }
    profile = WorkerExecutionProfile(
        id="isolated-codex",
        adapter="codex",
        executable="codex",
        model="gpt-fixture",
        sandbox_profile="external-broker",
        allowed_environment_names=("LANG", "OPENAI_API_KEY"),
    )
    worker = SimpleNamespace(
        name="codex",
        profile=profile,
        environment={
            "LANG": "C.UTF-8",
            "OPENAI_API_KEY": credential_value.decode(),
        },
    )
    source = Event.new(
        EventType.HUMAN_INPUT,
        actor=Actor(kind=ActorKind.HUMAN, id="operator"),
        payload={"content": "Inspect the repository."},
    )
    task = WorkerTask(source, "Capture the candidate change.")
    task_event = Event.new(
        EventType.WORKER_TASK_REQUESTED,
        actor=Actor(kind=ActorKind.HOST, id="orchestrator"),
        payload={
            "job_id": "job_isolation_archive",
            "worker_execution_profile": profile.redacted_snapshot(),
            "worker_routing": {"selected_adapter": "codex"},
        },
    )
    result = AgentRunResult(
        adapter="codex",
        command=("/guest/bin/codex", "exec"),
        exit_code=17 if failed else 0,
        stdout=("failed audit stream\x00�" if failed else "worker audit stream\n"),
        stderr="failed stderr�" if failed else "",
        elapsed_ms=1000,
        events=(),
        workspace="/guest/workspace",
        prompt=task.render(),
        stdout_bytes=(b"failed audit stream\x00\xff" if failed else b"worker audit stream\n"),
        stderr_bytes=b"failed stderr\x80" if failed else b"",
        executable_path="/guest/bin/codex",
        executable_version="codex fixture-1",
        executable_version_status="reported",
        source_worktree_unchanged=True,
        isolation_attestation=MappingProxyType(attestation),
        isolation_sandbox_id="oracle-worker-fixture-01",
        isolation_cleanup_confirmed=True,
        workspace_export_sha256=None if failed else "5" * 64,
        workspace_export_bytes=None if failed else 1234,
        workspace_export_entries=None if failed else 7,
    )
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    try:
        record = service._archive_agent_result(
            run_id="run_isolation_archive",
            task_event=task_event,
            task=task,
            worker=worker,
            result=result,
            started_at=_STARTED_AT,
            finished_at=_FINISHED_AT,
            status="failed" if failed else "completed",
        )
    finally:
        service.close()
    return record, attestation, credential_value


def test_worker_task_archive_preserves_exact_isolation_execution_capture(tmp_path: Path) -> None:
    record, attestation, _credential_value = _write_isolated_archive(tmp_path)

    task_document = json.loads(record.task.path.read_bytes())
    capture = task_document["execution_capture"]
    assert capture["isolation_attestation"] == attestation
    assert capture["isolation_sandbox_id"] == "oracle-worker-fixture-01"
    assert capture["isolation_cleanup_confirmed"] is True
    assert capture["workspace_export_sha256"] == "5" * 64
    assert capture["workspace_export_bytes"] == 1234
    assert capture["workspace_export_entries"] == 7


def test_worker_archive_records_credential_names_but_never_values(tmp_path: Path) -> None:
    record, _attestation, credential_value = _write_isolated_archive(tmp_path)

    for artifact in record.artifacts:
        assert credential_value not in artifact.path.read_bytes()
    metadata = json.loads(record.metadata.path.read_bytes())
    assert metadata["environment"] == {
        "names": ["LANG", "OPENAI_API_KEY"],
        "redaction_status": "values_omitted",
        "values_archived": False,
    }


def test_failed_isolated_worker_archive_preserves_raw_output_without_export_or_patch(
    tmp_path: Path,
) -> None:
    record, attestation, _credential_value = _write_isolated_archive(tmp_path, failed=True)

    assert record.stdout.path.read_bytes() == b"failed audit stream\x00\xff"
    assert record.stderr.path.read_bytes() == b"failed stderr\x80"
    assert record.patch.path.read_bytes() == b""
    task_document = json.loads(record.task.path.read_bytes())
    capture = task_document["execution_capture"]
    assert capture["isolation_attestation"] == attestation
    assert capture["isolation_sandbox_id"] == "oracle-worker-fixture-01"
    assert capture["isolation_cleanup_confirmed"] is True
    assert capture["workspace_export_sha256"] is None
    assert capture["workspace_export_bytes"] is None
    assert capture["workspace_export_entries"] is None
    metadata = json.loads(record.metadata.path.read_bytes())
    assert metadata["execution"] == {
        "exit_code": {"status": "known", "value": 17},
        "output_limited": {"status": "known", "value": False},
        "status": {"status": "known", "value": "failed"},
        "timed_out": {"status": "known", "value": False},
    }
