from __future__ import annotations

import datetime as dt
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

import oracle_lab.bundle_import as bundle_import_module
import oracle_lab.cli as cli
from oracle_lab.archive import RawResponseArchive
from oracle_lab.bundle_import import BundleImportError, ResearchBundleImporter
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jobs import JobStatus
from oracle_lab.jsonutil import sha256_bytes, sha256_json
from oracle_lab.projections import VirtualStateService
from oracle_lab.providers import OracleGenerateResponse
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventStore
from oracle_lab.validation_archive import SandboxValidationArchive, ValidationRunMetadata
from oracle_lab.virtual import SourceEvidence, VirtualWorldRuntime
from oracle_lab.worker_archive import WorkerRunArchive, WorkerRunMetadata

CONFIG = Path(__file__).parents[1] / "config"
FIXTURES = Path(__file__).parent / "fixtures"


class ContinuationProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.raw = (FIXTURES / "historical_continuation_001.json").read_bytes()

    async def generate(self, request: Any) -> OracleGenerateResponse:
        del request
        self.calls += 1
        body = json.loads(self.raw)
        choice = body["choices"][0]
        return OracleGenerateResponse(
            raw_bytes=self.raw,
            status_code=200,
            headers={"x-request-id": body["id"]},
            provider_name="test-provider",
            provider_model_id=body["model"],
            content=choice["message"]["content"],
            finish_reason=choice["finish_reason"],
            usage=body["usage"],
            elapsed_ms=1.0,
            request_id=body["id"],
            parsed=body,
            material_origin="historical_fixture",
        )


class ForbiddenProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def generate(self, request: Any) -> OracleGenerateResponse:
        del request
        self.calls += 1
        raise AssertionError("bundle reconstruction must not query an oracle provider")


def _service(
    root: Path,
    *,
    provider: ContinuationProvider | ForbiddenProvider | None = None,
) -> OracleLabService:
    root.mkdir(parents=True, exist_ok=True)
    return OracleLabService(
        EventStore(root / "oracle.db"),
        home=root / "home",
        config_dir=CONFIG,
        provider_factory=None if provider is None else lambda _profile: provider,
    )


def _export_complete_chain(tmp_path: Path) -> tuple[OracleLabService, Path, str, bytes]:
    provider = ContinuationProvider()
    service = _service(tmp_path / "source", provider=provider)
    exact_prompt = "  計算し直せ。\n"
    source = tmp_path / "historical.json"
    source.write_text(
        json.dumps(
            {
                "messages": [
                    {"role": "user", "content": exact_prompt},
                    {
                        "role": "assistant",
                        "content": (
                            "TIME_DILATION_FACTOR=1.78\nThe compressed day lasts exactly 148 hours."
                        ),
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    imported = service.import_session(source, title="portable chain")
    historical_output = service.store.require(imported["assistant_event_ids"][0])
    service.replay_host_analysis(historical_output.id)
    inconsistency = service.store.list_events(event_type=EventType.ANALYSIS_NUMERIC_INCONSISTENCY)[
        0
    ]
    service.request_tool(inconsistency.id)
    service.run_automation(max_jobs=1)
    service.run_automation(max_jobs=1)
    continuation = next(
        event
        for event in service.store.list_events(event_type=EventType.ORACLE_OUTPUT)
        if event.id != historical_output.id
    )
    service.keep(continuation.id)
    bundle = tmp_path / "bundle"
    service.export("bundle", bundle, session_id=continuation.session_id)
    return service, bundle, continuation.id, provider.raw


def _rehash(bundle: Path, relative_path: str) -> None:
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["sha256"][relative_path] = sha256_bytes((bundle / relative_path).read_bytes())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def _rewrite_context_records(bundle: Path, mutation: str) -> None:
    events_path = bundle / "events.jsonl"
    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    context = next(
        record
        for record in events
        if record["type"] == "oracle.context_built" and record["payload"]["messages"]
    )
    if mutation in {"messages", "source-text"}:
        context["payload"]["messages"][0]["content"] += " forged"
    else:
        context["payload"]["source_event_ids"][0] = context["id"]
    if mutation == "source-text":
        digest = sha256_json(context["payload"]["messages"])
        context["payload"]["sha256"] = digest
        request_id = context["causation_id"]
        for event in events:
            if (
                event["id"] == request_id or event.get("causation_id") == request_id
            ) and "context_hash" in event["payload"]:
                event["payload"]["context_hash"] = digest
    session_path = bundle / "session.jsonl"
    snapshots = [json.loads(line) for line in session_path.read_text(encoding="utf-8").splitlines()]
    snapshot = next(record for record in snapshots if record["id"] == context["id"])
    snapshot["payload"] = json.loads(json.dumps(context["payload"]))
    for path, records in ((events_path, events), (session_path, snapshots)):
        path.write_text(
            "\n".join(json.dumps(record) for record in records) + "\n",
            encoding="utf-8",
        )
        _rehash(bundle, path.name)


def _archive_manifest(record: Any) -> dict[str, dict[str, Any]]:
    return {
        artifact.name: {
            "path": str(artifact.path),
            "sha256": artifact.sha256,
            "size_bytes": artifact.size_bytes,
        }
        for artifact in record.artifacts
    }


def _export_worker_validation_chain(
    tmp_path: Path,
) -> tuple[OracleLabService, Path, dict[str, str], bytes, bytes]:
    service = _service(tmp_path / "worker-source")
    session = service.new_session("portable worker chain")
    root = service.store.require(session["root_event_id"])
    task = service.store.append(
        Event.new(
            EventType.WORKER_TASK_REQUESTED,
            actor=Actor(kind=ActorKind.HOST, id="worker-orchestrator"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={
                "job_id": "job_portable",
                "task_kind": "repository_edit",
                "source_event_id": root.id,
                "source_event_ids": [root.id],
                "goal": "Change target.txt exactly.",
                "repository_path": "/source/repository",
                "base_commit": "a" * 40,
                "validation_commands": ["pytest -q"],
            },
        )
    )
    run_id = "run_portable"
    started = service.store.append(
        Event.new(
            EventType.WORKER_RUN_STARTED,
            actor=Actor(kind=ActorKind.WORKER, id="fake-codex"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=task.id,
            causation_id=task.id,
            correlation_id=root.correlation_id,
            payload={
                "run_id": run_id,
                "task_event_id": task.id,
                "adapter_id": "fake-codex",
            },
        )
    )
    patch_bytes = b"diff --git a/target.txt b/target.txt\n@@ -1 +1 @@\n-before\n+after\n"
    worker_record = WorkerRunArchive(service.archive_root / "workers").write(
        run_id=run_id,
        task={
            "task_event_id": task.id,
            "source_event_id": root.id,
            "task_kind": "repository_edit",
        },
        prompt="Change target.txt exactly.\n",
        command=("fake-codex", "exec"),
        stdout=b"worker\x00stdout\xff\n",
        stderr=b"worker\x80stderr\n",
        patch=patch_bytes,
        run_metadata=WorkerRunMetadata(
            adapter="fake-codex",
            adapter_version="1.0",
            model=None,
            base_commit="a" * 40,
            started_at=started.created_at,
            finished_at=started.created_at + dt.timedelta(seconds=1),
            status="completed",
            exit_code=0,
            timed_out=False,
            output_limited=False,
            environment_names=("PATH",),
        ),
        archived_at=started.created_at,
    )
    worker_manifest = _archive_manifest(worker_record)
    terminal = service.store.append(
        Event.new(
            EventType.WORKER_RUN_COMPLETED,
            actor=Actor(kind=ActorKind.WORKER, id="fake-codex"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=started.id,
            causation_id=task.id,
            correlation_id=root.correlation_id,
            payload={
                "run_id": run_id,
                "task_event_id": task.id,
                "job_id": "job_portable",
                "archive_path": str(worker_record.directory),
                "archive_manifest": worker_manifest,
                "produced_event_ids": [],
                "candidate_patch": {
                    "repository_path": "/source/repository",
                    "patch_archive_path": str(worker_record.patch.path),
                    "patch_sha256": worker_record.patch.sha256,
                    "patch_size_bytes": worker_record.patch.size_bytes,
                    "base_commit": "a" * 40,
                    "workspace_head": None,
                    "changed_paths": ["target.txt"],
                    "changed_modes": {},
                    "precondition_sha256": {},
                },
            },
            metadata={"schema_version": 1, "artifact_origin": "worker_generated"},
        )
    )
    patch = service.store.append(
        Event.new(
            EventType.WORKER_PATCH_PROPOSED,
            actor=Actor(kind=ActorKind.WORKER, id="fake-codex"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=terminal.id,
            causation_id=task.id,
            correlation_id=root.correlation_id,
            payload={
                "worker_run_id": run_id,
                "task_event_id": task.id,
                "repository_path": "/source/repository",
                "base_commit": "a" * 40,
                "patch_archive_path": str(worker_record.patch.path),
                "patch_sha256": worker_record.patch.sha256,
                "patch_size_bytes": worker_record.patch.size_bytes,
                "changed_paths": ["target.txt"],
                "source_event_ids": [root.id],
                "artifact_origin": "worker_generated",
            },
        )
    )
    approval = service.store.append(
        Event.new(
            EventType.HUMAN_PATCH_APPROVED,
            actor=Actor(kind=ActorKind.HUMAN, id="operator"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=patch.id,
            causation_id=patch.id,
            correlation_id=root.correlation_id,
            payload={
                "patch_event_id": patch.id,
                "patch_sha256": worker_record.patch.sha256,
                "base_commit": "a" * 40,
            },
        )
    )
    apply_job = service._job_queue().enqueue(
        "worker.patch.apply",
        {"patch_event_id": patch.id, "approval_event_id": approval.id},
        source_event_id=approval.id,
        idempotency_key=f"worker.patch.apply:{patch.id}:{approval.id}",
        session_id=patch.session_id,
        branch_id=patch.branch_id,
        serialize_branch=True,
        max_attempts=1,
    )
    application = service.store.append(
        Event.new(
            EventType.WORKER_PATCH_APPLIED,
            actor=Actor(kind=ActorKind.SYSTEM, id="patch-applier"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=approval.id,
            causation_id=patch.id,
            correlation_id=root.correlation_id,
            payload={
                "patch_event_id": patch.id,
                "approval_event_id": approval.id,
                "patch_sha256": worker_record.patch.sha256,
                "base_commit": "a" * 40,
                "staging_path": "/source/staging/portable",
                "target_tree": "b" * 40,
            },
        )
    )
    validation_record = SandboxValidationArchive(service.archive_root / "validations").write(
        run_id="validation_job",
        validation_id="patch-portable",
        task={
            "patch_event_id": patch.id,
            "approval_event_id": approval.id,
            "application_event_id": application.id,
            "patch_sha256": worker_record.patch.sha256,
            "base_commit": "a" * 40,
            "target_tree": "b" * 40,
            "staging_path": "/source/staging/portable",
            "commands": ["pytest -q"],
        },
        command=("/bin/sh", "-lc", "set -eu\n(pytest -q)"),
        stdout=b"validation\x00stdout\xff\n",
        stderr=b"validation\x80stderr\n",
        run_metadata=ValidationRunMetadata(
            started_at=application.created_at,
            finished_at=application.created_at + dt.timedelta(seconds=1),
            exit_code=0,
            timed_out=False,
            output_limited=False,
            status="ok",
            error=None,
        ),
        archived_at=application.created_at,
    )
    validation = service.store.append(
        Event.new(
            EventType.WORKER_VALIDATION_COMPLETED,
            actor=Actor(kind=ActorKind.TOOL, id="docker-validation"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=application.id,
            causation_id=patch.id,
            correlation_id=root.correlation_id,
            payload={
                "patch_event_id": patch.id,
                "approval_event_id": approval.id,
                "application_event_id": application.id,
                "patch_sha256": worker_record.patch.sha256,
                "base_commit": "a" * 40,
                "target_tree": "b" * 40,
                "commands": ["pytest -q"],
                "status": "ok",
                "error": None,
                "exit_code": 0,
                "timed_out": False,
                "output_limited": False,
                "archive_path": str(validation_record.directory),
                "archive_manifest": _archive_manifest(validation_record),
                "truth_domain": "sandbox",
                "artifact_origin": "tool_result",
            },
            metadata={
                "schema_version": 1,
                "truth_domain": "sandbox",
                "artifact_origin": "tool_result",
            },
        )
    )
    bundle = tmp_path / "worker-bundle"
    service.export("bundle", bundle, session_id=root.session_id)
    return (
        service,
        bundle,
        {
            "terminal": terminal.id,
            "patch": patch.id,
            "approval": approval.id,
            "application": application.id,
            "validation": validation.id,
            "apply_job": apply_job.id,
        },
        worker_record.stdout.path.read_bytes(),
        validation_record.stdout.path.read_bytes(),
    )


def test_bundle_round_trip_reconstructs_exact_chain_without_provider_call(
    tmp_path: Path,
) -> None:
    source, bundle, continuation_id, provider_raw = _export_complete_chain(tmp_path)
    original_events = source.store.list_events()
    original_by_id = {event.id: event for event in original_events}
    context_ids = {
        event.id for event in original_events if event.type is EventType.ORACLE_CONTEXT_BUILT
    }
    session_records = [
        json.loads(line)
        for line in (bundle / "session.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert {record["id"] for record in session_records} == context_ids
    # Portability must not depend on the old machine-local archive, nor on the
    # JSONL already being in append order.
    original_archive = Path(str(original_by_id[continuation_id].payload["archive_path"]))
    original_archive.unlink()
    original_archive.with_name(f"{original_archive.stem}.metadata.json").unlink()
    event_lines = (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    (bundle / "events.jsonl").write_text(
        "\n".join(reversed(event_lines)) + "\n",
        encoding="utf-8",
    )
    _rehash(bundle, "events.jsonl")

    forbidden = ForbiddenProvider()
    restored = _service(tmp_path / "restored", provider=forbidden)
    result = restored.import_session(
        bundle,
        authorize_human_curation=True,
        authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
    )

    assert forbidden.calls == 0
    assert set(result["event_ids"]) == set(original_by_id)
    for event_id in result["event_ids"]:
        before = original_by_id[event_id]
        after = restored.store.require(event_id)
        assert (
            after.id,
            after.type,
            after.created_at,
            after.session_id,
            after.branch_id,
            after.parent_event_id,
            after.causation_id,
            after.correlation_id,
            after.actor,
        ) == (
            before.id,
            before.type,
            before.created_at,
            before.session_id,
            before.branch_id,
            before.parent_event_id,
            before.causation_id,
            before.correlation_id,
            before.actor,
        )

    original_prompt = next(
        event for event in original_events if event.type is EventType.HUMAN_INPUT
    )
    restored_prompt = restored.store.require(original_prompt.id)
    assert restored_prompt.payload["text"] == "  計算し直せ。\n"
    original_contexts = {
        event.id: event.payload["messages"]
        for event in original_events
        if event.type is EventType.ORACLE_CONTEXT_BUILT
    }
    assert {
        event.id: event.payload["messages"]
        for event in restored.store.list_events(event_type=EventType.ORACLE_CONTEXT_BUILT)
    } == original_contexts

    continuation = restored.store.require(continuation_id)
    restored_raw_path = Path(str(continuation.payload["archive_path"]))
    assert restored_raw_path.is_relative_to((tmp_path / "restored" / "home" / "archive").resolve())
    assert restored_raw_path.read_bytes() == provider_raw
    assert sha256_bytes(provider_raw) == continuation.payload["archive_sha256"]
    assert continuation.metadata["bundle_import"]["original_archive_path"] == str(
        original_by_id[continuation_id].payload["archive_path"]
    )
    tool_result = restored.store.list_events(event_type=EventType.TOOL_OUTPUT)[0]
    assert tool_result.payload["truth_domain"] == "real"
    curation = restored.store.connection.execute(
        "SELECT action, event_id, action_event_id FROM curation WHERE event_id = ?",
        (continuation_id,),
    ).fetchone()
    assert tuple(curation) == (
        "keep",
        continuation_id,
        result["human_curation_event_ids"][0],
    )
    audit = restored.store.require(result["audit_event_id"])
    assert audit.payload["human_curation_authorized"] is True
    assert result["human_curation_event_ids"] == list(
        audit.payload["authorized_curation_event_ids"]
    )
    assert audit.payload["archive_repoints"][continuation_id]["original_archive_path"] == str(
        original_archive
    )
    moved_bundle = bundle.rename(tmp_path / "moved-bundle")
    assert moved_bundle.is_dir()
    assert restored_raw_path.read_bytes() == provider_raw
    assert restored_raw_path.with_name(f"{restored_raw_path.stem}.metadata.json").is_file()
    assert source.claims() == restored.claims()
    assert restored.store.verify_integrity() == []


def test_bundle_round_trip_rebuilds_sparse_virtual_clock_without_wall_time(
    tmp_path: Path,
) -> None:
    source = _service(tmp_path / "clock-source")
    session = source.new_session("portable sparse clock")
    root = source.store.require(session["root_event_id"])
    virtual_state = VirtualStateService(source.store)
    runtime = VirtualWorldRuntime(
        mutation_sink=virtual_state.mutation_sink(
            session_id=str(root.session_id),
            branch_id=str(root.branch_id),
            actor=Actor(kind=ActorKind.HOST, id="virtual-materializer"),
        )
    )
    evidence = SourceEvidence((root.id,), "explicit")
    runtime.clocks.create("observer", evidence=evidence)
    runtime.clocks.set("observer", "148", "hour", evidence=evidence)
    bundle = tmp_path / "clock-bundle"
    source.export("bundle", bundle, session_id=root.session_id)

    restored = _service(tmp_path / "clock-restored", provider=ForbiddenProvider())
    restored.import_bundle(bundle)

    clock = (
        VirtualStateService(restored.store).hydrate(str(root.branch_id)).clocks.require("observer")
    )
    assert clock.current_revision is not None
    assert clock.current_revision.value == "148"
    assert clock.current_revision.unit == "hour"
    assert clock.provenance == [root.id]
    restored.store.rebuild_projections()
    rebuilt = (
        VirtualStateService(restored.store).hydrate(str(root.branch_id)).clocks.require("observer")
    )
    assert rebuilt.to_dict() == clock.to_dict()
    assert restored.store.verify_integrity() == []


def test_bundle_round_trip_repoints_worker_patch_and_validation_archives(
    tmp_path: Path,
) -> None:
    source, bundle, event_ids, worker_stdout, validation_stdout = _export_worker_validation_chain(
        tmp_path
    )
    original_terminal = source.store.require(event_ids["terminal"])
    original_validation = source.store.require(event_ids["validation"])
    original_worker_directory = Path(str(original_terminal.payload["archive_path"]))
    original_validation_directory = Path(str(original_validation.payload["archive_path"]))
    shutil.rmtree(source.archive_root)

    # Dependency order is an import concern, not an accidental JSONL ordering
    # requirement. This specifically places validation/application before the
    # patch, approval, task, and run events in the source file.
    event_lines = (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    (bundle / "events.jsonl").write_text(
        "\n".join(reversed(event_lines)) + "\n",
        encoding="utf-8",
    )
    _rehash(bundle, "events.jsonl")

    restored = _service(tmp_path / "worker-restored")
    result = restored.import_session(bundle)

    terminal = restored.store.require(event_ids["terminal"])
    patch = restored.store.require(event_ids["patch"])
    validation = restored.store.require(event_ids["validation"])
    worker_directory = Path(str(terminal.payload["archive_path"]))
    validation_directory = Path(str(validation.payload["archive_path"]))
    assert worker_directory.is_relative_to(restored.archive_root / "workers")
    assert validation_directory.is_relative_to(restored.archive_root / "validations")
    assert worker_directory != original_worker_directory
    assert validation_directory != original_validation_directory
    assert Path(terminal.payload["archive_manifest"]["stdout.bin"]["path"]).read_bytes() == (
        worker_stdout
    )
    assert Path(validation.payload["archive_manifest"]["stdout.bin"]["path"]).read_bytes() == (
        validation_stdout
    )
    assert (
        patch.payload["patch_archive_path"]
        == terminal.payload["archive_manifest"]["patch.diff"]["path"]
    )
    assert (
        terminal.payload["candidate_patch"]["patch_archive_path"]
        == patch.payload["patch_archive_path"]
    )
    assert terminal.metadata["bundle_import"]["original_archive_path"] == str(
        original_worker_directory
    )
    assert validation.metadata["bundle_import"]["original_archive_path"] == str(
        original_validation_directory
    )
    assert restored._job_queue().require(event_ids["apply_job"]).status is JobStatus.CANCELLED
    assert (
        restored.store.require(event_ids["approval"]).metadata["bundle_import_authority"]
        == "historical_only"
    )
    restored.store.rebuild_projections()
    projected = restored.patch_status(event_ids["patch"])["state"]
    assert projected["status"] == "imported_historical"
    assert projected["validation_status"] == "passed"
    assert restored.store.verify_integrity() == []
    assert set(result["event_ids"]) >= {
        value for key, value in event_ids.items() if key != "apply_job"
    }
    assert result["import_event"]["payload"]["quarantined_worker_job_ids"] == [
        event_ids["apply_job"]
    ]
    assert restored.run_automation(max_jobs=1)["processed"] == []
    with pytest.raises(ServiceError, match="cannot be validated locally"):
        restored._execute_patch_validation_job(
            SimpleNamespace(
                payload={
                    "patch_event_id": event_ids["patch"],
                    "approval_event_id": event_ids["approval"],
                    "application_event_id": event_ids["application"],
                }
            )
        )

    moved_bundle = bundle.rename(tmp_path / "worker-bundle-moved")
    shutil.rmtree(moved_bundle)
    assert Path(patch.payload["patch_archive_path"]).read_bytes()
    assert Path(validation.payload["archive_manifest"]["stdout.bin"]["path"]).read_bytes() == (
        validation_stdout
    )


def test_imported_patch_approval_and_pending_apply_job_are_quarantined(
    tmp_path: Path,
) -> None:
    source, bundle, event_ids, _, _ = _export_worker_validation_chain(tmp_path)
    events_path = bundle / "events.jsonl"
    records = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    records = [
        record
        for record in records
        if record["id"] not in {event_ids["application"], event_ids["validation"]}
    ]
    events_path.write_text(
        "\n".join(json.dumps(record) for record in reversed(records)) + "\n",
        encoding="utf-8",
    )
    shutil.rmtree(bundle / "validations")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["counts"]["events"] = len(records)
    manifest["archive_counts"]["validations"] = 0
    manifest["sha256"] = {
        name: digest
        for name, digest in manifest["sha256"].items()
        if not name.startswith(f"validations/{event_ids['validation']}/")
    }
    manifest["sha256"]["events.jsonl"] = sha256_bytes(events_path.read_bytes())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    shutil.rmtree(source.archive_root)

    restored = _service(tmp_path / "approval-restored")
    result = restored.import_session(bundle)

    approval = restored.store.require(event_ids["approval"])
    assert approval.metadata["bundle_import_authority"] == "historical_only"
    apply_job = restored._job_queue().require(event_ids["apply_job"])
    assert apply_job.status is JobStatus.CANCELLED
    assert result["import_event"]["payload"]["quarantined_worker_job_ids"] == [apply_job.id]
    assert restored.run_automation(max_jobs=1)["processed"] == []
    with pytest.raises(ServiceError, match="not local execution authority"):
        restored._execute_patch_application_job(apply_job)
    assert not restored.store.list_events(event_type=EventType.WORKER_PATCH_APPLIED)
    restored.store.rebuild_projections()
    assert restored._job_queue().require(apply_job.id).status is JobStatus.CANCELLED
    assert restored.store.verify_integrity() == []


def test_imported_unapproved_patch_cannot_become_a_local_filesystem_capability(
    tmp_path: Path,
) -> None:
    source, bundle, event_ids, _, _ = _export_worker_validation_chain(tmp_path)
    events_path = bundle / "events.jsonl"
    records = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    excluded_event_ids = {
        event_ids["approval"],
        event_ids["application"],
        event_ids["validation"],
    }
    records = [
        record
        for record in records
        if record["id"] not in excluded_event_ids
        and record.get("payload", {}).get("id") != event_ids["apply_job"]
    ]
    events_path.write_text(
        "\n".join(json.dumps(record) for record in reversed(records)) + "\n",
        encoding="utf-8",
    )
    shutil.rmtree(bundle / "validations")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["counts"]["events"] = len(records)
    manifest["archive_counts"]["validations"] = 0
    manifest["sha256"] = {
        name: digest
        for name, digest in manifest["sha256"].items()
        if not name.startswith(f"validations/{event_ids['validation']}/")
    }
    manifest["sha256"]["events.jsonl"] = sha256_bytes(events_path.read_bytes())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    shutil.rmtree(source.archive_root)

    restored = _service(tmp_path / "unapproved-restored")
    restored.import_session(bundle)

    patch = restored.store.require(event_ids["patch"])
    assert patch.metadata["bundle_import_authority"] == "historical_only"
    assert restored.patch_status(patch.id)["state"]["status"] == "imported_historical"
    with pytest.raises(ServiceError, match="requires local rebind"):
        restored.approve_patch(patch.id)
    with pytest.raises(ServiceError, match="immutable historical evidence"):
        restored.reject_patch(patch.id)
    assert restored.run_automation(until_human=True, max_jobs=1) == {
        "processed": [],
        "stopped": "idle",
    }
    restored.store.rebuild_projections()
    assert restored.patch_status(patch.id)["state"]["status"] == "imported_historical"
    assert restored.store.verify_integrity() == []


def test_worker_bundle_manifest_mismatch_fails_before_creating_local_archives(
    tmp_path: Path,
) -> None:
    _, bundle, event_ids, _, _ = _export_worker_validation_chain(tmp_path)
    events_path = bundle / "events.jsonl"
    records = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    terminal = next(record for record in records if record["id"] == event_ids["terminal"])
    terminal["payload"]["archive_manifest"]["stdout.bin"]["size_bytes"] += 1
    events_path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    _rehash(bundle, "events.jsonl")
    restored = _service(tmp_path / "manifest-mismatch-restored")

    with pytest.raises(BundleImportError, match="differs from event manifest"):
        restored.import_session(bundle)

    assert restored.store.count_events() == 0
    assert not (restored.archive_root / "workers").exists()
    assert not (restored.archive_root / "validations").exists()


def test_worker_bundle_import_rejects_symlinked_destination_ancestors(tmp_path: Path) -> None:
    _, bundle, _, _, _ = _export_worker_validation_chain(tmp_path)
    restored = _service(tmp_path / "symlink-destination-restored")
    outside = tmp_path / "outside-archive"
    outside.mkdir()
    restored.archive_root.parent.mkdir(parents=True, exist_ok=True)
    restored.archive_root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(BundleImportError, match="contains a symlink"):
        restored.import_session(bundle)

    assert restored.store.count_events() == 0
    assert list(outside.iterdir()) == []


def test_bundle_curation_requires_explicit_human_import_authorization(tmp_path: Path) -> None:
    _, bundle, _, _ = _export_complete_chain(tmp_path)
    store = EventStore(tmp_path / "unauthorized.db")

    with pytest.raises(BundleImportError, match="human-authorized"):
        ResearchBundleImporter(store).import_directory(bundle)

    assert store.count_events() == 0


def test_bundle_import_remains_compatible_with_original_v1_manifest(tmp_path: Path) -> None:
    _, bundle, continuation_id, provider_raw = _export_complete_chain(tmp_path)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("archive_counts")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    restored = _service(tmp_path / "v1-restored", provider=ForbiddenProvider())

    restored.import_bundle(
        bundle,
        authorize_human_curation=True,
        authorizer=Actor(kind=ActorKind.HUMAN, id="v1-importer"),
    )

    event = restored.store.require(continuation_id)
    assert Path(str(event.payload["archive_path"])).read_bytes() == provider_raw
    assert restored.store.verify_integrity() == []


def test_bundle_context_validation_ignores_a_rejection_appended_after_the_snapshot(
    tmp_path: Path,
) -> None:
    source, _first_bundle, _, _ = _export_complete_chain(tmp_path)
    context, cited_oracle = next(
        (context_event, source.store.require(event_id))
        for context_event in source.store.list_events(event_type=EventType.ORACLE_CONTEXT_BUILT)
        for event_id in context_event.payload["source_event_ids"]
        if source.store.require(event_id).type is EventType.ORACLE_OUTPUT
    )
    rejection = source.reject(cited_oracle.id)
    bundle = tmp_path / "future-reject-bundle"
    source.export("bundle", bundle, session_id=cited_oracle.session_id)
    restored = _service(tmp_path / "future-reject-restored", provider=ForbiddenProvider())

    result = restored.import_bundle(
        bundle,
        authorize_human_curation=True,
        authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
    )

    assert restored.store.require(context.id).payload["sha256"] == context.payload["sha256"]
    assert restored.store.require(rejection["id"]).type is EventType.HUMAN_REJECT
    assert result["session_id"] == cited_oracle.session_id


def test_bundle_import_reuses_an_exact_archive_pair_left_by_an_interrupted_import(
    tmp_path: Path,
) -> None:
    source, bundle, continuation_id, provider_raw = _export_complete_chain(tmp_path)
    source_event = source.store.require(continuation_id)
    restored = _service(tmp_path / "orphan-retry", provider=ForbiddenProvider())
    archive = RawResponseArchive(restored.archive_root / "raw")
    raw_path, sidecar_path = archive.paths_for(continuation_id, source_event.created_at)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes((bundle / "raw" / f"{continuation_id}.json").read_bytes())
    sidecar_path.write_bytes((bundle / "raw" / f"{continuation_id}.metadata.json").read_bytes())

    result = restored.import_bundle(
        bundle,
        authorize_human_curation=True,
        authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
    )

    assert raw_path.read_bytes() == provider_raw
    imported = restored.store.require(continuation_id)
    assert imported.metadata["bundle_import"]["reused_verified_orphan"] is True
    assert result["session_id"] == source_event.session_id


@pytest.mark.parametrize("orphan_shape", ["raw_only", "different_raw", "different_sidecar"])
def test_bundle_import_rejects_incomplete_or_different_archive_orphans(
    tmp_path: Path,
    orphan_shape: str,
) -> None:
    source, bundle, continuation_id, _ = _export_complete_chain(tmp_path)
    source_event = source.store.require(continuation_id)
    restored = _service(tmp_path / f"bad-orphan-{orphan_shape}")
    archive = RawResponseArchive(restored.archive_root / "raw")
    raw_path, sidecar_path = archive.paths_for(continuation_id, source_event.created_at)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_bytes = (bundle / "raw" / f"{continuation_id}.json").read_bytes()
    sidecar_bytes = (bundle / "raw" / f"{continuation_id}.metadata.json").read_bytes()
    raw_path.write_bytes(b"different" if orphan_shape == "different_raw" else raw_bytes)
    if orphan_shape != "raw_only":
        sidecar_path.write_bytes(b"{}" if orphan_shape == "different_sidecar" else sidecar_bytes)

    with pytest.raises(BundleImportError, match=r"orphan|different bytes"):
        restored.import_bundle(
            bundle,
            authorize_human_curation=True,
            authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
        )

    assert restored.store.count_events() == 0

    service = _service(tmp_path / "unauthorized-service")
    with pytest.raises(BundleImportError, match="human-authorized"):
        service.import_bundle(bundle)
    assert service.store.count_events() == 0


def test_bundle_import_rejects_tampering_atomically(tmp_path: Path) -> None:
    source = _service(tmp_path / "minimal-source")
    session = source.new_session("tamper")
    bundle = tmp_path / "tampered"
    source.export("bundle", bundle, session_id=session["id"])
    events_path = bundle / "events.jsonl"
    events_path.write_bytes(events_path.read_bytes() + b"\n")
    target = EventStore(tmp_path / "tampered-target.db")

    with pytest.raises(BundleImportError, match="hash mismatch"):
        ResearchBundleImporter(target).import_directory(bundle)

    assert target.count_events() == 0

    wrong_counts = tmp_path / "wrong-counts"
    source.export("bundle", wrong_counts, session_id=session["id"])
    manifest_path = wrong_counts / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["counts"]["events"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    count_target = EventStore(tmp_path / "wrong-count-target.db")
    with pytest.raises(BundleImportError, match="counts do not match"):
        ResearchBundleImporter(count_target).import_directory(wrong_counts)
    assert count_target.count_events() == 0


def test_bundle_import_rejects_identity_collisions_without_partial_append(
    tmp_path: Path,
) -> None:
    source = _service(tmp_path / "collision-source")
    session = source.new_session("collision")
    bundle = tmp_path / "collision-bundle"
    source.export("bundle", bundle, session_id=session["id"])
    target = _service(tmp_path / "collision-target")
    first = target.import_bundle(bundle)
    count = target.store.count_events()

    with pytest.raises(BundleImportError, match="identity collision"):
        target.import_bundle(bundle)

    assert target.store.count_events() == count
    assert target.store.require(first["audit_event_id"])


def test_bundle_import_rejects_manifest_traversal_and_symlinks(tmp_path: Path) -> None:
    source = _service(tmp_path / "unsafe-source")
    session = source.new_session("unsafe")

    traversal = tmp_path / "traversal"
    source.export("bundle", traversal, session_id=session["id"])
    manifest_path = traversal / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["sha256"]["../outside"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(BundleImportError, match="unsafe manifest path"):
        ResearchBundleImporter(EventStore(tmp_path / "traversal.db")).import_directory(traversal)

    symlinked = tmp_path / "symlinked"
    source.export("bundle", symlinked, session_id=session["id"])
    (symlinked / "raw" / "escape").symlink_to(tmp_path / "outside")
    with pytest.raises(BundleImportError, match="symlink"):
        ResearchBundleImporter(EventStore(tmp_path / "symlink.db")).import_directory(symlinked)


@pytest.mark.parametrize("damage", ["dangling", "cycle", "cross-session"])
def test_bundle_import_rejects_invalid_event_graph_atomically(
    tmp_path: Path,
    damage: str,
) -> None:
    source = _service(tmp_path / f"graph-source-{damage}")
    session = source.new_session(damage)
    source.ask("確認しろ。")
    bundle = tmp_path / f"graph-{damage}"
    source.export("bundle", bundle, session_id=session["id"])
    records = [
        json.loads(line)
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    if damage == "dangling":
        records[-1]["parent_event_id"] = "evt_00000000000000000000000000"
    elif damage == "cycle":
        records[0]["parent_event_id"] = records[0]["id"]
    else:
        records[-1]["session_id"] = "ses_00000000000000000000000000"
    (bundle / "events.jsonl").write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    _rehash(bundle, "events.jsonl")
    target = EventStore(tmp_path / f"graph-target-{damage}.db")

    with pytest.raises(BundleImportError):
        ResearchBundleImporter(target).import_directory(bundle)

    assert target.count_events() == 0


def test_bundle_import_ignores_forged_derived_files_and_rebuilds_from_events(
    tmp_path: Path,
) -> None:
    source = _service(tmp_path / "derived-source")
    session = source.new_session("derived")
    bundle = tmp_path / "derived-bundle"
    source.export("bundle", bundle, session_id=session["id"])
    root_id = session["root_event_id"]
    forged = [{"id": "clm_forged", "source_event_id": root_id, "raw_text": "forged"}]
    (bundle / "claims.json").write_text(json.dumps(forged), encoding="utf-8")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["counts"]["claims"] = 1
    manifest["sha256"]["claims.json"] = sha256_bytes((bundle / "claims.json").read_bytes())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    restored = _service(tmp_path / "derived-restored")

    restored.import_bundle(bundle)

    assert restored.claims() == []


def test_bundle_import_cleans_new_archive_files_when_projection_rebuild_fails(
    tmp_path: Path,
) -> None:
    _, bundle, _, _ = _export_complete_chain(tmp_path)
    records = [
        json.loads(line)
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    claim = next(record for record in records if record["type"] == "analysis.claim_detected")
    claim["payload"]["claims"] = [{}]
    claim["payload"].pop("raw_text", None)
    claim["payload"].pop("text", None)
    (bundle / "events.jsonl").write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    _rehash(bundle, "events.jsonl")
    restored = _service(tmp_path / "failed-restored")

    with pytest.raises(Exception, match="each detected claim requires"):
        restored.import_bundle(
            bundle,
            authorize_human_curation=True,
            authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
        )

    assert restored.store.count_events() == 0
    archive_root = tmp_path / "failed-restored" / "home" / "archive" / "raw"
    assert not archive_root.exists() or not [
        path for path in archive_root.rglob("*") if path.is_file()
    ]


def test_shuffled_multibranch_bundle_rebuilds_deterministically(tmp_path: Path) -> None:
    source = _service(tmp_path / "branch-source")
    session = source.new_session("branches")
    input_event = source.ask("確認しろ。")["input"]
    child = source.fork(input_event["id"], "child")
    original = tmp_path / "branch-original"
    source.export("bundle", original, session_id=session["id"])
    shuffled = tmp_path / "branch-shuffled"
    shutil.copytree(original, shuffled)
    lines = (shuffled / "events.jsonl").read_text(encoding="utf-8").splitlines()
    (shuffled / "events.jsonl").write_text(
        "\n".join(reversed(lines)) + "\n",
        encoding="utf-8",
    )
    _rehash(shuffled, "events.jsonl")
    first = _service(tmp_path / "branch-first")
    second = _service(tmp_path / "branch-second")

    first_result = first.import_bundle(original)
    second_result = second.import_bundle(shuffled)

    assert first_result["branch_id"] == second_result["branch_id"] == child["id"]
    assert first.list_sessions()[0]["current_branch_id"] == child["id"]
    assert second.list_sessions()[0]["current_branch_id"] == child["id"]
    projection_columns = (
        "id",
        "session_id",
        "parent_branch_id",
        "fork_event_id",
        "title",
        "created_at",
        "archived_at",
    )
    assert [
        tuple(row[column] for column in projection_columns)
        for row in first.store.connection.execute("SELECT * FROM branches ORDER BY id")
    ] == [
        tuple(row[column] for column in projection_columns)
        for row in second.store.connection.execute("SELECT * FROM branches ORDER BY id")
    ]


def test_archive_write_failure_leaves_no_partial_file_and_is_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle, _, _ = _export_complete_chain(tmp_path)
    restored = _service(tmp_path / "write-failure")
    original_fsync = bundle_import_module.os.fsync

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("injected fsync failure")

    monkeypatch.setattr(bundle_import_module.os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="injected fsync failure"):
        restored.import_bundle(
            bundle,
            authorize_human_curation=True,
            authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
        )
    assert restored.store.count_events() == 0
    archive_root = tmp_path / "write-failure" / "home" / "archive" / "raw"
    assert not archive_root.exists() or not [
        path for path in archive_root.rglob("*") if path.is_file()
    ]

    monkeypatch.setattr(bundle_import_module.os, "fsync", original_fsync)
    result = restored.import_bundle(
        bundle,
        authorize_human_curation=True,
        authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
    )
    assert result["raw_event_ids"]


@pytest.mark.parametrize("mutation", ["messages", "uncited-source", "source-text"])
def test_bundle_import_rejects_forged_context_snapshots_atomically(
    tmp_path: Path,
    mutation: str,
) -> None:
    _, bundle, _, _ = _export_complete_chain(tmp_path)
    _rewrite_context_records(bundle, mutation)
    restored = _service(tmp_path / f"context-{mutation}")

    with pytest.raises(BundleImportError, match="context event"):
        restored.import_bundle(
            bundle,
            authorize_human_curation=True,
            authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
        )

    assert restored.store.count_events() == 0


def test_opaque_external_provider_event_id_is_not_a_local_graph_edge(
    tmp_path: Path,
) -> None:
    _, bundle, continuation_id, _ = _export_complete_chain(tmp_path)
    records = [
        json.loads(line)
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    output = next(record for record in records if record["id"] == continuation_id)
    output["payload"]["api_response_metadata"]["provider_event_id"] = "external-123"
    (bundle / "events.jsonl").write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    _rehash(bundle, "events.jsonl")
    restored = _service(tmp_path / "opaque-restored")

    restored.import_bundle(
        bundle,
        authorize_human_curation=True,
        authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
    )

    assert (
        restored.store.require(continuation_id).payload["api_response_metadata"][
            "provider_event_id"
        ]
        == "external-123"
    )


def test_bundle_import_rejects_non_synthetic_output_without_persisted_context(
    tmp_path: Path,
) -> None:
    _, bundle, _, _ = _export_complete_chain(tmp_path)
    events_path = bundle / "events.jsonl"
    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    context = next(record for record in events if record["type"] == "oracle.context_built")
    context["type"] = "session.checkpointed"
    events_path.write_text(
        "\n".join(json.dumps(record) for record in events) + "\n",
        encoding="utf-8",
    )
    session_path = bundle / "session.jsonl"
    snapshots = [json.loads(line) for line in session_path.read_text(encoding="utf-8").splitlines()]
    snapshots = [record for record in snapshots if record["id"] != context["id"]]
    session_path.write_text(
        "\n".join(json.dumps(record) for record in snapshots) + "\n",
        encoding="utf-8",
    )
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["counts"]["session_records"] -= 1
    manifest["sha256"]["events.jsonl"] = sha256_bytes(events_path.read_bytes())
    manifest["sha256"]["session.jsonl"] = sha256_bytes(session_path.read_bytes())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    restored = _service(tmp_path / "missing-context")

    with pytest.raises(BundleImportError, match="no persisted request context"):
        restored.import_bundle(
            bundle,
            authorize_human_curation=True,
            authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
        )

    assert restored.store.count_events() == 0


def test_bundle_import_rejects_sibling_branch_parent_leakage(tmp_path: Path) -> None:
    source = _service(tmp_path / "sibling-source")
    session = source.new_session("siblings")
    first = source.ask("first")["input"]
    source.fork(first["id"], "child")
    child_input = source.ask("child")["input"]
    bundle = tmp_path / "sibling-bundle"
    source.export("bundle", bundle, session_id=session["id"])
    records = [
        json.loads(line)
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    root_sibling = next(
        record
        for record in records
        if record["branch_id"] == first["branch_id"]
        and record["id"] != first["id"]
        and record["type"] == "oracle.request"
    )
    child = next(record for record in records if record["id"] == child_input["id"])
    child["parent_event_id"] = root_sibling["id"]
    (bundle / "events.jsonl").write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    _rehash(bundle, "events.jsonl")
    restored = _service(tmp_path / "sibling-restored")

    with pytest.raises(
        BundleImportError,
        match=r"crosses branches without session\.forked",
    ):
        restored.import_bundle(bundle)

    assert restored.store.count_events() == 0


def test_session_import_help_mentions_research_bundle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(cli, "_service_factory", lambda: _service(tmp_path / "cli"))

    result = CliRunner().invoke(cli.app, ["session", "import", "--help"])

    assert result.exit_code == 0
    assert "research-bundle" in result.output
    assert "directory" in result.output
    assert "--authorize-human-curation" in result.output
