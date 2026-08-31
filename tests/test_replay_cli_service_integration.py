from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

import oracle_lab.cli as cli
from oracle_lab.agent_adapters import DirectAPIHost, HostWorkerRouter
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.provenance import ProvenanceService
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.usage import UsageKind, UsageService
from tests.support import historical_oracle_fixture

CONFIG = Path(__file__).parents[1] / "config"
FIXTURES = Path(__file__).parent / "fixtures"


class ForbiddenProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def generate(self, _request: Any) -> Any:
        self.calls += 1
        raise AssertionError("exact replay must not call an oracle provider")


def _service(
    tmp_path: Path,
    *,
    provider: ForbiddenProvider | None = None,
    router: HostWorkerRouter | None = None,
) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        provider_factory=None if provider is None else lambda _profile: provider,
        host_worker_router=router,
    )


def _historical_output(
    service: OracleLabService,
    *,
    content: str,
    archive_path: Path | None = None,
) -> Event:
    session_id, branch_id = service._active()
    parent = service.store.list_events(branch_id=branch_id, ascending=False, limit=1)[0]
    if archive_path is None:
        raise ValueError("historical replay fixtures require their source archive")
    return service.store.append(
        historical_oracle_fixture(
            content,
            source_path=archive_path,
            actor_id="historical-r1",
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent.id,
            causation_id=parent.id,
            correlation_id=parent.correlation_id,
            payload_extra={
                "model_profile_id": "r1-initial-openrouter",
                "provider": "historical-provider",
                "archive_path": str(archive_path),
            },
        )
    )


def test_replay_exact_cli_rebuilds_projections_and_jobs_without_provider_call(
    tmp_path: Path,
    monkeypatch,
) -> None:
    provider = ForbiddenProvider()
    service = _service(tmp_path, provider=provider)
    session = service.new_session("exact replay")
    raw_response = b'{"content":"line 1\\r\\n**0 is not zero**"}\r\n'
    archive_path = tmp_path / "historical-response.bin"
    archive_path.write_bytes(raw_response)
    content = "line 1\r\n**0 is not zero**"
    output = _historical_output(service, content=content, archive_path=archive_path)
    claim = service.store.append(
        Event.new(
            EventType.ANALYSIS_CLAIM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="historical-host"),
            session_id=output.session_id,
            branch_id=output.branch_id,
            parent_event_id=output.id,
            causation_id=output.id,
            correlation_id=output.correlation_id,
            payload={
                "raw_text": content,
                "status": "raw_claim",
                "source_event_ids": [output.id],
            },
        )
    )
    job = service._job_queue().enqueue(
        "historical.analysis",
        {"source_event_id": output.id},
        source_event_id=output.id,
        idempotency_key="historical-analysis",
        session_id=output.session_id,
        branch_id=output.branch_id,
    )
    before_output = service.store.require(output.id).to_dict()
    before_archive = archive_path.read_bytes()
    with service.store.transaction() as connection:
        for table in (
            "claim_occurrences",
            "claim_transitions",
            "branch_claim_states",
            "claims",
            "jobs",
        ):
            connection.execute(f"DELETE FROM {table}")
    assert service.store.connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
    assert service.store.connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0

    monkeypatch.setattr(cli, "_service_factory", lambda: service)
    result = CliRunner().invoke(
        cli.app,
        [
            "replay",
            "exact",
            "--session",
            session["id"],
            "--branch",
            session["current_branch_id"],
        ],
    )

    assert result.exit_code == 0, result.output
    replay = json.loads(result.output)
    assert replay["mode"] == "exact"
    assert replay["projections_rebuilt"] is True
    assert replay["audit_event"]["type"] == EventType.SESSION_REPLAYED.value
    assert replay["audit_event"]["payload"]["oracle_queried"] is False
    assert provider.calls == 0
    assert service.store.require(output.id).to_dict() == before_output
    assert service.store.require(output.id).payload["content"].encode() == content.encode()
    assert archive_path.read_bytes() == before_archive
    assert (
        service.store.connection.execute(
            "SELECT COUNT(*) FROM claims WHERE source_event_id = ?", (output.id,)
        ).fetchone()[0]
        == 1
    )
    assert service._job_queue().require(job.id).kind == "historical.analysis"
    assert claim.id in replay["input_event_ids"]


def test_router_backed_host_replay_creates_fresh_identified_jobs_and_consumes_them(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def newer_host(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        calls.append((task_type, payload))
        return {
            "events": [
                {
                    "type": "analysis.session_summary_updated",
                    "payload": {"operation": "newer-host-replay"},
                    "source_event_ids": [payload["source_event_id"]],
                }
            ]
        }

    router = HostWorkerRouter(direct=DirectAPIHost(newer_host))
    service = _service(tmp_path, router=router)
    service.new_session("host replay")
    fixture = FIXTURES / "oracle_output_001.md"
    output = _historical_output(
        service,
        content=fixture.read_text(encoding="utf-8"),
        archive_path=fixture,
    )
    before_output = output.to_dict()

    first = service.replay_host_analysis(output.id, host_profile_label="frontier-v2")
    second = service.replay_host_analysis(output.id, host_profile_label="frontier-v3")

    expected_tasks = {
        "extract_claims",
        "detect_new_mechanisms",
        "extract_entities",
        "check_numeric_consistency",
        "detect_attractors",
        "detect_motifs",
        "detect_recurrence",
        "detect_tool_intent",
    }
    assert {job["kind"] for job in first["jobs"]} == expected_tasks
    assert {job["kind"] for job in second["jobs"]} == expected_tasks
    first_audit = first["audit_event"]["id"]
    second_audit = second["audit_event"]["id"]
    assert first_audit != second_audit
    assert {job["id"] for job in first["jobs"]}.isdisjoint({job["id"] for job in second["jobs"]})
    assert all(job["source_event_id"] == first_audit for job in first["jobs"])
    assert all(job["payload"]["analysis_source_event_id"] == output.id for job in first["jobs"])
    assert all(job["payload"]["replay_event_id"] == first_audit for job in first["jobs"])
    assert all(job["payload"]["host_profile_label"] == "frontier-v2" for job in first["jobs"])

    run = service.run_automation(max_jobs=1)

    assert run["processed"][0]["status"] == "completed"
    assert calls[0][1]["source_event_id"] == output.id
    assert calls[0][1]["replay_event_id"] == first_audit
    assert calls[0][1]["host_profile_label"] == "frontier-v2"
    usage = UsageService(service.store).list_records(kind=UsageKind.HOST)
    assert usage[-1].request_event_id == first_audit
    replayed_analysis = service.store.list_events(
        event_type=EventType.ANALYSIS_SESSION_SUMMARY_UPDATED
    )[-1]
    replay_edges = ProvenanceService(service.store).edges_for("event", replayed_analysis.id)
    assert first_audit in {edge.source_event_id for edge in replay_edges}
    assert service.store.require(output.id).to_dict() == before_output
    audits = service.store.list_events(event_type=EventType.SESSION_REPLAYED)
    assert [event.payload["host_profile_label"] for event in audits] == [
        "frontier-v2",
        "frontier-v3",
    ]


def test_host_replay_without_router_is_explicit_deterministic_local_reanalysis(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    service.new_session("local host replay")
    fixture = FIXTURES / "oracle_output_001.md"
    output = _historical_output(
        service,
        content=fixture.read_text(encoding="utf-8"),
        archive_path=fixture,
    )
    before = output.to_dict()

    replay = service.replay_host_analysis(output.id)

    assert replay["execution"] == "deterministic_local"
    assert replay["host_profile_label"] == "deterministic-local"
    assert replay["jobs"] == []
    assert replay["analysis_event_ids"]
    assert replay["generated_event_ids"]
    assert replay["audit_event"]["payload"]["oracle_queried"] is False
    assert service.store.require(output.id).to_dict() == before
    assert UsageService(service.store).list_records(kind=UsageKind.HOST) == []
