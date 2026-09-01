from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import oracle_lab.cli as cli


class RecordingService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        def method(*args: Any, **kwargs: Any) -> dict[str, Any]:
            self.calls.append((name, args, kwargs))
            return {"method": name, "args": args, "kwargs": kwargs}

        return method


@pytest.fixture
def cli_service(monkeypatch: pytest.MonkeyPatch) -> tuple[CliRunner, RecordingService]:
    service = RecordingService()
    monkeypatch.setattr(cli, "_service_factory", lambda: service)
    return CliRunner(), service


def test_root_and_nested_help_expose_the_complete_control_plane(
    cli_service: tuple[CliRunner, RecordingService],
) -> None:
    runner, _ = cli_service

    root = runner.invoke(cli.app, ["--help"])
    session = runner.invoke(cli.app, ["session", "--help"])
    tool = runner.invoke(cli.app, ["tool", "--help"])
    sandbox = runner.invoke(cli.app, ["sandbox", "--help"])
    export = runner.invoke(cli.app, ["export", "--help"])
    provenance = runner.invoke(cli.app, ["provenance", "--help"])
    research = runner.invoke(cli.app, ["research", "--help"])
    replay = runner.invoke(cli.app, ["replay", "--help"])
    worker = runner.invoke(cli.app, ["worker", "--help"])
    worker_enqueue = runner.invoke(cli.app, ["worker", "enqueue", "--help"])
    worker_isolation = runner.invoke(cli.app, ["worker", "isolation", "--help"])
    worker_patch = runner.invoke(cli.app, ["worker", "patch", "--help"])

    assert root.exit_code == 0
    for command in (
        "session",
        "ask",
        "continue",
        "sample",
        "retry",
        "pause",
        "resume",
        "events",
        "tail",
        "show",
        "tree",
        "trace",
        "keep",
        "canonize",
        "reject",
        "star",
        "quarantine",
        "revisit",
        "note",
        "pin-claim",
        "claims",
        "contradictions",
        "motifs",
        "attractors",
        "search",
        "origin",
        "provenance",
        "research",
        "replay",
        "worker",
        "tool",
        "sandbox",
        "run",
        "jobs",
        "cost",
        "compare-models",
        "export",
        "tui",
    ):
        assert command in root.output
    assert (
        session.exit_code
        == tool.exit_code
        == sandbox.exit_code
        == export.exit_code
        == provenance.exit_code
        == research.exit_code
        == replay.exit_code
        == worker.exit_code
        == worker_enqueue.exit_code
        == worker_isolation.exit_code
        == worker_patch.exit_code
        == 0
    )
    for command in (
        "new",
        "list",
        "show",
        "switch",
        "checkpoint",
        "fork",
        "approve-fork",
        "archive",
    ):
        assert command in session.output
    assert "run" in tool.output and "approve" in tool.output
    assert "inspect" in sandbox.output
    assert all(
        command in export.output for command in ("bundle", "public-bundle", "transcript", "corpus")
    )
    assert "trace" in provenance.output and "event" in provenance.output
    assert "exact" in replay.output and "host" in replay.output
    assert all(
        command in worker.output
        for command in ("enqueue", "isolation", "patch", "readiness", "status")
    )
    assert "repository-edit" in worker_enqueue.output
    assert "probe" in worker_isolation.output
    for command in ("show", "approve", "reject", "status"):
        assert command in worker_patch.output
    for command in (
        "contradiction-mechanisms",
        "latex-prefixes",
        "fork-before-attractor",
        "prompt-attractors",
    ):
        assert command in research.output


@pytest.mark.parametrize(
    ("arguments", "method"),
    [
        (["session", "new", "--title", "lab"], "new_session"),
        (["session", "list"], "list_sessions"),
        (["session", "show", "ses_1"], "show_session"),
        (["session", "switch", "ses_1"], "switch_session"),
        (["session", "checkpoint"], "checkpoint"),
        (["session", "fork", "evt_1", "--name", "branch"], "fork"),
        (["session", "approve-fork", "evt_proposal"], "approve_branch"),
        (["session", "archive"], "archive_session"),
        (["ask", "確認しろ。"], "ask"),
        (["continue"], "continue_session"),
        (["sample", "-n", "2"], "sample"),
        (["retry", "evt_1"], "retry"),
        (["pause", "--note", "hold"], "pause"),
        (["resume", "--note", "continue"], "resume"),
        (
            [
                "worker",
                "enqueue",
                "repository-edit",
                "--source",
                "evt_1",
                "--goal",
                "Implement exactly.",
                "--repository",
                "/tmp/fixture",
            ],
            "enqueue_repository_edit",
        ),
        (["worker", "status", "evt_task"], "worker_task_status"),
        (["worker", "patch", "show", "evt_patch"], "patch_show"),
        (["worker", "patch", "approve", "evt_patch"], "approve_patch"),
        (["worker", "patch", "reject", "evt_patch"], "reject_patch"),
        (["worker", "patch", "status", "evt_patch"], "patch_status"),
        (["events"], "list_events"),
        (["tail"], "tail"),
        (["show", "evt_1"], "show_event"),
        (["tree"], "event_tree"),
        (["trace", "evt_1"], "trace_event"),
        (["provenance", "trace", "virtual_file", "/dev/void"], "provenance_trace"),
        (["provenance", "event", "evt_1"], "trace_event"),
        (
            ["research", "contradiction-mechanisms", "--session", "ses_1"],
            "contradiction_mechanism_branches",
        ),
        (["research", "latex-prefixes", "-n", "3"], "words_before_latex_attractors"),
        (
            ["research", "fork-before-attractor", "evt_attractor"],
            "fork_before_attractor",
        ),
        (
            ["research", "prompt-attractors", "--phrase", "報告書"],
            "prompt_attractor_statistics",
        ),
        (["keep", "evt_1"], "keep"),
        (["canonize", "evt_candidate"], "approve_canon_candidate"),
        (["reject", "evt_1"], "reject"),
        (["star", "evt_1"], "star"),
        (["quarantine", "evt_1", "--note", "hold"], "quarantine"),
        (["revisit", "evt_1", "--note", "later"], "revisit"),
        (["note", "evt_1", "interesting"], "note"),
        (["pin-claim", "clm_1"], "pin_claim"),
        (["claims"], "claims"),
        (["contradictions"], "contradictions"),
        (["motifs"], "motifs"),
        (["attractors"], "attractors"),
        (["search", "34.7"], "search"),
        (["origin", "/dev/void"], "origin"),
        (["tool", "run", "evt_1"], "request_tool"),
        (["tool", "approve", "evt_req"], "approve_tool"),
        (["sandbox", "inspect", "sbx_1"], "inspect_sandbox"),
        (["run"], "run_automation"),
        (["jobs"], "list_jobs"),
        (["jobs", "retry", "job_1"], "retry_jobs"),
        (["replay", "exact", "--session", "ses_1"], "replay_exact"),
        (
            ["replay", "host", "evt_1", "--host-profile", "frontier-v2"],
            "replay_host_analysis",
        ),
        (["cost", "--session", "ses_1"], "cost"),
        (
            ["export", "public-bundle", "public-bundle-dir", "--session", "ses_1"],
            "export",
        ),
        (
            [
                "compare-models",
                "--session",
                "ses_1",
                "--event",
                "evt_1",
                "r1-a",
                "r1-b",
            ],
            "compare_models",
        ),
        (["export", "bundle", "bundle-dir"], "export"),
        (["export", "transcript", "transcript.md"], "export"),
        (["export", "corpus", "corpus.jsonl"], "export"),
    ],
)
def test_commands_are_connected_to_service_methods(
    cli_service: tuple[CliRunner, RecordingService],
    arguments: list[str],
    method: str,
) -> None:
    runner, service = cli_service

    result = runner.invoke(cli.app, arguments)

    assert result.exit_code == 0, result.output
    assert service.calls[-1][0] == method


def test_run_until_human_forwards_the_policy_stop_flag(
    cli_service: tuple[CliRunner, RecordingService],
) -> None:
    runner, service = cli_service

    result = runner.invoke(cli.app, ["run", "--until-human", "--max-jobs", "7"])

    assert result.exit_code == 0, result.output
    assert service.calls[-1] == (
        "run_automation",
        (),
        {"until_human": True, "max_jobs": 7},
    )


def test_worker_readiness_is_service_less_and_reports_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agents_config = tmp_path / "agents.toml"
    agents_config.write_text(
        '[router]\nenabled = false\nisolation_backend = "disabled"\n',
        encoding="utf-8",
    )

    def forbidden_service() -> object:
        raise AssertionError("readiness initialized the Oracle Lab service")

    monkeypatch.setattr(cli, "_service_factory", forbidden_service)

    result = CliRunner().invoke(
        cli.app,
        ["worker", "readiness", "--agents-config", str(agents_config)],
    )

    assert result.exit_code == 1, result.output
    document = json.loads(result.output)
    assert document["status"] == "blocked"
    assert document["ready"] is False
    assert document["safe_to_start_worker"] is False


def test_worker_readiness_cli_redacts_valid_toml_scalar_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "operator-secret-must-not-appear"
    agents_config = tmp_path / "agents.toml"
    agents_config.write_text(
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

    def forbidden_service() -> object:
        raise AssertionError("readiness initialized the Oracle Lab service")

    monkeypatch.setattr(cli, "_service_factory", forbidden_service)

    result = CliRunner().invoke(
        cli.app,
        ["worker", "readiness", "--agents-config", str(agents_config)],
    )

    assert result.exit_code == 1, result.output
    assert secret not in result.output
    document = json.loads(result.output)
    assert document["status"] == "failed"
    assert document["checks"][0]["reason_id"] == "agents_config_invalid"


def test_worker_isolation_probe_requires_explicit_read_only_observation_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_service() -> object:
        raise AssertionError("isolation probe initialized the Oracle Lab service")

    def forbidden_probe(**_options: object) -> object:
        raise AssertionError("isolation probe ran without the explicit execution gate")

    monkeypatch.setattr(cli, "_service_factory", forbidden_service)
    monkeypatch.setattr(cli, "observe_and_archive_no_model_sbx", forbidden_probe)

    result = CliRunner().invoke(
        cli.app,
        [
            "worker",
            "isolation",
            "probe",
            "--archive-root",
            str(tmp_path / "archive"),
        ],
    )

    assert result.exit_code == 1, result.output
    document = json.loads(result.output)
    assert document == {
        "attestation_issued": False,
        "ready": False,
        "reason_id": "read_only_sbx_observation_not_confirmed",
        "safe_to_start_worker": False,
        "schema_version": 1,
        "status": "blocked",
    }


def test_worker_isolation_probe_is_service_less_and_emits_non_authorizing_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_root = tmp_path / "archive"
    calls: list[dict[str, object]] = []

    class Report:
        status = "observed"

        @staticmethod
        def to_public_dict() -> dict[str, object]:
            return {
                "schema_version": 1,
                "status": "observed",
                "ready": False,
                "safe_to_start_worker": False,
                "attestation_issued": False,
            }

    class Archive:
        @staticmethod
        def to_public_dict() -> dict[str, object]:
            return {"manifest_sha256": "f" * 64}

    def forbidden_service() -> object:
        raise AssertionError("isolation probe initialized the Oracle Lab service")

    def fake_probe(**options: object) -> tuple[Report, Archive]:
        calls.append(dict(options))
        return Report(), Archive()

    monkeypatch.setattr(cli, "_service_factory", forbidden_service)
    monkeypatch.setattr(cli, "observe_and_archive_no_model_sbx", fake_probe)

    result = CliRunner().invoke(
        cli.app,
        [
            "worker",
            "isolation",
            "probe",
            "--archive-root",
            str(archive_root),
            "--sandbox-name",
            "existing-sandbox",
            "--observe-read-only-control-plane",
        ],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["status"] == "observed"
    assert document["ready"] is False
    assert document["safe_to_start_worker"] is False
    assert document["attestation_issued"] is False
    assert document["archive"]["manifest_sha256"] == "f" * 64
    assert calls == [
        {
            "archive_root": str(archive_root),
            "sandbox_name": "existing-sandbox",
            "executable": "sbx",
        }
    ]
