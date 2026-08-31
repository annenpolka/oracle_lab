from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import stat
import sys
import typing
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path

import pytest

from oracle_lab import sbx_observation_payload as payload_module
from oracle_lab import sbx_probe as probe_module
from oracle_lab.coding_isolation import PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS
from oracle_lab.docker_sbx_isolation import CommandResult, SubprocessCommandRunner
from oracle_lab.sbx_observation import SbxV039Version
from oracle_lab.sbx_probe import (
    DockerSbxNoModelProbe,
    SbxObservationArchive,
    SbxProbeError,
)

_FIXED_UUID = uuid.UUID("01234567-89ab-cdef-0123-456789abcdef")
_NAME = "oracle-lab-existing-sandbox"
_SERVER_UUID = "13a6f276-18fc-4358-8a02-d257962b61cb"
_REBOUND_UUID = "1a6f2761-18fc-4358-8a02-d257962b61cb"
_WORKSPACE = "/private/tmp/oracle-lab-read-only.fixture"
_IMAGE = "docker.io/docker/sandbox-templates:shell-docker"
_DIGEST = "sha256:" + "5" * 64
_CLOCK = dt.datetime(2026, 8, 31, 7, 30, tzinfo=dt.UTC)


def test_probe_public_exports_and_type_identity_remain_stable() -> None:
    assert probe_module.__all__ == [
        "DockerSbxNoModelProbe",
        "RawSbxCommandObservation",
        "SbxNoModelObservationReport",
        "SbxObservationArchive",
        "SbxObservationArchiveRecord",
        "SbxProbeError",
        "observe_and_archive_no_model_sbx",
    ]
    assert probe_module.RawSbxCommandObservation.__module__ == probe_module.__name__
    assert probe_module.SbxDerivedProvenanceEdge.__module__ == probe_module.__name__
    assert probe_module.SbxNoModelObservationReport.__module__ == probe_module.__name__
    assert probe_module.SbxProbeError.__module__ == probe_module.__name__
    assert typing.get_type_hints(probe_module.RawSbxCommandObservation)
    assert typing.get_type_hints(probe_module.SbxNoModelObservationReport)
    assert typing.get_type_hints(probe_module.SbxNoModelObservationReport.to_public_dict)
    assert not hasattr(probe_module, "_REAL_REPORT_ISSUER")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


class _FixtureRunner:
    evidence_origin = "synthetic_fixture"

    def __init__(
        self,
        *,
        include_sandbox: bool = False,
        rebind_on_verification: bool = False,
        mismatch_returned_argv: bool = False,
        fail_first_transport: bool = False,
        stderr: bytes = b"",
    ) -> None:
        self.include_sandbox = include_sandbox
        self.rebind_on_verification = rebind_on_verification
        self.mismatch_returned_argv = mismatch_returned_argv
        self.fail_first_transport = fail_first_transport
        self.stderr = stderr
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str]] = []
        self.inventory_count = 0

    def _entry(self) -> dict[str, object]:
        self.inventory_count += 1
        server_uuid = (
            _REBOUND_UUID
            if self.rebind_on_verification and self.inventory_count >= 2
            else _SERVER_UUID
        )
        return {
            "name": _NAME,
            "id": server_uuid,
            "agent": "shell",
            "status": "running",
            "ports": [],
            "workspaces": [_WORKSPACE],
        }

    @staticmethod
    def _inspect() -> bytes:
        return _json_bytes(
            {
                "name": _NAME,
                "agent": "shell",
                "kits": [],
                "state": "running",
                "uptime": "22s",
                "image": _IMAGE,
                "image_digest": _DIGEST,
                "workspace": _WORKSPACE,
                "network": _NAME,
                "network_policy": {"scope": "global"},
                "proxy": "172.17.0.1:3128",
                "secrets": [{"name": "private-service-name", "source": "uploaded"}],
                "mcp_gateway": True,
                "ports": [],
                "sessions": 0,
                "daemon_version": "v0.39.0",
                "daemon_uptime": "3m",
            }
        )

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
        if self.fail_first_transport and not self.calls:
            raise OSError("secret-shaped transport failure")
        command = tuple(argv)
        self.calls.append(command)
        self.environments.append(dict(environment))
        returned_argv = ("forged",) if self.mismatch_returned_argv else command
        arguments = command[1:]
        if arguments == ("version",):
            return CommandResult(
                returned_argv,
                0,
                b"sbx version: v0.39.0 def8cb0523a77e757bdd6ef52b459fe374f3783e\n",
                self.stderr,
            )
        if arguments == ("ls", "--json"):
            entries = [self._entry()] if self.include_sandbox else []
            return CommandResult(
                returned_argv,
                0,
                _json_bytes({"sandboxes": entries}),
                self.stderr,
            )
        if arguments == ("inspect", _NAME, "--json"):
            return CommandResult(returned_argv, 0, self._inspect(), self.stderr)
        raise AssertionError(f"mutating or unexpected sbx command: {arguments!r}")


class _ForgedProductionRunner(_FixtureRunner):
    evidence_origin = "production"


def _executable(tmp_path: Path) -> Path:
    executable = tmp_path / "sbx"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "if sys.argv[1:] == ['version']:\n"
        "    sys.stdout.buffer.write("
        "b'sbx version: v0.39.0 def8cb0523a77e757bdd6ef52b459fe374f3783e\\n')\n"
        "elif sys.argv[1:] == ['ls', '--json']:\n"
        "    sys.stdout.buffer.write(b'{\"sandboxes\":[]}\\n')\n"
        "else:\n"
        "    raise SystemExit(64)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    return executable


def _probe(
    tmp_path: Path,
    runner: _FixtureRunner,
    *,
    sandbox_name: str | None = None,
):
    return DockerSbxNoModelProbe(
        executable=str(_executable(tmp_path)),
        runner=runner,
        clock=lambda: _CLOCK,
        uuid_factory=lambda: _FIXED_UUID,
    ).observe_control_plane(sandbox_name=sandbox_name)


def _issued_real_raw_replay_fixture():
    # Test-only, manually reconstructed current-schema replay of raw/identity
    # metadata from obs_f58ae6caf1564d198feb8d8c7cfed1b6. It is not a session
    # import and does not claim to reproduce that older manifest: its unknown
    # summary fields were null, while the current report contract derives false.
    executable = "/opt/homebrew/Caskroom/sbx/0.39.0/bin/sbx"
    observations = (
        probe_module.RawSbxCommandObservation(
            operation_id="version",
            argv=(executable, "version"),
            exit_code=0,
            stdout=(b"sbx version: v0.39.0 def8cb0523a77e757bdd6ef52b459fe374f3783e\n"),
            stderr=b"",
            timed_out=False,
            output_limited=False,
            truth_domain="real",
            observed_at=dt.datetime(2026, 8, 31, 8, 27, 2, 428079, tzinfo=dt.UTC),
        ),
        probe_module.RawSbxCommandObservation(
            operation_id="initial_inventory",
            argv=(executable, "ls", "--json"),
            exit_code=0,
            stdout=b'{"sandboxes":[]}\n',
            stderr=b"",
            timed_out=False,
            output_limited=False,
            truth_domain="real",
            observed_at=dt.datetime(2026, 8, 31, 8, 27, 2, 654114, tzinfo=dt.UTC),
        ),
    )
    return payload_module._issue_real_sbx_observation_report(
        probe_id="obs_f58ae6caf1564d198feb8d8c7cfed1b6",
        status="observed",
        reason_id=None,
        observed_at=dt.datetime(2026, 8, 31, 8, 27, 2, 701350, tzinfo=dt.UTC),
        broker_executable_path=executable,
        broker_executable_sha256="f2a9e83f41a1cc20292d1f0e40974c495065f59a933aaec98f0619c286ddbeaf",
        version=SbxV039Version(
            version="v0.39.0",
            commit_sha="def8cb0523a77e757bdd6ef52b459fe374f3783e",
        ),
        inventory_sandbox_count=0,
        name_selected_inspect_observed=False,
        cleanup_performed=False,
        cleanup_confirmed=None,
        observations=observations,
        provenance_edges=(
            probe_module.SbxDerivedProvenanceEdge("version", ("version",)),
            probe_module.SbxDerivedProvenanceEdge("inventory", ("initial_inventory",)),
        ),
    )


def test_read_only_probe_observes_empty_control_plane_and_never_authorizes(
    tmp_path: Path,
) -> None:
    runner = _FixtureRunner()

    report = _probe(tmp_path, runner)

    assert report.status == "observed"
    assert report.reason_id is None
    assert report.inventory_sandbox_count == 0
    assert report.name_selected_inspect_observed is False
    assert report.atomic_instance_binding_proven is False
    assert report.cleanup_performed is False
    assert report.cleanup_confirmed is None
    assert report.ready is False
    assert report.safe_to_start_worker is False
    assert report.attestation_issued is False
    assert report.production_evidence_blockers == PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS
    assert [edge.derived_field for edge in report.provenance_edges] == [
        "version",
        "inventory",
    ]
    assert runner.environments
    assert all(environment["PATH"] == os.defpath for environment in runner.environments)
    assert [call[1:] for call in runner.calls] == [("version",), ("ls", "--json")]


def test_existing_name_is_inspected_twice_without_claiming_instance_binding(
    tmp_path: Path,
) -> None:
    runner = _FixtureRunner(include_sandbox=True)

    report = _probe(tmp_path, runner, sandbox_name=_NAME)

    assert report.status == "observed"
    assert report.name_selected_inspect_observed is True
    assert report.atomic_instance_binding_proven is False
    assert [call[1] for call in runner.calls] == [
        "version",
        "ls",
        "inspect",
        "ls",
        "inspect",
    ]
    forbidden = {"create", "exec", "run", "stop", "rm", "remove", "delete"}
    assert forbidden.isdisjoint(call[1] for call in runner.calls)
    inspect_edge = next(
        edge for edge in report.provenance_edges if edge.derived_field == "name_selected_inspect"
    )
    assert inspect_edge.source_operation_ids == (
        "initial_inventory",
        "initial_inspect",
        "verification_inventory",
        "verification_inspect",
    )
    public = report.to_public_dict()
    rendered = json.dumps(public, sort_keys=True)
    assert _NAME not in rendered
    assert _WORKSPACE not in rendered
    assert _IMAGE not in rendered
    assert _SERVER_UUID not in rendered
    assert "identity" not in public


def test_uuid_rebind_cannot_be_misreported_as_a_composite_instance_identity(
    tmp_path: Path,
) -> None:
    runner = _FixtureRunner(include_sandbox=True, rebind_on_verification=True)

    report = _probe(tmp_path, runner, sandbox_name=_NAME)

    assert report.status == "observed"
    assert report.name_selected_inspect_observed is True
    assert report.atomic_instance_binding_proven is False
    assert "identity" not in report.to_public_dict()
    assert report.cleanup_performed is False
    assert all(call[1] not in {"rm", "remove", "delete"} for call in runner.calls)


def test_name_selected_inspect_requires_the_name_in_each_inventory_view(
    tmp_path: Path,
) -> None:
    report = _probe(tmp_path, _FixtureRunner(), sandbox_name=_NAME)

    assert report.status == "incomplete"
    assert report.reason_id == "sbx_probe_name_selected_view_inconsistent"
    assert report.name_selected_inspect_observed is False
    assert report.atomic_instance_binding_proven is False


def test_public_report_redacts_raw_bytes_and_argv_while_archive_preserves_raw_streams(
    tmp_path: Path,
) -> None:
    secret = b"credential-shaped-secret"
    runner = _FixtureRunner(stderr=secret)
    report = _probe(tmp_path, runner)

    public_document = report.to_public_dict()
    public = json.dumps(public_document, sort_keys=True)
    assert secret.decode() not in public
    for observation in public_document["observations"]:
        assert "argv" not in observation
        assert "stdout" not in observation
        assert "stderr" not in observation
        assert "argv_sha256" in observation
        assert "stdout_sha256" in observation
        assert "stderr_sha256" in observation

    archive = SbxObservationArchive(tmp_path / "archive")
    record = archive.write(report)
    manifest = record.manifest_path.read_text(encoding="utf-8")
    raw_files = sorted(record.directory.glob("*.bin"))

    assert secret.decode() not in manifest
    assert any(path.read_bytes() == secret for path in raw_files)
    assert stat.S_IMODE(record.manifest_path.stat().st_mode) == 0o600
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in raw_files)
    with pytest.raises(SbxProbeError, match="sbx_observation_archive_not_write_once") as captured:
        archive.write(report)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_self_declared_production_runner_cannot_mint_real_evidence(tmp_path: Path) -> None:
    runner = _ForgedProductionRunner()
    probe = DockerSbxNoModelProbe(
        executable=str(_executable(tmp_path)),
        runner=runner,
        clock=lambda: _CLOCK,
        uuid_factory=lambda: _FIXED_UUID,
    )

    with pytest.raises(SbxProbeError, match="sbx_probe_runner_origin_untrusted"):
        probe.observe_control_plane()

    assert runner.calls == []


def test_injected_exact_subprocess_runner_cannot_mint_real_evidence(tmp_path: Path) -> None:
    runner = SubprocessCommandRunner()

    def forged_run(*_args: object, **_kwargs: object) -> CommandResult:
        raise AssertionError("injected runner was invoked")

    runner.run = forged_run  # type: ignore[method-assign]
    probe = DockerSbxNoModelProbe(
        executable=str(_executable(tmp_path)),
        runner=runner,
        clock=lambda: _CLOCK,
        uuid_factory=lambda: _FIXED_UUID,
    )

    with pytest.raises(SbxProbeError, match="sbx_probe_runner_origin_untrusted"):
        probe.observe_control_plane()


def test_returned_argv_must_exactly_match_requested_argv(tmp_path: Path) -> None:
    report = _probe(tmp_path, _FixtureRunner(mismatch_returned_argv=True))

    assert report.status == "incomplete"
    assert report.reason_id == "sbx_probe_runner_result_invalid"
    assert report.observations == ()
    assert report.provenance_edges == ()


def test_first_transport_failure_retains_primary_reason_without_exception_cause(
    tmp_path: Path,
) -> None:
    report = _probe(tmp_path, _FixtureRunner(fail_first_transport=True))

    assert report.status == "incomplete"
    assert report.reason_id == "sbx_probe_version_unavailable"
    assert report.observations == ()
    assert report.provenance_edges == ()
    record = SbxObservationArchive(tmp_path / "archive").write(report)
    assert record.raw_file_count == 0
    assert json.loads(record.manifest_path.read_bytes())["reason_id"] == report.reason_id


def test_executable_drift_after_command_is_archived_as_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_hash = probe_module._hash_regular_executable
    calls = 0

    def drifting_hash(configured: str) -> tuple[str, str]:
        nonlocal calls
        calls += 1
        path, digest = original_hash(configured)
        return (path, digest if calls == 1 else "0" * 64)

    monkeypatch.setattr(probe_module, "_hash_regular_executable", drifting_hash)

    report = _probe(tmp_path, _FixtureRunner())

    assert report.status == "incomplete"
    assert report.reason_id == "sbx_probe_executable_changed"
    assert [item.operation_id for item in report.observations] == ["version"]


def test_invalid_sandbox_name_is_rejected_before_runner_or_archive(tmp_path: Path) -> None:
    runner = _FixtureRunner()
    probe = DockerSbxNoModelProbe(
        executable=str(_executable(tmp_path)),
        runner=runner,
        clock=lambda: _CLOCK,
        uuid_factory=lambda: _FIXED_UUID,
    )

    with pytest.raises(SbxProbeError, match="sbx_probe_sandbox_name_invalid"):
        probe.observe_control_plane(sandbox_name="secret?token=value")

    assert runner.calls == []


def test_probe_internal_runner_rejects_every_mutating_command_before_execution(
    tmp_path: Path,
) -> None:
    comparison_calls: list[str] = []

    class DeceptiveArgument(str):
        def __eq__(self, _other: object) -> bool:
            comparison_calls.append("eq")
            return True

    runner = _FixtureRunner()
    executable = str(_executable(tmp_path))
    resolved, digest = probe_module._hash_regular_executable(executable)
    probe = DockerSbxNoModelProbe(
        executable=executable,
        runner=runner,
        clock=lambda: _CLOCK,
        uuid_factory=lambda: _FIXED_UUID,
    )

    for operation, arguments in (
        ("create", ("create", "shell", "/tmp/fixture")),
        ("execute", ("exec", _NAME, "true")),
        ("remove", ("rm", "--force", _NAME)),
        ("version_extra", ("version", "--json")),
    ):
        with pytest.raises(SbxProbeError, match="sbx_probe_command_not_read_only"):
            probe._run(resolved, digest, operation, arguments, [])
    with pytest.raises(SbxProbeError, match="sbx_probe_command_not_read_only"):
        probe._run(
            resolved,
            digest,
            "initial_inventory",
            (DeceptiveArgument("rm"), DeceptiveArgument("sandbox")),
            [],
        )

    assert runner.calls == []
    assert comparison_calls == []


def test_runtime_status_cannot_exceed_observed(tmp_path: Path) -> None:
    report = _probe(tmp_path, _FixtureRunner())

    with pytest.raises(SbxProbeError, match="sbx_probe_status_invalid"):
        replace(report, status="passed")  # type: ignore[arg-type]

    real_observations = tuple(
        replace(observation, truth_domain="real") for observation in report.observations
    )
    with pytest.raises(SbxProbeError, match="sbx_probe_real_origin_unissued"):
        replace(
            report,
            evidence_origin="real",
            observations=real_observations,
        )


def test_version_string_subclass_cannot_bypass_exact_schema(tmp_path: Path) -> None:
    class DeceptiveVersion(str):
        def __ne__(self, _other: object) -> bool:
            return False

    report = _probe(tmp_path, _FixtureRunner())
    forged = SbxV039Version(
        version=DeceptiveVersion("credential-shaped-version"),
        commit_sha="d" * 40,
    )

    with pytest.raises(SbxProbeError, match="sbx_probe_version_invalid"):
        replace(report, version=forged)


def test_datetime_subclass_is_rejected_without_invoking_overrides(tmp_path: Path) -> None:
    calls: list[str] = []

    class StatefulDatetime(dt.datetime):
        def utcoffset(self) -> dt.timedelta | None:
            calls.append("utcoffset")
            return super().utcoffset()

        def isoformat(self, *args: object, **kwargs: object) -> str:
            calls.append("isoformat")
            return super().isoformat(*args, **kwargs)

    forged = StatefulDatetime(2026, 8, 31, 7, 30, tzinfo=dt.UTC)
    report = _probe(tmp_path, _FixtureRunner())

    with pytest.raises(SbxProbeError, match="sbx_probe_timestamp_invalid"):
        replace(report, observed_at=forged)
    with pytest.raises(SbxProbeError, match="sbx_probe_timestamp_invalid"):
        replace(report.observations[0], observed_at=forged)

    assert calls == []


def test_exact_builtin_zero_offset_timezone_remains_accepted(tmp_path: Path) -> None:
    safe_timezone = dt.timezone(dt.timedelta(0), "safe-zero-offset")
    observed_at = dt.datetime(2026, 8, 31, 7, 30, tzinfo=safe_timezone)
    report = _probe(tmp_path, _FixtureRunner())
    observation = replace(report.observations[0], observed_at=observed_at)

    changed = replace(
        report,
        observed_at=observed_at,
        observations=(observation, *report.observations[1:]),
    )

    assert changed.observed_at is observed_at
    assert changed.observations[0].observed_at is observed_at


def test_exact_datetime_with_callback_tzinfo_is_rejected_without_invoking_it(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    class StatefulTimezone(dt.tzinfo):
        def utcoffset(self, _value: dt.datetime | None) -> dt.timedelta:
            calls.append("utcoffset")
            raise RuntimeError("credential-shaped-secret")

    forged = dt.datetime(2026, 8, 31, 7, 30, tzinfo=StatefulTimezone())
    report = _probe(tmp_path, _FixtureRunner())

    with pytest.raises(SbxProbeError, match="sbx_probe_timestamp_invalid"):
        replace(report, observed_at=forged)
    with pytest.raises(SbxProbeError, match="sbx_probe_timestamp_invalid"):
        replace(report.observations[0], observed_at=forged)

    assert calls == []


def test_report_binds_every_observation_argv_to_broker_executable(tmp_path: Path) -> None:
    report = _probe(tmp_path, _FixtureRunner())
    forged_observation = replace(
        report.observations[0],
        argv=("/bin/rm", "version"),
    )

    with pytest.raises(SbxProbeError, match="sbx_probe_argv_invalid"):
        replace(
            report,
            observations=(forged_observation, *report.observations[1:]),
        )

    object.__setattr__(report.observations[0], "argv", ("/bin/rm", "version"))
    root = tmp_path / "archive"
    with pytest.raises(
        SbxProbeError,
        match="sbx_observation_archive_observation_invalid",
    ):
        SbxObservationArchive(root).write(report)
    assert not root.exists()


def test_report_cannot_join_inspects_of_different_sandbox_names(tmp_path: Path) -> None:
    report = _probe(tmp_path, _FixtureRunner(include_sandbox=True), sandbox_name=_NAME)
    changed = replace(
        report.observations[-1],
        argv=(report.broker_executable_path, "inspect", "different-safe-name", "--json"),
    )
    object.__setattr__(report, "observations", (*report.observations[:-1], changed))
    root = tmp_path / "archive"

    with pytest.raises(
        SbxProbeError,
        match="sbx_observation_archive_observation_invalid",
    ):
        SbxObservationArchive(root).write(report)

    assert not root.exists()


def test_real_raw_replay_fixture_is_not_misidentified_as_historical_import() -> None:
    report = _issued_real_raw_replay_fixture()
    payload = payload_module.build_canonical_sbx_observation_payload(report)

    assert hashlib.sha256(report.observations[0].stdout).hexdigest() == (
        "ec2bca6825c9cd3381ca85639c5bdef6e7fbf21b4df7e59f1c1a646b2dbd5ab7"
    )
    assert hashlib.sha256(report.observations[1].stdout).hexdigest() == (
        "ff633471906a30693c90bca61630b80539277f2a7e253113f9bd3bcc6586fb35"
    )
    replay_hash = hashlib.sha256(payload.manifest_bytes).hexdigest()
    assert replay_hash == "ab54d0fc4548d44c3520cd9d612e11380549289ad835875f92545dea902e969b"
    assert replay_hash != "b46838075c9851fa6744f221e921f6e1140680501daf29b5970bce2b0eee3049"


def test_issued_real_report_seal_rejects_replace_and_in_place_tampering(
    tmp_path: Path,
) -> None:
    report = _issued_real_raw_replay_fixture()

    assert report._real_evidence_seal_valid()
    with pytest.raises(SbxProbeError, match="sbx_probe_real_origin_unissued"):
        replace(report, inventory_sandbox_count=1)

    object.__setattr__(report, "inventory_sandbox_count", 1)
    with pytest.raises(
        SbxProbeError,
        match="sbx_observation_archive_real_origin_untrusted",
    ):
        SbxObservationArchive(tmp_path / "archive").write(report)


def test_issued_real_report_seal_binds_exact_raw_bytes(tmp_path: Path) -> None:
    report = _issued_real_raw_replay_fixture()
    object.__setattr__(report.observations[0], "stdout", b"tampered\x00raw")
    root = tmp_path / "archive"

    with pytest.raises(
        SbxProbeError,
        match="sbx_observation_archive_real_origin_untrusted",
    ):
        SbxObservationArchive(root).write(report)

    assert not root.exists()


def test_issued_untampered_real_report_archives_without_exposing_seal(tmp_path: Path) -> None:
    report = _issued_real_raw_replay_fixture()

    record = SbxObservationArchive(tmp_path / "archive").write(report)
    manifest = json.loads(record.manifest_path.read_bytes())
    public = json.dumps(manifest, sort_keys=True)

    assert manifest["evidence_origin"] == "real"
    assert all(item["truth_domain"] == "real" for item in manifest["observations"])
    assert "_issuance_seal" not in public
    assert "_issue_authority" not in public
    assert record.manifest_sha256 == hashlib.sha256(record.manifest_path.read_bytes()).hexdigest()


def test_executable_os_failure_does_not_retain_secret_path_context(tmp_path: Path) -> None:
    secret = "credential-shaped-secret"
    probe = DockerSbxNoModelProbe(
        executable=str(tmp_path / secret / "missing-sbx"),
        runner=_FixtureRunner(),
        clock=lambda: _CLOCK,
        uuid_factory=lambda: _FIXED_UUID,
    )

    with pytest.raises(SbxProbeError) as captured:
        probe.observe_control_plane()

    error = captured.value
    assert secret not in str(error)
    assert secret not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None


def test_untrusted_callbacks_do_not_leak_exception_text(tmp_path: Path) -> None:
    secret = "credential-shaped-secret"

    class ExplodingOriginRunner:
        @property
        def evidence_origin(self) -> str:
            raise RuntimeError(secret)

        def run(self, *_args: object, **_kwargs: object) -> CommandResult:
            raise AssertionError("runner must not be called")

    def explode() -> object:
        raise RuntimeError(secret)

    probes = (
        DockerSbxNoModelProbe(
            executable=str(_executable(tmp_path)),
            runner=ExplodingOriginRunner(),
            clock=lambda: _CLOCK,
            uuid_factory=lambda: _FIXED_UUID,
        ),
        DockerSbxNoModelProbe(
            executable=str(_executable(tmp_path)),
            runner=_FixtureRunner(),
            clock=lambda: _CLOCK,
            uuid_factory=explode,  # type: ignore[arg-type]
        ),
        DockerSbxNoModelProbe(
            executable=str(_executable(tmp_path)),
            runner=_FixtureRunner(),
            clock=explode,  # type: ignore[arg-type]
            uuid_factory=lambda: _FIXED_UUID,
        ),
    )
    expected_reasons = (
        "sbx_probe_runner_origin_untrusted",
        "sbx_probe_uuid_factory_invalid",
        "sbx_probe_timestamp_invalid",
    )

    for probe, reason_id in zip(probes, expected_reasons, strict=True):
        with pytest.raises(SbxProbeError) as captured:
            probe.observe_control_plane()
        error = captured.value
        assert error.reason_id == reason_id
        assert secret not in str(error)
        assert secret not in repr(error)
        assert error.__cause__ is None
        assert error.__context__ is None


def test_executable_string_subclass_is_rejected_without_invoking_overrides() -> None:
    calls: list[str] = []

    class ExplodingExecutable(str):
        def strip(self, *_args: object, **_kwargs: object) -> str:
            calls.append("strip")
            raise RuntimeError("credential-shaped-secret")

    with pytest.raises(SbxProbeError, match="sbx_probe_executable_invalid") as captured:
        DockerSbxNoModelProbe(executable=ExplodingExecutable("secret"))

    assert calls == []
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
