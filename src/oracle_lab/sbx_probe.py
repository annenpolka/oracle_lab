"""Read-only, non-authorizing observations of the real Docker ``sbx`` CLI.

The probe in this module is deliberately not a coding-worker isolation broker.
It observes the v0.39 control plane and, optionally, an already-existing
sandbox.  It never creates, executes in, stops, or removes a sandbox.  This is
important because v0.39 only exposes name-selected mutation commands: a
list/inspect check followed by ``rm NAME`` cannot rule out name reuse in the
check-to-use gap.  Its strongest success state is ``observed``; it cannot
construct an ``IsolationAttestation`` and it never starts Codex, OpenCode, an
OracleProvider, or a model-authored command.

Raw stdout and stderr are kept out of public JSON.  A write-once archive stores
the exact bounded bytes with mode 0600 and a canonical manifest containing
hashes, provenance, truth domains, and the still-unresolved production blocker
IDs.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import shutil
import stat
import uuid
from collections.abc import Callable, Sequence
from dataclasses import InitVar as InitVar
from dataclasses import replace
from pathlib import Path
from typing import Any as Any

from oracle_lab.docker_sbx_isolation import (
    CommandResult,
    CommandRunner,
    SubprocessCommandRunner,
)
from oracle_lab.sbx_observation import (
    SbxObservationError,
    SbxV039Inspect,
    SbxV039Inventory,
    SbxV039Version,
    decode_v039_inspect,
    decode_v039_inventory,
    decode_v039_version,
)
from oracle_lab.sbx_observation_archive import (
    HardenedSbxObservationArchive,
    SbxObservationArchiveError,
    SbxObservationArchiveRecord,
)
from oracle_lab.sbx_observation_payload import (
    ObservationOrigin,
    ObservationStatus,
    RawSbxCommandObservation,
    SbxDerivedProvenanceEdge,
    SbxNoModelObservationReport,
    SbxProbeError,
    TruthDomain,
    _issue_real_sbx_observation_report,
    _new_provenance_edge,
    is_fixed_read_only_sbx_operation,
    is_safe_sbx_sandbox_name,
)

_CONTROL_TIMEOUT_SECONDS = 30.0
_CONTROL_MAX_OUTPUT_BYTES = 1024 * 1024
_TRUSTED_SUBPROCESS_RUN = SubprocessCommandRunner.run


def _invoke_probe_callback[T](callback: Callable[[], T], reason_id: str) -> T:
    try:
        return callback()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass
    raise SbxProbeError(reason_id) from None


class SbxObservationArchive(HardenedSbxObservationArchive):
    """Probe-facing adapter that preserves stable ``SbxProbeError`` IDs."""

    def write(self, report: SbxNoModelObservationReport) -> SbxObservationArchiveRecord:
        try:
            return super().write(report)
        except SbxObservationArchiveError as error:
            failure_reason = error.reason_id
        raise SbxProbeError(failure_reason) from None


def _hash_regular_executable(configured: str) -> tuple[str, str]:
    candidate_text = configured if Path(configured).is_absolute() else shutil.which(configured)
    if type(candidate_text) is not str or not candidate_text or "\x00" in candidate_text:
        raise SbxProbeError("sbx_probe_executable_unavailable")
    try:
        resolved = Path(candidate_text).expanduser().resolve(strict=True)
        details = resolved.lstat()
    except OSError:
        resolved = details = None
    if resolved is None or details is None:
        raise SbxProbeError("sbx_probe_executable_unavailable")
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise SbxProbeError("sbx_probe_executable_not_regular")
    if not os.access(resolved, os.X_OK):
        raise SbxProbeError("sbx_probe_executable_not_executable")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise SbxProbeError("sbx_probe_executable_identity_unavailable")
    try:
        descriptor = os.open(resolved, os.O_RDONLY | nofollow)
    except OSError:
        descriptor = None
    if descriptor is None:
        raise SbxProbeError("sbx_probe_executable_unreadable")
    digest = hashlib.sha256()
    before: os.stat_result | None = None
    after: os.stat_result | None = None
    try:
        before = os.fstat(descriptor)
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            digest.update(block)
        after = os.fstat(descriptor)
    except OSError:
        after = None
    finally:
        try:
            os.close(descriptor)
        except OSError:
            after = None
    if before is None or after is None:
        raise SbxProbeError("sbx_probe_executable_unreadable")

    if (
        not os.path.samestat(before, after)
        or before.st_mode != after.st_mode
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
        or not stat.S_ISREG(before.st_mode)
    ):
        raise SbxProbeError("sbx_probe_executable_changed")
    return str(resolved), digest.hexdigest()


class DockerSbxNoModelProbe:
    """Observe one disposable shell sandbox without issuing an attestation."""

    def __init__(
        self,
        *,
        executable: str = "sbx",
        runner: CommandRunner | None = None,
        clock: Callable[[], dt.datetime] | None = None,
        uuid_factory: Callable[[], uuid.UUID] | None = None,
    ) -> None:
        if type(executable) is not str or not executable.strip() or "\x00" in executable:
            raise SbxProbeError("sbx_probe_executable_invalid")
        self._configured_executable = executable
        self._runner_is_internal = runner is None
        self._runner = runner or SubprocessCommandRunner()
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        self._uuid_factory = uuid_factory or uuid.uuid4

    def _environment(self) -> dict[str, str]:
        environment = {
            name: os.environ[name]
            for name in (
                "HOME",
                "LANG",
                "LC_ALL",
                "TMPDIR",
                "XDG_CONFIG_HOME",
                "XDG_RUNTIME_DIR",
                "XDG_STATE_HOME",
            )
            if name in os.environ
        }
        # Keep any Host helper lookup on the platform default path instead of
        # inheriting an operator PATH that could redirect helper execution.
        environment["PATH"] = os.defpath
        environment["SBX_NO_TELEMETRY"] = "1"
        return environment

    def _origin(self) -> tuple[ObservationOrigin, TruthDomain]:
        if self._runner_is_internal and type(self._runner) is SubprocessCommandRunner:
            return "real", "real"
        declared_origin = _invoke_probe_callback(
            lambda: getattr(self._runner, "evidence_origin", None),
            "sbx_probe_runner_origin_untrusted",
        )
        if type(declared_origin) is str and declared_origin == "synthetic_fixture":
            return "synthetic_fixture", "synthetic"
        raise SbxProbeError("sbx_probe_runner_origin_untrusted") from None

    def _now(self) -> dt.datetime:
        return _invoke_probe_callback(self._clock, "sbx_probe_timestamp_invalid")

    def _new_probe_id(self) -> str:
        generated = _invoke_probe_callback(self._uuid_factory, "sbx_probe_uuid_factory_invalid")
        if type(generated) is not uuid.UUID:
            raise SbxProbeError("sbx_probe_uuid_factory_invalid") from None
        return f"obs_{generated.hex}"

    def _run(
        self,
        executable: str,
        executable_sha256: str,
        operation_id: str,
        arguments: Sequence[str],
        observations: list[RawSbxCommandObservation],
        *,
        timeout_seconds: float = _CONTROL_TIMEOUT_SECONDS,
    ) -> CommandResult:
        if not is_fixed_read_only_sbx_operation(operation_id, arguments):
            raise SbxProbeError("sbx_probe_command_not_read_only")
        _origin, truth_domain = self._origin()
        invoked_argv = (executable, *arguments)
        result: CommandResult | None = None
        runner_failed = False
        try:
            options = {
                "input_bytes": b"",
                "environment": self._environment(),
                "timeout_seconds": timeout_seconds,
                "max_output_bytes": _CONTROL_MAX_OUTPUT_BYTES,
            }
            if self._runner_is_internal:
                result = _TRUSTED_SUBPROCESS_RUN(self._runner, invoked_argv, **options)
            else:
                result = self._runner.run(invoked_argv, **options)
        except BaseException as error:
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            runner_failed = True
        if runner_failed or result is None:
            raise SbxProbeError(f"sbx_probe_{operation_id}_unavailable")
        if type(result) is not CommandResult:
            raise SbxProbeError("sbx_probe_runner_result_invalid")
        result_snapshot = replace(result)
        result_argv = result_snapshot.argv
        stdout = result_snapshot.stdout
        stderr = result_snapshot.stderr
        if (
            type(result_argv) is not tuple
            or any(type(value) is not str for value in result_argv)
            or result_argv != invoked_argv
            or type(stdout) is not bytes
            or type(stderr) is not bytes
            or len(stdout) + len(stderr) > _CONTROL_MAX_OUTPUT_BYTES
        ):
            raise SbxProbeError("sbx_probe_runner_result_invalid")
        observed_at = self._now()
        observations.append(
            RawSbxCommandObservation(
                operation_id=operation_id,
                argv=result_snapshot.argv,
                exit_code=result_snapshot.exit_code,
                stdout=result_snapshot.stdout,
                stderr=result_snapshot.stderr,
                timed_out=result_snapshot.timed_out,
                output_limited=result_snapshot.output_limited,
                truth_domain=truth_domain,
                observed_at=observed_at,
            )
        )
        current_path, current_sha256 = _hash_regular_executable(executable)
        if current_path != executable or current_sha256 != executable_sha256:
            raise SbxProbeError("sbx_probe_executable_changed")
        return result_snapshot

    @staticmethod
    def _require_success(result: CommandResult, *, operation_id: str) -> None:
        if result.timed_out:
            raise SbxProbeError(f"sbx_probe_{operation_id}_timed_out")
        if result.output_limited:
            raise SbxProbeError(f"sbx_probe_{operation_id}_output_limited")
        if result.exit_code != 0:
            raise SbxProbeError(f"sbx_probe_{operation_id}_failed")

    def _read_json[T](
        self,
        executable: str,
        executable_sha256: str,
        operation_id: str,
        arguments: Sequence[str],
        observations: list[RawSbxCommandObservation],
        decoder: Callable[[bytes], T],
    ) -> T:
        result = self._run(
            executable,
            executable_sha256,
            operation_id,
            arguments,
            observations,
        )
        self._require_success(result, operation_id=operation_id)
        return decoder(result.stdout)

    @staticmethod
    def _require_name_selected_view(
        inventory: SbxV039Inventory,
        sandbox_name: str,
        inspected: SbxV039Inspect,
    ) -> None:
        """Validate only the name-selected view, never a UUID/instance join."""

        matches = tuple(item for item in inventory.sandboxes if item.name == sandbox_name)
        if len(matches) != 1 or inspected.name != sandbox_name or inspected.network != sandbox_name:
            raise SbxProbeError("sbx_probe_name_selected_view_inconsistent")

    def observe_control_plane(
        self,
        *,
        sandbox_name: str | None = None,
    ) -> SbxNoModelObservationReport:
        """Observe version/inventory and optionally one existing sandbox.

        Every issued command is read-only.  Optional ``inspect`` results remain
        separate name-selected views: v0.39 exposes no atomic field that can
        bind their image/workspace data to the UUID returned by ``ls``.
        """

        if sandbox_name is not None and not is_safe_sbx_sandbox_name(sandbox_name):
            raise SbxProbeError("sbx_probe_sandbox_name_invalid")

        executable, executable_sha256 = _hash_regular_executable(self._configured_executable)
        evidence_origin, _truth_domain = self._origin()
        probe_id = self._new_probe_id()

        observations: list[RawSbxCommandObservation] = []
        version: SbxV039Version | None = None
        inventory_sandbox_count: int | None = None
        name_selected_inspect_observed = False
        reason_id: str | None = None
        provenance_edges: list[SbxDerivedProvenanceEdge] = []

        try:
            version = self._read_json(
                executable,
                executable_sha256,
                "version",
                ("version",),
                observations,
                decode_v039_version,
            )
            provenance_edges.append(_new_provenance_edge("version"))

            initial = self._read_json(
                executable,
                executable_sha256,
                "initial_inventory",
                ("ls", "--json"),
                observations,
                decode_v039_inventory,
            )
            inventory_sandbox_count = len(initial.sandboxes)
            provenance_edges.append(_new_provenance_edge("inventory"))
            if sandbox_name is not None:
                initially_inspected = self._read_json(
                    executable,
                    executable_sha256,
                    "initial_inspect",
                    ("inspect", sandbox_name, "--json"),
                    observations,
                    decode_v039_inspect,
                )
                self._require_name_selected_view(
                    initial,
                    sandbox_name,
                    initially_inspected,
                )
                verification = self._read_json(
                    executable,
                    executable_sha256,
                    "verification_inventory",
                    ("ls", "--json"),
                    observations,
                    decode_v039_inventory,
                )
                verification_inspect = self._read_json(
                    executable,
                    executable_sha256,
                    "verification_inspect",
                    ("inspect", sandbox_name, "--json"),
                    observations,
                    decode_v039_inspect,
                )
                self._require_name_selected_view(
                    verification,
                    sandbox_name,
                    verification_inspect,
                )
                name_selected_inspect_observed = True
                provenance_edges.append(_new_provenance_edge("name_selected_inspect"))
        except (SbxObservationError, SbxProbeError) as error:
            reason_id = error.reason_id

        status: ObservationStatus = "observed" if reason_id is None else "incomplete"
        report_values = dict(
            probe_id=probe_id,
            status=status,
            reason_id=reason_id,
            observed_at=self._now(),
            broker_executable_path=executable,
            broker_executable_sha256=executable_sha256,
            version=version,
            inventory_sandbox_count=inventory_sandbox_count,
            name_selected_inspect_observed=name_selected_inspect_observed,
            cleanup_performed=False,
            cleanup_confirmed=None,
            observations=tuple(observations),
            provenance_edges=tuple(provenance_edges),
        )
        if evidence_origin == "real":
            return _issue_real_sbx_observation_report(**report_values)
        return SbxNoModelObservationReport(
            **report_values,
            evidence_origin="synthetic_fixture",
        )


def observe_and_archive_no_model_sbx(
    *,
    archive_root: str | Path,
    sandbox_name: str | None = None,
    executable: str = "sbx",
    runner: CommandRunner | None = None,
    clock: Callable[[], dt.datetime] | None = None,
    uuid_factory: Callable[[], uuid.UUID] | None = None,
) -> tuple[SbxNoModelObservationReport, SbxObservationArchiveRecord]:
    probe = DockerSbxNoModelProbe(
        executable=executable,
        runner=runner,
        clock=clock,
        uuid_factory=uuid_factory,
    )
    report = probe.observe_control_plane(sandbox_name=sandbox_name)
    record = SbxObservationArchive(archive_root).write(report)
    return report, record


__all__ = [
    "DockerSbxNoModelProbe",
    "RawSbxCommandObservation",
    "SbxNoModelObservationReport",
    "SbxObservationArchive",
    "SbxObservationArchiveRecord",
    "SbxProbeError",
    "observe_and_archive_no_model_sbx",
]
