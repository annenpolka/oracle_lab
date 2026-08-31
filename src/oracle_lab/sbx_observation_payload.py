"""Canonical contracts and archive payloads for read-only ``sbx`` evidence.

This module is the single semantic boundary between the subprocess probe and
the filesystem archive.  It owns the fixed read-only command grammar, immutable
observation/report types, provenance validation, public metadata construction,
and exact raw-artifact manifest assembly.  The probe calls these contracts when
it creates a report; the archive calls them again immediately before writing.

Only an exact, validated :class:`SbxNoModelObservationReport` can produce a
payload.  Protocol-shaped objects are inspected only far enough to reject them
with a stable, secret-free reason.  They are never converted into archival
evidence.  The resulting payload contains filenames and bytes only, so the
filesystem sink does not decode SBX JSON or decide truth domains.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import InitVar, dataclass, field, replace
from pathlib import Path
from typing import Any, Literal, NoReturn, Protocol

from oracle_lab.coding_isolation import PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS
from oracle_lab.jsonutil import canonical_json, sha256_bytes
from oracle_lab.sbx_observation import SbxV039Version

ObservationStatus = Literal["observed", "incomplete"]
ObservationOrigin = Literal["real", "synthetic_fixture"]
TruthDomain = Literal["real", "synthetic"]

_SAFE_PROBE_ID = re.compile(r"\Aobs_[0-9a-f]{32}\Z")
_SAFE_OPERATION = re.compile(r"\A[a-z][a-z0-9_]{0,63}\Z")
_SAFE_SANDBOX_NAME = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{1,199}\Z")
_SHA256 = re.compile(r"\A[0-9a-f]{64}\Z")
_MAX_OBSERVATIONS = 128
_MAX_ARGV_BYTES = 64 * 1024
_MAX_STREAM_BYTES = 1024 * 1024
_ARCHIVE_SCHEMA_VERSION = 1
_PAYLOAD_ISSUER = object()
_EXPECTED_PROVENANCE_SOURCES = {
    "version": ("version",),
    "inventory": ("initial_inventory",),
    "name_selected_inspect": (
        "initial_inventory",
        "initial_inspect",
        "verification_inventory",
        "verification_inspect",
    ),
}
_EXPECTED_OPERATION_ORDER = (
    "version",
    "initial_inventory",
    "initial_inspect",
    "verification_inventory",
    "verification_inspect",
)
_OBSERVATION_ARCHIVE_REASON_IDS = frozenset(
    {
        "sbx_probe_operation_invalid",
        "sbx_probe_argv_invalid",
        "sbx_probe_command_not_read_only",
        "sbx_probe_exit_status_invalid",
        "sbx_probe_result_flags_invalid",
        "sbx_probe_raw_observation_invalid",
        "sbx_probe_truth_domain_invalid",
        "sbx_probe_observations_invalid",
    }
)
_REPORT_REASON_IDS = (
    _OBSERVATION_ARCHIVE_REASON_IDS - {"sbx_probe_observations_invalid"}
) | frozenset(
    {
        "sbx_probe_executable_changed",
        "sbx_probe_executable_identity_unavailable",
        "sbx_probe_executable_not_executable",
        "sbx_probe_executable_not_regular",
        "sbx_probe_executable_unavailable",
        "sbx_probe_executable_unreadable",
        "sbx_probe_fixture_only",
        "sbx_probe_name_selected_view_inconsistent",
        "sbx_probe_provenance_field_invalid",
        "sbx_probe_provenance_sources_invalid",
        "sbx_probe_runner_origin_untrusted",
        "sbx_probe_runner_result_invalid",
        "sbx_probe_timestamp_invalid",
        *(f"sbx_v039_{kind}_schema_invalid" for kind in ("inspect", "inventory", "version")),
        *(
            f"sbx_probe_{operation}_{failure}"
            for operation in _EXPECTED_OPERATION_ORDER
            for failure in ("failed", "output_limited", "timed_out", "unavailable")
        ),
    }
)
_FORBIDDEN_PUBLIC_KEYS = frozenset(
    {
        "argv",
        "broker_executable_path",
        "directory",
        "image",
        "image_reference",
        "manifest_path",
        "name",
        "path",
        "raw",
        "raw_bytes",
        "stderr",
        "stderr_bytes",
        "stdout",
        "stdout_bytes",
        "template",
        "template_reference",
        "workspace",
    }
)


class SbxProbeError(RuntimeError):
    """Secret-free probe or archive failure with a stable reason ID."""

    __slots__ = ("reason_id",)

    def __init__(self, reason_id: str) -> None:
        self.reason_id = reason_id
        super().__init__(reason_id)


class SbxObservationPayloadError(RuntimeError):
    """Secret-free rejection while constructing a canonical archive payload."""

    __slots__ = ("reason_id",)

    def __init__(self, reason_id: str) -> None:
        self.reason_id = reason_id
        super().__init__(reason_id)


class SbxObservationLike(Protocol):
    """Legacy structural input surface; implementations are never trusted."""

    operation_id: str
    argv: Sequence[str]
    stdout: bytes
    stderr: bytes

    def to_public_dict(self) -> Mapping[str, Any]: ...


class SbxObservationReportLike(Protocol):
    """Legacy archive input surface retained for API compatibility."""

    probe_id: str
    observations: Sequence[SbxObservationLike]

    def to_public_dict(self) -> Mapping[str, Any]: ...


def is_safe_sbx_sandbox_name(value: object) -> bool:
    return type(value) is str and _SAFE_SANDBOX_NAME.fullmatch(value) is not None


def is_fixed_read_only_sbx_operation(
    operation_id: object,
    arguments: Sequence[object],
) -> bool:
    """Return whether an operation is one exact, supported read-only command."""

    if type(operation_id) is not str:
        return False
    try:
        values = tuple(arguments)
    except TypeError:
        return False
    if any(type(value) is not str for value in values):
        return False
    if operation_id == "version":
        return values == ("version",)
    if operation_id in {"initial_inventory", "verification_inventory"}:
        return values == ("ls", "--json")
    return (
        operation_id in {"initial_inspect", "verification_inspect"}
        and len(values) == 3
        and values[0] == "inspect"
        and is_safe_sbx_sandbox_name(values[1])
        and values[2] == "--json"
    )


@dataclass(frozen=True, slots=True)
class RawSbxCommandObservation:
    operation_id: str
    argv: tuple[str, ...]
    exit_code: int | None
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)
    timed_out: bool
    output_limited: bool
    truth_domain: TruthDomain
    observed_at: dt.datetime

    def __post_init__(self) -> None:
        _validate_observation(self)

    @property
    def succeeded(self) -> bool:
        return not self.timed_out and not self.output_limited and self.exit_code == 0

    def to_public_dict(self) -> dict[str, Any]:
        return _observation_public(self)


@dataclass(frozen=True, slots=True)
class SbxDerivedProvenanceEdge:
    """Explicit edge from one derived report field to raw operations."""

    derived_field: str
    source_operation_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_provenance_edge(self)

    def to_public_dict(self) -> dict[str, Any]:
        return _provenance_public(self)


def _new_provenance_edge(derived_field: str) -> SbxDerivedProvenanceEdge:
    return SbxDerivedProvenanceEdge(derived_field, _EXPECTED_PROVENANCE_SOURCES[derived_field])


@dataclass(frozen=True, slots=True)
class SbxNoModelObservationReport:
    probe_id: str
    status: ObservationStatus
    reason_id: str | None
    evidence_origin: ObservationOrigin
    observed_at: dt.datetime
    broker_executable_path: str
    broker_executable_sha256: str
    version: SbxV039Version | None
    inventory_sandbox_count: int | None
    name_selected_inspect_observed: bool
    cleanup_performed: bool
    cleanup_confirmed: bool | None
    observations: tuple[RawSbxCommandObservation, ...]
    provenance_edges: tuple[SbxDerivedProvenanceEdge, ...]
    production_evidence_blockers: tuple[str, ...] = PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS
    schema_version: int = 1
    _issuance_seal: str | None = field(default=None, repr=False, compare=False)
    _issue_authority: InitVar[object | None] = None

    def __post_init__(self, _issue_authority: object | None) -> None:
        _validate_report(self)
        if self.evidence_origin == "real":
            if _is_real_report_authority(_issue_authority):
                assert callable(_issue_authority)
                object.__setattr__(self, "_issuance_seal", _issue_authority(self))
            elif not self._real_evidence_seal_valid():
                raise SbxProbeError("sbx_probe_real_origin_unissued")
        elif self._issuance_seal is not None or _issue_authority is not None:
            raise SbxProbeError("sbx_probe_synthetic_origin_token_invalid")

    @property
    def ready(self) -> bool:
        return False

    @property
    def safe_to_start_worker(self) -> bool:
        return False

    @property
    def attestation_issued(self) -> bool:
        return False

    @property
    def atomic_instance_binding_proven(self) -> bool:
        return False

    def _real_evidence_seal_valid(self) -> bool:
        return _validate_real_evidence_seal(self, self._issuance_seal)

    def to_public_dict(self) -> dict[str, Any]:
        return _report_public(self)


def _make_real_report_authority() -> tuple[Any, Any, Any]:
    """Keep the issuance capability opaque to ordinary report consumers.

    Code executing inside this trusted Python process can still subvert private
    implementation details.  The security boundary here is the configured
    runner path, archive input, and serialized evidence—not hostile in-process
    code.
    """

    key = os.urandom(32)

    def authority(report: SbxNoModelObservationReport) -> str:
        return hmac.new(
            key,
            canonical_json(_report_public(report)).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def issue(**values: Any) -> SbxNoModelObservationReport:
        return SbxNoModelObservationReport(
            **values,
            evidence_origin="real",
            _issue_authority=authority,
        )

    def is_authority(candidate: object) -> bool:
        return candidate is authority

    def validate(report: SbxNoModelObservationReport, candidate: object) -> bool:
        return type(candidate) is str and hmac.compare_digest(candidate, authority(report))

    return issue, is_authority, validate


(
    _issue_real_sbx_observation_report,
    _is_real_report_authority,
    _validate_real_evidence_seal,
) = _make_real_report_authority()


# Preserve the established import/pickle identity while keeping the concrete
# implementations beside their single validator and serializer.
for _probe_public_type in (
    SbxProbeError,
    RawSbxCommandObservation,
    SbxDerivedProvenanceEdge,
    SbxNoModelObservationReport,
):
    _probe_public_type.__module__ = "oracle_lab.sbx_probe"
for _archive_public_type in (SbxObservationLike, SbxObservationReportLike):
    _archive_public_type.__module__ = "oracle_lab.sbx_observation_archive"


@dataclass(frozen=True, slots=True)
class SbxArchiveFile:
    filename: str
    content: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class CanonicalSbxObservationPayload:
    probe_id: str
    raw_files: tuple[SbxArchiveFile, ...]
    manifest_bytes: bytes = field(repr=False)
    _issue_authority: InitVar[object | None] = None

    def __post_init__(self, _issue_authority: object | None) -> None:
        if _issue_authority is not _PAYLOAD_ISSUER:
            raise SbxObservationPayloadError("sbx_observation_archive_report_invalid")


def _validate_observation(observation: RawSbxCommandObservation) -> None:
    if (
        type(observation.operation_id) is not str
        or _SAFE_OPERATION.fullmatch(observation.operation_id) is None
    ):
        raise SbxProbeError("sbx_probe_operation_invalid")
    if (
        type(observation.argv) is not tuple
        or not observation.argv
        or any(type(value) is not str or not value or "\x00" in value for value in observation.argv)
    ):
        raise SbxProbeError("sbx_probe_argv_invalid")
    if not is_fixed_read_only_sbx_operation(observation.operation_id, observation.argv[1:]):
        raise SbxProbeError("sbx_probe_command_not_read_only")
    try:
        argv_bytes = canonical_json(list(observation.argv)).encode("utf-8")
    except (TypeError, ValueError):
        raise SbxProbeError("sbx_probe_argv_invalid") from None
    if len(argv_bytes) > _MAX_ARGV_BYTES:
        raise SbxProbeError("sbx_probe_argv_invalid")
    if observation.exit_code is not None and (
        type(observation.exit_code) is not int or not -(2**31) <= observation.exit_code < 2**31
    ):
        raise SbxProbeError("sbx_probe_exit_status_invalid")
    if type(observation.timed_out) is not bool or type(observation.output_limited) is not bool:
        raise SbxProbeError("sbx_probe_result_flags_invalid")
    if (
        type(observation.stdout) is not bytes
        or type(observation.stderr) is not bytes
        or len(observation.stdout) > _MAX_STREAM_BYTES
        or len(observation.stderr) > _MAX_STREAM_BYTES
    ):
        raise SbxProbeError("sbx_probe_raw_observation_invalid")
    if type(observation.truth_domain) is not str or observation.truth_domain not in {
        "real",
        "synthetic",
    }:
        raise SbxProbeError("sbx_probe_truth_domain_invalid")
    if not _datetime_is_utc(observation.observed_at):
        raise SbxProbeError("sbx_probe_timestamp_invalid")


def _validate_provenance_edge(edge: SbxDerivedProvenanceEdge) -> None:
    derived_field = edge.derived_field
    if type(derived_field) is not str or derived_field not in _EXPECTED_PROVENANCE_SOURCES:
        raise SbxProbeError("sbx_probe_provenance_field_invalid")
    expected_sources = _EXPECTED_PROVENANCE_SOURCES[derived_field]
    source_operation_ids = edge.source_operation_ids
    if (
        type(source_operation_ids) is not tuple
        or any(type(value) is not str for value in source_operation_ids)
        or source_operation_ids != expected_sources
    ):
        raise SbxProbeError("sbx_probe_provenance_sources_invalid")


def _validate_report(report: SbxNoModelObservationReport) -> None:
    if type(report.probe_id) is not str or _SAFE_PROBE_ID.fullmatch(report.probe_id) is None:
        raise SbxProbeError("sbx_probe_id_invalid")
    if type(report.schema_version) is not int or report.schema_version != 1:
        raise SbxProbeError("sbx_probe_schema_invalid")
    if type(report.status) is not str or report.status not in {"observed", "incomplete"}:
        raise SbxProbeError("sbx_probe_status_invalid")
    if report.status == "observed" and (
        report.reason_id is not None
        or report.version is None
        or report.inventory_sandbox_count is None
        or not report.observations
    ):
        raise SbxProbeError("sbx_probe_observed_report_incomplete")
    if report.status == "incomplete" and (
        type(report.reason_id) is not str or not report.reason_id
    ):
        raise SbxProbeError("sbx_probe_incomplete_reason_missing")
    if report.reason_id is not None and (
        type(report.reason_id) is not str or report.reason_id not in _REPORT_REASON_IDS
    ):
        raise SbxProbeError("sbx_probe_reason_invalid")
    if report.inventory_sandbox_count is not None and (
        type(report.inventory_sandbox_count) is not int or report.inventory_sandbox_count < 0
    ):
        raise SbxProbeError("sbx_probe_inventory_summary_invalid")
    if type(report.name_selected_inspect_observed) is not bool:
        raise SbxProbeError("sbx_probe_inspect_summary_invalid")
    if type(report.evidence_origin) is not str or report.evidence_origin not in {
        "real",
        "synthetic_fixture",
    }:
        raise SbxProbeError("sbx_probe_evidence_origin_invalid")
    if not _datetime_is_utc(report.observed_at):
        raise SbxProbeError("sbx_probe_timestamp_invalid")
    if (
        type(report.broker_executable_path) is not str
        or len(report.broker_executable_path) > 4096
        or any(ord(character) < 0x20 for character in report.broker_executable_path)
        or type(report.broker_executable_sha256) is not str
        or _SHA256.fullmatch(report.broker_executable_sha256) is None
        or not Path(report.broker_executable_path).is_absolute()
    ):
        raise SbxProbeError("sbx_probe_broker_identity_invalid")
    if (
        type(report.production_evidence_blockers) is not tuple
        or any(type(value) is not str for value in report.production_evidence_blockers)
        or report.production_evidence_blockers != PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS
    ):
        raise SbxProbeError("sbx_probe_blocker_registry_changed")
    if (
        type(report.observations) is not tuple
        or len(report.observations) > _MAX_OBSERVATIONS
        or any(type(item) is not RawSbxCommandObservation for item in report.observations)
    ):
        raise SbxProbeError("sbx_probe_observations_invalid")
    if type(report.provenance_edges) is not tuple or any(
        type(edge) is not SbxDerivedProvenanceEdge for edge in report.provenance_edges
    ):
        raise SbxProbeError("sbx_probe_provenance_invalid")
    if report.version is not None and (
        type(report.version) is not SbxV039Version
        or type(report.version.version) is not str
        or report.version.version != "v0.39.0"
        or type(report.version.commit_sha) is not str
        or re.fullmatch(r"[0-9a-f]{40}", report.version.commit_sha) is None
    ):
        raise SbxProbeError("sbx_probe_version_invalid")
    if report.cleanup_performed is not False or report.cleanup_confirmed is not None:
        raise SbxProbeError("sbx_probe_read_only_cleanup_invalid")
    for observation in report.observations:
        _validate_observation(observation)
    if any(
        observation.argv[0] != report.broker_executable_path for observation in report.observations
    ):
        raise SbxProbeError("sbx_probe_argv_invalid")
    for edge in report.provenance_edges:
        _validate_provenance_edge(edge)
    if report.status == "observed" and any(
        not observation.succeeded for observation in report.observations
    ):
        raise SbxProbeError("sbx_probe_observed_report_incomplete")

    operation_ids = tuple(item.operation_id for item in report.observations)
    if len(operation_ids) != len(set(operation_ids)):
        raise SbxProbeError("sbx_probe_operation_duplicate")
    if operation_ids != _EXPECTED_OPERATION_ORDER[: len(operation_ids)]:
        raise SbxProbeError("sbx_probe_observations_invalid")
    if report.status == "observed":
        expected_count = 5 if report.name_selected_inspect_observed else 2
        if len(operation_ids) != expected_count:
            raise SbxProbeError("sbx_probe_observations_invalid")
    inspect_names = tuple(
        item.argv[2]
        for item in report.observations
        if item.operation_id in {"initial_inspect", "verification_inspect"}
    )
    if len(set(inspect_names)) > 1:
        raise SbxProbeError("sbx_probe_observations_invalid")
    expected_truth = "real" if report.evidence_origin == "real" else "synthetic"
    if any(item.truth_domain != expected_truth for item in report.observations):
        raise SbxProbeError("sbx_probe_truth_domain_origin_mismatch")
    edge_fields = tuple(edge.derived_field for edge in report.provenance_edges)
    if len(edge_fields) != len(set(edge_fields)):
        raise SbxProbeError("sbx_probe_provenance_field_duplicate")
    available_operations = frozenset(operation_ids)
    if any(
        not set(edge.source_operation_ids).issubset(available_operations)
        for edge in report.provenance_edges
    ):
        raise SbxProbeError("sbx_probe_provenance_source_missing")
    field_presence = {
        "version": report.version is not None,
        "inventory": report.inventory_sandbox_count is not None,
        "name_selected_inspect": report.name_selected_inspect_observed,
    }
    expected_edge_fields = tuple(
        field for field in _EXPECTED_PROVENANCE_SOURCES if field_presence[field]
    )
    if edge_fields != expected_edge_fields:
        raise SbxProbeError("sbx_probe_provenance_incomplete")


def _datetime_is_utc(value: object) -> bool:
    # Exact built-ins make ``isoformat`` deterministic and exclude callback tzinfo.
    if type(value) is not dt.datetime or type(value.tzinfo) is not dt.timezone:
        return False
    return value.utcoffset() == dt.timedelta(0)


def _observation_public(observation: RawSbxCommandObservation) -> dict[str, Any]:
    argv_bytes = canonical_json(list(observation.argv)).encode("utf-8")
    return {
        "operation_id": observation.operation_id,
        "argv_sha256": sha256_bytes(argv_bytes),
        "argv_count": len(observation.argv),
        "exit_code": observation.exit_code,
        "timed_out": observation.timed_out,
        "output_limited": observation.output_limited,
        "truth_domain": observation.truth_domain,
        "observed_at": observation.observed_at.isoformat(),
        "stdout_sha256": sha256_bytes(observation.stdout),
        "stdout_size_bytes": len(observation.stdout),
        "stderr_sha256": sha256_bytes(observation.stderr),
        "stderr_size_bytes": len(observation.stderr),
    }


def _provenance_public(edge: SbxDerivedProvenanceEdge) -> dict[str, Any]:
    return {
        "derived_field": edge.derived_field,
        "source_operation_ids": list(edge.source_operation_ids),
    }


def _report_public(report: SbxNoModelObservationReport) -> dict[str, Any]:
    version = None
    if report.version is not None:
        version = {
            "version": report.version.version,
            "commit_sha": report.version.commit_sha,
        }
    return {
        "schema_version": report.schema_version,
        "probe_id": report.probe_id,
        "status": report.status,
        "reason_id": report.reason_id,
        "ready": report.ready,
        "safe_to_start_worker": report.safe_to_start_worker,
        "attestation_issued": report.attestation_issued,
        "evidence_origin": report.evidence_origin,
        "observed_at": report.observed_at.isoformat(),
        "broker_executable_path_sha256": sha256_bytes(
            report.broker_executable_path.encode("utf-8")
        ),
        "broker_executable_sha256": report.broker_executable_sha256,
        "version": version,
        "inventory_sandbox_count": report.inventory_sandbox_count,
        "name_selected_inspect_observed": report.name_selected_inspect_observed,
        "atomic_instance_binding_proven": report.atomic_instance_binding_proven,
        "side_effecting_commands_attempted": False,
        "cleanup_performed": report.cleanup_performed,
        "cleanup_confirmed": report.cleanup_confirmed,
        "observations": [_observation_public(item) for item in report.observations],
        "provenance_edges": [_provenance_public(edge) for edge in report.provenance_edges],
        "production_evidence_blockers": list(report.production_evidence_blockers),
    }


def _json_snapshot(value: Any) -> Any | None:
    try:
        return json.loads(canonical_json(value))
    except Exception:
        return None


def _public_value_is_safe(value: Any) -> bool:
    if value is None or type(value) in {bool, int, float, str}:
        return True
    if isinstance(value, list):
        return all(_public_value_is_safe(item) for item in value)
    if type(value) is not dict:
        return False
    return all(
        type(key) is str
        and key.casefold() not in _FORBIDDEN_PUBLIC_KEYS
        and _public_value_is_safe(item)
        for key, item in value.items()
    )


def _payload_fail(reason_id: str) -> NoReturn:
    raise SbxObservationPayloadError(reason_id) from None


def _snapshot_report(report: SbxNoModelObservationReport) -> SbxNoModelObservationReport:
    version = report.version
    if version is not None and type(version) is not SbxV039Version:
        raise SbxProbeError("sbx_probe_version_invalid")
    observations = report.observations
    if type(observations) is not tuple or any(
        type(item) is not RawSbxCommandObservation for item in observations
    ):
        raise SbxProbeError("sbx_probe_observations_invalid")
    provenance_edges = report.provenance_edges
    if type(provenance_edges) is not tuple or any(
        type(item) is not SbxDerivedProvenanceEdge for item in provenance_edges
    ):
        raise SbxProbeError("sbx_probe_provenance_invalid")
    return replace(
        report,
        version=replace(version) if version is not None else None,
        observations=tuple(replace(item) for item in observations),
        provenance_edges=tuple(replace(item) for item in provenance_edges),
    )


def _archive_reason_for_probe_error(claimed_real: bool, reason_id: str) -> str:
    if claimed_real or reason_id == "sbx_probe_real_origin_unissued":
        return "sbx_observation_archive_real_origin_untrusted"
    if reason_id in _OBSERVATION_ARCHIVE_REASON_IDS:
        return "sbx_observation_archive_observation_invalid"
    if reason_id == "sbx_probe_broker_identity_invalid":
        return "sbx_observation_archive_public_metadata_invalid"
    return "sbx_observation_archive_public_metadata_mismatch"


def build_canonical_sbx_observation_payload(
    report: SbxObservationReportLike,
) -> CanonicalSbxObservationPayload:
    """Validate one exact report and freeze its complete archive representation."""

    if type(report) is not SbxNoModelObservationReport:
        _payload_fail("sbx_observation_archive_report_invalid")

    claimed_origin = report.evidence_origin
    claimed_real = type(claimed_origin) is str and claimed_origin == "real"
    failure_reason: str | None = None
    try:
        snapshot = _snapshot_report(report)
    except SbxProbeError as error:
        failure_reason = _archive_reason_for_probe_error(claimed_real, error.reason_id)
        snapshot = None
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        failure_reason = (
            "sbx_observation_archive_real_origin_untrusted"
            if claimed_real
            else "sbx_observation_archive_public_metadata_mismatch"
        )
        snapshot = None
    if failure_reason is not None or snapshot is None:
        _payload_fail(failure_reason or "sbx_observation_archive_report_invalid")

    raw_files: list[SbxArchiveFile] = []
    raw_artifacts: list[dict[str, Any]] = []
    for index, observation in enumerate(snapshot.observations):
        prefix = f"{index:03d}-{observation.operation_id}"
        argv_bytes = canonical_json(list(observation.argv)).encode("utf-8")
        for artifact_role, filename, content in (
            ("argv", f"{prefix}.argv.json", argv_bytes),
            ("stdout", f"{prefix}.stdout.bin", observation.stdout),
            ("stderr", f"{prefix}.stderr.bin", observation.stderr),
        ):
            raw_files.append(SbxArchiveFile(filename=filename, content=content))
            raw_artifacts.append(
                {
                    "file": filename,
                    "sha256": sha256_bytes(content),
                    "size_bytes": len(content),
                    "artifact_role": artifact_role,
                    "provenance": {
                        "observation_index": index,
                        "operation_id": observation.operation_id,
                        "observed_at": observation.observed_at.isoformat(),
                        "truth_domain": observation.truth_domain,
                    },
                }
            )
    manifest = {
        **_report_public(snapshot),
        "archive_schema_version": _ARCHIVE_SCHEMA_VERSION,
        "raw_artifacts": raw_artifacts,
    }
    manifest_snapshot = _json_snapshot(manifest)
    if not isinstance(manifest_snapshot, dict) or not _public_value_is_safe(manifest_snapshot):
        _payload_fail("sbx_observation_archive_manifest_invalid")
    try:
        manifest_bytes = (canonical_json(manifest_snapshot) + "\n").encode("utf-8")
    except Exception:
        manifest_bytes = None
    if manifest_bytes is None:
        _payload_fail("sbx_observation_archive_manifest_invalid")
    return CanonicalSbxObservationPayload(
        probe_id=snapshot.probe_id,
        raw_files=tuple(raw_files),
        manifest_bytes=manifest_bytes,
        _issue_authority=_PAYLOAD_ISSUER,
    )


__all__ = [
    "CanonicalSbxObservationPayload",
    "ObservationOrigin",
    "ObservationStatus",
    "RawSbxCommandObservation",
    "SbxArchiveFile",
    "SbxDerivedProvenanceEdge",
    "SbxNoModelObservationReport",
    "SbxObservationLike",
    "SbxObservationPayloadError",
    "SbxObservationReportLike",
    "SbxProbeError",
    "TruthDomain",
    "build_canonical_sbx_observation_payload",
    "is_fixed_read_only_sbx_operation",
    "is_safe_sbx_sandbox_name",
]
