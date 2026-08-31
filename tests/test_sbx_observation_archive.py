from __future__ import annotations

import datetime as dt
import errno
import hashlib
import json
import os
import stat
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from oracle_lab import sbx_observation_archive as archive_module
from oracle_lab import sbx_observation_payload as payload_module
from oracle_lab.coding_isolation import PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS
from oracle_lab.jsonutil import canonical_json
from oracle_lab.sbx_observation import SbxV039Version
from oracle_lab.sbx_observation_archive import (
    HardenedSbxObservationArchive,
    SbxObservationArchiveError,
)
from oracle_lab.sbx_observation_payload import SbxObservationPayloadError
from oracle_lab.sbx_probe import (
    RawSbxCommandObservation,
    SbxDerivedProvenanceEdge,
    SbxNoModelObservationReport,
    SbxProbeError,
)

_PROBE_ID = "obs_0123456789abcdef0123456789abcdef"


def test_archive_public_exports_and_type_identity_remain_stable() -> None:
    assert archive_module.__all__ == [
        "HardenedSbxObservationArchive",
        "SbxObservationArchiveError",
        "SbxObservationArchiveRecord",
        "SbxObservationLike",
        "SbxObservationReportLike",
    ]
    assert archive_module.SbxObservationLike.__module__ == archive_module.__name__
    assert archive_module.SbxObservationReportLike.__module__ == archive_module.__name__
    assert SbxObservationArchiveError.__bases__ == (RuntimeError,)
    assert SbxObservationPayloadError.__bases__ == (RuntimeError,)
    assert SbxProbeError.__bases__ == (RuntimeError,)
    assert typing.get_type_hints(archive_module.SbxObservationLike)
    assert typing.get_type_hints(archive_module.SbxObservationLike.to_public_dict)
    assert typing.get_type_hints(archive_module.SbxObservationReportLike)
    assert typing.get_type_hints(archive_module.SbxObservationReportLike.to_public_dict)


@dataclass
class _Observation:
    operation_id: str
    stdout: bytes
    stderr: bytes
    truth_domain: str = "synthetic"
    argv: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.argv:
            self.argv = ("/fixed/sbx", self.operation_id)

    def to_public_dict(self) -> dict[str, Any]:
        argv_bytes = canonical_json(list(self.argv)).encode("utf-8")
        return {
            "operation_id": self.operation_id,
            "argv_sha256": hashlib.sha256(argv_bytes).hexdigest(),
            "argv_count": len(self.argv),
            "exit_code": 0,
            "timed_out": False,
            "output_limited": False,
            "truth_domain": self.truth_domain,
            "observed_at": "2026-08-31T07:30:00+00:00",
            "stdout_sha256": hashlib.sha256(self.stdout).hexdigest(),
            "stdout_size_bytes": len(self.stdout),
            "stderr_sha256": hashlib.sha256(self.stderr).hexdigest(),
            "stderr_size_bytes": len(self.stderr),
        }


@dataclass
class _Report:
    observations: tuple[_Observation, ...]
    probe_id: str = _PROBE_ID

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "probe_id": self.probe_id,
            "status": "observed",
            "reason_id": None,
            "ready": False,
            "safe_to_start_worker": False,
            "attestation_issued": False,
            "side_effecting_commands_attempted": False,
            "cleanup_performed": False,
            "cleanup_confirmed": None,
            "atomic_instance_binding_proven": False,
            "evidence_origin": "synthetic_fixture",
            "observed_at": "2026-08-31T07:30:00+00:00",
            "broker_executable_path_sha256": "e" * 64,
            "broker_executable_sha256": "f" * 64,
            "version": {"version": "v0.39.0", "commit_sha": "d" * 40},
            "inventory_sandbox_count": 0,
            "name_selected_inspect_observed": False,
            "observations": [item.to_public_dict() for item in self.observations],
            "provenance_edges": [
                {"derived_field": "version", "source_operation_ids": ["version"]},
                {
                    "derived_field": "inventory",
                    "source_operation_ids": ["initial_inventory"],
                },
            ],
            "production_evidence_blockers": list(PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS),
        }


def _report(
    *,
    secret: bytes = b"credential-shaped-secret\x00\xff",
) -> SbxNoModelObservationReport:
    observed_at = dt.datetime(2026, 8, 31, 7, 30, tzinfo=dt.UTC)
    observations = (
        RawSbxCommandObservation(
            operation_id="version",
            argv=("/fixed/sbx", "version"),
            exit_code=0,
            stdout=b"sbx version: v0.39.0\n",
            stderr=b"",
            timed_out=False,
            output_limited=False,
            truth_domain="synthetic",
            observed_at=observed_at,
        ),
        RawSbxCommandObservation(
            operation_id="initial_inventory",
            argv=("/fixed/sbx", "ls", "--json"),
            exit_code=0,
            stdout=b"created\r\n",
            stderr=secret,
            timed_out=False,
            output_limited=False,
            truth_domain="synthetic",
            observed_at=observed_at,
        ),
    )
    return SbxNoModelObservationReport(
        probe_id=_PROBE_ID,
        status="observed",
        reason_id=None,
        evidence_origin="synthetic_fixture",
        observed_at=observed_at,
        broker_executable_path="/fixed/sbx",
        broker_executable_sha256="f" * 64,
        version=SbxV039Version(version="v0.39.0", commit_sha="d" * 40),
        inventory_sandbox_count=0,
        name_selected_inspect_observed=False,
        cleanup_performed=False,
        cleanup_confirmed=None,
        observations=observations,
        provenance_edges=(
            SbxDerivedProvenanceEdge("version", ("version",)),
            SbxDerivedProvenanceEdge("inventory", ("initial_inventory",)),
        ),
    )


def _structural_report(
    *,
    secret: bytes = b"credential-shaped-secret\x00\xff",
) -> _Report:
    return _Report(
        observations=(
            _Observation("version", b"sbx version: v0.39.0\n", b""),
            _Observation(
                "initial_inventory",
                b"created\r\n",
                secret,
                argv=("/fixed/sbx", "ls", "--json"),
            ),
        )
    )


def test_archive_preserves_exact_bytes_and_commits_canonical_public_manifest(
    tmp_path: Path,
) -> None:
    report = _report()
    root = tmp_path / "archive"

    record = HardenedSbxObservationArchive(root).write(report)

    raw_files = sorted(path for path in record.directory.iterdir() if path.name != "manifest.json")
    assert len(raw_files) == 6
    assert (record.directory / "000-version.stdout.bin").read_bytes() == b"sbx version: v0.39.0\n"
    assert (record.directory / "000-version.argv.json").read_bytes() == canonical_json(
        list(report.observations[0].argv)
    ).encode("utf-8")
    assert (
        record.directory / "001-initial_inventory.stderr.bin"
    ).read_bytes() == report.observations[1].stderr
    manifest_bytes = record.manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    assert manifest_bytes.endswith(b"\n")
    assert manifest_bytes == (canonical_json(manifest) + "\n").encode("utf-8")
    assert b"credential-shaped-secret" not in manifest_bytes
    assert "argv" not in manifest["observations"][0]
    assert "stdout" not in manifest["observations"][0]
    assert "stderr" not in manifest["observations"][0]
    assert len(manifest["raw_artifacts"]) == 6
    assert manifest["raw_artifacts"][0]["provenance"] == {
        "observation_index": 0,
        "observed_at": "2026-08-31T07:30:00+00:00",
        "operation_id": "version",
        "truth_domain": "synthetic",
    }
    assert record.manifest_sha256 == hashlib.sha256(manifest_bytes).hexdigest()
    assert (
        record.manifest_sha256 == "3f7a5067ccaa7e510bcbec1d001a68c1e380abd6f7aa1f9f13cdcb3a6d3df2ff"
    )
    assert record.raw_file_count == 6
    expected_raw = {
        "000-version.argv.json": canonical_json(["/fixed/sbx", "version"]).encode(),
        "000-version.stdout.bin": b"sbx version: v0.39.0\n",
        "000-version.stderr.bin": b"",
        "001-initial_inventory.argv.json": canonical_json(["/fixed/sbx", "ls", "--json"]).encode(),
        "001-initial_inventory.stdout.bin": b"created\r\n",
        "001-initial_inventory.stderr.bin": b"credential-shaped-secret\x00\xff",
    }
    assert {path.name: path.read_bytes() for path in raw_files} == expected_raw
    assert [item["file"] for item in manifest["raw_artifacts"]] == list(expected_raw)
    assert all(
        item["sha256"] == hashlib.sha256(expected_raw[item["file"]]).hexdigest()
        and item["size_bytes"] == len(expected_raw[item["file"]])
        for item in manifest["raw_artifacts"]
    )
    public_record = record.to_public_dict()
    assert "directory" not in public_record
    assert "manifest_path" not in public_record
    assert str(record.directory) not in json.dumps(public_record, sort_keys=True)
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE(record.directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(record.manifest_path.stat().st_mode) == 0o600
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in raw_files)
    assert not (root / f".{_PROBE_ID}.staging").exists()


def test_writer_accepts_the_real_sbx_report_interface(tmp_path: Path) -> None:
    observed_at = dt.datetime(2026, 8, 31, 7, 30, tzinfo=dt.UTC)
    observation = RawSbxCommandObservation(
        operation_id="version",
        argv=("/fixed/sbx", "version"),
        exit_code=0,
        stdout=b"sbx version: v0.39.0\n",
        stderr=b"",
        timed_out=False,
        output_limited=False,
        truth_domain="synthetic",
        observed_at=observed_at,
    )
    report = SbxNoModelObservationReport(
        probe_id=_PROBE_ID,
        status="incomplete",
        reason_id="sbx_probe_fixture_only",
        evidence_origin="synthetic_fixture",
        observed_at=observed_at,
        broker_executable_path="/fixed/sbx",
        broker_executable_sha256="f" * 64,
        version=None,
        inventory_sandbox_count=None,
        name_selected_inspect_observed=False,
        cleanup_performed=False,
        cleanup_confirmed=None,
        observations=(observation,),
        provenance_edges=(),
    )

    record = HardenedSbxObservationArchive(tmp_path / "archive").write(report)

    assert (record.directory / "000-version.stdout.bin").read_bytes() == observation.stdout
    assert (record.directory / "000-version.argv.json").read_bytes() == canonical_json(
        list(observation.argv)
    ).encode("utf-8")
    assert json.loads(record.manifest_path.read_bytes())["probe_id"] == _PROBE_ID


def test_archive_preserves_whitespace_and_malformed_formatting_byte_for_byte(
    tmp_path: Path,
) -> None:
    raw_stdout = b" \n\t```broken\r\n{x\x00\xff"
    raw_stderr = b"\r\n  malformed: [\n"
    observed_at = dt.datetime(2026, 8, 31, 7, 30, tzinfo=dt.UTC)
    observation = RawSbxCommandObservation(
        operation_id="version",
        argv=("/fixed/sbx", "version"),
        exit_code=1,
        stdout=raw_stdout,
        stderr=raw_stderr,
        timed_out=False,
        output_limited=False,
        truth_domain="synthetic",
        observed_at=observed_at,
    )
    report = SbxNoModelObservationReport(
        probe_id=_PROBE_ID,
        status="incomplete",
        reason_id="sbx_probe_fixture_only",
        evidence_origin="synthetic_fixture",
        observed_at=observed_at,
        broker_executable_path="/fixed/sbx",
        broker_executable_sha256="f" * 64,
        version=None,
        inventory_sandbox_count=None,
        name_selected_inspect_observed=False,
        cleanup_performed=False,
        cleanup_confirmed=None,
        observations=(observation,),
        provenance_edges=(),
    )

    record = HardenedSbxObservationArchive(tmp_path / "archive").write(report)

    assert (record.directory / "000-version.stdout.bin").read_bytes() == raw_stdout
    assert (record.directory / "000-version.stderr.bin").read_bytes() == raw_stderr


def test_archive_is_atomic_no_replace_and_probe_id_stays_write_once(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    archive = HardenedSbxObservationArchive(root)
    record = archive.write(_report(secret=b"first"))
    original_manifest = record.manifest_path.read_bytes()

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_not_write_once",
    ):
        archive.write(_report(secret=b"second"))

    assert record.manifest_path.read_bytes() == original_manifest
    assert not any(path.read_bytes() == b"second" for path in record.directory.glob("*.bin"))


def test_staging_permission_failure_is_not_misreported_as_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_mkdir = archive_module._mkdir_at

    def deny_staging(parent_fd: int, name: str) -> tuple[bool, int | None]:
        if name.startswith(f".{_PROBE_ID}.staging"):
            return False, errno.EPERM
        return original_mkdir(parent_fd, name)

    monkeypatch.setattr(archive_module, "_mkdir_at", deny_staging)

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_staging_create_failed",
    ):
        HardenedSbxObservationArchive(tmp_path / "archive").write(_report())


def test_atomic_commit_does_not_replace_destination_created_during_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "archive"
    original_rename = archive_module._atomic_rename_noreplace

    def race(parent_fd: int, source: str, destination: str) -> None:
        os.mkdir(destination, mode=0o700, dir_fd=parent_fd)
        original_rename(parent_fd, source, destination)

    monkeypatch.setattr(archive_module, "_atomic_rename_noreplace", race)

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_not_write_once",
    ):
        HardenedSbxObservationArchive(root).write(_report())

    assert (root / _PROBE_ID).is_dir()
    assert list((root / _PROBE_ID).iterdir()) == []
    assert (root / f".{_PROBE_ID}.staging").is_dir()


def test_archive_rejects_symlinked_ancestor_without_writing_through_it(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_root_invalid",
    ):
        HardenedSbxObservationArchive(linked_parent / "archive").write(_report())

    assert list(real_parent.iterdir()) == []


def test_archive_closes_child_dirfd_when_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_try_open = archive_module._try_open_directory
    original_close = archive_module._close
    child_descriptors: list[int] = []
    closed_descriptors: list[int] = []

    def record_child(name: str, *, dir_fd: int | None) -> tuple[int | None, int | None]:
        descriptor, error_number = original_try_open(name, dir_fd=dir_fd)
        if descriptor is not None:
            child_descriptors.append(descriptor)
        return descriptor, error_number

    def reject_child(descriptor: int) -> os.stat_result:
        if child_descriptors and descriptor == child_descriptors[0]:
            raise SbxObservationArchiveError("sbx_observation_archive_root_invalid")
        return archive_module.os.fstat(descriptor)

    def record_close(descriptor: int | None) -> None:
        if descriptor is not None:
            closed_descriptors.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(archive_module, "_try_open_directory", record_child)
    monkeypatch.setattr(archive_module, "_fstat_directory", reject_child)
    monkeypatch.setattr(archive_module, "_close", record_close)

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_root_invalid",
    ):
        archive_module._open_archive_root(tmp_path / "archive")

    assert child_descriptors[0] in closed_descriptors
    assert len(set(closed_descriptors)) == 2


def test_archive_refuses_existing_final_symlink_and_leaves_target_unchanged(
    tmp_path: Path,
) -> None:
    root = tmp_path / "archive"
    root.mkdir(mode=0o700)
    target = tmp_path / "target"
    target.mkdir()
    final = root / _PROBE_ID
    final.symlink_to(target, target_is_directory=True)

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_not_write_once",
    ):
        HardenedSbxObservationArchive(root).write(_report())

    assert final.is_symlink()
    assert list(target.iterdir()) == []


def test_public_raw_field_is_rejected_before_archive_root_is_created(tmp_path: Path) -> None:
    report = _report()
    object.__setattr__(report, "broker_executable_path", "/secret/\nraw-stderr")
    root = tmp_path / "archive"

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_public_metadata_invalid",
    ):
        HardenedSbxObservationArchive(root).write(report)

    assert not root.exists()


@pytest.mark.parametrize(
    "reason_id",
    (
        "sbx_probe_credential_shaped_secret",
        "sbx_probe_ghp_deadbeef0123456789",
        "sbx_probe_observations_invalid",
        "sbx_probe_sk_live_deadbeef",
    ),
)
def test_unissued_reason_id_is_rejected_before_archive_root_is_created(
    tmp_path: Path,
    reason_id: str,
) -> None:
    report = _report()
    object.__setattr__(report, "status", "incomplete")
    object.__setattr__(report, "reason_id", reason_id)
    root = tmp_path / "archive"

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_public_metadata_mismatch",
    ) as captured:
        HardenedSbxObservationArchive(root).write(report)

    assert "deadbeef" not in str(captured.value)
    assert "credential_shaped_secret" not in str(captured.value)
    assert not root.exists()


def test_archive_boundary_rejects_authority_and_exact_path_claims(tmp_path: Path) -> None:
    report = _report()
    object.__setattr__(report, "status", "passed")
    object.__setattr__(report, "broker_executable_path", "/secret/path")
    root = tmp_path / "archive"
    with pytest.raises(SbxObservationArchiveError):
        HardenedSbxObservationArchive(root).write(report)

    assert not root.exists()


@pytest.mark.parametrize(
    ("field", "secret_value"),
    [
        ("exit_code", "secret-exit"),
        ("timed_out", "secret-timeout"),
        ("output_limited", "secret-limit"),
        ("observed_at", "secret-timestamp"),
    ],
)
def test_observation_scalar_carriers_are_type_checked_without_leaking(
    tmp_path: Path,
    field: str,
    secret_value: str,
) -> None:
    report = _report()
    object.__setattr__(report.observations[0], field, secret_value)
    with pytest.raises(SbxObservationArchiveError) as captured:
        HardenedSbxObservationArchive(tmp_path / "archive").write(report)

    assert secret_value not in str(captured.value)
    assert not (tmp_path / "archive").exists()


def test_structural_report_cannot_launder_synthetic_bytes_as_real(tmp_path: Path) -> None:
    report = _report()
    object.__setattr__(report, "evidence_origin", "real")
    for observation in report.observations:
        object.__setattr__(observation, "truth_domain", "real")
    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_real_origin_untrusted",
    ):
        HardenedSbxObservationArchive(tmp_path / "archive").write(report)


def test_archive_rejects_mutating_argv_even_when_public_flag_claims_read_only(
    tmp_path: Path,
) -> None:
    report = _report()
    object.__setattr__(
        report.observations[0],
        "argv",
        ("/fixed/sbx", "rm", "--force", "sandbox"),
    )

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_observation_invalid",
    ):
        HardenedSbxObservationArchive(tmp_path / "archive").write(report)


def test_archive_rejects_extra_argument_on_otherwise_read_only_command(tmp_path: Path) -> None:
    report = _report()
    object.__setattr__(
        report.observations[0],
        "argv",
        ("/fixed/sbx", "version", "--json"),
    )

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_observation_invalid",
    ):
        HardenedSbxObservationArchive(tmp_path / "archive").write(report)


def test_archive_rejects_arbitrary_protocol_even_when_metadata_is_well_formed(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_report_invalid",
    ):
        HardenedSbxObservationArchive(tmp_path / "archive").write(_structural_report())


def test_archive_rejects_tampered_provenance_before_creating_root(tmp_path: Path) -> None:
    report = _report()
    object.__setattr__(report, "provenance_edges", report.provenance_edges[:1])
    root = tmp_path / "archive"

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_public_metadata_mismatch",
    ) as captured:
        HardenedSbxObservationArchive(root).write(report)

    assert not root.exists()
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_archive_rejects_provenance_string_subclass_without_callbacks(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    secret = "credential-shaped-secret"

    class DeceptiveSource(str):
        def __eq__(self, _other: object) -> bool:
            calls.append("eq")
            return True

        def __hash__(self) -> int:
            calls.append("hash")
            return hash("version")

    report = _report()
    object.__setattr__(
        report.provenance_edges[0],
        "source_operation_ids",
        (DeceptiveSource(secret),),
    )
    root = tmp_path / "archive"

    with pytest.raises(SbxObservationArchiveError) as captured:
        HardenedSbxObservationArchive(root).write(report)

    assert calls == []
    assert secret not in str(captured.value)
    assert not root.exists()


def test_archive_rejects_swapped_or_reordered_canonical_provenance(tmp_path: Path) -> None:
    swapped = _report()
    object.__setattr__(
        swapped.provenance_edges[0],
        "source_operation_ids",
        ("initial_inventory",),
    )
    reordered = _report()
    object.__setattr__(
        reordered,
        "provenance_edges",
        tuple(reversed(reordered.provenance_edges)),
    )

    for index, report in enumerate((swapped, reordered)):
        root = tmp_path / f"archive-{index}"
        with pytest.raises(
            SbxObservationArchiveError,
            match="sbx_observation_archive_public_metadata_mismatch",
        ):
            HardenedSbxObservationArchive(root).write(report)
        assert not root.exists()


def test_archive_rejects_reordered_observations_before_creating_root(tmp_path: Path) -> None:
    report = _report()
    object.__setattr__(report, "observations", tuple(reversed(report.observations)))
    root = tmp_path / "archive"

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_observation_invalid",
    ):
        HardenedSbxObservationArchive(root).write(report)

    assert not root.exists()


def test_archive_payload_uses_one_immutable_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report(secret=b"before-snapshot")
    original_sha256 = payload_module.sha256_bytes
    mutated = False

    def mutate_original_after_snapshot(content: bytes) -> str:
        nonlocal mutated
        if not mutated:
            mutated = True
            object.__setattr__(report.observations[1], "stderr", b"after-snapshot")
        return original_sha256(content)

    monkeypatch.setattr(payload_module, "sha256_bytes", mutate_original_after_snapshot)

    record = HardenedSbxObservationArchive(tmp_path / "archive").write(report)

    assert mutated is True
    assert (record.directory / "001-initial_inventory.stderr.bin").read_bytes() == (
        b"before-snapshot"
    )


def test_archive_rejects_regular_file_owner_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_fstat = archive_module.os.fstat

    def foreign_file_owner(descriptor: int) -> os.stat_result:
        details = original_fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            return details
        values = list(details)
        values[stat.ST_UID] = os.geteuid() + 1
        return os.stat_result(values)

    monkeypatch.setattr(archive_module.os, "fstat", foreign_file_owner)
    root = tmp_path / "archive"

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_write_failed",
    ):
        HardenedSbxObservationArchive(root).write(_report())

    assert not (root / _PROBE_ID).exists()


def test_archive_rejects_group_writable_root_before_staging(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    root.mkdir(mode=0o770)
    root.chmod(0o770)

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_root_permissions_unsafe",
    ):
        HardenedSbxObservationArchive(root).write(_report())

    assert list(root.iterdir()) == []


def test_preexisting_staging_entry_is_preserved_and_reported_as_collision(
    tmp_path: Path,
) -> None:
    root = tmp_path / "archive"
    root.mkdir(mode=0o700)
    staging = root / f".{_PROBE_ID}.staging"
    staging.mkdir(mode=0o700)
    marker = staging / "marker"
    marker.write_bytes(b"preexisting")

    with pytest.raises(
        SbxObservationArchiveError,
        match="sbx_observation_archive_staging_collision",
    ):
        HardenedSbxObservationArchive(root).write(_report())

    assert marker.read_bytes() == b"preexisting"
    assert not (root / _PROBE_ID).exists()


def test_unhashable_evidence_origin_fails_with_stable_archive_error(tmp_path: Path) -> None:
    report = _report()
    object.__setattr__(report, "evidence_origin", [])
    with pytest.raises(SbxObservationArchiveError) as captured:
        HardenedSbxObservationArchive(tmp_path / "archive").write(report)

    assert captured.value.reason_id == "sbx_observation_archive_public_metadata_mismatch"


def test_inherited_failure_text_is_not_retained_by_archive_exception(tmp_path: Path) -> None:
    secret = "credential-shaped-secret"
    calls: list[str] = []

    class _ExplodingReport(_Report):
        def to_public_dict(self) -> dict[str, Any]:
            calls.append("called")
            raise RuntimeError(secret)

    report = _ExplodingReport(observations=(_Observation("version", b"", b""),))

    with pytest.raises(SbxObservationArchiveError) as captured:
        HardenedSbxObservationArchive(tmp_path / "archive").write(report)

    error = captured.value
    assert secret not in str(error)
    assert secret not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert calls == []


def test_os_failure_text_is_not_retained_by_archive_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "credential-shaped-secret"
    original_stat = archive_module.os.stat

    def failing_stat(
        path: str,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        if path == _PROBE_ID and dir_fd is not None and not follow_symlinks:
            raise OSError(secret)
        return original_stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(archive_module.os, "stat", failing_stat)

    with pytest.raises(SbxObservationArchiveError) as captured:
        HardenedSbxObservationArchive(tmp_path / "archive").write(_report())

    error = captured.value
    assert secret not in str(error)
    assert secret not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None


def test_relative_archive_root_is_rejected_fail_closed(tmp_path: Path) -> None:
    previous = Path.cwd()
    try:
        os.chdir(tmp_path)
        with pytest.raises(
            SbxObservationArchiveError,
            match="sbx_observation_archive_root_invalid",
        ):
            HardenedSbxObservationArchive(Path("relative-archive")).write(_report())
    finally:
        os.chdir(previous)

    assert not (tmp_path / "relative-archive").exists()
