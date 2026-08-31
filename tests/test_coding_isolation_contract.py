from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from oracle_lab.coding_isolation import (
    REQUIRED_ISOLATION_CAPABILITIES,
    CodingIsolationError,
    CodingWorkerIsolationBinding,
    IsolationAttestation,
    IsolationRunFailed,
    IsolationRunRequest,
    IsolationRunResult,
    receipt_sha256,
    require_conforming_binding,
)


def _passing_receipt() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "passed",
        "observed_at": "2026-08-31T00:00:00+00:00",
        "checks": [
            {
                "id": capability,
                "status": "passed",
                "evidence": {"fixture_observation": capability},
            }
            for capability in sorted(REQUIRED_ISOLATION_CAPABILITIES)
        ],
    }


def _attestation(
    *,
    capabilities: tuple[str, ...] | None = None,
    receipt: Mapping[str, Any] | None = None,
    conformance_receipt_sha256: str | None = None,
    broker_executable_path: str = "/usr/local/bin/oracle-isolation-broker",
) -> IsolationAttestation:
    receipt_document = dict(receipt or _passing_receipt())
    return IsolationAttestation(
        backend="fixture-microvm",
        broker_executable_path=broker_executable_path,
        broker_executable_sha256="1" * 64,
        client_version="fixture-client-1",
        server_version="fixture-server-1",
        template_reference="fixture-template@sha256:" + "a" * 64,
        template_identity="sha256:" + "a" * 64,
        policy_sha256="2" * 64,
        conformance_suite_version="fixture-suite-1",
        conformance_receipt_sha256=(conformance_receipt_sha256 or receipt_sha256(receipt_document)),
        capabilities=capabilities or tuple(REQUIRED_ISOLATION_CAPABILITIES),
        receipt=receipt_document,
    )


def _request(*, workspace: Path = Path("/isolated/workspace")) -> IsolationRunRequest:
    return IsolationRunRequest(
        adapter="codex",
        workspace=workspace,
        command=("codex", "exec", "--json"),
        input_bytes=b"preserve exact input\x00\xff",
        environment={"TERM": "dumb"},
        timeout_seconds=30,
        max_output_bytes=4096,
        max_workspace_export_bytes=8192,
        max_workspace_entries=32,
    )


def _result(
    attestation: IsolationAttestation,
    **overrides: Any,
) -> IsolationRunResult:
    workspace_export = b"ORACLELAB-WORKSPACE-V1\x00" + (0).to_bytes(8, "big")
    values: dict[str, Any] = {
        "exit_code": 0,
        "stdout": b"worker output\x00\xff",
        "stderr": b"",
        "timed_out": False,
        "output_limited": False,
        "actual_command": ("/guest/bin/codex", "exec", "--json"),
        "guest_executable_path": "/guest/bin/codex",
        "guest_executable_version": "codex fixture-1",
        "guest_executable_version_status": "reported",
        "sandbox_id": "sandbox-fixture-01",
        "workspace_export": workspace_export,
        "workspace_export_sha256": hashlib.sha256(workspace_export).hexdigest(),
        "workspace_export_bytes": len(workspace_export),
        "workspace_export_entries": 0,
        "cleanup_confirmed": True,
        "attestation": attestation,
    }
    values.update(overrides)
    return IsolationRunResult(**values)


def _failure(
    attestation: IsolationAttestation,
    **overrides: Any,
) -> IsolationRunFailed:
    values: dict[str, Any] = {
        "exit_code": 17,
        "stdout": b"bounded stdout\x00\xff",
        "stderr": b"bounded stderr\x80",
        "timed_out": False,
        "output_limited": False,
        "actual_command": ("/guest/bin/codex", "exec", "--json"),
        "guest_executable_path": "/guest/bin/codex",
        "guest_executable_version": "codex fixture-1",
        "guest_executable_version_status": "reported",
        "sandbox_id": "sandbox-failed-fixture-01",
        "cleanup_confirmed": True,
        "attestation": attestation,
        "max_output_bytes": 4096,
    }
    values.update(overrides)
    return IsolationRunFailed(**values)


class _FakeBinding:
    def __init__(self, attestation: IsolationAttestation) -> None:
        self.attestation = attestation
        self.requests: list[IsolationRunRequest] = []

    def run(self, request: IsolationRunRequest) -> IsolationRunResult:
        self.requests.append(request)
        return _result(self.attestation)


def test_complete_attestation_and_binding_are_accepted() -> None:
    attestation = _attestation()
    binding = _FakeBinding(attestation)

    assert set(attestation.capabilities) == REQUIRED_ISOLATION_CAPABILITIES
    assert len(attestation.fingerprint) == 64
    assert isinstance(binding, CodingWorkerIsolationBinding)
    assert require_conforming_binding(binding) is binding


@pytest.mark.parametrize("missing", sorted(REQUIRED_ISOLATION_CAPABILITIES))
def test_attestation_rejects_each_missing_required_capability(missing: str) -> None:
    capabilities = tuple(REQUIRED_ISOLATION_CAPABILITIES - {missing})

    with pytest.raises(
        CodingIsolationError,
        match=rf"missing required capabilities: {missing}$",
    ):
        _attestation(capabilities=capabilities)


@pytest.mark.parametrize(
    ("receipt", "message"),
    [
        (
            {
                "schema_version": 1,
                "status": "failed",
                "observed_at": "2026-08-31T00:00:00+00:00",
                "checks": [{"id": "boundary", "status": "passed"}],
            },
            "conformance receipt did not pass",
        ),
        (
            {
                "schema_version": 1,
                "status": "passed",
                "observed_at": "2026-08-31T00:00:00+00:00",
                "checks": [{"id": "boundary", "status": "failed"}],
            },
            "every isolation conformance check must pass",
        ),
        (
            {
                "schema_version": 1,
                "status": "passed",
                "observed_at": "2026-08-31T00:00:00+00:00",
                "checks": [{"id": "boundary", "status": "skipped"}],
            },
            "every isolation conformance check must pass",
        ),
        (
            {
                "schema_version": 1,
                "status": "passed",
                "observed_at": "2026-08-31T00:00:00+00:00",
                "checks": [{"id": "boundary", "status": ""}],
            },
            "every isolation conformance check must pass",
        ),
        (
            {
                "schema_version": 1,
                "status": "passed",
                "observed_at": "2026-08-31T00:00:00+00:00",
                "checks": [],
            },
            "conformance receipt has no checks",
        ),
    ],
    ids=(
        "failed-receipt",
        "failed-check",
        "skipped-check",
        "empty-check-status",
        "empty-checks",
    ),
)
def test_attestation_rejects_nonpassing_conformance_receipts(
    receipt: Mapping[str, Any],
    message: str,
) -> None:
    with pytest.raises(CodingIsolationError, match=message):
        _attestation(receipt=receipt)


def test_attestation_rejects_receipt_hash_mismatch() -> None:
    with pytest.raises(CodingIsolationError, match="receipt hash mismatch"):
        _attestation(conformance_receipt_sha256="f" * 64)


def test_attestation_rejects_claimed_capability_without_its_own_check() -> None:
    receipt = _passing_receipt()
    receipt["checks"] = receipt["checks"][1:]

    with pytest.raises(CodingIsolationError, match="lack passing conformance checks"):
        _attestation(receipt=receipt)


@pytest.mark.parametrize(
    "evidence",
    [None, {}, [], "fixture observation"],
    ids=("missing", "empty", "list", "text"),
)
def test_attestation_requires_nonempty_evidence_mapping_for_every_check(
    evidence: Any,
) -> None:
    receipt = _passing_receipt()
    check = receipt["checks"][0]
    if evidence is None:
        check.pop("evidence")
    else:
        check["evidence"] = evidence

    with pytest.raises(CodingIsolationError, match="non-empty evidence mapping"):
        _attestation(receipt=receipt)


def test_attestation_rejects_extra_generic_check_not_declared_as_a_capability() -> None:
    receipt = _passing_receipt()
    receipt["checks"].append(
        {
            "id": "generic_boundary_probe",
            "status": "passed",
            "evidence": {"fixture_observation": "generic"},
        }
    )

    with pytest.raises(
        CodingIsolationError,
        match="conformance checks contain undeclared capabilities: generic_boundary_probe",
    ):
        _attestation(receipt=receipt)


def test_attestation_receipt_hash_covers_exact_evidence_document() -> None:
    receipt = _passing_receipt()
    digest_before_evidence_change = receipt_sha256(receipt)
    receipt["checks"][0]["evidence"]["fixture_observation"] = "changed"

    with pytest.raises(CodingIsolationError, match="receipt hash mismatch"):
        _attestation(
            receipt=receipt,
            conformance_receipt_sha256=digest_before_evidence_change,
        )


@pytest.mark.parametrize(
    "observed_at",
    [None, "not-a-timestamp", "2026-08-31T00:00:00"],
    ids=("missing", "malformed", "timezone-naive"),
)
def test_attestation_requires_an_auditable_receipt_timestamp(
    observed_at: str | None,
) -> None:
    receipt = _passing_receipt()
    receipt["observed_at"] = observed_at

    with pytest.raises(CodingIsolationError, match="receipt timestamp"):
        _attestation(receipt=receipt)


def test_attestation_rejects_nonabsolute_broker_path() -> None:
    with pytest.raises(CodingIsolationError, match="broker executable path must be absolute"):
        _attestation(broker_executable_path="bin/oracle-isolation-broker")


def test_run_request_rejects_nonabsolute_workspace() -> None:
    with pytest.raises(CodingIsolationError, match="workspace must be absolute"):
        _request(workspace=Path("relative/workspace"))


def test_run_request_rejects_credential_environment_names() -> None:
    with pytest.raises(CodingIsolationError, match="environment contains unsafe names"):
        IsolationRunRequest(
            adapter="codex",
            workspace=Path("/isolated/workspace"),
            command=("codex", "exec", "-"),
            input_bytes=b"prompt",
            environment={"OPENAI_API_KEY": "must-not-cross"},
            timeout_seconds=30,
            max_output_bytes=4096,
            max_workspace_export_bytes=8192,
            max_workspace_entries=32,
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"cleanup_confirmed": False}, "requires confirmed sandbox cleanup"),
        ({"workspace_export_bytes": -1}, "export counters must not be negative"),
        ({"workspace_export_entries": -1}, "export counters must not be negative"),
        ({"workspace_export_sha256": "not-a-sha256"}, "export must carry a SHA-256"),
        ({"workspace_export_sha256": "f" * 64}, "export SHA-256 mismatch"),
        ({"workspace_export_bytes": 0}, "export byte count mismatch"),
        ({"sandbox_id": "sandbox/escape"}, "invalid isolated sandbox identity"),
    ],
    ids=(
        "cleanup",
        "export-bytes",
        "export-entries",
        "export-hash-format",
        "export-hash-mismatch",
        "export-size-mismatch",
        "sandbox-id",
    ),
)
def test_run_result_requires_cleanup_and_bounded_export_evidence(
    overrides: Mapping[str, Any],
    message: str,
) -> None:
    with pytest.raises(CodingIsolationError, match=message):
        _result(_attestation(), **overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"exit_code": 17},
        {"exit_code": None, "timed_out": True},
        {"exit_code": 137, "output_limited": True},
    ],
    ids=("nonzero", "timeout", "output-limit"),
)
def test_failed_isolated_run_preserves_exact_bounded_output_without_export(
    overrides: Mapping[str, Any],
) -> None:
    failure = _failure(_attestation(), **overrides)

    assert failure.stdout == b"bounded stdout\x00\xff"
    assert failure.stderr == b"bounded stderr\x80"
    assert failure.cleanup_confirmed is True
    assert failure.attestation.fingerprint == _attestation().fingerprint
    assert not hasattr(failure, "workspace_export")
    assert "bounded stdout" not in str(failure)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"exit_code": 0, "timed_out": False, "output_limited": False},
            "requires a failed worker result",
        ),
        ({"cleanup_confirmed": False}, "requires confirmed sandbox cleanup"),
        ({"max_output_bytes": 1}, "output exceeds its declared bound"),
        ({"stdout": "not bytes"}, "must be exact bytes"),
        ({"sandbox_id": "sandbox/escape"}, "invalid isolated sandbox identity"),
    ],
    ids=("success", "cleanup", "output-bound", "raw-bytes", "sandbox-id"),
)
def test_failed_isolated_run_contract_rejects_untrusted_evidence(
    overrides: Mapping[str, Any],
    message: str,
) -> None:
    with pytest.raises(CodingIsolationError, match=message):
        _failure(_attestation(), **overrides)


def test_export_result_rejects_failed_worker_flags() -> None:
    with pytest.raises(CodingIsolationError, match="must use IsolationRunFailed"):
        _result(_attestation(), exit_code=17)


def test_fake_protocol_binding_runs_without_an_external_worker() -> None:
    binding = _FakeBinding(_attestation())
    request = _request()

    result = require_conforming_binding(binding).run(request)

    assert binding.requests == [request]
    assert result.stdout == b"worker output\x00\xff"
    assert result.workspace_export_bytes == len(result.workspace_export)
    assert result.workspace_export_entries == 0
    assert result.cleanup_confirmed is True
    assert result.attestation is binding.attestation
