"""OS-enforced isolation contract for untrusted coding-agent workers.

The configured worker profile is a request, not proof that isolation exists.
Only a broker binding carrying a complete, mechanically produced attestation
may activate a production Codex or OpenCode adapter.  Fake adapters used by
unit tests continue to use the explicit dependency-injection seam and never
mint an attestation.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from oracle_lab.jsonutil import canonical_json


class CodingIsolationError(RuntimeError):
    """Raised when a coding-worker boundary cannot be proven or maintained."""


REQUIRED_ISOLATION_CAPABILITIES = frozenset(
    {
        "microvm_or_equivalent_os_boundary",
        "host_filesystem_unavailable",
        "host_git_control_unavailable",
        "workspace_source_read_only",
        "workspace_changes_private_until_export",
        "network_default_deny",
        "network_exact_allowlist",
        "credential_proxy_values_unavailable",
        "host_processes_unavailable",
        "host_docker_unavailable",
        "all_descendants_confined",
        "all_descendants_destroyed_on_cleanup",
        "shared_agent_state_disabled",
        "bounded_workspace_export",
    }
)
SAFE_ISOLATED_ENVIRONMENT_NAMES = frozenset(
    {"COLORTERM", "LANG", "LC_ALL", "LC_CTYPE", "NO_COLOR", "TERM"}
)

_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_BACKEND_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_SAFE_SANDBOX_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,199}$")


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    return value


def _thaw_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_value(item) for item in value]
    return value


def _frozen_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return _freeze_value(value)


@dataclass(frozen=True, slots=True)
class IsolationAttestation:
    """Stable identity of a broker plus a complete conformance receipt.

    ``observed_at`` deliberately remains inside the receipt document rather
    than being regenerated for each bind.  A queued task therefore keeps the
    exact receipt it was authorized under and configuration drift remains
    detectable across restarts.
    """

    backend: str
    broker_executable_path: str
    broker_executable_sha256: str
    client_version: str
    server_version: str
    template_reference: str
    template_identity: str
    policy_sha256: str
    conformance_suite_version: str
    conformance_receipt_sha256: str
    capabilities: tuple[str, ...]
    receipt: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise CodingIsolationError("unsupported isolation attestation schema")
        if not _SAFE_BACKEND_ID.fullmatch(self.backend):
            raise CodingIsolationError("isolation backend identity is invalid")
        executable = Path(self.broker_executable_path)
        if not executable.is_absolute():
            raise CodingIsolationError("isolation broker executable path must be absolute")
        for field_name in (
            "broker_executable_sha256",
            "policy_sha256",
            "conformance_receipt_sha256",
        ):
            if not _HEX_SHA256.fullmatch(str(getattr(self, field_name))):
                raise CodingIsolationError(f"{field_name} must be a lowercase SHA-256")
        for field_name in (
            "client_version",
            "server_version",
            "template_reference",
            "template_identity",
            "conformance_suite_version",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise CodingIsolationError(f"{field_name} must not be blank")
        template_match = re.fullmatch(
            r".+@sha256:([0-9a-f]{64})",
            self.template_reference,
        )
        if template_match is None:
            raise CodingIsolationError("isolation template reference must pin a lowercase SHA-256")
        if self.template_identity != f"sha256:{template_match.group(1)}":
            raise CodingIsolationError(
                "isolation template identity does not match the requested digest"
            )
        if any(
            not isinstance(capability, str) or not capability for capability in self.capabilities
        ):
            raise CodingIsolationError("isolation capability IDs must be non-blank")
        if len(self.capabilities) != len(set(self.capabilities)):
            raise CodingIsolationError("isolation capabilities must be unique")
        capabilities = tuple(sorted(self.capabilities))
        missing = REQUIRED_ISOLATION_CAPABILITIES.difference(capabilities)
        if missing:
            raise CodingIsolationError(
                "isolation attestation is missing required capabilities: "
                + ", ".join(sorted(missing))
            )
        receipt = dict(self.receipt)
        if receipt.get("schema_version") != 1:
            raise CodingIsolationError("isolation conformance receipt schema is invalid")
        if receipt.get("status") != "passed":
            raise CodingIsolationError("isolation conformance receipt did not pass")
        observed_at = receipt.get("observed_at")
        if not isinstance(observed_at, str):
            raise CodingIsolationError("isolation conformance receipt timestamp is missing")
        try:
            parsed_observed_at = dt.datetime.fromisoformat(observed_at)
        except ValueError as error:
            raise CodingIsolationError(
                "isolation conformance receipt timestamp is invalid"
            ) from error
        if parsed_observed_at.tzinfo is None:
            raise CodingIsolationError(
                "isolation conformance receipt timestamp must be timezone-aware"
            )
        checks = receipt.get("checks")
        if not isinstance(checks, list) or not checks:
            raise CodingIsolationError("isolation conformance receipt has no checks")
        if any(
            not isinstance(check, Mapping) or check.get("status") != "passed" for check in checks
        ):
            raise CodingIsolationError("every isolation conformance check must pass")
        check_ids = [check.get("id") for check in checks]
        if any(not isinstance(check_id, str) or not check_id for check_id in check_ids):
            raise CodingIsolationError("isolation conformance check IDs must be non-blank")
        if len(check_ids) != len(set(check_ids)):
            raise CodingIsolationError("isolation conformance check IDs must be unique")
        if any(
            not isinstance(check.get("evidence"), Mapping)
            or not check["evidence"]
            or any(
                not isinstance(evidence_key, str) or not evidence_key.strip()
                for evidence_key in check["evidence"]
            )
            for check in checks
        ):
            raise CodingIsolationError(
                "every isolation conformance check must carry a non-empty evidence mapping"
            )
        unexpected = set(check_ids).difference(capabilities)
        if unexpected:
            raise CodingIsolationError(
                "isolation conformance checks contain undeclared capabilities: "
                + ", ".join(sorted(unexpected))
            )
        unchecked = set(capabilities).difference(check_ids)
        if unchecked:
            raise CodingIsolationError(
                "isolation capabilities lack passing conformance checks: "
                + ", ".join(sorted(unchecked))
            )
        receipt_sha256 = hashlib.sha256(canonical_json(receipt).encode("utf-8")).hexdigest()
        if receipt_sha256 != self.conformance_receipt_sha256:
            raise CodingIsolationError("isolation conformance receipt hash mismatch")
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(self, "receipt", _frozen_mapping(receipt))

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(canonical_json(self.to_dict()).encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "backend": self.backend,
            "broker_executable_path": self.broker_executable_path,
            "broker_executable_sha256": self.broker_executable_sha256,
            "client_version": self.client_version,
            "server_version": self.server_version,
            "template_reference": self.template_reference,
            "template_identity": self.template_identity,
            "policy_sha256": self.policy_sha256,
            "conformance_suite_version": self.conformance_suite_version,
            "conformance_receipt_sha256": self.conformance_receipt_sha256,
            "capabilities": list(self.capabilities),
            "receipt": _thaw_value(self.receipt),
        }


@dataclass(frozen=True, slots=True)
class IsolationRunRequest:
    """One bounded run inside an already attested isolation binding."""

    adapter: str
    workspace: Path
    command: tuple[str, ...]
    input_bytes: bytes
    environment: Mapping[str, str]
    timeout_seconds: float
    max_output_bytes: int
    max_workspace_export_bytes: int
    max_workspace_entries: int

    def __post_init__(self) -> None:
        if self.adapter not in {"codex", "opencode"}:
            raise CodingIsolationError("isolated adapter must be codex or opencode")
        workspace = Path(self.workspace)
        if not workspace.is_absolute():
            raise CodingIsolationError("isolated workspace must be absolute")
        if not self.command or any(not isinstance(item, str) or not item for item in self.command):
            raise CodingIsolationError("isolated command must contain non-blank arguments")
        if not isinstance(self.input_bytes, bytes):
            raise CodingIsolationError("isolated input must be exact bytes")
        if self.timeout_seconds <= 0:
            raise CodingIsolationError("isolated timeout must be positive")
        if (
            min(
                self.max_output_bytes,
                self.max_workspace_export_bytes,
                self.max_workspace_entries,
            )
            <= 0
        ):
            raise CodingIsolationError("isolated byte and entry limits must be positive")
        invalid_environment = sorted(
            str(name) for name in self.environment if name not in SAFE_ISOLATED_ENVIRONMENT_NAMES
        )
        if invalid_environment:
            raise CodingIsolationError(
                "isolated environment contains unsafe names: " + ", ".join(invalid_environment)
            )
        if any(
            not isinstance(value, str) or "\x00" in value or len(value) > 4096
            for value in self.environment.values()
        ):
            raise CodingIsolationError("isolated environment contains an unsafe value")
        object.__setattr__(self, "workspace", workspace)
        object.__setattr__(self, "command", tuple(self.command))
        object.__setattr__(self, "environment", _frozen_mapping(dict(self.environment)))


@dataclass(frozen=True, slots=True)
class IsolationRunResult:
    """Successful measured result returned after the sandbox was destroyed."""

    exit_code: int | None
    stdout: bytes
    stderr: bytes
    timed_out: bool
    output_limited: bool
    actual_command: tuple[str, ...]
    guest_executable_path: str | None
    guest_executable_version: str | None
    guest_executable_version_status: str
    sandbox_id: str
    workspace_export: bytes
    workspace_export_sha256: str
    workspace_export_bytes: int
    workspace_export_entries: int
    cleanup_confirmed: bool
    attestation: IsolationAttestation

    def __post_init__(self) -> None:
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)
        ):
            raise CodingIsolationError("isolated exit_code must be an integer or unknown")
        if not isinstance(self.stdout, bytes) or not isinstance(self.stderr, bytes):
            raise CodingIsolationError("isolated stdout and stderr must be exact bytes")
        if not self.actual_command:
            raise CodingIsolationError("isolated actual command must not be empty")
        if self.guest_executable_version_status not in {"reported", "unknown"}:
            raise CodingIsolationError("invalid guest executable version status")
        if not _SAFE_SANDBOX_ID.fullmatch(self.sandbox_id):
            raise CodingIsolationError("invalid isolated sandbox identity")
        if not _HEX_SHA256.fullmatch(self.workspace_export_sha256):
            raise CodingIsolationError("workspace export must carry a SHA-256")
        if not isinstance(self.workspace_export, bytes):
            raise CodingIsolationError("workspace export must contain exact bytes")
        if self.workspace_export_bytes < 0 or self.workspace_export_entries < 0:
            raise CodingIsolationError("workspace export counters must not be negative")
        if hashlib.sha256(self.workspace_export).hexdigest() != self.workspace_export_sha256:
            raise CodingIsolationError("workspace export SHA-256 mismatch")
        if len(self.workspace_export) != self.workspace_export_bytes:
            raise CodingIsolationError("workspace export byte count mismatch")
        if not self.cleanup_confirmed:
            raise CodingIsolationError("isolation result requires confirmed sandbox cleanup")
        if not isinstance(self.attestation, IsolationAttestation):
            raise CodingIsolationError("isolation result attestation is invalid")
        if self.exit_code != 0 or self.timed_out or self.output_limited:
            raise CodingIsolationError(
                "failed isolated runs must use IsolationRunFailed without a workspace export"
            )
        object.__setattr__(self, "actual_command", tuple(self.actual_command))


class IsolationRunFailed(CodingIsolationError):
    """Bounded failed-worker observation emitted only after confirmed cleanup.

    A failed worker's private workspace is intentionally absent from this
    contract.  The exact bounded stdout and stderr remain available for the
    write-once worker archive without making a failed candidate tree eligible
    for Host materialization.
    """

    __slots__ = (
        "actual_command",
        "attestation",
        "cleanup_confirmed",
        "exit_code",
        "guest_executable_path",
        "guest_executable_version",
        "guest_executable_version_status",
        "max_output_bytes",
        "output_limited",
        "sandbox_id",
        "stderr",
        "stdout",
        "timed_out",
    )

    def __init__(
        self,
        *,
        exit_code: int | None,
        stdout: bytes,
        stderr: bytes,
        timed_out: bool,
        output_limited: bool,
        actual_command: tuple[str, ...],
        guest_executable_path: str | None,
        guest_executable_version: str | None,
        guest_executable_version_status: str,
        sandbox_id: str,
        cleanup_confirmed: bool,
        attestation: IsolationAttestation,
        max_output_bytes: int,
    ) -> None:
        if exit_code is not None and (
            isinstance(exit_code, bool) or not isinstance(exit_code, int)
        ):
            raise CodingIsolationError("isolated failure exit_code must be an integer or unknown")
        if not isinstance(stdout, bytes) or not isinstance(stderr, bytes):
            raise CodingIsolationError("isolated failure stdout and stderr must be exact bytes")
        if not isinstance(timed_out, bool) or not isinstance(output_limited, bool):
            raise CodingIsolationError("isolated failure flags must be booleans")
        if exit_code == 0 and not timed_out and not output_limited:
            raise CodingIsolationError("isolated failure contract requires a failed worker result")
        if not actual_command or any(
            not isinstance(argument, str) or not argument or "\x00" in argument
            for argument in actual_command
        ):
            raise CodingIsolationError("isolated failure actual command must not be empty")
        if guest_executable_version_status not in {"reported", "unknown"}:
            raise CodingIsolationError("invalid guest executable version status")
        for field_name, value in (
            ("guest_executable_path", guest_executable_path),
            ("guest_executable_version", guest_executable_version),
        ):
            if value is not None and (not isinstance(value, str) or not value or "\x00" in value):
                raise CodingIsolationError(f"invalid isolated failure {field_name}")
        if not _SAFE_SANDBOX_ID.fullmatch(sandbox_id):
            raise CodingIsolationError("invalid isolated sandbox identity")
        if cleanup_confirmed is not True:
            raise CodingIsolationError("isolated failure requires confirmed sandbox cleanup")
        if not isinstance(attestation, IsolationAttestation):
            raise CodingIsolationError("isolated failure attestation is invalid")
        if (
            isinstance(max_output_bytes, bool)
            or not isinstance(max_output_bytes, int)
            or max_output_bytes <= 0
        ):
            raise CodingIsolationError("isolated failure output bound must be positive")
        if len(stdout) + len(stderr) > max_output_bytes:
            raise CodingIsolationError("isolated failure output exceeds its declared bound")

        super().__init__(
            "isolated worker failed after confirmed cleanup; no workspace export was produced"
        )
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out
        self.output_limited = output_limited
        self.actual_command = tuple(actual_command)
        self.guest_executable_path = guest_executable_path
        self.guest_executable_version = guest_executable_version
        self.guest_executable_version_status = guest_executable_version_status
        self.sandbox_id = sandbox_id
        self.cleanup_confirmed = cleanup_confirmed
        self.attestation = attestation
        self.max_output_bytes = max_output_bytes


@runtime_checkable
class CodingWorkerIsolationBinding(Protocol):
    """Profile-bound capability which owns each complete sandbox lifecycle."""

    @property
    def attestation(self) -> IsolationAttestation: ...

    def run(self, request: IsolationRunRequest) -> IsolationRunResult: ...


@runtime_checkable
class CodingWorkerIsolationBroker(Protocol):
    """Trusted broker factory; a config string can never implement this contract."""

    def bind(self, profile: Any) -> CodingWorkerIsolationBinding: ...


def require_conforming_binding(value: Any) -> CodingWorkerIsolationBinding:
    """Return a structurally conforming binding or fail before worker startup."""

    if not isinstance(value, CodingWorkerIsolationBinding):
        raise CodingIsolationError("coding-worker isolation binding is unavailable")
    attestation = value.attestation
    if not isinstance(attestation, IsolationAttestation):
        raise CodingIsolationError("coding-worker isolation attestation is unavailable")
    # Construction validates the complete capability and receipt set.  Access
    # the fingerprint here as a final deterministic serialization check.
    if not _HEX_SHA256.fullmatch(attestation.fingerprint):
        raise CodingIsolationError("coding-worker isolation fingerprint is invalid")
    return value


def receipt_sha256(receipt: Mapping[str, Any]) -> str:
    """Build the exact digest required by :class:`IsolationAttestation`."""

    return hashlib.sha256(canonical_json(dict(receipt)).encode("utf-8")).hexdigest()


__all__ = [
    "REQUIRED_ISOLATION_CAPABILITIES",
    "SAFE_ISOLATED_ENVIRONMENT_NAMES",
    "CodingIsolationError",
    "CodingWorkerIsolationBinding",
    "CodingWorkerIsolationBroker",
    "IsolationAttestation",
    "IsolationRunFailed",
    "IsolationRunRequest",
    "IsolationRunResult",
    "receipt_sha256",
    "require_conforming_binding",
]
