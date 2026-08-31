"""Side-effect-free readiness diagnostics for production coding workers.

This module deliberately does less than an isolation broker.  It reads only the
coding-worker configuration and the configured broker executable, never starts a
process, never creates a sandbox, and never mints an :class:`IsolationAttestation`.
Its report is an operator aid, not authority to start Codex or OpenCode.

The production Docker ``sbx`` guard remains active while any identifier in
``PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS`` is present.  Static prerequisites may
therefore pass while ``ready`` and ``safe_to_start_worker`` remain false.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import shutil
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from oracle_lab.coding_isolation import (
    PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS,
    SAFE_ISOLATED_ENVIRONMENT_NAMES,
)
from oracle_lab.config import AgentRuntimeConfig, load_agents

ReadinessCheckStatus = Literal["passed", "failed", "blocked"]
ReadinessReportStatus = Literal["failed", "blocked"]


_PINNED_TEMPLATE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_CHECK_IDS = (
    "agents_config",
    "router_enabled",
    "enabled_coding_workers",
    "coding_worker_selection",
    "isolation_backend",
    "worker_sandbox_profiles",
    "worker_template_references",
    "worker_network_allowlists",
    "worker_environment_allowlists",
    "broker_executable",
    "production_isolation_evidence",
)


@dataclass(frozen=True, slots=True)
class ReadinessCheck:
    """One stable, machine-readable prerequisite observation."""

    id: str
    status: ReadinessCheckStatus
    reason_id: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if self.id not in _CHECK_IDS:
            raise ValueError(f"unknown worker readiness check ID: {self.id}")
        if self.status == "passed" and self.reason_id is not None:
            raise ValueError("passed readiness checks may not carry a failure reason")
        if self.status != "passed" and not self.reason_id:
            raise ValueError("non-passing readiness checks require a stable reason ID")
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "reason_id": self.reason_id,
            "evidence": _thaw(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class WorkerReadinessReport:
    """Static diagnostic report which can never authorize a production run."""

    config_path: str
    status: ReadinessReportStatus
    checks: tuple[ReadinessCheck, ...]
    production_evidence_blockers: tuple[str, ...] = PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported worker readiness report schema")
        check_ids = tuple(check.id for check in self.checks)
        if check_ids != _CHECK_IDS:
            raise ValueError("worker readiness report checks are incomplete or out of order")
        if tuple(self.production_evidence_blockers) != PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS:
            raise ValueError("worker readiness report changed the production blocker registry")
        if self.status == "failed" and not any(check.status == "failed" for check in self.checks):
            raise ValueError("failed readiness report has no failed check")
        if self.status == "blocked" and any(check.status == "failed" for check in self.checks):
            raise ValueError("blocked readiness report contains a failed prerequisite")
        object.__setattr__(self, "checks", tuple(self.checks))
        object.__setattr__(
            self,
            "production_evidence_blockers",
            tuple(self.production_evidence_blockers),
        )

    @property
    def ready(self) -> bool:
        """Return false while the explicit production evidence guard exists."""

        return False

    @property
    def safe_to_start_worker(self) -> bool:
        """Return false because this static report is never execution authority."""

        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "ready": self.ready,
            "safe_to_start_worker": self.safe_to_start_worker,
            "config_path": self.config_path,
            "checks": [check.to_dict() for check in self.checks],
            "production_evidence_blockers": list(self.production_evidence_blockers),
        }


class _ExecutableInspectionError(RuntimeError):
    def __init__(self, reason_id: str) -> None:
        super().__init__(reason_id)
        self.reason_id = reason_id


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _inspect_broker_executable(
    configured: str,
    *,
    executable_resolver: Callable[[str], str | None],
) -> Mapping[str, str]:
    """Resolve and hash one executable without invoking it.

    Package managers commonly expose binaries through a symlink (Homebrew's
    ``/opt/homebrew/bin/sbx`` is one example).  Authority is bound to the fully
    resolved regular file and its digest, matching the production broker's
    identity rule, rather than to the mutable launcher path.
    """

    if not isinstance(configured, str) or not configured.strip() or "\x00" in configured:
        raise _ExecutableInspectionError("broker_executable_invalid")
    candidate_text = (
        configured
        if Path(configured).expanduser().is_absolute()
        else executable_resolver(configured)
    )
    if not isinstance(candidate_text, str) or not candidate_text or "\x00" in candidate_text:
        raise _ExecutableInspectionError("broker_executable_unavailable")
    candidate = Path(candidate_text).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
        resolved_details = resolved.lstat()
    except (OSError, ValueError) as error:
        raise _ExecutableInspectionError("broker_executable_unavailable") from error
    if stat.S_ISLNK(resolved_details.st_mode) or not stat.S_ISREG(resolved_details.st_mode):
        raise _ExecutableInspectionError("broker_executable_not_regular")
    if not os.access(resolved, os.X_OK):
        raise _ExecutableInspectionError("broker_executable_not_executable")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise _ExecutableInspectionError("broker_executable_nofollow_unavailable")
    try:
        descriptor = os.open(resolved, os.O_RDONLY | nofollow)
    except OSError as error:
        raise _ExecutableInspectionError("broker_executable_unreadable") from error
    try:
        before = os.fstat(descriptor)
        digest = hashlib.sha256()
        while block := os.read(descriptor, 1024 * 1024):
            digest.update(block)
        after = os.fstat(descriptor)
    except OSError as error:
        raise _ExecutableInspectionError("broker_executable_unreadable") from error
    finally:
        with contextlib.suppress(OSError):
            os.close(descriptor)
    if (
        _file_identity(before) != _file_identity(after)
        or not stat.S_ISREG(before.st_mode)
        or before.st_ino != resolved_details.st_ino
        or before.st_dev != resolved_details.st_dev
    ):
        raise _ExecutableInspectionError("broker_executable_identity_changed")
    return MappingProxyType(
        {
            "resolved_path": str(resolved),
            "sha256": digest.hexdigest(),
            "truth_domain": "real",
        }
    )


def _check(
    check_id: str,
    status: ReadinessCheckStatus,
    reason_id: str | None = None,
    **evidence: Any,
) -> ReadinessCheck:
    return ReadinessCheck(check_id, status, reason_id, evidence)


def _unavailable_checks(reason_id: str) -> tuple[ReadinessCheck, ...]:
    return tuple(_check(check_id, "blocked", reason_id) for check_id in _CHECK_IDS[1:-1])


def _inspect_loaded_config(
    config: AgentRuntimeConfig,
    *,
    config_path: Path,
    executable_resolver: Callable[[str], str | None],
) -> WorkerReadinessReport:
    checks: list[ReadinessCheck] = [
        _check("agents_config", "passed", path=str(config_path)),
    ]

    if config.enabled:
        checks.append(_check("router_enabled", "passed"))
    else:
        checks.append(_check("router_enabled", "blocked", "router_disabled"))

    coding_workers = tuple(
        sorted(
            (
                (worker_id, worker)
                for worker_id, worker in config.workers.items()
                if worker.enabled and worker.adapter in {"codex", "opencode"}
            ),
            key=lambda item: item[0],
        )
    )
    worker_ids = tuple(worker_id for worker_id, _worker in coding_workers)
    if coding_workers:
        checks.append(
            _check(
                "enabled_coding_workers",
                "passed",
                worker_ids=worker_ids,
            )
        )
    else:
        checks.append(
            _check(
                "enabled_coding_workers",
                "blocked",
                "no_enabled_coding_worker",
            )
        )

    adapter_counts: dict[str, int] = {}
    for _worker_id, worker in coding_workers:
        adapter_counts[worker.adapter] = adapter_counts.get(worker.adapter, 0) + 1
    if not coding_workers:
        checks.append(
            _check(
                "coding_worker_selection",
                "blocked",
                "no_enabled_coding_worker",
            )
        )
    elif adapter_counts.get(config.prefer_coding_agent, 0) != 1:
        checks.append(
            _check(
                "coding_worker_selection",
                "failed",
                "preferred_coding_worker_not_uniquely_enabled",
                preferred_adapter=config.prefer_coding_agent,
            )
        )
    elif any(count > 1 for count in adapter_counts.values()):
        checks.append(
            _check(
                "coding_worker_selection",
                "failed",
                "duplicate_enabled_coding_adapter",
            )
        )
    else:
        checks.append(
            _check(
                "coding_worker_selection",
                "passed",
                preferred_adapter=config.prefer_coding_agent,
            )
        )

    if config.isolation_backend == "docker-sbx-microvm":
        checks.append(_check("isolation_backend", "passed", backend=config.isolation_backend))
    else:
        checks.append(
            _check(
                "isolation_backend",
                "blocked",
                "production_isolation_backend_disabled",
                backend=config.isolation_backend,
            )
        )

    if not coding_workers:
        for check_id in (
            "worker_sandbox_profiles",
            "worker_template_references",
            "worker_network_allowlists",
            "worker_environment_allowlists",
        ):
            checks.append(_check(check_id, "blocked", "no_enabled_coding_worker"))
    else:
        invalid_sandbox = tuple(
            worker_id
            for worker_id, worker in coding_workers
            if worker.sandbox_profile != "external-broker"
        )
        checks.append(
            _check(
                "worker_sandbox_profiles",
                "failed",
                "coding_worker_not_external_broker",
                worker_ids=invalid_sandbox,
            )
            if invalid_sandbox
            else _check("worker_sandbox_profiles", "passed", worker_ids=worker_ids)
        )

        invalid_templates = tuple(
            worker_id
            for worker_id, worker in coding_workers
            if worker.isolation_template_reference is None
            or _PINNED_TEMPLATE.fullmatch(worker.isolation_template_reference) is None
        )
        checks.append(
            _check(
                "worker_template_references",
                "failed",
                "worker_template_not_digest_pinned",
                worker_ids=invalid_templates,
            )
            if invalid_templates
            else _check("worker_template_references", "passed", worker_ids=worker_ids)
        )

        missing_network = tuple(
            worker_id for worker_id, worker in coding_workers if not worker.isolation_allowed_hosts
        )
        checks.append(
            _check(
                "worker_network_allowlists",
                "failed",
                "worker_exact_network_allowlist_missing",
                worker_ids=missing_network,
            )
            if missing_network
            else _check("worker_network_allowlists", "passed", worker_ids=worker_ids)
        )

        unsafe_environment = tuple(
            worker_id
            for worker_id, worker in coding_workers
            if set(worker.allowed_environment_names) - SAFE_ISOLATED_ENVIRONMENT_NAMES
        )
        checks.append(
            _check(
                "worker_environment_allowlists",
                "failed",
                "worker_environment_allowlist_unsafe",
                worker_ids=unsafe_environment,
            )
            if unsafe_environment
            else _check("worker_environment_allowlists", "passed", worker_ids=worker_ids)
        )

    if config.isolation_backend != "docker-sbx-microvm":
        checks.append(
            _check(
                "broker_executable",
                "blocked",
                "production_isolation_backend_disabled",
            )
        )
    else:
        try:
            broker_evidence = _inspect_broker_executable(
                config.isolation_broker_executable,
                executable_resolver=executable_resolver,
            )
        except _ExecutableInspectionError as error:
            checks.append(_check("broker_executable", "failed", error.reason_id))
        else:
            checks.append(_check("broker_executable", "passed", **broker_evidence))

    checks.append(
        _check(
            "production_isolation_evidence",
            "blocked",
            "production_isolation_evidence_unproven",
            blocker_ids=PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS,
        )
    )
    report_status: ReadinessReportStatus = (
        "failed" if any(check.status == "failed" for check in checks) else "blocked"
    )
    return WorkerReadinessReport(
        config_path=str(config_path),
        status=report_status,
        checks=tuple(checks),
    )


def inspect_worker_readiness(
    agents_config_path: str | Path,
    *,
    executable_resolver: Callable[[str], str | None] = shutil.which,
) -> WorkerReadinessReport:
    """Inspect static coding-worker prerequisites without granting run authority.

    The returned object is deterministic for unchanged configuration and broker
    bytes.  It does not resolve credential values, invoke the broker executable,
    initialize Oracle Lab storage, or contact a model provider.
    """

    config_path = Path(agents_config_path).expanduser().resolve(strict=False)
    if not config_path.is_file():
        checks = (
            _check("agents_config", "failed", "agents_config_unavailable"),
            *_unavailable_checks("agents_config_unavailable"),
            _check(
                "production_isolation_evidence",
                "blocked",
                "production_isolation_evidence_unproven",
                blocker_ids=PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS,
            ),
        )
        return WorkerReadinessReport(
            config_path=str(config_path),
            status="failed",
            checks=checks,
        )
    try:
        config = load_agents(config_path)
    except (OSError, OverflowError, TypeError, ValueError):
        checks = (
            _check("agents_config", "failed", "agents_config_invalid"),
            *_unavailable_checks("agents_config_invalid"),
            _check(
                "production_isolation_evidence",
                "blocked",
                "production_isolation_evidence_unproven",
                blocker_ids=PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS,
            ),
        )
        return WorkerReadinessReport(
            config_path=str(config_path),
            status="failed",
            checks=checks,
        )
    return _inspect_loaded_config(
        config,
        config_path=config_path,
        executable_resolver=executable_resolver,
    )


__all__ = [
    "PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS",
    "ReadinessCheck",
    "WorkerReadinessReport",
    "inspect_worker_readiness",
]
