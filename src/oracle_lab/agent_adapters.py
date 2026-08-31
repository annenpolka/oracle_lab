"""Isolated OpenCode/Codex worker adapters with structured event ingestion."""

from __future__ import annotations

import contextlib
import hashlib
import inspect
import json
import os
import selectors
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

from oracle_lab.coding_isolation import (
    CodingIsolationError,
    CodingWorkerIsolationBinding,
    CodingWorkerIsolationBroker,
    IsolationRunFailed,
    IsolationRunRequest,
    require_conforming_binding,
)
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.git_control import (
    GitControlError,
    create_standalone_clone,
    detached_head_from_control,
    fingerprint_git_control,
    remove_standalone_clone,
    replace_worktree_from_untrusted,
    run_git,
)
from oracle_lab.jsonutil import canonical_json
from oracle_lab.workspace_archive import (
    WorkspaceArchiveError,
    WorkspaceArchiveLimits,
    materialize_workspace_export,
    validate_workspace_archive,
)


class AgentAdapterError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WorkerExecutionProfile:
    """Auditable execution policy; environment values are resolved only at run time."""

    id: str
    adapter: str
    executable: str
    model: str | None = None
    timeout_seconds: float = 300
    max_output_bytes: int = 4 * 1024 * 1024
    sandbox_profile: str = "workspace-write"
    allowed_environment_names: tuple[str, ...] = ()
    fallback_adapter: str | None = None
    max_retries: int = 0
    validation_commands: tuple[str, ...] = ()
    host_provider_kind: str | None = None
    host_provider_id: str | None = None
    host_base_url: str | None = None
    host_api_key_env: str | None = None
    host_temperature: float | None = None
    host_top_p: float | None = None
    host_max_tokens: int | None = None
    host_allow_fallback: bool = False
    isolation_template_reference: str | None = None
    isolation_allowed_hosts: tuple[str, ...] = ()
    max_workspace_export_bytes: int = 64 * 1024 * 1024
    max_workspace_entries: int = 100_000
    isolation_attestation: Mapping[str, Any] | None = None

    @classmethod
    def from_config(cls, value: Any) -> WorkerExecutionProfile:
        model = getattr(value, "model", None)
        return cls(
            id=str(value.id),
            adapter=str(value.adapter),
            executable=str(value.executable),
            model=(None if model is None or not str(model).strip() else str(model)),
            timeout_seconds=float(value.timeout_seconds),
            max_output_bytes=int(value.max_output_bytes),
            sandbox_profile=str(value.sandbox_profile),
            allowed_environment_names=tuple(value.allowed_environment_names),
            fallback_adapter=getattr(value, "fallback_adapter", None),
            max_retries=int(getattr(value, "max_retries", 0)),
            validation_commands=tuple(getattr(value, "validation_commands", ())),
            host_provider_kind=getattr(value, "host_provider_kind", None),
            host_provider_id=getattr(value, "host_provider_id", None),
            host_base_url=getattr(value, "host_base_url", None),
            host_api_key_env=getattr(value, "host_api_key_env", None),
            host_temperature=getattr(value, "host_temperature", None),
            host_top_p=getattr(value, "host_top_p", None),
            host_max_tokens=getattr(value, "host_max_tokens", None),
            host_allow_fallback=bool(getattr(value, "host_allow_fallback", False)),
            isolation_template_reference=getattr(value, "isolation_template_reference", None),
            isolation_allowed_hosts=tuple(getattr(value, "isolation_allowed_hosts", ())),
            max_workspace_export_bytes=int(
                getattr(value, "max_workspace_export_bytes", 64 * 1024 * 1024)
            ),
            max_workspace_entries=int(getattr(value, "max_workspace_entries", 100_000)),
        )

    def with_isolation_attestation(self, value: Mapping[str, Any]) -> WorkerExecutionProfile:
        """Bind a runtime-produced attestation without mutating config state."""

        return replace(self, isolation_attestation=_freeze_worker_value(value))

    def resolved_environment(self) -> dict[str, str]:
        return {
            name: os.environ[name] for name in self.allowed_environment_names if name in os.environ
        }

    def redacted_snapshot(self) -> dict[str, Any]:
        """Return the complete durable profile without resolved environment values."""

        return {
            "schema_version": 1,
            "id": self.id,
            "adapter": self.adapter,
            "executable": self.executable,
            "model": self.model,
            "timeout_seconds": self.timeout_seconds,
            "max_output_bytes": self.max_output_bytes,
            "sandbox_profile": self.sandbox_profile,
            "allowed_environment_names": list(self.allowed_environment_names),
            "fallback_adapter": self.fallback_adapter,
            "max_retries": self.max_retries,
            "validation_commands": list(self.validation_commands),
            "host_provider_kind": self.host_provider_kind,
            "host_provider_id": self.host_provider_id,
            "host_base_url": self.host_base_url,
            "host_api_key_env": self.host_api_key_env,
            "host_temperature": self.host_temperature,
            "host_top_p": self.host_top_p,
            "host_max_tokens": self.host_max_tokens,
            "host_allow_fallback": self.host_allow_fallback,
            "isolation_template_reference": self.isolation_template_reference,
            "isolation_allowed_hosts": list(self.isolation_allowed_hosts),
            "max_workspace_export_bytes": self.max_workspace_export_bytes,
            "max_workspace_entries": self.max_workspace_entries,
            "isolation_attestation": (
                None
                if self.isolation_attestation is None
                else _thaw_worker_value(self.isolation_attestation)
            ),
        }


def _freeze_worker_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_worker_value(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_worker_value(item) for item in value)
    return value


def _thaw_worker_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_worker_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_worker_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class WorkerTask:
    source_event: Event
    goal: str
    related_claims: tuple[Mapping[str, Any], ...] = ()
    recent_events: tuple[Event, ...] = ()
    extra_instructions: tuple[str, ...] = ()
    task_kind: str = "analysis"
    repository: str | None = None
    base_commit: str | None = None
    validation_commands: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.goal.strip():
            raise AgentAdapterError("worker task goal must not be empty")
        if self.task_kind == "repository_edit" and not self.repository:
            raise AgentAdapterError("repository_edit requires an explicit repository path")

    def render(self) -> str:
        recent = self.recent_events[-20:]
        task_data = {
            "source_event": self.source_event.to_dict(),
            "related_claims": [dict(claim) for claim in self.related_claims],
            "recent_events": [event.to_dict() for event in recent],
        }
        instructions = "\n".join(f"- {item}" for item in self.extra_instructions)
        if instructions:
            instructions = f"\nAdditional constraints:\n{instructions}\n"
        output_instructions = (
            "Return resulting events only as this JSON envelope:\n"
            '{"events":[{"type":"analysis.*","payload":{},'
            '"source_event_ids":["evt_..."]}]}\n'
        )
        repository_instructions = ""
        if self.task_kind == "repository_edit":
            repository_instructions = (
                "Edit only the repository in the current working directory.\n"
                "Do not commit, push, merge, or access another worktree.\n"
                "The Host will capture and validate the candidate patch after exit.\n"
                "Your only candidate artifact is the filesystem patch.\n"
                "Do not emit Oracle Lab events or imitate oracle output.\n"
                "stdout and stderr are audit streams only and are never ingested.\n"
            )
            output_instructions = ""
        return (
            f"You are processing event {self.source_event.id}.\n\n"
            "Read the event payload, related claims, and last 20 session events in TASK_DATA.\n\n"
            f"Goal:\n{self.goal.strip()}\n\n"
            "Do not rewrite, sanitize, improve, correct, or replace oracle text.\n"
            "Every factual assertion about the session must cite an existing event ID.\n"
            "If evidence is missing, emit unknown rather than inventing it.\n"
            f"{repository_instructions}"
            f"{output_instructions}"
            f"{instructions}\n"
            f"TASK_DATA={canonical_json(task_data)}\n"
        )


@dataclass(frozen=True, slots=True)
class StructuredWorkerEvent:
    event_type: EventType
    payload: Mapping[str, Any]
    source_event_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _freeze_worker_value(self.payload))
        object.__setattr__(self, "source_event_ids", tuple(self.source_event_ids))


@dataclass(frozen=True, slots=True)
class AgentRunResult:
    adapter: str
    command: tuple[str, ...]
    exit_code: int | None
    stdout: str
    stderr: str
    elapsed_ms: float
    events: tuple[StructuredWorkerEvent, ...]
    workspace: str
    timed_out: bool = False
    output_limited: bool = False
    prompt: str = ""
    stdout_bytes: bytes = b""
    stderr_bytes: bytes = b""
    executable_path: str | None = None
    executable_version: str | None = None
    executable_version_status: str = "unknown"
    base_commit: str | None = None
    workspace_head: str | None = None
    patch_bytes: bytes = b""
    changed_paths: tuple[str, ...] = ()
    changed_modes: Mapping[str, str | None] = field(default_factory=lambda: MappingProxyType({}))
    precondition_sha256: Mapping[str, str | None] = field(
        default_factory=lambda: MappingProxyType({})
    )
    source_status_before_sha256: str | None = None
    source_status_after_sha256: str | None = None
    source_head_before: str | None = None
    source_head_after: str | None = None
    source_index_before_sha256: str | None = None
    source_index_after_sha256: str | None = None
    source_snapshot_before_sha256: str | None = None
    source_snapshot_after_sha256: str | None = None
    source_git_control_before_sha256: str | None = None
    source_git_control_after_sha256: str | None = None
    source_worktree_unchanged: bool | None = None
    worker_committed: bool = False
    worker_git_control_tampered: bool = False
    isolation_attestation: Mapping[str, Any] | None = None
    isolation_sandbox_id: str | None = None
    isolation_cleanup_confirmed: bool | None = None
    workspace_export_sha256: str | None = None
    workspace_export_bytes: int | None = None
    workspace_export_entries: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "precondition_sha256",
            MappingProxyType(dict(self.precondition_sha256)),
        )
        object.__setattr__(
            self,
            "changed_modes",
            MappingProxyType(dict(self.changed_modes)),
        )
        if self.isolation_attestation is not None:
            required = {
                "isolation_sandbox_id": self.isolation_sandbox_id,
                "isolation_cleanup_confirmed": self.isolation_cleanup_confirmed,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise AgentAdapterError(
                    "isolated agent result is missing execution evidence: " + ", ".join(missing)
                )
            if self.isolation_cleanup_confirmed is not True:
                raise AgentAdapterError("isolated agent result requires confirmed cleanup")
            export_evidence = (
                self.workspace_export_sha256,
                self.workspace_export_bytes,
                self.workspace_export_entries,
            )
            has_any_export_evidence = any(value is not None for value in export_evidence)
            has_complete_export_evidence = all(value is not None for value in export_evidence)
            if has_any_export_evidence != has_complete_export_evidence:
                raise AgentAdapterError(
                    "isolated agent result carries partial workspace export evidence"
                )
            execution_failed = self.exit_code != 0 or self.timed_out or self.output_limited
            if execution_failed and has_complete_export_evidence:
                raise AgentAdapterError(
                    "failed isolated agent result may not carry workspace export evidence"
                )
            if not execution_failed and not has_complete_export_evidence:
                raise AgentAdapterError(
                    "successful isolated agent result is missing workspace export evidence"
                )
            object.__setattr__(
                self,
                "isolation_attestation",
                _freeze_worker_value(self.isolation_attestation),
            )
        elif any(
            value is not None
            for value in (
                self.isolation_sandbox_id,
                self.isolation_cleanup_confirmed,
                self.workspace_export_sha256,
                self.workspace_export_bytes,
                self.workspace_export_entries,
            )
        ):
            raise AgentAdapterError(
                "non-isolated agent result may not carry partial isolation evidence"
            )

    @property
    def succeeded(self) -> bool:
        return (
            self.exit_code == 0
            and not self.timed_out
            and not self.output_limited
            and self.source_worktree_unchanged is not False
            and not self.worker_committed
            and not self.worker_git_control_tampered
            and (self.isolation_attestation is None or self.isolation_cleanup_confirmed is True)
        )


@dataclass(frozen=True, slots=True)
class _BoundedProcessResult:
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    timed_out: bool
    output_limited: bool


@dataclass(frozen=True, slots=True)
class _SourceWorktreeAudit:
    head: str
    status: bytes
    index_sha256: str
    snapshot_sha256: str
    git_control_sha256: str


class WorkspaceFactory(Protocol):
    def __call__(self) -> contextlib.AbstractContextManager[Path]: ...


class DedicatedWorkspace:
    def __init__(self, root: str | Path | None = None, *, prefix: str = "oracle-agent-") -> None:
        self.root = None if root is None else Path(root)
        self.prefix = prefix

    @contextlib.contextmanager
    def __call__(self) -> Iterable[Path]:
        if self.root is not None:
            self.root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=self.prefix, dir=self.root) as raw:
            path = Path(raw)
            marker = path / ".oracle-lab-worker"
            marker.write_text("isolated worker workspace\n", encoding="utf-8")
            yield path


class GitStandaloneCloneWorkspace:
    """Disposable source-independent clone for untrusted repository edits."""

    def __init__(
        self,
        repository: str | Path,
        *,
        root: str | Path | None = None,
        revision: str = "HEAD",
    ) -> None:
        self.repository = Path(repository).resolve()
        self.root = None if root is None else Path(root)
        self.revision = revision

    @contextlib.contextmanager
    def __call__(self) -> Iterable[Path]:
        if self.root is not None:
            self.root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="oracle-clone-parent-", dir=self.root) as raw:
            path = Path(raw) / "clone"
            try:
                create_standalone_clone(self.repository, path, self.revision)
                yield path
            except GitControlError as error:
                raise AgentAdapterError(str(error)) from error
            finally:
                if path.exists() and path.is_dir() and not path.is_symlink():
                    remove_standalone_clone(path)


# Compatibility name for callers that imported the earlier factory.  Its
# semantics are now a standalone clone, never a linked Git worktree.
GitWorktreeWorkspace = GitStandaloneCloneWorkspace


CommandBuilder = Callable[[str], Sequence[str]]


def _allowed_worker_event(event_type: EventType) -> bool:
    """Return whether untrusted host output may cross the ingestion boundary.

    Coding agents and direct host models can describe observations and proposals,
    but they cannot perform claim-lifecycle or world-state transitions.  Those
    transitions belong to deterministic application code (and, for canon, an
    explicit human gate).  Host usage is also recorded by the trusted service
    wrapper rather than accepted from model-authored output.
    """

    return event_type.value.startswith("analysis.") and event_type not in {
        EventType.ANALYSIS_PROMOTED_TO_ORACLE,
    }


def parse_structured_events(
    output: str,
    *,
    expected_source_event_id: str,
) -> tuple[StructuredWorkerEvent, ...]:
    """Extract only explicit Oracle Lab event objects from JSON/JSONL output."""

    candidates: list[Mapping[str, Any]] = []
    stripped = output.strip()
    if not stripped:
        return ()
    parsed_whole: Any = None
    with contextlib.suppress(json.JSONDecodeError):
        parsed_whole = json.loads(stripped)
    values: list[Any]
    if parsed_whole is not None:
        values = parsed_whole if isinstance(parsed_whole, list) else [parsed_whole]
    else:
        values = []
        for line in stripped.splitlines():
            try:
                values.append(json.loads(line))
            except json.JSONDecodeError:
                # Agent CLIs may print progress lines; those are never ingested.
                continue

    def collect(value: Any, depth: int = 0) -> None:
        if depth > 6:
            return
        if isinstance(value, Mapping):
            nested = value.get("events")
            if isinstance(nested, list):
                candidates.extend(item for item in nested if isinstance(item, Mapping))
            elif value.get("oracle_lab_event") is True:
                candidates.append(value)
            for key in ("text", "content", "output", "result", "message"):
                wrapped = value.get(key)
                if isinstance(wrapped, (Mapping, list)):
                    collect(wrapped, depth + 1)
                elif isinstance(wrapped, str) and wrapped.lstrip().startswith(("{", "[")):
                    with contextlib.suppress(json.JSONDecodeError):
                        collect(json.loads(wrapped), depth + 1)
        elif isinstance(value, list):
            for item in value:
                collect(item, depth + 1)

    for value in values:
        collect(value)

    events: list[StructuredWorkerEvent] = []
    for candidate in candidates:
        raw_type = candidate.get("type")
        try:
            event_type = EventType(str(raw_type))
        except ValueError as exc:
            raise AgentAdapterError(f"worker emitted unknown event type: {raw_type}") from exc
        if not _allowed_worker_event(event_type):
            raise AgentAdapterError(f"worker is not authorized to emit {event_type.value}")
        payload = candidate.get("payload", {})
        if not isinstance(payload, Mapping):
            raise AgentAdapterError("worker event payload must be an object")
        raw_sources = candidate.get("source_event_ids", payload.get("source_event_ids", ()))
        if not isinstance(raw_sources, (list, tuple)) or not all(
            isinstance(item, str) for item in raw_sources
        ):
            raise AgentAdapterError("worker event source_event_ids must be an array of strings")
        source_ids = tuple(raw_sources)
        if expected_source_event_id not in source_ids:
            raise AgentAdapterError("worker event does not cite its assigned source event")
        events.append(StructuredWorkerEvent(event_type, payload, source_ids))
    return tuple(events)


def prepare_structured_events(
    proposals: Sequence[StructuredWorkerEvent],
    *,
    source: Event,
    store: Any,
    actor_kind: ActorKind,
    actor_id: str,
    worker_run_id: str | None = None,
) -> tuple[Event, ...]:
    """Validate every proposal and build an all-or-nothing event batch."""

    if source.session_id is None or source.branch_id is None:
        raise AgentAdapterError("worker source must belong to a session branch")
    from oracle_lab.branching import BranchError, visible_event_ids_from_connection

    try:
        visible_ids = set(
            visible_event_ids_from_connection(
                store.connection,
                source.branch_id,
                until_event_id=source.id,
            )
        )
    except BranchError as error:
        branch_exists = store.connection.execute(
            "SELECT 1 FROM branches WHERE id = ?",
            (source.branch_id,),
        ).fetchone()
        if branch_exists is not None:
            raise AgentAdapterError(str(error)) from error
        # Small adapter unit fixtures may omit the session projection. Preserve
        # the same safety property by accepting only the source's parent chain.
        visible_ids = set()
        current: Event | None = source
        while current is not None:
            if current.id in visible_ids:
                raise AgentAdapterError("worker source parent chain contains a cycle") from error
            if current.session_id != source.session_id:
                raise AgentAdapterError("worker source ancestry crosses sessions") from error
            visible_ids.add(current.id)
            parent_event_id = current.parent_event_id
            if parent_event_id is None:
                current = None
                continue
            current = store.get(parent_event_id)
            if current is None:
                raise AgentAdapterError("worker source parent event is missing") from error

    created: list[Event] = []
    for proposal in proposals:
        if not _allowed_worker_event(proposal.event_type):
            raise AgentAdapterError(f"worker is not authorized to emit {proposal.event_type.value}")
        missing = [
            event_id for event_id in proposal.source_event_ids if store.get(event_id) is None
        ]
        if missing:
            raise AgentAdapterError(f"worker cited unknown source events: {missing}")
        cross_session = [
            event_id
            for event_id in proposal.source_event_ids
            if store.require(event_id).session_id != source.session_id
        ]
        if cross_session:
            raise AgentAdapterError(f"worker cited events from another session: {cross_session}")
        invisible = [
            event_id for event_id in proposal.source_event_ids if event_id not in visible_ids
        ]
        if invisible:
            raise AgentAdapterError(
                f"worker cited events not visible at its assigned source: {invisible}"
            )
        payload = dict(proposal.payload)
        if actor_kind is ActorKind.WORKER:
            claimed_artifact_origin = payload.get("artifact_origin")
            if claimed_artifact_origin not in {None, "worker_generated"}:
                raise AgentAdapterError("coding worker may only emit worker_generated artifacts")
            if payload.get("material_origin") is not None:
                raise AgentAdapterError("coding worker may not claim an Oracle material origin")
        if proposal.event_type.value.startswith("analysis."):
            from oracle_lab.host import (
                HostAnalysisError,
                HostOutputValidator,
                ProposedAnalysis,
            )

            try:
                validated = HostOutputValidator().validate(
                    ProposedAnalysis(
                        proposal.event_type,
                        payload,
                        proposal.source_event_ids,
                        confidence=payload.get("confidence"),
                        rationale=payload.get("rationale"),
                    ),
                    existing_event_ids={event_id for event_id in proposal.source_event_ids},
                )
            except HostAnalysisError as exc:
                raise AgentAdapterError(str(exc)) from exc
            payload = dict(validated.payload)
        payload["source_event_ids"] = list(proposal.source_event_ids)
        metadata: dict[str, Any] = {"schema_version": 1}
        if actor_kind is ActorKind.WORKER:
            payload["artifact_origin"] = "worker_generated"
            metadata["artifact_origin"] = "worker_generated"
        if worker_run_id is not None:
            payload["worker_run_id"] = worker_run_id
        for key in (
            "automation_depth",
            "automation_budget_remaining",
            "automation_loop_detector",
            "loop_signature",
        ):
            if key in source.payload and key not in payload:
                payload[key] = source.payload[key]
        event = Event.new(
            proposal.event_type,
            actor=Actor(kind=actor_kind, id=actor_id),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload=payload,
            metadata=metadata,
        )
        created.append(event)
    return tuple(created)


def ingest_structured_events(
    proposals: Sequence[StructuredWorkerEvent],
    *,
    source: Event,
    store: Any,
    actor_kind: ActorKind,
    actor_id: str,
    worker_run_id: str | None = None,
) -> tuple[Event, ...]:
    """Validate all worker proposals before atomically appending any of them."""

    prepared = prepare_structured_events(
        proposals,
        source=source,
        store=store,
        actor_kind=actor_kind,
        actor_id=actor_id,
        worker_run_id=worker_run_id,
    )
    return store.append_many(prepared)


class BaseAgentAdapter:
    name = "agent"

    def __init__(
        self,
        *,
        executable: str,
        command_builder: CommandBuilder,
        workspace_factory: WorkspaceFactory | None = None,
        timeout_seconds: float = 300,
        max_output_bytes: int = 4 * 1024 * 1024,
        environment: Mapping[str, str] | None = None,
        profile: WorkerExecutionProfile | None = None,
        repository_workspace_root: str | Path | None = None,
        isolation_binding: CodingWorkerIsolationBinding | None = None,
    ) -> None:
        if profile is not None:
            executable = profile.executable
            timeout_seconds = profile.timeout_seconds
            max_output_bytes = profile.max_output_bytes
            if environment is None:
                environment = profile.resolved_environment()
        self.executable = executable
        self.command_builder = command_builder
        self.workspace_factory = workspace_factory or DedicatedWorkspace()
        if timeout_seconds <= 0:
            raise AgentAdapterError("agent timeout_seconds must be positive")
        if max_output_bytes <= 0:
            raise AgentAdapterError("agent max_output_bytes must be positive")
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes
        self.environment = None if environment is None else dict(environment)
        self.profile = profile
        self.isolation_binding = isolation_binding
        self.repository_workspace_root = (
            None if repository_workspace_root is None else Path(repository_workspace_root)
        )

    def _environment(self) -> dict[str, str]:
        if self.environment is not None:
            return dict(self.environment)
        allowed = ("PATH", "LANG", "LC_ALL", "TERM")
        return {key: os.environ[key] for key in allowed if key in os.environ}

    @staticmethod
    def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
        """Kill the worker and children created in its dedicated process group."""

        if os.name == "posix":
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(ProcessLookupError, OSError):
            process.kill()

    def _run_bounded_process(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        input_bytes: bytes,
        environment: Mapping[str, str],
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> _BoundedProcessResult:
        """Run one worker without ever buffering more than the configured output cap."""

        if timeout_seconds <= 0 or max_output_bytes <= 0:
            raise AgentAdapterError("worker process limits must be positive")
        with tempfile.TemporaryFile() as input_handle:
            input_handle.write(input_bytes)
            input_handle.flush()
            input_handle.seek(0)
            process = subprocess.Popen(
                list(command),
                cwd=cwd,
                stdin=input_handle,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=dict(environment),
                start_new_session=True,
            )
            assert process.stdout is not None and process.stderr is not None
            selector = selectors.DefaultSelector()
            streams = {
                "stdout": process.stdout,
                "stderr": process.stderr,
            }
            buffers = {
                "stdout": bytearray(),
                "stderr": bytearray(),
            }
            for name, stream in streams.items():
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)

            timed_out = False
            output_limited = False
            deadline = time.monotonic() + timeout_seconds

            def consume_ready(key: selectors.SelectorKey) -> None:
                nonlocal output_limited
                captured = len(buffers["stdout"]) + len(buffers["stderr"])
                remaining_capacity = max_output_bytes - captured
                read_size = (
                    65_536 if output_limited else min(65_536, max(1, remaining_capacity + 1))
                )
                try:
                    block = os.read(key.fileobj.fileno(), read_size)
                except BlockingIOError:
                    return
                if not block:
                    with contextlib.suppress(KeyError):
                        selector.unregister(key.fileobj)
                    return
                buffers[str(key.data)].extend(block[: max(0, remaining_capacity)])
                if len(block) > remaining_capacity:
                    output_limited = True

            try:
                while selector.get_map() and not output_limited:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        timed_out = True
                        break
                    for key, _mask in selector.select(min(remaining, 0.1)):
                        consume_ready(key)
                        if output_limited:
                            break

                if not timed_out and not output_limited:
                    remaining = deadline - time.monotonic()
                    try:
                        process.wait(timeout=max(0.001, remaining))
                    except subprocess.TimeoutExpired:
                        timed_out = True

                if process.returncode is not None and not timed_out and not output_limited:
                    # A successful CLI may still leave background descendants.
                    # The run boundary owns the whole group, so no child may
                    # survive after its foreground process exits.
                    self._terminate_process_group(process)

                if timed_out or output_limited:
                    self._terminate_process_group(process)
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        self._terminate_process_group(process)
                        process.wait(timeout=2)

                # Preserve bytes already written into the pipes before the group
                # was stopped. Capacity remains hard-bounded during this drain.
                drain_deadline = time.monotonic() + 1.0
                while selector.get_map() and time.monotonic() < drain_deadline:
                    ready = selector.select(0.05)
                    if not ready:
                        continue
                    for key, _mask in ready:
                        consume_ready(key)
                if process.poll() is None:
                    self._terminate_process_group(process)
                    process.wait(timeout=2)
            finally:
                selector.close()
                process.stdout.close()
                process.stderr.close()

        return _BoundedProcessResult(
            exit_code=None if timed_out else process.returncode,
            stdout=bytes(buffers["stdout"]),
            stderr=bytes(buffers["stderr"]),
            timed_out=timed_out,
            output_limited=output_limited,
        )

    @staticmethod
    def _hash_source_tree(repository: Path) -> str:
        """Hash every source-worktree entry except Git's private metadata."""

        digest = hashlib.sha256()

        def frame(kind: bytes, relative: Path, payload: bytes) -> None:
            relative_bytes = os.fsencode(str(relative))
            digest.update(kind)
            digest.update(len(relative_bytes).to_bytes(8, "big"))
            digest.update(relative_bytes)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)

        def hash_entry(path: Path, relative: Path) -> None:
            details = path.lstat()
            mode = stat.S_IMODE(details.st_mode).to_bytes(4, "big")
            if stat.S_ISLNK(details.st_mode):
                frame(b"L", relative, mode + os.fsencode(os.readlink(path)))
                return
            if stat.S_ISDIR(details.st_mode):
                frame(b"D", relative, mode)
                return
            if stat.S_ISREG(details.st_mode):
                header = mode + details.st_size.to_bytes(8, "big")
                frame(b"F", relative, header)
                with path.open("rb") as handle:
                    while block := handle.read(1024 * 1024):
                        digest.update(block)
                return
            frame(b"S", relative, mode + int(details.st_rdev).to_bytes(8, "big"))

        def fail_walk(error: OSError) -> None:
            raise AgentAdapterError(
                f"source worktree fingerprint is incomplete: {type(error).__name__}"
            ) from error

        for raw_root, directory_names, file_names in os.walk(
            repository,
            topdown=True,
            onerror=fail_walk,
            followlinks=False,
        ):
            root = Path(raw_root)
            relative_root = root.relative_to(repository)
            hash_entry(root, relative_root)
            if relative_root == Path("."):
                directory_names[:] = [name for name in directory_names if name != ".git"]
                file_names[:] = [name for name in file_names if name != ".git"]
            directory_names.sort()
            file_names.sort()
            for name in tuple(directory_names):
                path = root / name
                hash_entry(path, path.relative_to(repository))
                if path.is_symlink():
                    directory_names.remove(name)
            for name in file_names:
                path = root / name
                hash_entry(path, path.relative_to(repository))
        return digest.hexdigest()

    def _source_worktree_audit(
        self,
        repository: Path,
        *,
        git_directory: Path,
        common_directory: Path,
        index_path: Path,
        known_head: str | None = None,
        known_git_control_sha256: str | None = None,
    ) -> _SourceWorktreeAudit:
        """Inspect source state without status/diff/filter or hook execution."""

        head = known_head
        if head is None:
            head = (
                self._required_git(
                    repository,
                    "rev-parse",
                    "--verify",
                    "HEAD^{commit}",
                )
                .decode("ascii")
                .strip()
            )
        if index_path.is_symlink() or not index_path.is_file():
            raise AgentAdapterError("source Git index is missing or is a symlink")
        try:
            index_bytes = index_path.read_bytes()
        except OSError as error:
            raise AgentAdapterError(
                f"source Git index cannot be read: {type(error).__name__}"
            ) from error
        index_sha256 = hashlib.sha256(index_bytes).hexdigest()
        snapshot_sha256 = self._hash_source_tree(repository)
        try:
            git_control_sha256 = known_git_control_sha256 or fingerprint_git_control(
                git_directory,
                common_directory=common_directory,
            )
        except GitControlError as error:
            raise AgentAdapterError(str(error)) from error
        # Preserve the legacy status-hash archive field without invoking
        # status, which can execute clean filters or fsmonitor configured by a
        # repository.  Index and full filesystem drift have dedicated hashes;
        # this field remains a non-executing HEAD identity marker.
        status = head.encode("ascii")
        return _SourceWorktreeAudit(
            head=head,
            status=status,
            index_sha256=index_sha256,
            snapshot_sha256=snapshot_sha256,
            git_control_sha256=git_control_sha256,
        )

    @staticmethod
    def _git(
        repository: Path,
        *arguments: str,
        input_bytes: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        try:
            return run_git(repository, *arguments, input_bytes=input_bytes, timeout=30)
        except GitControlError as error:
            raise AgentAdapterError(str(error)) from error

    @classmethod
    def _required_git(
        cls,
        repository: Path,
        *arguments: str,
        input_bytes: bytes | None = None,
    ) -> bytes:
        result = cls._git(repository, *arguments, input_bytes=input_bytes)
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", "replace").strip()
            raise AgentAdapterError(
                f"git {' '.join(arguments)} failed: {detail or result.returncode}"
            )
        return bytes(result.stdout)

    def _executable_version(
        self,
        executable: str,
        *,
        workspace: Path,
    ) -> tuple[str | None, str]:
        try:
            result = self._run_bounded_process(
                (executable, "--version"),
                cwd=workspace,
                input_bytes=b"",
                environment=self._environment(),
                timeout_seconds=min(10.0, self.timeout_seconds),
                max_output_bytes=min(4096, self.max_output_bytes),
            )
        except OSError:
            return None, "unknown"
        text = (result.stdout or result.stderr).decode("utf-8", "replace").strip()
        if result.exit_code != 0 or result.timed_out or result.output_limited or not text:
            return None, "unknown"
        return text[:4096], "reported"

    def _capture_repository_patch(
        self,
        workspace: Path,
        *,
        base_commit: str,
    ) -> tuple[
        str,
        bytes,
        tuple[str, ...],
        dict[str, str | None],
        dict[str, str | None],
    ]:
        head = self._required_git(workspace, "rev-parse", "--verify", "HEAD").decode().strip()
        self._required_git(workspace, "add", "--all", "--")
        patch = self._required_git(
            workspace,
            "diff",
            "--cached",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            base_commit,
            "--",
        )
        raw_paths = self._required_git(
            workspace,
            "diff",
            "--cached",
            "--name-only",
            "-z",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            base_commit,
            "--",
        )
        try:
            changed_paths = tuple(
                value.decode("utf-8") for value in raw_paths.split(b"\0") if value
            )
        except UnicodeDecodeError as error:
            raise AgentAdapterError("candidate patch contains a non-UTF-8 path") from error
        preconditions: dict[str, str | None] = {}
        modes: dict[str, str | None] = {}
        for path in changed_paths:
            original = self._git(workspace, "show", f"{base_commit}:{path}")
            preconditions[path] = (
                hashlib.sha256(original.stdout).hexdigest() if original.returncode == 0 else None
            )
            index_entry = self._git(workspace, "ls-files", "-s", "--", path)
            first = index_entry.stdout.split(maxsplit=1)[0] if index_entry.returncode == 0 else b""
            modes[path] = first.decode("ascii") if first else None
        return head, patch, changed_paths, preconditions, modes

    def run(self, task: WorkerTask) -> AgentRunResult:
        prompt = task.render()
        environment = self._environment()
        if self.isolation_binding is None:
            executable = shutil.which(self.executable, path=environment.get("PATH"))
            if executable is None:
                raise AgentAdapterError(f"{self.name} executable is unavailable: {self.executable}")
        else:
            if Path(self.executable).name != self.executable:
                raise AgentAdapterError(
                    "brokered coding-worker executable must be a guest command name"
                )
            executable = self.executable
        started = time.monotonic()
        repository: Path | None = None
        base_commit: str | None = None
        source_before: _SourceWorktreeAudit | None = None
        source_after: _SourceWorktreeAudit | None = None
        source_git_directory: Path | None = None
        source_common_directory: Path | None = None
        source_index_path: Path | None = None
        workspace_factory = self.workspace_factory
        if task.task_kind == "repository_edit":
            assert task.repository is not None
            repository = Path(task.repository).expanduser().resolve()
            top_level = (
                self._required_git(
                    repository,
                    "rev-parse",
                    "--show-toplevel",
                )
                .decode()
                .strip()
            )
            if Path(top_level).resolve() != repository:
                raise AgentAdapterError("repository_edit requires the repository top-level path")
            revision = task.base_commit or "HEAD"
            base_commit = (
                self._required_git(
                    repository,
                    "rev-parse",
                    "--verify",
                    f"{revision}^{{commit}}",
                )
                .decode()
                .strip()
            )
            source_git_directory = Path(
                self._required_git(
                    repository,
                    "rev-parse",
                    "--path-format=absolute",
                    "--absolute-git-dir",
                )
                .decode("utf-8", "strict")
                .strip()
            ).resolve(strict=True)
            source_common_directory = Path(
                self._required_git(
                    repository,
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-common-dir",
                )
                .decode("utf-8", "strict")
                .strip()
            ).resolve(strict=True)
            source_index_path = Path(
                self._required_git(
                    repository,
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-path",
                    "index",
                )
                .decode("utf-8", "strict")
                .strip()
            ).resolve(strict=False)
            source_before = self._source_worktree_audit(
                repository,
                git_directory=source_git_directory,
                common_directory=source_common_directory,
                index_path=source_index_path,
            )
            workspace_factory = GitStandaloneCloneWorkspace(
                repository,
                root=self.repository_workspace_root,
                revision=base_commit,
            )

        executable_version: str | None = None
        version_status = "unknown"
        if self.isolation_binding is None:
            with DedicatedWorkspace(
                self.repository_workspace_root, prefix="oracle-version-"
            )() as version_workspace:
                executable_version, version_status = self._executable_version(
                    executable,
                    workspace=version_workspace,
                )

        stdout_raw = b""
        stderr_raw = b""
        exit_code: int | None = None
        timed_out = False
        output_limited = False
        patch_bytes = b""
        changed_paths: tuple[str, ...] = ()
        preconditions: dict[str, str | None] = {}
        changed_modes: dict[str, str | None] = {}
        workspace_head: str | None = None
        worker_committed = False
        worker_git_control_tampered = False
        isolation_attestation: Mapping[str, Any] | None = None
        isolation_sandbox_id: str | None = None
        isolation_cleanup_confirmed: bool | None = None
        workspace_export_sha256: str | None = None
        workspace_export_bytes: int | None = None
        workspace_export_entries: int | None = None
        isolated_worker_failed = False
        workspace_value = ""
        with workspace_factory() as workspace:
            workspace_value = str(workspace)
            workspace_git_directory = workspace / ".git" if repository is not None else None
            workspace_control_before: str | None = None
            if workspace_git_directory is not None:
                try:
                    workspace_control_before = fingerprint_git_control(workspace_git_directory)
                except GitControlError as error:
                    raise AgentAdapterError(str(error)) from error
            command = list(self.command_builder(prompt))
            if not command:
                raise AgentAdapterError("agent command builder returned no arguments")
            command[0] = executable
            if self.isolation_binding is None:
                completed = self._run_bounded_process(
                    command,
                    cwd=workspace,
                    input_bytes=prompt.encode("utf-8"),
                    environment=environment,
                    timeout_seconds=self.timeout_seconds,
                    max_output_bytes=self.max_output_bytes,
                )
            else:
                profile = self.profile
                if profile is None or profile.isolation_attestation is None:
                    raise AgentAdapterError(
                        "brokered coding worker has no frozen isolation attestation"
                    )
                try:
                    isolated = self.isolation_binding.run(
                        IsolationRunRequest(
                            adapter=self.name,
                            workspace=workspace.resolve(strict=True),
                            command=tuple(command),
                            input_bytes=prompt.encode("utf-8"),
                            environment=environment,
                            timeout_seconds=self.timeout_seconds,
                            max_output_bytes=self.max_output_bytes,
                            max_workspace_export_bytes=profile.max_workspace_export_bytes,
                            max_workspace_entries=profile.max_workspace_entries,
                        )
                    )
                except IsolationRunFailed as failure:
                    if failure.max_output_bytes != self.max_output_bytes:
                        raise AgentAdapterError(
                            "coding-worker failure output bound drifted"
                        ) from failure
                    actual_attestation = failure.attestation.to_dict()
                    if canonical_json(actual_attestation) != canonical_json(
                        dict(profile.isolation_attestation)
                    ):
                        raise AgentAdapterError(
                            "coding-worker isolation attestation drifted"
                        ) from failure
                    command = list(failure.actual_command)
                    completed = _BoundedProcessResult(
                        exit_code=failure.exit_code,
                        stdout=failure.stdout,
                        stderr=failure.stderr,
                        timed_out=failure.timed_out,
                        output_limited=failure.output_limited,
                    )
                    executable = failure.guest_executable_path or self.executable
                    executable_version = failure.guest_executable_version
                    version_status = failure.guest_executable_version_status
                    isolation_attestation = actual_attestation
                    isolation_sandbox_id = failure.sandbox_id
                    isolation_cleanup_confirmed = failure.cleanup_confirmed
                    isolated_worker_failed = True
                except CodingIsolationError as error:
                    raise AgentAdapterError(str(error)) from error
                else:
                    actual_attestation = isolated.attestation.to_dict()
                    if canonical_json(actual_attestation) != canonical_json(
                        dict(profile.isolation_attestation)
                    ):
                        raise AgentAdapterError("coding-worker isolation attestation drifted")
                    archive_limits = WorkspaceArchiveLimits(
                        max_raw_bytes=profile.max_workspace_export_bytes,
                        max_entries=profile.max_workspace_entries,
                        max_regular_payload_bytes=profile.max_workspace_export_bytes,
                    )
                    try:
                        workspace_export = validate_workspace_archive(
                            isolated.workspace_export,
                            archive_limits,
                        )
                    except WorkspaceArchiveError as error:
                        raise AgentAdapterError(
                            f"coding-worker workspace export is invalid: {error}"
                        ) from error
                    if (
                        workspace_export.sha256 != isolated.workspace_export_sha256
                        or workspace_export.size_bytes != isolated.workspace_export_bytes
                        or workspace_export.entry_count != isolated.workspace_export_entries
                    ):
                        raise AgentAdapterError(
                            "coding-worker workspace export counters do not match verified bytes"
                        )
                    if repository is not None:
                        export_parent = self.repository_workspace_root
                        if export_parent is not None:
                            export_parent.mkdir(parents=True, exist_ok=True)
                        with tempfile.TemporaryDirectory(
                            prefix="oracle-export-quarantine-",
                            dir=export_parent,
                        ) as raw_export_parent:
                            exported_tree = Path(raw_export_parent) / "workspace"
                            try:
                                materialize_workspace_export(
                                    workspace_export,
                                    exported_tree,
                                )
                                replace_worktree_from_untrusted(exported_tree, workspace)
                            except (GitControlError, WorkspaceArchiveError) as error:
                                raise AgentAdapterError(
                                    f"coding-worker workspace export could not be imported: {error}"
                                ) from error
                    command = list(isolated.actual_command)
                    completed = _BoundedProcessResult(
                        exit_code=isolated.exit_code,
                        stdout=isolated.stdout,
                        stderr=isolated.stderr,
                        timed_out=isolated.timed_out,
                        output_limited=isolated.output_limited,
                    )
                    executable = isolated.guest_executable_path or self.executable
                    executable_version = isolated.guest_executable_version
                    version_status = isolated.guest_executable_version_status
                    isolation_attestation = actual_attestation
                    isolation_sandbox_id = isolated.sandbox_id
                    isolation_cleanup_confirmed = isolated.cleanup_confirmed
                    workspace_export_sha256 = isolated.workspace_export_sha256
                    workspace_export_bytes = isolated.workspace_export_bytes
                    workspace_export_entries = isolated.workspace_export_entries
            stdout_raw = completed.stdout
            stderr_raw = completed.stderr
            exit_code = completed.exit_code
            timed_out = completed.timed_out
            output_limited = completed.output_limited
            if repository is not None and base_commit is not None:
                assert workspace_git_directory is not None
                assert source_before is not None
                assert source_git_directory is not None
                assert source_common_directory is not None
                assert source_index_path is not None

                # This raw check happens before any post-worker Git command.  A
                # modified config, hook, ref, object, or other Git control file
                # is never loaded by the Host.
                try:
                    workspace_control_after = fingerprint_git_control(workspace_git_directory)
                except GitControlError:
                    workspace_control_after = None
                workspace_head = detached_head_from_control(workspace_git_directory)
                worker_git_control_tampered = (
                    workspace_control_after is None
                    or workspace_control_after != workspace_control_before
                    or workspace_head is None
                )
                worker_committed = (
                    workspace_head is not None
                    and workspace_head.casefold() != base_commit.casefold()
                )

                # Inspect source Git control bytes before invoking Git again.
                # If a worker reached into the source .git directory, no
                # repository-config-aware command is allowed to follow.
                try:
                    source_control_after_worker = fingerprint_git_control(
                        source_git_directory,
                        common_directory=source_common_directory,
                    )
                except GitControlError as error:
                    source_control_after_worker = hashlib.sha256(
                        f"unreadable:{type(error).__name__}".encode("ascii")
                    ).hexdigest()
                source_control_safe = (
                    source_control_after_worker == source_before.git_control_sha256
                )
                source_after = self._source_worktree_audit(
                    repository,
                    git_directory=source_git_directory,
                    common_directory=source_common_directory,
                    index_path=source_index_path,
                    known_head=None if source_control_safe else source_before.head,
                    known_git_control_sha256=source_control_after_worker,
                )

                # Reconstruct the patch in a second clone whose Git directory
                # was never exposed to the worker.  Only ordinary filesystem
                # entries cross this boundary; `.git` is excluded by name.
                if source_control_safe and not isolated_worker_failed:
                    worker_tree_before_copy = self._hash_source_tree(workspace)
                    capture_factory = GitStandaloneCloneWorkspace(
                        repository,
                        root=self.repository_workspace_root,
                        revision=base_commit,
                    )
                    with capture_factory() as capture_workspace:
                        try:
                            replace_worktree_from_untrusted(workspace, capture_workspace)
                        except GitControlError as error:
                            raise AgentAdapterError(str(error)) from error
                        if self._hash_source_tree(workspace) != worker_tree_before_copy:
                            worker_git_control_tampered = True
                        (
                            capture_head,
                            patch_bytes,
                            changed_paths,
                            preconditions,
                            changed_modes,
                        ) = self._capture_repository_patch(
                            capture_workspace,
                            base_commit=base_commit,
                        )
                        if capture_head.casefold() != base_commit.casefold():
                            raise AgentAdapterError("trusted capture clone changed its base commit")

                    # Bundle/clone must also be observationally read-only with
                    # respect to the source repository.
                    try:
                        final_control = fingerprint_git_control(
                            source_git_directory,
                            common_directory=source_common_directory,
                        )
                    except GitControlError as error:
                        final_control = hashlib.sha256(
                            f"unreadable:{type(error).__name__}".encode("ascii")
                        ).hexdigest()
                    final_control_safe = final_control == source_before.git_control_sha256
                    source_after = self._source_worktree_audit(
                        repository,
                        git_directory=source_git_directory,
                        common_directory=source_common_directory,
                        index_path=source_index_path,
                        known_head=None if final_control_safe else source_before.head,
                        known_git_control_sha256=final_control,
                    )

        source_unchanged = (
            None if source_before is None or source_after is None else source_before == source_after
        )
        available = self.max_output_bytes
        stdout_view = stdout_raw[:available]
        available -= len(stdout_view)
        stderr_view = stderr_raw[: max(0, available)]
        stdout = stdout_view.decode("utf-8", "replace")
        stderr = stderr_view.decode("utf-8", "replace")
        events = (
            parse_structured_events(
                stdout,
                expected_source_event_id=task.source_event.id,
            )
            if exit_code == 0
            and not timed_out
            and not output_limited
            and source_unchanged is not False
            and not worker_committed
            and not worker_git_control_tampered
            and task.task_kind != "repository_edit"
            else ()
        )
        return AgentRunResult(
            self.name,
            tuple(command),
            exit_code,
            stdout,
            stderr,
            (time.monotonic() - started) * 1000,
            events,
            workspace_value,
            timed_out=timed_out,
            output_limited=output_limited,
            prompt=prompt,
            stdout_bytes=stdout_raw,
            stderr_bytes=stderr_raw,
            executable_path=executable,
            executable_version=executable_version,
            executable_version_status=version_status,
            base_commit=base_commit,
            workspace_head=workspace_head,
            patch_bytes=patch_bytes,
            changed_paths=changed_paths,
            precondition_sha256=preconditions,
            changed_modes=changed_modes,
            source_status_before_sha256=(
                None if source_before is None else hashlib.sha256(source_before.status).hexdigest()
            ),
            source_status_after_sha256=(
                None if source_after is None else hashlib.sha256(source_after.status).hexdigest()
            ),
            source_head_before=None if source_before is None else source_before.head,
            source_head_after=None if source_after is None else source_after.head,
            source_index_before_sha256=(
                None if source_before is None else source_before.index_sha256
            ),
            source_index_after_sha256=(None if source_after is None else source_after.index_sha256),
            source_snapshot_before_sha256=(
                None if source_before is None else source_before.snapshot_sha256
            ),
            source_snapshot_after_sha256=(
                None if source_after is None else source_after.snapshot_sha256
            ),
            source_git_control_before_sha256=(
                None if source_before is None else source_before.git_control_sha256
            ),
            source_git_control_after_sha256=(
                None if source_after is None else source_after.git_control_sha256
            ),
            source_worktree_unchanged=source_unchanged,
            worker_committed=worker_committed,
            worker_git_control_tampered=worker_git_control_tampered,
            isolation_attestation=isolation_attestation,
            isolation_sandbox_id=isolation_sandbox_id,
            isolation_cleanup_confirmed=isolation_cleanup_confirmed,
            workspace_export_sha256=workspace_export_sha256,
            workspace_export_bytes=workspace_export_bytes,
            workspace_export_entries=workspace_export_entries,
        )

    def ingest(
        self,
        result: AgentRunResult,
        *,
        source: Event,
        store: Any,
        worker_run_id: str | None = None,
    ) -> tuple[Event, ...]:
        if not result.succeeded:
            raise AgentAdapterError("cannot ingest events from a failed agent run")
        return ingest_structured_events(
            result.events,
            source=source,
            store=store,
            actor_kind=ActorKind.WORKER,
            actor_id=self.name,
            worker_run_id=worker_run_id,
        )


def _opencode_command(
    prompt: str,
    profile: WorkerExecutionProfile | None = None,
) -> Sequence[str]:
    command = ["opencode", "run", "--format", "json"]
    if profile is not None and profile.model:
        command.extend(("--model", profile.model))
    command.append(prompt)
    return tuple(command)


def _validated_opencode_wrapper(executable: str) -> str:
    """Return an absolute executable wrapper path or fail closed."""

    wrapper = Path(executable)
    if not wrapper.is_absolute():
        raise AgentAdapterError("OpenCode sandbox wrapper executable must be an absolute path")
    if wrapper.is_symlink() or not wrapper.is_file() or not os.access(wrapper, os.X_OK):
        raise AgentAdapterError(
            "OpenCode sandbox wrapper must be a real executable file, not a symlink"
        )
    if wrapper.name.lower() in {"opencode", "opencode.exe"}:
        raise AgentAdapterError(
            "OpenCode executable must be an external sandbox wrapper, not opencode itself"
        )
    return str(wrapper)


def _codex_command(
    prompt: str,
    profile: WorkerExecutionProfile | None = None,
    *,
    externally_isolated: bool = False,
) -> Sequence[str]:
    del prompt
    command = ["codex", "exec", "--json"]
    if profile is not None:
        if externally_isolated:
            command.extend(
                (
                    "--ephemeral",
                    "--ignore-user-config",
                    "--ignore-rules",
                    "--dangerously-bypass-approvals-and-sandbox",
                    "--color",
                    "never",
                )
            )
        else:
            command.extend(("--sandbox", profile.sandbox_profile))
        if profile.model:
            command.extend(("--model", profile.model))
    command.append("-")
    return tuple(command)


class OpenCodeAdapter(BaseAgentAdapter):
    name = "opencode"

    def __init__(self, **kwargs: Any) -> None:
        profile = kwargs.get("profile")
        binding = kwargs.get("isolation_binding")
        if binding is None and (
            profile is None or profile.sandbox_profile != "external-sandbox-wrapper"
        ):
            raise AgentAdapterError(
                "OpenCode requires an external-sandbox-wrapper execution profile"
            )
        if binding is None:
            kwargs["executable"] = _validated_opencode_wrapper(profile.executable)
        kwargs.setdefault("executable", "opencode")
        kwargs.setdefault(
            "command_builder",
            lambda prompt: _opencode_command(prompt, profile),
        )
        super().__init__(**kwargs)


class CodexAdapter(BaseAgentAdapter):
    name = "codex"

    def __init__(self, **kwargs: Any) -> None:
        profile = kwargs.get("profile")
        binding = kwargs.get("isolation_binding")
        kwargs.setdefault("executable", "codex")
        kwargs.setdefault(
            "command_builder",
            lambda prompt: _codex_command(
                prompt,
                profile,
                externally_isolated=binding is not None,
            ),
        )
        super().__init__(**kwargs)


DirectCall = Callable[[str, Mapping[str, Any]], Any]


@dataclass(frozen=True, slots=True)
class DirectHostResult:
    task_type: str
    output: Mapping[str, Any]
    elapsed_ms: float
    events: tuple[StructuredWorkerEvent, ...] = ()
    prompt: str = ""
    raw_response: bytes = b""
    requested_provider_id: str | None = None
    requested_model: str | None = None
    actual_provider: str | None = None
    returned_model: str | None = None
    routing_settings: Mapping[str, Any] = field(default_factory=dict)
    sampling_settings: Mapping[str, Any] = field(default_factory=dict)
    api_response_metadata: Mapping[str, Any] = field(default_factory=dict)
    usage: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", _freeze_worker_value(self.output))
        object.__setattr__(self, "raw_response", bytes(self.raw_response))
        for field_name in (
            "routing_settings",
            "sampling_settings",
            "api_response_metadata",
            "usage",
        ):
            object.__setattr__(self, field_name, _freeze_worker_value(getattr(self, field_name)))


class DirectAPIHost:
    """Lightweight host path for extraction/classification work.

    The injected callable owns the concrete frontier-model API.  Keeping this
    adapter protocol-only prevents any oracle provider assumptions from leaking
    into host analysis.
    """

    allowed_tasks = frozenset(
        {
            "classification",
            "claim_extraction",
            "embedding",
            "novelty_analysis",
        }
    )

    name = "direct"

    def __init__(
        self,
        call: DirectCall,
        *,
        profile: WorkerExecutionProfile | None = None,
    ) -> None:
        self.call = call
        # In-process test/custom callables remain an explicit injection seam.
        # Unknown provider/model facts stay unknown rather than being borrowed
        # from the Oracle model registry.
        self.profile = profile or WorkerExecutionProfile(
            id="injected-direct",
            adapter="direct",
            executable="direct-api",
            model=None,
            sandbox_profile="api-only",
        )

    @staticmethod
    def render_prompt(task_type: str, payload: Mapping[str, Any]) -> str:
        """Make the non-Oracle Host contract an exact, archived prompt."""

        from oracle_lab.host_provider import HOST_PROMPT_CONTRACT_VERSION

        task_prompt = payload.get("prompt")
        if not isinstance(task_prompt, str):
            task_prompt = canonical_json(dict(payload))
        return (
            f"HOST_PROMPT_CONTRACT={HOST_PROMPT_CONTRACT_VERSION}\n"
            "You are a Host analysis worker, not the Oracle.\n"
            "Do not write oracle output or promote, keep, star, canonize, or mutate world state.\n"
            "Return exactly one JSON object matching the analysis event envelope in TASK_PROMPT.\n"
            f"HOST_TASK_TYPE={task_type}\n"
            "TASK_PROMPT_BEGIN\n"
            f"{task_prompt}\n"
            "TASK_PROMPT_END\n"
        )

    async def run(self, task_type: str, payload: Mapping[str, Any]) -> DirectHostResult:
        if task_type not in self.allowed_tasks:
            raise AgentAdapterError(f"direct API host does not accept task: {task_type}")
        prompt = self.render_prompt(task_type, payload)
        call_payload = {**dict(payload), "prompt": prompt}
        started = time.monotonic()
        result = self.call(task_type, call_payload)
        if inspect.isawaitable(result):
            result = await result
        from oracle_lab.host_provider import HostProviderResponse

        if isinstance(result, HostProviderResponse):
            output = result.output
            raw_response = result.raw_response
            requested_provider_id = result.requested_provider_id
            requested_model = result.requested_model
            actual_provider = result.actual_provider
            returned_model = result.returned_model
            routing_settings = result.routing_settings
            sampling_settings = result.sampling_settings
            api_response_metadata = result.api_response_metadata
            usage = result.usage
            elapsed_ms = result.elapsed_ms
        elif isinstance(result, Mapping):
            output = result
            raw_response = canonical_json(result).encode("utf-8")
            profile = self.profile
            requested_provider_id = getattr(profile, "host_provider_id", None)
            requested_model = getattr(profile, "model", None)
            actual_provider = None
            returned_model = None
            routing_settings = {}
            sampling_settings = {}
            api_response_metadata = {"transport": "injected_callable"}
            usage_value = result.get("usage")
            usage = usage_value if isinstance(usage_value, Mapping) else {}
            elapsed_ms = (time.monotonic() - started) * 1000
        else:
            raise AgentAdapterError("direct API host must return a structured object")
        events: tuple[StructuredWorkerEvent, ...] = ()
        if "events" in output:
            source_event_id = payload.get("source_event_id")
            if not isinstance(source_event_id, str) or not source_event_id:
                raise AgentAdapterError(
                    "direct API event output requires a source_event_id in the task payload"
                )
            events = parse_structured_events(
                canonical_json(output),
                expected_source_event_id=source_event_id,
            )
        return DirectHostResult(
            task_type,
            output,
            elapsed_ms,
            events,
            prompt=prompt,
            raw_response=raw_response,
            requested_provider_id=requested_provider_id,
            requested_model=requested_model,
            actual_provider=actual_provider,
            returned_model=returned_model,
            routing_settings=routing_settings,
            sampling_settings=sampling_settings,
            api_response_metadata=api_response_metadata,
            usage=usage,
        )

    def ingest(
        self,
        result: DirectHostResult,
        *,
        source: Event,
        store: Any,
    ) -> tuple[Event, ...]:
        return ingest_structured_events(
            result.events,
            source=source,
            store=store,
            actor_kind=ActorKind.HOST,
            actor_id="direct-api-host",
        )


class WorkerRouter:
    """Deterministically choose direct API versus isolated coding-agent workers."""

    direct_task_map = MappingProxyType(
        {
            "extract_claims": "claim_extraction",
            "detect_new_mechanisms": "classification",
            "extract_entities": "classification",
            "check_numeric_consistency": "classification",
            "detect_attractors": "classification",
            "detect_motifs": "classification",
            "detect_recurrence": "classification",
            "detect_tool_intent": "classification",
            "compare_claim_history": "classification",
            "propose_calculation": "classification",
            "novelty_analysis": "novelty_analysis",
        }
    )
    coding_task_types = frozenset(
        {
            "repository_edit",
            "host_analysis_implementation",
            "event_migration",
            "test_generation",
            "investigation",
        }
    )

    @property
    def supported_task_kinds(self) -> frozenset[str]:
        supported: set[str] = set()
        if self.direct is not None:
            supported.update(self.direct_task_map)
        if self.opencode is not None or self.codex is not None:
            supported.update(self.coding_task_types)
        return frozenset(supported)

    def __init__(
        self,
        *,
        direct: DirectAPIHost | None = None,
        opencode: OpenCodeAdapter | None = None,
        codex: CodexAdapter | None = None,
        prefer_coding_agent: str = "opencode",
        fallback_coding_agent: str | None = None,
    ) -> None:
        if prefer_coding_agent not in {"opencode", "codex"}:
            raise AgentAdapterError("preferred coding agent must be opencode or codex")
        self.direct = direct
        self.opencode = opencode
        self.codex = codex
        self.prefer_coding_agent = prefer_coding_agent
        if fallback_coding_agent not in {None, "opencode", "codex"}:
            raise AgentAdapterError("fallback coding agent must be opencode, codex, or absent")
        if fallback_coding_agent == prefer_coding_agent:
            raise AgentAdapterError("fallback coding agent must differ from the preferred agent")
        self.fallback_coding_agent = fallback_coding_agent

    def route(self, task_kind: str) -> tuple[str, Any]:
        direct_kind = self.direct_task_map.get(task_kind)
        if direct_kind is not None:
            if self.direct is None:
                raise AgentAdapterError(
                    f"direct API host is not configured for lightweight task: {task_kind}"
                )
            return direct_kind, self.direct
        if direct_kind is None and task_kind not in self.coding_task_types:
            raise AgentAdapterError(f"unsupported host worker task: {task_kind}")
        preferred = self.opencode if self.prefer_coding_agent == "opencode" else self.codex
        fallback = (
            self.opencode
            if self.fallback_coding_agent == "opencode"
            else self.codex
            if self.fallback_coding_agent == "codex"
            else None
        )
        adapter = preferred or fallback
        if adapter is None:
            raise AgentAdapterError(f"no worker is configured for task: {task_kind}")
        return task_kind, adapter


def build_worker_router(
    config: Any,
    *,
    workspace_root: str | Path,
    direct: DirectAPIHost | None = None,
    direct_http_client: Any | None = None,
    coding_worker_broker: CodingWorkerIsolationBroker | None = None,
) -> WorkerRouter | None:
    """Build only explicitly enabled adapters from the redacted runtime config."""

    if not bool(getattr(config, "enabled", False)):
        return None
    profiles = {
        worker_id: WorkerExecutionProfile.from_config(worker)
        for worker_id, worker in dict(getattr(config, "workers", {})).items()
        if bool(getattr(worker, "enabled", False))
    }
    if not profiles:
        raise AgentAdapterError("agents router is enabled but no worker is enabled")
    adapter_profiles: dict[str, list[str]] = {}
    for profile_id, profile in profiles.items():
        adapter_profiles.setdefault(profile.adapter, []).append(profile_id)
    ambiguous = {
        adapter_kind: tuple(sorted(profile_ids))
        for adapter_kind, profile_ids in adapter_profiles.items()
        if len(profile_ids) > 1
    }
    if ambiguous:
        raise AgentAdapterError(
            "multiple enabled worker profiles select the same adapter: "
            + ", ".join(
                f"{adapter_kind}={list(profile_ids)!r}"
                for adapter_kind, profile_ids in sorted(ambiguous.items())
            )
        )

    def adapter(kind: str) -> BaseAgentAdapter | None:
        profile = next(
            (item for item in profiles.values() if item.adapter == kind),
            None,
        )
        if profile is None:
            return None
        if coding_worker_broker is None:
            if kind == "codex" and profile.sandbox_profile not in {
                "read-only",
                "workspace-write",
                "external-broker",
            }:
                raise AgentAdapterError(
                    "Codex worker requires read-only, workspace-write, or "
                    "external-broker sandbox_profile"
                )
            if kind == "opencode" and profile.sandbox_profile not in {
                "external-sandbox-wrapper",
                "external-broker",
            }:
                raise AgentAdapterError("OpenCode requires an explicit external isolation profile")
            if kind == "opencode" and profile.sandbox_profile == "external-sandbox-wrapper":
                _validated_opencode_wrapper(profile.executable)
            # The worker/model is untrusted even though this profile was
            # written by a Human. CLI flags and wrapper paths are still not an
            # isolation attestation, so config remains fail-closed here.
            raise AgentAdapterError(
                f"{kind} worker profile {profile.id!r} cannot be enabled: "
                "OS-level coding-worker isolation broker is unavailable; "
                "CLI sandbox flags and wrapper paths are not isolation attestations"
            )
        if profile.sandbox_profile != "external-broker":
            raise AgentAdapterError(
                f"brokered {kind} worker requires sandbox_profile=external-broker"
            )
        try:
            binding = require_conforming_binding(coding_worker_broker.bind(profile))
        except (CodingIsolationError, OSError) as error:
            raise AgentAdapterError(f"{kind} worker isolation binding failed: {error}") from error
        bound_profile = profile.with_isolation_attestation(binding.attestation.to_dict())
        common = {
            "executable": bound_profile.executable,
            "profile": bound_profile,
            "workspace_factory": DedicatedWorkspace(workspace_root),
            "repository_workspace_root": workspace_root,
            "isolation_binding": binding,
        }
        if kind == "codex":
            return CodexAdapter(**common)
        if kind == "opencode":
            return OpenCodeAdapter(**common)
        raise AgentAdapterError(f"unsupported coding worker adapter: {kind}")

    preferred = str(getattr(config, "prefer_coding_agent", "codex"))
    preferred_profile = next(
        (profile for profile in profiles.values() if profile.adapter == preferred),
        None,
    )
    fallback = None if preferred_profile is None else preferred_profile.fallback_adapter
    fallback = (
        None if fallback is None or fallback not in profiles else profiles[str(fallback)].adapter
    )
    direct_enabled = any(profile.adapter == "direct" for profile in profiles.values())
    direct_profile = next(
        (profile for profile in profiles.values() if profile.adapter == "direct"),
        None,
    )
    if direct_enabled and direct is None:
        assert direct_profile is not None
        if (
            direct_profile.host_provider_kind != "openai_compatible"
            or not direct_profile.host_provider_id
            or not direct_profile.host_base_url
            or not direct_profile.model
        ):
            raise AgentAdapterError("direct worker Host provider profile is incomplete")
        from oracle_lab.host_provider import OpenAICompatibleHostCall

        direct = DirectAPIHost(
            OpenAICompatibleHostCall(
                provider_id=direct_profile.host_provider_id,
                base_url=direct_profile.host_base_url,
                api_key_env=direct_profile.host_api_key_env,
                model=direct_profile.model,
                temperature=direct_profile.host_temperature,
                top_p=direct_profile.host_top_p,
                max_tokens=direct_profile.host_max_tokens,
                allow_fallback=direct_profile.host_allow_fallback,
                timeout_seconds=direct_profile.timeout_seconds,
                max_output_bytes=direct_profile.max_output_bytes,
                client=direct_http_client,
            ),
            profile=direct_profile,
        )
    elif direct_enabled and direct is not None:
        if direct_profile is None or direct.profile is None:
            raise AgentAdapterError(
                "configured direct worker injection requires the matching execution profile"
            )
        if direct.profile.redacted_snapshot() != direct_profile.redacted_snapshot():
            raise AgentAdapterError("injected DirectAPIHost profile differs from config")
    return WorkerRouter(
        direct=direct if direct_enabled else None,
        codex=adapter("codex"),
        opencode=adapter("opencode"),
        prefer_coding_agent=preferred,
        fallback_coding_agent=fallback,
    )


HostWorkerRouter = WorkerRouter


__all__ = [
    "AgentAdapterError",
    "AgentRunResult",
    "BaseAgentAdapter",
    "CodexAdapter",
    "DedicatedWorkspace",
    "DirectAPIHost",
    "DirectHostResult",
    "GitStandaloneCloneWorkspace",
    "GitWorktreeWorkspace",
    "HostWorkerRouter",
    "OpenCodeAdapter",
    "StructuredWorkerEvent",
    "WorkerExecutionProfile",
    "WorkerRouter",
    "WorkerTask",
    "build_worker_router",
    "ingest_structured_events",
    "parse_structured_events",
    "prepare_structured_events",
]
