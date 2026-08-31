"""Application service facade shared by the CLI and TUI.

The facade keeps presentation code independent from provider, queue, and event
storage implementations.  All state-changing methods append events; the only
sidecar is the operator's active session/branch selection, which is control
plane state rather than experiment history.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any

from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.exporting import (
    export_research_bundle,
    export_selected_corpus,
    export_transcript,
)
from oracle_lab.git_control import (
    GitControlError,
    create_standalone_clone,
    remove_standalone_clone,
    run_git,
)
from oracle_lab.ids import new_id
from oracle_lab.jsonutil import canonical_json, sha256_bytes, sha256_json, sha256_text
from oracle_lab.material import is_synthetic_lineage, is_worker_lineage, material_origins
from oracle_lab.observability import ObservabilityService
from oracle_lab.retrieval import RetrievalDocument, RetrievalIndex
from oracle_lab.store import EventStore

_LATEX_START_RE = re.compile(
    r"\$\$|(?<!\$)\$(?!\$)(?=[^$\n]+\$)|\\\[|\\\(|"
    r"\\begin\s*\{[A-Za-z*]+\}|\\frac\s*\{"
)
_RESEARCH_WORD_RE = re.compile(r"[\w./:=°+\-]+", re.UNICODE)
_PROMPT_ATTRACTOR_PHRASES = (
    "証明",
    "定理",
    "反論",
    "観測記録",
    "報告書",
    "メモ",
    "実行",
    "確認",
    "疑似科学",
    "破滅",
    "救済",
    "フィクション",
    "詩",
    "寓話",
)


class ServiceError(RuntimeError):
    """Raised for an invalid control-plane operation."""


class NonRetryableWorkerError(ServiceError):
    """Raised when another worker attempt cannot add new information."""


class _AutomationLeaseHeartbeat:
    """Renew one automation lease until its handler durably acknowledges it."""

    def __init__(
        self,
        queue: Any,
        *,
        job_id: str,
        worker_id: str,
        lease_seconds: float,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._queue = queue
        self._job_id = job_id
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._interval_seconds = max(0.01, min(30.0, lease_seconds / 3.0))
        self._stop = threading.Event()
        self._failure_lock = threading.Lock()
        self._failure: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"oracle-lease-heartbeat-{job_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """Stop renewal and wait until no heartbeat thread remains."""

        self._stop.set()
        self._thread.join()

    def raise_if_failed(self) -> None:
        """Fail closed before acknowledging work after renewal failed."""

        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise ServiceError(
                f"lease heartbeat failed for job {self._job_id}: {failure}"
            ) from failure

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._queue.heartbeat(
                    self._job_id,
                    self._worker_id,
                    lease_seconds=self._lease_seconds,
                )
            except BaseException as error:
                with self._failure_lock:
                    self._failure = error
                self._stop.set()
                return


def _git_worktree_root(path: str | Path) -> Path | None:
    """Resolve a Git worktree root without inheriting Git control variables."""

    candidate = Path(path).expanduser().resolve(strict=False)
    try:
        result = run_git(candidate, "rev-parse", "--show-toplevel", timeout=10)
    except GitControlError:
        return None
    if result.returncode != 0:
        return None
    try:
        root = Path(result.stdout.decode("utf-8").strip()).resolve(strict=True)
    except (OSError, UnicodeDecodeError):
        return None
    return root if root.is_dir() and not root.is_symlink() else None


def _path_is_inside(path: str | Path, root: str | Path) -> bool:
    candidate = Path(path).expanduser().resolve(strict=False)
    boundary = Path(root).expanduser().resolve(strict=False)
    return candidate == boundary or candidate.is_relative_to(boundary)


def _external_data_home(*, protected_roots: Sequence[Path] = ()) -> Path:
    """Choose a persistent default outside every active Git worktree."""

    xdg = os.environ.get("XDG_DATA_HOME")
    candidates = []
    if xdg:
        candidates.append(Path(xdg).expanduser() / "oracle-lab")
    candidates.extend(
        (
            Path.home() / ".local" / "share" / "oracle-lab",
            Path.home() / "Library" / "Application Support" / "oracle-lab",
            Path(tempfile.gettempdir()) / f"oracle-lab-{getattr(os, 'getuid', lambda: 0)()}",
        )
    )
    for candidate in candidates:
        resolved = candidate.resolve(strict=False)
        if not any(_path_is_inside(resolved, root) for root in protected_roots):
            return resolved
    raise ServiceError("no Oracle Lab data root is outside the current Git worktree")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Event):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_jsonable(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _jsonable(to_dict())
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _jsonable(model_dump(mode="json"))
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return value


class OracleLabService:
    """Synchronous application boundary used by Typer and Textual."""

    def __init__(
        self,
        store: EventStore,
        *,
        home: str | Path,
        config_dir: str | Path = "config",
        archive_root: str | Path | None = None,
        rendering_root: str | Path | None = None,
        owns_store: bool = False,
        job_handler: Callable[[Any], Any] | None = None,
        provider_factory: Callable[[Any], Any] | None = None,
        host_worker_router: Any | None = None,
    ) -> None:
        self.store = store
        self.observability = ObservabilityService(store)
        self.home = Path(home).expanduser().resolve(strict=False)
        self.home.mkdir(parents=True, exist_ok=True)
        self.config_dir = Path(config_dir)
        self.archive_root = Path(
            archive_root
            if archive_root is not None
            else os.environ.get("ORACLE_LAB_ARCHIVE", str(self.home / "archive"))
        )
        self.rendering_root = Path(
            rendering_root
            if rendering_root is not None
            else os.environ.get("ORACLE_LAB_RENDERING", str(self.home / "rendering"))
        )
        self._staging_root_explicit = "ORACLE_LAB_STAGING" in os.environ
        self.staging_root = Path(os.environ.get("ORACLE_LAB_STAGING", str(self.home / "staging")))
        self.owns_store = owns_store
        self.job_handler = job_handler
        self.provider_factory = provider_factory
        self.host_worker_router = host_worker_router
        self._state_path = self.home / "control.json"
        self._state = self._read_control_state()
        self._config: Any = None
        self._tool_broker: Any = None

    @classmethod
    def default(cls) -> OracleLabService:
        current_root = _git_worktree_root(Path.cwd())
        protected_roots = () if current_root is None else (current_root,)
        raw_home = os.environ.get("ORACLE_LAB_HOME")
        home = (
            _external_data_home(protected_roots=protected_roots)
            if raw_home is None
            else Path(raw_home).expanduser().resolve(strict=False)
        )
        if any(_path_is_inside(home, root) for root in protected_roots):
            raise ServiceError(f"ORACLE_LAB_HOME must be outside the current Git worktree: {home}")
        home.mkdir(parents=True, exist_ok=True)
        database = (
            Path(os.environ.get("ORACLE_LAB_DB", str(home / "oracle.db")))
            .expanduser()
            .resolve(strict=False)
        )
        if any(_path_is_inside(database, root) for root in protected_roots):
            raise ServiceError(
                f"ORACLE_LAB_DB must be outside the current Git worktree: {database}"
            )
        database.parent.mkdir(parents=True, exist_ok=True)
        config_dir = Path(os.environ.get("ORACLE_LAB_CONFIG", "config"))
        from oracle_lab.agent_adapters import build_worker_router
        from oracle_lab.config import load_runtime_config
        from oracle_lab.docker_sbx_isolation import (
            build_coding_worker_isolation_broker,
        )

        runtime_config = load_runtime_config(config_dir)
        worker_workspace_root = home / "worker-workspaces"
        coding_worker_broker = build_coding_worker_isolation_broker(
            runtime_config.agents,
            state_root=home / "coding-isolation",
            workspace_root=worker_workspace_root,
        )
        host_worker_router = build_worker_router(
            runtime_config.agents,
            workspace_root=worker_workspace_root,
            coding_worker_broker=coding_worker_broker,
        )
        service = cls(
            EventStore(database),
            home=home,
            config_dir=config_dir,
            owns_store=True,
            host_worker_router=host_worker_router,
        )
        service._config = runtime_config
        return service

    def close(self) -> None:
        if self.owns_store:
            self.store.close()

    @property
    def runtime_config(self) -> Any:
        if self._config is None:
            from oracle_lab.config import load_runtime_config

            self._config = load_runtime_config(self.config_dir)
        return self._config

    def _read_control_state(self) -> dict[str, Any]:
        try:
            value = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        return value if isinstance(value, dict) else {}

    def _write_control_state(self) -> None:
        temporary = self._state_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self._state, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self._state_path)

    def _set_active(self, session_id: str, branch_id: str) -> None:
        self._state.update({"session_id": session_id, "branch_id": branch_id})
        self._write_control_state()

    def _active(self) -> tuple[str, str]:
        session_id = self._state.get("session_id")
        branch_id = self._state.get("branch_id")
        if isinstance(session_id, str) and isinstance(branch_id, str):
            return session_id, branch_id
        sessions = self.list_sessions()
        if len(sessions) == 1:
            session = sessions[0]
            self._set_active(str(session["id"]), str(session["current_branch_id"]))
            return str(session["id"]), str(session["current_branch_id"])
        raise ServiceError("no active session; run `oracle session new` or `session switch`")

    def _branch_service(self) -> Any:
        try:
            from oracle_lab.branching import BranchService
        except ImportError as error:  # pragma: no cover - integration guard
            raise ServiceError("branch service is unavailable") from error
        return BranchService(self.store)

    def new_session(
        self,
        title: str | None = None,
        *,
        model_profile_id: str | None = None,
    ) -> dict[str, Any]:
        if model_profile_id is None:
            model_profile_id = next(iter(self.runtime_config.models), None)
        session = self._branch_service().create_session(
            title=title,
            model_profile_id=model_profile_id,
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
        )
        value = _jsonable(session)
        branch_id = value.get("current_branch_id") or value.get("branch_id")
        if not isinstance(branch_id, str):
            raise ServiceError("new session has no current branch")
        self._set_active(str(value["id"]), branch_id)
        root_event_id = value.get("root_event_id")
        if not isinstance(root_event_id, str):
            raise ServiceError("new session has no root event")
        snapshot = self._configuration_snapshot()
        self._append(
            EventType.SESSION_CHECKPOINTED,
            {
                "operation": "configuration.snapshot",
                "configuration": snapshot,
                "sha256": sha256_json(snapshot),
            },
            actor=Actor(kind=ActorKind.SYSTEM, id="configuration-snapshot"),
            session_id=str(value["id"]),
            branch_id=branch_id,
            parent_event_id=root_event_id,
            causation_id=root_event_id,
        )
        return value

    def _configuration_snapshot(self) -> dict[str, Any]:
        """Return JSON-only experiment configuration without credential values."""
        snapshot = dataclasses.asdict(self.runtime_config)
        providers = snapshot.get("providers", {})
        if isinstance(providers, dict):
            for provider in providers.values():
                if not isinstance(provider, dict):
                    continue
                headers = provider.get("headers")
                if isinstance(headers, dict):
                    provider["headers"] = {
                        key: "[redacted]"
                        if key.casefold() in {"authorization", "cookie", "x-api-key"}
                        else value
                        for key, value in headers.items()
                    }
        return snapshot

    def list_sessions(self) -> list[dict[str, Any]]:
        try:
            sessions = self._branch_service().list_sessions()
            return [_jsonable(session) for session in sessions]
        except ServiceError:
            rows = self.store.connection.execute(
                "SELECT * FROM sessions ORDER BY created_at, id"
            ).fetchall()
            return [dict(row) for row in rows]

    def show_session(self, session_id: str) -> dict[str, Any]:
        session = self._branch_service().get_session(session_id)
        if session is None:
            raise ServiceError(f"session not found: {session_id}")
        return {
            "session": _jsonable(session),
            "events": [_jsonable(event) for event in self.store.list_events(session_id=session_id)],
        }

    def switch_session(self, session_id: str) -> dict[str, Any]:
        session = self._branch_service().get_session(session_id)
        if session is None:
            raise ServiceError(f"session not found: {session_id}")
        value = _jsonable(session)
        branch_id = value.get("current_branch_id")
        if not isinstance(branch_id, str):
            raise ServiceError(f"session has no current branch: {session_id}")
        self._set_active(session_id, branch_id)
        return value

    def import_session(
        self,
        source: str | Path,
        *,
        title: str | None = None,
        authorize_human_curation: bool = False,
        authorizer: Actor | None = None,
    ) -> dict[str, Any]:
        """Import a historical log or verified research-bundle directory."""
        source_path = Path(source)
        if source_path.is_dir():
            if title is not None:
                raise ServiceError("--title cannot rewrite a research-bundle session")
            return self.import_bundle(
                source_path,
                authorize_human_curation=authorize_human_curation,
                authorizer=authorizer,
            )
        from oracle_lab.historical_import import HistoricalSessionImporter

        with self.observability.operation(
            "session.import",
            fields={"source_name": Path(source).name},
        ):
            result = HistoricalSessionImporter(self.store).import_file(source, title=title)
        self._set_active(result.session_id, result.branch_id)
        audit = self.store.require(result.import_event_id)
        self.observability.log_event(audit, fields={"operation": "session.import"})
        return {
            **_jsonable(dataclasses.asdict(result)),
            "session": self.show_session(result.session_id)["session"],
            "import_event": audit.to_dict(),
        }

    def import_bundle(
        self,
        source: str | Path,
        *,
        authorize_human_curation: bool = False,
        authorizer: Actor | None = None,
    ) -> dict[str, Any]:
        """Reconstruct one portable bundle under an explicit human import gate."""
        from oracle_lab.bundle_import import ResearchBundleImporter

        if authorize_human_curation and (
            authorizer is None or authorizer.kind is not ActorKind.HUMAN
        ):
            raise ServiceError("curation import authorization requires an explicit human actor")
        with self.observability.operation(
            "session.import_bundle",
            fields={"source_name": Path(source).name},
        ):
            result = ResearchBundleImporter(self.store).import_directory(
                source,
                archive_root=self.archive_root / "raw",
                worker_archive_root=self.archive_root / "workers",
                validation_archive_root=self.archive_root / "validations",
                authorizer=authorizer,
                authorize_human_curation=authorize_human_curation,
            )
        self._set_active(result.session_id, result.branch_id)
        audit = self.store.require(result.audit_event_id)
        self.observability.log_event(audit, fields={"operation": "session.import_bundle"})
        return {
            **_jsonable(dataclasses.asdict(result)),
            "session": self.show_session(result.session_id)["session"],
            "import_event": audit.to_dict(),
        }

    def list_branches(self, session_id: str | None = None) -> list[dict[str, Any]]:
        if session_id is None:
            session_id, _ = self._active()
        return self._rows(
            "SELECT * FROM branches WHERE session_id = ? ORDER BY created_at, id",
            (session_id,),
        )

    def switch_branch(self, session_id: str, branch_id: str) -> dict[str, Any]:
        row = self.store.connection.execute(
            "SELECT * FROM branches WHERE id = ? AND session_id = ?",
            (branch_id, session_id),
        ).fetchone()
        if row is None:
            raise ServiceError(f"branch not found in session {session_id}: {branch_id}")
        self._set_active(session_id, branch_id)
        return dict(row)

    def checkpoint(self, note: str | None = None) -> dict[str, Any]:
        _, branch_id = self._active()
        event = self._branch_service().checkpoint(
            branch_id,
            title=note,
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
        )
        return event.to_dict()

    def fork(self, event_id: str, title: str | None = None) -> dict[str, Any]:
        branch = self._branch_service().fork(event_id, title=title)
        value = _jsonable(branch)
        source = self.store.require(event_id)
        branch_id = value.get("id") or value.get("branch_id")
        if not isinstance(branch_id, str) or source.session_id is None:
            raise ServiceError("fork did not return a branch identity")
        self._set_active(source.session_id, branch_id)
        return value

    def approve_branch(self, proposal_event_id: str) -> dict[str, Any]:
        proposal = self.store.require(proposal_event_id)
        if proposal.type is not EventType.ANALYSIS_BRANCH_PROPOSED:
            raise ServiceError(f"event is not a branch proposal: {proposal_event_id}")
        decision = next(
            (
                item
                for item in self._dispatcher().evaluate(proposal)
                if item.rule_id == "branch-proposal-creation"
            ),
            None,
        )
        if decision is None:
            raise ServiceError("branch proposal has no configured dispatch rule")
        from oracle_lab.dispatcher import DecisionStatus

        if decision.status is not DecisionStatus.PENDING_APPROVAL:
            raise ServiceError("branch proposal does not require human approval")
        target_id = proposal.payload.get("fork_event_id")
        approval = self._append(
            EventType.HUMAN_REQUEST_FORK,
            {
                "event_id": target_id,
                "proposal_event_id": proposal.id,
                "target_event_id": target_id,
            },
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=proposal.session_id,
            branch_id=proposal.branch_id,
            parent_event_id=proposal.id,
            causation_id=proposal.id,
            correlation_id=proposal.correlation_id,
        )
        approved = self._dispatcher().approve(
            decision,
            approver_event_id=approval.id,
            source_event=proposal,
        )
        jobs = [
            _jsonable(job)
            for job in self._job_queue().list_jobs(kind="branch.create")
            if job.source_event_id == proposal.id
        ]
        return {
            "approval_event": approval.to_dict(),
            "decision": _jsonable(approved),
            "jobs": jobs,
        }

    def archive_session(self, session_id: str | None = None) -> dict[str, Any]:
        active_session, active_branch = self._active()
        target_session = session_id or active_session
        archived_session = self._branch_service().archive_session(
            target_session,
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
        )
        if target_session == active_session:
            self._state.pop("session_id", None)
            self._state.pop("branch_id", None)
            self._write_control_state()
        return {
            "session_id": target_session,
            "branch_id": active_branch,
            "session": _jsonable(archived_session),
        }

    def _last_event(self, session_id: str, branch_id: str) -> Event | None:
        events = self.store.list_events(
            session_id=session_id,
            branch_id=branch_id,
            ascending=False,
        )
        return next(
            (event for event in events if not event.type.value.startswith("job.")),
            None,
        )

    def _append(
        self,
        event_type: EventType | str,
        payload: Mapping[str, Any],
        *,
        actor: Actor,
        session_id: str | None = None,
        branch_id: str | None = None,
        parent_event_id: str | None = None,
        causation_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Event:
        if session_id is None or branch_id is None:
            active_session, active_branch = self._active()
            session_id = session_id or active_session
            branch_id = branch_id or active_branch
        if parent_event_id is None:
            parent = self._last_event(session_id, branch_id)
            parent_event_id = parent.id if parent is not None else None
        event = Event.new(
            event_type,
            actor=actor,
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent_event_id,
            causation_id=causation_id,
            correlation_id=correlation_id or new_id("cor"),
            payload=payload,
        )
        with self.observability.span("service.event.append", event=event):
            stored = self.store.append(event)
        self.observability.log_event(
            stored,
            fields={"operation": "service.event.append"},
        )
        return stored

    def _automation_state(self, source: Event) -> tuple[int, int]:
        """Return the auditable loop depth and remaining event budget."""

        raw_depth = source.payload.get("automation_depth", 0)
        raw_budget = source.payload.get(
            "automation_budget_remaining",
            self.runtime_config.policies.max_auto_budget,
        )
        depth = raw_depth if isinstance(raw_depth, int) and not isinstance(raw_depth, bool) else 0
        budget = (
            raw_budget
            if isinstance(raw_budget, int) and not isinstance(raw_budget, bool)
            else self.runtime_config.policies.max_auto_budget
        )
        return max(0, depth), max(0, budget)

    def _automation_payload(
        self,
        source: Event,
        *,
        consume: int = 0,
        depth_increment: int = 0,
        loop_signature: str | None = None,
    ) -> dict[str, Any]:
        depth, budget = self._automation_state(source)
        payload: dict[str, Any] = {
            "automation_depth": depth + depth_increment,
            "automation_budget_remaining": max(0, budget - consume),
            "automation_loop_detector": "sha256-equivalent-event-v1",
        }
        if loop_signature is not None:
            payload["loop_signature"] = loop_signature
        elif isinstance(source.payload.get("loop_signature"), str):
            payload["loop_signature"] = source.payload["loop_signature"]
        return payload

    def _stop_automation(
        self,
        source: Event,
        reason: str,
        *,
        detail: Mapping[str, Any] | None = None,
    ) -> Event:
        """Append one idempotent, queryable terminal event for an auto-chain."""

        prior = [
            event
            for event in self.store.list_events(
                event_type=EventType.SYSTEM_AUTOMATION_STOPPED,
                correlation_id=source.correlation_id,
            )
            if event.payload.get("reason") == reason
            and event.payload.get("source_event_id") == source.id
        ]
        if prior:
            return prior[0]
        return self._append(
            EventType.SYSTEM_AUTOMATION_STOPPED,
            {
                "reason": reason,
                "source_event_id": source.id,
                "source_event_ids": [source.id],
                **self._automation_payload(source),
                **dict(detail or {}),
            },
            actor=Actor(kind=ActorKind.SYSTEM, id="automation-boundary"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
        )

    def _active_pause(self, session_id: str, branch_id: str) -> Event | None:
        visible = self._branch_service().visible_events(branch_id)
        controls = [
            event
            for event in visible
            if event.session_id == session_id
            and event.type in {EventType.HUMAN_PAUSE, EventType.HUMAN_RESUME}
        ]
        if not controls or controls[-1].type is EventType.HUMAN_RESUME:
            return None
        return controls[-1]

    def _paused_job_branches(self) -> dict[tuple[str, str], Event]:
        """Return every queued branch currently stopped by a visible Human pause."""

        rows = self.store.connection.execute(
            """
            SELECT DISTINCT session_id, branch_id FROM jobs
            WHERE status IN ('pending', 'leased')
              AND session_id IS NOT NULL
              AND branch_id IS NOT NULL
            """
        ).fetchall()
        paused: dict[tuple[str, str], Event] = {}
        for row in rows:
            identity = (str(row["session_id"]), str(row["branch_id"]))
            pause = self._active_pause(*identity)
            if pause is not None:
                paused[identity] = pause
        return paused

    def pause(self, note: str | None = None) -> dict[str, Any]:
        session_id, branch_id = self._active()
        event = self._append(
            EventType.HUMAN_PAUSE,
            {"scope": "branch", "note": note},
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=session_id,
            branch_id=branch_id,
        )
        return event.to_dict()

    def resume(self, note: str | None = None) -> dict[str, Any]:
        session_id, branch_id = self._active()
        event = self._append(
            EventType.HUMAN_RESUME,
            {"scope": "branch", "note": note},
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=session_id,
            branch_id=branch_id,
        )
        return event.to_dict()

    def _session_profile(self, session_id: str) -> str:
        row = self.store.connection.execute(
            "SELECT model_profile_id FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        profile = row[0] if row is not None else None
        if isinstance(profile, str) and profile:
            return profile
        try:
            return next(iter(self.runtime_config.models))
        except StopIteration as error:
            raise ServiceError("no model profiles configured") from error

    def _system_prompt_source_event_id(
        self,
        *,
        branch_id: str,
        system_prompt: str,
    ) -> str | None:
        if not system_prompt:
            return None
        candidates = [
            event
            for event in self._branch_service().visible_events(branch_id)
            if event.type is EventType.SESSION_CHECKPOINTED
            and event.payload.get("operation") == "configuration.snapshot"
        ]
        if not candidates:
            raise ServiceError("non-empty system prompt has no configuration snapshot source")
        return candidates[-1].id

    def _enqueue_request(self, request_event: Event) -> Any:
        self._enforce_cost_safeguards(request_event)
        profile_id = str(request_event.payload.get("model_profile_id"))
        profile = self.runtime_config.model(profile_id)
        self._enforce_provider_rate_limit(request_event, profile.provider)
        return self._job_queue().enqueue(
            "oracle.generate",
            {"request_event_id": request_event.id, "model_profile_id": profile_id},
            source_event_id=request_event.id,
            idempotency_key=f"oracle.generate:{request_event.id}",
            provider_id=profile.provider,
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            serialize_branch=request_event.payload.get("sample_group_id") is None,
        )

    def _job_queue(self) -> Any:
        from oracle_lab.jobs import JobQueue

        limits = {
            provider_id: provider.max_concurrency
            for provider_id, provider in self.runtime_config.providers.items()
        }
        return JobQueue(self.store, provider_limits=limits)

    def _worker_profile(self, task_kind: str) -> tuple[str, Any, Any]:
        if self.host_worker_router is None:
            raise ServiceError("coding workers are disabled in config/agents.toml")
        routed_task_type, worker = self.host_worker_router.route(task_kind)
        profile = getattr(worker, "profile", None)
        if profile is None:
            raise ServiceError(f"{task_kind} is not routed to a configured coding worker")
        return routed_task_type, worker, profile

    def _worker_routing_snapshot(self, *, profile: Any) -> dict[str, Any]:
        if self.host_worker_router is None:
            raise ServiceError("host worker router is not configured")
        selected_adapter = str(profile.adapter)
        if selected_adapter == "direct":
            return {
                "schema_version": 1,
                "route_class": "direct_host",
                "preferred_adapter": "direct",
                "selected_adapter": "direct",
                "selected_profile_id": str(profile.id),
                "fallback_occurred": False,
                "fallback_adapter": None,
            }
        preferred_adapter = self.host_worker_router.prefer_coding_agent
        return {
            "schema_version": 1,
            "route_class": "coding_worker",
            "preferred_adapter": preferred_adapter,
            "selected_adapter": selected_adapter,
            "selected_profile_id": str(profile.id),
            "fallback_occurred": selected_adapter != preferred_adapter,
            "fallback_adapter": self.host_worker_router.fallback_coding_agent,
        }

    @staticmethod
    def _validation_sandbox_snapshot(config: Any) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "backend": config.backend,
            "image_requested": config.image,
            "network": config.network,
            "read_only_root": config.read_only_root,
            "timeout_ms": config.timeout_ms,
            "memory_mb": config.memory_mb,
            "cpus": config.cpus,
            "pids_limit": config.pids_limit,
            "max_output_bytes": config.max_output_bytes,
        }

    @staticmethod
    def _validation_sandbox_from_snapshot(value: Any) -> Any:
        from oracle_lab.config import SandboxConfig

        expected_keys = {
            "schema_version",
            "backend",
            "image_requested",
            "network",
            "read_only_root",
            "timeout_ms",
            "memory_mb",
            "cpus",
            "pids_limit",
            "max_output_bytes",
        }
        if not isinstance(value, Mapping) or set(value) != expected_keys:
            raise ServiceError("validation sandbox snapshot is incomplete")
        if value.get("schema_version") != 1:
            raise ServiceError("validation sandbox snapshot version is unsupported")
        try:
            return SandboxConfig(
                backend=value["backend"],
                image=value["image_requested"],
                network=value["network"],
                read_only_root=value["read_only_root"],
                timeout_ms=value["timeout_ms"],
                memory_mb=value["memory_mb"],
                cpus=value["cpus"],
                pids_limit=value["pids_limit"],
                max_output_bytes=value["max_output_bytes"],
            )
        except (TypeError, ValueError) as error:
            raise ServiceError("validation sandbox snapshot is invalid") from error

    def _assert_frozen_worker_execution(
        self,
        *,
        job: Any,
        task_event: Event,
        routed_task_type: str,
        worker: Any,
    ) -> None:
        """Reject restart/config drift before the coding-agent process can start."""

        profile = getattr(worker, "profile", None)
        if profile is None or not callable(getattr(profile, "redacted_snapshot", None)):
            raise ServiceError("coding worker lacks a redacted execution profile")
        current_profile = profile.redacted_snapshot()
        current_routing = self._worker_routing_snapshot(profile=profile)
        task_payload = thaw_json(task_event.payload)
        job_payload = thaw_json(job.payload)
        expected_scalars = {
            "task_event_id": task_event.id,
            "worker_profile_id": profile.id,
            "routed_task_type": routed_task_type,
        }
        if any(job_payload.get(key) != value for key, value in expected_scalars.items()):
            raise ServiceError("worker job routing identity differs from the current router")
        if (
            task_payload.get("worker_profile_id") != profile.id
            or task_payload.get("worker_adapter") != getattr(worker, "name", None)
            or task_payload.get("routed_task_type") != routed_task_type
            or task_payload.get("worker_execution_profile") != current_profile
            or job_payload.get("worker_execution_profile") != current_profile
            or task_payload.get("worker_routing") != current_routing
            or job_payload.get("worker_routing") != current_routing
        ):
            raise ServiceError("frozen worker execution profile or routing has drifted")
        task_job_fields = (
            "source_event_id",
            "goal",
            "repository_path",
            "base_commit",
            "validation_commands",
            "validation_sandbox",
        )
        for key in task_job_fields:
            task_value = task_payload.get(key)
            job_value = job_payload.get(key)
            if key == "validation_commands":
                task_value = tuple(task_value or ())
                job_value = tuple(job_value or ())
            if task_value != job_value:
                raise ServiceError(f"worker job differs from its frozen task field: {key}")
        if (
            task_payload.get("job_id") != job.id
            or task_payload.get("task_kind") != job.kind
            or job.source_event_id != task_event.id
        ):
            raise ServiceError("worker job/task durable identity is inconsistent")

    def _assert_frozen_direct_host_execution(
        self,
        *,
        job: Any,
        source: Event,
        task_event: Event,
        routed_task_type: str,
        worker: Any,
    ) -> None:
        """Reject Direct Host config drift before any HTTP request is sent."""

        profile = getattr(worker, "profile", None)
        if profile is None or not callable(getattr(profile, "redacted_snapshot", None)):
            raise ServiceError("configured Direct Host lacks an execution profile")
        task_payload = thaw_json(task_event.payload)
        source_ids = {
            value
            for value in (
                job.payload.get("analysis_source_event_id"),
                job.payload.get("source_event_id"),
                job.source_event_id,
            )
            if isinstance(value, str)
        }
        if source.id not in source_ids:
            raise ServiceError("Direct Host job source identity has drifted")
        expected = {
            "job_id": job.id,
            "task_kind": job.kind,
            "routed_task_type": routed_task_type,
            "source_event_id": source.id,
            "worker_profile_id": profile.id,
            "worker_adapter": "direct",
            "worker_execution_profile": profile.redacted_snapshot(),
            "worker_routing": self._worker_routing_snapshot(profile=profile),
        }
        if any(task_payload.get(key) != value for key, value in expected.items()):
            raise ServiceError("frozen Direct Host task profile, routing, or source has drifted")

    @staticmethod
    def _protected_worker_roots(repository: Path) -> tuple[Path, ...]:
        repository_root = repository.expanduser().resolve(strict=False)
        roots = [repository_root]
        current_root = _git_worktree_root(Path.cwd())
        if current_root is not None and current_root not in roots:
            roots.append(current_root)
        return tuple(roots)

    @staticmethod
    def _require_outside_worktrees(
        label: str,
        path: str | Path,
        protected_roots: Sequence[Path],
    ) -> Path:
        resolved = Path(path).expanduser().resolve(strict=False)
        conflict = next(
            (root for root in protected_roots if _path_is_inside(resolved, root)),
            None,
        )
        if conflict is not None:
            raise ServiceError(
                f"{label} must be outside the target and current Git worktrees: "
                f"{resolved} is inside {conflict}"
            )
        return resolved

    def _database_paths(self) -> tuple[Path, ...]:
        paths: list[Path] = []
        for row in self.store.connection.execute("PRAGMA database_list"):
            raw_path = row[2]
            if isinstance(raw_path, str) and raw_path:
                paths.append(Path(raw_path).expanduser().resolve(strict=False))
        return tuple(paths)

    def _isolated_staging_root(
        self,
        repository: Path,
        *,
        protected_roots: Sequence[Path] | None = None,
    ) -> Path:
        protected = tuple(protected_roots or self._protected_worker_roots(repository))
        configured = self.staging_root.expanduser().resolve(strict=False)
        if not any(_path_is_inside(configured, root) for root in protected):
            return configured
        if self._staging_root_explicit:
            self._require_outside_worktrees(
                "configured staging root",
                configured,
                protected,
            )
        fallback = _external_data_home(protected_roots=protected) / "staging"
        return self._require_outside_worktrees("derived staging root", fallback, protected)

    def _assert_control_storage_isolated(self, repository: Path) -> tuple[Path, ...]:
        """Reject Host control-plane storage inside protected worktrees."""

        protected = self._protected_worker_roots(repository)
        for label, path in (
            ("Oracle Lab home", self.home),
            ("worker archive root", self.archive_root),
            ("control-state path", self._state_path),
        ):
            self._require_outside_worktrees(label, path, protected)
        for database_path in self._database_paths():
            self._require_outside_worktrees(
                "Oracle Lab database",
                database_path,
                protected,
            )
        self._isolated_staging_root(repository, protected_roots=protected)
        return protected

    def _assert_worker_storage_isolated(self, repository: Path, *, worker: Any) -> None:
        """Fail before launch if worker state can mutate a user worktree."""

        protected = self._assert_control_storage_isolated(repository)
        workspace_root = getattr(worker, "repository_workspace_root", None)
        if workspace_root is None:
            raise ServiceError("repository-edit worker requires a dedicated workspace root")
        self._require_outside_worktrees(
            "worker repository workspace root",
            workspace_root,
            protected,
        )

    def _worker_automation_fields(
        self,
        source: Event,
        *,
        signature_seed: Mapping[str, Any],
    ) -> dict[str, Any]:
        policies = self.runtime_config.policies
        prior_depth = source.payload.get("automation_depth", 0)
        prior_budget = source.payload.get("automation_budget_remaining", policies.max_auto_budget)
        if (
            isinstance(prior_depth, bool)
            or not isinstance(prior_depth, int)
            or isinstance(prior_budget, bool)
            or not isinstance(prior_budget, int)
        ):
            raise ServiceError("worker automation boundary metadata is invalid")
        depth = prior_depth + 1
        budget = prior_budget - 1
        if depth > policies.max_auto_depth or budget < 0:
            raise ServiceError("worker task exceeds automation depth or budget")
        signature = sha256_json(signature_seed)
        detector = source.payload.get("automation_loop_detector", "sha256-equivalent-event-v1")
        if detector != "sha256-equivalent-event-v1":
            raise ServiceError("worker automation loop detector is invalid")
        pending = [source]
        seen: set[str] = set()
        while pending:
            current = pending.pop()
            if current.id in seen:
                continue
            seen.add(current.id)
            if current.payload.get("loop_signature") == signature:
                raise ServiceError("worker task repeats an equivalent automation event")
            cited = current.payload.get("source_event_ids", ())
            if isinstance(cited, Sequence) and not isinstance(cited, (str, bytes, bytearray)):
                ancestor_ids = [value for value in cited if isinstance(value, str)]
            else:
                ancestor_ids = []
            ancestor_ids.extend(
                value
                for value in (current.parent_event_id, current.causation_id)
                if isinstance(value, str)
            )
            for ancestor_id in ancestor_ids:
                ancestor = self.store.get(ancestor_id)
                if ancestor is not None and ancestor.session_id == source.session_id:
                    pending.append(ancestor)
        return {
            "automation_depth": depth,
            "automation_budget_remaining": budget,
            "automation_loop_detector": "sha256-equivalent-event-v1",
            "loop_signature": signature,
        }

    def enqueue_repository_edit(
        self,
        source_event_id: str,
        goal: str,
        *,
        repository: str | Path = ".",
    ) -> dict[str, Any]:
        """Create a durable, human-authored repository-edit worker task."""

        if not goal.strip():
            raise ServiceError("repository-edit goal must preserve non-blank exact text")
        source = self.store.require(source_event_id)
        active_session, active_branch = self._active()
        if source.session_id != active_session or source.branch_id != active_branch:
            raise ServiceError("worker source must be on the active session branch")
        visible_ids = {
            event.id
            for event in self._branch_service().visible_events(
                active_branch, until_event_id=source.id
            )
        }
        if source.id not in visible_ids:
            raise ServiceError("worker source is not visible at the requested fork point")
        repository_path = Path(repository).expanduser().resolve()

        def git(*arguments: str) -> str:
            try:
                result = run_git(repository_path, *arguments, timeout=30)
            except GitControlError as error:
                raise ServiceError(str(error)) from error
            if result.returncode != 0:
                detail = result.stderr.decode("utf-8", "replace").strip()
                raise ServiceError(f"git {' '.join(arguments)} failed: {detail}")
            return result.stdout.decode("utf-8", "strict").strip()

        top_level = Path(git("rev-parse", "--show-toplevel")).resolve()
        if top_level != repository_path:
            raise ServiceError("repository must be its Git top-level directory")
        base_commit = git("rev-parse", "--verify", "HEAD^{commit}")
        routed_kind, worker, profile = self._worker_profile("repository_edit")
        self._assert_worker_storage_isolated(repository_path, worker=worker)
        profile_snapshot = profile.redacted_snapshot()
        routing_snapshot = self._worker_routing_snapshot(profile=profile)
        validation_sandbox = self._validation_sandbox_snapshot(self.runtime_config.tools.sandbox)
        automation = self._worker_automation_fields(
            source,
            signature_seed={
                "task_kind": "repository_edit",
                "source_event_id": source.id,
                "goal": goal,
                "repository_path": str(repository_path),
                "base_commit": base_commit,
                "worker_profile_id": profile.id,
            },
        )
        idempotency_key = "worker.repository_edit:" + sha256_json(
            {
                "source_event_id": source.id,
                "goal": goal,
                "repository_path": str(repository_path),
                "base_commit": base_commit,
                "worker_profile_id": profile.id,
                "worker_execution_profile": profile_snapshot,
                "worker_routing": routing_snapshot,
                "validation_sandbox": validation_sandbox,
            }
        )
        queue = self._job_queue()
        with self.store.transaction():
            existing = next(
                (
                    event
                    for event in self.store.list_events(event_type=EventType.WORKER_TASK_REQUESTED)
                    if event.payload.get("idempotency_key") == idempotency_key
                ),
                None,
            )
            job_id = str(existing.payload["job_id"]) if existing is not None else new_id("job")
            if existing is None:
                task_event = self.store.append(
                    Event.new(
                        EventType.WORKER_TASK_REQUESTED,
                        actor=Actor(kind=ActorKind.HUMAN, id="cli"),
                        session_id=source.session_id,
                        branch_id=source.branch_id,
                        parent_event_id=source.id,
                        causation_id=source.id,
                        correlation_id=source.correlation_id or new_id("corr"),
                        payload={
                            "job_id": job_id,
                            "task_kind": "repository_edit",
                            "routed_task_type": routed_kind,
                            "source_event_id": source.id,
                            "source_event_ids": [source.id],
                            "goal": goal,
                            "repository_path": str(repository_path),
                            "base_commit": base_commit,
                            "worker_profile_id": profile.id,
                            "worker_adapter": getattr(worker, "name", type(worker).__name__),
                            "worker_execution_profile": profile_snapshot,
                            "worker_routing": routing_snapshot,
                            "validation_commands": list(profile.validation_commands),
                            "validation_sandbox": validation_sandbox,
                            "idempotency_key": idempotency_key,
                            "artifact_origin": "human_authored_task",
                            **automation,
                        },
                    )
                )
            else:
                task_event = existing
            job = queue.get(job_id)
            if job is None:
                job = queue.enqueue(
                    "repository_edit",
                    {
                        "task_event_id": task_event.id,
                        "source_event_id": source.id,
                        "goal": goal,
                        "repository_path": str(repository_path),
                        "base_commit": base_commit,
                        "worker_profile_id": profile.id,
                        "routed_task_type": routed_kind,
                        "worker_execution_profile": profile_snapshot,
                        "worker_routing": routing_snapshot,
                        "validation_commands": list(profile.validation_commands),
                        "validation_sandbox": validation_sandbox,
                    },
                    source_event_id=task_event.id,
                    idempotency_key=idempotency_key,
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    serialize_branch=True,
                    max_attempts=profile.max_retries + 1,
                    job_id=job_id,
                )
            if (
                job.id != job_id
                or job.source_event_id != task_event.id
                or job.payload.get("task_event_id") != task_event.id
            ):
                raise ServiceError("repository-edit task/job idempotency identity is inconsistent")
        return {"task_event": task_event.to_dict(), "job": _jsonable(job)}

    def worker_task_status(self, task_event_id: str) -> dict[str, Any]:
        task = self.store.require(task_event_id)
        if task.type is not EventType.WORKER_TASK_REQUESTED:
            raise ServiceError("event is not a worker task")
        runs = [
            dict(row)
            for row in self.store.connection.execute(
                "SELECT * FROM worker_runs WHERE task_event_id = ? ORDER BY created_at",
                (task.id,),
            )
        ]
        patches = [
            dict(row)
            for row in self.store.connection.execute(
                """
                SELECT p.* FROM candidate_patches p
                JOIN worker_runs r ON r.run_id = p.worker_run_id
                WHERE r.task_event_id = ? ORDER BY p.created_at
                """,
                (task.id,),
            )
        ]
        return {"task": task.to_dict(), "runs": runs, "patches": patches}

    def patch_show(self, patch_event_id: str) -> dict[str, Any]:
        patch = self.store.require(patch_event_id)
        if patch.type is not EventType.WORKER_PATCH_PROPOSED:
            raise ServiceError("event is not a candidate patch")
        row = self.store.connection.execute(
            "SELECT * FROM candidate_patches WHERE patch_event_id = ?",
            (patch.id,),
        ).fetchone()
        if row is None:
            raise ServiceError("candidate patch projection is missing")
        state = dict(row)
        for field_name in ("changed_paths_json", "validation_event_ids_json"):
            raw_value = state.get(field_name)
            if isinstance(raw_value, str):
                state[field_name.removesuffix("_json")] = json.loads(raw_value)
        run = self.store.connection.execute(
            "SELECT * FROM worker_runs WHERE run_id = ?",
            (patch.payload["worker_run_id"],),
        ).fetchone()
        return {
            "patch": patch.to_dict(),
            "state": state,
            "worker_run": None if run is None else dict(run),
        }

    def patch_status(self, patch_event_id: str) -> dict[str, Any]:
        return self.patch_show(patch_event_id)

    def approve_patch(self, patch_event_id: str) -> dict[str, Any]:
        patch = self.store.require(patch_event_id)
        active_session, active_branch = self._active()
        if patch.session_id != active_session or patch.branch_id != active_branch:
            raise ServiceError("candidate patch is not on the active session branch")
        with self.store.transaction():
            row = self.store.connection.execute(
                "SELECT status FROM candidate_patches WHERE patch_event_id = ?",
                (patch.id,),
            ).fetchone()
            if patch.type is not EventType.WORKER_PATCH_PROPOSED or row is None:
                raise ServiceError("candidate patch does not exist")
            if patch.metadata.get("bundle_import_authority") == "historical_only":
                raise ServiceError(
                    "imported candidate patch is historical evidence and requires local rebind"
                )
            if row["status"] != "pending_human":
                raise ServiceError("candidate patch is no longer awaiting human approval")
            approval = self.store.append(
                Event.new(
                    EventType.HUMAN_PATCH_APPROVED,
                    actor=Actor(kind=ActorKind.HUMAN, id="cli"),
                    session_id=patch.session_id,
                    branch_id=patch.branch_id,
                    parent_event_id=patch.id,
                    causation_id=patch.id,
                    correlation_id=patch.correlation_id,
                    payload={
                        "patch_event_id": patch.id,
                        "patch_sha256": patch.payload["patch_sha256"],
                        "base_commit": patch.payload["base_commit"],
                    },
                )
            )
            job = self._job_queue().enqueue(
                "worker.patch.apply",
                {"patch_event_id": patch.id, "approval_event_id": approval.id},
                source_event_id=approval.id,
                idempotency_key=f"worker.patch.apply:{patch.id}:{approval.id}",
                session_id=patch.session_id,
                branch_id=patch.branch_id,
                serialize_branch=True,
                max_attempts=1,
            )
        return {"approval_event": approval.to_dict(), "job": _jsonable(job)}

    def reject_patch(self, patch_event_id: str, *, reason: str | None = None) -> dict[str, Any]:
        patch = self.store.require(patch_event_id)
        active_session, active_branch = self._active()
        if patch.session_id != active_session or patch.branch_id != active_branch:
            raise ServiceError("candidate patch is not on the active session branch")
        row = self.store.connection.execute(
            "SELECT status FROM candidate_patches WHERE patch_event_id = ?",
            (patch.id,),
        ).fetchone()
        if patch.type is not EventType.WORKER_PATCH_PROPOSED or row is None:
            raise ServiceError("candidate patch does not exist")
        if patch.metadata.get("bundle_import_authority") == "historical_only":
            raise ServiceError("imported candidate patch is immutable historical evidence")
        if row["status"] != "pending_human":
            raise ServiceError("candidate patch is no longer awaiting human judgment")
        rejection = self.store.append(
            Event.new(
                EventType.HUMAN_PATCH_REJECTED,
                actor=Actor(kind=ActorKind.HUMAN, id="cli"),
                session_id=patch.session_id,
                branch_id=patch.branch_id,
                parent_event_id=patch.id,
                causation_id=patch.id,
                correlation_id=patch.correlation_id,
                payload={
                    "patch_event_id": patch.id,
                    "patch_sha256": patch.payload["patch_sha256"],
                    "base_commit": patch.payload["base_commit"],
                    "reason": reason,
                },
            )
        )
        return rejection.to_dict()

    def _dispatcher(self) -> Any:
        """Build the dispatcher from the currently loaded runtime policies."""

        from oracle_lab.dispatcher import EventDispatcher, default_rules

        policies = self.runtime_config.policies
        return EventDispatcher(
            default_rules(
                analysis=policies.analysis,
                human_gate=policies.human_gate,
            ),
            queue=self._job_queue(),
            event_sink=self.store,
            max_auto_depth=policies.max_auto_depth,
            max_auto_budget=policies.max_auto_budget,
        )

    def _enforce_provider_rate_limit(
        self,
        request_event: Event,
        provider_id: str,
    ) -> None:
        provider = self.runtime_config.providers[provider_id]
        limit = provider.requests_per_minute
        if limit is None:
            return
        cutoff = request_event.created_at - dt.timedelta(minutes=1)
        recent = 0
        for event in self.store.list_events(event_type=EventType.ORACLE_REQUEST):
            if event.created_at < cutoff:
                continue
            profile_id = event.payload.get("model_profile_id")
            if not isinstance(profile_id, str):
                continue
            with contextlib.suppress(Exception):
                if self.runtime_config.model(profile_id).provider == provider_id:
                    recent += 1
        if recent <= limit:
            return
        self.store.append(
            Event.new(
                EventType.ORACLE_ERROR,
                actor=Actor(kind=ActorKind.SYSTEM, id="provider-rate-limit"),
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                parent_event_id=request_event.id,
                causation_id=request_event.id,
                correlation_id=request_event.correlation_id,
                payload={
                    "error_type": "ProviderRateLimit",
                    "provider_id": provider_id,
                    "requests_in_window": recent,
                    "requests_per_minute": limit,
                },
            )
        )
        raise ServiceError(
            f"provider rate limit reached for {provider_id}: {recent} > {limit}/minute"
        )

    def _enforce_cost_safeguards(self, request_event: Event) -> None:
        policies = self.runtime_config.policies
        rows = self.store.connection.execute(
            """
            SELECT provider_cost, created_at, session_id
            FROM usage_records
            WHERE kind = 'oracle' AND provider_cost IS NOT NULL
            """
        ).fetchall()
        today = dt.datetime.now(dt.UTC).date()
        daily_cost = sum(
            (
                Decimal(str(row["provider_cost"]))
                for row in rows
                if dt.datetime.fromisoformat(str(row["created_at"]).replace("Z", "+00:00")).date()
                == today
            ),
            Decimal("0"),
        )
        hard_limit = policies.hard_limit_usd_per_day
        if hard_limit is not None and daily_cost >= Decimal(str(hard_limit)):
            failure = Event.new(
                EventType.ORACLE_ERROR,
                actor=Actor(kind=ActorKind.SYSTEM, id="cost-safeguard"),
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                parent_event_id=request_event.id,
                causation_id=request_event.id,
                correlation_id=request_event.correlation_id,
                payload={
                    "error_type": "DailyCostHardLimit",
                    "daily_cost_usd": str(daily_cost),
                    "hard_limit_usd": str(hard_limit),
                },
            )
            self.store.append(failure)
            raise ServiceError(
                f"daily provider cost hard limit reached: {daily_cost} >= {hard_limit}"
            )

        warn_limit = policies.warn_limit_usd_per_session
        if warn_limit is None or request_event.session_id is None:
            return
        session_cost = sum(
            (
                Decimal(str(row["provider_cost"]))
                for row in rows
                if row["session_id"] == request_event.session_id
            ),
            Decimal("0"),
        )
        if session_cost < Decimal(str(warn_limit)):
            return
        prior_warnings = [
            event
            for event in self.store.list_events(
                session_id=request_event.session_id,
                event_type=EventType.ANALYSIS_SESSION_SUMMARY_UPDATED,
            )
            if event.payload.get("operation") == "cost.warning"
        ]
        if not prior_warnings:
            self.store.append(
                Event.new(
                    EventType.ANALYSIS_SESSION_SUMMARY_UPDATED,
                    actor=Actor(kind=ActorKind.SYSTEM, id="cost-safeguard"),
                    session_id=request_event.session_id,
                    branch_id=request_event.branch_id,
                    parent_event_id=request_event.id,
                    causation_id=request_event.id,
                    correlation_id=request_event.correlation_id,
                    payload={
                        "operation": "cost.warning",
                        "session_cost_usd": str(session_cost),
                        "warn_limit_usd": str(warn_limit),
                        "source_event_ids": [request_event.id],
                    },
                )
            )

    def _request(
        self,
        *,
        operation: str,
        parent_event_id: str,
        model_profile_id: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        source = self.store.require(parent_event_id)
        if source.session_id is None or source.branch_id is None:
            raise ServiceError("oracle requests require session and branch identities")
        profile_id = model_profile_id or self._session_profile(source.session_id)
        profile = self.runtime_config.model(profile_id)
        supplied = dict(extra or {})
        payload = {
            "operation": operation,
            "model_profile_id": profile_id,
            "context_policy": {
                "include_reasoning_in_next_turn": profile.include_reasoning_in_next_turn,
                "max_context_messages": profile.max_context_messages,
                "system_prompt_sha256": sha256_text(profile.system_prompt),
            },
            **self._automation_payload(source),
            **supplied,
        }
        request = self._append(
            EventType.ORACLE_REQUEST,
            payload,
            actor=Actor(kind=ActorKind.HOST, id="control-plane"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
        )
        job = self._enqueue_request(request)
        return {"request": request.to_dict(), "job": _jsonable(job)}

    def ask(self, text: str, *, model_profile_id: str | None = None) -> dict[str, Any]:
        if not text:
            raise ServiceError("input text must not be empty")
        human = self._append(
            EventType.HUMAN_INPUT,
            {"text": text, "content": text, "role": "user"},
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
        )
        request = self._request(
            operation="ask",
            parent_event_id=human.id,
            model_profile_id=model_profile_id,
        )
        return {"input": human.to_dict(), **request}

    def continue_session(self, *, model_profile_id: str | None = None) -> dict[str, Any]:
        session_id, branch_id = self._active()
        source = self._last_event(session_id, branch_id)
        if source is None:
            raise ServiceError("cannot continue an empty session")
        return self._request(
            operation="continue",
            parent_event_id=source.id,
            model_profile_id=model_profile_id,
        )

    def sample(
        self,
        count: int,
        *,
        temperature: float | None = None,
        top_p: float | None = None,
        model_profile_id: str | None = None,
        session_id: str | None = None,
        from_event_id: str | None = None,
    ) -> dict[str, Any]:
        if count < 1:
            raise ServiceError("sample count must be positive")
        if temperature is not None and not 0 <= temperature <= 2:
            raise ServiceError("temperature must be between 0 and 2")
        if top_p is not None and not 0 < top_p <= 1:
            raise ServiceError("top-p must be in (0, 1]")
        if from_event_id is not None:
            source = self.store.require(from_event_id)
            if source.session_id is None or source.branch_id is None:
                raise ServiceError("sample source must belong to a session and branch")
            if session_id is not None and session_id != source.session_id:
                raise ServiceError("sample source belongs to a different session")
            session_id = source.session_id
            branch_id = source.branch_id
            # Reject sibling/non-visible event IDs before constructing a group.
            self._branch_service().visible_events(branch_id, until_event_id=source.id)
        else:
            if session_id is None:
                session_id, branch_id = self._active()
            else:
                session = self._branch_service().get_session(session_id)
                if session is None or session.current_branch_id is None:
                    raise ServiceError(f"session has no current branch: {session_id}")
                branch_id = session.current_branch_id
            source = self._last_event(session_id, branch_id)
        if source is None:
            raise ServiceError("cannot sample an empty session")
        from oracle_lab.sampling import SamplingService
        from oracle_lab.session import SessionContextBuilder

        profile_id = model_profile_id or self._session_profile(session_id)
        profile = self.runtime_config.model(profile_id)
        visible = self._branch_service().visible_events(branch_id, until_event_id=source.id)
        context = SessionContextBuilder().build(
            visible,
            session_id=session_id,
            branch_id=branch_id,
            tip_event_id=source.id,
            system_prompt=profile.system_prompt,
            system_prompt_source_event_id=self._system_prompt_source_event_id(
                branch_id=branch_id,
                system_prompt=profile.system_prompt,
            ),
            include_reasoning=profile.include_reasoning_in_next_turn,
            max_messages=profile.max_context_messages,
        )
        sampling = {
            "temperature": profile.temperature if temperature is None else temperature,
            "top_p": profile.top_p if top_p is None else top_p,
            "max_tokens": profile.max_tokens,
        }
        group = SamplingService(self.store).create_group(
            from_event_id=source.id,
            context=context.provider_messages(),
            provider_id=profile.provider,
            model_id=profile.id,
            sampling={key: value for key, value in sampling.items() if value is not None},
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
        )
        requests = [
            self._request(
                operation="sample",
                parent_event_id=group.created_event_id,
                model_profile_id=profile_id,
                extra={
                    "sample_group_id": group.id,
                    "sample_ordinal": ordinal,
                    "context_hash": group.context_hash,
                    "from_event_id": source.id,
                    "parallel_sampling": True,
                    **sampling,
                },
            )
            for ordinal in range(count)
        ]
        return {"sample_group": _jsonable(group), "requests": requests}

    def retry(self, event_id: str) -> dict[str, Any]:
        source = self.store.require(event_id)
        retry_event = self._append(
            EventType.ORACLE_RETRY,
            {"event_id": event_id},
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            causation_id=source.id,
        )
        request = self._request(operation="retry", parent_event_id=retry_event.id)
        return {"retry": retry_event.to_dict(), **request}

    def list_events(self, session_id: str | None = None) -> list[dict[str, Any]]:
        if session_id is None:
            with contextlib.suppress(ServiceError):
                session_id, _ = self._active()
        results: list[dict[str, Any]] = []
        for event in self.store.list_events(session_id=session_id, ascending=True):
            value = event.to_dict()
            origins = material_origins(event, self.store.get)
            # Presentation-only annotations.  The canonical envelope/payload is
            # untouched; these fields let curation clients enforce the same
            # transitive synthetic-fixture boundary as projections and exports.
            value["material_origins"] = sorted(origin.value for origin in origins)
            value["synthetic_lineage"] = is_synthetic_lineage(event, self.store.get)
            results.append(value)
        return results

    def tail(self, limit: int = 20) -> list[dict[str, Any]]:
        session_id, _ = self._active()
        events = self.store.list_events(session_id=session_id, ascending=False, limit=limit)
        return [event.to_dict() for event in reversed(events)]

    def show_event(self, event_id: str) -> dict[str, Any]:
        return self.store.require(event_id).to_dict()

    def event_tree(self) -> list[dict[str, Any]]:
        session_id, _ = self._active()
        return [
            {
                "id": event.id,
                "type": event.type.value,
                "parent_event_id": event.parent_event_id,
                "branch_id": event.branch_id,
            }
            for event in self.store.list_events(session_id=session_id)
        ]

    @staticmethod
    def _provenance_result(
        *,
        target: Mapping[str, Any],
        direct_edges: Sequence[Any],
        origins: Sequence[Any],
        actor_origins: Sequence[str],
    ) -> dict[str, Any]:
        """Serialize one provenance query without discarding graph identities."""
        edge_values = [_jsonable(edge) for edge in direct_edges]
        origin_values = [_jsonable(origin) for origin in origins]
        return {
            "target": dict(target),
            "direct_edges": edge_values,
            "direct_source_event_ids": list(
                dict.fromkeys(str(edge["source_event_id"]) for edge in edge_values)
            ),
            "creator_event_ids": list(
                dict.fromkeys(str(edge["created_event_id"]) for edge in edge_values)
            ),
            "actor_origins": sorted(dict.fromkeys(actor_origins)),
            "source_event_ids": [
                str(origin["event"]["id"])
                for origin in origin_values
                if isinstance(origin, Mapping)
                and isinstance(origin.get("event"), Mapping)
                and origin["event"].get("id") is not None
            ],
            "origins": origin_values,
        }

    def provenance_trace(self, derived_kind: str, derived_id: str) -> dict[str, Any]:
        """Return direct edges and complete source lineage for a derived record."""
        from oracle_lab.provenance import ProvenanceService

        provenance = ProvenanceService(self.store)
        return self._provenance_result(
            target={"kind": derived_kind, "id": derived_id},
            direct_edges=provenance.edges_for(derived_kind, derived_id),
            origins=provenance.trace(derived_kind, derived_id),
            actor_origins=tuple(provenance.actor_origins(derived_kind, derived_id)),
        )

    def trace_event(self, event_id: str) -> dict[str, Any]:
        """Return explicit provenance and breadth-first lineage for one event."""
        from oracle_lab.provenance import ProvenanceService

        event = self.store.require(event_id)
        provenance = ProvenanceService(self.store)
        return self._provenance_result(
            target={"kind": "event", "id": event.id, "event": event.to_dict()},
            direct_edges=provenance.edges_for_event(event.id),
            origins=provenance.trace_event(event.id),
            actor_origins=tuple(provenance.actor_origins_for_event(event.id)),
        )

    def _curate(
        self, event_type: EventType, event_id: str, *, note: str | None = None
    ) -> dict[str, Any]:
        source = self.store.require(event_id)
        synthetic = is_synthetic_lineage(source, self.store.get)
        if synthetic and event_type in {EventType.HUMAN_KEEP, EventType.HUMAN_STAR}:
            raise ServiceError("synthetic fixtures cannot be kept or starred as oracle material")
        worker_artifact = is_worker_lineage(source, self.store.get)
        if worker_artifact and event_type in {EventType.HUMAN_KEEP, EventType.HUMAN_STAR}:
            raise ServiceError(
                "worker-generated artifacts cannot be kept or starred as oracle material"
            )
        payload = {"event_id": event_id, "target_event_id": event_id}
        if note is not None:
            payload["note"] = note
        event = self._append(
            event_type,
            payload,
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            causation_id=source.id,
        )
        return event.to_dict()

    def keep(self, event_id: str) -> dict[str, Any]:
        return self._curate(EventType.HUMAN_KEEP, event_id)

    def approve_canon_candidate(self, event_id: str) -> dict[str, Any]:
        """Canonize one nominated claim through an explicit Human decision.

        ``analysis.canon_candidate`` remains advisory.  This method persists a
        claim-specific ``human.keep`` event and then asks the deterministic
        dispatcher to emit ``claim.promoted``.  Both events share one
        transaction, so a mismatched or otherwise invalid approval cannot
        consume the durable gate or leave a half-applied canon decision.
        """

        candidate = self.store.require(event_id)
        if candidate.type is not EventType.ANALYSIS_CANON_CANDIDATE:
            raise ServiceError(f"event is not a canon candidate: {event_id}")
        claim_id = candidate.payload.get("claim_id")
        if not isinstance(claim_id, str) or not claim_id:
            raise ServiceError(f"canon candidate has no claim_id: {event_id}")
        if is_synthetic_lineage(candidate, self.store.get):
            raise ServiceError("synthetic fixture lineage cannot be canonized")
        if is_worker_lineage(candidate, self.store.get):
            raise ServiceError("worker-generated lineage cannot be canonized")
        visible_claim = self.store.connection.execute(
            """
            SELECT 1 FROM branch_claim_states
            WHERE claim_id = ? AND branch_id = ?
            """,
            (claim_id, candidate.branch_id),
        ).fetchone()
        if visible_claim is None:
            raise ServiceError(
                f"canon candidate references a claim not visible on its branch: {claim_id}"
            )

        dispatcher = self._dispatcher()
        decision = next(
            (
                item
                for item in dispatcher.evaluate(candidate)
                if item.rule_id == "human-approve-canon"
            ),
            None,
        )
        if decision is None:
            raise ServiceError("canon candidate has no configured dispatch rule")
        from oracle_lab.dispatcher import DecisionStatus

        if decision.status is not DecisionStatus.PENDING_APPROVAL:
            raise ServiceError("canon candidate does not require human approval")

        with self.store.transaction():
            approval = next(
                (
                    event
                    for event in self.store.list_events(
                        event_type=EventType.HUMAN_KEEP,
                        session_id=candidate.session_id,
                        branch_id=candidate.branch_id,
                        ascending=True,
                    )
                    if event.payload.get("claim_id") == claim_id
                    and event.payload.get("candidate_event_id") == candidate.id
                    and event.payload.get("target_event_id") == candidate.id
                    and event.payload.get("event_id") == candidate.id
                    and event.parent_event_id == candidate.id
                    and event.causation_id == candidate.id
                ),
                None,
            )
            if approval is None:
                approval = self.store.append(
                    Event.new(
                        EventType.HUMAN_KEEP,
                        actor=Actor(kind=ActorKind.HUMAN, id="cli"),
                        session_id=candidate.session_id,
                        branch_id=candidate.branch_id,
                        parent_event_id=candidate.id,
                        causation_id=candidate.id,
                        correlation_id=candidate.correlation_id,
                        payload={
                            "claim_id": claim_id,
                            "candidate_event_id": candidate.id,
                            "event_id": candidate.id,
                            "target_event_id": candidate.id,
                        },
                    )
                )
            approved = dispatcher.approve(
                decision,
                approver_event_id=approval.id,
                source_event=candidate,
            )
            promotion = next(
                (
                    event
                    for event in self.store.list_events(
                        event_type=EventType.CLAIM_PROMOTED,
                        session_id=candidate.session_id,
                        branch_id=candidate.branch_id,
                        ascending=True,
                    )
                    if event.payload.get("claim_id") == claim_id
                    and event.payload.get("candidate_event_id") == candidate.id
                    and event.payload.get("source_event_id") == candidate.id
                    and event.payload.get("approver_event_id") == approval.id
                    and event.payload.get("to_status") == "canonical"
                ),
                None,
            )
            if promotion is None:
                raise ServiceError("canon approval did not emit a canonical promotion")
        return {
            "candidate": candidate.to_dict(),
            "approval_event": approval.to_dict(),
            "decision": _jsonable(approved),
            "promotion_event": promotion.to_dict(),
        }

    def reject(self, event_id: str) -> dict[str, Any]:
        return self._curate(EventType.HUMAN_REJECT, event_id)

    def star(self, event_id: str) -> dict[str, Any]:
        return self._curate(EventType.HUMAN_STAR, event_id)

    def quarantine(self, event_id: str, note: str | None = None) -> dict[str, Any]:
        """Record an explicit human quarantine without rewriting or rejecting it."""
        return self._curate(EventType.HUMAN_QUARANTINE, event_id, note=note)

    def revisit(self, event_id: str, note: str | None = None) -> dict[str, Any]:
        """Record an explicit human request to revisit an archived observation."""
        return self._curate(EventType.HUMAN_REVISIT, event_id, note=note)

    def note(self, event_id: str, text: str) -> dict[str, Any]:
        return self._curate(EventType.HUMAN_NOTE, event_id, note=text)

    def pin_claim(self, claim_id: str) -> dict[str, Any]:
        session_id, branch_id = self._active()
        row = self.store.connection.execute(
            """
            SELECT o.event_id, b.status
            FROM branch_claim_states b
            JOIN claim_occurrences o
              ON o.claim_id = b.claim_id AND o.branch_id = b.branch_id
            WHERE b.claim_id = ? AND b.branch_id = ?
            ORDER BY o.created_at, o.event_id
            LIMIT 1
            """,
            (claim_id, branch_id),
        ).fetchone()
        if row is None:
            raise ServiceError(f"claim is not visible on the active branch: {claim_id}")
        source = self.store.require(str(row["event_id"]))
        event = self._append(
            EventType.HUMAN_PIN,
            {
                "claim_id": claim_id,
                "target_kind": "claim",
                "target_id": claim_id,
            },
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=session_id,
            branch_id=branch_id,
            causation_id=source.id,
        )
        return event.to_dict()

    def _rows(self, sql: str, parameters: Sequence[Any] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self.store.connection.execute(sql, parameters)]

    def claims(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM claims ORDER BY first_seen_at, id")

    def contradictions(self) -> list[dict[str, Any]]:
        return [
            event.to_dict()
            for event in self.store.list_events(
                event_type=[
                    EventType.ANALYSIS_CONTRADICTION_DETECTED,
                    EventType.ANALYSIS_NUMERIC_INCONSISTENCY,
                ]
            )
            if not is_synthetic_lineage(event, self.store.get)
        ]

    def motifs(self) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT id, label, description, length(embedding) AS embedding_bytes "
            "FROM motifs ORDER BY id"
        )

    def attractors(self) -> list[dict[str, Any]]:
        return [
            event.to_dict()
            for event in self.store.list_events(
                event_type=EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED
            )
            if not is_synthetic_lineage(event, self.store.get)
        ]

    def prompt_attractor_statistics(
        self,
        *,
        session_id: str | None = None,
        phrase: str | None = None,
    ) -> dict[str, Any]:
        """Relate exact input wording to observed output-format attractors."""

        if phrase is not None and not phrase:
            raise ServiceError("phrase must not be empty")
        if session_id is None:
            session_id, _ = self._active()
        events = [
            event
            for event in self.store.list_events(session_id=session_id)
            if not is_synthetic_lineage(event, self.store.get)
        ]
        by_id = {event.id: event for event in events}
        prompt_types = {
            EventType.HUMAN_INPUT,
            EventType.ORACLE_CONTEXT_MESSAGE,
            EventType.TOOL_RESULT_ADAPTED,
        }

        def exact_text(event: Event) -> str | None:
            for key in ("text", "content"):
                value = event.payload.get(key)
                if isinstance(value, str):
                    return value
            message = event.payload.get("message")
            if isinstance(message, Mapping) and isinstance(message.get("content"), str):
                return str(message["content"])
            return None

        def nearest_prompt(output: Event) -> Event | None:
            queue: list[str] = [
                value
                for value in (output.causation_id, output.parent_event_id)
                if isinstance(value, str)
            ]
            seen: set[str] = set()
            while queue:
                event_id = queue.pop(0)
                if event_id in seen:
                    continue
                seen.add(event_id)
                candidate = by_id.get(event_id)
                if candidate is None:
                    continue
                if candidate.type in prompt_types and exact_text(candidate) is not None:
                    return candidate
                queue.extend(
                    value
                    for value in (candidate.parent_event_id, candidate.causation_id)
                    if isinstance(value, str) and value not in seen
                )
            return None

        attractors_by_output: dict[str, set[str]] = {}
        for event in events:
            if event.type is not EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED:
                continue
            attractor = event.payload.get("attractor")
            if not isinstance(attractor, str):
                continue
            raw_sources = event.payload.get("source_event_ids", ())
            source_ids = (
                [value for value in raw_sources if isinstance(value, str)]
                if isinstance(raw_sources, Sequence)
                and not isinstance(raw_sources, (str, bytes, bytearray))
                else []
            )
            if isinstance(event.causation_id, str):
                source_ids.append(event.causation_id)
            for source_id in source_ids:
                source = by_id.get(source_id)
                if source is not None and source.type is EventType.ORACLE_OUTPUT:
                    attractors_by_output.setdefault(source.id, set()).add(attractor)

        pairs: list[dict[str, Any]] = []
        for output in events:
            if output.type is not EventType.ORACLE_OUTPUT:
                continue
            prompt_event = nearest_prompt(output)
            if prompt_event is None or (prompt_text := exact_text(prompt_event)) is None:
                continue
            pairs.append(
                {
                    "prompt_event_id": prompt_event.id,
                    "prompt_event_type": prompt_event.type.value,
                    "exact_prompt": prompt_text,
                    "prompt_sha256": sha256_text(prompt_text),
                    "output_event_id": output.id,
                    "attractors": sorted(attractors_by_output.get(output.id, set())),
                }
            )

        phrases = (
            [phrase]
            if phrase is not None
            else sorted(
                {
                    *_PROMPT_ATTRACTOR_PHRASES,
                    *(
                        token
                        for pair in pairs
                        for token in _RESEARCH_WORD_RE.findall(str(pair["exact_prompt"]))
                    ),
                }
            )
        )
        statistics: list[dict[str, Any]] = []
        for candidate_phrase in phrases:
            matching = [pair for pair in pairs if candidate_phrase in str(pair["exact_prompt"])]
            if not matching:
                continue
            attractor_counts: dict[str, int] = {}
            for pair in matching:
                for attractor in pair["attractors"]:
                    attractor_counts[attractor] = attractor_counts.get(attractor, 0) + 1
            denominator = len(matching)
            statistics.append(
                {
                    "phrase": candidate_phrase,
                    "prompt_count": len({str(pair["prompt_event_id"]) for pair in matching}),
                    "output_count": denominator,
                    "attractor_counts": dict(sorted(attractor_counts.items())),
                    "attractor_probability": {
                        key: count / denominator for key, count in sorted(attractor_counts.items())
                    },
                    "prompt_event_ids": list(
                        dict.fromkeys(str(pair["prompt_event_id"]) for pair in matching)
                    ),
                    "output_event_ids": [str(pair["output_event_id"]) for pair in matching],
                }
            )
        return {
            "session_id": session_id,
            "pair_count": len(pairs),
            "phrase_statistics": statistics,
            "pairs": pairs,
        }

    def contradiction_mechanism_branches(
        self, session_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Return branches where a contradiction is visible before a mechanism."""
        if session_id is None:
            session_id, _ = self._active()
        branch_service = self._branch_service()
        results: list[dict[str, Any]] = []
        contradiction_types = {
            EventType.ANALYSIS_CONTRADICTION_DETECTED,
            EventType.ANALYSIS_NUMERIC_INCONSISTENCY,
        }

        def ancestry(source_ids: Sequence[str]) -> set[str]:
            pending = list(source_ids)
            seen: set[str] = set()
            while pending:
                event_id = pending.pop()
                if event_id in seen:
                    continue
                event = self.store.get(event_id)
                if event is None:
                    continue
                seen.add(event.id)
                pending.extend(
                    identifier
                    for identifier in (event.parent_event_id, event.causation_id)
                    if identifier is not None and identifier not in seen
                )
            return seen

        for branch in branch_service.list_branches(session_id=session_id, include_archived=True):
            preceding: list[str] = []
            sequences: list[dict[str, Any]] = []
            for event in branch_service.visible_events(branch.id):
                if is_synthetic_lineage(event, self.store.get):
                    continue
                if event.type in contradiction_types:
                    preceding.append(event.id)
                    continue
                if event.type is not EventType.ANALYSIS_NEW_MECHANISM_DETECTED or not preceding:
                    continue
                raw_sources = event.payload.get("source_event_ids", ())
                source_ids = [item for item in raw_sources if isinstance(item, str)]
                causal_predecessors = [
                    event_id for event_id in preceding if event_id in ancestry(source_ids)
                ]
                if not causal_predecessors:
                    continue
                sequences.append(
                    {
                        "mechanism_event_id": event.id,
                        "mechanism": event.payload.get("mechanism"),
                        "mechanism_source_event_ids": list(
                            event.payload.get("source_event_ids", ())
                        ),
                        "preceding_contradiction_event_ids": causal_predecessors,
                    }
                )
            if sequences:
                results.append({"branch": _jsonable(branch), "sequences": sequences})
        return results

    def words_before_latex_attractors(
        self,
        *,
        session_id: str | None = None,
        word_count: int = 5,
    ) -> list[dict[str, Any]]:
        """Return lexical windows immediately preceding detected LaTeX notation."""
        if word_count < 1:
            raise ServiceError("word_count must be positive")
        if session_id is None:
            session_id, _ = self._active()
        results: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        attractors = self.store.list_events(
            session_id=session_id,
            event_type=EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED,
        )
        for attractor in attractors:
            if is_synthetic_lineage(attractor, self.store.get):
                continue
            markers = tuple(
                marker for marker in attractor.payload.get("markers", ()) if isinstance(marker, str)
            )
            if attractor.payload.get("attractor") != "latex_notation" and not any(
                _LATEX_START_RE.search(marker) for marker in markers
            ):
                continue
            raw_sources = attractor.payload.get("source_event_ids", ())
            source_ids = [item for item in raw_sources if isinstance(item, str)]
            if not source_ids and attractor.causation_id is not None:
                source_ids = [attractor.causation_id]
            for source_id in source_ids:
                source = self.store.require(source_id)
                if is_synthetic_lineage(source, self.store.get):
                    continue
                text = next(
                    (
                        value
                        for key in ("raw_text", "content", "text", "output")
                        if isinstance((value := source.payload.get(key)), str)
                    ),
                    "",
                )
                display_math_open = False
                for match in _LATEX_START_RE.finditer(text):
                    if match.group(0) == "$$":
                        if display_math_open:
                            display_math_open = False
                            continue
                        display_math_open = True
                    identity = (source.id, match.start())
                    if identity in seen:
                        continue
                    seen.add(identity)
                    words = _RESEARCH_WORD_RE.findall(text[: match.start()])[-word_count:]
                    results.append(
                        {
                            "attractor_event_id": attractor.id,
                            "source_event_id": source.id,
                            "branch_id": source.branch_id,
                            "latex_marker": match.group(0),
                            "offset": match.start(),
                            "words": words,
                            "prefix": " ".join(words),
                        }
                    )
        return results

    def fork_before_attractor(
        self, attractor_event_id: str, title: str | None = None
    ) -> dict[str, Any]:
        """Fork at the event immediately before an attractor's oracle source."""
        attractor = self.store.require(attractor_event_id)
        if attractor.type is not EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED:
            raise ServiceError(f"event is not an attractor analysis: {attractor_event_id}")
        if is_synthetic_lineage(attractor, self.store.get):
            raise ServiceError("synthetic attractors cannot be used as fork sources")
        raw_sources = attractor.payload.get("source_event_ids", ())
        candidates = [item for item in raw_sources if isinstance(item, str)]
        if attractor.causation_id is not None:
            candidates.append(attractor.causation_id)
        source = next(
            (
                event
                for event_id in dict.fromkeys(candidates)
                if (event := self.store.get(event_id)) is not None
                and event.type is EventType.ORACLE_OUTPUT
            ),
            None,
        )
        if source is None:
            raise ServiceError(f"attractor has no cited oracle output: {attractor_event_id}")
        if is_synthetic_lineage(source, self.store.get):
            raise ServiceError(
                "synthetic oracle material cannot be used as an attractor fork source"
            )
        if source.parent_event_id is None:
            raise ServiceError(f"oracle output has no pre-attractor event: {source.id}")
        branch = self.fork(
            source.parent_event_id,
            title or f"before-{attractor.payload.get('attractor', 'attractor')}",
        )
        return {
            "attractor_event_id": attractor.id,
            "source_event_id": source.id,
            "fork_event_id": source.parent_event_id,
            "branch": branch,
        }

    def search(
        self, query: str, *, semantic: bool = False, limit: int = 20
    ) -> list[dict[str, Any]]:
        session_id, _ = self._active()
        events = [
            event
            for event in self.store.list_events(session_id=session_id)
            if not is_synthetic_lineage(event, self.store.get)
        ]
        index = RetrievalIndex.from_events(events)
        motif_rows = self._rows(
            """
            SELECT m.id, m.label, m.description, m.embedding,
                   em.event_id AS source_event_id,
                   e.session_id, e.branch_id, e.created_at
            FROM motifs m
            JOIN event_motifs em ON em.motif_id = m.id
            JOIN events e ON e.id = em.event_id
            WHERE e.session_id = ?
            ORDER BY e.created_at, em.event_id, m.id
            """,
            (session_id,),
        )
        motif_records: dict[str, dict[str, Any]] = {}
        for motif in motif_rows:
            motif_id = str(motif["id"])
            record = motif_records.setdefault(
                motif_id,
                {**motif, "source_event_ids": []},
            )
            source_event_id = motif.get("source_event_id")
            if isinstance(source_event_id, str):
                record["source_event_ids"].append(source_event_id)
        for motif in motif_records.values():
            motif["source_event_ids"] = list(dict.fromkeys(motif["source_event_ids"]))
            index.add(RetrievalDocument.from_motif(motif))
        hits = (
            index.semantic_search(query, limit=limit)
            if semantic
            else index.by_text_substring(query, case_sensitive=False)
        )
        return [
            {
                "document_id": hit.document.id,
                "event_id": hit.document.metadata.get("source_event_id", hit.event_id),
                "kind": hit.document.kind,
                "source_event_id": hit.document.metadata.get("source_event_id"),
                "source_event_ids": list(hit.document.metadata.get("source_event_ids", ())),
                "score": hit.score,
                "matched_by": hit.matched_by,
                "text": hit.document.text,
            }
            for hit in hits[:limit]
        ]

    def origin(self, query: str) -> dict[str, Any] | None:
        direct = self.store.get(query)
        if direct is not None:
            return self.trace_event(direct.id)
        hits = self.search(query, limit=1)
        if not hits:
            return None
        hit = hits[0]
        kind = str(hit.get("kind", "event"))
        identifier = str(hit["document_id"])
        if kind == "motif":
            return self.provenance_trace("motif", identifier)
        return self.trace_event(identifier)

    def propose_probe(self, event_id: str) -> dict[str, Any]:
        source = self.store.require(event_id)
        human = self._append(
            EventType.HUMAN_REQUEST_PROBE,
            {"event_id": event_id},
            actor=Actor(kind=ActorKind.HUMAN, id="tui"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            causation_id=source.id,
        )
        if source.type == EventType.ANALYSIS_PROBE_PROPOSED:
            probe = source.payload.get("probe")
            if not isinstance(probe, str) or not probe.strip():
                raise ServiceError(f"probe proposal has no text: {source.id}")
            adapter = self._append(
                EventType.ORACLE_CONTEXT_MESSAGE,
                {
                    "role": "user",
                    "content": probe,
                    "source_event_id": source.id,
                    "approval_event_id": human.id,
                },
                actor=Actor(kind=ActorKind.HOST, id="probe-approval-adapter"),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=human.id,
                causation_id=human.id,
                correlation_id=source.correlation_id,
            )
            request = self._request(
                operation="approved-probe",
                parent_event_id=adapter.id,
            )
            return {
                "human_request": human.to_dict(),
                "context_message": adapter.to_dict(),
                **request,
            }

        probe_text = "確認しろ。"
        if source.type == EventType.ANALYSIS_CONTRADICTION_DETECTED:
            suggested = source.payload.get("suggested_probe")
            if isinstance(suggested, str) and suggested.strip():
                probe_text = suggested
        proposal = self._append(
            EventType.ANALYSIS_PROBE_PROPOSED,
            {
                "probe": probe_text,
                "approval_required": self.runtime_config.policies.human_gate.get(
                    "probe_generation", True
                ),
                "tests_one_dimension": True,
                "source_event_ids": [source.id],
            },
            actor=Actor(kind=ActorKind.HOST, id="probe-planner"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=human.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
        )
        if not self.runtime_config.policies.human_gate.get("probe_generation", True):
            self._dispatcher().dispatch(proposal)
        return {
            "human_request": human.to_dict(),
            "proposal": proposal.to_dict(),
        }

    def _tool_policy_modes(self) -> dict[str, str]:
        configured = self.runtime_config.policies.tools
        return {
            "calculator": str(configured.get("calculator", "auto")),
            "unit_convert": str(configured.get("unit_conversion", "auto")),
            "regex": str(configured.get("regex_text", "auto")),
            "checksum": str(configured.get("checksum", "auto")),
            "file_parse": str(configured.get("file_parsing", "auto")),
            "python": str(configured.get("python_sandbox", configured.get("python", "deny"))),
            "shell": str(configured.get("shell_sandbox", configured.get("shell", "ask"))),
            "virtual": str(configured.get("virtual_world", configured.get("virtual", "auto"))),
            "web_verify": str(configured.get("web_verify", "ask")),
        }

    def _tool_request_from_source(self, source: Event) -> Any:
        from oracle_lab.tooling import ToolRequest

        payload = thaw_json(source.payload)
        nested = payload.get("tool_request") or payload.get("request")
        if isinstance(nested, Mapping):
            return ToolRequest.from_dict(nested, source_event_id=source.id)
        if all(key in payload for key in ("tool", "execution", "input")):
            return ToolRequest.from_dict(payload, source_event_id=source.id)
        if source.type == EventType.ORACLE_OUTPUT:
            related = self.store.list_events(
                event_type=EventType.ANALYSIS_TOOL_INTENT_DETECTED,
                causation_id=source.id,
            )
            if related:
                return self._tool_request_from_source(related[-1])
        commands = payload.get("commands")
        if isinstance(commands, Sequence) and not isinstance(commands, (str, bytes)):
            command = next((value for value in commands if isinstance(value, str)), None)
            if command is not None:
                return ToolRequest(
                    tool="shell",
                    execution="real_sandbox",
                    input={"command": command},
                    source_event_id=source.id,
                    resume_oracle=True,
                    timeout_ms=self.runtime_config.tools.sandbox.timeout_ms,
                )
        factor = payload.get("factor")
        base_seconds = payload.get("base_seconds")
        expression = payload.get("expression")
        if (
            not isinstance(expression, str)
            and isinstance(factor, (int, float))
            and isinstance(base_seconds, (int, float))
        ):
            expression = f"{factor} * {base_seconds}"
        if isinstance(expression, str):
            return ToolRequest(
                tool="calculator",
                execution="real_deterministic",
                input={"expression": expression},
                source_event_id=source.id,
                resume_oracle=True,
            )
        raise ServiceError(f"event has no executable tool-request schema: {source.id}")

    def _schedule_tool_request(self, request_event: Event, *, approved: bool) -> dict[str, Any]:
        from oracle_lab.tooling import ToolRequest

        request = ToolRequest.from_dict(thaw_json(request_event.payload))
        mode = self._tool_policy_modes().get(request.tool, "deny")
        if mode == "deny":
            return {
                "request": request_event.to_dict(),
                "approval": "denied",
                "job": None,
            }
        if mode == "ask" and not approved:
            return {
                "request": request_event.to_dict(),
                "approval": "required",
                "job": None,
            }
        job = self._job_queue().enqueue(
            "tool.execute",
            {"request_event_id": request_event.id, "approved": approved},
            source_event_id=request_event.id,
            idempotency_key=f"tool.execute:{request_event.id}:{int(approved)}",
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            serialize_branch=True,
        )
        return {
            "request": request_event.to_dict(),
            "approval": "approved" if approved else mode,
            "job": _jsonable(job),
        }

    def _dispatch_tool_intent(self, source: Event) -> dict[str, Any] | None:
        """Turn a structured host proposal into one durable policy-gated request."""
        if source.type != EventType.ANALYSIS_TOOL_INTENT_DETECTED:
            return None
        nested = source.payload.get("tool_request")
        if not isinstance(nested, Mapping):
            return None
        existing = self.store.list_events(
            event_type=EventType.TOOL_REQUEST,
            causation_id=source.id,
        )
        if existing:
            return self._schedule_tool_request(existing[0], approved=False)
        _, budget = self._automation_state(source)
        if budget <= 0:
            stopped = self._stop_automation(source, "budget_exhausted")
            return {"request": None, "approval": "stopped", "stop": stopped.to_dict()}
        from oracle_lab.tooling import ToolRequest

        request = ToolRequest.from_dict(nested, source_event_id=source.id)
        request_event = self._append(
            EventType.TOOL_REQUEST,
            {
                **request.to_dict(),
                **self._automation_payload(source, consume=1),
            },
            actor=Actor(kind=ActorKind.HOST, id="tool-intent-dispatcher"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
        )
        return self._schedule_tool_request(request_event, approved=False)

    def _verification_request_on_fork(
        self,
        request_event: Event,
        *,
        authorizer_event: Event,
    ) -> Event:
        """Clone an approved verification request onto an isolated branch."""
        if request_event.payload.get("tool") != "web_verify":
            return request_event
        if request_event.payload.get("verification_origin_request_id") is not None:
            return request_event
        existing = [
            event
            for event in self.store.list_events(event_type=EventType.TOOL_REQUEST)
            if event.payload.get("verification_origin_request_id") == request_event.id
        ]
        if existing:
            return existing[0]
        source_id = request_event.payload.get("source_event_id")
        source = self.store.get(str(source_id)) if isinstance(source_id, str) else None
        if source is None or source.session_id is None or source.branch_id is None:
            source = request_event
        fork_request = self._append(
            EventType.HUMAN_REQUEST_FORK,
            {
                "event_id": source.id,
                "purpose": "factual_verification",
                "tool_request_event_id": request_event.id,
                "authorizer_event_id": authorizer_event.id,
            },
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=request_event.id,
            causation_id=authorizer_event.id,
            correlation_id=request_event.correlation_id,
        )
        branch = self._branch_service().fork(
            source.id,
            title=f"verify-{source.id[-8:]}",
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            correlation_id=request_event.correlation_id,
        )
        fork_event = self.store.list_events(
            event_type=EventType.SESSION_FORKED,
            branch_id=branch.id,
            ascending=False,
            limit=1,
        )[0]
        return self._append(
            EventType.TOOL_REQUEST,
            {
                **thaw_json(request_event.payload),
                "verification_origin_request_id": request_event.id,
                "verification_source_event_id": source.id,
                "verification_authorizer_event_id": authorizer_event.id,
                "verification_fork_request_event_id": fork_request.id,
                "truth_domain": "retrieved",
            },
            actor=Actor(kind=ActorKind.HOST, id="verification-branch-router"),
            session_id=source.session_id,
            branch_id=branch.id,
            parent_event_id=fork_event.id,
            causation_id=authorizer_event.id,
            correlation_id=request_event.correlation_id,
        )

    def request_tool(self, event_id: str) -> dict[str, Any]:
        source = self.store.require(event_id)
        if source.type == EventType.TOOL_REQUEST:
            return self._schedule_tool_request(source, approved=False)
        automatic = self.store.list_events(
            event_type=EventType.TOOL_REQUEST,
            causation_id=source.id,
        )
        if automatic:
            return self._schedule_tool_request(automatic[0], approved=False)
        tool_request = self._tool_request_from_source(source)
        human = self._append(
            EventType.HUMAN_REQUEST_TOOL,
            {"event_id": event_id, "tool_request": tool_request.to_dict()},
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            causation_id=source.id,
        )
        request = self._append(
            EventType.TOOL_REQUEST,
            {
                **tool_request.to_dict(),
                **self._automation_payload(human),
            },
            actor=Actor(kind=ActorKind.HOST, id="tool-broker"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=human.id,
            causation_id=human.id,
            correlation_id=source.correlation_id,
        )
        return self._schedule_tool_request(request, approved=False)

    def approve_tool(self, request_id: str) -> dict[str, Any]:
        request = self.store.require(request_id)
        if request.type != EventType.TOOL_REQUEST:
            raise ServiceError(f"event is not a tool request: {request_id}")
        event = self._append(
            EventType.TOOL_APPROVED,
            {"request_event_id": request_id},
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=request.session_id,
            branch_id=request.branch_id,
            causation_id=request.id,
        )
        execution_request = self._verification_request_on_fork(
            request,
            authorizer_event=event,
        )
        execution_approval = event
        if execution_request.id != request.id:
            execution_approval = self._append(
                EventType.TOOL_APPROVED,
                {
                    "request_event_id": execution_request.id,
                    "verification_origin_request_id": request.id,
                    "authorizer_event_id": event.id,
                },
                actor=Actor(kind=ActorKind.HUMAN, id="cli"),
                session_id=execution_request.session_id,
                branch_id=execution_request.branch_id,
                parent_event_id=execution_request.id,
                causation_id=execution_request.id,
                correlation_id=execution_request.correlation_id,
            )
        scheduled = self._schedule_tool_request(execution_request, approved=True)
        return {
            "approval_event": event.to_dict(),
            "execution_approval_event": execution_approval.to_dict(),
            "verification_branch_id": (
                execution_request.branch_id if execution_request.id != request.id else None
            ),
            **scheduled,
        }

    def inspect_sandbox(self, identifier: str) -> list[dict[str, Any]]:
        return [
            event.to_dict()
            for event in self.store.list_events(
                event_type=[
                    EventType.TOOL_REQUEST,
                    EventType.TOOL_APPROVED,
                    EventType.TOOL_STARTED,
                    EventType.TOOL_OUTPUT,
                    EventType.TOOL_ERROR,
                    EventType.TOOL_TIMEOUT,
                ]
            )
            if event.id == identifier
            or event.causation_id == identifier
            or event.payload.get("request_event_id") == identifier
        ]

    def generation_metadata(self, event_id: str) -> dict[str, Any]:
        event = self.store.require(event_id)
        payload = thaw_json(event.payload)
        return {
            "event_id": event.id,
            "created_at": event.created_at.isoformat(),
            "model": payload.get("model") or payload.get("model_profile_id"),
            "provider": payload.get("provider"),
            "sampling": payload.get("sampling"),
            "requested_sampling": payload.get("sampling"),
            "effective_sampling": payload.get("effective_sampling"),
            "context_hash": payload.get("context_hash"),
            "material_origin": payload.get("material_origin", "unknown"),
            "model_identity": payload.get("model_identity"),
            "api_response_metadata": payload.get("api_response_metadata"),
            "provider_request_id": payload.get("provider_request_id"),
            "archive_path": payload.get("archive_path"),
            "archive_sha256": payload.get("archive_sha256"),
            "metadata": thaw_json(event.metadata),
        }

    def _pending_human_judgment(self) -> Event | None:
        session_id, branch_id = self._active()
        pending_patch = self.store.connection.execute(
            """
            SELECT patch_event_id FROM candidate_patches
            WHERE status = 'pending_human' AND session_id = ? AND branch_id = ?
            ORDER BY created_at, patch_event_id LIMIT 1
            """,
            (session_id, branch_id),
        ).fetchone()
        if pending_patch is not None:
            return self.store.require(str(pending_patch["patch_event_id"]))
        proposals = self.store.list_events(
            event_type=[
                EventType.ANALYSIS_PROBE_PROPOSED,
                EventType.ANALYSIS_CANON_CANDIDATE,
                EventType.ANALYSIS_BRANCH_PROPOSED,
            ],
            session_id=session_id,
            branch_id=branch_id,
        )
        actions = self.store.list_events(
            event_type=[
                EventType.HUMAN_REQUEST_PROBE,
                EventType.HUMAN_REQUEST_FORK,
                EventType.HUMAN_KEEP,
                EventType.HUMAN_REJECT,
            ],
            session_id=session_id,
            branch_id=branch_id,
        )
        resolved = {
            str(
                action.payload.get("proposal_event_id")
                or action.payload.get("event_id")
                or action.payload.get("target_event_id")
            )
            for action in actions
        }
        gates = self.runtime_config.policies.human_gate

        def requires_approval(proposal: Event) -> bool:
            if proposal.type == EventType.ANALYSIS_PROBE_PROPOSED:
                return gates.get("probe_generation", True)
            if proposal.type == EventType.ANALYSIS_BRANCH_PROPOSED:
                return gates.get("branch_creation", False)
            return True

        pending = next(
            (
                proposal
                for proposal in proposals
                if proposal.id not in resolved and requires_approval(proposal)
            ),
            None,
        )
        if pending is not None:
            return pending
        tool_requests = self.store.list_events(
            event_type=EventType.TOOL_REQUEST,
            session_id=session_id,
            branch_id=branch_id,
        )
        approvals = {
            str(event.payload.get("request_event_id"))
            for event in self.store.list_events(
                event_type=EventType.TOOL_APPROVED,
                session_id=session_id,
                branch_id=branch_id,
            )
        }
        for request_event in tool_requests:
            tool = str(request_event.payload.get("tool", ""))
            if (
                self._tool_policy_modes().get(tool, "deny") == "ask"
                and request_event.id not in approvals
            ):
                return request_event
        return None

    def _broker(self, request_event: Event | None = None) -> Any:
        if request_event is not None and request_event.payload.get("execution") == "virtual":
            if request_event.session_id is None or request_event.branch_id is None:
                raise ServiceError("virtual tool requests require session and branch identities")
            from oracle_lab.projections import VirtualStateService
            from oracle_lab.tooling import (
                DockerShellSandbox,
                HttpVerificationTool,
                ToolBroker,
                ToolPolicy,
            )
            from oracle_lab.virtual import VirtualArtifactMaterializer, VirtualClockMaterializer

            virtual_state = VirtualStateService(self.store)
            execution_sink = virtual_state.mutation_sink(
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                actor=Actor(kind=ActorKind.TOOL, id="virtual-runtime"),
                correlation_id=request_event.correlation_id,
            )
            materialization_sink = virtual_state.mutation_sink(
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                actor=Actor(kind=ActorKind.HOST, id="virtual-materializer"),
                correlation_id=request_event.correlation_id,
            )
            virtual = virtual_state.hydrate(
                request_event.branch_id,
                mutation_sink=execution_sink,
            )

            def prepare_virtual_operation(command: str) -> None:
                branch_events = self._branch_service().visible_events(request_event.branch_id)
                cutoff = (request_event.created_at, request_event.id)
                visible = [
                    event
                    for event in branch_events
                    if event.branch_id != request_event.branch_id
                    or (event.created_at, event.id) <= cutoff
                ]
                virtual.set_mutation_sink(materialization_sink)
                try:
                    VirtualArtifactMaterializer().materialize_for_operation(
                        virtual,
                        command,
                        visible_events=visible,
                        request_event=request_event,
                    )
                    VirtualClockMaterializer().materialize_for_operation(
                        virtual,
                        command,
                        request_event=request_event,
                    )
                finally:
                    virtual.set_mutation_sink(execution_sink)

            return ToolBroker(
                policy=ToolPolicy(modes=self._tool_policy_modes()),
                shell=DockerShellSandbox(self.runtime_config.tools.sandbox),
                virtual=virtual,
                verification=(
                    HttpVerificationTool(
                        allowed_hosts=self.runtime_config.tools.verification_allowed_hosts,
                        max_output_bytes=(self.runtime_config.tools.verification_max_output_bytes),
                    )
                    if self.runtime_config.tools.verification_allowed_hosts
                    else None
                ),
                allowed_virtual_commands=self.runtime_config.tools.allowed_virtual_commands,
                virtual_operation_preparer=prepare_virtual_operation,
            )
        if self._tool_broker is None:
            from oracle_lab.tooling import (
                DockerShellSandbox,
                HttpVerificationTool,
                ToolBroker,
                ToolPolicy,
            )

            self._tool_broker = ToolBroker(
                policy=ToolPolicy(modes=self._tool_policy_modes()),
                shell=DockerShellSandbox(self.runtime_config.tools.sandbox),
                verification=(
                    HttpVerificationTool(
                        allowed_hosts=self.runtime_config.tools.verification_allowed_hosts,
                        max_output_bytes=(self.runtime_config.tools.verification_max_output_bytes),
                    )
                    if self.runtime_config.tools.verification_allowed_hosts
                    else None
                ),
                allowed_virtual_commands=self.runtime_config.tools.allowed_virtual_commands,
            )
        return self._tool_broker

    @staticmethod
    def _mechanical_tool_result_content(request: Any, result_event: Event) -> str:
        """Format a tool observation without interpretive Host prose."""

        tool_input = thaw_json(request.input)
        command = tool_input.get("command")
        expression = tool_input.get("expression")
        url = tool_input.get("url")
        if isinstance(command, str):
            invocation = command
        elif isinstance(expression, str):
            invocation = f"{request.tool} {expression}"
        elif isinstance(url, str) and request.tool == "web_verify":
            invocation = f"GET {url}"
        else:
            invocation = f"{request.tool} {canonical_json(tool_input)}"
        output = result_event.payload.get("output", "")
        return f"$ {invocation}\n{output if isinstance(output, str) else str(output)}"

    @staticmethod
    def _tool_loop_signature(request: Any, result_event: Event) -> str:
        """Hash semantic tool input/result fields, excluding event identities."""

        return sha256_json(
            {
                "tool": request.tool,
                "execution": request.execution.value,
                "input": thaw_json(request.input),
                "status": result_event.payload.get("status"),
                "output": result_event.payload.get("output"),
                "error": result_event.payload.get("error"),
                "exit_code": result_event.payload.get("exit_code"),
                "truth_domain": result_event.payload.get("truth_domain"),
            }
        )

    def _execute_tool_job(self, job: Any) -> Event:
        from oracle_lab.tooling import ToolRequest, ToolStatus
        from oracle_lab.usage import UsageService

        request_event = self.store.require(str(job.payload["request_event_id"]))
        if request_event.type != EventType.TOOL_REQUEST:
            raise ServiceError(f"tool job source is not a tool.request: {request_event.id}")
        request = ToolRequest.from_dict(
            thaw_json(request_event.payload), source_event_id=request_event.id
        )
        approved = bool(job.payload.get("approved", False))
        if approved and self._tool_policy_modes().get(request.tool, "deny") == "ask":
            approvals = [
                event
                for event in self.store.list_events(
                    event_type=EventType.TOOL_APPROVED,
                    causation_id=request_event.id,
                )
                if event.payload.get("request_event_id") == request_event.id
                and event.session_id == request_event.session_id
                and event.branch_id == request_event.branch_id
                and event.actor.kind is ActorKind.HUMAN
            ]
            if not approvals:
                raise ServiceError(
                    f"approved tool job has no matching human approval event: {request_event.id}"
                )

        result_events = self.store.list_events(
            event_type=[
                EventType.TOOL_OUTPUT,
                EventType.TOOL_ERROR,
                EventType.TOOL_TIMEOUT,
                EventType.TOOL_DENIED,
            ],
            causation_id=request_event.id,
        )
        matching_results = [
            event for event in result_events if event.payload.get("request_id") == request.id
        ]
        if matching_results:
            result_event = matching_results[0]
            result_status = ToolStatus(str(result_event.payload.get("status", "error")))
            elapsed_ms = float(result_event.payload.get("elapsed_ms", 0.0))
        else:
            started = self._append(
                EventType.TOOL_STARTED,
                {
                    "request_event_id": request_event.id,
                    "tool_request_id": request.id,
                    **self._automation_payload(request_event),
                },
                actor=Actor(kind=ActorKind.TOOL, id=request.tool),
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                parent_event_id=request_event.id,
                causation_id=request_event.id,
                correlation_id=request_event.correlation_id,
            )
            with self.observability.operation(
                "tool.execute",
                event=request_event,
                fields={"tool": request.tool, "execution": request.execution.value},
            ):
                result = self._broker(request_event).execute(request, approved=approved)
            result_status = result.status
            elapsed_ms = result.elapsed_ms
            generated = result.to_event(
                request,
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                correlation_id=request_event.correlation_id,
            )
            result_event = Event.new(
                generated.type,
                actor=generated.actor,
                session_id=generated.session_id,
                branch_id=generated.branch_id,
                parent_event_id=started.id,
                causation_id=request_event.id,
                correlation_id=generated.correlation_id,
                payload={
                    **thaw_json(generated.payload),
                    **self._automation_payload(request_event),
                },
                metadata=thaw_json(generated.metadata),
            )
            self.store.append(result_event)
            self.observability.log_event(
                result_event,
                fields={"operation": "tool.execute", "tool": request.tool},
            )

        usage_events = self.store.list_events(
            event_type=EventType.USAGE_TOOL,
            causation_id=request_event.id,
        )
        if not any(
            event.metadata.get("result_event_id") == result_event.id for event in usage_events
        ):
            usage = UsageService(self.store).record(
                "tool",
                request_event_id=request_event.id,
                tool_id=request.tool,
                latency_ms=elapsed_ms,
                metadata={"result_event_id": result_event.id},
            )
            self.observability.log_event(
                self.store.require(usage.event_id),
                fields={"operation": "tool.usage", "tool": request.tool},
            )
        if request.resume_oracle and result_status != ToolStatus.OK:
            self._stop_automation(
                result_event,
                "tool_failure",
                detail={"status": result_status.value, "tool": request.tool},
            )
        if (
            result_status == ToolStatus.OK
            and request.resume_oracle
            and self.runtime_config.policies.auto_continue_after_tool_result
        ):
            previous_adapters = self.store.list_events(
                event_type=EventType.TOOL_RESULT_ADAPTED,
                correlation_id=request_event.correlation_id,
            )
            adapter = next(
                (
                    event
                    for event in previous_adapters
                    if event.payload.get("tool_output_event_id") == result_event.id
                ),
                None,
            )
            signature = self._tool_loop_signature(request, result_event)
            if adapter is None:
                depth, budget = self._automation_state(request_event)
                if depth >= self.runtime_config.policies.max_auto_depth:
                    self._stop_automation(
                        result_event,
                        "max_depth",
                        detail={"loop_signature": signature},
                    )
                    return result_event
                if budget <= 0:
                    self._stop_automation(
                        result_event,
                        "budget_exhausted",
                        detail={"loop_signature": signature},
                    )
                    return result_event
                repeated = next(
                    (
                        event
                        for event in previous_adapters
                        if event.payload.get("loop_signature") == signature
                    ),
                    None,
                )
                if repeated is not None:
                    self._stop_automation(
                        result_event,
                        "repeated_equivalent_event",
                        detail={
                            "loop_signature": signature,
                            "equivalent_event_id": repeated.id,
                        },
                    )
                    return result_event
                content = self._mechanical_tool_result_content(request, result_event)
                adapter = self._append(
                    EventType.TOOL_RESULT_ADAPTED,
                    {
                        "role": "user",
                        "content": content,
                        "content_sha256": sha256_text(content),
                        "formatter_id": "mechanical-tool-result",
                        "formatter_version": 1,
                        "truth_domain": result_event.payload.get("truth_domain"),
                        "source_event_id": result_event.id,
                        "source_event_ids": [result_event.id, request_event.id],
                        "tool_request_event_id": request_event.id,
                        "tool_output_event_id": result_event.id,
                        **self._automation_payload(
                            request_event,
                            consume=1,
                            depth_increment=1,
                            loop_signature=signature,
                        ),
                    },
                    actor=Actor(kind=ActorKind.HOST, id="tool-result-adapter"),
                    session_id=request_event.session_id,
                    branch_id=request_event.branch_id,
                    parent_event_id=result_event.id,
                    causation_id=result_event.id,
                    correlation_id=request_event.correlation_id,
                )
            continuations = [
                event
                for event in self.store.list_events(
                    event_type=EventType.ORACLE_REQUEST,
                    session_id=request_event.session_id,
                    branch_id=request_event.branch_id,
                )
                if event.parent_event_id == adapter.id
                and event.payload.get("operation") == "tool-result"
            ]
            if continuations:
                self._enqueue_request(continuations[0])
            else:
                adapter_depth, adapter_budget = self._automation_state(adapter)
                if adapter_budget <= 0:
                    self._stop_automation(
                        adapter,
                        "budget_exhausted",
                        detail={"loop_signature": signature},
                    )
                else:
                    self._request(
                        operation="tool-result",
                        parent_event_id=adapter.id,
                        extra={
                            "automation_depth": adapter_depth,
                            "automation_budget_remaining": adapter_budget - 1,
                            "automation_loop_detector": ("sha256-equivalent-event-v1"),
                            "loop_signature": signature,
                        },
                    )
        return result_event

    def _execute_oracle_job(self, job: Any) -> Event:
        request_event = self.store.require(str(job.payload["request_event_id"]))
        profile_id = str(job.payload["model_profile_id"])
        profile = self.runtime_config.model(profile_id)
        prior_outputs = self.store.list_events(
            event_type=EventType.ORACLE_OUTPUT,
            causation_id=request_event.id,
        )
        if prior_outputs:
            return self._postprocess_oracle_output(prior_outputs[0])
        from oracle_lab.archive import RawResponseArchive
        from oracle_lab.providers import OracleGenerateRequest, create_provider
        from oracle_lab.session import SessionContextBuilder

        provider_config = self.runtime_config.provider_for(profile)
        provider = (
            self.provider_factory(profile)
            if self.provider_factory is not None
            else create_provider(provider_config, self.runtime_config.models)
        )
        events = self.store.list_events(session_id=request_event.session_id)
        context = SessionContextBuilder().build(
            events,
            session_id=str(request_event.session_id),
            branch_id=str(request_event.branch_id),
            tip_event_id=request_event.id,
            system_prompt=profile.system_prompt,
            system_prompt_source_event_id=self._system_prompt_source_event_id(
                branch_id=str(request_event.branch_id),
                system_prompt=profile.system_prompt,
            ),
            include_reasoning=profile.include_reasoning_in_next_turn,
            max_messages=profile.max_context_messages,
        )
        request = OracleGenerateRequest(
            profile_id,
            context.provider_messages(),
            temperature=(
                request_event.payload.get("temperature")
                if request_event.payload.get("temperature") is not None
                else profile.temperature
            ),
            top_p=(
                request_event.payload.get("top_p")
                if request_event.payload.get("top_p") is not None
                else profile.top_p
            ),
            max_tokens=(
                request_event.payload.get("max_tokens")
                if request_event.payload.get("max_tokens") is not None
                else profile.max_tokens
            ),
            provider_pin=profile.pin_provider,
            seed=request_event.payload.get("seed"),
            metadata={
                "request_event_id": request_event.id,
                "sample_group_id": request_event.payload.get("sample_group_id"),
                "sample_ordinal": request_event.payload.get("sample_ordinal"),
                "requested_model_slug": profile.slug,
                "requested_provider_id": profile.provider,
                "provider_routing": {
                    "pin_provider": profile.pin_provider,
                    "allow_fallback": profile.allow_fallback,
                },
                "model_family": profile.model_family,
                "checkpoint": profile.checkpoint,
                "runtime": profile.runtime,
                "quantization": profile.quantization,
            },
        )
        try:
            from oracle_lab.oracle_worker import OracleWorker
        except ImportError as error:  # pragma: no cover - integration guard
            raise ServiceError("OracleWorker is unavailable") from error
        worker = OracleWorker(
            provider,
            RawResponseArchive(self.archive_root / "raw"),
            self.store,
            max_retries=provider_config.max_retries,
            retry_base_seconds=provider_config.retry_base_seconds,
        )
        with self.observability.operation(
            "oracle.provider.generate",
            event=request_event,
            fields={"provider": profile.provider, "model_profile_id": profile_id},
        ):
            output = asyncio.run(worker.run(request_event, request, context=context))
        if worker.last_run is not None:
            for recorded in (
                worker.last_run.context_event,
                worker.last_run.truncation_event,
                worker.last_run.fallback_event,
                worker.last_run.output_event,
                worker.last_run.usage_event,
            ):
                if recorded is not None:
                    self.observability.log_event(
                        recorded,
                        fields={"operation": "oracle.provider.generate"},
                    )
        return self._postprocess_oracle_output(output)

    def _postprocess_oracle_output(self, output: Event) -> Event:
        """Resume idempotent local work after a durably committed model call."""
        if is_synthetic_lineage(output, self.store.get):
            # Synthetic oracle-like text is useful for isolated tests, but is
            # never admitted to rendering/search caches or derived research
            # projections as genuine oracle material.
            return output
        from oracle_lab.rendering import MarkdownArtifact, MarkdownArtifactStore

        raw_text = output.payload.get("content")
        if isinstance(raw_text, str):
            MarkdownArtifactStore(self.rendering_root).save(
                output.id,
                MarkdownArtifact.capture(raw_text),
            )
        if self.host_worker_router is None:
            self._run_host_analysis(output)
        else:
            self._enqueue_host_analysis_jobs(output)
        return output

    def _enqueue_host_analysis_jobs(self, source: Event) -> None:
        """Persist deterministic analysis tasks for the configured worker router."""

        with (
            self.observability.operation("host.analysis.enqueue", event=source),
            self.store.transaction(),
        ):
            self._dispatcher().dispatch(source)
            if self.host_worker_router is None:
                return
            from oracle_lab.agent_adapters import DirectAPIHost

            for job in self._job_queue().list_jobs():
                if (
                    job.source_event_id != source.id
                    or job.kind not in self.host_worker_router.supported_task_kinds
                ):
                    continue
                routed_task_type, worker = self.host_worker_router.route(job.kind)
                if not isinstance(worker, DirectAPIHost):
                    continue
                self._ensure_worker_task_event(
                    job=job,
                    source=source,
                    goal=self._host_worker_goal(job.kind, job.payload),
                    routed_task_type=routed_task_type,
                    worker=worker,
                )

    @staticmethod
    def _host_worker_goal(task_kind: str, payload: Mapping[str, Any]) -> str:
        goals = {
            "extract_claims": "Extract verifiable claims only.",
            "detect_new_mechanisms": (
                "Detect only explicit mechanism/layer/interface/field markers."
            ),
            "extract_entities": "Extract named entities only.",
            "check_numeric_consistency": "Detect numeric inconsistencies only.",
            "detect_attractors": "Classify format and content attractors only.",
            "detect_motifs": "Detect recurring motifs only.",
            "detect_recurrence": "Detect repeated lines or structures only.",
            "detect_tool_intent": "Detect proposed tool intent without executing it.",
            "compare_claim_history": "Compare this claim with cited session history only.",
            "propose_calculation": "Propose a calculation probe without executing it.",
            "novelty_analysis": "Assess novelty against cited prior events only.",
        }
        return goals.get(task_kind, str(payload.get("goal", task_kind)))

    def _record_host_call(
        self,
        source: Event,
        *,
        result: Any | None,
        latency_ms: float | None,
        job: Any,
        routed_task_type: str,
        worker: Any,
        status: str,
    ) -> None:
        from oracle_lab.usage import UsageService

        replay_event_id = job.payload.get("replay_event_id")
        usage_source_id = replay_event_id if isinstance(replay_event_id, str) else source.id
        replay_metadata = {
            key: job.payload[key]
            for key in ("replay_event_id", "replay_mode", "host_profile_label")
            if key in job.payload
        }
        usage = UsageService(self.store).record_host_call(
            request_event_id=usage_source_id,
            result=result,
            latency_ms=latency_ms,
            provider_id=(
                getattr(result, "requested_provider_id", None)
                or getattr(getattr(worker, "profile", None), "host_provider_id", None)
            ),
            model_id=(
                getattr(result, "returned_model", None)
                or getattr(result, "requested_model", None)
                or getattr(getattr(worker, "profile", None), "model", None)
            ),
            metadata={
                "job_id": job.id,
                "job_kind": job.kind,
                "routed_task_type": routed_task_type,
                "worker_type": type(worker).__name__,
                "status": status,
                **replay_metadata,
            },
        )
        usage_event = self.store.require(usage.event_id)
        self.observability.log_event(
            usage_event,
            fields={
                "operation": "host.worker.call",
                "job_id": job.id,
                "job_kind": job.kind,
                "status": status,
            },
        )

    def _ensure_worker_task_event(
        self,
        *,
        job: Any,
        source: Event,
        goal: str,
        routed_task_type: str,
        worker: Any,
    ) -> Event:
        raw_task_id = job.payload.get("task_event_id")
        if isinstance(raw_task_id, str):
            task_event = self.store.require(raw_task_id)
            if task_event.type is not EventType.WORKER_TASK_REQUESTED:
                raise ServiceError("worker job task_event_id has the wrong event type")
            return task_event
        existing = next(
            (
                event
                for event in self.store.list_events(event_type=EventType.WORKER_TASK_REQUESTED)
                if event.payload.get("job_id") == job.id
            ),
            None,
        )
        if existing is not None:
            return existing
        profile = getattr(worker, "profile", None)
        profile_snapshot = (
            profile.redacted_snapshot()
            if profile is not None and callable(getattr(profile, "redacted_snapshot", None))
            else None
        )
        routing_snapshot = (
            self._worker_routing_snapshot(profile=profile) if profile is not None else None
        )
        automation = self._worker_automation_fields(
            source,
            signature_seed={
                "task_kind": job.kind,
                "source_event_id": source.id,
                "goal": goal,
                "worker_profile_id": getattr(profile, "id", None),
            },
        )
        return self.store.append(
            Event.new(
                EventType.WORKER_TASK_REQUESTED,
                actor=Actor(kind=ActorKind.HOST, id="worker-orchestrator"),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=source.id,
                causation_id=source.id,
                correlation_id=source.correlation_id or new_id("corr"),
                payload={
                    "job_id": job.id,
                    "task_kind": job.kind,
                    "routed_task_type": routed_task_type,
                    "source_event_id": source.id,
                    "source_event_ids": [source.id],
                    "goal": goal,
                    "worker_profile_id": getattr(profile, "id", None),
                    "worker_adapter": getattr(worker, "name", type(worker).__name__),
                    "worker_execution_profile": profile_snapshot,
                    "worker_routing": routing_snapshot,
                    "artifact_origin": "host_generated_task",
                    **automation,
                },
            )
        )

    @staticmethod
    def _worker_archive_manifest(record: Any) -> dict[str, Any]:
        return {
            artifact.name: {
                "path": str(artifact.path),
                "sha256": artifact.sha256,
                "size_bytes": artifact.size_bytes,
            }
            for artifact in record.artifacts
        }

    def _archive_agent_result(
        self,
        *,
        run_id: str,
        task_event: Event,
        task: Any,
        worker: Any,
        result: Any | None,
        started_at: dt.datetime,
        finished_at: dt.datetime,
        status: str,
        failure: Exception | None = None,
    ) -> Any:
        from oracle_lab.worker_archive import WorkerRunArchive, WorkerRunMetadata

        profile = getattr(worker, "profile", None)
        prompt = task.render() if result is None else result.prompt
        task_document: dict[str, Any] = {
            "task_event_id": task_event.id,
            "job_id": task_event.payload.get("job_id"),
            "task_kind": task.task_kind,
            "source_event_id": task.source_event.id,
            "goal": task.goal,
            "repository_path": task.repository,
            "requested_base_commit": task.base_commit,
            "validation_commands": list(task.validation_commands),
            "validation_sandbox": thaw_json(task_event.payload).get("validation_sandbox"),
            "worker_profile_id": getattr(profile, "id", None),
            "worker_execution_profile": thaw_json(task_event.payload).get(
                "worker_execution_profile"
            ),
            "worker_routing": thaw_json(task_event.payload).get("worker_routing"),
            "automation": {
                key: thaw_json(task_event.payload).get(key)
                for key in (
                    "automation_depth",
                    "automation_budget_remaining",
                    "automation_loop_detector",
                    "loop_signature",
                )
            },
        }
        if result is not None:
            task_document["execution_capture"] = {
                "executable_path": result.executable_path,
                "executable_version_status": result.executable_version_status,
                "base_commit": result.base_commit,
                "workspace_head": result.workspace_head,
                "changed_paths": list(result.changed_paths),
                "changed_modes": dict(result.changed_modes),
                "precondition_sha256": dict(result.precondition_sha256),
                "source_status_before_sha256": result.source_status_before_sha256,
                "source_status_after_sha256": result.source_status_after_sha256,
                "source_head_before": result.source_head_before,
                "source_head_after": result.source_head_after,
                "source_index_before_sha256": result.source_index_before_sha256,
                "source_index_after_sha256": result.source_index_after_sha256,
                "source_snapshot_before_sha256": result.source_snapshot_before_sha256,
                "source_snapshot_after_sha256": result.source_snapshot_after_sha256,
                "source_git_control_before_sha256": result.source_git_control_before_sha256,
                "source_git_control_after_sha256": result.source_git_control_after_sha256,
                "source_worktree_unchanged": result.source_worktree_unchanged,
                "worker_committed": result.worker_committed,
                "worker_git_control_tampered": result.worker_git_control_tampered,
                "isolation_attestation": (
                    None
                    if result.isolation_attestation is None
                    else thaw_json(result.isolation_attestation)
                ),
                "isolation_sandbox_id": result.isolation_sandbox_id,
                "isolation_cleanup_confirmed": result.isolation_cleanup_confirmed,
                "workspace_export_sha256": result.workspace_export_sha256,
                "workspace_export_bytes": result.workspace_export_bytes,
                "workspace_export_entries": result.workspace_export_entries,
            }
        if failure is not None:
            task_document["host_observed_failure"] = {
                "origin": "host_generated",
                "error_type": type(failure).__name__,
                "message": str(failure),
            }
        command: Sequence[str] = () if result is None else result.command
        if result is None and callable(getattr(worker, "command_builder", None)):
            requested = list(worker.command_builder(prompt))
            if requested:
                requested[0] = str(getattr(worker, "executable", requested[0]))
                command = tuple(requested)
                task_document["command_capture_status"] = "requested_not_confirmed_started"
        worker_environment = getattr(worker, "environment", None)
        environment_names = (
            tuple(sorted(str(name) for name in worker_environment))
            if isinstance(worker_environment, Mapping)
            else ()
        )
        return WorkerRunArchive(self.archive_root / "workers").write(
            run_id=run_id,
            task=task_document,
            prompt=prompt,
            command=command,
            stdout=b"" if result is None else result.stdout_bytes,
            stderr=(
                (
                    f"{type(failure).__name__}: {failure}".encode("utf-8", "replace")
                    if failure is not None
                    else b""
                )
                if result is None
                else result.stderr_bytes
            ),
            patch=b"" if result is None else result.patch_bytes,
            run_metadata=WorkerRunMetadata(
                adapter=getattr(worker, "name", type(worker).__name__),
                adapter_version=(None if result is None else result.executable_version),
                model=getattr(profile, "model", None),
                base_commit=(task.base_commit if result is None else result.base_commit),
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                exit_code=None if result is None else result.exit_code,
                timed_out=None if result is None else result.timed_out,
                output_limited=None if result is None else result.output_limited,
                environment_names=environment_names,
            ),
            # A stable timestamp lets a post-archive retry discover and verify
            # the same orphan directory instead of inventing another identity.
            archived_at=started_at,
        )

    def _archive_direct_host_result(
        self,
        *,
        run_id: str,
        task_event: Event,
        task: Any,
        routed_task_type: str,
        worker: Any,
        result: Any | None,
        started_at: dt.datetime,
        finished_at: dt.datetime,
        status: str,
        failure: Exception | None = None,
    ) -> Any:
        """Archive one Host-model call without reclassifying it as Oracle output."""

        from oracle_lab.host_provider import (
            HOST_PROMPT_CONTRACT_VERSION,
            HostProviderError,
        )
        from oracle_lab.worker_archive import WorkerRunArchive, WorkerRunMetadata

        profile = getattr(worker, "profile", None)
        if profile is None:
            raise ServiceError("Direct Host archive requires an execution profile")
        prompt = (
            worker.render_prompt(routed_task_type, {"prompt": task.render()})
            if result is None
            else result.prompt
        )
        response_document: dict[str, Any] = {
            "output": None,
            "requested_provider_id": profile.host_provider_id,
            "requested_model": profile.model,
            "actual_provider": None,
            "returned_model": None,
            "routing_settings": {},
            "sampling_settings": {},
            "api_response_metadata": {},
            "usage": {},
            "elapsed_ms": None,
        }
        raw_response = b""
        if result is not None:
            response_document = {
                "output": thaw_json(result.output),
                "requested_provider_id": result.requested_provider_id,
                "requested_model": result.requested_model,
                "actual_provider": result.actual_provider,
                "returned_model": result.returned_model,
                "routing_settings": thaw_json(result.routing_settings),
                "sampling_settings": thaw_json(result.sampling_settings),
                "api_response_metadata": thaw_json(result.api_response_metadata),
                "usage": thaw_json(result.usage),
                "elapsed_ms": result.elapsed_ms,
            }
            raw_response = result.raw_response
        elif isinstance(failure, HostProviderError):
            raw_response = failure.raw_response
            response_document = {
                "output": None,
                "requested_provider_id": (
                    failure.requested_provider_id or profile.host_provider_id
                ),
                "requested_model": failure.requested_model or profile.model,
                "actual_provider": failure.actual_provider,
                "returned_model": failure.returned_model,
                "routing_settings": thaw_json(failure.routing_settings),
                "sampling_settings": thaw_json(failure.sampling_settings),
                "api_response_metadata": thaw_json(failure.api_response_metadata),
                "usage": thaw_json(failure.usage),
                "elapsed_ms": failure.elapsed_ms,
            }
        task_document: dict[str, Any] = {
            "task_event_id": task_event.id,
            "job_id": task_event.payload.get("job_id"),
            "task_kind": task_event.payload.get("task_kind"),
            "routed_task_type": routed_task_type,
            "source_event_id": task.source_event.id,
            "goal": task.goal,
            "worker_profile_id": profile.id,
            "worker_execution_profile": thaw_json(task_event.payload).get(
                "worker_execution_profile"
            ),
            "worker_routing": thaw_json(task_event.payload).get("worker_routing"),
            "host_prompt_contract": HOST_PROMPT_CONTRACT_VERSION,
            "idempotency_key": f"host-direct:{task_event.id}",
            "direct_host_response": response_document,
            "automation": {
                key: thaw_json(task_event.payload).get(key)
                for key in (
                    "automation_depth",
                    "automation_budget_remaining",
                    "automation_loop_detector",
                    "loop_signature",
                )
            },
        }
        if failure is not None:
            task_document["host_observed_failure"] = {
                "origin": "host_generated",
                "error_type": type(failure).__name__,
                "message": str(failure),
            }
        call = getattr(worker, "call", None)
        adapter_version = getattr(call, "adapter_version", None)
        return WorkerRunArchive(self.archive_root / "workers").write(
            run_id=run_id,
            task=task_document,
            prompt=prompt,
            command=(
                "direct-api",
                str(profile.host_provider_id or "unknown"),
                str(profile.model or "unknown"),
            ),
            stdout=raw_response,
            stderr=(
                b""
                if failure is None
                else f"{type(failure).__name__}: {failure}".encode("utf-8", "replace")
            ),
            patch=b"",
            run_metadata=WorkerRunMetadata(
                adapter="direct",
                adapter_version=(str(adapter_version) if adapter_version else None),
                model=profile.model,
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                exit_code=0 if result is not None else None,
                timed_out=False if result is not None else None,
                output_limited=(
                    False
                    if result is not None
                    else failure.output_limited
                    if isinstance(failure, HostProviderError)
                    else False
                ),
                environment_names=(),
                artifact_origin="host_generated",
            ),
            archived_at=started_at,
        )

    def _existing_worker_terminal(self, run_id: str) -> Event | None:
        return next(
            (
                event
                for event in self.store.list_events(
                    event_type=[
                        EventType.WORKER_RUN_COMPLETED,
                        EventType.WORKER_RUN_FAILED,
                    ]
                )
                if event.payload.get("run_id") == run_id
            ),
            None,
        )

    def _worker_failure_is_repeated(self, task_event_id: str, signature: str) -> bool:
        return any(
            event.payload.get("task_event_id") == task_event_id
            and event.payload.get("failure_signature") == signature
            for event in self.store.list_events(event_type=EventType.WORKER_RUN_FAILED)
        )

    def _propose_repository_patch(
        self,
        *,
        terminal: Event,
        task_event: Event,
        source: Event,
        worker: Any,
    ) -> tuple[Event, ...]:
        existing = [
            event
            for event in self.store.list_events(
                event_type=[
                    EventType.WORKER_PATCH_PROPOSED,
                    EventType.WORKER_PATCH_SECURITY_REJECTED,
                ]
            )
            if event.payload.get("worker_run_id") == terminal.payload.get("run_id")
        ]
        if existing:
            return tuple(existing)
        candidate = terminal.payload.get("candidate_patch")
        if not isinstance(candidate, Mapping):
            raise ServiceError("completed repository run has no candidate patch capture")
        from oracle_lab.patches import (
            CandidatePatch,
            CandidatePatchError,
            PatchApplicationError,
            preflight_candidate_patch,
        )

        reasons: list[str] = []
        validated = None
        patch_path = Path(str(candidate.get("patch_archive_path", "")))
        if patch_path.is_symlink() or not patch_path.is_file():
            reasons.append("patch_archive_unavailable")
        else:
            try:
                validated = CandidatePatch.from_capture(
                    worker_run_id=str(terminal.payload["run_id"]),
                    source_event_ids=(source.id,),
                    base_commit=str(candidate.get("base_commit", "")),
                    workspace_head=str(candidate.get("workspace_head", "")),
                    diff_bytes=patch_path.read_bytes(),
                    patch_sha256=str(candidate.get("patch_sha256", "")),
                    changed_paths=tuple(candidate.get("changed_paths", ())),
                    precondition_sha256=dict(candidate.get("precondition_sha256", {})),
                    changed_modes=dict(candidate.get("changed_modes", {})),
                )
            except (
                CandidatePatchError,
                PatchApplicationError,
                OSError,
                TypeError,
                ValueError,
            ) as error:
                reasons.append(f"candidate_patch_rejected:{error}")
        if candidate.get("source_worktree_unchanged") is not True:
            reasons.append("source_worktree_changed")
        if candidate.get("repository_path") != task_event.payload.get(
            "repository_path"
        ) or candidate.get("base_commit") != task_event.payload.get("base_commit"):
            reasons.append("candidate_task_identity_mismatch")
        if validated is not None and not reasons:
            try:
                preflight_candidate_patch(
                    validated,
                    Path(str(task_event.payload["repository_path"])),
                )
            except (
                CandidatePatchError,
                PatchApplicationError,
                OSError,
                TypeError,
                ValueError,
            ) as error:
                reasons.append(f"target_precondition_failed:{error}")
        if reasons:
            rejected = self.store.append(
                Event.new(
                    EventType.WORKER_PATCH_SECURITY_REJECTED,
                    actor=Actor(kind=ActorKind.SYSTEM, id="candidate-patch-inspector"),
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    parent_event_id=terminal.id,
                    causation_id=task_event.id,
                    correlation_id=task_event.correlation_id,
                    payload={
                        "worker_run_id": terminal.payload["run_id"],
                        "task_event_id": task_event.id,
                        "source_event_ids": [source.id],
                        "reasons": sorted(set(reasons)),
                        "artifact_origin": "worker_generated",
                    },
                )
            )
            return (rejected,)
        assert validated is not None
        proposed = self.store.append(
            Event.new(
                EventType.WORKER_PATCH_PROPOSED,
                actor=Actor(kind=ActorKind.WORKER, id=getattr(worker, "name", "worker")),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=terminal.id,
                causation_id=task_event.id,
                correlation_id=task_event.correlation_id,
                payload={
                    "worker_run_id": terminal.payload["run_id"],
                    "task_event_id": task_event.id,
                    "repository_path": candidate["repository_path"],
                    "base_commit": candidate["base_commit"],
                    "patch_archive_path": candidate["patch_archive_path"],
                    "patch_sha256": candidate["patch_sha256"],
                    "patch_size_bytes": candidate["patch_size_bytes"],
                    "workspace_head": validated.workspace_head,
                    "changed_paths": list(validated.changed_paths),
                    "changed_modes": dict(validated.changed_modes),
                    "precondition_sha256": dict(validated.precondition_sha256),
                    "precondition_modes": dict(validated.precondition_modes),
                    "source_status_before_sha256": candidate.get("source_status_before_sha256"),
                    "source_status_after_sha256": candidate.get("source_status_after_sha256"),
                    "source_head_before": candidate.get("source_head_before"),
                    "source_head_after": candidate.get("source_head_after"),
                    "source_index_before_sha256": candidate.get("source_index_before_sha256"),
                    "source_index_after_sha256": candidate.get("source_index_after_sha256"),
                    "source_snapshot_before_sha256": candidate.get("source_snapshot_before_sha256"),
                    "source_snapshot_after_sha256": candidate.get("source_snapshot_after_sha256"),
                    "source_event_ids": [source.id],
                    "worker_identity": terminal.payload.get("worker_identity"),
                    "artifact_origin": "worker_generated",
                    "material_origin": None,
                },
                metadata={
                    "schema_version": 1,
                    "emitter": "trusted_candidate_patch_harvester",
                },
            )
        )
        return (proposed,)

    @staticmethod
    def _matches_prepared_worker_event(existing: Event, prepared: Event) -> bool:
        """Compare a legacy orphan event to its archived structured proposal."""

        return (
            existing.type is prepared.type
            and existing.actor == prepared.actor
            and existing.session_id == prepared.session_id
            and existing.branch_id == prepared.branch_id
            and existing.parent_event_id == prepared.parent_event_id
            and existing.causation_id == prepared.causation_id
            and existing.correlation_id == prepared.correlation_id
            and thaw_json(existing.payload) == thaw_json(prepared.payload)
            and thaw_json(existing.metadata) == thaw_json(prepared.metadata)
        )

    @staticmethod
    def _expected_worker_argv(worker: Any, prompt: str, *, started: bool) -> tuple[str, ...]:
        builder = getattr(worker, "command_builder", None)
        if not callable(builder):
            raise ServiceError("coding worker has no deterministic command builder")
        command = list(builder(prompt))
        if not command or any(not isinstance(argument, str) for argument in command):
            raise ServiceError("coding worker command builder returned invalid arguments")
        executable = str(getattr(worker, "executable", command[0]))
        if started:
            environment = getattr(worker, "environment", None)
            path = environment.get("PATH") if isinstance(environment, Mapping) else None
            resolved = shutil.which(executable, path=path)
            if resolved is None:
                raise ServiceError("archived coding worker executable is no longer resolvable")
            executable = resolved
        command[0] = executable
        return tuple(command)

    def _resume_archived_worker_run(
        self,
        *,
        task_event: Event,
        task: Any,
        source: Event,
        worker: Any,
    ) -> tuple[Event, ...] | None:
        """Finish a complete post-crash archive without invoking the agent again."""

        from oracle_lab.worker_archive import WorkerRunArchive

        archive_service = WorkerRunArchive(self.archive_root / "workers")
        starts = sorted(
            (
                event
                for event in self.store.list_events(event_type=EventType.WORKER_RUN_STARTED)
                if event.payload.get("task_event_id") == task_event.id
                and self._existing_worker_terminal(str(event.payload.get("run_id"))) is None
            ),
            key=lambda event: (event.created_at, event.id),
        )

        def known(document: Mapping[str, Any], section: str, key: str) -> Any:
            group = document.get(section)
            item = group.get(key) if isinstance(group, Mapping) else None
            return (
                item.get("value")
                if isinstance(item, Mapping) and item.get("status") == "known"
                else None
            )

        for started in starts:
            run_id = str(started.payload["run_id"])
            directory = archive_service.directory_for(run_id, started.created_at)
            if directory.is_symlink():
                raise ServiceError("worker orphan archive directory is a symlink")
            if not directory.exists():
                continue
            snapshot = archive_service.load(
                run_id=run_id,
                archived_at=started.created_at,
            )
            task_payload = thaw_json(task_event.payload)
            expected_automation = {
                key: task_payload.get(key)
                for key in (
                    "automation_depth",
                    "automation_budget_remaining",
                    "automation_loop_detector",
                    "loop_signature",
                )
            }
            if (
                snapshot.task.get("task_event_id") != task_event.id
                or snapshot.task.get("job_id") != task_event.payload.get("job_id")
                or snapshot.task.get("source_event_id") != source.id
                or snapshot.task.get("task_kind") != task.task_kind
                or snapshot.task.get("goal") != task.goal
                or snapshot.task.get("repository_path") != task.repository
                or snapshot.task.get("requested_base_commit") != task.base_commit
                or tuple(snapshot.task.get("validation_commands", ()))
                != tuple(task.validation_commands)
                or snapshot.task.get("validation_sandbox") != task_payload.get("validation_sandbox")
                or snapshot.task.get("worker_profile_id")
                != task_event.payload.get("worker_profile_id")
                or snapshot.task.get("worker_execution_profile")
                != task_payload.get("worker_execution_profile")
                or snapshot.task.get("worker_routing") != task_payload.get("worker_routing")
                or snapshot.task.get("automation") != expected_automation
                or started.payload.get("job_id") != task_event.payload.get("job_id")
            ):
                raise ServiceError("worker orphan archive belongs to another task")
            manifest = self._worker_archive_manifest(snapshot.record)
            status = known(snapshot.metadata, "execution", "status")
            adapter_id = known(snapshot.metadata, "identity", "adapter")
            expected_prompt = task.render()
            expected_argv = self._expected_worker_argv(
                worker,
                expected_prompt,
                started=snapshot.task.get("command_capture_status")
                != "requested_not_confirmed_started",
            )
            if (
                adapter_id != started.payload.get("adapter_id")
                or started.payload.get("adapter_id") != task_event.payload.get("worker_adapter")
                or adapter_id != getattr(worker, "name", None)
                or known(snapshot.metadata, "identity", "base_commit") != task.base_commit
                or snapshot.prompt != expected_prompt
                or snapshot.command != expected_argv
            ):
                raise ServiceError("worker orphan prompt, argv, or adapter identity does not match")
            actor_id = (
                str(adapter_id)
                if isinstance(adapter_id, str)
                else getattr(worker, "name", "worker")
            )
            if status != "completed":
                self.store.append(
                    Event.new(
                        EventType.WORKER_RUN_FAILED,
                        actor=Actor(kind=ActorKind.WORKER, id=actor_id),
                        session_id=source.session_id,
                        branch_id=source.branch_id,
                        parent_event_id=started.id,
                        causation_id=task_event.id,
                        correlation_id=task_event.correlation_id,
                        payload={
                            "run_id": run_id,
                            "task_event_id": task_event.id,
                            "job_id": task_event.payload["job_id"],
                            "reasons": ["recovered_archived_failure"],
                            "archive_path": str(snapshot.record.directory),
                            "archive_manifest": manifest,
                        },
                        metadata={
                            "schema_version": 1,
                            "artifact_origin": "worker_generated",
                            "envelope_emitter": "trusted_host_orchestrator",
                        },
                    )
                )
                continue
            if (
                known(snapshot.metadata, "execution", "exit_code") != 0
                or known(snapshot.metadata, "execution", "timed_out") is not False
                or known(snapshot.metadata, "execution", "output_limited") is not False
            ):
                raise ServiceError("completed worker orphan has contradictory execution metadata")
            capture = snapshot.task.get("execution_capture")
            if not isinstance(capture, Mapping):
                raise ServiceError("completed worker orphan lacks execution capture")
            existing_produced = tuple(
                event
                for event in self.store.list_events()
                if event.payload.get("worker_run_id") == run_id
            )
            produced = existing_produced
            pending_produced: tuple[Event, ...] = ()
            candidate_patch = None
            if task.task_kind == "repository_edit":
                candidate_patch = {
                    "repository_path": snapshot.task.get("repository_path"),
                    "base_commit": capture.get("base_commit"),
                    "workspace_head": capture.get("workspace_head"),
                    "patch_archive_path": str(snapshot.record.patch.path),
                    "patch_sha256": snapshot.record.patch.sha256,
                    "patch_size_bytes": snapshot.record.patch.size_bytes,
                    "changed_paths": capture.get("changed_paths", []),
                    "changed_modes": capture.get("changed_modes", {}),
                    "precondition_sha256": capture.get("precondition_sha256", {}),
                    "source_status_before_sha256": capture.get("source_status_before_sha256"),
                    "source_status_after_sha256": capture.get("source_status_after_sha256"),
                    "source_head_before": capture.get("source_head_before"),
                    "source_head_after": capture.get("source_head_after"),
                    "source_index_before_sha256": capture.get("source_index_before_sha256"),
                    "source_index_after_sha256": capture.get("source_index_after_sha256"),
                    "source_snapshot_before_sha256": capture.get("source_snapshot_before_sha256"),
                    "source_snapshot_after_sha256": capture.get("source_snapshot_after_sha256"),
                    "source_worktree_unchanged": capture.get("source_worktree_unchanged"),
                    "worker_committed": capture.get("worker_committed"),
                }
            else:
                from oracle_lab.agent_adapters import (
                    parse_structured_events,
                    prepare_structured_events,
                )

                proposals = parse_structured_events(
                    snapshot.stdout.decode("utf-8", "replace"),
                    expected_source_event_id=source.id,
                )
                prepared = prepare_structured_events(
                    proposals,
                    source=source,
                    store=self.store,
                    actor_kind=ActorKind.WORKER,
                    actor_id=actor_id,
                    worker_run_id=run_id,
                )
                if len(existing_produced) > len(prepared) or any(
                    not self._matches_prepared_worker_event(existing, expected)
                    for existing, expected in zip(existing_produced, prepared, strict=False)
                ):
                    raise ServiceError(
                        "worker orphan structured events are not a prefix of archived stdout"
                    )
                pending_produced = tuple(prepared[len(existing_produced) :])
                produced = (*existing_produced, *pending_produced)
            terminal_event = Event.new(
                EventType.WORKER_RUN_COMPLETED,
                actor=Actor(kind=ActorKind.WORKER, id=actor_id),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=started.id,
                causation_id=task_event.id,
                correlation_id=task_event.correlation_id,
                payload={
                    "run_id": run_id,
                    "task_event_id": task_event.id,
                    "job_id": task_event.payload["job_id"],
                    "exit_code": 0,
                    "archive_path": str(snapshot.record.directory),
                    "archive_manifest": manifest,
                    "produced_event_ids": [event.id for event in produced],
                    "candidate_patch": candidate_patch,
                    "worker_identity": {
                        "adapter": adapter_id,
                        "executable_version": known(
                            snapshot.metadata, "identity", "adapter_version"
                        ),
                        "model": known(snapshot.metadata, "identity", "model"),
                        "execution_profile": snapshot.task.get("worker_execution_profile"),
                        "routing": snapshot.task.get("worker_routing"),
                        "recovered_verified_orphan": True,
                    },
                },
                metadata={
                    "schema_version": 1,
                    "artifact_origin": "worker_generated",
                    "envelope_emitter": "trusted_host_orchestrator",
                },
            )
            if task.task_kind == "repository_edit":
                terminal = self.store.append(terminal_event)
            else:
                appended = self.store.append_many((*pending_produced, terminal_event))
                produced = (*existing_produced, *appended[:-1])
                terminal = appended[-1]
            if task.task_kind == "repository_edit":
                return self._propose_repository_patch(
                    terminal=terminal,
                    task_event=task_event,
                    source=source,
                    worker=worker,
                )
            return produced
        return None

    def _execute_coding_worker(
        self,
        *,
        job: Any,
        source: Event,
        task: Any,
        routed_task_type: str,
        worker: Any,
    ) -> tuple[Event, ...]:
        if task.task_kind == "repository_edit":
            if not isinstance(task.repository, str) or not task.repository:
                raise ServiceError("repository-edit task has no repository identity")
            self._assert_worker_storage_isolated(Path(task.repository), worker=worker)
        run_id = f"{job.id}-a{job.attempts}"
        task_event = self._ensure_worker_task_event(
            job=job,
            source=source,
            goal=task.goal,
            routed_task_type=routed_task_type,
            worker=worker,
        )
        if task.task_kind == "repository_edit" or "worker_execution_profile" in job.payload:
            self._assert_frozen_worker_execution(
                job=job,
                task_event=task_event,
                routed_task_type=routed_task_type,
                worker=worker,
            )
        if task_event.metadata.get("bundle_import_authority") == "historical_only":
            raise ServiceError(
                "imported worker task is historical evidence and cannot be executed locally"
            )
        resumed = self._resume_archived_worker_run(
            task_event=task_event,
            task=task,
            source=source,
            worker=worker,
        )
        if resumed is not None:
            return resumed
        completed_prior = [
            event
            for event in self.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)
            if event.payload.get("task_event_id") == task_event.id
        ]
        if completed_prior:
            terminal = completed_prior[-1]
            if task.task_kind == "repository_edit":
                return self._propose_repository_patch(
                    terminal=terminal,
                    task_event=task_event,
                    source=source,
                    worker=worker,
                )
            return tuple(
                self.store.require(str(event_id))
                for event_id in terminal.payload.get("produced_event_ids", ())
            )
        terminal = self._existing_worker_terminal(run_id)
        if terminal is not None:
            if terminal.type is EventType.WORKER_RUN_FAILED:
                raise ServiceError(f"worker run already failed: {run_id}")
            if task.task_kind == "repository_edit":
                return self._propose_repository_patch(
                    terminal=terminal,
                    task_event=task_event,
                    source=source,
                    worker=worker,
                )
            return tuple(
                self.store.require(str(event_id))
                for event_id in terminal.payload.get("produced_event_ids", ())
            )
        if self._job_queue().is_archive_recovery_lease(job.id):
            raise NonRetryableWorkerError(
                "bounded worker archive recovery found no recoverable terminal state"
            )
        started_event = next(
            (
                event
                for event in self.store.list_events(event_type=EventType.WORKER_RUN_STARTED)
                if event.payload.get("run_id") == run_id
            ),
            None,
        )
        if started_event is None:
            started_event = self.store.append(
                Event.new(
                    EventType.WORKER_RUN_STARTED,
                    actor=Actor(kind=ActorKind.WORKER, id=getattr(worker, "name", "worker")),
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    parent_event_id=task_event.id,
                    causation_id=task_event.id,
                    correlation_id=task_event.correlation_id,
                    payload={
                        "run_id": run_id,
                        "task_event_id": task_event.id,
                        "job_id": job.id,
                        "attempt": job.attempts,
                        "adapter_id": getattr(worker, "name", type(worker).__name__),
                    },
                    metadata={
                        "schema_version": 1,
                        "artifact_origin": "worker_generated",
                        "envelope_emitter": "trusted_host_orchestrator",
                    },
                )
            )
        started_at = started_event.created_at
        try:
            result = worker.run(task)
        except Exception as error:
            finished_at = dt.datetime.now(dt.UTC)
            failure_signature = sha256_json(
                {
                    "kind": "adapter_error",
                    "adapter": getattr(worker, "name", type(worker).__name__),
                    "error_type": type(error).__name__,
                    "message": str(error),
                }
            )
            repeated = self._worker_failure_is_repeated(task_event.id, failure_signature)
            archive = self._archive_agent_result(
                run_id=run_id,
                task_event=task_event,
                task=task,
                worker=worker,
                result=None,
                started_at=started_at,
                finished_at=finished_at,
                status="adapter_error",
                failure=error,
            )
            failed = self.store.append(
                Event.new(
                    EventType.WORKER_RUN_FAILED,
                    actor=Actor(kind=ActorKind.WORKER, id=getattr(worker, "name", "worker")),
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    parent_event_id=started_event.id,
                    causation_id=task_event.id,
                    correlation_id=task_event.correlation_id,
                    payload={
                        "run_id": run_id,
                        "task_event_id": task_event.id,
                        "job_id": job.id,
                        "failure_type": type(error).__name__,
                        "failure_signature": failure_signature,
                        "repeated_equivalent_failure": repeated,
                        "archive_path": str(archive.directory),
                        "archive_manifest": self._worker_archive_manifest(archive),
                    },
                    metadata={
                        "schema_version": 1,
                        "artifact_origin": "worker_generated",
                        "envelope_emitter": "trusted_host_orchestrator",
                    },
                )
            )
            error_type = NonRetryableWorkerError if repeated else ServiceError
            raise error_type(f"worker adapter failed ({failed.id}): {error}") from error

        finished_at = dt.datetime.now(dt.UTC)
        archive = self._archive_agent_result(
            run_id=run_id,
            task_event=task_event,
            task=task,
            worker=worker,
            result=result,
            started_at=started_at,
            finished_at=finished_at,
            status="completed" if result.succeeded else "failed",
        )
        manifest = self._worker_archive_manifest(archive)
        if not result.succeeded:
            reasons = []
            if result.exit_code != 0:
                reasons.append("nonzero_exit")
            if result.timed_out:
                reasons.append("timeout")
            if result.output_limited:
                reasons.append("output_limit")
            if result.source_worktree_unchanged is False:
                reasons.append("source_worktree_changed")
            if result.worker_committed:
                reasons.append("worker_created_commit")
            if result.worker_git_control_tampered:
                reasons.append("worker_git_control_tampered")
            failure_signature = sha256_json(
                {
                    "kind": "worker_result",
                    "adapter": result.adapter,
                    "exit_code": result.exit_code,
                    "timed_out": result.timed_out,
                    "output_limited": result.output_limited,
                    "source_worktree_unchanged": result.source_worktree_unchanged,
                    "worker_committed": result.worker_committed,
                    "worker_git_control_tampered": result.worker_git_control_tampered,
                    "stderr_sha256": sha256_bytes(result.stderr_bytes),
                }
            )
            repeated = self._worker_failure_is_repeated(task_event.id, failure_signature)
            self.store.append(
                Event.new(
                    EventType.WORKER_RUN_FAILED,
                    actor=Actor(kind=ActorKind.WORKER, id=getattr(worker, "name", "worker")),
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    parent_event_id=started_event.id,
                    causation_id=task_event.id,
                    correlation_id=task_event.correlation_id,
                    payload={
                        "run_id": run_id,
                        "task_event_id": task_event.id,
                        "job_id": job.id,
                        "reasons": reasons or ["unknown_failure"],
                        "failure_signature": failure_signature,
                        "repeated_equivalent_failure": repeated,
                        "exit_code": result.exit_code,
                        "timed_out": result.timed_out,
                        "output_limited": result.output_limited,
                        "archive_path": str(archive.directory),
                        "archive_manifest": manifest,
                    },
                    metadata={
                        "schema_version": 1,
                        "artifact_origin": "worker_generated",
                        "envelope_emitter": "trusted_host_orchestrator",
                    },
                )
            )
            error_type = NonRetryableWorkerError if repeated else ServiceError
            raise error_type(f"worker run failed: {', '.join(reasons) or 'unknown'}")

        produced: tuple[Event, ...] = ()
        if task.task_kind != "repository_edit":
            from oracle_lab.agent_adapters import prepare_structured_events

            produced = prepare_structured_events(
                result.events,
                source=source,
                store=self.store,
                actor_kind=ActorKind.WORKER,
                actor_id=getattr(worker, "name", "worker"),
                worker_run_id=run_id,
            )
        candidate_patch = None
        if task.task_kind == "repository_edit":
            patch_artifact = archive.patch
            candidate_patch = {
                "repository_path": task.repository,
                "base_commit": result.base_commit,
                "workspace_head": result.workspace_head,
                "patch_archive_path": str(patch_artifact.path),
                "patch_sha256": patch_artifact.sha256,
                "patch_size_bytes": patch_artifact.size_bytes,
                "changed_paths": list(result.changed_paths),
                "changed_modes": dict(result.changed_modes),
                "precondition_sha256": dict(result.precondition_sha256),
                "source_status_before_sha256": result.source_status_before_sha256,
                "source_status_after_sha256": result.source_status_after_sha256,
                "source_head_before": result.source_head_before,
                "source_head_after": result.source_head_after,
                "source_index_before_sha256": result.source_index_before_sha256,
                "source_index_after_sha256": result.source_index_after_sha256,
                "source_snapshot_before_sha256": result.source_snapshot_before_sha256,
                "source_snapshot_after_sha256": result.source_snapshot_after_sha256,
                "source_git_control_before_sha256": result.source_git_control_before_sha256,
                "source_git_control_after_sha256": result.source_git_control_after_sha256,
                "source_worktree_unchanged": result.source_worktree_unchanged,
                "worker_committed": result.worker_committed,
                "worker_git_control_tampered": result.worker_git_control_tampered,
            }
        terminal_event = Event.new(
            EventType.WORKER_RUN_COMPLETED,
            actor=Actor(kind=ActorKind.WORKER, id=getattr(worker, "name", "worker")),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=started_event.id,
            causation_id=task_event.id,
            correlation_id=task_event.correlation_id,
            payload={
                "run_id": run_id,
                "task_event_id": task_event.id,
                "job_id": job.id,
                "exit_code": result.exit_code,
                "elapsed_ms": result.elapsed_ms,
                "archive_path": str(archive.directory),
                "archive_manifest": manifest,
                "produced_event_ids": [event.id for event in produced],
                "candidate_patch": candidate_patch,
                "worker_identity": {
                    "adapter": result.adapter,
                    "executable_path": result.executable_path,
                    "executable_version": result.executable_version,
                    "executable_version_status": result.executable_version_status,
                    "profile_id": getattr(getattr(worker, "profile", None), "id", None),
                    "model": getattr(getattr(worker, "profile", None), "model", None),
                    "execution_profile": thaw_json(task_event.payload).get(
                        "worker_execution_profile"
                    ),
                    "routing": thaw_json(task_event.payload).get("worker_routing"),
                },
            },
            metadata={
                "schema_version": 1,
                "artifact_origin": "worker_generated",
                "envelope_emitter": "trusted_host_orchestrator",
            },
        )
        if task.task_kind == "repository_edit":
            terminal = self.store.append(terminal_event)
        else:
            appended = self.store.append_many((*produced, terminal_event))
            produced = tuple(appended[:-1])
            terminal = appended[-1]
        if task.task_kind == "repository_edit":
            return self._propose_repository_patch(
                terminal=terminal,
                task_event=task_event,
                source=source,
                worker=worker,
            )
        return produced

    def _execute_direct_host(
        self,
        *,
        job: Any,
        source: Event,
        task: Any,
        routed_task_type: str,
        worker: Any,
        call_payload: Mapping[str, Any],
    ) -> tuple[Event, ...]:
        """Execute or recover one auditable, explicitly non-Oracle Host call."""

        from oracle_lab.agent_adapters import DirectHostResult, prepare_structured_events
        from oracle_lab.host_provider import HOST_PROMPT_CONTRACT_VERSION, HostProviderError
        from oracle_lab.worker_archive import WorkerRunArchive

        task_event = self._ensure_worker_task_event(
            job=job,
            source=source,
            goal=task.goal,
            routed_task_type=routed_task_type,
            worker=worker,
        )
        self._assert_frozen_direct_host_execution(
            job=job,
            source=source,
            task_event=task_event,
            routed_task_type=routed_task_type,
            worker=worker,
        )
        if task_event.metadata.get("bundle_import_authority") == "historical_only":
            raise ServiceError(
                "imported Direct Host task is historical evidence and cannot call a provider"
            )

        completed_prior = [
            event
            for event in self.store.list_events(event_type=EventType.WORKER_RUN_COMPLETED)
            if event.payload.get("task_event_id") == task_event.id
            and event.payload.get("adapter_id") == "direct"
        ]
        if completed_prior:
            return tuple(
                self.store.require(str(event_id))
                for event_id in completed_prior[-1].payload.get("produced_event_ids", ())
            )

        profile = worker.profile
        expected_prompt = worker.render_prompt(
            routed_task_type,
            {"prompt": task.render()},
        )
        expected_task = {
            "task_event_id": task_event.id,
            "job_id": job.id,
            "task_kind": job.kind,
            "routed_task_type": routed_task_type,
            "source_event_id": source.id,
            "goal": task.goal,
            "worker_profile_id": profile.id,
            "worker_execution_profile": thaw_json(task_event.payload).get(
                "worker_execution_profile"
            ),
            "worker_routing": thaw_json(task_event.payload).get("worker_routing"),
            "host_prompt_contract": HOST_PROMPT_CONTRACT_VERSION,
            "idempotency_key": f"host-direct:{task_event.id}",
            "automation": {
                key: thaw_json(task_event.payload).get(key)
                for key in (
                    "automation_depth",
                    "automation_budget_remaining",
                    "automation_loop_detector",
                    "loop_signature",
                )
            },
        }

        def response_result(
            response: Mapping[str, Any],
            *,
            proposals: tuple[Any, ...],
        ) -> DirectHostResult:
            output = response.get("output")
            if not isinstance(output, Mapping):
                raise ServiceError("archived Direct Host response has no structured output")
            return DirectHostResult(
                task_type=routed_task_type,
                output=output,
                elapsed_ms=float(response.get("elapsed_ms") or 0),
                events=proposals,
                prompt=expected_prompt,
                requested_provider_id=(
                    str(response["requested_provider_id"])
                    if response.get("requested_provider_id") is not None
                    else None
                ),
                requested_model=(
                    str(response["requested_model"])
                    if response.get("requested_model") is not None
                    else None
                ),
                actual_provider=(
                    str(response["actual_provider"])
                    if response.get("actual_provider") is not None
                    else None
                ),
                returned_model=(
                    str(response["returned_model"])
                    if response.get("returned_model") is not None
                    else None
                ),
                routing_settings=(
                    response["routing_settings"]
                    if isinstance(response.get("routing_settings"), Mapping)
                    else {}
                ),
                sampling_settings=(
                    response["sampling_settings"]
                    if isinstance(response.get("sampling_settings"), Mapping)
                    else {}
                ),
                api_response_metadata=(
                    response["api_response_metadata"]
                    if isinstance(response.get("api_response_metadata"), Mapping)
                    else {}
                ),
                usage=response["usage"] if isinstance(response.get("usage"), Mapping) else {},
            )

        def terminal_event(
            *,
            started: Event,
            archive: Any,
            result: DirectHostResult,
            produced: Sequence[Event],
            recovered: bool,
        ) -> Event:
            return Event.new(
                EventType.WORKER_RUN_COMPLETED,
                actor=Actor(kind=ActorKind.HOST, id="direct-api-host"),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=started.id,
                causation_id=task_event.id,
                correlation_id=task_event.correlation_id,
                payload={
                    "run_id": str(started.payload["run_id"]),
                    "task_event_id": task_event.id,
                    "job_id": job.id,
                    "adapter_id": "direct",
                    "artifact_origin": "host_generated",
                    "exit_code": 0,
                    "elapsed_ms": result.elapsed_ms,
                    "archive_path": str(archive.directory),
                    "archive_manifest": self._worker_archive_manifest(archive),
                    "produced_event_ids": [event.id for event in produced],
                    "host_identity": {
                        "prompt_contract": HOST_PROMPT_CONTRACT_VERSION,
                        "profile_id": profile.id,
                        "requested_provider_id": result.requested_provider_id,
                        "requested_model": result.requested_model,
                        "actual_provider": result.actual_provider,
                        "returned_model": result.returned_model,
                        "routing_settings": thaw_json(result.routing_settings),
                        "sampling_settings": thaw_json(result.sampling_settings),
                        "api_response_metadata": thaw_json(result.api_response_metadata),
                        "usage": thaw_json(result.usage),
                        "execution_profile": thaw_json(task_event.payload).get(
                            "worker_execution_profile"
                        ),
                        "routing": thaw_json(task_event.payload).get("worker_routing"),
                        "recovered_verified_orphan": recovered,
                    },
                },
                metadata={
                    "schema_version": 1,
                    "artifact_origin": "host_generated",
                    "envelope_emitter": "trusted_host_orchestrator",
                },
            )

        def append_completed(
            *,
            started: Event,
            archive: Any,
            result: DirectHostResult,
            recovered: bool,
        ) -> tuple[Event, ...]:
            prepared = prepare_structured_events(
                result.events,
                source=source,
                store=self.store,
                actor_kind=ActorKind.HOST,
                actor_id="direct-api-host",
                worker_run_id=str(started.payload["run_id"]),
            )
            existing = tuple(
                event
                for event in self.store.list_events()
                if event.payload.get("worker_run_id") == started.payload.get("run_id")
            )
            if len(existing) > len(prepared) or any(
                not self._matches_prepared_worker_event(observed, expected)
                for observed, expected in zip(existing, prepared, strict=False)
            ):
                raise ServiceError(
                    "Direct Host orphan structured events are not an archived response prefix"
                )
            pending = tuple(prepared[len(existing) :])
            all_produced = (*existing, *pending)
            terminal = terminal_event(
                started=started,
                archive=archive,
                result=result,
                produced=all_produced,
                recovered=recovered,
            )
            with self.store.transaction():
                self._record_host_call(
                    source,
                    result=result,
                    latency_ms=None,
                    job=job,
                    routed_task_type=routed_task_type,
                    worker=worker,
                    status="completed",
                )
                appended = self.store.append_many((*pending, terminal))
            return (*existing, *appended[:-1])

        def append_failed(
            *,
            started: Event,
            archive: Any,
            error: Exception,
            latency_ms: float,
            recovered: bool,
        ) -> Event:
            provider_failure = error if isinstance(error, HostProviderError) else None
            output_limited = bool(provider_failure is not None and provider_failure.output_limited)
            response_metadata = (
                thaw_json(provider_failure.api_response_metadata)
                if provider_failure is not None
                else {}
            )
            disposition = response_metadata.get("raw_response_disposition")
            reasons = []
            if output_limited:
                reasons.append("output_limit")
            if disposition == "quarantined_credential":
                reasons.append("credential_response_quarantined")
            if not reasons:
                reasons.append("provider_error")
            signature = sha256_json(
                {
                    "kind": "direct_host_error",
                    "provider_id": profile.host_provider_id,
                    "model": profile.model,
                    "error_type": type(error).__name__,
                    "message": str(error),
                }
            )
            repeated = self._worker_failure_is_repeated(task_event.id, signature)
            failed = Event.new(
                EventType.WORKER_RUN_FAILED,
                actor=Actor(kind=ActorKind.HOST, id="direct-api-host"),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=started.id,
                causation_id=task_event.id,
                correlation_id=task_event.correlation_id,
                payload={
                    "run_id": str(started.payload["run_id"]),
                    "task_event_id": task_event.id,
                    "job_id": job.id,
                    "adapter_id": "direct",
                    "artifact_origin": "host_generated",
                    "failure_type": type(error).__name__,
                    "reasons": reasons,
                    "output_limited": output_limited,
                    "failure_signature": signature,
                    "repeated_equivalent_failure": repeated,
                    "recovered_verified_orphan": recovered,
                    "archive_path": str(archive.directory),
                    "archive_manifest": self._worker_archive_manifest(archive),
                    "host_identity": {
                        "prompt_contract": HOST_PROMPT_CONTRACT_VERSION,
                        "profile_id": profile.id,
                        "requested_provider_id": (
                            provider_failure.requested_provider_id
                            if provider_failure is not None
                            else profile.host_provider_id
                        ),
                        "requested_model": (
                            provider_failure.requested_model
                            if provider_failure is not None
                            else profile.model
                        ),
                        "actual_provider": (
                            provider_failure.actual_provider
                            if provider_failure is not None
                            else None
                        ),
                        "returned_model": (
                            provider_failure.returned_model
                            if provider_failure is not None
                            else None
                        ),
                        "routing_settings": (
                            thaw_json(provider_failure.routing_settings)
                            if provider_failure is not None
                            else {}
                        ),
                        "sampling_settings": (
                            thaw_json(provider_failure.sampling_settings)
                            if provider_failure is not None
                            else {}
                        ),
                        "api_response_metadata": response_metadata,
                        "usage": (
                            thaw_json(provider_failure.usage)
                            if provider_failure is not None
                            else {}
                        ),
                        "execution_profile": thaw_json(task_event.payload).get(
                            "worker_execution_profile"
                        ),
                        "routing": thaw_json(task_event.payload).get("worker_routing"),
                    },
                },
                metadata={
                    "schema_version": 1,
                    "artifact_origin": "host_generated",
                    "envelope_emitter": "trusted_host_orchestrator",
                },
            )
            with self.store.transaction():
                self._record_host_call(
                    source,
                    result=provider_failure,
                    latency_ms=latency_ms,
                    job=job,
                    routed_task_type=routed_task_type,
                    worker=worker,
                    status="failed",
                )
                return self.store.append(failed)

        archive_service = WorkerRunArchive(self.archive_root / "workers")
        orphan_starts = sorted(
            (
                event
                for event in self.store.list_events(event_type=EventType.WORKER_RUN_STARTED)
                if event.payload.get("task_event_id") == task_event.id
                and event.payload.get("adapter_id") == "direct"
                and self._existing_worker_terminal(str(event.payload.get("run_id"))) is None
            ),
            key=lambda event: (event.created_at, event.id),
        )
        for started in orphan_starts:
            run_id = str(started.payload["run_id"])
            directory = archive_service.directory_for(run_id, started.created_at)
            if not directory.exists():
                continue
            snapshot = archive_service.load(run_id=run_id, archived_at=started.created_at)
            if (
                snapshot.metadata.get("artifact_origin") != "host_generated"
                or snapshot.prompt != expected_prompt
                or snapshot.command
                != (
                    "direct-api",
                    str(profile.host_provider_id or "unknown"),
                    str(profile.model or "unknown"),
                )
                or any(snapshot.task.get(key) != value for key, value in expected_task.items())
            ):
                raise ServiceError("Direct Host orphan archive identity has drifted")
            status_item = snapshot.metadata.get("execution", {}).get("status")
            status = (
                status_item.get("value")
                if isinstance(status_item, Mapping) and status_item.get("status") == "known"
                else None
            )
            response = snapshot.task.get("direct_host_response")
            if status == "completed" and isinstance(response, Mapping):
                output = response.get("output")
                if not isinstance(output, Mapping):
                    raise ServiceError("completed Direct Host orphan lacks output")
                from oracle_lab.agent_adapters import parse_structured_events

                proposals = (
                    parse_structured_events(
                        canonical_json(output),
                        expected_source_event_id=source.id,
                    )
                    if "events" in output
                    else ()
                )
                recovered_result = response_result(response, proposals=proposals)
                return append_completed(
                    started=started,
                    archive=snapshot.record,
                    result=recovered_result,
                    recovered=True,
                )
            error_document = snapshot.task.get("host_observed_failure")
            message = (
                str(error_document.get("message"))
                if isinstance(error_document, Mapping)
                else "archived Direct Host failure"
            )
            response = snapshot.task.get("direct_host_response")
            response_document = response if isinstance(response, Mapping) else {}
            execution = snapshot.metadata.get("execution")
            output_limited_item = (
                execution.get("output_limited") if isinstance(execution, Mapping) else None
            )
            output_limited = bool(
                isinstance(output_limited_item, Mapping)
                and output_limited_item.get("status") == "known"
                and output_limited_item.get("value") is True
            )
            failure = HostProviderError(
                message,
                raw_response=snapshot.stdout,
                api_response_metadata=(
                    response_document["api_response_metadata"]
                    if isinstance(response_document.get("api_response_metadata"), Mapping)
                    else {}
                ),
                output_limited=output_limited,
                requested_provider_id=(
                    str(response_document["requested_provider_id"])
                    if response_document.get("requested_provider_id") is not None
                    else None
                ),
                requested_model=(
                    str(response_document["requested_model"])
                    if response_document.get("requested_model") is not None
                    else None
                ),
                actual_provider=(
                    str(response_document["actual_provider"])
                    if response_document.get("actual_provider") is not None
                    else None
                ),
                returned_model=(
                    str(response_document["returned_model"])
                    if response_document.get("returned_model") is not None
                    else None
                ),
                routing_settings=(
                    response_document["routing_settings"]
                    if isinstance(response_document.get("routing_settings"), Mapping)
                    else {}
                ),
                sampling_settings=(
                    response_document["sampling_settings"]
                    if isinstance(response_document.get("sampling_settings"), Mapping)
                    else {}
                ),
                usage=(
                    response_document["usage"]
                    if isinstance(response_document.get("usage"), Mapping)
                    else {}
                ),
                elapsed_ms=float(response_document.get("elapsed_ms") or 0),
            )
            append_failed(
                started=started,
                archive=snapshot.record,
                error=failure,
                latency_ms=0,
                recovered=True,
            )
            raise ServiceError("recovered an archived Direct Host failure")

        if self._job_queue().is_archive_recovery_lease(job.id):
            raise NonRetryableWorkerError(
                "bounded Direct Host archive recovery found no recoverable terminal state"
            )
        run_id = f"{job.id}-a{job.attempts}"
        terminal = self._existing_worker_terminal(run_id)
        if terminal is not None:
            if terminal.type is EventType.WORKER_RUN_FAILED:
                raise ServiceError(f"Direct Host run already failed: {run_id}")
            return tuple(
                self.store.require(str(event_id))
                for event_id in terminal.payload.get("produced_event_ids", ())
            )
        started_event = next(
            (event for event in orphan_starts if event.payload.get("run_id") == run_id),
            None,
        )
        if started_event is None:
            started_event = self.store.append(
                Event.new(
                    EventType.WORKER_RUN_STARTED,
                    actor=Actor(kind=ActorKind.HOST, id="direct-api-host"),
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    parent_event_id=task_event.id,
                    causation_id=task_event.id,
                    correlation_id=task_event.correlation_id,
                    payload={
                        "run_id": run_id,
                        "task_event_id": task_event.id,
                        "job_id": job.id,
                        "attempt": job.attempts,
                        "adapter_id": "direct",
                        "artifact_origin": "host_generated",
                    },
                    metadata={
                        "schema_version": 1,
                        "artifact_origin": "host_generated",
                        "envelope_emitter": "trusted_host_orchestrator",
                    },
                )
            )
        request_payload = {
            **dict(call_payload),
            "idempotency_key": f"host-direct:{task_event.id}",
        }
        call_started = time.monotonic()
        try:
            result = asyncio.run(worker.run(routed_task_type, request_payload))
        except Exception as error:
            finished_at = dt.datetime.now(dt.UTC)
            failure_status = "provider_error"
            if isinstance(error, HostProviderError):
                disposition = error.api_response_metadata.get("raw_response_disposition")
                if disposition == "quarantined_credential":
                    failure_status = "credential_quarantined"
                elif error.output_limited:
                    failure_status = "output_limit"
            archive = self._archive_direct_host_result(
                run_id=run_id,
                task_event=task_event,
                task=task,
                routed_task_type=routed_task_type,
                worker=worker,
                result=None,
                started_at=started_event.created_at,
                finished_at=finished_at,
                status=failure_status,
                failure=error,
            )
            failed = append_failed(
                started=started_event,
                archive=archive,
                error=error,
                latency_ms=(time.monotonic() - call_started) * 1000,
                recovered=False,
            )
            repeated = bool(failed.payload.get("repeated_equivalent_failure"))
            error_type = NonRetryableWorkerError if repeated else ServiceError
            raise error_type(f"Direct Host call failed ({failed.id}): {error}") from error
        archive = self._archive_direct_host_result(
            run_id=run_id,
            task_event=task_event,
            task=task,
            routed_task_type=routed_task_type,
            worker=worker,
            result=result,
            started_at=started_event.created_at,
            finished_at=dt.datetime.now(dt.UTC),
            status="completed",
        )
        return append_completed(
            started=started_event,
            archive=archive,
            result=result,
            recovered=False,
        )

    def _execute_host_worker_job(self, job: Any) -> tuple[Event, ...]:
        """Route one analysis job to a direct host or isolated coding agent."""

        if self.host_worker_router is None:
            raise ServiceError("host worker router is not configured")
        from oracle_lab.agent_adapters import DirectAPIHost, WorkerTask

        source_event_id = (
            job.payload.get("analysis_source_event_id")
            or job.payload.get("source_event_id")
            or job.source_event_id
        )
        if not isinstance(source_event_id, str):
            raise ServiceError("host worker job has no source event")
        source = self.store.require(source_event_id)
        if source.branch_id is None:
            raise ServiceError("host worker source must belong to a branch")
        visible = self._branch_service().visible_events(source.branch_id, until_event_id=source.id)
        related_claims = tuple(
            thaw_json(event.payload)
            for event in visible
            if event.type == EventType.ANALYSIS_CLAIM_DETECTED
        )
        goal = self._host_worker_goal(job.kind, job.payload)
        task_kind = job.kind if job.kind in {"repository_edit"} else "analysis"
        task = WorkerTask(
            source,
            goal,
            related_claims=related_claims,
            recent_events=tuple(visible[-20:]),
            task_kind=task_kind,
            repository=(
                str(job.payload.get("repository_path")) if task_kind == "repository_edit" else None
            ),
            base_commit=(
                str(job.payload.get("base_commit")) if task_kind == "repository_edit" else None
            ),
            validation_commands=tuple(job.payload.get("validation_commands", ())),
        )
        routed_task_type, worker = self.host_worker_router.route(job.kind)
        replay_context = {
            key: job.payload[key]
            for key in ("replay_event_id", "replay_mode", "host_profile_label")
            if key in job.payload
        }
        operation_fields = {
            "job_id": job.id,
            "job_kind": job.kind,
            "routed_task_type": routed_task_type,
            "worker_type": type(worker).__name__,
        }
        with self.observability.operation(
            "host.worker.call",
            event=source,
            fields=operation_fields,
        ):
            started = time.monotonic()
            try:
                if isinstance(worker, DirectAPIHost):
                    produced = self._execute_direct_host(
                        job=job,
                        source=source,
                        task=task,
                        routed_task_type=routed_task_type,
                        worker=worker,
                        call_payload={
                            "source_event_id": source.id,
                            "source_event": source.to_dict(),
                            "related_claims": list(related_claims),
                            "recent_events": [event.to_dict() for event in visible[-20:]],
                            "goal": goal,
                            "prompt": task.render(),
                            **replay_context,
                        },
                    )
                    result = None
                else:
                    produced = self._execute_coding_worker(
                        job=job,
                        source=source,
                        task=task,
                        routed_task_type=routed_task_type,
                        worker=worker,
                    )
                    result = None
            except Exception:
                if not isinstance(worker, DirectAPIHost):
                    self._record_host_call(
                        source,
                        result=None,
                        latency_ms=(time.monotonic() - started) * 1000,
                        job=job,
                        routed_task_type=routed_task_type,
                        worker=worker,
                        status="failed",
                    )
                raise
            else:
                if not isinstance(worker, DirectAPIHost):
                    self._record_host_call(
                        source,
                        result=result,
                        latency_ms=None,
                        job=job,
                        routed_task_type=routed_task_type,
                        worker=worker,
                        status="completed",
                    )
            replay_event_id = job.payload.get("replay_event_id")
            if isinstance(replay_event_id, str):
                from oracle_lab.provenance import ProvenanceRelation, ProvenanceService

                replay_event = self.store.require(replay_event_id)
                provenance = ProvenanceService(self.store)
                for event in produced:
                    provenance.link(
                        "event",
                        event.id,
                        replay_event.id,
                        relation=ProvenanceRelation.CAUSED_BY,
                        actor=Actor(kind=ActorKind.SYSTEM, id="host-replay"),
                        session_id=source.session_id,
                        branch_id=source.branch_id,
                        correlation_id=replay_event.correlation_id,
                    )
            for event in produced:
                self.observability.log_event(
                    event,
                    fields={**operation_fields, "operation": "host.worker.ingest"},
                )

        # Derived events are first-class dispatcher inputs.  This is how a
        # claim comparison can lead to a contradiction/probe job without an
        # in-memory orchestration chain.
        dispatcher = self._dispatcher()
        for event in produced:
            dispatcher.dispatch(event)
            self._dispatch_tool_intent(event)
        return produced

    def _execute_branch_creation_job(self, job: Any) -> dict[str, Any]:
        """Materialize one policy-authorized Host branch proposal idempotently."""

        proposal_id = job.payload.get("source_event_id") or job.source_event_id
        if not isinstance(proposal_id, str):
            raise ServiceError("branch creation job has no proposal event")
        proposal = self.store.require(proposal_id)
        if proposal.type is not EventType.ANALYSIS_BRANCH_PROPOSED:
            raise ServiceError(f"branch job source is not a proposal: {proposal.id}")
        existing = [
            event
            for event in self.store.list_events(event_type=EventType.SESSION_FORKED)
            if event.payload.get("proposal_event_id") == proposal.id
        ]
        if existing:
            branch_id = existing[0].payload.get("branch_id")
            branch = self._branch_service().get_branch(str(branch_id))
            return {"branch": _jsonable(branch), "fork_event": existing[0].to_dict()}

        target_id = proposal.payload.get("fork_event_id")
        if not isinstance(target_id, str):
            raise ServiceError("branch proposal requires fork_event_id")
        if proposal.branch_id is None:
            raise ServiceError("branch proposal must belong to a branch")
        visible_ids = {
            event.id for event in self._branch_service().visible_events(proposal.branch_id)
        }
        if target_id not in visible_ids:
            raise ServiceError("branch proposal target is not visible from its source branch")

        approver_event_id = job.payload.get("approver_event_id")
        actor = Actor(kind=ActorKind.SYSTEM, id="branch-policy")
        if isinstance(approver_event_id, str):
            approval = self.store.require(approver_event_id)
            if (
                approval.type is not EventType.HUMAN_REQUEST_FORK
                or approval.actor.kind is not ActorKind.HUMAN
                or approval.payload.get("proposal_event_id") != proposal.id
            ):
                raise ServiceError("branch creation approval is not a matching human action")
            actor = Actor(kind=ActorKind.HUMAN, id=approval.actor.id)
        branch = self._branch_service().fork(
            target_id,
            title=(
                str(proposal.payload["title"])
                if proposal.payload.get("title") is not None
                else None
            ),
            actor=actor,
            correlation_id=proposal.correlation_id,
            causation_id=proposal.id,
            proposal_event_id=proposal.id,
        )
        fork_event = self.store.list_events(
            event_type=EventType.SESSION_FORKED,
            branch_id=branch.id,
            ascending=False,
            limit=1,
        )[0]
        return {"branch": _jsonable(branch), "fork_event": fork_event.to_dict()}

    @staticmethod
    def _run_git_bytes(
        repository: Path,
        *arguments: str,
        input_bytes: bytes | None = None,
    ) -> Any:
        try:
            return run_git(repository, *arguments, input_bytes=input_bytes, timeout=60)
        except GitControlError as error:
            raise ServiceError(str(error)) from error

    @classmethod
    def _require_git_bytes(
        cls,
        repository: Path,
        *arguments: str,
        input_bytes: bytes | None = None,
    ) -> bytes:
        result = cls._run_git_bytes(repository, *arguments, input_bytes=input_bytes)
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", "replace").strip()
            raise ServiceError(f"git {' '.join(arguments)} failed: {detail or result.returncode}")
        return bytes(result.stdout)

    @staticmethod
    def _verify_git_object_bytes(object_type: str, object_id: str, content: bytes) -> None:
        """Verify bytes against the immutable object ID used to select them."""

        if len(object_id) not in {40, 64} or any(
            character not in "0123456789abcdef" for character in object_id
        ):
            raise ServiceError(f"Git {object_type} object has an invalid identity")
        framed = f"{object_type} {len(content)}\0".encode("ascii") + content
        if len(object_id) == 40:
            actual = hashlib.sha1(framed, usedforsecurity=False).hexdigest()
        else:
            actual = hashlib.sha256(framed).hexdigest()
        if actual != object_id:
            raise ServiceError(f"Git {object_type} bytes do not match their immutable identity")

    @classmethod
    def _materialize_git_tree(
        cls,
        repository: Path,
        target_tree: str,
    ) -> tuple[dict[str, bytes], dict[str, int]]:
        """Read validation input from one frozen tree, never from the mutable index."""

        raw_tree = cls._require_git_bytes(repository, "cat-file", "tree", target_tree)
        cls._verify_git_object_bytes("tree", target_tree, raw_tree)
        raw_entries = cls._require_git_bytes(
            repository,
            "ls-tree",
            "-r",
            "-z",
            "--full-tree",
            target_tree,
        )
        files: dict[str, bytes] = {}
        file_modes: dict[str, int] = {}
        for entry in (item for item in raw_entries.split(b"\0") if item):
            try:
                metadata, raw_path = entry.split(b"\t", 1)
                raw_mode, raw_kind, raw_object_id = metadata.split(b" ")
                mode = raw_mode.decode("ascii")
                kind = raw_kind.decode("ascii")
                object_id = raw_object_id.decode("ascii")
                path = raw_path.decode("utf-8")
            except (ValueError, UnicodeDecodeError) as error:
                raise ServiceError("frozen Git tree contains an unsafe path entry") from error
            candidate = PurePosixPath(path)
            if (
                kind != "blob"
                or candidate.is_absolute()
                or not candidate.parts
                or ".." in candidate.parts
                or candidate.parts[0] == ".git"
                or str(candidate) != path
                or path in files
            ):
                raise ServiceError(f"frozen Git tree contains an unsafe entry: {path}")
            if mode not in {"100644", "100755"}:
                raise ServiceError(f"sandbox snapshot rejects Git mode {mode}: {path}")
            content = cls._require_git_bytes(repository, "cat-file", "blob", object_id)
            cls._verify_git_object_bytes("blob", object_id, content)
            files[path] = content
            file_modes[path] = 0o755 if mode == "100755" else 0o644
        return files, file_modes

    def _safe_staging_path(self, patch_event: Event, repository: Path) -> Path:
        protected = self._protected_worker_roots(repository)
        root = self._isolated_staging_root(repository, protected_roots=protected)
        path = (
            root / sha256_text(str(repository.resolve(strict=False)))[:16] / patch_event.id
        ).resolve(strict=False)
        return self._require_outside_worktrees("staging worktree", path, protected)

    def _enqueue_patch_validation_if_configured(
        self,
        *,
        patch: Event,
        application: Event,
    ) -> Any | None:
        """Idempotently bind an applied patch to its required validation job."""

        task = self.store.require(str(patch.payload["task_event_id"]))
        commands = task.payload.get("validation_commands", ())
        if (
            not isinstance(commands, Sequence)
            or isinstance(commands, (str, bytes, bytearray))
            or not commands
        ):
            return None
        target_tree = application.payload.get("target_tree")
        if not isinstance(target_tree, str) or not target_tree:
            raise ServiceError("applied patch lacks its immutable target tree")
        approval_event_id = application.payload.get("approval_event_id")
        if not isinstance(approval_event_id, str) or not approval_event_id:
            raise ServiceError("applied patch lacks its human approval identity")
        sandbox_snapshot = task.payload.get("validation_sandbox")
        self._validation_sandbox_from_snapshot(sandbox_snapshot)
        return self._job_queue().enqueue(
            "worker.patch.validate",
            {
                "patch_event_id": patch.id,
                "approval_event_id": approval_event_id,
                "application_event_id": application.id,
                "validation_commands": list(commands),
                "validation_sandbox": thaw_json(sandbox_snapshot),
            },
            source_event_id=application.id,
            idempotency_key=f"worker.patch.validate:{patch.id}:{target_tree}",
            session_id=patch.session_id,
            branch_id=patch.branch_id,
            serialize_branch=True,
            max_attempts=1,
        )

    def _execute_patch_application_job(self, job: Any) -> tuple[Event, ...]:
        """Apply one approved immutable patch only to a managed staging worktree."""

        patch_event_id = job.payload.get("patch_event_id")
        approval_event_id = job.payload.get("approval_event_id")
        if not isinstance(patch_event_id, str) or not isinstance(approval_event_id, str):
            raise ServiceError("patch application job is missing patch or approval identity")
        patch = self.store.require(patch_event_id)
        approval = self.store.require(approval_event_id)
        existing = [
            event
            for event in self.store.list_events(
                event_type=[
                    EventType.WORKER_PATCH_APPLIED,
                    EventType.WORKER_PATCH_CONFLICT,
                ]
            )
            if event.payload.get("patch_event_id") == patch.id
            and event.payload.get("approval_event_id") == approval.id
        ]
        if existing:
            if not (
                patch.metadata.get("bundle_import_authority") == "historical_only"
                or approval.metadata.get("bundle_import_authority") == "historical_only"
            ):
                for event in existing:
                    if event.type is EventType.WORKER_PATCH_APPLIED:
                        with self.store.transaction():
                            self._enqueue_patch_validation_if_configured(
                                patch=patch,
                                application=event,
                            )
            return tuple(existing)
        if (
            patch.metadata.get("bundle_import_authority") == "historical_only"
            or approval.metadata.get("bundle_import_authority") == "historical_only"
        ):
            raise ServiceError("imported patch history is evidence, not local execution authority")
        if (
            patch.type is not EventType.WORKER_PATCH_PROPOSED
            or approval.type is not EventType.HUMAN_PATCH_APPROVED
            or approval.payload.get("patch_event_id") != patch.id
            or approval.payload.get("patch_sha256") != patch.payload.get("patch_sha256")
            or approval.payload.get("base_commit") != patch.payload.get("base_commit")
        ):
            raise ServiceError("patch application lacks its exact human approval")

        repository = Path(str(patch.payload["repository_path"])).resolve()
        protected = self._assert_control_storage_isolated(repository)
        top = Path(
            self._require_git_bytes(repository, "rev-parse", "--show-toplevel").decode().strip()
        ).resolve()
        if top != repository:
            raise ServiceError("candidate patch repository identity changed")
        base_commit = str(patch.payload["base_commit"])
        resolved_base = (
            self._require_git_bytes(
                repository, "rev-parse", "--verify", f"{base_commit}^{{commit}}"
            )
            .decode()
            .strip()
        )
        if resolved_base != base_commit:
            raise ServiceError("candidate patch base commit is not immutable")
        patch_path = self._require_outside_worktrees(
            "candidate patch archive",
            Path(str(patch.payload["patch_archive_path"])),
            protected,
        )
        if patch_path.is_symlink() or not patch_path.is_file():
            raise ServiceError("candidate patch archive is missing or is a symlink")
        patch_bytes = patch_path.read_bytes()
        if sha256_bytes(patch_bytes) != patch.payload["patch_sha256"]:
            raise ServiceError("candidate patch archive hash mismatch")
        from oracle_lab.patches import (
            CandidatePatch,
            CandidatePatchError,
            PatchApplicationError,
            PatchDecision,
            PatchDecisionKind,
            PatchDecisionState,
            evaluate_patch_decisions,
            preflight_candidate_patch,
        )

        try:
            validated_candidate = CandidatePatch.from_capture(
                worker_run_id=str(patch.payload["worker_run_id"]),
                source_event_ids=tuple(patch.payload["source_event_ids"]),
                base_commit=base_commit,
                workspace_head=str(patch.payload["workspace_head"]),
                diff_bytes=patch_bytes,
                patch_sha256=str(patch.payload["patch_sha256"]),
                changed_paths=tuple(patch.payload["changed_paths"]),
                precondition_sha256=dict(patch.payload["precondition_sha256"]),
                changed_modes=dict(patch.payload["changed_modes"]),
                precondition_modes=dict(patch.payload["precondition_modes"]),
            )
            decision = PatchDecision(
                decision_event_id=approval.id,
                patch_event_id=patch.id,
                worker_run_id=validated_candidate.worker_run_id,
                patch_sha256=validated_candidate.patch_sha256,
                base_commit=validated_candidate.base_commit,
                decision=PatchDecisionKind.APPROVE,
                actor_kind=approval.actor.kind.value,
            )
            decision_state = evaluate_patch_decisions(
                validated_candidate,
                patch_event_id=patch.id,
                decisions=(decision,),
            )
        except (CandidatePatchError, KeyError, TypeError, ValueError) as error:
            raise ServiceError(
                f"candidate patch failed application revalidation: {error}"
            ) from error
        if decision_state.state is not PatchDecisionState.APPROVED:
            raise ServiceError("candidate patch is not uniquely approved")
        staging = self._safe_staging_path(patch, repository)
        stale_staging_removed = False
        if staging.is_symlink():
            raise ServiceError("staging worktree path may not be a symlink")
        if staging.exists():
            try:
                remove_standalone_clone(staging)
            except GitControlError as error:
                raise ServiceError(
                    f"failed to discard incomplete staging clone: {error}"
                ) from error
            stale_staging_removed = True
        try:
            preflight_candidate_patch(validated_candidate, repository)
        except PatchApplicationError as error:
            conflict = self.store.append(
                Event.new(
                    EventType.WORKER_PATCH_CONFLICT,
                    actor=Actor(kind=ActorKind.SYSTEM, id="patch-application-service"),
                    session_id=patch.session_id,
                    branch_id=patch.branch_id,
                    parent_event_id=approval.id,
                    causation_id=patch.id,
                    correlation_id=patch.correlation_id,
                    payload={
                        "patch_event_id": patch.id,
                        "approval_event_id": approval.id,
                        "patch_sha256": patch.payload["patch_sha256"],
                        "base_commit": base_commit,
                        "staging_path": str(staging),
                        "staging_created": False,
                        "staging_removed": stale_staging_removed,
                        "reasons": [f"source_precondition_conflict:{error}"],
                    },
                )
            )
            return (conflict,)
        staging.parent.mkdir(parents=True, exist_ok=True)
        try:
            create_standalone_clone(repository, staging, base_commit)
        except GitControlError as error:
            raise ServiceError(f"failed to create staging clone: {error}") from error
        created = True
        staging_top = Path(
            self._require_git_bytes(staging, "rev-parse", "--show-toplevel").decode().strip()
        ).resolve()
        staging_head = (
            self._require_git_bytes(staging, "rev-parse", "--verify", "HEAD^{commit}")
            .decode()
            .strip()
        )
        reasons: list[str] = []
        if staging_top != staging.resolve():
            reasons.append("staging_top_level_mismatch")
        if staging_head != base_commit:
            reasons.append("staging_base_commit_mismatch")
        if created and not reasons:
            try:
                preflight_candidate_patch(
                    validated_candidate,
                    staging,
                    _trusted_git_directory=True,
                )
            except PatchApplicationError as error:
                reasons.append(f"candidate_preflight_failed:{error}")
        unstaged = self._run_git_bytes(
            staging,
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--quiet",
            "--",
        )
        if unstaged.returncode not in {0, 1}:
            reasons.append("staging_worktree_inspection_failed")
        elif unstaged.returncode == 1:
            reasons.append("staging_contains_unstaged_changes")
        untracked = self._require_git_bytes(
            staging, "ls-files", "--others", "--exclude-standard", "-z"
        )
        if untracked:
            reasons.append("staging_contains_untracked_files")

        current_patch = self._require_git_bytes(
            staging,
            "diff",
            "--cached",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            base_commit,
            "--",
        )
        already_applied = bool(current_patch) and sha256_bytes(current_patch) == sha256_bytes(
            patch_bytes
        )
        if current_patch and not already_applied:
            reasons.append("staging_contains_different_changes")
        if not reasons and not already_applied:
            checked = self._run_git_bytes(
                staging,
                "apply",
                "--check",
                "--index",
                "--binary",
                "--whitespace=nowarn",
                "-",
                input_bytes=patch_bytes,
            )
            if checked.returncode != 0:
                reasons.append(
                    "patch_preflight_failed:" + checked.stderr.decode("utf-8", "replace").strip()
                )
            else:
                applied = self._run_git_bytes(
                    staging,
                    "apply",
                    "--index",
                    "--binary",
                    "--whitespace=nowarn",
                    "-",
                    input_bytes=patch_bytes,
                )
                if applied.returncode != 0:
                    reasons.append(
                        "patch_application_failed:"
                        + applied.stderr.decode("utf-8", "replace").strip()
                    )
        if reasons:
            staging_removed = False
            if created:
                try:
                    remove_standalone_clone(staging)
                    staging_removed = not staging.exists()
                except GitControlError:
                    staging_removed = False
            conflict = self.store.append(
                Event.new(
                    EventType.WORKER_PATCH_CONFLICT,
                    actor=Actor(kind=ActorKind.SYSTEM, id="patch-application-service"),
                    session_id=patch.session_id,
                    branch_id=patch.branch_id,
                    parent_event_id=approval.id,
                    causation_id=patch.id,
                    correlation_id=patch.correlation_id,
                    payload={
                        "patch_event_id": patch.id,
                        "approval_event_id": approval.id,
                        "patch_sha256": patch.payload["patch_sha256"],
                        "base_commit": base_commit,
                        "staging_path": str(staging),
                        "staging_created": created,
                        "staging_removed": staging_removed,
                        "reasons": reasons,
                    },
                )
            )
            return (conflict,)

        staged_patch = self._require_git_bytes(
            staging,
            "diff",
            "--cached",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            base_commit,
            "--",
        )
        if sha256_bytes(staged_patch) != sha256_bytes(patch_bytes):
            raise ServiceError("staging patch differs from the approved immutable patch")
        target_tree = self._require_git_bytes(staging, "write-tree").decode().strip()
        with self.store.transaction():
            applied_event = self.store.append(
                Event.new(
                    EventType.WORKER_PATCH_APPLIED,
                    actor=Actor(kind=ActorKind.SYSTEM, id="patch-application-service"),
                    session_id=patch.session_id,
                    branch_id=patch.branch_id,
                    parent_event_id=approval.id,
                    causation_id=patch.id,
                    correlation_id=patch.correlation_id,
                    payload={
                        "patch_event_id": patch.id,
                        "approval_event_id": approval.id,
                        "patch_sha256": patch.payload["patch_sha256"],
                        "base_commit": base_commit,
                        "staging_path": str(staging),
                        "target_tree": target_tree,
                        "source_repository_path": str(repository),
                        "committed": False,
                    },
                )
            )
            self._enqueue_patch_validation_if_configured(
                patch=patch,
                application=applied_event,
            )
        return (applied_event,)

    def _execute_patch_validation_job(self, job: Any) -> tuple[Event, ...]:
        """Validate the applied staging tree in the configured Docker sandbox."""

        patch_event_id = job.payload.get("patch_event_id")
        approval_event_id = job.payload.get("approval_event_id")
        application_event_id = job.payload.get("application_event_id")
        if not all(
            isinstance(identifier, str)
            for identifier in (patch_event_id, approval_event_id, application_event_id)
        ):
            raise ServiceError("patch validation job lacks patch/approval/application identity")
        assert isinstance(patch_event_id, str)
        assert isinstance(approval_event_id, str)
        assert isinstance(application_event_id, str)
        patch = self.store.require(patch_event_id)
        approval = self.store.require(approval_event_id)
        application = self.store.require(application_event_id)
        if (
            patch.type is not EventType.WORKER_PATCH_PROPOSED
            or approval.type is not EventType.HUMAN_PATCH_APPROVED
            or approval.actor.kind is not ActorKind.HUMAN
            or approval.payload.get("patch_event_id") != patch.id
            or application.type is not EventType.WORKER_PATCH_APPLIED
            or application.payload.get("patch_event_id") != patch.id
            or application.payload.get("approval_event_id") != approval.id
        ):
            raise ServiceError("patch validation source events do not match")
        if (
            patch.metadata.get("bundle_import_authority") == "historical_only"
            or approval.metadata.get("bundle_import_authority") == "historical_only"
            or application.metadata.get("bundle_import_authority") == "historical_only"
        ):
            raise ServiceError(
                "imported patch application is historical evidence and cannot be validated locally"
            )
        existing = [
            event
            for event in self.store.list_events(
                event_type=[
                    EventType.WORKER_VALIDATION_COMPLETED,
                    EventType.WORKER_VALIDATION_FAILED,
                ]
            )
            if event.payload.get("patch_event_id") == patch.id
            and event.payload.get("application_event_id") == application.id
        ]
        if existing:
            return tuple(existing)
        task = self.store.require(str(patch.payload["task_event_id"]))
        configured_commands = task.payload.get("validation_commands", ())
        commands = job.payload.get("validation_commands", ())
        if (
            not isinstance(commands, Sequence)
            or isinstance(commands, (str, bytes, bytearray))
            or not commands
            or any(not isinstance(command, str) or not command.strip() for command in commands)
        ):
            raise ServiceError("patch validation requires configured command strings")
        if tuple(commands) != tuple(configured_commands):
            raise ServiceError("validation commands differ from the frozen worker task")
        sandbox_snapshot = thaw_json(job.payload).get("validation_sandbox")
        if sandbox_snapshot != thaw_json(task.payload).get("validation_sandbox"):
            raise ServiceError("validation sandbox differs from the frozen worker task")
        sandbox_config = self._validation_sandbox_from_snapshot(sandbox_snapshot)
        staging = Path(str(application.payload["staging_path"])).resolve()
        if staging.is_symlink() or not staging.is_dir():
            raise ServiceError("staging worktree is unavailable for validation")
        target_tree = self._require_git_bytes(staging, "write-tree").decode().strip()
        if target_tree != application.payload.get("target_tree"):
            raise ServiceError("staging tree changed after approved patch application")
        files, file_modes = self._materialize_git_tree(staging, target_tree)

        from oracle_lab.tooling import DockerShellSandbox, ToolStatus
        from oracle_lab.validation_archive import (
            SandboxValidationArchive,
            ValidationRunMetadata,
        )

        command_text = "set -eu\n" + "\n".join(f"({command})" for command in commands)
        run_id = job.id
        validation_id = f"patch-{patch.id}"
        archive_service = SandboxValidationArchive(self.archive_root / "validations")
        archive_directory = archive_service.directory_for(
            run_id,
            validation_id,
            application.created_at,
        )
        if archive_directory.is_symlink():
            raise ServiceError("validation orphan archive directory is a symlink")
        if archive_directory.exists():
            snapshot = archive_service.load(
                run_id=run_id,
                validation_id=validation_id,
                archived_at=application.created_at,
            )
            if (
                snapshot.task.get("patch_event_id") != patch.id
                or snapshot.task.get("approval_event_id") != approval.id
                or snapshot.task.get("application_event_id") != application.id
                or snapshot.task.get("patch_sha256") != patch.payload.get("patch_sha256")
                or snapshot.task.get("base_commit") != patch.payload.get("base_commit")
                or snapshot.task.get("target_tree") != target_tree
                or snapshot.task.get("staging_path") != str(staging)
                or tuple(snapshot.task.get("commands", ())) != tuple(commands)
                or snapshot.command != ("/bin/sh", "-lc", command_text)
                or snapshot.task.get("sandbox_config") != sandbox_snapshot
            ):
                raise ServiceError("validation orphan archive belongs to another task")
            image_identity = snapshot.task.get("sandbox_image_identity")
            if (
                not isinstance(image_identity, Mapping)
                or image_identity.get("requested")
                != {"status": "known", "value": sandbox_config.image}
                or not isinstance(image_identity.get("actual"), Mapping)
                or image_identity["actual"].get("status") not in {"known", "unknown"}
            ):
                raise ServiceError("validation orphan image identity is invalid")
            actual_identity = image_identity["actual"]
            actual_value = actual_identity.get("value")
            if (
                actual_identity.get("status") == "known"
                and (
                    not isinstance(actual_value, str)
                    or not re.fullmatch(r"sha256:[0-9a-f]{64}", actual_value)
                )
            ) or (actual_identity.get("status") == "unknown" and actual_value is not None):
                raise ServiceError("validation orphan actual image identity is invalid")

            def observation(key: str) -> tuple[bool, Any]:
                execution = snapshot.metadata.get("execution")
                item = execution.get(key) if isinstance(execution, Mapping) else None
                known = isinstance(item, Mapping) and item.get("status") == "known"
                return known, item.get("value") if known else None

            archive = snapshot.record
            status_known, status_value = observation("status")
            error_known, tool_error = observation("error")
            if not status_known or not error_known or not isinstance(status_value, str):
                raise ServiceError("validation orphan lacks exact ToolResult status/error")
            try:
                tool_status = ToolStatus(status_value)
            except ValueError as error:
                raise ServiceError("validation orphan has invalid ToolResult status") from error
            if tool_error is not None and not isinstance(tool_error, str):
                raise ServiceError("validation orphan has invalid ToolResult error")
            _, exit_code = observation("exit_code")
            _, timed_out_value = observation("timed_out")
            _, output_limited_value = observation("output_limited")
            timed_out = timed_out_value is True
            output_limited = output_limited_value is True
        else:
            if self._job_queue().is_archive_recovery_lease(job.id):
                raise NonRetryableWorkerError(
                    "bounded validation archive recovery cannot start a new sandbox run"
                )
            started_at = dt.datetime.now(dt.UTC)
            request_id = f"validation_{sha256_text(patch.id + application.id)[:24]}"
            result = DockerShellSandbox(sandbox_config).run(
                command_text,
                request_id=request_id,
                source_event_id=application.id,
                files=files,
                file_modes=file_modes,
            )
            finished_at = dt.datetime.now(dt.UTC)
            tool_status = result.status
            tool_error = result.error
            exit_code = result.exit_code
            timed_out = result.status is ToolStatus.TIMEOUT
            output_limited = result.status is ToolStatus.OUTPUT_LIMIT
            result_metadata = result.metadata if isinstance(result.metadata, Mapping) else {}
            reported_requested = result_metadata.get("sandbox_image_requested")
            if reported_requested not in {None, sandbox_config.image}:
                raise ServiceError("sandbox reported a different requested image")
            actual_image = result_metadata.get("sandbox_image_actual")
            actual_known = isinstance(actual_image, str) and bool(actual_image)
            if actual_known and not re.fullmatch(r"sha256:[0-9a-f]{64}", actual_image):
                raise ServiceError("sandbox reported an invalid actual image identifier")
            image_identity = {
                "requested": {"status": "known", "value": sandbox_config.image},
                "actual": {
                    "status": "known" if actual_known else "unknown",
                    "value": actual_image if actual_known else None,
                },
            }
            archive = archive_service.write(
                run_id=run_id,
                validation_id=validation_id,
                task={
                    "patch_event_id": patch.id,
                    "approval_event_id": approval.id,
                    "application_event_id": application.id,
                    "patch_sha256": patch.payload["patch_sha256"],
                    "base_commit": patch.payload["base_commit"],
                    "target_tree": target_tree,
                    "staging_path": str(staging),
                    "commands": list(commands),
                    "sandbox_config": sandbox_snapshot,
                    "sandbox_image_identity": image_identity,
                },
                command=("/bin/sh", "-lc", command_text),
                stdout=result.raw_stdout,
                stderr=result.raw_stderr,
                run_metadata=ValidationRunMetadata(
                    started_at=started_at,
                    finished_at=finished_at,
                    exit_code=exit_code,
                    timed_out=timed_out,
                    output_limited=output_limited,
                    status=tool_status.value,
                    error=tool_error,
                ),
                archived_at=application.created_at,
            )
        manifest = {
            artifact.name: {
                "path": str(artifact.path),
                "sha256": artifact.sha256,
                "size_bytes": artifact.size_bytes,
            }
            for artifact in archive.artifacts
        }
        succeeded = tool_status is ToolStatus.OK
        validation_event = self.store.append(
            Event.new(
                (
                    EventType.WORKER_VALIDATION_COMPLETED
                    if succeeded
                    else EventType.WORKER_VALIDATION_FAILED
                ),
                actor=Actor(kind=ActorKind.TOOL, id="docker-validation"),
                session_id=patch.session_id,
                branch_id=patch.branch_id,
                parent_event_id=application.id,
                causation_id=patch.id,
                correlation_id=patch.correlation_id,
                payload={
                    "patch_event_id": patch.id,
                    "approval_event_id": approval.id,
                    "application_event_id": application.id,
                    "patch_sha256": patch.payload["patch_sha256"],
                    "base_commit": patch.payload["base_commit"],
                    "target_tree": target_tree,
                    "commands": list(commands),
                    "status": tool_status.value,
                    "error": tool_error,
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "output_limited": output_limited,
                    "sandbox_config": sandbox_snapshot,
                    "sandbox_image_identity": image_identity,
                    "archive_path": str(archive.directory),
                    "archive_manifest": manifest,
                    "truth_domain": "sandbox",
                    "artifact_origin": "tool_result",
                },
                metadata={
                    "schema_version": 1,
                    "truth_domain": "sandbox",
                    "artifact_origin": "tool_result",
                },
            )
        )
        return (validation_event,)

    def _run_host_analysis(self, source: Event) -> list[Event]:
        from oracle_lab.host import AnalysisContext, HostRunner

        runner = HostRunner.default(analysis=self.runtime_config.policies.analysis)
        pending = [source]
        produced: list[Event] = []
        while pending:
            current = pending.pop(0)
            if current.branch_id is None:
                raise ServiceError("host analysis source must belong to a branch")
            branch_events = self._branch_service().visible_events(current.branch_id)
            cutoff = (current.created_at, current.id)
            all_events = [
                event
                for event in branch_events
                if event.branch_id != current.branch_id or (event.created_at, event.id) <= cutoff
            ]
            historical_claims = tuple(
                thaw_json(event.payload)
                for event in all_events
                if event.type == EventType.ANALYSIS_CLAIM_DETECTED and event.id != current.id
            )
            context = AnalysisContext(
                existing_event_ids=frozenset(event.id for event in all_events),
                historical_claims=historical_claims,
                recent_events=tuple(all_events[-20:]),
            )
            with self.observability.operation("host.analysis.local", event=current):
                derived = runner.analyze(current, context)
            existing = self.store.list_events(
                session_id=current.session_id,
                branch_id=current.branch_id,
                causation_id=current.id,
            )
            by_signature = {
                (
                    event.type.value,
                    event.actor.id,
                    canonical_json(thaw_json(event.payload)),
                ): event
                for event in existing
            }
            selected: list[Event] = []
            new_events: list[Event] = []
            for proposal in derived:
                signature = (
                    proposal.type.value,
                    proposal.actor.id,
                    canonical_json(thaw_json(proposal.payload)),
                )
                match = by_signature.get(signature)
                selected.append(match or proposal)
                if match is None:
                    new_events.append(proposal)
            if new_events:
                self.store.append_many(new_events)
                for event in new_events:
                    self.observability.log_event(
                        event,
                        fields={"operation": "host.analysis.local"},
                    )
            produced.extend(selected)
            pending.extend(selected)
            for event in selected:
                if event.type in {
                    EventType.ANALYSIS_PROBE_PROPOSED,
                    EventType.ANALYSIS_CANON_CANDIDATE,
                    EventType.ANALYSIS_BRANCH_PROPOSED,
                }:
                    self._dispatcher().dispatch(event)
                self._dispatch_tool_intent(event)
        return produced

    @staticmethod
    def _known_archive_value(document: Mapping[str, Any], section: str, key: str) -> Any:
        group = document.get(section)
        item = group.get(key) if isinstance(group, Mapping) else None
        return (
            item.get("value")
            if isinstance(item, Mapping) and item.get("status") == "known"
            else None
        )

    def _verified_worker_recovery_archive(self, job: Any) -> bool:
        """Verify the exact completed archive for one expired coding-worker lease."""

        if job.kind != "repository_edit":
            return False
        task_event_id = job.payload.get("task_event_id")
        original_source_event_id = job.payload.get("source_event_id")
        if not isinstance(task_event_id, str) or not isinstance(original_source_event_id, str):
            return False
        task_event = self.store.get(task_event_id)
        execution_source = (
            self.store.get(job.source_event_id) if isinstance(job.source_event_id, str) else None
        )
        original_source = self.store.get(original_source_event_id)
        if (
            task_event is None
            or task_event.type is not EventType.WORKER_TASK_REQUESTED
            or execution_source is None
            or execution_source.id != task_event.id
            or original_source is None
            or task_event.payload.get("job_id") != job.id
            or task_event.payload.get("source_event_id") != original_source_event_id
            or task_event.session_id != job.session_id
            or task_event.branch_id != job.branch_id
        ):
            return False
        if self.host_worker_router is None:
            return False
        routed_task_type, worker = self.host_worker_router.route(job.kind)
        self._assert_frozen_worker_execution(
            job=job,
            task_event=task_event,
            routed_task_type=routed_task_type,
            worker=worker,
        )
        thawed_job_payload = thaw_json(job.payload)
        thawed_task_payload = thaw_json(task_event.payload)
        expected_event_payload = {
            "job_id": job.id,
            "task_kind": "repository_edit",
            "source_event_id": original_source_event_id,
            "goal": job.payload.get("goal"),
            "repository_path": job.payload.get("repository_path"),
            "base_commit": job.payload.get("base_commit"),
            "validation_commands": tuple(job.payload.get("validation_commands", ())),
            "validation_sandbox": thawed_job_payload.get("validation_sandbox"),
            "worker_profile_id": job.payload.get("worker_profile_id"),
            "worker_execution_profile": thawed_job_payload.get("worker_execution_profile"),
            "worker_routing": thawed_job_payload.get("worker_routing"),
        }
        for key, expected in expected_event_payload.items():
            observed = thawed_task_payload.get(key)
            if key == "validation_commands":
                observed = tuple(observed or ())
            if observed != expected:
                return False
        run_id = f"{job.id}-a{job.attempts}"
        started = next(
            (
                event
                for event in self.store.list_events(event_type=EventType.WORKER_RUN_STARTED)
                if event.payload.get("run_id") == run_id
                and event.payload.get("task_event_id") == task_event.id
                and event.payload.get("job_id") == job.id
                and event.payload.get("attempt") == job.attempts
            ),
            None,
        )
        if (
            started is None
            or started.session_id != job.session_id
            or started.branch_id != job.branch_id
        ):
            return False
        from oracle_lab.worker_archive import WorkerRunArchive

        snapshot = WorkerRunArchive(self.archive_root / "workers").load(
            run_id=run_id,
            archived_at=started.created_at,
        )
        expected_task = {
            "task_event_id": task_event.id,
            "job_id": job.id,
            "task_kind": "repository_edit",
            "source_event_id": original_source.id,
            "goal": job.payload.get("goal"),
            "repository_path": job.payload.get("repository_path"),
            "requested_base_commit": job.payload.get("base_commit"),
            "validation_commands": list(job.payload.get("validation_commands", ())),
            "validation_sandbox": thawed_job_payload.get("validation_sandbox"),
            "worker_profile_id": job.payload.get("worker_profile_id"),
            "worker_execution_profile": thawed_job_payload.get("worker_execution_profile"),
        }
        if any(snapshot.task.get(key) != value for key, value in expected_task.items()):
            return False
        if snapshot.task.get("worker_routing") != thawed_task_payload.get("worker_routing"):
            return False
        expected_automation = {
            key: thawed_task_payload.get(key)
            for key in (
                "automation_depth",
                "automation_budget_remaining",
                "automation_loop_detector",
                "loop_signature",
            )
        }
        if snapshot.task.get("automation") != expected_automation:
            return False
        status = self._known_archive_value(snapshot.metadata, "execution", "status")
        archive_adapter = self._known_archive_value(snapshot.metadata, "identity", "adapter")
        archive_base = self._known_archive_value(snapshot.metadata, "identity", "base_commit")
        from oracle_lab.agent_adapters import WorkerTask

        visible = self._branch_service().visible_events(
            str(original_source.branch_id), until_event_id=original_source.id
        )
        expected_worker_task = WorkerTask(
            original_source,
            str(job.payload.get("goal")),
            related_claims=tuple(
                thaw_json(event.payload)
                for event in visible
                if event.type is EventType.ANALYSIS_CLAIM_DETECTED
            ),
            recent_events=tuple(visible[-20:]),
            task_kind="repository_edit",
            repository=str(job.payload.get("repository_path")),
            base_commit=str(job.payload.get("base_commit")),
            validation_commands=tuple(job.payload.get("validation_commands", ())),
        )
        expected_prompt = expected_worker_task.render()
        expected_argv = self._expected_worker_argv(
            worker,
            expected_prompt,
            started=snapshot.task.get("command_capture_status")
            != "requested_not_confirmed_started",
        )
        return (
            isinstance(status, str)
            and bool(status)
            and started.payload.get("adapter_id") == task_event.payload.get("worker_adapter")
            and archive_adapter == started.payload.get("adapter_id")
            and archive_adapter == getattr(worker, "name", None)
            and archive_base == job.payload.get("base_commit")
            and snapshot.prompt == expected_prompt
            and snapshot.command == expected_argv
        )

    def _verified_validation_recovery_archive(self, job: Any) -> bool:
        """Verify the exact completed archive for one expired validation lease."""

        if job.kind != "worker.patch.validate":
            return False
        patch_event_id = job.payload.get("patch_event_id")
        approval_event_id = job.payload.get("approval_event_id")
        application_event_id = job.payload.get("application_event_id")
        if not all(
            isinstance(identifier, str)
            for identifier in (patch_event_id, approval_event_id, application_event_id)
        ):
            return False
        assert isinstance(patch_event_id, str)
        assert isinstance(approval_event_id, str)
        assert isinstance(application_event_id, str)
        patch = self.store.get(patch_event_id)
        approval = self.store.get(approval_event_id)
        application = self.store.get(application_event_id)
        if (
            patch is None
            or patch.type is not EventType.WORKER_PATCH_PROPOSED
            or approval is None
            or approval.type is not EventType.HUMAN_PATCH_APPROVED
            or approval.actor.kind is not ActorKind.HUMAN
            or approval.payload.get("patch_event_id") != patch.id
            or application is None
            or application.type is not EventType.WORKER_PATCH_APPLIED
            or application.payload.get("patch_event_id") != patch.id
            or application.payload.get("approval_event_id") != approval.id
            or patch.session_id != job.session_id
            or patch.branch_id != job.branch_id
        ):
            return False
        target_tree = application.payload.get("target_tree")
        commands = job.payload.get("validation_commands", ())
        command_text = "set -eu\n" + "\n".join(f"({command})" for command in commands)
        from oracle_lab.validation_archive import SandboxValidationArchive

        snapshot = SandboxValidationArchive(self.archive_root / "validations").load(
            run_id=job.id,
            validation_id=f"patch-{patch.id}",
            archived_at=application.created_at,
        )
        if (
            snapshot.task.get("patch_event_id") != patch.id
            or snapshot.task.get("approval_event_id") != approval.id
            or snapshot.task.get("application_event_id") != application.id
            or snapshot.task.get("patch_sha256") != patch.payload.get("patch_sha256")
            or snapshot.task.get("base_commit") != patch.payload.get("base_commit")
            or snapshot.task.get("target_tree") != target_tree
            or snapshot.task.get("staging_path") != application.payload.get("staging_path")
            or tuple(snapshot.task.get("commands", ())) != tuple(commands)
            or snapshot.task.get("sandbox_config")
            != thaw_json(job.payload).get("validation_sandbox")
            or snapshot.command != ("/bin/sh", "-lc", command_text)
        ):
            return False
        image_identity = snapshot.task.get("sandbox_image_identity")
        if not isinstance(image_identity, Mapping):
            return False
        requested_image = image_identity.get("requested")
        actual_image = image_identity.get("actual")
        if (
            not isinstance(requested_image, Mapping)
            or requested_image.get("status") != "known"
            or requested_image.get("value")
            != job.payload.get("validation_sandbox", {}).get("image_requested")
            or not isinstance(actual_image, Mapping)
            or actual_image.get("status") not in {"known", "unknown"}
        ):
            return False
        actual_status = actual_image.get("status")
        actual_value = actual_image.get("value")
        if (
            actual_status == "known"
            and (
                not isinstance(actual_value, str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", actual_value)
            )
        ) or (actual_status == "unknown" and actual_value is not None):
            return False
        execution = snapshot.metadata.get("execution")
        if not isinstance(execution, Mapping):
            return False
        return all(
            isinstance(execution.get(key), Mapping) and execution[key].get("status") == "known"
            for key in ("status", "error", "exit_code", "timed_out", "output_limited")
        )

    def _verified_expired_archive_recovery_ids(self, queue: Any) -> frozenset[str]:
        """Select only expired jobs whose exact same-run archive verifies now."""

        verified: set[str] = set()
        for job in queue.expired_leases():
            try:
                recoverable = self._verified_worker_recovery_archive(
                    job
                ) or self._verified_validation_recovery_archive(job)
            except (OSError, RuntimeError, TypeError, ValueError):
                recoverable = False
            if recoverable:
                verified.add(job.id)
        return frozenset(verified)

    def _automation_lease_seconds(self) -> float:
        """Return a lease long enough for configured workers, with heartbeat backup."""

        lease_seconds = 60.0
        if self.host_worker_router is None:
            return lease_seconds
        configured_timeouts = [
            float(getattr(worker, "timeout_seconds", 0))
            for worker in (
                getattr(self.host_worker_router, "codex", None),
                getattr(self.host_worker_router, "opencode", None),
            )
            if worker is not None
        ]
        if configured_timeouts:
            lease_seconds = max(lease_seconds, max(configured_timeouts) + 60.0)
        return lease_seconds

    def _dispatch_automation_job(self, job: Any) -> Any:
        """Dispatch one leased job without changing its queue state."""

        if job.kind == "oracle.generate":
            return self._execute_oracle_job(job)
        if job.kind == "tool.execute":
            return self._execute_tool_job(job)
        if job.kind == "branch.create":
            return self._execute_branch_creation_job(job)
        if job.kind == "worker.patch.apply":
            return self._execute_patch_application_job(job)
        if job.kind == "worker.patch.validate":
            return self._execute_patch_validation_job(job)
        if (
            self.host_worker_router is not None
            and job.kind in self.host_worker_router.supported_task_kinds
        ):
            return self._execute_host_worker_job(job)
        if self.job_handler is not None:
            return self.job_handler(job)
        raise ServiceError(f"no handler for job kind: {job.kind}")

    def _fail_automation_job(
        self,
        queue: Any,
        job: Any,
        error: Exception,
        *,
        worker_id: str,
        archive_recovery_only: bool,
    ) -> dict[str, Any]:
        """Durably fail one owned lease and emit any automation stop boundary."""

        queue.fail(
            job.id,
            str(error),
            worker_id=worker_id,
            retryable=(
                not archive_recovery_only and not isinstance(error, NonRetryableWorkerError)
            ),
        )
        if job.kind in {"oracle.generate", "tool.execute"}:
            raw_source_id = job.payload.get("request_event_id")
            source_event = (
                self.store.get(str(raw_source_id)) if isinstance(raw_source_id, str) else None
            )
            if source_event is not None:
                reason = "tool_failure"
                if job.kind == "oracle.generate":
                    completed_outputs = self.store.list_events(
                        event_type=EventType.ORACLE_OUTPUT,
                        causation_id=source_event.id,
                    )
                    reason = "postprocessing_failure" if completed_outputs else "provider_failure"
                self._stop_automation(
                    source_event,
                    reason,
                    detail={
                        "job_id": job.id,
                        "error_type": type(error).__name__,
                    },
                )
        return {"job_id": job.id, "status": "failed", "error": str(error)}

    def run_automation(
        self,
        *,
        until_human: bool = False,
        max_jobs: int = 100,
    ) -> dict[str, Any]:
        queue = self._job_queue()
        worker_id = f"oracle-cli:{new_id('runner')}"
        processed = []
        stopped = "idle"
        while len(processed) < max_jobs:
            try:
                active_session_id, active_branch_id = self._active()
            except ServiceError:
                active_session_id = active_branch_id = None
            paused_branches = self._paused_job_branches()
            active_pause = (
                self._active_pause(active_session_id, active_branch_id)
                if active_session_id is not None and active_branch_id is not None
                else None
            )
            if until_human and (pending := self._pending_human_judgment()) is not None:
                stop_event = self._stop_automation(pending, "human_gate")
                stopped = "human_judgment"
                return {
                    "processed": processed,
                    "stopped": stopped,
                    "event_id": pending.id,
                    "stop_event_id": stop_event.id,
                }
            kinds: list[str] | None = None
            if self.job_handler is None:
                kinds = [
                    "oracle.generate",
                    "tool.execute",
                    "worker.patch.apply",
                    "worker.patch.validate",
                ]
                kinds.append("branch.create")
                if self.host_worker_router is not None:
                    kinds.extend(sorted(self.host_worker_router.supported_task_kinds))
            lease_seconds = self._automation_lease_seconds()
            recoverable_expired = self._verified_expired_archive_recovery_ids(queue)
            job = queue.lease_one(
                worker_id,
                kinds=kinds,
                lease_seconds=lease_seconds,
                excluded_branches=paused_branches,
                recover_expired_job_ids=recoverable_expired,
                allow_archive_recovery=True,
            )
            if job is None:
                if active_pause is not None:
                    return {
                        "processed": processed,
                        "stopped": "paused",
                        "event_id": active_pause.id,
                    }
                break
            archive_recovery_only = queue.is_archive_recovery_lease(
                job.id,
                worker_id=worker_id,
            )
            heartbeat = _AutomationLeaseHeartbeat(
                queue,
                job_id=job.id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
            )
            heartbeat.start()
            try:
                try:
                    result = self._dispatch_automation_job(job)
                    heartbeat.raise_if_failed()
                except Exception as error:
                    processed.append(
                        self._fail_automation_job(
                            queue,
                            job,
                            error,
                            worker_id=worker_id,
                            archive_recovery_only=archive_recovery_only,
                        )
                    )
                else:
                    queue.complete(job.id, worker_id=worker_id)
                    processed.append(
                        {"job_id": job.id, "status": "completed", "result": _jsonable(result)}
                    )
            finally:
                heartbeat.stop()
        if len(processed) >= max_jobs:
            stopped = "max_jobs"
        return {"processed": processed, "stopped": stopped}

    def list_jobs(self) -> list[dict[str, Any]]:
        return [_jsonable(job) for job in self._job_queue().list_jobs()]

    def retry_jobs(self, job_id: str | None = None) -> list[dict[str, Any]]:
        from oracle_lab.jobs import JobStatus

        queue = self._job_queue()
        jobs = [queue.require(job_id)] if job_id else queue.list_jobs(status=JobStatus.DEAD_LETTER)
        retried: list[Any] = []
        coding_kinds = {"repository_edit"}
        if self.host_worker_router is not None:
            coding_kinds.update(self.host_worker_router.coding_task_types)
        for job in jobs:
            if job.kind not in coding_kinds and not job.kind.startswith("worker.patch."):
                retried.append(queue.retry_dead_letter(job.id, reset_attempts=True))
                continue
            existing = next(
                (
                    candidate
                    for candidate in queue.list_jobs()
                    if candidate.payload.get("retry_of_job_id") == job.id
                ),
                None,
            )
            if existing is not None:
                retried.append(existing)
                continue
            new_job_id = new_id("job")
            payload = dict(job.payload)
            payload["retry_of_job_id"] = job.id
            source_event_id = job.source_event_id
            if job.kind in coding_kinds:
                raw_task_id = payload.get("task_event_id")
                old_task = (
                    self.store.get(str(raw_task_id))
                    if isinstance(raw_task_id, str)
                    else next(
                        (
                            event
                            for event in self.store.list_events(
                                event_type=EventType.WORKER_TASK_REQUESTED
                            )
                            if event.payload.get("job_id") == job.id
                        ),
                        None,
                    )
                )
                if old_task is None:
                    raise ServiceError("dead worker job has no durable task event")
                terminal = next(
                    (
                        event
                        for event in reversed(
                            self.store.list_events(
                                event_type=[
                                    EventType.WORKER_RUN_COMPLETED,
                                    EventType.WORKER_RUN_FAILED,
                                ]
                            )
                        )
                        if event.payload.get("task_event_id") == old_task.id
                    ),
                    old_task,
                )
                retry_task_payload = thaw_json(old_task.payload)
                original_source = self.store.require(str(retry_task_payload["source_event_id"]))
                automation = self._worker_automation_fields(
                    original_source,
                    signature_seed={
                        "operation": "explicit_worker_retry",
                        "retry_of_job_id": job.id,
                        "retry_of_task_event_id": old_task.id,
                    },
                )
                retry_task_payload.update(
                    {
                        "job_id": new_job_id,
                        "retry_of_job_id": job.id,
                        "retry_of_task_event_id": old_task.id,
                        "previous_terminal_event_id": terminal.id,
                        "idempotency_key": f"worker.explicit_retry:{job.id}",
                        **automation,
                    }
                )
                retry_task = self.store.append(
                    Event.new(
                        EventType.WORKER_TASK_REQUESTED,
                        actor=Actor(kind=ActorKind.HUMAN, id="cli"),
                        session_id=old_task.session_id,
                        branch_id=old_task.branch_id,
                        parent_event_id=terminal.id,
                        causation_id=terminal.id,
                        correlation_id=old_task.correlation_id,
                        payload=retry_task_payload,
                    )
                )
                payload["task_event_id"] = retry_task.id
                source_event_id = retry_task.id
            retried.append(
                queue.enqueue(
                    job.kind,
                    payload,
                    source_event_id=source_event_id,
                    idempotency_key=f"worker.explicit_retry:{job.id}",
                    priority=job.priority,
                    provider_id=job.provider_id,
                    session_id=job.session_id,
                    branch_id=job.branch_id,
                    serialize_branch=job.serialize_branch,
                    max_attempts=job.max_attempts,
                    job_id=new_job_id,
                )
            )
        return [_jsonable(job) for job in retried]

    def cost(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> dict[str, Any]:
        clauses = []
        parameters: list[Any] = []
        if session_id:
            clauses.append("session_id = ?")
            parameters.append(session_id)
        if model_id:
            clauses.append("model_id = ?")
            parameters.append(model_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        row = self.store.connection.execute(
            f"""
            SELECT COUNT(*) AS records,
                   COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                   COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                   COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
                   COALESCE(SUM(CAST(provider_cost AS REAL)), 0.0) AS provider_cost,
                   COALESCE(SUM(request_count), 0) AS request_count
            FROM usage_records{where}
            """,
            parameters,
        ).fetchone()
        return dict(row)

    def compare_models(
        self,
        *,
        session_id: str,
        event_id: str,
        model_profile_ids: Sequence[str],
    ) -> dict[str, Any]:
        if not model_profile_ids:
            raise ServiceError("compare-models requires at least one model profile")
        source = self.store.require(event_id)
        if source.session_id != session_id:
            raise ServiceError("comparison event belongs to another session")
        if source.branch_id is None:
            raise ServiceError("comparison source does not belong to a branch")
        from oracle_lab.sampling import SamplingService
        from oracle_lab.session import SessionContextBuilder

        profiles = [self.runtime_config.model(profile_id) for profile_id in model_profile_ids]
        system_prompts = {profile.system_prompt for profile in profiles}
        if len(system_prompts) != 1:
            raise ServiceError("compared profiles must use the same visible system prompt")
        context_limits = {profile.max_context_messages for profile in profiles}
        if len(context_limits) != 1:
            raise ServiceError("compared profiles must use the same context-message limit")
        visible = self._branch_service().visible_events(source.branch_id, until_event_id=source.id)
        context = SessionContextBuilder().build(
            visible,
            session_id=session_id,
            branch_id=source.branch_id,
            tip_event_id=source.id,
            system_prompt=profiles[0].system_prompt,
            system_prompt_source_event_id=self._system_prompt_source_event_id(
                branch_id=source.branch_id,
                system_prompt=profiles[0].system_prompt,
            ),
            include_reasoning=False,
            max_messages=profiles[0].max_context_messages,
        )
        request_compare = self._append(
            EventType.HUMAN_REQUEST_COMPARE,
            {
                "event_id": event_id,
                "model_profile_ids": list(model_profile_ids),
            },
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
        )
        group = SamplingService(self.store).create_group(
            from_event_id=event_id,
            context=context.provider_messages(),
            provider_id="model-comparison",
            model_id="multiple-profiles",
            sampling={"model_profile_ids": list(model_profile_ids)},
            actor=Actor(kind=ActorKind.HOST, id="model-archaeology"),
            session_id=session_id,
            branch_id=source.branch_id,
            correlation_id=request_compare.correlation_id,
        )
        requests = [
            self._request(
                operation="compare-models",
                parent_event_id=group.created_event_id,
                model_profile_id=profile_id,
                extra={
                    "sample_group_id": group.id,
                    "sample_ordinal": ordinal,
                    "context_hash": group.context_hash,
                    "from_event_id": event_id,
                },
            )
            for ordinal, profile_id in enumerate(model_profile_ids)
        ]
        return {"sample_group": _jsonable(group), "requests": requests}

    def replay_exact(
        self,
        *,
        session_id: str | None = None,
        branch_id: str | None = None,
        record: bool = True,
    ) -> dict[str, Any]:
        """Rebuild projections from fixed history without querying an oracle."""

        from oracle_lab.replay import ReplayService

        with self.observability.operation(
            "replay.exact",
            fields={"session_id": session_id, "branch_id": branch_id},
        ):
            result = ReplayService(self.store).exact(
                session_id=session_id,
                branch_id=branch_id,
                rebuild_projections=True,
                record=record,
                actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            )
        value = _jsonable(result)
        if result.replay_event_id is not None:
            audit = self.store.require(result.replay_event_id)
            self.observability.log_event(audit, fields={"operation": "replay.exact"})
            value["audit_event"] = audit.to_dict()
        return value

    def _record_host_replay(
        self,
        source: Event,
        *,
        execution: str,
        host_profile_label: str,
        generated_event_ids: Sequence[str] = (),
        planned_job_kinds: Sequence[str] = (),
    ) -> Event:
        from oracle_lab.replay import ReplayMode

        return self._append(
            EventType.SESSION_REPLAYED,
            {
                "mode": ReplayMode.HOST_ANALYSIS.value,
                "execution": execution,
                "host_profile_label": host_profile_label,
                "host_router": (
                    None
                    if self.host_worker_router is None
                    else type(self.host_worker_router).__name__
                ),
                "input_event_ids": [source.id],
                "generated_event_ids": list(generated_event_ids),
                "planned_job_kinds": list(planned_job_kinds),
                "oracle_queried": False,
            },
            actor=Actor(kind=ActorKind.HUMAN, id="cli"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=new_id("cor"),
        )

    def replay_host_analysis(
        self,
        event_id: str,
        *,
        host_profile_label: str | None = None,
    ) -> dict[str, Any]:
        """Replay host analysis over one immutable historical oracle output."""

        source = self.store.require(event_id)
        if source.type is not EventType.ORACLE_OUTPUT:
            raise ServiceError(f"host replay source is not oracle.output: {event_id}")
        if source.session_id is None or source.branch_id is None:
            raise ServiceError("host replay source must belong to a session and branch")

        if self.host_worker_router is None:
            label = (host_profile_label or "deterministic-local").strip()
            if not label:
                raise ServiceError("host profile label must not be blank")
            before = {event.id for event in self.store.list_events()}
            with self.observability.operation(
                "replay.host.local",
                event=source,
                fields={"host_profile_label": label},
            ):
                analysis = self._run_host_analysis(source)
            generated = tuple(event.id for event in analysis if event.id not in before)
            audit = self._record_host_replay(
                source,
                execution="deterministic_local",
                host_profile_label=label,
                generated_event_ids=generated,
            )
            from oracle_lab.provenance import ProvenanceRelation, ProvenanceService

            provenance = ProvenanceService(self.store)
            for event in analysis:
                provenance.link(
                    "event",
                    event.id,
                    audit.id,
                    relation=ProvenanceRelation.CAUSED_BY,
                    actor=Actor(kind=ActorKind.SYSTEM, id="host-replay"),
                    session_id=source.session_id,
                    branch_id=source.branch_id,
                    correlation_id=audit.correlation_id,
                )
            return {
                "mode": "host_analysis",
                "execution": "deterministic_local",
                "source_event_id": source.id,
                "host_profile_label": label,
                "analysis_event_ids": [event.id for event in analysis],
                "generated_event_ids": list(generated),
                "jobs": [],
                "audit_event": audit.to_dict(),
            }

        if host_profile_label is None or not host_profile_label.strip():
            raise ServiceError("router-backed host replay requires --host-profile")
        label = host_profile_label.strip()
        decisions = [
            decision
            for decision in self._dispatcher().evaluate(source)
            if decision.rule_id == "oracle-output-analysis" and decision.action.kind == "task"
        ]
        planned = [decision.action.name for decision in decisions]
        supported = set(self.host_worker_router.supported_task_kinds)
        unsupported = sorted(set(planned) - supported)
        if unsupported:
            raise ServiceError(f"host router cannot consume replay tasks: {', '.join(unsupported)}")
        audit = self._record_host_replay(
            source,
            execution="durable_host_jobs",
            host_profile_label=label,
            planned_job_kinds=planned,
        )
        queue = self._job_queue()
        jobs = [
            queue.enqueue(
                decision.action.name,
                {
                    **dict(decision.action.payload),
                    "source_event_id": source.id,
                    "analysis_source_event_id": source.id,
                    "replay_event_id": audit.id,
                    "replay_mode": "host_analysis",
                    "host_profile_label": label,
                    "host_router": type(self.host_worker_router).__name__,
                    "dispatch_rule_id": decision.rule_id,
                },
                source_event_id=audit.id,
                idempotency_key=(f"host.replay:{audit.id}:{index}:{decision.action.name}"),
                priority=decision.action.priority,
                session_id=source.session_id,
                branch_id=source.branch_id,
            )
            for index, decision in enumerate(decisions)
        ]
        return {
            "mode": "host_analysis",
            "execution": "durable_host_jobs",
            "source_event_id": source.id,
            "host_profile_label": label,
            "analysis_event_ids": [],
            "generated_event_ids": [],
            "jobs": [_jsonable(job) for job in jobs],
            "audit_event": audit.to_dict(),
        }

    def export(
        self,
        kind: str,
        destination: str | Path,
        *,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        if session_id is None:
            session_id, _ = self._active()
        events = [
            event
            for event in self.store.list_events(session_id=session_id)
            if not is_synthetic_lineage(event, self.store.get)
        ]
        path = Path(destination)
        if kind == "bundle":
            session_projection = self._branch_service().get_session(session_id)
            if session_projection is None:
                raise ServiceError(f"session not found: {session_id}")
            raw_records: dict[str, Any] = {}
            for event in events:
                if event.type is not EventType.ORACLE_OUTPUT:
                    continue
                archive_path = event.payload.get("archive_path")
                if not isinstance(archive_path, str):
                    continue
                raw_path = Path(archive_path)
                if not raw_path.is_file():
                    raise ServiceError(f"raw archive is missing for {event.id}: {raw_path}")
                raw_bytes = raw_path.read_bytes()
                expected_sha256 = event.payload.get("archive_sha256")
                if expected_sha256 is not None and sha256_bytes(raw_bytes) != expected_sha256:
                    raise ServiceError(f"raw archive hash mismatch for {event.id}")
                metadata_path = raw_path.with_name(f"{raw_path.stem}.metadata.json")
                if not metadata_path.is_file():
                    raise ServiceError(f"archive metadata sidecar is missing for {event.id}")
                try:
                    sidecar = json.loads(metadata_path.read_bytes())
                except (OSError, json.JSONDecodeError) as error:
                    raise ServiceError(
                        f"archive metadata sidecar is invalid for {event.id}"
                    ) from error
                if not isinstance(sidecar, Mapping):
                    raise ServiceError(f"archive metadata sidecar is not an object for {event.id}")
                if (
                    sidecar.get("event_id") != event.id
                    or sidecar.get("raw_file") != raw_path.name
                    or sidecar.get("raw_sha256") != expected_sha256
                    or sidecar.get("material_origin") != event.payload.get("material_origin")
                ):
                    raise ServiceError(f"archive metadata sidecar mismatch for {event.id}")
                request_sha256 = sidecar.get("request_sha256")
                if not isinstance(request_sha256, str) or len(request_sha256) != 64:
                    raise ServiceError(f"archive request hash is missing for {event.id}")
                record: dict[str, Any] = {
                    "archive_raw_bytes": raw_bytes,
                    "archive_metadata_bytes": metadata_path.read_bytes(),
                }
                raw_records[event.id] = record

            def portable_archives(
                event_types: set[EventType],
                artifact_names: set[str],
            ) -> dict[str, dict[str, bytes]]:
                records: dict[str, dict[str, bytes]] = {}
                for event in events:
                    if event.type not in event_types:
                        continue
                    archive_path = event.payload.get("archive_path")
                    archive_manifest = event.payload.get("archive_manifest")
                    if (
                        not isinstance(archive_path, str)
                        or not isinstance(archive_manifest, Mapping)
                        or set(archive_manifest) != artifact_names
                    ):
                        raise ServiceError(f"archive manifest is incomplete for {event.id}")
                    directory = Path(archive_path)
                    if directory.is_symlink() or not directory.is_dir():
                        raise ServiceError(f"archive directory is unavailable for {event.id}")
                    if {entry.name for entry in directory.iterdir()} != artifact_names:
                        raise ServiceError(
                            f"archive directory has unexpected artifacts for {event.id}"
                        )
                    artifact_bytes: dict[str, bytes] = {}
                    for name in sorted(artifact_names):
                        integrity = archive_manifest[name]
                        if not isinstance(integrity, Mapping):
                            raise ServiceError(
                                f"archive integrity is invalid for {event.id}/{name}"
                            )
                        raw_path = integrity.get("path")
                        digest = integrity.get("sha256")
                        size = integrity.get("size_bytes")
                        if (
                            not isinstance(raw_path, str)
                            or not isinstance(digest, str)
                            or not isinstance(size, int)
                            or isinstance(size, bool)
                            or size < 0
                        ):
                            raise ServiceError(f"archive identity is invalid for {event.id}/{name}")
                        artifact_path = Path(raw_path)
                        if artifact_path.is_symlink() or not artifact_path.is_file():
                            raise ServiceError(
                                f"archive artifact is unavailable for {event.id}/{name}"
                            )
                        try:
                            relative = artifact_path.resolve().relative_to(directory.resolve())
                        except ValueError as error:
                            raise ServiceError(
                                f"archive artifact escapes its directory for {event.id}/{name}"
                            ) from error
                        if relative.parts != (name,):
                            raise ServiceError(
                                f"archive artifact path mismatch for {event.id}/{name}"
                            )
                        content = artifact_path.read_bytes()
                        if len(content) != size or sha256_bytes(content) != digest:
                            raise ServiceError(
                                f"archive artifact hash mismatch for {event.id}/{name}"
                            )
                        artifact_bytes[name] = content
                    records[event.id] = artifact_bytes
                return records

            worker_archives = portable_archives(
                {EventType.WORKER_RUN_COMPLETED, EventType.WORKER_RUN_FAILED},
                {
                    "task.json",
                    "prompt.txt",
                    "command.json",
                    "stdout.bin",
                    "stderr.bin",
                    "patch.diff",
                    "metadata.json",
                },
            )
            validation_archives = portable_archives(
                {
                    EventType.WORKER_VALIDATION_COMPLETED,
                    EventType.WORKER_VALIDATION_FAILED,
                },
                {"task.json", "command.json", "stdout.bin", "stderr.bin", "metadata.json"},
            )
            claims = self._rows(
                "SELECT * FROM claims WHERE source_event_id IN "
                "(SELECT id FROM events WHERE session_id = ?) ORDER BY id",
                (session_id,),
            )
            motif_rows = self._rows(
                """
                SELECT m.id, m.label, m.description, em.event_id AS source_event_id
                FROM motifs m JOIN event_motifs em ON em.motif_id = m.id
                JOIN events e ON e.id = em.event_id
                WHERE e.session_id = ? ORDER BY m.id, em.event_id
                """,
                (session_id,),
            )
            motif_map: dict[str, dict[str, Any]] = {}
            for row in motif_rows:
                motif_id = str(row["id"])
                motif = motif_map.setdefault(
                    motif_id,
                    {
                        "id": motif_id,
                        "label": row["label"],
                        "description": row["description"],
                        "source_event_ids": [],
                    },
                )
                motif["source_event_ids"].append(str(row["source_event_id"]))
            motifs = list(motif_map.values())
            provenance = self._rows(
                """
                SELECT p.* FROM provenance_edges p
                JOIN events e ON e.id = p.source_event_id
                WHERE e.session_id = ? ORDER BY p.id
                """,
                (session_id,),
            )
            result = export_research_bundle(
                path,
                events=events,
                session_records=[
                    event for event in events if event.type is EventType.ORACLE_CONTEXT_BUILT
                ],
                raw_records=raw_records,
                claims=claims,
                motifs=motifs,
                provenance=provenance,
                manifest={
                    "session_id": session_id,
                    "current_branch_id": session_projection.current_branch_id,
                },
                worker_archives=worker_archives,
                validation_archives=validation_archives,
            )
        elif kind == "transcript":
            result = export_transcript(path, events=events, title=f"Session {session_id}")
        elif kind in {"corpus", "selected"}:
            provenance_rows = self._rows(
                "SELECT derived_id, source_event_id FROM provenance_edges ORDER BY id"
            )
            provenance_map: dict[str, list[str]] = {}
            for row in provenance_rows:
                provenance_map.setdefault(str(row["derived_id"]), []).append(
                    str(row["source_event_id"])
                )
            result = export_selected_corpus(path, events=events, provenance=provenance_map)
        else:
            raise ServiceError(f"unknown export kind: {kind}")
        return {"kind": kind, "path": str(result), "session_id": session_id}


__all__ = ["OracleLabService", "ServiceError"]
