"""Session creation, arbitrary event forks, and visible branch lineage."""

from __future__ import annotations

import datetime as dt
import sqlite3
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.ids import new_id

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


class Session(BaseModel):
    """A rebuildable session projection."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    title: str | None = None
    root_event_id: str | None = None
    current_branch_id: str | None = None
    model_profile_id: str | None = None
    created_at: dt.datetime
    archived_at: dt.datetime | None = None


class Branch(BaseModel):
    """A branch node pointing at its immutable fork event."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    session_id: str
    parent_branch_id: str | None = None
    fork_event_id: str | None = None
    title: str | None = None
    created_at: dt.datetime
    archived_at: dt.datetime | None = None


class BranchError(RuntimeError):
    """Raised for invalid forks or lineage operations."""


def visible_event_ids_from_connection(
    connection: sqlite3.Connection,
    branch_id: str,
    *,
    until_event_id: str | None = None,
) -> list[str]:
    """Resolve narrative lineage using only an existing connection.

    This helper is public for projection plugins that must reconstruct state at
    a historical fork without consulting a mutable current-branch snapshot.
    A fork inherits the ``parent_event_id`` ancestry of its fork event, not a
    timestamp prefix; parallel sampling siblings therefore cannot leak into
    the new branch.
    """
    lineage: list[sqlite3.Row] = []
    seen: set[str] = set()
    current_id: str | None = branch_id
    while current_id is not None:
        if current_id in seen:
            raise BranchError(f"branch lineage cycle at {current_id}")
        seen.add(current_id)
        row = connection.execute("SELECT * FROM branches WHERE id = ?", (current_id,)).fetchone()
        if row is None:
            raise BranchError(f"branch not found: {current_id}")
        lineage.append(row)
        current_id = row["parent_branch_id"]
    lineage.reverse()

    allowed_branches = {str(row["id"]) for row in lineage}
    leaf = lineage[-1]
    inherited: list[str] = []
    if leaf["fork_event_id"] is not None:
        inherited = _parent_chain(connection, str(leaf["fork_event_id"]))

    own = [
        str(row["id"])
        for row in connection.execute(
            "SELECT id FROM events WHERE branch_id = ? ORDER BY created_at, id",
            (branch_id,),
        ).fetchall()
    ]
    result = list(dict.fromkeys((*inherited, *own)))
    if until_event_id is None:
        return result

    if until_event_id not in result:
        raise BranchError(f"event {until_event_id} is not visible from branch {branch_id}")
    chain = _parent_chain(connection, until_event_id)
    for event_id in chain:
        row = connection.execute(
            "SELECT branch_id FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None or row["branch_id"] not in allowed_branches:
            raise BranchError(f"event {event_id} escapes branch lineage for {branch_id}")
    return chain


def _parent_chain(connection: sqlite3.Connection, event_id: str) -> list[str]:
    chain: list[str] = []
    seen: set[str] = set()
    current_id: str | None = event_id
    while current_id is not None:
        if current_id in seen:
            raise BranchError(f"event parent cycle at {current_id}")
        seen.add(current_id)
        row = connection.execute(
            "SELECT id, parent_event_id FROM events WHERE id = ?", (current_id,)
        ).fetchone()
        if row is None:
            raise BranchError(f"event not found in parent chain: {current_id}")
        chain.append(str(row["id"]))
        current_id = row["parent_event_id"]
    chain.reverse()
    return chain


class BranchService:
    """Append session/branch events and query branch-visible histories."""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def create_session(
        self,
        *,
        title: str | None = None,
        model_profile_id: str | None = None,
        session_id: str | None = None,
        branch_id: str | None = None,
        actor: Actor | None = None,
    ) -> Session:
        """Create a root session using a taxonomy-valid checkpoint event."""
        session_identifier = session_id or new_id("ses")
        branch_identifier = branch_id or new_id("br")
        if self.get_session(session_identifier) is not None:
            raise BranchError(f"session already exists: {session_identifier}")
        event = Event(
            type=EventType.HUMAN_CHECKPOINT,
            actor=actor or Actor(kind=ActorKind.HUMAN, id="local-curator"),
            session_id=session_identifier,
            branch_id=branch_identifier,
            correlation_id=new_id("corr"),
            payload={
                "operation": "session.created",
                "title": title,
                "model_profile_id": model_profile_id,
                "branch_id": branch_identifier,
                "branch_title": "main",
            },
        )
        self.store.append(event)
        session = self.get_session(session_identifier)
        if session is None:
            raise RuntimeError(f"session projection did not apply for {event.id}")
        return session

    def fork(
        self,
        from_event_id: str,
        *,
        title: str | None = None,
        branch_id: str | None = None,
        actor: Actor | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        proposal_event_id: str | None = None,
    ) -> Branch:
        """Fork from any historical event and preserve history through it."""
        source = self.store.require(from_event_id)
        if source.session_id is None or source.branch_id is None:
            raise BranchError("fork source must belong to a session and branch")
        parent = self.get_branch(source.branch_id)
        if parent is None:
            raise BranchError(f"source branch projection not found: {source.branch_id}")
        if parent.archived_at is not None:
            raise BranchError(f"cannot fork archived branch: {parent.id}")
        identifier = branch_id or new_id("br")
        if self.get_branch(identifier) is not None:
            raise BranchError(f"branch already exists: {identifier}")
        event = Event(
            type=EventType.SESSION_FORKED,
            actor=actor or Actor(kind=ActorKind.HUMAN, id="local-curator"),
            session_id=source.session_id,
            branch_id=identifier,
            parent_event_id=source.id,
            causation_id=causation_id or source.id,
            correlation_id=correlation_id or source.correlation_id or new_id("corr"),
            payload={
                "branch_id": identifier,
                "parent_branch_id": source.branch_id,
                "fork_event_id": source.id,
                "title": title,
                "proposal_event_id": proposal_event_id,
            },
        )
        self.store.append(event)
        branch = self.get_branch(identifier)
        if branch is None:
            raise RuntimeError(f"branch projection did not apply for {event.id}")
        return branch

    def checkpoint(
        self,
        branch_id: str,
        *,
        title: str | None = None,
        actor: Actor | None = None,
    ) -> Event:
        """Append a branch checkpoint after its latest visible event."""
        branch = self.require_branch(branch_id)
        events = self.visible_events(branch_id)
        parent = events[-1] if events else None
        event = Event(
            type=EventType.SESSION_CHECKPOINTED,
            actor=actor or Actor(kind=ActorKind.HUMAN, id="local-curator"),
            session_id=branch.session_id,
            branch_id=branch.id,
            parent_event_id=None if parent is None else parent.id,
            causation_id=None if parent is None else parent.id,
            correlation_id=(
                new_id("corr") if parent is None else parent.correlation_id or new_id("corr")
            ),
            payload={"title": title},
        )
        return self.store.append(event)

    def merge_metadata(
        self,
        source_branch_id: str,
        target_branch_id: str,
        metadata: Mapping[str, Any],
        *,
        actor: Actor | None = None,
    ) -> Event:
        """Record a metadata-only merge without copying or rewriting events."""
        source = self.require_branch(source_branch_id)
        target = self.require_branch(target_branch_id)
        if source.session_id != target.session_id:
            raise BranchError("branches from different sessions cannot be merged")
        target_events = self.visible_events(target.id)
        parent = target_events[-1] if target_events else None
        event = Event(
            type=EventType.SESSION_MERGED,
            actor=actor or Actor(kind=ActorKind.HUMAN, id="local-curator"),
            session_id=target.session_id,
            branch_id=target.id,
            parent_event_id=None if parent is None else parent.id,
            causation_id=None if parent is None else parent.id,
            correlation_id=new_id("corr"),
            payload={
                "source_branch_id": source.id,
                "target_branch_id": target.id,
                "metadata": dict(metadata),
                "history_rewritten": False,
            },
        )
        return self.store.append(event)

    def archive(self, branch_id: str, *, actor: Actor | None = None) -> Branch:
        """Append ``branch.archived`` and return the updated projection.

        Archiving the final live branch also marks its session archived in the
        projection; no session history is deleted.
        """
        branch = self.require_branch(branch_id)
        events = self.visible_events(branch.id)
        parent = events[-1] if events else None
        event = Event(
            type=EventType.BRANCH_ARCHIVED,
            actor=actor or Actor(kind=ActorKind.HUMAN, id="local-curator"),
            session_id=branch.session_id,
            branch_id=branch.id,
            parent_event_id=None if parent is None else parent.id,
            causation_id=None if parent is None else parent.id,
            correlation_id=new_id("corr"),
            payload={"branch_id": branch.id},
        )
        self.store.append(event)
        return self.require_branch(branch.id)

    def archive_session(self, session_id: str, *, actor: Actor | None = None) -> Session:
        """Archive every live branch and thereby archive the session."""
        session = self.get_session(session_id)
        if session is None:
            raise BranchError(f"session not found: {session_id}")
        live = self.list_branches(session_id=session_id, include_archived=False)
        for branch in live:
            self.archive(branch.id, actor=actor)
        archived = self.get_session(session_id)
        if archived is None or archived.archived_at is None:
            raise RuntimeError(f"session projection did not archive {session_id}")
        return archived

    def visible_events(self, branch_id: str, *, until_event_id: str | None = None) -> list[Event]:
        """Return inherited history plus this branch's independent future."""
        identifiers = visible_event_ids_from_connection(
            self.store.connection, branch_id, until_event_id=until_event_id
        )
        return [self.store.require(identifier) for identifier in identifiers]

    def lineage(self, branch_id: str) -> list[Branch]:
        """Return root-to-leaf branch ancestry and reject cycles."""
        result: list[Branch] = []
        seen: set[str] = set()
        current = self.require_branch(branch_id)
        while True:
            if current.id in seen:
                raise BranchError(f"branch lineage cycle at {current.id}")
            seen.add(current.id)
            result.append(current)
            if current.parent_branch_id is None:
                break
            current = self.require_branch(current.parent_branch_id)
        result.reverse()
        return result

    def get_session(self, session_id: str) -> Session | None:
        """Return one session projection."""
        row = self.store.connection.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        return None if row is None else self._row_to_session(row)

    def list_sessions(self, *, include_archived: bool = False) -> list[Session]:
        """List sessions in creation order."""
        where = "" if include_archived else " WHERE archived_at IS NULL"
        rows = self.store.connection.execute(
            f"SELECT * FROM sessions{where} ORDER BY created_at, id"
        ).fetchall()
        return [self._row_to_session(row) for row in rows]

    def get_branch(self, branch_id: str) -> Branch | None:
        """Return one branch projection."""
        row = self.store.connection.execute(
            "SELECT * FROM branches WHERE id = ?", (branch_id,)
        ).fetchone()
        return None if row is None else self._row_to_branch(row)

    def require_branch(self, branch_id: str) -> Branch:
        """Return one branch or raise :class:`BranchError`."""
        branch = self.get_branch(branch_id)
        if branch is None:
            raise BranchError(f"branch not found: {branch_id}")
        return branch

    def list_branches(
        self, *, session_id: str | None = None, include_archived: bool = False
    ) -> list[Branch]:
        """List branches, optionally scoped to a session."""
        clauses: list[str] = []
        params: list[Any] = []
        if session_id is not None:
            clauses.append("session_id = ?")
            params.append(session_id)
        if not include_archived:
            clauses.append("archived_at IS NULL")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.store.connection.execute(
            f"SELECT * FROM branches{where} ORDER BY created_at, id", params
        ).fetchall()
        return [self._row_to_branch(row) for row in rows]

    @staticmethod
    def _row_to_session(row: sqlite3.Row) -> Session:
        return Session(
            id=row["id"],
            title=row["title"],
            root_event_id=row["root_event_id"],
            current_branch_id=row["current_branch_id"],
            model_profile_id=row["model_profile_id"],
            created_at=_parse(row["created_at"]),
            archived_at=_parse(row["archived_at"]),
        )

    @staticmethod
    def _row_to_branch(row: sqlite3.Row) -> Branch:
        return Branch(
            id=row["id"],
            session_id=row["session_id"],
            parent_branch_id=row["parent_branch_id"],
            fork_event_id=row["fork_event_id"],
            title=row["title"],
            created_at=_parse(row["created_at"]),
            archived_at=_parse(row["archived_at"]),
        )


def _parse(value: str | None) -> dt.datetime | None:
    return None if value is None else dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


__all__ = [
    "Branch",
    "BranchError",
    "BranchService",
    "Session",
    "visible_event_ids_from_connection",
]
