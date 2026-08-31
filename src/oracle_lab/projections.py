"""Rebuildable SQLite projections over the authoritative event log."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from oracle_lab.events import ActorKind, Event, EventType, thaw_json
from oracle_lab.retrieval import encode_local_embedding

if TYPE_CHECKING:
    from oracle_lab.store import EventStore


class ClaimStatus(StrEnum):
    """Non-boolean claim lifecycle states."""

    RAW_CLAIM = "raw_claim"
    PROVISIONAL = "provisional"
    OBSERVED = "observed"
    RECURRENT = "recurrent"
    LAW_CANDIDATE = "law_candidate"
    CANONICAL = "canonical"
    CONFLICTED = "conflicted"
    SUPERSEDED = "superseded"
    REJECTED = "rejected"


class Projection(Protocol):
    """Minimal plugin contract used by :class:`ProjectionManager`."""

    name: str
    tables: Sequence[str]

    def apply(self, connection: sqlite3.Connection, event: Event) -> None: ...


class ProjectionError(RuntimeError):
    """Raised when a projection cannot consume an otherwise valid event."""


_TERMINAL_CLAIM_STATES = {
    ClaimStatus.SUPERSEDED,
    ClaimStatus.REJECTED,
}
_PROMOTION_ORDER = (
    ClaimStatus.RAW_CLAIM,
    ClaimStatus.PROVISIONAL,
    ClaimStatus.OBSERVED,
    ClaimStatus.RECURRENT,
    ClaimStatus.LAW_CANDIDATE,
    ClaimStatus.CANONICAL,
)


def _synthetic_lineage(connection: sqlite3.Connection, event: Event) -> bool:
    """Return true when a derived event ultimately cites a synthetic fixture."""

    def marker(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> bool:
        return (
            payload.get("material_origin") == "synthetic_fixture"
            or metadata.get("material_origin") == "synthetic_fixture"
            or payload.get("synthetic_fixture") is True
            or metadata.get("synthetic_fixture") is True
        )

    if marker(event.payload, event.metadata):
        return True
    queue = list(
        dict.fromkeys(
            identifier
            for identifier in (
                event.causation_id,
                event.parent_event_id,
                event.payload.get("source_event_id"),
                event.payload.get("target_event_id"),
                event.payload.get("event_id"),
                event.payload.get("verification_source_event_id"),
                event.payload.get("approver_event_id"),
            )
            if isinstance(identifier, str)
        )
    )
    explicit = event.payload.get("source_event_ids", ())
    if isinstance(explicit, Sequence) and not isinstance(explicit, (str, bytes, bytearray)):
        queue.extend(str(value) for value in explicit if isinstance(value, str))
    seen: set[str] = set()
    while queue:
        event_id = queue.pop()
        if event_id in seen:
            continue
        seen.add(event_id)
        if len(seen) > 10_000:
            raise ValueError("synthetic lineage exceeds safety limit")
        row = connection.execute(
            """
            SELECT parent_event_id, causation_id, payload_json, metadata_json
            FROM events WHERE id = ?
            """,
            (event_id,),
        ).fetchone()
        if row is None:
            continue
        payload = json.loads(row["payload_json"])
        metadata = json.loads(row["metadata_json"])
        if marker(payload, metadata):
            return True
        for identifier in (
            row["causation_id"],
            row["parent_event_id"],
            payload.get("source_event_id"),
            payload.get("target_event_id"),
            payload.get("event_id"),
            payload.get("verification_source_event_id"),
            payload.get("approver_event_id"),
        ):
            if isinstance(identifier, str) and identifier not in seen:
                queue.append(identifier)
        values = payload.get("source_event_ids", ())
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
            queue.extend(
                str(identifier)
                for identifier in values
                if isinstance(identifier, str) and identifier not in seen
            )
    return False


def _worker_marker(
    event_type: str,
    actor_kind: str,
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> bool:
    return (
        event_type.startswith("worker.")
        or actor_kind == ActorKind.WORKER.value
        or payload.get("artifact_origin") == "worker_generated"
        or metadata.get("artifact_origin") == "worker_generated"
    )


def _lineage_references(
    parent_event_id: Any,
    causation_id: Any,
    payload: Mapping[str, Any],
) -> list[str]:
    identifiers = [
        value
        for value in (
            parent_event_id,
            causation_id,
            payload.get("source_event_id"),
            payload.get("target_event_id"),
            payload.get("event_id"),
            payload.get("verification_source_event_id"),
            payload.get("approver_event_id"),
        )
        if isinstance(value, str)
    ]
    explicit = payload.get("source_event_ids", ())
    if isinstance(explicit, Sequence) and not isinstance(explicit, (str, bytes, bytearray)):
        identifiers.extend(str(value) for value in explicit if isinstance(value, str))
    return identifiers


def _stored_event_has_worker_lineage(connection: sqlite3.Connection, event_id: str) -> bool:
    queue = [event_id]
    seen: set[str] = set()
    while queue:
        current_id = queue.pop()
        if current_id in seen:
            continue
        seen.add(current_id)
        if len(seen) > 10_000:
            raise ValueError("worker lineage exceeds safety limit")
        row = connection.execute(
            """
            SELECT type, actor_kind, parent_event_id, causation_id,
                   payload_json, metadata_json
            FROM events WHERE id = ?
            """,
            (current_id,),
        ).fetchone()
        if row is None:
            continue
        payload = json.loads(row["payload_json"])
        metadata = json.loads(row["metadata_json"])
        if _worker_marker(row["type"], row["actor_kind"], payload, metadata):
            return True
        queue.extend(
            identifier
            for identifier in _lineage_references(
                row["parent_event_id"], row["causation_id"], payload
            )
            if identifier not in seen
        )
    return False


def _worker_lineage(connection: sqlite3.Connection, event: Event) -> bool:
    if _worker_marker(
        event.type.value,
        event.actor.kind.value,
        event.payload,
        event.metadata,
    ):
        return True
    return any(
        _stored_event_has_worker_lineage(connection, identifier)
        for identifier in _lineage_references(
            event.parent_event_id,
            event.causation_id,
            event.payload,
        )
    )


class CoreProjection:
    """Project sessions, branches, claims, entities, motifs, and curation."""

    name = "core"
    tables = (
        "curation",
        "event_motifs",
        "motifs",
        "entities",
        "claim_occurrences",
        "claim_transitions",
        "branch_claim_states",
        "claims",
        "branches",
        "sessions",
    )

    def apply(self, connection: sqlite3.Connection, event: Event) -> None:
        self._project_session_and_branch(connection, event)
        if _synthetic_lineage(connection, event) or _worker_lineage(connection, event):
            return
        self._project_claim(connection, event)
        self._project_entity(connection, event)
        self._project_motif(connection, event)
        self._project_curation(connection, event)

    def _project_session_and_branch(self, connection: sqlite3.Connection, event: Event) -> None:
        if event.session_id is None:
            return
        payload = event.payload
        is_session_creation = (
            event.type is EventType.HUMAN_CHECKPOINT
            and payload.get("operation") == "session.created"
        )

        if event.type is EventType.SESSION_FORKED:
            new_branch_id = str(payload.get("branch_id") or event.branch_id or "")
            parent_branch_id = str(payload.get("parent_branch_id") or "")
            fork_event_id = str(payload.get("fork_event_id") or event.parent_event_id or "")
            if not new_branch_id or not parent_branch_id or not fork_event_id:
                raise ValueError("session.forked requires branch, parent branch, and fork event")
            connection.execute(
                """
                INSERT OR REPLACE INTO branches (
                    id, session_id, parent_branch_id, fork_event_id, title,
                    created_at, archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    new_branch_id,
                    event.session_id,
                    parent_branch_id,
                    fork_event_id,
                    payload.get("title"),
                    event.created_at.isoformat(),
                ),
            )
            self._inherit_claim_states(
                connection,
                new_branch_id=new_branch_id,
                parent_branch_id=parent_branch_id,
                fork_event_id=fork_event_id,
            )

        root_branch = str(payload.get("branch_id") or event.branch_id or "")
        connection.execute(
            """
            INSERT OR IGNORE INTO sessions (
                id, title, root_event_id, current_branch_id,
                model_profile_id, created_at, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                event.session_id,
                payload.get("title") if is_session_creation else None,
                event.id,
                root_branch or None,
                payload.get("model_profile_id") if is_session_creation else None,
                event.created_at.isoformat(),
            ),
        )
        if event.branch_id is not None and event.type is not EventType.SESSION_FORKED:
            connection.execute(
                """
                INSERT OR IGNORE INTO branches (
                    id, session_id, parent_branch_id, fork_event_id,
                    title, created_at, archived_at
                ) VALUES (?, ?, NULL, NULL, ?, ?, NULL)
                """,
                (
                    event.branch_id,
                    event.session_id,
                    payload.get("branch_title") if is_session_creation else None,
                    event.created_at.isoformat(),
                ),
            )
        current_branch = (
            str(payload.get("branch_id"))
            if event.type is EventType.SESSION_FORKED and payload.get("branch_id")
            else event.branch_id
        )
        if current_branch is not None:
            connection.execute(
                "UPDATE sessions SET current_branch_id = ? WHERE id = ?",
                (current_branch, event.session_id),
            )
        if is_session_creation:
            connection.execute(
                """
                UPDATE sessions
                SET title = COALESCE(?, title), model_profile_id = COALESCE(?, model_profile_id)
                WHERE id = ?
                """,
                (payload.get("title"), payload.get("model_profile_id"), event.session_id),
            )
        if event.type is EventType.BRANCH_ARCHIVED:
            archived_branch = str(payload.get("branch_id") or event.branch_id or "")
            connection.execute(
                "UPDATE branches SET archived_at = ? WHERE id = ?",
                (event.created_at.isoformat(), archived_branch),
            )
            open_branch = connection.execute(
                """
                SELECT 1 FROM branches
                WHERE session_id = ? AND archived_at IS NULL
                LIMIT 1
                """,
                (event.session_id,),
            ).fetchone()
            if open_branch is None:
                connection.execute(
                    "UPDATE sessions SET archived_at = ? WHERE id = ?",
                    (event.created_at.isoformat(), event.session_id),
                )

    def _project_claim(self, connection: sqlite3.Connection, event: Event) -> None:
        payload = event.payload
        if event.type is EventType.ANALYSIS_CLAIM_DETECTED:
            raw_claims = payload.get("claims")
            if isinstance(raw_claims, Sequence) and not isinstance(raw_claims, (str, bytes)):
                detected = list(raw_claims)
            elif raw_claims is not None:
                detected = [raw_claims]
            elif payload.get("raw_text") is not None or payload.get("text") is not None:
                detected = [payload]
            else:
                # A deterministic extractor may legitimately report no claims
                # while retaining numbers/entities in the same analysis event.
                detected = []
            for index, item in enumerate(detected):
                self._project_detected_claim(connection, event, item, index=index)
            return

        claim_id_value = payload.get("claim_id")
        if claim_id_value is None:
            return
        claim_id = str(claim_id_value)
        current = self._current_claim_status(connection, claim_id, event.branch_id)
        if current is None:
            raise ValueError(f"claim transition references unknown claim: {claim_id}")

        to_status: ClaimStatus | None = None
        if event.type is EventType.CLAIM_PROVISIONAL:
            to_status = ClaimStatus.PROVISIONAL
        elif event.type is EventType.CLAIM_OBSERVED:
            to_status = ClaimStatus.OBSERVED
        elif event.type is EventType.ANALYSIS_RECURRENCE_DETECTED:
            to_status = ClaimStatus.RECURRENT
        elif event.type is EventType.CLAIM_CONFLICTED:
            to_status = ClaimStatus.CONFLICTED
        elif event.type is EventType.CLAIM_SUPERSEDED:
            to_status = ClaimStatus.SUPERSEDED
        elif event.type is EventType.CLAIM_PROMOTED:
            to_status = (
                ClaimStatus(str(payload["to_status"]))
                if payload.get("to_status") is not None
                else _next_promotion(current)
            )
            if to_status is ClaimStatus.CANONICAL:
                self._require_human_canon_approval(connection, event, claim_id)
        elif event.type is EventType.CLAIM_DEMOTED:
            to_status = ClaimStatus(str(payload.get("to_status", ClaimStatus.PROVISIONAL.value)))
        elif event.type is EventType.HUMAN_KEEP:
            to_status = ClaimStatus.CANONICAL
        elif event.type is EventType.HUMAN_REJECT:
            to_status = ClaimStatus.REJECTED

        # analysis.canon_candidate is deliberately advisory and never mutates state.
        if to_status is not None:
            self._transition(
                connection,
                event,
                claim_id=claim_id,
                to_status=to_status,
                reason=str(payload.get("reason") or payload.get("rationale") or event.type.value),
                evidence=_event_ids(payload),
                confidence=_confidence(payload.get("confidence")),
            )

    @staticmethod
    def _require_human_canon_approval(
        connection: sqlite3.Connection,
        event: Event,
        claim_id: str,
    ) -> None:
        """Require one exact Human keep bound to this canon candidate."""

        approver_event_id = event.payload.get("approver_event_id")
        if not isinstance(approver_event_id, str) or not approver_event_id:
            raise ValueError("canonical promotion requires an explicit human approval event")
        row = connection.execute(
            """
            SELECT type, actor_kind, session_id, branch_id, parent_event_id,
                   causation_id, payload_json
            FROM events WHERE id = ?
            """,
            (approver_event_id,),
        ).fetchone()
        if row is None or row["type"] != EventType.HUMAN_KEEP.value:
            raise ValueError("canonical promotion approver must be a human.keep event")
        if row["actor_kind"] != ActorKind.HUMAN.value:
            raise ValueError("canonical promotion approver must have a human actor")
        approval_payload = json.loads(row["payload_json"])
        approved_claim_id = approval_payload.get("claim_id")
        if approved_claim_id != claim_id:
            raise ValueError("canonical promotion approval references another claim")
        candidate_event_id = approval_payload.get("candidate_event_id")
        candidate = connection.execute(
            """
            SELECT type, session_id, branch_id, payload_json
            FROM events WHERE id = ?
            """,
            (candidate_event_id,),
        ).fetchone()
        if candidate is None or candidate["type"] != EventType.ANALYSIS_CANON_CANDIDATE.value:
            raise ValueError("canonical promotion approval requires a canon candidate")
        candidate_payload = json.loads(candidate["payload_json"])
        if candidate_payload.get("claim_id") != claim_id:
            raise ValueError("canonical promotion candidate references another claim")
        if event.payload.get("candidate_event_id") != candidate_event_id:
            raise ValueError("canonical promotion references another candidate")
        if (
            approval_payload.get("target_event_id") != candidate_event_id
            or approval_payload.get("event_id") != candidate_event_id
            or row["parent_event_id"] != candidate_event_id
            or row["causation_id"] != candidate_event_id
        ):
            raise ValueError("canonical approval does not target its candidate")
        if (
            event.payload.get("source_event_id") != candidate_event_id
            or event.parent_event_id != candidate_event_id
            or event.causation_id != candidate_event_id
        ):
            raise ValueError("canonical promotion causal source is not its candidate")
        contexts = {
            (candidate["session_id"], candidate["branch_id"]),
            (row["session_id"], row["branch_id"]),
            (event.session_id, event.branch_id),
        }
        if len(contexts) != 1:
            raise ValueError("canonical approval, candidate, and promotion must share a branch")

    def _project_detected_claim(
        self,
        connection: sqlite3.Connection,
        event: Event,
        item: Any,
        *,
        index: int,
    ) -> None:
        payload = event.payload
        if isinstance(item, str):
            claim: dict[str, Any] = {"raw": item}
        elif isinstance(item, Mapping):
            claim = dict(item)
        else:
            raise ValueError("analysis.claim_detected claims must be strings or mappings")
        payload_claims = payload.get("claims")
        singleton_payload = not (
            isinstance(payload_claims, Sequence)
            and not isinstance(payload_claims, (str, bytes))
            and len(payload_claims) > 1
        )
        claim_id = str(
            claim.get("id")
            or claim.get("claim_id")
            or (payload.get("claim_id") if index == 0 and singleton_payload else None)
            or f"clm_{event.id.removeprefix('evt_')}_{index:03d}"
        )
        raw_text = (
            claim.get("raw")
            or claim.get("raw_text")
            or claim.get("text")
            or payload.get("raw_text")
            or payload.get("text")
        )
        if not isinstance(raw_text, str) or not raw_text:
            raise ValueError("each detected claim requires raw/raw_text/text")
        status = ClaimStatus(
            claim.get("status", payload.get("status", ClaimStatus.RAW_CLAIM.value))
        )
        confidence = _confidence(claim.get("confidence", payload.get("confidence")))
        branch_current = self._current_claim_status(connection, claim_id, event.branch_id)
        existed = connection.execute("SELECT 1 FROM claims WHERE id = ?", (claim_id,)).fetchone()
        source_event_id = (
            claim.get("source_event_id")
            or payload.get("source_event_id")
            or event.causation_id
            or event.id
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO claims (
                id, source_event_id, normalized_subject,
                normalized_predicate, normalized_object, raw_text,
                status, confidence, first_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id,
                source_event_id,
                claim.get("normalized_subject", claim.get("subject")),
                claim.get("normalized_predicate", claim.get("predicate")),
                _normalized_object(claim),
                raw_text,
                status.value,
                confidence,
                event.created_at.isoformat(),
            ),
        )
        relation = "reused" if existed else "introduced"
        connection.execute(
            """
            INSERT OR IGNORE INTO claim_occurrences (
                claim_id, event_id, branch_id, relation, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (claim_id, event.id, event.branch_id, relation, event.created_at.isoformat()),
        )
        explicit_status = claim.get("status") is not None or payload.get("status") is not None
        if branch_current is None or explicit_status:
            self._transition(
                connection,
                event,
                claim_id=claim_id,
                to_status=status,
                reason=str(payload.get("rationale") or "claim detected"),
                evidence=_event_ids(payload),
                confidence=confidence,
                initial=branch_current is None,
            )

    def _transition(
        self,
        connection: sqlite3.Connection,
        event: Event,
        *,
        claim_id: str,
        to_status: ClaimStatus,
        reason: str,
        evidence: list[str],
        confidence: float | None,
        initial: bool = False,
    ) -> None:
        current = (
            None if initial else self._current_claim_status(connection, claim_id, event.branch_id)
        )
        if current in _TERMINAL_CLAIM_STATES and current != to_status:
            raise ValueError(f"terminal claim {claim_id} cannot transition from {current}")
        connection.execute(
            """
            INSERT OR REPLACE INTO claim_transitions (
                event_id, claim_id, branch_id, from_status, to_status,
                reason, evidence_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                claim_id,
                event.branch_id,
                None if current is None else current.value,
                to_status.value,
                reason,
                json.dumps(evidence, sort_keys=True, separators=(",", ":")),
                event.created_at.isoformat(),
            ),
        )
        if event.branch_id is not None:
            connection.execute(
                """
                INSERT INTO branch_claim_states (
                    claim_id, branch_id, status, confidence, last_event_id,
                    inherited_from_branch_id
                ) VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(claim_id, branch_id) DO UPDATE SET
                    status = excluded.status,
                    confidence = COALESCE(excluded.confidence, branch_claim_states.confidence),
                    last_event_id = excluded.last_event_id,
                    inherited_from_branch_id = NULL
                """,
                (claim_id, event.branch_id, to_status.value, confidence, event.id),
            )
        connection.execute(
            """
            UPDATE claims
            SET status = ?, confidence = COALESCE(?, confidence)
            WHERE id = ?
            """,
            (to_status.value, confidence, claim_id),
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO claim_occurrences (
                claim_id, event_id, branch_id, relation, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (claim_id, event.id, event.branch_id, "transition", event.created_at.isoformat()),
        )

    @staticmethod
    def _current_claim_status(
        connection: sqlite3.Connection, claim_id: str, branch_id: str | None
    ) -> ClaimStatus | None:
        row = None
        if branch_id is not None:
            row = connection.execute(
                "SELECT status FROM branch_claim_states WHERE claim_id = ? AND branch_id = ?",
                (claim_id, branch_id),
            ).fetchone()
            return None if row is None else ClaimStatus(row["status"])
        if row is None:
            row = connection.execute(
                "SELECT status FROM claims WHERE id = ?", (claim_id,)
            ).fetchone()
        return None if row is None else ClaimStatus(row["status"])

    def _inherit_claim_states(
        self,
        connection: sqlite3.Connection,
        *,
        new_branch_id: str,
        parent_branch_id: str,
        fork_event_id: str,
    ) -> None:
        from oracle_lab.branching import visible_event_ids_from_connection

        visible = set(
            visible_event_ids_from_connection(
                connection, parent_branch_id, until_event_id=fork_event_id
            )
        )
        if not visible:
            return
        states: dict[str, sqlite3.Row] = {}
        for row in connection.execute(
            """
            SELECT t.*, c.source_event_id
            FROM claim_transitions t
            JOIN claims c ON c.id = t.claim_id
            ORDER BY t.created_at, t.event_id
            """
        ).fetchall():
            evidence = set(json.loads(row["evidence_json"]))
            if (
                row["event_id"] in visible
                or row["source_event_id"] in visible
                or evidence.intersection(visible)
            ):
                states[row["claim_id"]] = row
        for claim_id, row in states.items():
            connection.execute(
                """
                INSERT OR REPLACE INTO branch_claim_states (
                    claim_id, branch_id, status, confidence, last_event_id,
                    inherited_from_branch_id
                )
                SELECT ?, ?, ?, confidence, ?, ? FROM claims WHERE id = ?
                """,
                (
                    claim_id,
                    new_branch_id,
                    row["to_status"],
                    row["event_id"],
                    parent_branch_id,
                    claim_id,
                ),
            )

    @staticmethod
    def _project_entity(connection: sqlite3.Connection, event: Event) -> None:
        payload = event.payload
        if event.type not in {
            EventType.ANALYSIS_ENTITY_DETECTED,
            EventType.ENTITY_CREATED,
            EventType.ENTITY_UPDATED,
        }:
            return
        entity_id = str(payload.get("entity_id") or f"ent_{event.id.removeprefix('evt_')}")
        existing = connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        canonical_name = payload.get("canonical_name") or payload.get("name")
        if existing is None and not canonical_name:
            raise ValueError("new entity requires canonical_name")
        properties = thaw_json(payload.get("properties", {}))
        if existing is not None:
            merged = json.loads(existing["properties_json"])
            merged.update(properties)
            properties = merged
        connection.execute(
            """
            INSERT INTO entities (id, canonical_name, type, properties_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                canonical_name = COALESCE(excluded.canonical_name, entities.canonical_name),
                type = COALESCE(excluded.type, entities.type),
                properties_json = excluded.properties_json
            """,
            (
                entity_id,
                canonical_name or existing["canonical_name"],
                payload.get("entity_type") or payload.get("type"),
                json.dumps(properties, sort_keys=True, separators=(",", ":")),
            ),
        )

    @staticmethod
    def _project_motif(connection: sqlite3.Connection, event: Event) -> None:
        if event.type is not EventType.ANALYSIS_MOTIF_DETECTED:
            return
        payload = event.payload
        motif_id = str(payload.get("motif_id") or f"mot_{event.id.removeprefix('evt_')}")
        label = payload.get("label")
        if not isinstance(label, str) or not label:
            raise ValueError("analysis.motif_detected requires label")
        description = payload.get("description")
        if description is not None and not isinstance(description, str):
            raise ValueError("analysis.motif_detected description must be text")
        # Imports may append historical events out of timestamp order. Recompute
        # this shared motif from authoritative event order so live projection
        # state and a later full rebuild cannot select different labels/scores.
        candidates: list[tuple[sqlite3.Row, dict[str, Any]]] = []
        rows = connection.execute(
            """
            SELECT id, created_at, causation_id, payload_json
            FROM events
            WHERE type = ?
            ORDER BY created_at, id
            """,
            (EventType.ANALYSIS_MOTIF_DETECTED.value,),
        ).fetchall()
        for row in rows:
            if _stored_event_has_worker_lineage(connection, str(row["id"])):
                continue
            candidate_payload = json.loads(row["payload_json"])
            candidate_id = str(
                candidate_payload.get("motif_id") or f"mot_{str(row['id']).removeprefix('evt_')}"
            )
            if candidate_id == motif_id:
                candidates.append((row, candidate_payload))
        _, canonical_payload = candidates[0]
        canonical_label = canonical_payload.get("label")
        if not isinstance(canonical_label, str) or not canonical_label:
            raise ValueError("analysis.motif_detected requires label")
        canonical_description = canonical_payload.get("description")
        if canonical_description is not None and not isinstance(canonical_description, str):
            raise ValueError("analysis.motif_detected description must be text")
        embedding_text = "\n".join(
            item for item in (canonical_label, canonical_description) if item
        )
        connection.execute(
            """
            INSERT INTO motifs (id, label, description, embedding)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                label = excluded.label,
                description = excluded.description,
                embedding = excluded.embedding
            """,
            (
                motif_id,
                canonical_label,
                canonical_description,
                encode_local_embedding(embedding_text),
            ),
        )
        by_source: dict[str, Any] = {}
        for row, candidate_payload in candidates:
            source_event_id = (
                candidate_payload.get("source_event_id") or row["causation_id"] or row["id"]
            )
            by_source.setdefault(str(source_event_id), candidate_payload.get("score"))
        connection.execute("DELETE FROM event_motifs WHERE motif_id = ?", (motif_id,))
        connection.executemany(
            """
            INSERT INTO event_motifs (event_id, motif_id, score)
            VALUES (?, ?, ?)
            """,
            ((source_event_id, motif_id, score) for source_event_id, score in by_source.items()),
        )

    @staticmethod
    def _project_curation(connection: sqlite3.Connection, event: Event) -> None:
        actions = {
            EventType.HUMAN_KEEP: "keep",
            EventType.HUMAN_REJECT: "reject",
            EventType.HUMAN_STAR: "star",
            EventType.HUMAN_UNSTAR: "unstar",
            EventType.HUMAN_PIN: "pin",
            EventType.HUMAN_UNPIN: "unpin",
            EventType.HUMAN_QUARANTINE: "quarantine",
            EventType.HUMAN_REVISIT: "revisit",
        }
        action = actions.get(event.type)
        if action is None:
            return
        if event.actor.kind is not ActorKind.HUMAN:
            raise ValueError(f"{event.type.value} requires a human actor")
        if event.type in {EventType.HUMAN_PIN, EventType.HUMAN_UNPIN}:
            target = event.payload.get("claim_id") or event.payload.get("target_id")
        else:
            target = (
                event.payload.get("target_event_id")
                or event.payload.get("event_id")
                or event.payload.get("output_event_id")
                or event.parent_event_id
            )
        if target is None:
            raise ValueError(f"{event.type.value} requires an explicit target")
        connection.execute(
            """
            INSERT OR IGNORE INTO curation (
                event_id, action, note, created_at, action_event_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                target,
                action,
                event.payload.get("note"),
                event.created_at.isoformat(),
                event.id,
            ),
        )


class VirtualProjection:
    """Project branch-scoped virtual files, commands, and processes."""

    name = "virtual"
    tables = (
        "virtual_clock_contradictions",
        "virtual_clock_revisions",
        "virtual_clocks",
        "virtual_process_signals",
        "virtual_processes",
        "virtual_commands",
        "virtual_content_versions",
        "virtual_nodes",
    )

    def apply(self, connection: sqlite3.Connection, event: Event) -> None:
        if event.type is EventType.SESSION_FORKED:
            new_branch_id = str(event.payload.get("branch_id") or event.branch_id or "")
            parent_branch_id = str(event.payload.get("parent_branch_id") or "")
            fork_event_id = str(event.payload.get("fork_event_id") or event.parent_event_id or "")
            if new_branch_id and parent_branch_id and fork_event_id:
                from oracle_lab.branching import visible_event_ids_from_connection

                identifiers = visible_event_ids_from_connection(
                    connection, parent_branch_id, until_event_id=fork_event_id
                )
                for identifier in identifiers:
                    row = connection.execute(
                        "SELECT * FROM events WHERE id = ?", (identifier,)
                    ).fetchone()
                    if row is not None:
                        from oracle_lab.events import Actor

                        source = Event(
                            id=row["id"],
                            type=row["type"],
                            created_at=row["created_at"],
                            session_id=row["session_id"],
                            branch_id=row["branch_id"],
                            parent_event_id=row["parent_event_id"],
                            causation_id=row["causation_id"],
                            correlation_id=row["correlation_id"],
                            actor=Actor(kind=row["actor_kind"], id=row["actor_id"]),
                            payload=json.loads(row["payload_json"]),
                            metadata=json.loads(row["metadata_json"]),
                        )
                        self._apply_mutation(connection, source, branch_override=new_branch_id)
            return
        self._apply_mutation(connection, event, branch_override=None)

    def _apply_mutation(
        self,
        connection: sqlite3.Connection,
        event: Event,
        *,
        branch_override: str | None,
    ) -> None:
        branch_id = branch_override or event.branch_id
        if branch_id is None:
            return
        payload = event.payload
        sources = _event_ids(payload)
        if event.causation_id is not None:
            sources.append(event.causation_id)
        sources = list(dict.fromkeys(sources))

        if event.type is EventType.VIRTUAL_FILE_CREATED:
            node_value = payload.get("node", payload)
            if not isinstance(node_value, Mapping):
                raise ValueError("virtual_file.created requires node mapping")
            node = dict(node_value)
            path = node.get("path")
            if not isinstance(path, str) or not path.startswith("/"):
                raise ValueError("virtual node requires an absolute path")
            provenance = list(dict.fromkeys(str(item) for item in node.get("provenance", sources)))
            if not provenance:
                raise ValueError("virtual node requires provenance")
            connection.execute(
                """
                INSERT OR REPLACE INTO virtual_nodes (
                    branch_id, path, inode, kind, properties_json,
                    unresolved_json, provenance_json, last_mutation_event
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    branch_id,
                    path,
                    node.get("inode") or f"vino_{event.id.removeprefix('evt_')}",
                    node.get("kind", "file"),
                    _json(node.get("properties", {})),
                    _json(sorted(node.get("unresolved_fields", ()))),
                    _json(provenance),
                    event.id,
                ),
            )
            for version in node.get("content_versions", ()):
                if not isinstance(version, Mapping):
                    continue
                connection.execute(
                    """
                    INSERT OR REPLACE INTO virtual_content_versions (
                        branch_id, path, version, content,
                        source_event_ids_json, event_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        branch_id,
                        path,
                        int(version["version"]),
                        str(version.get("content", "")),
                        _json(version.get("source_event_ids", provenance)),
                        event.id,
                    ),
                )
            return

        if event.type is EventType.VIRTUAL_FILE_UPDATED:
            path = payload.get("path")
            if not isinstance(path, str):
                raise ValueError("virtual_file.updated requires path")
            row = connection.execute(
                "SELECT * FROM virtual_nodes WHERE branch_id = ? AND path = ?",
                (branch_id, path),
            ).fetchone()
            if row is None:
                raise ValueError(f"virtual node not found on branch {branch_id}: {path}")
            properties = json.loads(row["properties_json"])
            unresolved = set(json.loads(row["unresolved_json"]))
            detail = payload.get("synthesized_detail")
            if isinstance(detail, Mapping):
                field = str(detail.get("field"))
                properties[field] = thaw_json(detail.get("value"))
                unresolved.discard(field)
            provenance = list(dict.fromkeys((*json.loads(row["provenance_json"]), *sources)))
            connection.execute(
                """
                UPDATE virtual_nodes
                SET properties_json = ?, unresolved_json = ?,
                    provenance_json = ?, last_mutation_event = ?
                WHERE branch_id = ? AND path = ?
                """,
                (
                    _json(properties),
                    _json(sorted(unresolved)),
                    _json(provenance),
                    event.id,
                    branch_id,
                    path,
                ),
            )
            if payload.get("content") is not None:
                version = int(payload.get("version") or 1)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO virtual_content_versions (
                        branch_id, path, version, content,
                        source_event_ids_json, event_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        branch_id,
                        path,
                        version,
                        str(payload["content"]),
                        _json(sources or provenance),
                        event.id,
                    ),
                )
            return

        if (
            event.type is EventType.ENTITY_CREATED
            and payload.get("entity_kind") == "virtual_command"
        ):
            command = payload.get("command")
            if not isinstance(command, str) or not command:
                raise ValueError("virtual command entity requires command")
            provenance = sources or [str(payload.get("first_seen_event") or event.id)]
            connection.execute(
                """
                INSERT OR REPLACE INTO virtual_commands (
                    branch_id, command, version, first_seen_event,
                    known_options_json, provenance_json, last_mutation_event
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    branch_id,
                    command,
                    str(payload.get("version", "")),
                    str(payload.get("first_seen_event") or provenance[0]),
                    _json(payload.get("known_options", ())),
                    _json(provenance),
                    event.id,
                ),
            )
            return

        if event.type is EventType.VIRTUAL_PROCESS_CREATED:
            process_value = payload.get("process", payload)
            if not isinstance(process_value, Mapping):
                raise ValueError("virtual_process.created requires process mapping")
            process = dict(process_value)
            provenance = list(
                dict.fromkeys(str(item) for item in process.get("provenance", sources))
            )
            if not provenance:
                raise ValueError("virtual process requires provenance")
            connection.execute(
                """
                INSERT OR REPLACE INTO virtual_processes (
                    branch_id, pid, parent_pid, executable, args_json,
                    state, signals_json, provenance_json, callbacks_json,
                    last_mutation_event
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    branch_id,
                    int(process["pid"]),
                    process.get("parent_pid"),
                    str(process["executable"]),
                    _json(process.get("args", ())),
                    str(process.get("state", "running")),
                    _json(process.get("signals", ())),
                    _json(provenance),
                    _json(process.get("event_callbacks", {})),
                    event.id,
                ),
            )
            return

        if event.type is EventType.VIRTUAL_PROCESS_SIGNAL_RECEIVED:
            pid = int(payload["pid"])
            row = connection.execute(
                "SELECT * FROM virtual_processes WHERE branch_id = ? AND pid = ?",
                (branch_id, pid),
            ).fetchone()
            if row is None:
                raise ValueError(f"virtual PID not found on branch {branch_id}: {pid}")
            signal = str(payload["signal"])
            signals = [*json.loads(row["signals_json"]), signal]
            provenance = list(dict.fromkeys((*json.loads(row["provenance_json"]), *sources)))
            state = str(payload.get("state") or row["state"])
            connection.execute(
                """
                UPDATE virtual_processes
                SET state = ?, signals_json = ?, provenance_json = ?,
                    last_mutation_event = ?
                WHERE branch_id = ? AND pid = ?
                """,
                (state, _json(signals), _json(provenance), event.id, branch_id, pid),
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO virtual_process_signals (
                    branch_id, pid, event_id, signal, state, callback,
                    source_event_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    branch_id,
                    pid,
                    event.id,
                    signal,
                    state,
                    payload.get("callback"),
                    _json(sources),
                ),
            )
            return

        if event.type is EventType.VIRTUAL_CLOCK_CREATED:
            clock_value = payload.get("clock")
            if not isinstance(clock_value, Mapping):
                raise ValueError("virtual_clock.created requires clock mapping")
            clock = dict(clock_value)
            clock_id = clock.get("clock_id")
            if not isinstance(clock_id, str) or not clock_id:
                raise ValueError("virtual clock requires an ID")
            provenance = list(dict.fromkeys(str(item) for item in clock.get("provenance", sources)))
            if not provenance:
                raise ValueError("virtual clock requires provenance")
            connection.execute(
                """
                INSERT OR REPLACE INTO virtual_clocks (
                    branch_id, clock_id, current_revision, value, unit,
                    unresolved_json, provenance_json, last_mutation_event
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    branch_id,
                    clock_id,
                    clock.get("current_revision"),
                    clock.get("value"),
                    clock.get("unit"),
                    _json(clock.get("unresolved_fields", ("unit", "value"))),
                    _json(provenance),
                    event.id,
                ),
            )
            return

        if event.type in {EventType.VIRTUAL_CLOCK_SET, EventType.VIRTUAL_CLOCK_ADVANCED}:
            clock_id = payload.get("clock_id")
            revision_value = payload.get("revision")
            if not isinstance(clock_id, str) or not isinstance(revision_value, Mapping):
                raise ValueError(f"{event.type.value} requires clock ID and revision")
            row = connection.execute(
                "SELECT * FROM virtual_clocks WHERE branch_id = ? AND clock_id = ?",
                (branch_id, clock_id),
            ).fetchone()
            if row is None:
                raise ValueError(f"virtual clock not found on branch {branch_id}: {clock_id}")
            revision = dict(revision_value)
            revision_sources = [str(item) for item in revision.get("source_event_ids", sources)]
            connection.execute(
                """
                INSERT OR REPLACE INTO virtual_clock_revisions (
                    branch_id, clock_id, revision, operation, value, unit,
                    delta, source_event_ids_json, event_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    branch_id,
                    clock_id,
                    int(revision["revision"]),
                    str(revision["operation"]),
                    str(revision["value"]),
                    str(revision["unit"]),
                    revision.get("delta"),
                    _json(revision_sources),
                    event.id,
                ),
            )
            provenance = list(dict.fromkeys((*json.loads(row["provenance_json"]), *sources)))
            connection.execute(
                """
                UPDATE virtual_clocks
                SET current_revision = ?, value = ?, unit = ?,
                    unresolved_json = ?, provenance_json = ?, last_mutation_event = ?
                WHERE branch_id = ? AND clock_id = ?
                """,
                (
                    int(revision["revision"]),
                    str(revision["value"]),
                    str(revision["unit"]),
                    _json(payload.get("unresolved_fields", ())),
                    _json(provenance),
                    event.id,
                    branch_id,
                    clock_id,
                ),
            )
            return

        if event.type is EventType.VIRTUAL_CLOCK_CONTRADICTION_DETECTED:
            clock_id = payload.get("clock_id")
            prior = payload.get("prior_reading")
            conflicting = payload.get("conflicting_reading")
            if (
                not isinstance(clock_id, str)
                or not isinstance(prior, Mapping)
                or not isinstance(conflicting, Mapping)
            ):
                raise ValueError("virtual_clock.contradiction_detected requires two cited readings")
            row = connection.execute(
                "SELECT 1 FROM virtual_clocks WHERE branch_id = ? AND clock_id = ?",
                (branch_id, clock_id),
            ).fetchone()
            if row is None:
                raise ValueError(f"virtual clock not found on branch {branch_id}: {clock_id}")
            connection.execute(
                """
                INSERT OR REPLACE INTO virtual_clock_contradictions (
                    branch_id, clock_id, event_id, prior_revision,
                    conflicting_revision, status, source_event_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    branch_id,
                    clock_id,
                    event.id,
                    int(prior["revision"]),
                    int(conflicting["revision"]),
                    str(payload.get("status", "unresolved")),
                    _json(sources),
                ),
            )


class VirtualStateService:
    """Bridge event-backed virtual projections and the in-memory runtime."""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def mutation_sink(
        self,
        *,
        session_id: str,
        branch_id: str,
        actor: Any | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """Return a sink that turns VirtualMutation values into events."""
        from oracle_lab.events import Actor, ActorKind

        event_actor = actor or Actor(kind=ActorKind.HOST, id="virtual-runtime")

        def persist(mutation: Any) -> None:
            source_ids = tuple(str(item) for item in mutation.source_event_ids)
            if not source_ids:
                raise ValueError("virtual mutation requires source event IDs")
            for source_id in source_ids:
                self.store.require(source_id)
            source = self.store.require(source_ids[-1])
            branch_events = self.store.list_events(branch_id=branch_id)
            parent = branch_events[-1] if branch_events else source
            payload = {**dict(mutation.payload), "source_event_ids": list(source_ids)}
            self.store.append(
                Event(
                    type=mutation.event_type,
                    actor=event_actor,
                    session_id=session_id,
                    branch_id=branch_id,
                    parent_event_id=parent.id,
                    causation_id=source.id,
                    correlation_id=correlation_id or source.correlation_id,
                    payload=payload,
                    metadata={
                        "schema_version": 1,
                        **(
                            {"truth_domain": "virtual"}
                            if payload.get("truth_domain") == "virtual"
                            else {}
                        ),
                    },
                )
            )

        return persist

    def snapshot(self, branch_id: str) -> dict[str, Any]:
        """Return a VirtualFileSystem-compatible snapshot from projections."""
        nodes: list[dict[str, Any]] = []
        rows = self.store.connection.execute(
            "SELECT * FROM virtual_nodes WHERE branch_id = ? ORDER BY path",
            (branch_id,),
        ).fetchall()
        for row in rows:
            versions = self.store.connection.execute(
                """
                SELECT * FROM virtual_content_versions
                WHERE branch_id = ? AND path = ? ORDER BY version
                """,
                (branch_id, row["path"]),
            ).fetchall()
            nodes.append(
                {
                    "path": row["path"],
                    "inode": row["inode"],
                    "kind": row["kind"],
                    "provenance": json.loads(row["provenance_json"]),
                    "content_versions": [
                        {
                            "version": version["version"],
                            "content": version["content"],
                            "source_event_ids": json.loads(version["source_event_ids_json"]),
                        }
                        for version in versions
                    ],
                    "properties": json.loads(row["properties_json"]),
                    "unresolved_fields": json.loads(row["unresolved_json"]),
                    "last_mutation_event": row["last_mutation_event"],
                }
            )
        return {"nodes": nodes}

    def hydrate(self, branch_id: str, *, mutation_sink: Any | None = None) -> Any:
        """Reconstruct a VirtualWorldRuntime after restart or replay."""
        from oracle_lab.virtual import (
            SourceEvidence,
            VirtualClock,
            VirtualClockContradiction,
            VirtualClockRevision,
            VirtualFileSystem,
            VirtualWorldRuntime,
        )

        runtime = VirtualWorldRuntime()
        runtime.fs = VirtualFileSystem.from_snapshot(self.snapshot(branch_id))
        command_rows = self.store.connection.execute(
            "SELECT * FROM virtual_commands WHERE branch_id = ? ORDER BY command",
            (branch_id,),
        ).fetchall()
        for row in command_rows:
            provenance = tuple(json.loads(row["provenance_json"]))
            runtime.commands.register(
                row["command"],
                row["version"],
                json.loads(row["known_options_json"]),
                evidence=SourceEvidence(provenance, "explicit"),
            )
        process_rows = self.store.connection.execute(
            "SELECT * FROM virtual_processes WHERE branch_id = ? ORDER BY pid",
            (branch_id,),
        ).fetchall()
        pending = list(process_rows)
        while pending:
            progressed = False
            for row in pending[:]:
                parent_pid = row["parent_pid"]
                if parent_pid is not None and parent_pid not in runtime.processes.processes:
                    continue
                provenance = tuple(json.loads(row["provenance_json"]))
                process = runtime.processes.create(
                    row["executable"],
                    json.loads(row["args_json"]),
                    evidence=SourceEvidence(provenance, "explicit"),
                    parent_pid=parent_pid,
                    state=row["state"],
                    event_callbacks=json.loads(row["callbacks_json"]),
                    pid=row["pid"],
                )
                process.signals[:] = json.loads(row["signals_json"])
                process.provenance[:] = list(provenance)
                pending.remove(row)
                progressed = True
            if not progressed:
                raise ValueError("virtual process projection contains unresolved parent PID")
        clock_rows = self.store.connection.execute(
            "SELECT * FROM virtual_clocks WHERE branch_id = ? ORDER BY clock_id",
            (branch_id,),
        ).fetchall()
        for row in clock_rows:
            revision_rows = self.store.connection.execute(
                """
                SELECT * FROM virtual_clock_revisions
                WHERE branch_id = ? AND clock_id = ? ORDER BY revision
                """,
                (branch_id, row["clock_id"]),
            ).fetchall()
            contradiction_rows = self.store.connection.execute(
                """
                SELECT * FROM virtual_clock_contradictions
                WHERE branch_id = ? AND clock_id = ? ORDER BY event_id
                """,
                (branch_id, row["clock_id"]),
            ).fetchall()
            runtime.clocks._clocks[row["clock_id"]] = VirtualClock(
                clock_id=row["clock_id"],
                provenance=json.loads(row["provenance_json"]),
                unresolved_fields=set(json.loads(row["unresolved_json"])),
                revisions=[
                    VirtualClockRevision(
                        revision=revision["revision"],
                        operation=revision["operation"],
                        value=revision["value"],
                        unit=revision["unit"],
                        delta=revision["delta"],
                        source_event_ids=tuple(json.loads(revision["source_event_ids_json"])),
                    )
                    for revision in revision_rows
                ],
                contradictions=[
                    VirtualClockContradiction(
                        prior_revision=conflict["prior_revision"],
                        conflicting_revision=conflict["conflicting_revision"],
                        source_event_ids=tuple(json.loads(conflict["source_event_ids_json"])),
                    )
                    for conflict in contradiction_rows
                ],
                last_mutation_event=row["last_mutation_event"],
            )
        if mutation_sink is not None:
            runtime.fs._sink = mutation_sink
            runtime.commands._sink = mutation_sink
            runtime.processes._sink = mutation_sink
            runtime.clocks._sink = mutation_sink
        return runtime


class ProjectionManager:
    """Apply idempotent projections or rebuild all derived state."""

    def __init__(self, store: EventStore, projections: Iterable[Projection] | None = None) -> None:
        self.store = store
        self.projections = tuple(projections or default_projections())
        names = [projection.name for projection in self.projections]
        if len(names) != len(set(names)):
            raise ValueError("projection names must be unique")

    def project(self, events: Iterable[Event]) -> None:
        """Apply each event exactly once per projection plugin."""
        for event in events:
            for projection in self.projections:
                with self.store.transaction() as connection:
                    applied = connection.execute(
                        """
                        SELECT 1 FROM projection_applied
                        WHERE projection_name = ? AND event_id = ?
                        """,
                        (projection.name, event.id),
                    ).fetchone()
                    if applied is not None:
                        continue
                    try:
                        projection.apply(connection, event)
                    except Exception as error:
                        raise ProjectionError(
                            f"projection {projection.name!r} failed on {event.id}: {error}"
                        ) from error
                    connection.execute(
                        """
                        INSERT INTO projection_applied (projection_name, event_id, applied_at)
                        VALUES (?, ?, ?)
                        """,
                        (projection.name, event.id, dt.datetime.now(dt.UTC).isoformat()),
                    )

    def rebuild(self) -> None:
        """Clear projection tables and replay every authoritative event."""
        tables: list[str] = []
        for projection in self.projections:
            tables.extend(projection.tables)
        with self.store.transaction() as connection:
            connection.execute("DELETE FROM projection_applied")
            for table in dict.fromkeys(tables):
                if not table.replace("_", "").isalnum():
                    raise ValueError(f"unsafe projection table name: {table}")
                connection.execute(f"DELETE FROM {table}")
        self.project(self.store.list_events())

    def applied_count(self, projection_name: str) -> int:
        """Return the number of events consumed by a projection."""
        row = self.store.connection.execute(
            "SELECT COUNT(*) FROM projection_applied WHERE projection_name = ?",
            (projection_name,),
        ).fetchone()
        return int(row[0])


def default_projections() -> tuple[Projection, ...]:
    """Return fresh default projection plugins in dependency order."""
    from oracle_lab.jobs import JobProjection
    from oracle_lab.provenance import ProvenanceProjection
    from oracle_lab.sampling import SamplingProjection
    from oracle_lab.usage import UsageProjection
    from oracle_lab.worker_projection import WorkerProjection

    return (
        CoreProjection(),
        VirtualProjection(),
        SamplingProjection(),
        UsageProjection(),
        JobProjection(),
        WorkerProjection(),
        ProvenanceProjection(),
    )


def _confidence(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not 0 <= result <= 1:
        raise ValueError("confidence must be between zero and one")
    return result


def _event_ids(payload: Any) -> list[str]:
    result: list[str] = []
    for key in ("source_event_id", "source_event_ids", "evidence_event_ids", "provenance"):
        value = payload.get(key)
        if isinstance(value, str):
            result.append(value)
        elif isinstance(value, Sequence):
            result.extend(str(item) for item in value)
    return list(dict.fromkeys(result))


def _next_promotion(current: ClaimStatus) -> ClaimStatus:
    if current is ClaimStatus.CONFLICTED:
        return ClaimStatus.LAW_CANDIDATE
    try:
        index = _PROMOTION_ORDER.index(current)
    except ValueError as error:
        raise ValueError(f"claim cannot be promoted from {current}") from error
    return _PROMOTION_ORDER[min(index + 1, len(_PROMOTION_ORDER) - 1)]


def _normalized_object(claim: Mapping[str, Any]) -> str | None:
    value = claim.get("normalized_object", claim.get("object"))
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> str:
    return json.dumps(
        thaw_json(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = [
    "ClaimStatus",
    "CoreProjection",
    "Projection",
    "ProjectionError",
    "ProjectionManager",
    "VirtualProjection",
    "VirtualStateService",
    "default_projections",
]
