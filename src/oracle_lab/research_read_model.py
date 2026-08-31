"""Read-only catalog queries for research projections and analysis events."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from oracle_lab.events import Event, EventType
from oracle_lab.jsonutil import sha256_text
from oracle_lab.material import is_synthetic_lineage
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


class ResearchCatalogReadModel:
    """Build global catalogs and explicit-session research views without mutation."""

    __slots__ = ("_store",)

    def __init__(self, store: EventStore) -> None:
        self._store = store

    def _rows(self, sql: str, parameters: Sequence[Any] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self._store.connection.execute(sql, parameters)]

    def claims(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM claims ORDER BY first_seen_at, id")

    def contradictions(self) -> list[dict[str, Any]]:
        return [
            event.to_dict()
            for event in self._store.list_events(
                event_type=[
                    EventType.ANALYSIS_CONTRADICTION_DETECTED,
                    EventType.ANALYSIS_NUMERIC_INCONSISTENCY,
                ]
            )
            if not is_synthetic_lineage(event, self._store.get)
        ]

    def motifs(self) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT id, label, description, length(embedding) AS embedding_bytes "
            "FROM motifs ORDER BY id"
        )

    def attractors(self) -> list[dict[str, Any]]:
        return [
            event.to_dict()
            for event in self._store.list_events(
                event_type=EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED
            )
            if not is_synthetic_lineage(event, self._store.get)
        ]

    def prompt_attractor_statistics(
        self,
        session_id: str,
        *,
        phrase: str | None = None,
    ) -> dict[str, Any]:
        """Relate exact input wording to observed output-format attractors."""
        events = [
            event
            for event in self._store.list_events(session_id=session_id)
            if not is_synthetic_lineage(event, self._store.get)
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

    def words_before_latex_attractors(
        self,
        session_id: str,
        *,
        word_count: int = 5,
    ) -> list[dict[str, Any]]:
        """Return lexical windows immediately preceding detected LaTeX notation."""
        results: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        attractors = self._store.list_events(
            session_id=session_id,
            event_type=EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED,
        )
        for attractor in attractors:
            if is_synthetic_lineage(attractor, self._store.get):
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
                source = self._store.require(source_id)
                if is_synthetic_lineage(source, self._store.get):
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

    def search(
        self,
        session_id: str,
        query: str,
        *,
        semantic: bool = False,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        events = [
            event
            for event in self._store.list_events(session_id=session_id)
            if not is_synthetic_lineage(event, self._store.get)
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


__all__ = ["ResearchCatalogReadModel"]
