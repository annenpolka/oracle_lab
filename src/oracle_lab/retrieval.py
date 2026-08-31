"""Targeted exact and local lexical-vector retrieval.

The semantic index is intentionally local and dependency-free.  It builds a
TF-IDF vector from word and Unicode character n-grams, which works for both
space-delimited text and Japanese oracle output.  Only the document text is
embedded; provider headers, credentials, and other private metadata never enter
the vector index.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import math
import re
import struct
import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

_WORD_RE = re.compile(r"[\w./:=°+\-]+", re.UNICODE)
LOCAL_EMBEDDING_DIMENSIONS = 128
_LOCAL_EMBEDDING_MAGIC = b"OLM1"


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        if isinstance(result, Mapping):
            return dict(result)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        result = model_dump(mode="python")
        if isinstance(result, Mapping):
            return dict(result)
    data = getattr(value, "__dict__", None)
    if isinstance(data, Mapping):
        return dict(data)
    raise TypeError(f"cannot convert {type(value).__name__} to a retrieval document")


def _first_text(payload: Mapping[str, Any], event: Mapping[str, Any]) -> str:
    for key in (
        "raw_text",
        "text",
        "content",
        "output",
        "note",
        "claim",
        "motif",
        "canonical_name",
        "label",
    ):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    for key in ("raw_text", "text", "content"):
        value = event.get(key)
        if isinstance(value, str):
            return value
    return ""


def _tuple_strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(item for item in value if isinstance(item, str))
    return ()


@dataclass(frozen=True, slots=True)
class RetrievalDocument:
    """A minimal searchable projection with an explicit source identifier."""

    id: str
    text: str
    kind: str = "event"
    session_id: str | None = None
    branch_id: str | None = None
    created_at: dt.datetime | None = None
    lineage: tuple[str, ...] = ()
    claims: tuple[str, ...] = ()
    entities: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict, compare=False)
    embedding: tuple[float, ...] | None = field(default=None, compare=False)

    @classmethod
    def from_event(cls, event: Any) -> RetrievalDocument:
        value = _as_mapping(event)
        payload_value = value.get("payload", {})
        payload = dict(payload_value) if isinstance(payload_value, Mapping) else {}
        metadata_value = value.get("metadata", {})
        metadata = dict(metadata_value) if isinstance(metadata_value, Mapping) else {}
        event_id = value.get("id") or value.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("retrieval documents require an event ID")
        kind = str(value.get("type") or value.get("kind") or "event")
        text = _first_text(payload, value)
        claims = _tuple_strings(payload.get("claims") or metadata.get("claims"))
        entities = _tuple_strings(
            payload.get("entities") or payload.get("canonical_name") or metadata.get("entities")
        )
        if not claims and kind in {
            "claim",
            "analysis.claim_detected",
            "claim.provisional",
        }:
            claims = (text,) if text else ()
        lineage = _tuple_strings(payload.get("lineage") or metadata.get("lineage"))
        created_at_value = value.get("created_at")
        created_at: dt.datetime | None = None
        if isinstance(created_at_value, dt.datetime):
            created_at = created_at_value
        elif isinstance(created_at_value, str):
            try:
                created_at = dt.datetime.fromisoformat(created_at_value.replace("Z", "+00:00"))
            except ValueError:
                created_at = None
        return cls(
            id=event_id,
            text=text,
            kind=kind,
            session_id=value.get("session_id"),
            branch_id=value.get("branch_id"),
            created_at=created_at,
            lineage=lineage,
            claims=claims,
            entities=entities,
            metadata=metadata,
        )

    @classmethod
    def from_motif(cls, motif: Any) -> RetrievalDocument:
        """Build a semantic document from the public motif projection fields."""
        value = _as_mapping(motif)
        motif_id = value.get("id") or value.get("motif_id")
        label = value.get("label")
        if not isinstance(motif_id, str) or not motif_id:
            raise ValueError("motif retrieval documents require an ID")
        if not isinstance(label, str) or not label:
            raise ValueError("motif retrieval documents require a label")
        description = value.get("description")
        text = "\n".join(
            item for item in (label, description if isinstance(description, str) else None) if item
        )
        raw_embedding = value.get("embedding")
        embedding = (
            decode_local_embedding(raw_embedding)
            if isinstance(raw_embedding, (bytes, bytearray, memoryview))
            else local_embedding(text)
        )
        created_at_value = value.get("created_at")
        created_at: dt.datetime | None = None
        if isinstance(created_at_value, dt.datetime):
            created_at = created_at_value
        elif isinstance(created_at_value, str):
            try:
                created_at = dt.datetime.fromisoformat(created_at_value.replace("Z", "+00:00"))
            except ValueError:
                created_at = None
        raw_source_ids = value.get("source_event_ids", ())
        source_event_ids = (
            (raw_source_ids,)
            if isinstance(raw_source_ids, str)
            else tuple(item for item in raw_source_ids if isinstance(item, str))
            if isinstance(raw_source_ids, Sequence)
            else ()
        )
        source_event_id = value.get("source_event_id")
        if isinstance(source_event_id, str):
            source_event_ids = tuple(dict.fromkeys((source_event_id, *source_event_ids)))
        metadata = {
            "source_event_id": source_event_ids[0] if source_event_ids else None,
            "source_event_ids": source_event_ids,
        }
        return cls(
            id=motif_id,
            text=text,
            kind="motif",
            session_id=value.get("session_id"),
            branch_id=value.get("branch_id"),
            created_at=created_at,
            metadata=metadata,
            embedding=embedding,
        )


@dataclass(frozen=True, slots=True)
class SearchHit:
    document: RetrievalDocument
    score: float
    matched_by: str

    @property
    def event_id(self) -> str:
        return self.document.id


def _normal_text(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


def _features(value: str) -> Counter[str]:
    normalized = _normal_text(value)
    counts: Counter[str] = Counter(f"w:{token}" for token in _WORD_RE.findall(normalized))
    compact = "".join(character for character in normalized if not character.isspace())
    for width in (2, 3):
        counts.update(
            f"c{width}:{compact[index : index + width]}"
            for index in range(max(0, len(compact) - width + 1))
        )
    return counts


def local_embedding(
    value: str, *, dimensions: int = LOCAL_EMBEDDING_DIMENSIONS
) -> tuple[float, ...]:
    """Return a deterministic dependency-free feature-hashed text vector."""
    if dimensions < 1:
        raise ValueError("embedding dimensions must be positive")
    vector = [0.0] * dimensions
    for feature, count in sorted(_features(value).items()):
        digest = hashlib.sha256(feature.encode("utf-8")).digest()
        index = int.from_bytes(digest[:8], "big") % dimensions
        vector[index] += 1 + math.log(count)
    norm = math.sqrt(sum(item * item for item in vector))
    if norm:
        vector = [item / norm for item in vector]
    return tuple(vector)


def encode_local_embedding(value: str) -> bytes:
    """Encode the stable local embedding as a self-describing SQLite BLOB."""
    vector = local_embedding(value)
    return (
        _LOCAL_EMBEDDING_MAGIC
        + struct.pack(">H", len(vector))
        + struct.pack(f">{len(vector)}f", *vector)
    )


def decode_local_embedding(value: bytes | bytearray | memoryview) -> tuple[float, ...]:
    """Decode and validate a local embedding BLOB."""
    raw = bytes(value)
    if len(raw) < 6 or raw[:4] != _LOCAL_EMBEDDING_MAGIC:
        raise ValueError("invalid local embedding header")
    dimensions = struct.unpack(">H", raw[4:6])[0]
    expected = 6 + dimensions * 4
    if dimensions < 1 or len(raw) != expected:
        raise ValueError("invalid local embedding length")
    return tuple(struct.unpack(f">{dimensions}f", raw[6:]))


def _weighted_vector(
    counts: Counter[str], document_frequency: Counter[str], total: int
) -> dict[str, float]:
    vector: dict[str, float] = {}
    for feature, count in counts.items():
        inverse_document_frequency = math.log((total + 1) / (document_frequency[feature] + 1)) + 1
        vector[feature] = (1 + math.log(count)) * inverse_document_frequency
    return vector


def _cosine(left: Mapping[str, float], right: Mapping[str, float]) -> float:
    if not left or not right:
        return 0.0
    if len(left) > len(right):
        left, right = right, left
    dot = sum(value * right.get(feature, 0.0) for feature, value in left.items())
    if dot == 0:
        return 0.0
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    return dot / (left_norm * right_norm)


def _dense_cosine(left: Sequence[float], right: Sequence[float]) -> float:
    if not left or len(left) != len(right):
        return 0.0
    dot = sum(left_value * right_value for left_value, right_value in zip(left, right, strict=True))
    if dot == 0:
        return 0.0
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    return dot / (left_norm * right_norm)


class RetrievalIndex:
    """An in-process projection supporting exact and semantic retrieval."""

    def __init__(self, documents: Iterable[RetrievalDocument] = ()) -> None:
        self._documents: dict[str, RetrievalDocument] = {}
        for document in documents:
            self.add(document)

    @classmethod
    def from_events(cls, events: Iterable[Any]) -> RetrievalIndex:
        return cls(RetrievalDocument.from_event(event) for event in events)

    def add(self, document: RetrievalDocument) -> None:
        if not document.id:
            raise ValueError("retrieval documents require a non-empty ID")
        self._documents[document.id] = document

    def remove(self, document_id: str) -> None:
        self._documents.pop(document_id, None)

    def get(self, event_id: str) -> RetrievalDocument | None:
        return self._documents.get(event_id)

    def by_event_id(self, event_id: str) -> list[SearchHit]:
        document = self.get(event_id)
        return [] if document is None else [SearchHit(document, 1.0, "event_id")]

    def by_claim(self, claim: str, *, case_sensitive: bool = False) -> list[SearchHit]:
        expected = claim if case_sensitive else _normal_text(claim)

        def matches(document: RetrievalDocument) -> bool:
            candidates = document.claims
            if document.kind in {"claim", "analysis.claim_detected", "claim.provisional"}:
                candidates += (document.text,)
            values = (
                candidates if case_sensitive else tuple(_normal_text(item) for item in candidates)
            )
            return expected in values

        return self._exact_hits(matches, "claim")

    def by_entity(self, entity: str, *, case_sensitive: bool = False) -> list[SearchHit]:
        expected = entity if case_sensitive else _normal_text(entity)

        def matches(document: RetrievalDocument) -> bool:
            candidates = document.entities
            if document.kind in {"entity", "analysis.entity_detected", "entity.created"}:
                candidates += (document.text,)
            values = (
                candidates if case_sensitive else tuple(_normal_text(item) for item in candidates)
            )
            return expected in values

        return self._exact_hits(matches, "entity")

    def by_text_substring(
        self,
        substring: str,
        *,
        case_sensitive: bool = True,
        session_id: str | None = None,
    ) -> list[SearchHit]:
        if not substring:
            raise ValueError("substring must not be empty")
        needle = substring if case_sensitive else _normal_text(substring)

        def matches(document: RetrievalDocument) -> bool:
            if session_id is not None and document.session_id != session_id:
                return False
            haystack = document.text if case_sensitive else _normal_text(document.text)
            return needle in haystack

        return self._exact_hits(matches, "text_substring")

    def by_session_lineage(self, session_id: str) -> list[SearchHit]:
        return self._exact_hits(
            lambda document: document.session_id == session_id or session_id in document.lineage,
            "session_lineage",
        )

    def semantic_search(
        self,
        query: str,
        *,
        limit: int = 10,
        kinds: Iterable[str] | None = None,
        session_id: str | None = None,
        min_score: float = 0.0,
    ) -> list[SearchHit]:
        """Rank local text vectors; no metadata is sent to an external model."""
        if not query.strip():
            raise ValueError("semantic query must not be empty")
        if limit < 1:
            raise ValueError("limit must be positive")
        allowed_kinds = set(kinds) if kinds is not None else None
        documents = [
            document
            for document in self._documents.values()
            if (session_id is None or document.session_id == session_id)
            and (allowed_kinds is None or document.kind in allowed_kinds)
            and document.text
        ]
        if not documents:
            return []
        feature_counts = {document.id: _features(document.text) for document in documents}
        document_frequency: Counter[str] = Counter()
        for counts in feature_counts.values():
            document_frequency.update(counts.keys())
        total = len(documents)
        query_vector = _weighted_vector(_features(query), document_frequency, total)
        local_query_vector = local_embedding(query)
        hits = []
        for document in documents:
            if document.embedding is not None:
                score = _dense_cosine(local_query_vector, document.embedding)
                matched_by = "semantic_local_embedding"
            else:
                document_vector = _weighted_vector(
                    feature_counts[document.id], document_frequency, total
                )
                score = _cosine(query_vector, document_vector)
                matched_by = "semantic_lexical_vector"
            if score > min_score:
                hits.append(SearchHit(document, score, matched_by))
        return sorted(hits, key=lambda hit: (-hit.score, hit.event_id))[:limit]

    def all_documents(self) -> tuple[RetrievalDocument, ...]:
        return tuple(self._documents.values())

    def _exact_hits(self, predicate: Any, matched_by: str) -> list[SearchHit]:
        latest = dt.datetime.max.replace(tzinfo=dt.UTC)
        documents = sorted(
            self._documents.values(),
            key=lambda document: (
                document.created_at.astimezone(dt.UTC)
                if document.created_at is not None
                and document.created_at.tzinfo is not None
                and document.created_at.utcoffset() is not None
                else latest,
                document.id,
            ),
        )
        return [
            SearchHit(document, 1.0, matched_by) for document in documents if predicate(document)
        ]


__all__ = [
    "LOCAL_EMBEDDING_DIMENSIONS",
    "RetrievalDocument",
    "RetrievalIndex",
    "SearchHit",
    "decode_local_embedding",
    "encode_local_embedding",
    "local_embedding",
]
