from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jsonutil import sha256_text
from oracle_lab.research_read_model import ResearchCatalogReadModel
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventStore
from tests.support import historical_oracle_fixture

CONFIG = Path(__file__).parents[1] / "config"
HISTORICAL_FIXTURE = Path(__file__).parent / "fixtures" / "oracle_output_001.md"


def _service(tmp_path: Path) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )


def _append_catalog(
    service: OracleLabService,
    session: dict[str, Any],
    suffix: str,
) -> tuple[Event, Event, Event, Event]:
    root = service.store.require(str(session["root_event_id"]))
    common = {
        "actor": Actor(kind=ActorKind.HOST, id=f"catalog-{suffix}"),
        "session_id": root.session_id,
        "branch_id": root.branch_id,
        "parent_event_id": root.id,
        "causation_id": root.id,
        "correlation_id": root.correlation_id,
    }
    claim = Event.new(
        EventType.ANALYSIS_CLAIM_DETECTED,
        **common,
        payload={
            "claims": [
                {
                    "id": f"clm_{suffix}",
                    "raw": f"claim {suffix}",
                    "source_event_id": root.id,
                }
            ],
            "source_event_ids": [root.id],
        },
    )
    motif = Event.new(
        EventType.ANALYSIS_MOTIF_DETECTED,
        **common,
        payload={
            "motif_id": f"mot_{suffix}",
            "label": f"motif {suffix}",
            "description": f"description {suffix}",
            "source_event_id": root.id,
            "source_event_ids": [root.id],
        },
    )
    contradiction = Event.new(
        EventType.ANALYSIS_CONTRADICTION_DETECTED,
        **common,
        payload={"kind": "semantic", "source_event_ids": [root.id]},
    )
    attractor = Event.new(
        EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED,
        **common,
        payload={
            "attractor": f"format_{suffix}",
            "markers": [f"marker-{suffix}"],
            "source_event_ids": [root.id],
        },
    )
    appended = service.store.append_many((claim, motif, contradiction, attractor))
    return appended[0], appended[1], appended[2], appended[3]


def _append_session_queries(
    service: OracleLabService,
    session: dict[str, Any],
    suffix: str,
) -> dict[str, Any]:
    root = service.store.require(str(session["root_event_id"]))
    phrase = f"phrase%_[{suffix}]"
    prompt_text = f"  preserve {phrase}\nexactly  "
    output_text = f"origin-{suffix} Literal%_[ alpha beta gamma $$ x^2 $$"
    prompt = service.store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id=f"query-{suffix}"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            created_at=root.created_at + dt.timedelta(seconds=1),
            payload={"text": prompt_text, "content": prompt_text, "role": "user"},
        )
    )
    output = service.store.append(
        historical_oracle_fixture(
            output_text,
            source_path=HISTORICAL_FIXTURE,
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=prompt.id,
            causation_id=prompt.id,
            correlation_id=root.correlation_id,
            created_at=root.created_at + dt.timedelta(seconds=2),
        )
    )
    attractor = service.store.append(
        Event.new(
            EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id=f"query-{suffix}"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=output.id,
            causation_id=output.id,
            correlation_id=root.correlation_id,
            created_at=root.created_at + dt.timedelta(seconds=3),
            payload={
                "attractor": "latex_notation",
                "markers": ["$$"],
                "source_event_ids": [output.id],
            },
        )
    )
    note = service.store.append(
        Event.new(
            EventType.HUMAN_NOTE,
            actor=Actor(kind=ActorKind.HUMAN, id=f"query-{suffix}"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=attractor.id,
            causation_id=attractor.id,
            correlation_id=root.correlation_id,
            created_at=root.created_at + dt.timedelta(seconds=4),
            payload={"content": f"follow-up {suffix} literal%_["},
        )
    )
    return {
        "session_id": root.session_id,
        "phrase": phrase,
        "prompt_text": prompt_text,
        "output_text": output_text,
        "prompt": prompt,
        "output": output,
        "attractor": attractor,
        "note": note,
    }


def test_research_catalog_read_model_preserves_global_scope_shape_order_and_read_only(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    first = service.new_session("first catalog")
    first_events = _append_catalog(service, first, "zeta")
    second = service.new_session("second catalog")
    second_events = _append_catalog(service, second, "alpha")
    read_model = ResearchCatalogReadModel(service.store)

    expected_claims = [
        dict(row)
        for row in service.store.connection.execute(
            "SELECT * FROM claims ORDER BY first_seen_at, id"
        )
    ]
    expected_motifs = [
        dict(row)
        for row in service.store.connection.execute(
            "SELECT id, label, description, length(embedding) AS embedding_bytes "
            "FROM motifs ORDER BY id"
        )
    ]
    event_ids_before = tuple(event.id for event in service.store.list_events())
    changes_before = service.store.connection.total_changes

    claims = read_model.claims()
    contradictions = read_model.contradictions()
    motifs = read_model.motifs()
    attractors = read_model.attractors()

    assert claims == expected_claims
    assert [row["id"] for row in claims] == ["clm_zeta", "clm_alpha"]
    assert contradictions == [first_events[2].to_dict(), second_events[2].to_dict()]
    assert motifs == expected_motifs
    assert [row["id"] for row in motifs] == ["mot_alpha", "mot_zeta"]
    assert set(motifs[0]) == {"id", "label", "description", "embedding_bytes"}
    assert attractors == [first_events[3].to_dict(), second_events[3].to_dict()]
    assert service.claims() == claims
    assert service.contradictions() == contradictions
    assert service.motifs() == motifs
    assert service.attractors() == attractors
    assert tuple(event.id for event in service.store.list_events()) == event_ids_before
    assert service.store.connection.total_changes == changes_before


def test_research_catalog_read_model_excludes_transitive_synthetic_lineage(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("synthetic catalog boundary")
    genuine = _append_catalog(service, session, "genuine")
    root = service.store.require(str(session["root_event_id"]))
    synthetic = service.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="synthetic-fixture"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={"content": "synthetic", "material_origin": "synthetic_fixture"},
            metadata={"schema_version": 1, "material_origin": "synthetic_fixture"},
        )
    )
    common = {
        "actor": Actor(kind=ActorKind.HOST, id="synthetic-catalog"),
        "session_id": synthetic.session_id,
        "branch_id": synthetic.branch_id,
        "parent_event_id": synthetic.id,
        "causation_id": synthetic.id,
        "correlation_id": synthetic.correlation_id,
    }
    service.store.append_many(
        (
            Event.new(
                EventType.ANALYSIS_CLAIM_DETECTED,
                **common,
                payload={
                    "claims": [{"id": "clm_synthetic", "raw": "synthetic claim"}],
                    "source_event_ids": [synthetic.id],
                },
            ),
            Event.new(
                EventType.ANALYSIS_MOTIF_DETECTED,
                **common,
                payload={
                    "motif_id": "mot_synthetic",
                    "label": "synthetic motif",
                    "source_event_id": synthetic.id,
                    "source_event_ids": [synthetic.id],
                },
            ),
            Event.new(
                EventType.ANALYSIS_NUMERIC_INCONSISTENCY,
                **common,
                payload={"source_event_ids": [synthetic.id]},
            ),
            Event.new(
                EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED,
                **common,
                payload={
                    "attractor": "synthetic format",
                    "markers": ["synthetic"],
                    "source_event_ids": [synthetic.id],
                },
            ),
        )
    )
    read_model = ResearchCatalogReadModel(service.store)

    assert [row["id"] for row in read_model.claims()] == ["clm_genuine"]
    assert read_model.contradictions() == [genuine[2].to_dict()]
    assert [row["id"] for row in read_model.motifs()] == ["mot_genuine"]
    assert read_model.attractors() == [genuine[3].to_dict()]


def test_session_queries_preserve_scope_shape_order_escaping_and_read_only(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    first = _append_session_queries(service, service.new_session("first queries"), "first")
    second = _append_session_queries(service, service.new_session("second queries"), "second")
    read_model = ResearchCatalogReadModel(service.store)
    event_ids_before = tuple(event.id for event in service.store.list_events())
    changes_before = service.store.connection.total_changes

    pair = {
        "prompt_event_id": second["prompt"].id,
        "prompt_event_type": EventType.HUMAN_INPUT.value,
        "exact_prompt": second["prompt_text"],
        "prompt_sha256": sha256_text(second["prompt_text"]),
        "output_event_id": second["output"].id,
        "attractors": ["latex_notation"],
    }
    statistic = {
        "phrase": second["phrase"],
        "prompt_count": 1,
        "output_count": 1,
        "attractor_counts": {"latex_notation": 1},
        "attractor_probability": {"latex_notation": 1.0},
        "prompt_event_ids": [second["prompt"].id],
        "output_event_ids": [second["output"].id],
    }
    expected_statistics = {
        "session_id": second["session_id"],
        "pair_count": 1,
        "phrase_statistics": [statistic],
        "pairs": [pair],
    }
    expected_latex = [
        {
            "attractor_event_id": second["attractor"].id,
            "source_event_id": second["output"].id,
            "branch_id": second["output"].branch_id,
            "latex_marker": "$$",
            "offset": second["output_text"].index("$$"),
            "words": ["alpha", "beta", "gamma"],
            "prefix": "alpha beta gamma",
        }
    ]
    expected_search = [
        {
            "document_id": second["output"].id,
            "event_id": second["output"].id,
            "kind": EventType.ORACLE_OUTPUT.value,
            "source_event_id": None,
            "source_event_ids": [],
            "score": 1.0,
            "matched_by": "text_substring",
            "text": second["output_text"],
        },
        {
            "document_id": second["note"].id,
            "event_id": second["note"].id,
            "kind": EventType.HUMAN_NOTE.value,
            "source_event_id": None,
            "source_event_ids": [],
            "score": 1.0,
            "matched_by": "text_substring",
            "text": second["note"].payload["content"],
        },
    ]

    assert service.prompt_attractor_statistics(phrase=second["phrase"]) == expected_statistics
    assert service.words_before_latex_attractors(word_count=3) == expected_latex
    assert service.search("literal%_[") == expected_search
    assert service.origin("origin-second")["target"]["id"] == second["output"].id
    assert service.prompt_attractor_statistics(
        session_id=first["session_id"], phrase=first["phrase"]
    ) == read_model.prompt_attractor_statistics(first["session_id"], phrase=first["phrase"])
    assert service.words_before_latex_attractors(
        session_id=first["session_id"], word_count=3
    ) == read_model.words_before_latex_attractors(first["session_id"], word_count=3)
    assert [hit["document_id"] for hit in read_model.search(first["session_id"], "literal%_[")] == [
        first["output"].id,
        first["note"].id,
    ]
    assert tuple(event.id for event in service.store.list_events()) == event_ids_before
    assert service.store.connection.total_changes == changes_before


def test_session_query_facade_preserves_validation_type_message_and_order(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)

    with pytest.raises(ServiceError) as phrase_error:
        service.prompt_attractor_statistics(phrase="")
    with pytest.raises(ServiceError) as word_count_error:
        service.words_before_latex_attractors(word_count=0)

    assert type(phrase_error.value) is ServiceError
    assert str(phrase_error.value) == "phrase must not be empty"
    assert type(word_count_error.value) is ServiceError
    assert str(word_count_error.value) == "word_count must be positive"
