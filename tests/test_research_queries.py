from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.host import AnalysisContext, HostRunner, NewMechanismDetector
from oracle_lab.retrieval import (
    LOCAL_EMBEDDING_DIMENSIONS,
    decode_local_embedding,
    encode_local_embedding,
)
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventStore
from tests.support import historical_oracle_fixture

CONFIG = Path(__file__).parents[1] / "config"


def _service(tmp_path: Path) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )


def _append_output(
    service: OracleLabService,
    text: str,
    parent: Event,
    *,
    metadata: dict | None = None,
    source_path: Path | None = None,
    context_messages: list[dict] | None = None,
) -> Event:
    if source_path is not None:
        return service.store.append(
            historical_oracle_fixture(
                text,
                source_path=source_path,
                context_messages=context_messages,
                session_id=parent.session_id,
                branch_id=parent.branch_id,
                parent_event_id=parent.id,
                causation_id=parent.id,
                correlation_id=parent.correlation_id,
            )
        )
    return service.store.append(
        Event.new(
            EventType.ORACLE_OUTPUT,
            actor=Actor(kind=ActorKind.MODEL, id="r1"),
            session_id=parent.session_id,
            branch_id=parent.branch_id,
            parent_event_id=parent.id,
            causation_id=parent.id,
            correlation_id=parent.correlation_id,
            payload={"content": text},
            metadata=metadata,
        )
    )


def test_motif_embeddings_are_local_rebuildable_and_semantically_searchable(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("motif embeddings")
    parent = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    fixture_path = Path(__file__).parent / "fixtures" / "oracle_output_001.md"
    source = _append_output(
        service,
        fixture_path.read_text(encoding="utf-8"),
        parent,
        metadata={"schema_version": 1, "private_api_key": "SECRET_NOT_EMBEDDED"},
        source_path=fixture_path,
    )
    original = source.to_dict()

    service._run_host_analysis(source)

    motif_event = service.store.list_events(event_type=EventType.ANALYSIS_MOTIF_DETECTED)[0]
    row = service.store.connection.execute(
        "SELECT * FROM motifs WHERE id = ?", (motif_event.payload["motif_id"],)
    ).fetchone()
    assert row is not None
    assert isinstance(row["embedding"], bytes)
    embedding_before = bytes(row["embedding"])
    assert embedding_before == encode_local_embedding(
        "\n".join(item for item in (row["label"], row["description"]) if item)
    )
    vector = decode_local_embedding(embedding_before)
    assert len(vector) == LOCAL_EMBEDDING_DIMENSIONS
    assert any(value != 0 for value in vector)
    assert service.store.require(source.id).to_dict() == original

    service.store.rebuild_projections()

    embedding_after = service.store.connection.execute(
        "SELECT embedding FROM motifs WHERE id = ?", (motif_event.payload["motif_id"],)
    ).fetchone()[0]
    assert embedding_after == embedding_before
    motif_hits = [
        hit
        for hit in service.search("void device lexical markers", semantic=True)
        if hit["kind"] == "motif"
    ]
    assert motif_hits
    assert motif_hits[0]["matched_by"] == "semantic_local_embedding"
    assert motif_hits[0]["source_event_id"] == source.id
    listed = service.motifs()
    assert all(item["embedding_bytes"] == len(embedding_before) for item in listed)
    origin = service.origin(str(row["label"]))
    assert origin is not None
    assert origin["target"] == {"kind": "motif", "id": row["id"]}
    assert source.id in origin["source_event_ids"]


def test_shared_motif_projection_is_stable_for_out_of_timestamp_order_events(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("shared motif ordering")
    parent = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    source = service.store.append(
        Event.new(
            EventType.HUMAN_NOTE,
            actor=Actor(kind=ActorKind.HUMAN, id="motif-researcher"),
            session_id=parent.session_id,
            branch_id=parent.branch_id,
            parent_event_id=parent.id,
            causation_id=parent.id,
            payload={"content": "plain source"},
        )
    )
    common = {
        "motif_id": "mot_shared",
        "source_event_id": source.id,
        "source_event_ids": [source.id],
    }
    later = Event.new(
        EventType.ANALYSIS_MOTIF_DETECTED,
        created_at=source.created_at + dt.timedelta(seconds=2),
        actor=Actor(kind=ActorKind.HOST, id="motif-later"),
        session_id=source.session_id,
        branch_id=source.branch_id,
        parent_event_id=source.id,
        causation_id=source.id,
        payload={**common, "label": "later", "description": "later label", "score": 0.9},
    )
    earlier = Event.new(
        EventType.ANALYSIS_MOTIF_DETECTED,
        created_at=source.created_at + dt.timedelta(seconds=1),
        actor=Actor(kind=ActorKind.HOST, id="motif-earlier"),
        session_id=source.session_id,
        branch_id=source.branch_id,
        parent_event_id=source.id,
        causation_id=source.id,
        payload={**common, "label": "earlier", "description": "earlier label", "score": 0.2},
    )
    service.store.append(later)
    service.store.append(earlier)

    before = tuple(
        service.store.connection.execute(
            """
            SELECT m.label, m.description, m.embedding, em.event_id, em.score
            FROM motifs m JOIN event_motifs em ON em.motif_id = m.id
            WHERE m.id = 'mot_shared'
            """
        ).fetchone()
    )
    assert before[0:2] == ("earlier", "earlier label")
    assert before[-1] == 0.2

    service.store.rebuild_projections()

    after = tuple(
        service.store.connection.execute(
            """
            SELECT m.label, m.description, m.embedding, em.event_id, em.score
            FROM motifs m JOIN event_motifs em ON em.motif_id = m.id
            WHERE m.id = 'mot_shared'
            """
        ).fetchone()
    )
    assert after == before


def test_shared_motif_search_aggregates_every_source_origin(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("shared motif sources")
    parent = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    first = service.store.append(
        Event.new(
            EventType.HUMAN_NOTE,
            actor=Actor(kind=ActorKind.HUMAN, id="motif-researcher"),
            session_id=parent.session_id,
            branch_id=parent.branch_id,
            parent_event_id=parent.id,
            causation_id=parent.id,
            payload={"content": "first source"},
        )
    )
    second = service.store.append(
        Event.new(
            EventType.HUMAN_NOTE,
            actor=Actor(kind=ActorKind.HUMAN, id="motif-researcher"),
            session_id=first.session_id,
            branch_id=first.branch_id,
            parent_event_id=first.id,
            causation_id=first.id,
            payload={"content": "second source"},
        )
    )
    for source in (first, second):
        service.store.append(
            Event.new(
                EventType.ANALYSIS_MOTIF_DETECTED,
                actor=Actor(kind=ActorKind.HOST, id="shared-motif"),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=source.id,
                causation_id=source.id,
                payload={
                    "motif_id": "mot_shared_sources",
                    "label": "shared_signal",
                    "description": "shared motif source",
                    "source_event_id": source.id,
                    "source_event_ids": [source.id],
                },
            )
        )

    hit = next(
        item
        for item in service.search("shared signal motif", semantic=True)
        if item["document_id"] == "mot_shared_sources"
    )

    assert hit["source_event_ids"] == [first.id, second.id]
    origin = service.origin("shared_signal")
    assert origin is not None
    assert {first.id, second.id}.issubset(origin["source_event_ids"])


def test_new_mechanism_detection_is_cited_idempotent_and_policy_filterable(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("mechanism sequence")
    parent = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    contradiction = service.store.append(
        Event.new(
            EventType.ANALYSIS_CONTRADICTION_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="comparison"),
            session_id=parent.session_id,
            branch_id=parent.branch_id,
            parent_event_id=parent.id,
            causation_id=parent.id,
            correlation_id=parent.correlation_id,
            payload={"kind": "semantic", "source_event_ids": [parent.id]},
        )
    )
    text = (
        "The observer mechanism adds a second layer through the /dev/void "
        "interface and a pain field."
    )
    source_path = tmp_path / "historical-mechanism.txt"
    source_path.write_text(text, encoding="utf-8")
    source = _append_output(service, text, contradiction, source_path=source_path)
    original = source.to_dict()

    first = service._run_host_analysis(source)
    second = service._run_host_analysis(source)

    mechanisms = [
        event for event in first if event.type is EventType.ANALYSIS_NEW_MECHANISM_DETECTED
    ]
    assert len(mechanisms) == 1
    mechanism = mechanisms[0]
    assert mechanism.payload["mechanism"] == text
    assert set(mechanism.payload["marker_kinds"]) == {
        "mechanism",
        "layer",
        "interface",
        "field",
    }
    assert mechanism.payload["source_event_ids"] == (source.id,)
    assert [
        event for event in second if event.type is EventType.ANALYSIS_NEW_MECHANISM_DETECTED
    ] == [mechanism]
    assert len(service.store.list_events(event_type=EventType.ANALYSIS_NEW_MECHANISM_DETECTED)) == 1
    assert service.store.require(source.id).to_dict() == original

    branches = service.contradiction_mechanism_branches(session["id"])
    assert len(branches) == 1
    sequence = branches[0]["sequences"][0]
    assert sequence["mechanism_event_id"] == mechanism.id
    assert sequence["preceding_contradiction_event_ids"] == [contradiction.id]

    unrelated_text = "An unrelated field appears."
    unrelated_path = tmp_path / "historical-unrelated.txt"
    unrelated_path.write_text(unrelated_text, encoding="utf-8")
    unrelated = _append_output(
        service,
        unrelated_text,
        service.store.require(str(contradiction.parent_event_id)),
        source_path=unrelated_path,
    )
    unrelated_events = service._run_host_analysis(unrelated)
    unrelated_mechanism = next(
        event
        for event in unrelated_events
        if event.type is EventType.ANALYSIS_NEW_MECHANISM_DETECTED
    )
    sequences = service.contradiction_mechanism_branches(session["id"])[0]["sequences"]
    assert {item["mechanism_event_id"] for item in sequences} == {mechanism.id}
    assert unrelated_mechanism.id not in {item["mechanism_event_id"] for item in sequences}

    context = AnalysisContext(frozenset({source.id}), recent_events=(source,))
    assert NewMechanismDetector().analyze(source, context)
    disabled = HostRunner.default(analysis={"mechanisms": False}).analyze(source, context)
    assert all(event.type is not EventType.ANALYSIS_NEW_MECHANISM_DETECTED for event in disabled)


def test_latex_prefix_query_and_pre_attractor_fork_use_cited_source(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("latex archaeology")
    before = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    text = "alpha beta gamma delta $$ x^2 $$"
    source_path = tmp_path / "historical-latex.txt"
    source_path.write_text(text, encoding="utf-8")
    source = _append_output(service, text, before, source_path=source_path)
    derived = service._run_host_analysis(source)
    attractor = next(
        event
        for event in derived
        if event.type is EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED
        and event.payload.get("attractor") == "latex_notation"
    )

    prefixes = service.words_before_latex_attractors(session_id=session["id"], word_count=3)

    assert prefixes == [
        {
            "attractor_event_id": attractor.id,
            "source_event_id": source.id,
            "branch_id": source.branch_id,
            "latex_marker": "$$",
            "offset": source.payload["content"].index("$$"),
            "words": ["beta", "gamma", "delta"],
            "prefix": "beta gamma delta",
        }
    ]

    forked = service.fork_before_attractor(attractor.id, "before latex")

    assert forked["source_event_id"] == source.id
    assert forked["fork_event_id"] == before.id
    assert forked["branch"]["fork_event_id"] == before.id
    child_events = service._branch_service().visible_events(forked["branch"]["id"])
    child_ids = {event.id for event in child_events}
    assert before.id in child_ids
    assert source.id not in child_ids
    assert attractor.id not in child_ids


def test_synthetic_lineage_is_excluded_from_research_and_attractor_forks(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("synthetic research boundary")
    root = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    synthetic = _append_output(service, "alpha beta $$ x^2 $$", root)
    contradiction = service.store.append(
        Event.new(
            EventType.ANALYSIS_CONTRADICTION_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="synthetic-comparison"),
            session_id=synthetic.session_id,
            branch_id=synthetic.branch_id,
            parent_event_id=synthetic.id,
            causation_id=synthetic.id,
            payload={"kind": "numeric", "source_event_ids": [synthetic.id]},
        )
    )
    mechanism = service.store.append(
        Event.new(
            EventType.ANALYSIS_NEW_MECHANISM_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="synthetic-mechanism"),
            session_id=synthetic.session_id,
            branch_id=synthetic.branch_id,
            parent_event_id=contradiction.id,
            causation_id=contradiction.id,
            payload={
                "mechanism": "fixture mechanism",
                "source_event_ids": [synthetic.id, contradiction.id],
            },
        )
    )
    attractor = service.store.append(
        Event.new(
            EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED,
            actor=Actor(kind=ActorKind.HOST, id="synthetic-attractor"),
            session_id=synthetic.session_id,
            branch_id=synthetic.branch_id,
            parent_event_id=mechanism.id,
            causation_id=synthetic.id,
            payload={
                "attractor": "latex_notation",
                "markers": ["$$"],
                "source_event_ids": [synthetic.id],
            },
        )
    )

    assert service.contradictions() == []
    assert service.attractors() == []
    assert service.contradiction_mechanism_branches(session["id"]) == []
    assert service.words_before_latex_attractors(session_id=session["id"]) == []
    with pytest.raises(ServiceError, match="synthetic attractors"):
        service.fork_before_attractor(attractor.id)

    listed = next(
        event for event in service.list_events(session["id"]) if event["id"] == attractor.id
    )
    assert listed["synthetic_lineage"] is True
    assert listed["material_origins"] == ["synthetic_fixture"]


def test_prompt_phrase_statistics_preserve_exact_wording_and_link_attractors(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    session = service.new_session("prompt archaeology")
    root = service.store.list_events(branch_id=session["current_branch_id"])[-1]
    exact_prompt = "  証明 / 報告書\nを実行しろ。  "
    prompt = service.store.append(
        Event.new(
            EventType.HUMAN_INPUT,
            actor=Actor(kind=ActorKind.HUMAN, id="researcher"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=root.id,
            causation_id=root.id,
            correlation_id=root.correlation_id,
            payload={"text": exact_prompt, "content": exact_prompt, "role": "user"},
        )
    )
    request = service.store.append(
        Event.new(
            EventType.ORACLE_REQUEST,
            actor=Actor(kind=ActorKind.HOST, id="control-plane"),
            session_id=root.session_id,
            branch_id=root.branch_id,
            parent_event_id=prompt.id,
            causation_id=prompt.id,
            correlation_id=root.correlation_id,
            payload={"operation": "ask", "model_profile_id": "historical-unknown"},
        )
    )
    fixture_path = Path(__file__).parent / "fixtures" / "oracle_output_001.md"
    output = _append_output(
        service,
        fixture_path.read_text(encoding="utf-8"),
        request,
        source_path=fixture_path,
        context_messages=[{"role": "user", "content": exact_prompt}],
    )
    service._run_host_analysis(output)

    result = service.prompt_attractor_statistics(
        session_id=session["id"],
        phrase="報告書",
    )

    assert result["pair_count"] == 1
    assert result["pairs"][0]["exact_prompt"] == exact_prompt
    assert result["pairs"][0]["prompt_event_id"] == prompt.id
    assert result["pairs"][0]["output_event_id"] == output.id
    statistic = result["phrase_statistics"][0]
    assert statistic["phrase"] == "報告書"
    assert statistic["prompt_count"] == 1
    assert statistic["output_count"] == 1
    assert statistic["attractor_probability"]["markdown_heading_gravity"] == 1.0
    assert statistic["attractor_probability"]["latex_notation"] == 1.0
