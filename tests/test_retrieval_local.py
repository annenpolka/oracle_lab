import datetime as dt
from pathlib import Path

from oracle_lab.retrieval import RetrievalDocument, RetrievalIndex

FIXTURES = Path(__file__).parent / "fixtures"


def _index() -> RetrievalIndex:
    first = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    second = (FIXTURES / "oracle_output_002.md").read_text(encoding="utf-8")
    return RetrievalIndex(
        [
            RetrievalDocument(
                id="evt_001",
                text=first,
                kind="oracle.output",
                session_id="ses_main",
                branch_id="main",
                claims=("pain phase = 34.7°",),
                entities=("/dev/void",),
                metadata={"private_api_key": "SECRET_SHOULD_NOT_BE_INDEXED"},
            ),
            RetrievalDocument(
                id="evt_002",
                text=second,
                kind="oracle.output",
                session_id="ses_branch",
                branch_id="collapse",
                lineage=("ses_main",),
                claims=("one day = 42.72 hours",),
                entities=("/dev/void",),
            ),
        ]
    )


def test_exact_retrieval_by_id_claim_entity_substring_and_lineage() -> None:
    index = _index()

    assert [hit.event_id for hit in index.by_event_id("evt_001")] == ["evt_001"]
    assert [hit.event_id for hit in index.by_claim("pain phase = 34.7°")] == ["evt_001"]
    assert [hit.event_id for hit in index.by_entity("/DEV/VOID")] == ["evt_001", "evt_002"]
    assert [hit.event_id for hit in index.by_text_substring("34.7°")] == [
        "evt_001",
        "evt_002",
    ]
    assert [hit.event_id for hit in index.by_session_lineage("ses_main")] == [
        "evt_001",
        "evt_002",
    ]


def test_local_lexical_vector_ranks_relevant_output_without_metadata() -> None:
    index = _index()

    hope_hits = index.semantic_search("similar outputs to hope_filter = null")
    secret_hits = index.semantic_search("SECRET_SHOULD_NOT_BE_INDEXED", min_score=0.5)

    assert hope_hits[0].event_id == "evt_001"
    assert secret_hits == []
    assert all(hit.matched_by == "semantic_lexical_vector" for hit in hope_hits)


def test_semantic_filters_limit_results_to_targeted_context() -> None:
    index = _index()

    hits = index.semantic_search("34.7 pain phase", session_id="ses_branch", limit=1)

    assert [hit.event_id for hit in hits] == ["evt_002"]


def test_events_can_be_projected_without_importing_event_store() -> None:
    event = {
        "id": "evt_mapping",
        "type": "analysis.claim_detected",
        "session_id": "ses",
        "branch_id": "main",
        "payload": {"text": "hope_filter = null", "claims": ["hope_filter = null"]},
        "metadata": {"schema_version": 1},
    }

    document = RetrievalDocument.from_event(event)

    assert document.id == "evt_mapping"
    assert document.claims == ("hope_filter = null",)


def test_host_claim_and_entity_events_are_exactly_retrievable() -> None:
    index = RetrievalIndex.from_events(
        [
            {
                "id": "evt_claim",
                "type": "analysis.claim_detected",
                "payload": {"raw_text": "pain phase = 34.7°"},
            },
            {
                "id": "evt_entity",
                "type": "analysis.entity_detected",
                "payload": {"canonical_name": "/dev/void"},
            },
        ]
    )

    assert [hit.event_id for hit in index.by_claim("pain phase = 34.7°")] == ["evt_claim"]
    assert [hit.event_id for hit in index.by_entity("/dev/void")] == ["evt_entity"]


def test_exact_retrieval_uses_authoritative_event_time_before_event_id() -> None:
    index = RetrievalIndex(
        [
            RetrievalDocument(
                id="evt_000",
                text="same origin marker",
                created_at=dt.datetime(2026, 8, 30, 0, 0, 2, tzinfo=dt.UTC),
            ),
            RetrievalDocument(
                id="evt_ZZZ",
                text="same origin marker",
                created_at=dt.datetime(2026, 8, 30, 0, 0, 1, tzinfo=dt.UTC),
            ),
        ]
    )

    assert [hit.event_id for hit in index.by_text_substring("origin marker")] == [
        "evt_ZZZ",
        "evt_000",
    ]
