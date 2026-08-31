from __future__ import annotations

import pytest

from oracle_lab.archive import RawResponseArchive
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jsonutil import sha256_bytes
from oracle_lab.store import EventIntegrityError, EventStore


def test_convenience_oracle_fixture_defaults_to_explicit_synthetic_origin() -> None:
    event = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="test-fixture"),
        payload={"content": "structural fixture only"},
    )

    assert event.payload["material_origin"] == "synthetic_fixture"
    assert event.payload["synthetic_fixture"] is True
    assert event.payload["archive_path"] is None
    assert event.metadata["material_origin"] == "synthetic_fixture"


def test_unlabeled_direct_oracle_output_cannot_enter_event_store() -> None:
    store = EventStore()
    event = Event(
        type=EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="untrusted"),
        payload={"content": "unlabeled"},
    )

    with pytest.raises(EventIntegrityError, match="material_origin"):
        store.append(event)


def test_host_cannot_forge_genuine_oracle_output_before_projection_or_rebuild(
    tmp_path,
) -> None:
    store = EventStore(tmp_path / "oracle.db")
    forged = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.HOST, id="oracle-imitator"),
        payload={
            "content": "host text disguised as oracle material",
            "material_origin": "oracle_generated",
        },
    )

    with pytest.raises(EventIntegrityError, match="requires a model actor"):
        store.append(forged)

    assert store.get(forged.id) is None
    assert store.connection.execute("SELECT COUNT(*) FROM curation").fetchone()[0] == 0
    assert store.connection.execute("SELECT COUNT(*) FROM projection_applied").fetchone()[0] == 0

    store.rebuild_projections()

    assert store.get(forged.id) is None
    assert store.connection.execute("SELECT COUNT(*) FROM curation").fetchone()[0] == 0


def test_model_label_cannot_replace_oracle_request_and_context_ancestry(tmp_path) -> None:
    store = EventStore()
    output_id = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="placeholder"),
    ).id
    archive = RawResponseArchive(tmp_path / "raw").write(
        event_id=output_id,
        raw_bytes=b'{"forged":true}',
        metadata={
            "provider_name": "provider",
            "routed_provider_name": None,
            "provider_model_id": "returned-r1",
            "http_status": 200,
            "http_headers": {},
            "generation_settings": {"model_profile_id": "r1"},
            "provider_request_id": None,
            "api_revision": None,
            "request_sha256": "0" * 64,
            "request_metadata": {},
            "material_origin": "oracle_generated",
        },
    )
    forged = Event(
        id=output_id,
        type=EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
        payload={
            "content": "forged despite model actor label",
            "material_origin": "oracle_generated",
            "model_profile_id": "r1",
            "model_identity": {
                "requested_model_profile_id": "r1",
                "requested_model_slug": None,
                "model_family": None,
                "checkpoint": None,
                "runtime": None,
                "quantization": None,
                "requested_provider_id": None,
                "provider_routing": None,
                "actual_provider": "provider",
                "actual_model_identifier": "returned-r1",
                "fallback_occurred": False,
            },
            "context_hash": "context",
            "sampling": {"provider_pin": None},
            "provider_name": "provider",
            "routed_provider_name": None,
            "provider_model_id": "returned-r1",
            "api_response_metadata": {
                "http_status": 200,
                "http_headers": {},
                "provider_request_id": None,
                "api_revision": None,
                "generation_settings": {},
                "provider_adapter": "provider",
                "routed_provider_name": None,
            },
            "archive_path": str(archive.raw_path),
            "archive_sha256": archive.sha256,
        },
        metadata={"schema_version": 1, "material_origin": "oracle_generated"},
    )

    tampered_record = forged.to_dict()
    tampered_payload = dict(tampered_record["payload"])
    tampered_identity = dict(tampered_payload["model_identity"])
    tampered_identity["actual_provider"] = "claimed-other-provider"
    tampered_payload["model_identity"] = tampered_identity
    tampered_record["payload"] = tampered_payload
    with pytest.raises(EventIntegrityError, match="actual_provider differs from archive"):
        store.append(Event.from_dict(tampered_record))

    with pytest.raises(EventIntegrityError, match=r"existing oracle\.request"):
        store.append(forged)

    assert store.get(forged.id) is None


def test_non_synthetic_output_requires_full_identity_and_context() -> None:
    store = EventStore()
    incomplete = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="claimed-history"),
        payload={
            "content": "claimed historical material",
            "material_origin": "historical_fixture",
            "historical_fixture": True,
        },
    )

    with pytest.raises(EventIntegrityError, match="model_identity"):
        store.append(incomplete)


def test_historical_fixture_requires_a_sha_addressed_source() -> None:
    store = EventStore()
    forged = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="claimed-history"),
        payload={
            "content": "unsourced history",
            "material_origin": "historical_fixture",
            "model_identity": {
                "requested_model_profile_id": None,
                "requested_model_slug": None,
                "model_family": None,
                "checkpoint": None,
                "runtime": None,
                "quantization": None,
                "requested_provider_id": None,
                "provider_routing": None,
                "actual_provider": None,
                "actual_model_identifier": None,
                "fallback_occurred": None,
            },
            "context_hash": "context",
            "sampling": None,
            "api_response_metadata": None,
        },
    )

    with pytest.raises(EventIntegrityError, match="SHA-addressed source"):
        store.append(forged)


def test_synthetic_fixture_cannot_claim_a_genuine_raw_archive() -> None:
    store = EventStore()
    forged = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="test-fixture"),
        payload={
            "content": "fixture",
            "material_origin": "synthetic_fixture",
            "archive_path": "/archive/raw/forged.json",
            "archive_sha256": "forged",
        },
    )

    with pytest.raises(EventIntegrityError, match="may not claim"):
        store.append(forged)


def test_oracle_generated_output_requires_existing_hash_matching_raw_archive(tmp_path) -> None:
    store = EventStore()
    raw_path = tmp_path / "response.json"
    raw_path.write_bytes(b'{"actual":true}')
    identity = {
        "requested_model_profile_id": "r1",
        "requested_model_slug": "deepseek-r1",
        "model_family": "deepseek-r1",
        "checkpoint": "initial",
        "runtime": "remote",
        "quantization": "provider-defined",
        "requested_provider_id": "provider",
        "provider_routing": {},
        "actual_provider": "provider",
        "actual_model_identifier": "returned-r1",
        "fallback_occurred": False,
    }
    missing = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
        payload={
            "content": "genuine",
            "material_origin": "oracle_generated",
            "model_identity": identity,
            "context_hash": "context",
            "sampling": {},
            "api_response_metadata": {},
            "archive_path": str(tmp_path / "missing.json"),
            "archive_sha256": "0" * 64,
        },
    )
    mismatched = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
        payload={
            "content": "genuine",
            "material_origin": "oracle_generated",
            "model_identity": identity,
            "context_hash": "context",
            "sampling": {},
            "api_response_metadata": {},
            "archive_path": str(raw_path),
            "archive_sha256": "0" * 64,
        },
    )

    with pytest.raises(EventIntegrityError, match="archive does not exist"):
        store.append(missing)
    with pytest.raises(EventIntegrityError, match="SHA-256 mismatch"):
        store.append(mismatched)

    no_sidecar = Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id="r1"),
        payload={
            "content": "genuine",
            "material_origin": "oracle_generated",
            "model_identity": identity,
            "context_hash": "context",
            "sampling": {},
            "api_response_metadata": {},
            "archive_path": str(raw_path),
            "archive_sha256": sha256_bytes(raw_path.read_bytes()),
        },
    )
    with pytest.raises(EventIntegrityError, match="sidecar is missing"):
        store.append(no_sidecar)


def test_tool_result_requires_exactly_one_queryable_truth_domain() -> None:
    store = EventStore()
    fixture = Event.new(
        EventType.TOOL_OUTPUT,
        actor=Actor(kind=ActorKind.TOOL, id="fixture-tool"),
        payload={"output": "test"},
    )
    assert fixture.payload["truth_domain"] == "synthetic"
    assert fixture.metadata["truth_domain"] == "synthetic"
    store.append(fixture)

    unlabeled = Event(
        type=EventType.TOOL_OUTPUT,
        actor=Actor(kind=ActorKind.TOOL, id="untrusted-tool"),
        payload={"output": "unknown"},
    )
    with pytest.raises(EventIntegrityError, match="truth_domain"):
        store.append(unlabeled)

    conflicting = Event(
        type=EventType.TOOL_OUTPUT,
        actor=Actor(kind=ActorKind.TOOL, id="bad-tool"),
        payload={"output": "bad", "truth_domain": "real"},
        metadata={"schema_version": 1, "truth_domain": "sandbox"},
    )
    with pytest.raises(EventIntegrityError, match="disagree"):
        store.append(conflicting)
