import copy
import json
from pathlib import Path
from typing import Any

import pytest

from oracle_lab.bundle_import import BundleImportError, ResearchBundleImporter
from oracle_lab.exporting import (
    export_public_bundle,
    export_research_bundle,
    public_bundle_records,
)
from oracle_lab.jsonutil import sha256_bytes, sha256_text
from oracle_lab.store import EventStore

FIXTURES = Path(__file__).parent / "fixtures"
HISTORICAL_SOURCE = FIXTURES / "oracle_output_001.md"
RAW_ORACLE_TEXT = HISTORICAL_SOURCE.read_text(encoding="utf-8")
COOKIE_SECRET = "session=private-cookie-value"
AUTH_SECRET = "Bearer private-authorization-value"
PREFIX_SECRET = "sk_live_private_generation_metadata"
ROUTING_SECRET = "opaque-private-routing-metadata"
ARCHIVE_PATH = "/private/oracle/archive/evt_kept.response.json"
REQUEST_ID = "provider-request-private"
EVENT_METADATA_SECRET = "private-event-metadata"


def _events() -> list[dict[str, Any]]:
    base = {
        "created_at": "2026-09-01T00:00:00+00:00",
        "session_id": "ses_public",
        "branch_id": "main",
    }
    kept_output = {
        **base,
        "id": "evt_kept",
        "type": "oracle.output",
        "actor": {"kind": "model", "id": "r1"},
        "payload": {
            "content": RAW_ORACLE_TEXT,
            "material_origin": "historical_fixture",
            "historical_fixture": True,
            "source_fixture": {
                "path": str(HISTORICAL_SOURCE),
                "sha256": sha256_bytes(HISTORICAL_SOURCE.read_bytes()),
                "size_bytes": len(HISTORICAL_SOURCE.read_bytes()),
            },
            "model": "deepseek/deepseek-r1",
            "provider": "openrouter",
            "sampling": {"temperature": 0.6, "top_p": 0.95, "max_tokens": 4096},
            "effective_sampling": {
                "temperature": 0.6,
                "AUTHORIZATION": AUTH_SECRET,
                "debug_note": PREFIX_SECRET,
            },
            "context_hash": "context-sha256",
            "model_identity": {
                "requested_model_slug": "deepseek/deepseek-r1",
                "actual_provider": "novita",
                "actual_model_identifier": "deepseek/deepseek-r1",
                "fallback_occurred": False,
                "provider_routing": {
                    "pin_provider": "novita",
                    "allow_fallback": False,
                    "opaque_note": ROUTING_SECRET,
                },
                "routing_note": ROUTING_SECRET,
            },
            "archive_sha256": "archive-sha256",
            "archive_size_bytes": len(RAW_ORACLE_TEXT.encode("utf-8")),
            "api_response_metadata": {
                "http_headers": {
                    "Set-Cookie": COOKIE_SECRET,
                    "authorization": AUTH_SECRET,
                },
                "provider_request_id": REQUEST_ID,
            },
            "provider_request_id": REQUEST_ID,
            "archive_path": ARCHIVE_PATH,
            "arbitrary_secret": PREFIX_SECRET,
        },
        "metadata": {
            "schema_version": 1,
            "material_origin": "historical_fixture",
            "historical_fixture": True,
            "secret": EVENT_METADATA_SECRET,
        },
    }
    keep = {
        **base,
        "id": "evt_keep",
        "type": "human.keep",
        "actor": {"kind": "human", "id": "curator"},
        "parent_event_id": "evt_kept",
        "payload": {"event_id": "evt_kept"},
        "metadata": {"schema_version": 1},
    }
    unkept = copy.deepcopy(kept_output)
    unkept["id"] = "evt_unkept"
    unkept["payload"]["content"] = "not selected"
    synthetic = copy.deepcopy(kept_output)
    synthetic["id"] = "evt_synthetic"
    synthetic["payload"]["content"] = "synthetic material"
    synthetic["payload"]["material_origin"] = "synthetic_fixture"
    synthetic["metadata"]["material_origin"] = "synthetic_fixture"
    synthetic_keep = copy.deepcopy(keep)
    synthetic_keep["id"] = "evt_keep_synthetic"
    synthetic_keep["parent_event_id"] = "evt_synthetic"
    synthetic_keep["payload"] = {"event_id": "evt_synthetic"}
    worker = {
        **base,
        "id": "evt_worker",
        "type": "analysis.note",
        "actor": {"kind": "worker", "id": "coder"},
        "payload": {"content": "worker artifact", "secret": PREFIX_SECRET},
        "metadata": {"schema_version": 1},
    }
    return [kept_output, keep, unkept, synthetic, synthetic_keep, worker]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def test_public_bundle_has_closed_non_authoritative_layout_and_safe_records(
    tmp_path: Path,
) -> None:
    events = _events()
    original = copy.deepcopy(events)
    destination = tmp_path / "public"

    export_public_bundle(
        destination,
        events=events,
        provenance={"evt_kept": ["evt_input"]},
    )

    assert events == original
    assert {
        str(path.relative_to(destination)) for path in destination.rglob("*") if path.is_file()
    } == {"manifest.json", "records.jsonl", "redactions.json"}

    manifest = _read_json(destination / "manifest.json")
    assert set(manifest) == {
        "authority",
        "content_review_required",
        "counts",
        "created_at",
        "format",
        "importable",
        "sha256",
        "version",
    }
    assert manifest["format"] == "oracle-lab-public-bundle"
    assert manifest["version"] == 1
    assert manifest["authority"] == "derived_public_view"
    assert manifest["importable"] is False
    assert manifest["content_review_required"] is True
    assert manifest["counts"] == {"records": 1}
    assert manifest["sha256"] == {
        "records.jsonl": sha256_bytes((destination / "records.jsonl").read_bytes()),
        "redactions.json": sha256_bytes((destination / "redactions.json").read_bytes()),
    }

    record_lines = (destination / "records.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(record_lines) == 1
    record = json.loads(record_lines[0])
    assert set(record) == {
        "actor",
        "branch_id",
        "event_id",
        "event_type",
        "generation_identity",
        "material_origin",
        "provenance_ids",
        "raw_sha256",
        "raw_text",
        "session_id",
    }
    assert record["event_id"] == "evt_kept"
    assert record["raw_text"] == RAW_ORACLE_TEXT
    assert record["raw_sha256"] == sha256_text(RAW_ORACLE_TEXT)
    assert record["provenance_ids"] == ["evt_kept", "evt_input"]
    assert record["actor"] == {"id": "r1", "kind": "model"}
    assert record["material_origin"] == "historical_fixture"
    assert set(record["generation_identity"]) == {
        "archive_sha256",
        "archive_size_bytes",
        "context_hash",
        "effective_sampling",
        "model",
        "model_identity",
        "provider",
        "sampling",
    }
    assert record["generation_identity"]["model"] == "deepseek/deepseek-r1"
    assert record["generation_identity"]["provider"] == "openrouter"
    assert record["generation_identity"]["sampling"] == {
        "max_tokens": 4096,
        "temperature": 0.6,
        "top_p": 0.95,
    }
    assert record["generation_identity"]["context_hash"] == "context-sha256"
    assert record["generation_identity"]["archive_sha256"] == "archive-sha256"
    assert record["generation_identity"]["archive_size_bytes"] == len(
        RAW_ORACLE_TEXT.encode("utf-8")
    )
    assert record["generation_identity"]["effective_sampling"] == {
        "temperature": 0.6,
    }
    assert record["generation_identity"]["model_identity"] == {
        "actual_model_identifier": "deepseek/deepseek-r1",
        "actual_provider": "novita",
        "fallback_occurred": False,
        "provider_routing": {"allow_fallback": False, "pin_provider": "novita"},
        "requested_model_slug": "deepseek/deepseek-r1",
    }

    public_output = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(destination.iterdir())
    )
    for private_value in (
        COOKIE_SECRET,
        AUTH_SECRET,
        PREFIX_SECRET,
        ROUTING_SECRET,
        ARCHIVE_PATH,
        str(HISTORICAL_SOURCE),
        REQUEST_ID,
        EVENT_METADATA_SECRET,
        "not selected",
        "synthetic material",
        "worker artifact",
        "evt_worker",
    ):
        assert private_value not in public_output

    redactions = _read_json(destination / "redactions.json")
    assert set(redactions) == {
        "omitted_categories",
        "policy",
        "preserved_categories",
        "transformed_categories",
    }
    assert redactions["policy"] == "generation_identity_allowlist"
    redactions_text = json.dumps(redactions, ensure_ascii=False)
    for private_value in (
        COOKIE_SECRET,
        AUTH_SECRET,
        PREFIX_SECRET,
        ROUTING_SECRET,
        ARCHIVE_PATH,
        str(HISTORICAL_SOURCE),
        REQUEST_ID,
        EVENT_METADATA_SECRET,
    ):
        assert private_value not in redactions_text


def test_public_bundle_records_select_only_human_kept_genuine_oracle_material() -> None:
    records = public_bundle_records(_events())

    assert [record["event_id"] for record in records] == ["evt_kept"]


def test_public_bundle_rejects_nonempty_destination(tmp_path: Path) -> None:
    destination = tmp_path / "public"
    destination.mkdir()
    marker = destination / "existing.txt"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(FileExistsError, match="absent or an empty directory"):
        export_public_bundle(destination, events=_events())

    assert marker.read_text(encoding="utf-8") == "keep"
    assert {path.name for path in destination.iterdir()} == {"existing.txt"}


def test_public_bundle_is_rejected_by_the_canonical_importer_before_store_mutation(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "public"
    store = EventStore()
    export_public_bundle(destination, events=_events())

    with pytest.raises(BundleImportError, match="unsupported research bundle format"):
        ResearchBundleImporter(store).import_directory(destination)

    assert store.count_events() == 0


def test_public_export_does_not_mutate_canonical_bundle_bytes(tmp_path: Path) -> None:
    events = _events()
    before = tmp_path / "canonical-before"
    public = tmp_path / "public"
    after = tmp_path / "canonical-after"

    export_research_bundle(before, events=events)
    export_public_bundle(public, events=events)
    export_research_bundle(after, events=events)

    before_files = {
        str(path.relative_to(before)): path.read_bytes()
        for path in before.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    after_files = {
        str(path.relative_to(after)): path.read_bytes()
        for path in after.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    assert before_files == after_files
    canonical_events = (before / "events.jsonl").read_text(encoding="utf-8")
    assert COOKIE_SECRET in canonical_events
    assert AUTH_SECRET in canonical_events
    assert PREFIX_SECRET in canonical_events
    assert ROUTING_SECRET in canonical_events
    assert ARCHIVE_PATH in canonical_events
    assert REQUEST_ID in canonical_events
    assert EVENT_METADATA_SECRET in canonical_events
