from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from oracle_lab.events import Actor, ActorKind, EventType
from oracle_lab.jsonutil import sha256_bytes
from oracle_lab.provenance import ProvenanceService
from oracle_lab.providers import OracleGenerateResponse
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"
HISTORICAL_RESPONSE = Path(__file__).parent / "fixtures" / "historical_continuation_001.json"
HISTORICAL_DOCUMENT = json.loads(HISTORICAL_RESPONSE.read_bytes())

RAW_PROVIDER_BYTES = HISTORICAL_RESPONSE.read_bytes()
ORACLE_TEXT = HISTORICAL_DOCUMENT["choices"][0]["message"]["content"]
COOKIE_SECRET = "session=private-cookie"
AUTH_SECRET = "Bearer private-authorization"
PREFIX_SECRET = "sk_live_private_transport_value"


class _HistoricalProvider:
    async def generate(self, request: Any) -> OracleGenerateResponse:
        del request
        return OracleGenerateResponse(
            raw_bytes=RAW_PROVIDER_BYTES,
            status_code=200,
            headers={
                "sEt-CoOkIe": COOKIE_SECRET,
                "AUTHORIZATION": AUTH_SECRET,
                "x-debug": PREFIX_SECRET,
                "x-request-id": "safe-request-id",
            },
            provider_name="historical-fixture",
            provider_model_id=HISTORICAL_DOCUMENT["model"],
            content=ORACLE_TEXT,
            reasoning=None,
            finish_reason="stop",
            usage={"prompt_tokens": 2, "completion_tokens": 3},
            elapsed_ms=1.25,
            request_id=None,
            parsed=HISTORICAL_DOCUMENT,
            generation_settings={},
            routed_provider_name=None,
            material_origin="historical_fixture",
        )


def _service(root: Path, *, provider: Any | None = None) -> OracleLabService:
    root.mkdir(parents=True, exist_ok=True)
    return OracleLabService(
        EventStore(root / "oracle.db"),
        home=root / "home",
        config_dir=CONFIG,
        provider_factory=None if provider is None else lambda _profile: provider,
    )


def test_canonical_oracle_metadata_round_trips_while_public_views_redact(
    tmp_path: Path,
) -> None:
    source = _service(tmp_path / "source", provider=_HistoricalProvider())
    session = source.new_session("privacy boundary")
    source.ask("確認しろ。")
    run = source.run_automation()
    output = source.store.list_events(event_type=EventType.ORACLE_OUTPUT)[0]
    source.keep(output.id)

    raw_path = Path(str(output.payload["archive_path"]))
    sidecar_path = raw_path.with_name(f"{raw_path.stem}.metadata.json")
    sidecar_bytes = sidecar_path.read_bytes()
    sidecar = json.loads(sidecar_bytes)
    canonical_headers = output.payload["api_response_metadata"]["http_headers"]

    run_view = json.dumps(run, ensure_ascii=False)
    assert COOKIE_SECRET not in run_view
    assert AUTH_SECRET not in run_view
    assert PREFIX_SECRET not in run_view
    assert "safe-request-id" in run_view

    assert raw_path.read_bytes() == RAW_PROVIDER_BYTES
    assert output.payload["archive_sha256"] == sha256_bytes(RAW_PROVIDER_BYTES)
    assert output.payload["archive_size_bytes"] == len(RAW_PROVIDER_BYTES)
    assert canonical_headers == sidecar["http_headers"]
    assert canonical_headers["sEt-CoOkIe"] == COOKIE_SECRET
    assert canonical_headers["AUTHORIZATION"] == AUTH_SECRET
    assert output.payload["content"] == ORACLE_TEXT
    assert output.to_dict()["payload"]["api_response_metadata"]["http_headers"] == (
        canonical_headers
    )

    public_event = source.show_event(output.id)
    public_generation = source.generation_metadata(output.id)
    for view in (public_event, public_generation):
        serialized = json.dumps(view, ensure_ascii=False)
        assert COOKIE_SECRET not in serialized
        assert AUTH_SECRET not in serialized
        assert PREFIX_SECRET not in serialized
        assert "safe-request-id" in serialized
    assert public_event["payload"]["content"] == ORACLE_TEXT

    canonical_bundle = tmp_path / "canonical-bundle"
    public_bundle = tmp_path / "public-bundle"
    source.export("bundle", canonical_bundle, session_id=session["id"])

    foreign_session = source.new_session("foreign session")
    foreign_input = source.ask("foreign provenance source")["input"]
    ProvenanceService(source.store).link(
        "event",
        output.id,
        foreign_input["id"],
        session_id=session["id"],
        branch_id=session["current_branch_id"],
    )
    source.export("public-bundle", public_bundle, session_id=session["id"])

    bundled_raw = canonical_bundle / "raw" / f"{output.id}.json"
    bundled_sidecar = canonical_bundle / "raw" / f"{output.id}.metadata.json"
    assert bundled_raw.read_bytes() == RAW_PROVIDER_BYTES
    assert bundled_sidecar.read_bytes() == sidecar_bytes
    canonical_events = (canonical_bundle / "events.jsonl").read_text(encoding="utf-8")
    assert COOKIE_SECRET in canonical_events
    assert AUTH_SECRET in canonical_events
    assert PREFIX_SECRET in canonical_events

    public_bytes = b"\n".join(
        path.read_bytes() for path in sorted(public_bundle.iterdir()) if path.is_file()
    )
    assert COOKIE_SECRET.encode() not in public_bytes
    assert AUTH_SECRET.encode() not in public_bytes
    assert PREFIX_SECRET.encode() not in public_bytes
    assert HISTORICAL_DOCUMENT["id"].encode() not in public_bytes
    public_record = json.loads(
        (public_bundle / "records.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    assert public_record["raw_text"] == ORACLE_TEXT
    assert public_record["raw_sha256"] == sha256_bytes(ORACLE_TEXT.encode("utf-8"))
    assert foreign_input["id"] not in public_record["provenance_ids"]
    assert foreign_session["id"] != session["id"]

    restored = _service(tmp_path / "restored")
    restored.import_session(
        canonical_bundle,
        authorize_human_curation=True,
        authorizer=Actor(kind=ActorKind.HUMAN, id="test-curator"),
    )
    restored_output = restored.store.require(output.id)
    restored_raw = Path(str(restored_output.payload["archive_path"]))
    restored_headers = restored_output.payload["api_response_metadata"]["http_headers"]

    assert restored_headers == canonical_headers
    assert (
        restored_output.payload["api_response_metadata"] == output.payload["api_response_metadata"]
    )
    assert restored_output.payload["model_identity"] == output.payload["model_identity"]
    assert restored_output.payload["context_hash"] == output.payload["context_hash"]
    assert restored_output.payload["sampling"] == output.payload["sampling"]
    assert restored_output.payload["effective_sampling"] == output.payload["effective_sampling"]
    assert restored_raw.read_bytes() == RAW_PROVIDER_BYTES
    assert restored_raw.with_name(f"{restored_raw.stem}.metadata.json").read_bytes() == (
        sidecar_bytes
    )
