from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

import oracle_lab.cli as cli
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.tui import _mapping
from tests.support import historical_oracle_fixture

CONFIG = Path(__file__).parents[1] / "config"
HISTORICAL_SOURCE = Path(__file__).parent / "fixtures" / "oracle_output_001.md"

PRIVATE_COOKIE = "session=private-cookie-value"
PRIVATE_BEARER = "Bearer private-bearer-value"
PRIVATE_PREFIX = "sk_live_private_metadata_value"
PRIVATE_EVENT_METADATA = "opaque-private-event-metadata"
EXACT_ORACLE_TEXT = HISTORICAL_SOURCE.read_text(encoding="utf-8")


def _service_with_private_metadata(tmp_path: Path) -> tuple[OracleLabService, str]:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    session = service.new_session("private metadata")
    output = historical_oracle_fixture(
        EXACT_ORACLE_TEXT,
        source_path=HISTORICAL_SOURCE,
        session_id=session["id"],
        branch_id=session["current_branch_id"],
        parent_event_id=session["root_event_id"],
        causation_id=session["root_event_id"],
        payload_extra={
            "model": "deepseek-r1",
            "provider": "openrouter",
            "api_response_metadata": {
                "http_status": 200,
                "http_headers": {
                    "Set-Cookie": PRIVATE_COOKIE,
                    "AUTHORIZATION": PRIVATE_BEARER,
                    "x-request-id": "safe-request-id",
                },
                "diagnostic": PRIVATE_PREFIX,
                "provider_adapter": "openrouter",
            },
        },
    )
    service.store.append(output)
    return service, output.id


def _assert_public_event(value: Any, *, expect_oracle_text: bool = True) -> None:
    serialized = json.dumps(value, ensure_ascii=False)
    assert PRIVATE_COOKIE not in serialized
    assert PRIVATE_BEARER not in serialized
    assert PRIVATE_PREFIX not in serialized
    assert "[redacted]" in serialized
    assert "safe-request-id" in serialized
    if expect_oracle_text:
        assert _contains_exact_value(value, EXACT_ORACLE_TEXT)


def _contains_exact_value(value: Any, expected: str) -> bool:
    if value == expected:
        return True
    if isinstance(value, Mapping):
        return any(_contains_exact_value(item, expected) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_contains_exact_value(item, expected) for item in value)
    return False


def test_service_read_surfaces_redact_private_metadata_without_changing_canonical_event(
    tmp_path: Path,
) -> None:
    service, event_id = _service_with_private_metadata(tmp_path)
    canonical = service.store.require(event_id)

    assert canonical.payload["api_response_metadata"]["http_headers"]["Set-Cookie"] == (
        PRIVATE_COOKIE
    )
    assert canonical.payload["content"] == EXACT_ORACLE_TEXT

    for view in (
        service.show_session(canonical.session_id or ""),
        service.list_events(canonical.session_id),
        service.tail(),
        service.show_event(event_id),
        service.trace_event(event_id),
    ):
        _assert_public_event(view)
    _assert_public_event(service.generation_metadata(event_id), expect_oracle_text=False)

    canonical_after = service.store.require(event_id)
    assert canonical_after == canonical
    assert canonical_after.payload["api_response_metadata"]["diagnostic"] == PRIVATE_PREFIX


class _LeakingService:
    def show_event(self, event_id: str) -> dict[str, Any]:
        return {
            "id": event_id,
            "payload": {
                "content": EXACT_ORACLE_TEXT,
                "effective_sampling": {
                    "max_tokens": 4096,
                    "debug": PRIVATE_PREFIX,
                },
                "model_identity": {"routing_note": PRIVATE_PREFIX},
                "api_response_metadata": {
                    "http_headers": {
                        "sEt-CoOkIe": PRIVATE_COOKIE,
                        "x-request-id": "safe-request-id",
                    },
                    "diagnostic": PRIVATE_PREFIX,
                },
            },
            "metadata": {"secret": PRIVATE_EVENT_METADATA},
        }


@dataclass(frozen=True)
class _LeakingResult:
    payload: Mapping[str, Any]


class _ObjectLeakingService:
    def show_event(self, event_id: str) -> _LeakingResult:
        return _LeakingResult(payload={**_LeakingService().show_event(event_id)["payload"]})


def test_cli_final_emission_redacts_an_injected_leaking_service(monkeypatch: Any) -> None:
    monkeypatch.setattr(cli, "_service_factory", _LeakingService)
    assert PRIVATE_EVENT_METADATA in json.dumps(_LeakingService().show_event("evt_test"))

    result = CliRunner().invoke(cli.app, ["show", "evt_test"])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    _assert_public_event(parsed)
    assert PRIVATE_EVENT_METADATA not in result.output
    assert parsed["payload"]["effective_sampling"]["max_tokens"] == 4096


def test_cli_redacts_metadata_after_serializing_a_service_result_object(monkeypatch: Any) -> None:
    monkeypatch.setattr(cli, "_service_factory", _ObjectLeakingService)

    result = CliRunner().invoke(cli.app, ["show", "evt_test"])

    assert result.exit_code == 0, result.output
    _assert_public_event(json.loads(result.output))


def test_tui_mapping_redacts_private_metadata_but_preserves_oracle_text() -> None:
    value = _LeakingService().show_event("evt_test")
    assert value["metadata"]["secret"] == PRIVATE_EVENT_METADATA

    mapped = _mapping(value)

    _assert_public_event(mapped)
    assert PRIVATE_EVENT_METADATA not in json.dumps(mapped)
    assert mapped["payload"]["effective_sampling"]["max_tokens"] == 4096
    assert value["payload"]["api_response_metadata"]["diagnostic"] == PRIVATE_PREFIX
