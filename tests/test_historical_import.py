from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import oracle_lab.cli as cli
from oracle_lab.events import EventType
from oracle_lab.historical_import import HistoricalImportError, HistoricalSessionImporter
from oracle_lab.jsonutil import sha256_bytes
from oracle_lab.replay import ReplayService
from oracle_lab.services import OracleLabService
from oracle_lab.session import SessionContextBuilder
from oracle_lab.store import EventStore

CONFIG = Path(__file__).parents[1] / "config"


class ForbiddenProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def generate(self, _request: Any) -> Any:
        self.calls += 1
        raise AssertionError("historical import and exact replay must not query a provider")


def _service(tmp_path: Path, *, provider: ForbiddenProvider | None = None) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
        provider_factory=None if provider is None else lambda _profile: provider,
    )


def test_json_import_preserves_exact_text_timestamps_unknowns_and_visible_roles(
    tmp_path: Path,
) -> None:
    source = tmp_path / "old-conversation.json"
    document = {
        "title": "old run",
        "metadata": {"api_key": "must-not-enter-the-event-log"},
        "messages": [
            {
                "role": "system",
                "content": "System line 1\r\n  System line 2  ",
                "created_at": "2024-01-02T03:04:05+09:00",
            },
            {
                "role": "user",
                "content": "\t確認しろ。  \n\n",
                "timestamp": "2024-01-02T03:05:00Z",
            },
            {
                "id": "legacy-message-3",
                "role": "assistant",
                "content": "# malformed  \n```sh\n$ echo  x\n```\n\\[x=  1",
                "timestamp": "2024-01-02T03:06:00+00:00",
            },
            {
                "role": "tool",
                "content": "$ printf '153792'\n153792\n",
                "truth_domain": "virtual",
                "timestamp": "2024-01-02T03:07:00Z",
                "tool_call_id": "historical-call-1",
            },
        ],
    }
    raw = json.dumps(document, ensure_ascii=False).encode()
    source.write_bytes(raw)
    service = _service(tmp_path)

    imported = service.import_session(source)

    message_events = [service.store.require(event_id) for event_id in imported["message_event_ids"]]
    assert [event.type for event in message_events] == [
        EventType.ORACLE_CONTEXT_MESSAGE,
        EventType.HUMAN_INPUT,
        EventType.ORACLE_OUTPUT,
        EventType.ORACLE_CONTEXT_MESSAGE,
    ]
    assert [event.payload["content"] for event in message_events] == [
        item["content"] for item in document["messages"]
    ]
    assert message_events[0].created_at == dt.datetime.fromisoformat("2024-01-02T03:04:05+09:00")
    assert message_events[1].created_at == dt.datetime(2024, 1, 2, 3, 5, tzinfo=dt.UTC)
    output = message_events[2]
    assert output.payload["material_origin"] == "historical_fixture"
    assert output.metadata["historical_fixture"] is True
    assert output.payload["raw_sha256"]
    assert output.payload["original_event_id"] == "legacy-message-3"
    assert output.payload["model_identity"]["actual_provider"] is None
    assert output.payload["model_identity"]["actual_model_identifier"] is None
    assert output.payload["sampling"] is None
    assert "actual_provider" in output.payload["model_identity"]["unknown_fields"]
    assert message_events[3].actor.kind.value == "tool"
    assert message_events[3].payload["truth_domain"] == "virtual"
    assert message_events[3].payload["truth_domain_status"] == "known_historical"
    assert message_events[3].payload["truth_domain_unknown_reason"] is None
    serialized = json.dumps(service.list_events(imported["session_id"]), ensure_ascii=False)
    assert "must-not-enter-the-event-log" not in serialized
    assert imported["source_file"]["sha256"] == sha256_bytes(raw)
    assert imported["source_file"]["path"].endswith(source.name)
    assert service.list_jobs() == []
    assert service.claims() == []
    assert service.motifs() == []

    context = SessionContextBuilder().build(
        service.store.list_events(session_id=imported["session_id"]),
        session_id=imported["session_id"],
        branch_id=imported["branch_id"],
        tip_event_id=imported["import_event_id"],
    )
    assert context.provider_messages() == [
        {"role": "system", "content": document["messages"][0]["content"]},
        {"role": "user", "content": document["messages"][1]["content"]},
        {"role": "assistant", "content": document["messages"][2]["content"]},
        {
            "role": "tool",
            "content": document["messages"][3]["content"],
            "tool_call_id": "historical-call-1",
        },
    ]
    assert ReplayService(service.store).context_from_event(output.id) == [
        {"role": "system", "content": document["messages"][0]["content"]},
        {"role": "user", "content": document["messages"][1]["content"]},
    ]
    assert service.search("malformed")[0]["event_id"] == output.id


def test_jsonl_import_is_starting_state_fork_point_replay_fixture_and_cli_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "conversation.jsonl"
    lines = [
        {
            "type": "metadata",
            "title": "fixture",
            "provider": "historical-provider",
            "model": "returned-r1-checkpoint",
            "temperature": 0.6,
        },
        {"role": "human", "value": "測れ。", "create_time": 1_700_000_000},
        {
            "role": "oracle",
            "value": "42.72 hours",
            "create_time": 1_700_000_001,
        },
        {"role": "user", "content": "もう一度見ろ。", "timestamp": 1_700_000_002_000},
    ]
    source.write_text(
        "\n".join(json.dumps(line, ensure_ascii=False) for line in lines) + "\n",
        encoding="utf-8",
    )
    provider = ForbiddenProvider()
    service = _service(tmp_path, provider=provider)
    monkeypatch.setattr(cli, "_service_factory", lambda: service)

    result = CliRunner().invoke(cli.app, ["session", "import", str(source)])

    assert result.exit_code == 0, result.output
    imported = json.loads(result.output)
    assert imported["session"]["model_profile_id"] is None
    assistant = service.store.require(imported["assistant_event_ids"][0])
    assert assistant.payload["provider"] == "historical-provider"
    assert assistant.payload["provider_model_id"] == "returned-r1-checkpoint"
    assert assistant.payload["sampling"] == {"temperature": 0.6}
    assert provider.calls == 0

    final_context = ReplayService(service.store).context_from_event(imported["import_event_id"])
    assert [message["content"] for message in final_context] == [
        "測れ。",
        "42.72 hours",
        "もう一度見ろ。",
    ]
    replay = service.replay_exact(
        session_id=imported["session_id"],
        branch_id=imported["branch_id"],
        record=False,
    )
    assert set(imported["message_event_ids"]) <= set(replay["input_event_ids"])
    assert provider.calls == 0

    continuation = service.continue_session(model_profile_id="r1-initial-openrouter")
    request_id = continuation["request"]["id"]
    continued_context = SessionContextBuilder().build(
        service.store.list_events(session_id=imported["session_id"]),
        session_id=imported["session_id"],
        branch_id=imported["branch_id"],
        tip_event_id=request_id,
    )
    assert continued_context.provider_messages() == final_context
    assert provider.calls == 0

    fork_source = imported["message_event_ids"][0]
    child = service.fork(fork_source, "historical-prefix")
    child_tip = service.store.list_events(branch_id=child["id"])[0]
    child_context = SessionContextBuilder().build(
        service.store.list_events(session_id=imported["session_id"]),
        session_id=imported["session_id"],
        branch_id=child["id"],
        tip_event_id=child_tip.id,
    )
    assert child_context.provider_messages() == [{"role": "user", "content": "測れ。"}]


def test_invalid_import_is_atomic_and_does_not_create_a_session(tmp_path: Path) -> None:
    source = tmp_path / "invalid.json"
    source.write_text(json.dumps([{"role": "assistant", "content": {"not": "text"}}]))
    store = EventStore(tmp_path / "oracle.db")

    with pytest.raises(HistoricalImportError, match="exact string content"):
        HistoricalSessionImporter(store).import_file(source)

    assert store.count_events() == 0
    assert store.connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0


def test_historical_tool_message_preserves_unknown_truth_domain_without_promotion(
    tmp_path: Path,
) -> None:
    source = tmp_path / "unknown-tool-domain.json"
    content = "$ legacy-command\nopaque historical result\n"
    source.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "role": "tool",
                        "content": content,
                        "tool_call_id": "legacy-tool-call",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    service = _service(tmp_path)

    imported = service.import_session(source)

    event = service.store.require(imported["message_event_ids"][0])
    assert event.type == EventType.ORACLE_CONTEXT_MESSAGE
    assert event.actor.kind.value == "tool"
    assert event.payload["content"] == content
    assert event.payload["message"] == {
        "role": "tool",
        "content": content,
        "tool_call_id": "legacy-tool-call",
    }
    assert event.payload["truth_domain"] is None
    assert event.payload["truth_domain_status"] == "unknown_historical"
    assert event.payload["truth_domain_unknown_reason"] == "not_present_in_source"
    assert not [
        item
        for item in service.store.list_events(session_id=imported["session_id"])
        if item.type == EventType.TOOL_OUTPUT
    ]


@pytest.mark.parametrize("truth_domain", ["", "unknown", "host", "oracle"])
def test_historical_tool_message_rejects_non_domain_labels_atomically(
    tmp_path: Path,
    truth_domain: str,
) -> None:
    source = tmp_path / "invalid-tool-domain.json"
    source.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "role": "tool",
                        "content": "opaque",
                        "truth_domain": truth_domain,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    store = EventStore(tmp_path / "oracle.db")

    with pytest.raises(HistoricalImportError, match="invalid truth_domain"):
        HistoricalSessionImporter(store).import_file(source)

    assert store.count_events() == 0
    assert store.connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0
