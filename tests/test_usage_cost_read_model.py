from __future__ import annotations

import dataclasses
import datetime as dt
import inspect
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import oracle_lab.cli as cli
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.services import OracleLabService
from oracle_lab.store import EventStore
from oracle_lab.usage_cost_read_model import UsageCostReadModel

CONFIG = Path(__file__).parents[1] / "config"
SUMMARY_KEYS = (
    "records",
    "prompt_tokens",
    "completion_tokens",
    "reasoning_tokens",
    "provider_cost",
    "request_count",
)


def _service(tmp_path: Path) -> OracleLabService:
    return OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )


def _append_usage(
    store: EventStore,
    *,
    kind: EventType = EventType.USAGE_ORACLE,
    session_id: str,
    model_id: str,
    created_at: dt.datetime,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    reasoning_tokens: int | None = None,
    provider_cost: str | None = None,
    request_count: int = 1,
) -> Event:
    return store.append(
        Event.new(
            kind,
            actor=Actor(kind=ActorKind.SYSTEM, id="usage-test"),
            session_id=session_id,
            created_at=created_at,
            payload={
                "model_id": model_id,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "reasoning_tokens": reasoning_tokens,
                "provider_cost": provider_cost,
                "latency_ms": 0,
                "request_count": request_count,
            },
        )
    )


def test_usage_cost_read_model_exposes_only_two_read_operations() -> None:
    assert tuple(inspect.signature(UsageCostReadModel).parameters) == ("store",)
    assert {
        name
        for name, value in vars(UsageCostReadModel).items()
        if not name.startswith("_") and callable(value)
    } == {"cost_summary", "oracle_cost_records"}


def test_empty_cost_summary_preserves_key_order_types_and_read_only_facade(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    read_model = UsageCostReadModel(service.store)
    event_ids_before = tuple(event.id for event in service.store.list_events())
    changes_before = service.store.connection.total_changes
    control_path = service.home / "control.json"

    summary = read_model.cost_summary()

    assert tuple(summary) == SUMMARY_KEYS
    assert summary == {
        "records": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "reasoning_tokens": 0,
        "provider_cost": 0.0,
        "request_count": 0,
    }
    assert all(type(summary[key]) is int for key in SUMMARY_KEYS if key != "provider_cost")
    assert type(summary["provider_cost"]) is float
    assert service.cost() == summary
    assert tuple(event.id for event in service.store.list_events()) == event_ids_before
    assert service.store.connection.total_changes == changes_before
    assert not control_path.exists()


def test_cost_summary_preserves_filters_aggregates_and_facade_shape(tmp_path: Path) -> None:
    service = _service(tmp_path)
    base = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
    _append_usage(
        service.store,
        session_id="ses_a",
        model_id="model_a",
        created_at=base,
        prompt_tokens=10,
        completion_tokens=5,
        provider_cost="1.25",
        request_count=3,
    )
    _append_usage(
        service.store,
        kind=EventType.USAGE_HOST,
        session_id="ses_a",
        model_id="model_a",
        created_at=base + dt.timedelta(seconds=1),
        prompt_tokens=20,
        completion_tokens=4,
        reasoning_tokens=2,
        provider_cost="2.50",
    )
    _append_usage(
        service.store,
        session_id="ses_a",
        model_id="model_b",
        created_at=base + dt.timedelta(seconds=2),
        prompt_tokens=7,
        completion_tokens=8,
        reasoning_tokens=3,
        provider_cost=None,
        request_count=2,
    )
    _append_usage(
        service.store,
        kind=EventType.USAGE_TOOL,
        session_id="ses_b",
        model_id="model_a",
        created_at=base + dt.timedelta(seconds=3),
        prompt_tokens=1,
        completion_tokens=2,
        provider_cost="4.00",
        request_count=4,
    )
    read_model = UsageCostReadModel(service.store)
    event_ids_before = tuple(event.id for event in service.store.list_events())
    changes_before = service.store.connection.total_changes

    cases: list[tuple[dict[str, Any], dict[str, Any]]] = [
        (
            {},
            {
                "records": 4,
                "prompt_tokens": 38,
                "completion_tokens": 19,
                "reasoning_tokens": 5,
                "provider_cost": 7.75,
                "request_count": 10,
            },
        ),
        (
            {"session_id": "ses_a"},
            {
                "records": 3,
                "prompt_tokens": 37,
                "completion_tokens": 17,
                "reasoning_tokens": 5,
                "provider_cost": 3.75,
                "request_count": 6,
            },
        ),
        (
            {"model_id": "model_a"},
            {
                "records": 3,
                "prompt_tokens": 31,
                "completion_tokens": 11,
                "reasoning_tokens": 2,
                "provider_cost": 7.75,
                "request_count": 8,
            },
        ),
        (
            {"session_id": "ses_a", "model_id": "model_a"},
            {
                "records": 2,
                "prompt_tokens": 30,
                "completion_tokens": 9,
                "reasoning_tokens": 2,
                "provider_cost": 3.75,
                "request_count": 4,
            },
        ),
    ]
    for filters, expected in cases:
        summary = read_model.cost_summary(**filters)
        assert tuple(summary) == SUMMARY_KEYS
        assert summary == expected
        assert service.cost(**filters) == expected

    global_summary = cases[0][1]
    assert read_model.cost_summary(session_id="") == global_summary
    assert read_model.cost_summary(model_id="") == global_summary
    assert read_model.cost_summary(session_id="", model_id="") == global_summary
    assert service.cost(session_id="", model_id="") == global_summary
    assert tuple(event.id for event in service.store.list_events()) == event_ids_before
    assert service.store.connection.total_changes == changes_before
    assert not (service.home / "control.json").exists()


def test_oracle_cost_records_preserve_sql_yield_order_columns_and_scope(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    base = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
    _append_usage(
        service.store,
        session_id="ses_new",
        model_id="model_a",
        created_at=base,
        provider_cost="2.00",
    )
    _append_usage(
        service.store,
        kind=EventType.USAGE_HOST,
        session_id="ses_host",
        model_id="model_a",
        created_at=base + dt.timedelta(days=1),
        provider_cost="99.00",
    )
    _append_usage(
        service.store,
        session_id="ses_null",
        model_id="model_a",
        created_at=base + dt.timedelta(days=2),
        provider_cost=None,
    )
    _append_usage(
        service.store,
        session_id="ses_old",
        model_id="model_b",
        created_at=base - dt.timedelta(days=365),
        provider_cost="1.00",
    )
    _append_usage(
        service.store,
        session_id="ses_new",
        model_id="model_a",
        created_at=base - dt.timedelta(days=1),
        provider_cost="2.00",
    )
    read_model = UsageCostReadModel(service.store)
    expected = [
        dict(row)
        for row in service.store.connection.execute(
            """
            SELECT provider_cost, created_at, session_id
            FROM usage_records
            WHERE kind = 'oracle' AND provider_cost IS NOT NULL
            """
        ).fetchall()
    ]
    event_ids_before = tuple(event.id for event in service.store.list_events())
    changes_before = service.store.connection.total_changes

    records = read_model.oracle_cost_records()

    assert type(records) is list
    assert records == expected
    assert [tuple(record) for record in records] == [
        ("provider_cost", "created_at", "session_id")
    ] * 3
    assert sorted(
        (record["provider_cost"], record["created_at"], record["session_id"]) for record in records
    ) == sorted(
        [
            ("2.00", base.isoformat(), "ses_new"),
            ("1.00", (base - dt.timedelta(days=365)).isoformat(), "ses_old"),
            ("2.00", (base - dt.timedelta(days=1)).isoformat(), "ses_new"),
        ]
    )
    assert [record["provider_cost"] for record in records] == [
        record["provider_cost"] for record in expected
    ]
    assert tuple(event.id for event in service.store.list_events()) == event_ids_before
    assert service.store.connection.total_changes == changes_before


def test_cost_safeguard_keeps_date_and_decimal_policy_in_service(tmp_path: Path) -> None:
    service = _service(tmp_path)
    session = service.new_session("cost safeguard boundary")
    config = service.runtime_config
    service._config = dataclasses.replace(
        config,
        policies=dataclasses.replace(
            config.policies,
            hard_limit_usd_per_day=1.0,
            warn_limit_usd_per_session=1.0,
        ),
    )
    now = dt.datetime.now(dt.UTC)
    _append_usage(
        service.store,
        session_id=str(session["id"]),
        model_id="model_a",
        created_at=now - dt.timedelta(days=2),
        provider_cost="2.00",
    )
    _append_usage(
        service.store,
        session_id=str(session["id"]),
        model_id="model_a",
        created_at=now,
        provider_cost="0.25",
    )
    _append_usage(
        service.store,
        kind=EventType.USAGE_HOST,
        session_id=str(session["id"]),
        model_id="host_model",
        created_at=now,
        provider_cost="100.00",
    )

    service.ask("continue")

    errors = service.store.list_events(event_type=EventType.ORACLE_ERROR)
    warnings = [
        event
        for event in service.store.list_events(
            event_type=EventType.ANALYSIS_SESSION_SUMMARY_UPDATED
        )
        if event.payload.get("operation") == "cost.warning"
    ]
    assert errors == []
    assert len(warnings) == 1
    assert warnings[0].payload["session_cost_usd"] == "2.25"
    assert len(service.list_jobs()) == 1


@pytest.mark.parametrize(
    "operation",
    ("read_model_summary", "read_model_records", "service_summary"),
)
def test_usage_cost_queries_propagate_sqlite_errors_unchanged(
    tmp_path: Path,
    operation: str,
) -> None:
    service = _service(tmp_path)
    read_model = UsageCostReadModel(service.store)
    service.store.connection.execute("DROP TABLE usage_records")

    with pytest.raises(sqlite3.OperationalError) as captured:
        if operation == "read_model_summary":
            read_model.cost_summary()
        elif operation == "read_model_records":
            read_model.oracle_cost_records()
        else:
            service.cost()

    assert type(captured.value) is sqlite3.OperationalError
    assert str(captured.value) == "no such table: usage_records"


def test_cost_cli_preserves_json_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service(tmp_path)
    _append_usage(
        service.store,
        session_id="ses_cli",
        model_id="model_cli",
        created_at=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
        prompt_tokens=4,
        completion_tokens=2,
        provider_cost="1.50",
        request_count=2,
    )
    monkeypatch.setattr(cli, "_service_factory", lambda: service)

    result = CliRunner().invoke(
        cli.app,
        ["cost", "--session", "ses_cli", "--model", "model_cli"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "records": 1,
        "prompt_tokens": 4,
        "completion_tokens": 2,
        "reasoning_tokens": 0,
        "provider_cost": 1.5,
        "request_count": 2,
    }
