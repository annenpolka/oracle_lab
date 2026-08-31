from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from oracle_lab.events import EventType
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventStore
from oracle_lab.usage import UsageService

CONFIG = Path(__file__).parents[1] / "config"


def _service_with_limits(
    tmp_path: Path,
    *,
    hard: float,
    warning: float,
) -> OracleLabService:
    service = OracleLabService(
        EventStore(tmp_path / "oracle.db"),
        home=tmp_path / "home",
        config_dir=CONFIG,
    )
    config = service.runtime_config
    policies = dataclasses.replace(
        config.policies,
        hard_limit_usd_per_day=hard,
        warn_limit_usd_per_session=warning,
    )
    service._config = dataclasses.replace(config, policies=policies)
    return service


def test_daily_hard_limit_records_error_and_prevents_provider_job(tmp_path: Path) -> None:
    service = _service_with_limits(tmp_path, hard=1.0, warning=0.5)
    session = service.new_session("cost hard limit")
    root = service.store.require(session["root_event_id"])
    UsageService(service.store).record(
        "oracle",
        request_event_id=root.id,
        provider_cost="1.00",
    )

    with pytest.raises(ServiceError, match="hard limit"):
        service.ask("continue")

    errors = service.store.list_events(event_type=EventType.ORACLE_ERROR)
    assert errors[-1].payload["error_type"] == "DailyCostHardLimit"
    assert service.list_jobs() == []


def test_session_warning_is_evented_once_without_blocking(tmp_path: Path) -> None:
    service = _service_with_limits(tmp_path, hard=10.0, warning=0.5)
    session = service.new_session("cost warning")
    root = service.store.require(session["root_event_id"])
    UsageService(service.store).record(
        "oracle",
        request_event_id=root.id,
        provider_cost="0.75",
    )

    service.ask("first")
    service.continue_session()

    warnings = [
        event
        for event in service.store.list_events(
            event_type=EventType.ANALYSIS_SESSION_SUMMARY_UPDATED
        )
        if event.payload.get("operation") == "cost.warning"
    ]
    assert len(warnings) == 1
    assert len(service.list_jobs()) == 2


def test_provider_rate_limit_is_evented_and_blocks_excess_request(tmp_path: Path) -> None:
    service = _service_with_limits(tmp_path, hard=10.0, warning=5.0)
    config = service.runtime_config
    provider = config.providers["openrouter"]
    providers = {
        **config.providers,
        "openrouter": dataclasses.replace(provider, requests_per_minute=1),
    }
    service._config = dataclasses.replace(config, providers=providers)
    service.new_session("rate limit")

    service.ask("first")
    with pytest.raises(ServiceError, match="rate limit"):
        service.continue_session()

    error = service.store.list_events(event_type=EventType.ORACLE_ERROR)[-1]
    assert error.payload["error_type"] == "ProviderRateLimit"
    assert len(service.list_jobs()) == 1
