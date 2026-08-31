from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.store import EventStore
from oracle_lab.worker_read_model import WorkerReadModel, WorkerReadModelError


def _service(tmp_path: Path) -> tuple[OracleLabService, str]:
    service = OracleLabService(EventStore(tmp_path / "oracle.db"), home=tmp_path / "home")
    session = service.new_session("worker read model", model_profile_id="test-model")
    return service, str(session["root_event_id"])


def test_worker_read_model_exposes_only_read_operations() -> None:
    assert tuple(inspect.signature(WorkerReadModel).parameters) == ("store",)
    assert {
        name
        for name, value in vars(WorkerReadModel).items()
        if not name.startswith("_") and callable(value)
    } == {"worker_task_status", "patch_show", "patch_status"}


@pytest.mark.parametrize(
    ("method_name", "message"),
    [
        ("worker_task_status", "event is not a worker task"),
        ("patch_show", "event is not a candidate patch"),
        ("patch_status", "event is not a candidate patch"),
    ],
)
def test_worker_read_model_errors_preserve_service_contract(
    tmp_path: Path,
    method_name: str,
    message: str,
) -> None:
    service, event_id = _service(tmp_path)
    read_model = WorkerReadModel(service.store)
    changes_before = service.store.connection.total_changes

    with pytest.raises(WorkerReadModelError) as internal_error:
        getattr(read_model, method_name)(event_id)
    assert str(internal_error.value) == message

    with pytest.raises(ServiceError) as service_error:
        getattr(service, method_name)(event_id)
    assert type(service_error.value) is ServiceError
    assert str(service_error.value) == message
    assert service.store.connection.total_changes == changes_before
