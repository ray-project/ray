import gc
import sys
import uuid
import weakref
from unittest.mock import Mock

import pytest
from celery import Celery, signals
from celery.utils.dispatch import Signal

from ray.serve import task_processor
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig


@pytest.fixture
def create_adapter(monkeypatch, request):
    for signal_name in ("task_failure", "task_unknown"):
        signal = Signal(name=signal_name)
        monkeypatch.setattr(signals, signal_name, signal)
        monkeypatch.setattr(task_processor, signal_name, signal)

    def create():
        queue = f"test_queue_{uuid.uuid4().hex}"
        adapter = task_processor.CeleryTaskProcessorAdapter(
            TaskProcessorConfig(
                queue_name=queue,
                failed_task_queue_name=f"{queue}_failed",
                unprocessable_task_queue_name=f"{queue}_unknown",
                adapter_config=CeleryAdapterConfig(
                    broker_url="memory://", backend_url="cache+memory://"
                ),
            )
        )
        adapter.initialize()
        monkeypatch.setattr(adapter._app, "send_task", Mock())
        request.addfinalizer(adapter._app.close)
        return adapter

    return create


def fail_task(app):
    @app.task(name=f"{app.main}.failure", lazy=False, shared=False)
    def fail():
        raise ValueError("Expected task failure")

    result = fail.apply(throw=False)
    assert result.status == "FAILURE"
    return result.id


@pytest.mark.parametrize("owner_index", [0, 1])
def test_task_failure_routes_only_to_owning_adapter(create_adapter, owner_index):
    adapters = [create_adapter(), create_adapter()]
    owner = adapters[owner_index]
    task_id = fail_task(owner._app)

    owner._app.send_task.assert_called_once()
    sent = owner._app.send_task.call_args.kwargs
    assert sent["queue"] == owner._config.failed_task_queue_name
    assert sent["args"][0] == task_id
    adapters[1 - owner_index]._app.send_task.assert_not_called()


@pytest.mark.parametrize("owner_index", [0, 1])
def test_unknown_task_routes_only_to_owning_adapter(create_adapter, owner_index):
    adapters = [create_adapter(), create_adapter()]
    owner = adapters[owner_index]
    signals.task_unknown.send(
        sender=Mock(app=owner._app),
        name="missing_task",
        id="unknown-task-id",
        message="Unregistered task",
        exc=LookupError("missing_task"),
    )

    owner._app.send_task.assert_called_once()
    sent = owner._app.send_task.call_args.kwargs
    assert sent["queue"] == owner._config.unprocessable_task_queue_name
    assert sent["args"][:2] == ["missing_task", "unknown-task-id"]
    adapters[1 - owner_index]._app.send_task.assert_not_called()


def test_unrelated_celery_app_does_not_use_adapter_dead_letter_queues(create_adapter):
    adapters = [create_adapter(), create_adapter()]
    with Celery("unrelated", broker="memory://", backend="cache+memory://") as app:
        fail_task(app)
        signals.task_unknown.send(
            sender=Mock(app=app), name="unrelated_task", id="unrelated-task-id"
        )

    for adapter in adapters:
        adapter._app.send_task.assert_not_called()


def test_signal_receivers_do_not_keep_adapter_alive(create_adapter):
    first = create_adapter()
    second = create_adapter()
    first_reference = weakref.ref(first)
    del first
    gc.collect()
    assert first_reference() is None

    fail_task(second._app)
    second._app.send_task.assert_called_once()
    assert (
        second._app.send_task.call_args.kwargs["queue"]
        == second._config.failed_task_queue_name
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
