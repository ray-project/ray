import time
from unittest.mock import MagicMock

import pytest

import ray
from ray.train.v2._internal.callbacks.metrics import (
    ControllerMetricsCallback,
    WorkerMetricsCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.metrics.base import MetricsTracker
from ray.train.v2._internal.metrics.worker import TRAIN_REPORT_TOTAL_BLOCKED_TIME_S
from ray.train.v2._internal.metrics.controller import (
    TRAIN_WORKER_GROUP_START_TOTAL_TIME_S,
    TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S,
)
from ray.train.v2.api.config import RunConfig


class MockGauge:
    """
    An Naive Mock class for `ray.util.metrics.Gauge`.

    This mock class initializes the value of the gauge to 0.0 and sets the value.
    The tags keys are not used in this mock class, just for API consistency.
    """

    def __init__(self, name: str, description: str, tag_keys: tuple = ()):
        self._value: float = 0.0

    def set(self, value: float, tags: dict):
        self._value = value

    def get(self):
        return self._value


def mock_on_report(self, value: float):
    """Mock function to set the value of the train_report_total_blocked_time"""
    self._metrics_tracker.update(TRAIN_REPORT_TOTAL_BLOCKED_TIME_S, {}, value)


def mock_on_worker_group_event(self, value: float, event: str):
    """Mock function to set the value of the worker group event"""
    if event == "start":
        self._metrics_tracker.update(TRAIN_WORKER_GROUP_START_TOTAL_TIME_S, {}, value)
    elif event == "shutdown":
        self._metrics_tracker.update(
            TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S, {}, value
        )


def test_worker_metrics_callback(monkeypatch):
    monkeypatch.setattr(MetricsTracker, "DEFAULT_METRICS_PUSH_INTERVAL_S", 0.05)
    monkeypatch.setattr(WorkerMetricsCallback, "on_report", mock_on_report)
    mock_train_context = MagicMock()
    mock_train_context.get_world_rank.return_value = 1
    mock_train_context.get_run_config.return_value = RunConfig(name="test_run_name")
    monkeypatch.setattr(
        ray.train.v2._internal.callbacks.metrics,
        "get_train_context",
        lambda: mock_train_context,
    )
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)

    callback = WorkerMetricsCallback(
        train_run_context=TrainRunContext(run_config=RunConfig(name="test_run_name"))
    )
    callback.after_init_train_context()

    # Check if the gauges is updated with the correct metrics
    callback.on_report(1.0)
    time.sleep(0.1)
    assert (
        callback._metrics_tracker.get_value(TRAIN_REPORT_TOTAL_BLOCKED_TIME_S, {})
        == 1.0
    )

    # Check if the gauges is updated with the correct metrics
    callback.on_report(1.0)
    time.sleep(0.1)
    assert (
        callback._metrics_tracker.get_value(TRAIN_REPORT_TOTAL_BLOCKED_TIME_S, {})
        == 2.0
    )


def test_controller_metrics_callback(monkeypatch):
    monkeypatch.setattr(MetricsTracker, "DEFAULT_METRICS_PUSH_INTERVAL_S", 0.05)
    monkeypatch.setattr(
        ControllerMetricsCallback, "on_worker_group_start", mock_on_worker_group_event
    )
    monkeypatch.setattr(
        ControllerMetricsCallback,
        "on_worker_group_shutdown",
        mock_on_worker_group_event,
    )
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)

    mock_train_context = MagicMock()
    mock_train_context.get_run_config.return_value = RunConfig(name="test_run_name")
    monkeypatch.setattr(
        ray.train.v2._internal.execution.context,
        "get_train_context",
        lambda: mock_train_context,
    )

    callback = ControllerMetricsCallback(
        train_run_context=TrainRunContext(run_config=RunConfig(name="test_run_name"))
    )
    callback.after_controller_start()

    # Check if the gauges is updated with the correct metrics
    callback.on_worker_group_start(2.0, "start")
    time.sleep(0.1)
    assert (
        callback._metrics_tracker.get_value(TRAIN_WORKER_GROUP_START_TOTAL_TIME_S, {})
        == 2.0
    )
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S, {}
        )
        is None
    )

    # Check if the gauges is updated with the correct metrics
    callback.on_worker_group_shutdown(1.0, "shutdown")
    time.sleep(0.1)
    assert (
        callback._metrics_tracker.get_value(TRAIN_WORKER_GROUP_START_TOTAL_TIME_S, {})
        == 2.0
    )
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S, {}
        )
        == 1.0
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
