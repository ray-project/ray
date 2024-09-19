import time
from unittest.mock import MagicMock

import pytest

import ray
from ray.train.v2._internal.callbacks.metrics import WorkerMetricsCallback


class MockGauge:
    """
    An Naive Mock class for `ray.util.metrics.Gauge`.

    This mock class initializes the value of the gauge to 0.0 and sets the value.
    The tags keys are not used in this mock class, just for API consistency.
    """

    def __init__(self, name: str, description: str, tag_keys: tuple):
        self._value: float = 0.0

    def set(self, value: float, tags: dict):
        self._value = value

    def get(self):
        return self._value


def mock_on_report(self, value: float):
    """Mock function to set the value of the train_report_total_blocked_time"""
    self._metrics.train_report_total_blocked_time += value


def test_worker_metrics_callback(monkeypatch):
    monkeypatch.setattr(WorkerMetricsCallback, "LOCAL_METRICS_PUSH_INTERVAL_S", 0.05)
    monkeypatch.setattr(WorkerMetricsCallback, "on_report", mock_on_report)
    mock_train_context = MagicMock()
    mock_train_context.get_world_rank.return_value = 1
    mock_train_context.get_experiment_name.return_value = "test_experiment"
    monkeypatch.setattr(
        ray.train.v2._internal.callbacks.metrics,
        "get_train_context",
        lambda: mock_train_context,
    )
    monkeypatch.setattr(ray.train.v2._internal.callbacks.metrics, "Gauge", MockGauge)

    callback = WorkerMetricsCallback()
    callback.after_init_train_context()

    # Check if the gauges is updated with the correct metrics
    callback.on_report(1.0)
    time.sleep(0.1)
    assert callback._metrics_gauges["train_report_total_blocked_time"].get() == 1.0

    # Check if the gauges is updated with the correct metrics
    callback.on_report(1.0)
    time.sleep(0.1)
    assert callback._metrics_gauges["train_report_total_blocked_time"].get() == 2.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
