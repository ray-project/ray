import enum
from unittest.mock import MagicMock

import pytest

import ray
from ray.train.v2._internal.callbacks.metrics import (
    ControllerMetricsCallback,
    WorkerMetricsCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller.state import (
    TrainControllerStateType,
)
from ray.train.v2._internal.metrics.base import EnumMetric, TimeMetric
from ray.train.v2._internal.metrics.controller import ControllerMetrics
from ray.train.v2._internal.metrics.worker import WorkerMetrics
from ray.train.v2.api.config import RunConfig


class MockGauge:
    """Mock class for ray.util.metrics.Gauge."""

    def __init__(self, name: str, description: str, tag_keys: tuple = ()):
        self._values: dict[set[str], float] = {}

    def set(self, value: float, tags: dict):
        self._values[frozenset(tags.items())] = value

    def get(self, tags: dict):
        return self._values.get(frozenset(tags.items()))


class MockTrainControllerState:
    def __init__(self, state_type):
        self._state_type = state_type


def mock_on_report(self, value: float):
    """Mock function to set the value of the train_report_total_blocked_time"""
    self._metrics_tracker.record(WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S, value)


def mock_time_monotonic(monkeypatch, time_values: list[float]):
    time_index = 0

    def mock_time():
        nonlocal time_index
        value = time_values[time_index]
        time_index = time_index + 1
        return value

    monkeypatch.setattr(
        ray.train.v2._internal.callbacks.metrics, "time_monotonic", mock_time
    )


def test_time_metric(monkeypatch):
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)

    base_tags = {"run_name": "test_run"}
    metric = TimeMetric(
        name="test_time",
        description="Test time metric",
        base_tags=base_tags,
    )

    # Test recording values
    metric.record(1.0)
    assert metric.get_value() == 1.0

    # Test updating metric
    metric.record(2.0)
    assert metric.get_value() == 3.0

    # Test reset
    metric.reset()
    assert metric.get_value() == 0.0


def test_enum_metric(monkeypatch):
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)

    class TestEnum(enum.Enum):
        A = "A"
        B = "B"
        C = "C"

    base_tags = {"run_name": "test_run"}
    metric = EnumMetric[TestEnum](
        name="test_enum",
        description="Test enum metric",
        base_tags=base_tags,
        enum_tag_key="state",
    )

    # Test recording values
    metric.record(TestEnum.A)
    assert metric.get_value(TestEnum.A) == 1
    assert metric.get_value(TestEnum.B) == 0
    assert metric.get_value(TestEnum.C) == 0

    metric.record(TestEnum.B)
    assert metric.get_value(TestEnum.A) == 0
    assert metric.get_value(TestEnum.B) == 1
    assert metric.get_value(TestEnum.C) == 0

    metric.record(TestEnum.C)
    assert metric.get_value(TestEnum.A) == 0
    assert metric.get_value(TestEnum.B) == 0
    assert metric.get_value(TestEnum.C) == 1

    # Test reset
    metric.reset()
    assert metric.get_value(TestEnum.A) == 0
    assert metric.get_value(TestEnum.B) == 0
    assert metric.get_value(TestEnum.C) == 0


def test_worker_metrics_callback(monkeypatch):
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)
    mock_time_monotonic(monkeypatch, [0.0, 1.0, 10.0, 12.0])

    mock_train_context = MagicMock()
    mock_train_context.get_world_rank.return_value = 1
    mock_train_context.get_run_config.return_value = RunConfig(name="test_run_name")
    monkeypatch.setattr(
        ray.train.v2._internal.callbacks.metrics,
        "get_train_context",
        lambda: mock_train_context,
    )

    callback = WorkerMetricsCallback(
        train_run_context=TrainRunContext(run_config=RunConfig(name="test_run_name"))
    )
    callback.after_init_train_context()

    # Check if the gauges is updated with the correct metrics
    with callback.on_report():
        pass
    assert (
        callback._metrics[WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S].get_value() == 1.0
    )

    # Check if the gauges is updated with the correct metrics
    with callback.on_report():
        pass
    assert (
        callback._metrics[WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S].get_value() == 3.0
    )


def test_controller_metrics_callback(monkeypatch):
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)
    mock_time_monotonic(monkeypatch, [0.0, 1.0, 10.0, 12.0])

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
    with callback.on_worker_group_start():
        pass
    assert (
        callback._metrics[ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S].get_value()
        == 1.0
    )
    assert (
        callback._metrics[
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S
        ].get_value()
        == 0.0
    )

    # Check if the gauges is updated with the correct metrics
    with callback.on_worker_group_shutdown():
        pass
    assert (
        callback._metrics[ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S].get_value()
        == 1.0
    )
    assert (
        callback._metrics[
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S
        ].get_value()
        == 2.0
    )


def test_controller_state_metrics(monkeypatch):
    """Test controller state transition metrics."""
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

    # Test initial state
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.INITIALIZING
        )
        == 1
    )

    # Test state transition

    previous_state = MockTrainControllerState(TrainControllerStateType.INITIALIZING)
    current_state = MockTrainControllerState(TrainControllerStateType.RUNNING)
    callback.after_controller_state_update(previous_state, current_state)

    # Verify state counts
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.INITIALIZING
        )
        == 0
    )
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.RUNNING
        )
        == 1
    )

    # Test another state transition
    previous_state = MockTrainControllerState(TrainControllerStateType.RUNNING)
    current_state = MockTrainControllerState(TrainControllerStateType.FINISHED)
    callback.after_controller_state_update(previous_state, current_state)

    # Verify updated state counts
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.INITIALIZING
        )
        == 0
    )
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.RUNNING
        )
        == 0
    )
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.FINISHED
        )
        == 1
    )

    callback.before_controller_shutdown()

    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.INITIALIZING
        )
        == 0
    )
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.RUNNING
        )
        == 0
    )
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.FINISHED
        )
        == 0
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
