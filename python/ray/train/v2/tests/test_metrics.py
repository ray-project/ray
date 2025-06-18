import enum
from unittest.mock import MagicMock

import pytest

import ray
from ray.train.v2._internal.callbacks.metrics import (
    ControllerMetricsCallback,
    WorkerMetricsCallback,
)
from ray.train.v2._internal.execution.controller.state import (
    TrainControllerState,
    TrainControllerStateType,
)
from ray.train.v2._internal.metrics.base import EnumMetric, TimeMetric
from ray.train.v2._internal.metrics.controller import ControllerMetrics
from ray.train.v2._internal.metrics.worker import WorkerMetrics
from ray.train.v2.api.config import RunConfig
from ray.train.v2.tests.util import create_dummy_run_context


class MockGauge:
    """Mock class for ray.util.metrics.Gauge."""

    def __init__(self, name: str, description: str, tag_keys: tuple = ()):
        self._values: dict[set[str], float] = {}

    def set(self, value: float, tags: dict):
        self._values[frozenset(tags.items())] = value


@pytest.fixture
def mock_gauge(monkeypatch):
    """Fixture that replaces ray.util.metrics.Gauge with MockGauge."""
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)
    return MockGauge


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


def mock_start_end_time(monkeypatch, time_values: list[tuple[float, float]]):
    """Mock the time_monotonic function to return the start and end times.

    This assumes that time_monotonic is called in the order of the start and end times.
    """
    all_times = []
    for start, end in time_values:
        all_times.append(start)
        all_times.append(end)
    mock_time_monotonic(monkeypatch, all_times)


def test_time_metric(monkeypatch, mock_gauge):
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


def test_enum_metric(monkeypatch, mock_gauge):
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


def test_worker_metrics_callback(monkeypatch, mock_gauge):
    t1 = 0.0
    t2 = 1.0
    t3 = 10.0
    t4 = 12.0
    mock_start_end_time(monkeypatch, [(t1, t2), (t3, t4)])

    mock_train_context = MagicMock()
    mock_train_context.get_world_rank.return_value = 1
    mock_train_context.train_run_context = create_dummy_run_context()
    monkeypatch.setattr(
        ray.train.v2._internal.callbacks.metrics,
        "get_train_context",
        lambda: mock_train_context,
    )

    callback = WorkerMetricsCallback(train_run_context=create_dummy_run_context())
    callback.after_init_train_context()

    # Check if the gauges is updated with the correct metrics
    with callback.on_report():
        pass
    assert (
        callback._metrics[WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S].get_value()
        == t2 - t1
    )

    # Check if the gauges is updated with the correct metrics
    with callback.on_report():
        pass
    assert callback._metrics[WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S].get_value() == (
        t2 - t1
    ) + (t4 - t3)

    callback.before_shutdown()
    assert (
        callback._metrics[WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S].get_value() == 0.0
    )


def test_controller_metrics_callback(monkeypatch, mock_gauge):
    t1 = 0.0
    t2 = 1.0
    t3 = 10.0
    t4 = 12.0
    mock_start_end_time(monkeypatch, [(t1, t2), (t3, t4)])

    mock_train_context = MagicMock()
    mock_train_context.get_run_config.return_value = RunConfig(name="test_run_name")
    monkeypatch.setattr(
        ray.train.v2._internal.execution.context,
        "get_train_context",
        lambda: mock_train_context,
    )

    callback = ControllerMetricsCallback()
    callback.after_controller_start(train_run_context=create_dummy_run_context())

    # Check if the gauges is updated with the correct metrics
    with callback.on_worker_group_start():
        pass
    assert (
        callback._metrics[ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S].get_value()
        == t2 - t1
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
        == t2 - t1
    )
    assert (
        callback._metrics[
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S
        ].get_value()
        == t4 - t3
    )

    callback.before_controller_shutdown()
    assert (
        callback._metrics[ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S].get_value()
        == 0.0
    )
    assert (
        callback._metrics[
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S
        ].get_value()
        == 0.0
    )


def test_controller_state_metrics(monkeypatch, mock_gauge):
    """Test controller state transition metrics."""
    mock_train_context = MagicMock()
    mock_train_context.get_run_config.return_value = RunConfig(name="test_run_name")
    monkeypatch.setattr(
        ray.train.v2._internal.execution.context,
        "get_train_context",
        lambda: mock_train_context,
    )

    callback = ControllerMetricsCallback()
    callback.after_controller_start(train_run_context=create_dummy_run_context())

    # Test initial state
    assert (
        callback._metrics[ControllerMetrics.CONTROLLER_STATE].get_value(
            TrainControllerStateType.INITIALIZING
        )
        == 1
    )

    # Test state transition

    previous_state = TrainControllerState(TrainControllerStateType.INITIALIZING)
    current_state = TrainControllerState(TrainControllerStateType.RUNNING)
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
    previous_state = TrainControllerState(TrainControllerStateType.RUNNING)
    current_state = TrainControllerState(TrainControllerStateType.FINISHED)
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
