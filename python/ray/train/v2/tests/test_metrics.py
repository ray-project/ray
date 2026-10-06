import asyncio
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
        enum_codes={TestEnum.A: 1, TestEnum.B: 2, TestEnum.C: 3},
    )
    assert metric.get_value() is None

    # Each value is recorded as its code on a single series.
    for value, code in [(TestEnum.A, 1), (TestEnum.B, 2), (TestEnum.C, 3)]:
        metric.record(value)
        assert metric.get_value() == value
        assert metric._gauge._values == {frozenset(base_tags.items()): code}

    # Reset records 0, meaning "no value".
    metric.reset()
    assert metric.get_value() is None
    assert metric._gauge._values == {frozenset(base_tags.items()): 0}


@pytest.mark.parametrize(
    "enum_codes",
    [{"A": 0, "B": 1}, {"A": 1, "B": 1}],
    ids=["zero_code", "duplicate_code"],
)
def test_enum_metric_invalid_codes(mock_gauge, enum_codes):
    with pytest.raises(ValueError):
        EnumMetric(
            name="test_enum",
            description="Test enum metric",
            base_tags={},
            enum_codes=enum_codes,
        )


def test_controller_state_codes_match_dashboard_panel():
    """The Grafana state timeline maps the gauge's codes back to state names."""
    from ray.dashboard.modules.metrics.dashboards.train_dashboard_panels import (
        CONTROLLER_STATE_CODES as PANEL_CONTROLLER_STATE_CODES,
    )

    assert set(ControllerMetrics.CONTROLLER_STATE_CODES) == set(
        TrainControllerStateType
    )
    assert {
        code: state.name
        for state, code in ControllerMetrics.CONTROLLER_STATE_CODES.items()
    } == {code: name for code, (name, _) in PANEL_CONTROLLER_STATE_CODES.items()}


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

    callback.before_worker_shutdown()
    assert (
        callback._metrics[WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S].get_value() == 0.0
    )


@pytest.mark.parametrize(
    "context_manager_name,metric_name",
    [
        ("on_checkpoint_sync", WorkerMetrics.CHECKPOINT_SYNC_TOTAL_TIME_S),
        ("on_checkpoint_transfer", WorkerMetrics.CHECKPOINT_TRANSFER_TOTAL_TIME_S),
    ],
)
def test_worker_checkpoint_metrics_callback(
    monkeypatch, mock_gauge, context_manager_name, metric_name
):
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

    context_manager = getattr(callback, context_manager_name)

    with context_manager():
        pass
    assert callback._metrics[metric_name].get_value() == t2 - t1

    with context_manager():
        pass
    assert callback._metrics[metric_name].get_value() == (t2 - t1) + (t4 - t3)

    callback.before_worker_shutdown()
    assert callback._metrics[metric_name].get_value() == 0.0


def test_worker_metrics_callback_shutdown_without_init(mock_gauge):
    """before_worker_shutdown should not crash when _metrics is None."""
    callback = WorkerMetricsCallback(train_run_context=create_dummy_run_context())
    # _metrics is None — after_init_train_context was never called
    callback.before_worker_shutdown()  # should not raise


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

    asyncio.run(callback.before_controller_shutdown())
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

    state_metric = callback._metrics[ControllerMetrics.CONTROLLER_STATE]

    def recorded_code():
        (code,) = state_metric._gauge._values.values()
        return code

    # Test initial state
    assert state_metric.get_value() == TrainControllerStateType.INITIALIZING
    assert (
        recorded_code()
        == ControllerMetrics.CONTROLLER_STATE_CODES[
            TrainControllerStateType.INITIALIZING
        ]
    )

    # Test state transitions
    for previous_type, current_type in [
        (TrainControllerStateType.INITIALIZING, TrainControllerStateType.RUNNING),
        (TrainControllerStateType.RUNNING, TrainControllerStateType.FINISHED),
    ]:
        callback.after_controller_state_update(
            TrainControllerState(previous_type), TrainControllerState(current_type)
        )
        assert state_metric.get_value() == current_type
        assert recorded_code() == ControllerMetrics.CONTROLLER_STATE_CODES[current_type]

    asyncio.run(callback.before_controller_shutdown())

    assert state_metric.get_value() is None
    assert recorded_code() == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
