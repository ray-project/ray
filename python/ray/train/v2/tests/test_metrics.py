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
from ray.train.v2._internal.metrics.base import Metric, MetricsTracker
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


def mock_on_report(self, value: float):
    """Mock function to set the value of the train_report_total_blocked_time"""
    self._metrics_tracker.record(WorkerMetrics.TRAIN_REPORT_TOTAL_BLOCKED_TIME_S, value)


def mock_on_worker_group_event(self, value: float, event: str):
    """Mock function to set the value of the worker group event"""
    if event == "start":
        self._metrics_tracker.record(
            ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S, value
        )
    elif event == "shutdown":
        self._metrics_tracker.record(
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S, value
        )


def test_metrics_tracker(monkeypatch):
    """Test the core functionality of MetricsTracker."""
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)

    base_tags = {"base_tag": "base_value"}

    test_metric = Metric(
        name="test_metric",
        type=float,
        default=0.0,
        description="Test metric",
        tag_keys=["base_tag"],
    )

    test_metric_with_tags = Metric(
        name="test_metric_with_tags",
        type=float,
        default=0.0,
        description="Test metric with tags",
        tag_keys=["base_tag", "tag1"],
    )

    # Initialize tracker with base tags
    metrics_dict = {m.name: m for m in [test_metric, test_metric_with_tags]}
    tracker = MetricsTracker(metrics_dict, base_tags)
    tracker.start()

    # Test initial state
    assert tracker.get_value(test_metric.name) == 0.0
    assert (
        tracker.get_value(
            test_metric_with_tags.name, additional_tags={"tag1": "value1"}
        )
        == 0.0
    )

    # Test updating metric with additional tags
    tracker.record(test_metric_with_tags.name, 1.0, additional_tags={"tag1": "value1"})
    assert (
        tracker.get_value(
            test_metric_with_tags.name, additional_tags={"tag1": "value1"}
        )
        == 1.0
    )

    # Test updating same metric-tag combination
    tracker.record(test_metric_with_tags.name, 2.0, additional_tags={"tag1": "value1"})
    assert (
        tracker.get_value(
            test_metric_with_tags.name, additional_tags={"tag1": "value1"}
        )
        == 3.00
    )

    # Test shutdown
    tracker.shutdown()
    for metric in tracker._metrics.values():
        assert metric._values == {}


def test_metrics_tracker_unknown_metric():
    """Test that updating an unknown metric raises an error."""
    tracker = MetricsTracker({}, {})
    unknown_metric = Metric(
        name="unknown_metric",
        type=float,
        default=0.0,
        description="Unknown metric",
        tag_keys=[],
    )

    with pytest.raises(ValueError, match="Unknown metric: unknown_metric"):
        tracker.record(unknown_metric.name, 1.0)


def test_metrics_tracker_reset(monkeypatch):
    """Test that all metric-tag combinations are properly reset."""
    monkeypatch.setattr(ray.train.v2._internal.metrics.base, "Gauge", MockGauge)

    # Create test metrics
    test_metric1 = Metric(
        name="test_metric1",
        type=float,
        default=0.0,
        description="Test metric 1",
        tag_keys=["base_tag"],
    )
    test_metric2 = Metric(
        name="test_metric2",
        type=int,
        default=0,
        description="Test metric 2",
        tag_keys=["base_tag", "tag1"],
    )

    # Initialize tracker with base tags
    base_tags = {"base_tag": "base_value"}
    metrics_dict = {m.name: m for m in [test_metric1, test_metric2]}
    tracker = MetricsTracker(metrics_dict, base_tags)
    tracker.start()

    tracker.record(test_metric1.name, 1.0)
    tracker.record(test_metric2.name, 2, additional_tags={"tag1": "value1"})
    tracker.record(test_metric2.name, 3, additional_tags={"tag1": "value2"})

    # Verify all values are set
    assert tracker.get_value(test_metric1.name) == 1.0
    assert tracker.get_value(test_metric2.name, additional_tags={"tag1": "value1"}) == 2
    assert tracker.get_value(test_metric2.name, additional_tags={"tag1": "value2"}) == 3

    # Reset gauges
    tracker.reset()

    # Verify all values are reset to default
    assert tracker.get_value(test_metric1.name) == 0.0
    assert tracker.get_value(test_metric2.name, additional_tags={"tag1": "value1"}) == 0
    assert tracker.get_value(test_metric2.name, additional_tags={"tag1": "value2"}) == 0

    tracker.shutdown()


def test_worker_metrics_callback(monkeypatch):
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
    assert (
        callback._metrics_tracker.get_value(
            WorkerMetrics.TRAIN_REPORT_TOTAL_BLOCKED_TIME_S
        )
        == 1.0
    )

    # Check if the gauges is updated with the correct metrics
    callback.on_report(2.0)
    assert (
        callback._metrics_tracker.get_value(
            WorkerMetrics.TRAIN_REPORT_TOTAL_BLOCKED_TIME_S
        )
        == 3.0
    )


def test_controller_metrics_callback(monkeypatch):
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
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S
        )
        == 2.0
    )
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S
        )
        == 0.0
    )

    # Check if the gauges is updated with the correct metrics
    callback.on_worker_group_shutdown(1.0, "shutdown")
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S
        )
        == 2.0
    )
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S
        )
        == 1.0
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
        callback._metrics_tracker.get_value(
            ControllerMetrics.CONTROLLER_STATE,
            {
                ControllerMetrics.CONTROLLER_STATE_TAG_KEY: TrainControllerStateType.INITIALIZING.name
            },
        )
        == 1
    )

    # Test state transition
    class MockState:
        def __init__(self, state_type):
            self._state_type = state_type

    previous_state = MockState(TrainControllerStateType.INITIALIZING)
    current_state = MockState(TrainControllerStateType.RUNNING)
    callback.after_controller_state_update(previous_state, current_state)

    # Verify state counts
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.CONTROLLER_STATE,
            {
                ControllerMetrics.CONTROLLER_STATE_TAG_KEY: TrainControllerStateType.INITIALIZING.name
            },
        )
        == 0
    )
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.CONTROLLER_STATE,
            {
                ControllerMetrics.CONTROLLER_STATE_TAG_KEY: TrainControllerStateType.RUNNING.name
            },
        )
        == 1
    )

    # Test another state transition
    previous_state = MockState(TrainControllerStateType.RUNNING)
    current_state = MockState(TrainControllerStateType.FINISHED)
    callback.after_controller_state_update(previous_state, current_state)

    # Verify updated state counts
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.CONTROLLER_STATE,
            {
                ControllerMetrics.CONTROLLER_STATE_TAG_KEY: TrainControllerStateType.INITIALIZING.name
            },
        )
        == 0
    )
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.CONTROLLER_STATE,
            {
                ControllerMetrics.CONTROLLER_STATE_TAG_KEY: TrainControllerStateType.RUNNING.name
            },
        )
        == 0
    )
    assert (
        callback._metrics_tracker.get_value(
            ControllerMetrics.CONTROLLER_STATE,
            {
                ControllerMetrics.CONTROLLER_STATE_TAG_KEY: TrainControllerStateType.FINISHED.name
            },
        )
        == 1
    )

    callback.before_controller_shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
