import threading
import time
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
from ray.train.v2._internal.metrics.controller import (
    TRAIN_CONTROLLER_STATE,
    TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S,
    TRAIN_WORKER_GROUP_START_TOTAL_TIME_S,
)
from ray.train.v2._internal.metrics.worker import TRAIN_REPORT_TOTAL_BLOCKED_TIME_S
from ray.train.v2.api.config import RunConfig


class MockGauge:
    """
    An Naive Mock class for `ray.util.metrics.Gauge`.

    This mock class initializes the value of the gauge to 0.0 and sets the value.
    The tags keys are not used in this mock class, just for API consistency.
    """

    def __init__(self, name: str, description: str, tag_keys: tuple = ()):
        self._values: dict[set[str], float] = {}

    def set(self, value: float, tags: dict):
        self._values[frozenset(tags.items())] = value

    def get(self, tags: dict):
        return self._values.get(frozenset(tags.items()))


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


def test_metrics_tracker(monkeypatch):
    """Test the core functionality of MetricsTracker."""
    monkeypatch.setattr(MetricsTracker, "DEFAULT_METRICS_PUSH_INTERVAL_S", 0.05)
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
        tag_keys=["base_tag", "tag1", "tag2"],
    )

    # Initialize tracker with base tags
    tracker = MetricsTracker([test_metric, test_metric_with_tags], base_tags)
    tracker.start()

    # Test initial state
    assert tracker.get_value(test_metric, {}) is None
    assert tracker.get_value(test_metric_with_tags, {"tag1": "value1"}) is None

    # Test updating metric with additional tags
    tracker.update(test_metric_with_tags, {"tag1": "value1"}, 1.0)
    assert tracker.get_value(test_metric_with_tags, {"tag1": "value1"}) == 1.0

    # Test updating same metric-tag combination
    tracker.update(test_metric_with_tags, {"tag1": "value1"}, 2.0)
    assert tracker.get_value(test_metric_with_tags, {"tag1": "value1"}) == 3.0

    # Test updating with different tags
    tracker.update(test_metric_with_tags, {"tag2": "value2"}, 4.0)
    assert tracker.get_value(test_metric_with_tags, {"tag2": "value2"}) == 4.0
    assert tracker.get_value(test_metric_with_tags, {"tag1": "value1"}) == 3.0

    # Test shutdown
    tracker.shutdown()
    assert tracker._thread is None
    assert tracker._thread_stop_event is None


def test_metrics_tracker_unknown_metric():
    """Test that updating an unknown metric raises an error."""
    tracker = MetricsTracker([], {})
    unknown_metric = Metric(
        name="unknown_metric",
        type=float,
        default=0.0,
        description="Unknown metric",
        tag_keys=[],
    )

    with pytest.raises(ValueError, match="Unknown metric: unknown_metric"):
        tracker.update(unknown_metric, {}, 1.0)


def test_metrics_tracker_reset(monkeypatch):
    """Test that all metric-tag combinations are properly reset."""
    monkeypatch.setattr(MetricsTracker, "DEFAULT_METRICS_PUSH_INTERVAL_S", 0.05)
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
    tracker = MetricsTracker([test_metric1, test_metric2], base_tags)
    tracker.start()

    # Update metrics with different tag combinations
    tag1_value1 = {"tag1": "value1"}
    tag1_value2 = {"tag1": "value2"}

    tracker.update(test_metric1, {}, 1.0)
    tracker.update(test_metric2, tag1_value1, 2)
    tracker.update(test_metric2, tag1_value2, 3)
    time.sleep(0.1)

    # Verify all values are set
    assert tracker._metrics_gauges[test_metric1.name].get(base_tags) == 1.0
    assert (
        tracker._metrics_gauges[test_metric2.name].get({**base_tags, **tag1_value1})
        == 2
    )
    assert (
        tracker._metrics_gauges[test_metric2.name].get({**base_tags, **tag1_value2})
        == 3
    )

    # Reset gauges
    tracker._stop_metrics_thread()
    tracker._reset_gauges()

    # Verify all values are reset to default
    assert tracker._metrics_gauges[test_metric1.name].get(base_tags) == 0.0
    assert (
        tracker._metrics_gauges[test_metric2.name].get({**base_tags, **tag1_value1})
        == 0
    )
    assert (
        tracker._metrics_gauges[test_metric2.name].get({**base_tags, **tag1_value2})
        == 0
    )

    tracker.shutdown()


def test_metrics_tracker_thread_safety():
    """Test that MetricsTracker is thread-safe."""
    test_metric = Metric(
        name="test_metric",
        type=int,
        default=0,
        description="Test metric",
        tag_keys=[],
    )

    tracker = MetricsTracker([test_metric], {})
    tracker.start()

    def update_metric():
        for _ in range(100):
            tracker.update(test_metric, {}, 1)

    # Create multiple threads updating the same metric
    threads = [threading.Thread(target=update_metric) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    time.sleep(0.1)
    # Should be exactly 1000 (100 updates * 10 threads)
    assert tracker.get_value(test_metric, {}) == 1000

    tracker.shutdown()


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


def test_controller_state_metrics(monkeypatch):
    """Test controller state transition metrics."""
    monkeypatch.setattr(MetricsTracker, "DEFAULT_METRICS_PUSH_INTERVAL_S", 0.05)
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
    time.sleep(0.1)
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_CONTROLLER_STATE,
            {"ray_train_controller_state": "INITIALIZING"},
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
    time.sleep(0.1)

    # Verify state counts
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_CONTROLLER_STATE,
            {"ray_train_controller_state": "INITIALIZING"},
        )
        == 0
    )
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_CONTROLLER_STATE,
            {"ray_train_controller_state": "RUNNING"},
        )
        == 1
    )

    # Test another state transition
    previous_state = MockState(TrainControllerStateType.RUNNING)
    current_state = MockState(TrainControllerStateType.FINISHED)
    callback.after_controller_state_update(previous_state, current_state)
    time.sleep(0.1)

    # Verify updated state counts
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_CONTROLLER_STATE,
            {"ray_train_controller_state": "INITIALIZING"},
        )
        == 0
    )
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_CONTROLLER_STATE,
            {"ray_train_controller_state": "RUNNING"},
        )
        == 0
    )
    assert (
        callback._metrics_tracker.get_value(
            TRAIN_CONTROLLER_STATE,
            {"ray_train_controller_state": "FINISHED"},
        )
        == 1
    )

    callback.before_controller_shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
