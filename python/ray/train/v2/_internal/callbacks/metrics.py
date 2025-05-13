from contextlib import contextmanager
from typing import Dict, Optional

from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    TrainContextCallback,
    WorkerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext, get_train_context
from ray.train.v2._internal.execution.controller.state import (
    TrainControllerState,
    TrainControllerStateType,
)
from ray.train.v2._internal.metrics.base import Metric
from ray.train.v2._internal.metrics.controller import ControllerMetrics
from ray.train.v2._internal.metrics.worker import WorkerMetrics
from ray.train.v2._internal.util import time_monotonic


class ControllerMetricsCallback(ControllerCallback, WorkerGroupCallback):
    """Callback that records controller-specific metrics."""

    def __init__(self, train_run_context: TrainRunContext):
        self._run_name = train_run_context.get_run_config().name
        self._metrics: Optional[Dict[str, Metric]] = None

    def after_controller_start(self):
        """Initialize metrics after controller starts."""
        self._metrics = ControllerMetrics.get_controller_metrics(self._run_name)
        # Record initial state
        self._metrics[ControllerMetrics.CONTROLLER_STATE].record(
            TrainControllerStateType.INITIALIZING
        )

    def before_controller_shutdown(self):
        """Shutdown metrics before controller shuts down."""
        for metric in self._metrics.values():
            metric.reset()

    def after_controller_state_update(
        self,
        previous_state: TrainControllerState,
        current_state: TrainControllerState,
    ):
        """Record state transitions after controller state updates."""
        self._metrics[ControllerMetrics.CONTROLLER_STATE].record(
            current_state._state_type
        )

    @contextmanager
    def on_worker_group_start(self):
        """Measure time taken to start worker group."""
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        self._metrics[ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S].record(
            elapsed_time_s
        )

    @contextmanager
    def on_worker_group_shutdown(self):
        """Measure time taken to shutdown worker group."""
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        self._metrics[ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S].record(
            elapsed_time_s
        )


class WorkerMetricsCallback(WorkerCallback, TrainContextCallback):
    """Callback that records worker-specific metrics."""

    def __init__(self, train_run_context: TrainRunContext):
        self._run_name = train_run_context.get_run_config().name
        self._metrics: Optional[Dict[str, Metric]] = None

    def after_init_train_context(self):
        """Initialize metrics after train context is initialized."""
        world_rank = get_train_context().get_world_rank()
        self._metrics = WorkerMetrics.get_worker_metrics(self._run_name, world_rank)

    def before_shutdown(self):
        """Shutdown metrics before shutdown."""
        for metric in self._metrics.values():
            metric.reset()

    @contextmanager
    def on_report(self):
        """
        Context manager to measure the time taken to report a checkpoint to the storage.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        self._metrics[WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S].record(elapsed_time_s)
