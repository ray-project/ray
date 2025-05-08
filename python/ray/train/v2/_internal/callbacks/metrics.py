from contextlib import contextmanager
from typing import Optional

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
from ray.train.v2._internal.metrics.base import RUN_NAME_TAG_KEY, MetricsTracker
from ray.train.v2._internal.metrics.controller import ControllerMetrics
from ray.train.v2._internal.metrics.worker import WorkerMetrics
from ray.train.v2._internal.util import time_monotonic


class ControllerMetricsCallback(ControllerCallback, WorkerGroupCallback):
    def __init__(self, train_run_context: TrainRunContext):
        """
        This callback is initialized on the driver process and then passed to the
        controller. This callback collects metrics from the controller actor as well
        as the metrics related to the worker groups.
        """
        self._run_name = train_run_context.get_run_config().name
        self._metrics_tracker: Optional[MetricsTracker] = None

    def after_controller_start(self):
        """
        Initialize metrics tracker and start the metrics thread after the train
        controller starts.
        """
        controller_tag = {
            RUN_NAME_TAG_KEY: self._run_name,
        }

        metrics = ControllerMetrics.get_controller_metrics()
        self._metrics_tracker = MetricsTracker(metrics, controller_tag)
        self._metrics_tracker.start()

        # Initialize with the INITIALIZING state
        self._metrics_tracker.record(
            ControllerMetrics.CONTROLLER_STATE,
            1,
            {
                ControllerMetrics.CONTROLLER_STATE_TAG_KEY: TrainControllerStateType.INITIALIZING.name
            },
        )

    def before_controller_shutdown(self):
        """
        Stop the metrics thread before the controller shuts down.
        """
        self._metrics_tracker.shutdown()

    def after_controller_state_update(
        self,
        previous_state: TrainControllerState,
        current_state: TrainControllerState,
    ):
        """Track state transitions by incrementing the counter for the new state."""
        if previous_state._state_type != current_state._state_type:
            previous_state_name = previous_state._state_type.name
            current_state_name = current_state._state_type.name

            # Decrement the previous state counter
            self._metrics_tracker.record(
                ControllerMetrics.CONTROLLER_STATE,
                -1,
                additional_tags={
                    ControllerMetrics.CONTROLLER_STATE_TAG_KEY: previous_state_name
                },
            )

            # Increment the counter for the new state
            self._metrics_tracker.record(
                ControllerMetrics.CONTROLLER_STATE,
                1,
                additional_tags={
                    ControllerMetrics.CONTROLLER_STATE_TAG_KEY: current_state_name
                },
            )

    @contextmanager
    def on_worker_group_start(self):
        """
        Context manager to measure the time taken to start a worker group.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        self._metrics_tracker.record(
            ControllerMetrics.WORKER_GROUP_START_TOTAL_TIME_S, elapsed_time_s
        )

    @contextmanager
    def on_worker_group_shutdown(self):
        """
        Context manager to measure the time taken to start a worker group.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        self._metrics_tracker.record(
            ControllerMetrics.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S, elapsed_time_s
        )


class WorkerMetricsCallback(WorkerCallback, TrainContextCallback):
    def __init__(self, train_run_context: TrainRunContext):
        """
        This callback is initialized on the driver process and then passed to the
        workers. When adding more class attributes, make sure the attributes are
        serializable picklable.

        TODO: Making Callbacks factory methods that when they are initialized on the
        driver process, we do not need to worry about pickling the callback instances.
        """
        self._run_name = train_run_context.get_run_config().name
        self._metrics_tracker: Optional[MetricsTracker] = None

    def after_init_train_context(self):
        """
        Initialize metrics tracker and start the metrics thread after the train
        context is initialized.

        Note:
            This method should be called after the train context is initialized on
            each of the worker. The thread should not be created in the `__init__`
            method which is called on the train driver process.
        """
        worker_tag = {
            RUN_NAME_TAG_KEY: self._run_name,
            WorkerMetrics.WORKER_WORLD_RANK_TAG_KEY: str(
                get_train_context().get_world_rank()
            ),
        }

        metrics = WorkerMetrics.get_worker_metrics()
        self._metrics_tracker = MetricsTracker(metrics, worker_tag)
        self._metrics_tracker.start()

    def before_worker_shutdown(self):
        """
        Stop the metrics thread before the worker shuts down.
        """
        self._metrics_tracker.shutdown()

    @contextmanager
    def on_report(self):
        """
        Context manager to measure the time taken to report a checkpoint to the storage.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        self._metrics_tracker.record(
            WorkerMetrics.REPORT_TOTAL_BLOCKED_TIME_S, elapsed_time_s
        )
