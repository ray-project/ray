import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from typing import Dict, List, Optional, Type, TypeVar

from ray.train.v2._internal.execution.controller.state import TrainControllerState
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    TrainContextCallback,
    WorkerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext, get_train_context
from ray.train.v2._internal.util import time_monotonic
from ray.util.metrics import Gauge

# Prometheus Tag keys for the worker and controller metrics.
RUN_NAME_TAG_KEY = "ray_train_run_name"
WORKER_WORLD_RANK_TAG_KEY = "ray_train_worker_world_rank"


################################################################################
# Controller Metrics
################################################################################


# TODO: There may be a better way to do this in order to capture more information.
# E.g. a ControllerMetric class and each of the metrics is an instance of that class.
@dataclass
class ControllerMetrics:
    """A list of Train controller metrics.

    Metric metadata attributes:
    - description (required): A human-readable description of the metric, also used as
        the chart description on the Ray Train dashboard.
    """

    train_worker_group_start_total_time_s: float = field(
        default=0.0,
        metadata={
            "description": (
                "Cumulative time in seconds to start worker groups in the Train job."
            ),
        },
    )

    train_worker_group_shutdown_total_time_s: float = field(
        default=0.0,
        metadata={
            "description": (
                "Cumulative time in seconds to shutdown worker groups in the Train job."
            ),
        },
    )


CONTROLLER_TAG_KEYS = (RUN_NAME_TAG_KEY,)

T = TypeVar("T")


@dataclass
class ControllerMetric:
    name: str
    type: Type[T]
    default: T
    description: str
    tag_keys: List[str] = CONTROLLER_TAG_KEYS


TRAIN_WORKER_GROUP_START_TOTAL_TIME_S = ControllerMetric(
    name="train_worker_group_start_total_time_s",
    type=float,
    default=0.0,
    description="Cumulative time in seconds to start worker groups in the Train job.",
)

TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S = ControllerMetric(
    name="train_worker_group_shutdown_total_time_s",
    type=float,
    default=0.0,
    description="Cumulative time in seconds to shutdown worker groups in the Train job.",
)

TRAIN_CONTROLLER_STATE = ControllerMetric(
    name="train_controller_state",
    type=int,
    default=0,
    description="The current state of the controller",
    tag_keys=[RUN_NAME_TAG_KEY, "ray_train_controller_state"],
)

CONTROLLER_METRICS = [
    TRAIN_WORKER_GROUP_START_TOTAL_TIME_S,
    TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S,
    TRAIN_CONTROLLER_STATE,
]

# Replace the ControllerMetrics dataclass with a MetricsTracker class
class MetricsTracker:
    """Tracks metric values for a set of defined metrics."""

    def __init__(self, metrics_definitions):
        self._metrics_definitions = {
            metric.name: metric for metric in metrics_definitions
        }
        self._values = {metric.name: metric.default for metric in metrics_definitions}

    def update(self, metric, value):
        """Update a metric value.

        Args:
            metric: Either a ControllerMetric instance or a metric name string
            value: The new value to set or add to the metric
        """
        metric_name = metric.name if hasattr(metric, "name") else metric
        if metric_name not in self._values:
            raise ValueError(f"Unknown metric: {metric_name}")

        # For numeric types, add the value; otherwise replace it
        if isinstance(self._values[metric_name], (int, float)):
            self._values[metric_name] += value
        else:
            self._values[metric_name] = value

    def set(self, metric, value):
        """Set a metric to a specific value.

        Args:
            metric: Either a ControllerMetric instance or a metric name string
            value: The value to set for the metric
        """
        metric_name = metric.name if hasattr(metric, "name") else metric
        if metric_name not in self._values:
            raise ValueError(f"Unknown metric: {metric_name}")
        self._values[metric_name] = value

    def get(self, metric):
        """Get the current value of a metric.

        Args:
            metric: Either a ControllerMetric instance or a metric name string

        Returns:
            The current value of the metric
        """
        metric_name = metric.name if hasattr(metric, "name") else metric
        return self._values.get(metric_name)

    def get_all(self):
        """Get all metric values.

        Returns:
            Dict mapping metric names to their current values
        """
        return dict(self._values)

    def get_definition(self, metric_name):
        """Get the definition for a metric.

        Args:
            metric_name: The name of the metric

        Returns:
            The ControllerMetric definition
        """
        return self._metrics_definitions.get(metric_name)


class ControllerMetricsCallback(ControllerCallback, WorkerGroupCallback):
    # Interval for pushing metrics to Prometheus.
    LOCAL_METRICS_PUSH_INTERVAL_S: float = 5.0

    def __init__(self, train_run_context: TrainRunContext):
        """
        This callback is initialized on the driver process and then passed to the
        controller. This callback collects metrics from the controller actor as well
        as the metrics related to the worker groups.
        """
        self._run_name = train_run_context.get_run_config().name
        self._thread: Optional[threading.Thread] = None
        self._thread_stop_event: Optional[threading.Event] = None
        self._metrics: Optional[MetricsTracker] = None
        self._metrics_lock: Optional[threading.Lock] = None
        self._controller_tag: Dict[str, str] = {}
        self._metrics_gauges: Dict[str, Gauge] = {}

        # Track state transitions with a dictionary of state names to counts
        self._state_transitions: Dict[str, int] = {}

    def _create_prometheus_controller_metrics(self) -> Dict[str, Gauge]:
        """Create Prometheus worker metrics for the controller metrics."""
        metrics = {}
        for metric_name in self._metrics.get_all().keys():
            metric_def = self._metrics.get_definition(metric_name)
            metrics[metric_name] = Gauge(
                metric_name,
                description=metric_def.description,
                tag_keys=metric_def.tag_keys,
            )
        return metrics

    def after_controller_start(self):
        """
        Creating a thread to periodically push local metrics to the gauges
        after the train controller starts.
        """
        self._controller_tag = {
            RUN_NAME_TAG_KEY: self._run_name,
        }
        self._thread_stop_event = threading.Event()
        self._metrics_lock = threading.Lock()
        self._metrics = MetricsTracker(CONTROLLER_METRICS)
        self._metrics_gauges = self._create_prometheus_controller_metrics()

        # Initialize with the INITIALIZING state
        with self._metrics_lock:
            self._state_transitions = {"INITIALIZING": 1}

        def push_local_metrics():
            while not self._thread_stop_event.is_set():
                with self._metrics_lock:
                    # Push regular metrics
                    metrics_dict = self._metrics.get_all()
                    # Make a copy of state transitions
                    state_transitions = dict(self._state_transitions)

                for metric_name, metric_value in metrics_dict.items():
                    # Skip state transitions metric as it's handled separately
                    if metric_name == TRAIN_CONTROLLER_STATE.name:
                        continue
                    self._metrics_gauges[metric_name].set(
                        metric_value, self._controller_tag
                    )

                # Push state transition metrics
                state_gauge = self._metrics_gauges.get(TRAIN_CONTROLLER_STATE.name)
                if state_gauge:
                    for state_name, count in state_transitions.items():
                        state_tag = {
                            RUN_NAME_TAG_KEY: self._run_name,
                            "ray_train_controller_state": state_name,
                        }
                        state_gauge.set(count, state_tag)

                time.sleep(ControllerMetricsCallback.LOCAL_METRICS_PUSH_INTERVAL_S)

        assert not self._thread
        self._thread = threading.Thread(target=push_local_metrics, daemon=True)
        self._thread.start()

    def after_controller_state_update(
        self,
        previous_state: TrainControllerState,
        current_state: TrainControllerState,
    ):
        """Track state transitions by incrementing the counter for the new state."""
        if not self._metrics_lock:
            return

        # Only count if the state type actually changed
        if previous_state._state_type != current_state._state_type:
            previous_state_name = previous_state._state_type.name
            current_state_name = current_state._state_type.name

            with self._metrics_lock:
                # Decrement the previous state counter
                if previous_state_name in self._state_transitions:
                    self._state_transitions[previous_state_name] -= 1

                # Increment the counter for the new state
                if current_state_name not in self._state_transitions:
                    self._state_transitions[current_state_name] = 0
                self._state_transitions[current_state_name] += 1

    # TODO: This should be done after the controller shuts down.
    def before_controller_shutdown(self):
        """
        Stop the thread that pushes local metrics to the gauges before the
        controller shuts down.
        """
        # Stop the thread that pushes local metrics to the metrics gauges.
        assert not self._thread_stop_event.is_set()
        self._thread_stop_event.set()
        # Reset the metrics to their default values.
        for metric_name, gauge in self._metrics_gauges.items():
            # Skip state transitions metric as it's handled separately
            if metric_name == TRAIN_CONTROLLER_STATE.name:
                continue
            metric_def = self._metrics.get_definition(metric_name)
            gauge.set(metric_def.default, self._controller_tag)

        # Reset state transition metrics
        state_gauge = self._metrics_gauges.get(TRAIN_CONTROLLER_STATE.name)
        if state_gauge:
            for state_name in self._state_transitions:
                state_tag = {
                    RUN_NAME_TAG_KEY: self._run_name,
                    "ray_train_controller_state": state_name,
                }
                state_gauge.set(TRAIN_CONTROLLER_STATE.default, state_tag)

    @contextmanager
    def on_worker_group_start(self):
        """
        Context manager to measure the time taken to start a worker group.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        with self._metrics_lock:
            self._metrics.update(TRAIN_WORKER_GROUP_START_TOTAL_TIME_S, elapsed_time_s)

    @contextmanager
    def on_worker_group_shutdown(self):
        """
        Context manager to measure the time taken to start a worker group.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        with self._metrics_lock:
            self._metrics.update(
                TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S, elapsed_time_s
            )


################################################################################
# Worker Metrics
################################################################################


@dataclass
class WorkerMetrics:
    """A list of Train worker metrics.

    Metric metadata attributes:
    - description (required): A human-readable description of the metric, also used as
        the chart description on the Ray Train dashboard.
    """

    train_report_total_blocked_time_s: float = field(
        default=0.0,
        metadata={
            "description": (
                "Cumulative time in seconds to report a checkpoint to the storage."
            ),
        },
    )


class WorkerMetricsCallback(WorkerCallback, TrainContextCallback):
    # Interval for pushing metrics to Prometheus.
    LOCAL_METRICS_PUSH_INTERVAL_S: float = 5.0
    WORKER_TAG_KEYS = (RUN_NAME_TAG_KEY, WORKER_WORLD_RANK_TAG_KEY)

    def __init__(self, train_run_context: TrainRunContext):
        """
        This callback is initialized on the driver process and then passed to the
        workers. When adding more class attributes, make sure the attributes are
        serializable picklable.

        TODO: Making Callbacks factory methods that when they are initialized on the
        driver process, we do not need to worry about pickling the callback instances.
        """
        self._run_name = train_run_context.get_run_config().name
        self._thread: Optional[threading.Thread] = None
        self._thread_stop_event: Optional[threading.Event] = None
        self._metrics_lock: Optional[threading.Lock] = None
        self._metrics: Optional[WorkerMetrics] = None
        self._worker_tag: Dict[str, str] = {}
        self._metrics_gauges: Dict[str, Gauge] = {}

    def _create_prometheus_worker_metrics(self) -> Dict[str, Gauge]:
        """Create Prometheus worker metrics for the TrainMetrics dataclass."""
        metrics = {}
        for _field in fields(self._metrics):
            metric_description = _field.metadata.get("description")
            metrics[_field.name] = Gauge(
                _field.name,
                description=metric_description,
                tag_keys=self.WORKER_TAG_KEYS,
            )
        return metrics

    def after_init_train_context(self):
        """
        Creating a thread to periodically push local metrics to the gauges
        after the train context is initialized.

        Note:
            This method should be called after the train context is initialized on
            each of the worker. The thread should not be created in the `__init__`
            method which is called on the train driver process.
        """
        self._worker_tag = {
            RUN_NAME_TAG_KEY: self._run_name,
            WORKER_WORLD_RANK_TAG_KEY: str(get_train_context().get_world_rank()),
        }
        self._thread_stop_event = threading.Event()
        self._metrics_lock = threading.Lock()
        self._metrics = WorkerMetrics()
        self._metrics_gauges = self._create_prometheus_worker_metrics()

        def push_local_metrics():
            while not self._thread_stop_event.is_set():
                with self._metrics_lock:
                    metrics_dict = asdict(self._metrics)
                for metric_name, metric_value in metrics_dict.items():
                    self._metrics_gauges[metric_name].set(
                        metric_value, self._worker_tag
                    )
                time.sleep(WorkerMetricsCallback.LOCAL_METRICS_PUSH_INTERVAL_S)

        assert not self._thread
        self._thread = threading.Thread(target=push_local_metrics, daemon=True)
        self._thread.start()

    def before_worker_shutdown(self):
        """
        Stop the thread that pushes local metrics to the metrics gauges before
        the worker group shuts down.
        """
        # Stop the thread that pushes local metrics to the gauges.
        assert not self._thread_stop_event.is_set()
        self._thread_stop_event.set()
        # Reset the metrics to their default values.
        for _field in fields(self._metrics):
            self._metrics_gauges[_field.name].set(_field.default, self._worker_tag)

    @contextmanager
    def on_report(self):
        """
        Context manager to measure the time taken to report a checkpoint to the storage.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        with self._metrics_lock:
            self._metrics.train_report_total_blocked_time_s += elapsed_time_s
