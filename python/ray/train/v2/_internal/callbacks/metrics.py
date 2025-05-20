import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from typing import Dict, Optional

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


class ControllerMetricsCallback(ControllerCallback, WorkerGroupCallback):
    # Interval for pushing metrics to Prometheus.
    LOCAL_METRICS_PUSH_INTERVAL_S: float = 5.0
    CONTROLLER_TAG_KEYS = (RUN_NAME_TAG_KEY,)

    def __init__(self, train_run_context: TrainRunContext):
        """
        This callback is initialized on the driver process and then passed to the
        controller. This callback collects metrics from the controller actor as well
        as the metrics related to the worker groups.
        """
        self._run_name = train_run_context.get_run_config().name
        self._thread: Optional[threading.Thread] = None
        self._thread_stop_event: Optional[threading.Event] = None
        self._metrics: Optional[ControllerMetrics] = None
        self._metrics_lock: Optional[threading.Lock] = None
        self._controller_tag: Dict[str, str] = {}
        self._metrics_gauges: Dict[str, Gauge] = {}

    def _create_prometheus_controller_metrics(self) -> Dict[str, Gauge]:
        """Create Prometheus worker metrics for the ControllerMetrics dataclass."""
        metrics = {}
        for _field in fields(ControllerMetrics):
            metric_description = _field.metadata.get("description")
            metrics[_field.name] = Gauge(
                _field.name,
                description=metric_description,
                tag_keys=self.CONTROLLER_TAG_KEYS,
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
        self._metrics = ControllerMetrics()
        self._metrics_gauges = self._create_prometheus_controller_metrics()

        def push_local_metrics():
            while not self._thread_stop_event.is_set():
                with self._metrics_lock:
                    metrics_dict = asdict(self._metrics)
                for metric_name, metric_value in metrics_dict.items():
                    self._metrics_gauges[metric_name].set(
                        metric_value, self._controller_tag
                    )
                time.sleep(ControllerMetricsCallback.LOCAL_METRICS_PUSH_INTERVAL_S)

        assert not self._thread
        self._thread = threading.Thread(target=push_local_metrics, daemon=True)
        self._thread.start()

    def before_controller_shutdown(self):
        """
        Stop the thread that pushes local metrics to the gauges before the
        controller shuts down.
        """
        # Stop the thread that pushes local metrics to the metrics gauges.
        assert not self._thread_stop_event.is_set()
        self._thread_stop_event.set()
        # Reset the metrics to their default values.
        for _field in fields(self._metrics):
            self._metrics_gauges[_field.name].set(_field.default, self._controller_tag)

    @contextmanager
    def on_worker_group_start(self):
        """
        Context manager to measure the time taken to start a worker group.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        with self._metrics_lock:
            self._metrics.train_worker_group_start_total_time_s += elapsed_time_s

    @contextmanager
    def on_worker_group_shutdown(self):
        """
        Context manager to measure the time taken to start a worker group.
        """
        start_time_s = time_monotonic()
        yield
        elapsed_time_s = time_monotonic() - start_time_s
        with self._metrics_lock:
            self._metrics.train_worker_group_shutdown_total_time_s += elapsed_time_s


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
