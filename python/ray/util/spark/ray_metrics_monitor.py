import logging
import os
import threading
from collections import defaultdict
from dataclasses import dataclass
import time
from requests.exceptions import ConnectionError

_logger = logging.getLogger(__name__)


class _RayMetricLabel:

    def __init__(self, name, to_str=None):
        self._name = name
        self._to_str = to_str

    @property
    def name(self):
        return self._name

    def to_str(self, value):
        if self._to_str is None:
            return str(value)
        return self._to_str(value)


class _RayMetricLabels:
    NAME = _RayMetricLabel("Name")
    STATE = _RayMetricLabel("State")
    IS_RETRY = _RayMetricLabel("IsRetry", to_str=lambda value: "isRetry" if value == "1" else "isNotRetry")
    NODE_ADDR = _RayMetricLabel("NodeAddress")
    LOCATION = _RayMetricLabel("Location")
    OBJECT_STATE = _RayMetricLabel("ObjectState")
    COMPONENT = _RayMetricLabel("Component")
    NODE_IP = _RayMetricLabel("ip")
    TYPE = _RayMetricLabel("Type")


@dataclass
class _RayMetricFamily:
    name: str
    labels: list[_RayMetricLabel]
    nodeLabel: _RayMetricLabel


_ray_metric_families = {
    family.name: family
    for family in [
        _RayMetricFamily("ray_tasks", [_RayMetricLabels.NAME, _RayMetricLabels.STATE, _RayMetricLabels.IS_RETRY], None),
        _RayMetricFamily("ray_actors", [_RayMetricLabels.NAME, _RayMetricLabels.STATE], None),
        _RayMetricFamily(
            "ray_resources", [_RayMetricLabels.NAME, _RayMetricLabels.STATE], _RayMetricLabels.NODE_ADDR
        ),
        _RayMetricFamily("ray_object_store_memory", [
            _RayMetricLabels.LOCATION, _RayMetricLabels.OBJECT_STATE,
        ],  _RayMetricLabels.NODE_ADDR),
        _RayMetricFamily("ray_placement_groups", [_RayMetricLabels.STATE], None),
        _RayMetricFamily("ray_memory_manager_worker_eviction_total", [_RayMetricLabels.TYPE], None),
        _RayMetricFamily("ray_component_uss_mb", [_RayMetricLabels.COMPONENT], None),
        _RayMetricFamily("ray_component_cpu_percentage", [
            _RayMetricLabels.COMPONENT,
        ], _RayMetricLabels.NODE_IP),
        _RayMetricFamily("ray_cluster_active_nodes", [], None),
        _RayMetricFamily("ray_cluster_failed_nodes", [], None),
        _RayMetricFamily("ray_cluster_pending_nodes", [], None),
        _RayMetricFamily("ray_node_disk_io_write_speed", [], _RayMetricLabels.NODE_IP),
        _RayMetricFamily("ray_node_disk_io_read_speed", [], _RayMetricLabels.NODE_IP),
        _RayMetricFamily("ray_node_network_receive_speed", [], _RayMetricLabels.NODE_IP),
        _RayMetricFamily("ray_node_network_send_speed", [], _RayMetricLabels.NODE_IP),
    ]
}


def collect_ray_metrics(node_ip, metrics_export_port):
    """
    Query ray metrics at current timestamp,
    return a dict of metric_key to metric_value
    the metric key can be either `system/ray/{metric_name}` or `system/{node_ip}/{metric_name}`
    depending on whether the metric values are grouped by nodes.
    """
    from prometheus_client.parser import text_string_to_metric_families
    import requests

    response = requests.get(f"http://{node_ip}:{metrics_export_port}")
    metric_data = response.text

    def gen_metric_key(_family, label_data):
        key = "system/"
        if _family.nodeLabel:
            key += f"{label_data[_family.nodeLabel.name]}/"
        key += f"ray/{_family.name}"
        label_strs = [
            label.to_str(label_data[label.name])
            for label in _family.labels
        ]
        if label_strs:
            key += f"_{'_'.join(label_strs)}"
        return key

    metric_dict = defaultdict(lambda: 0.0)
    for family_data in text_string_to_metric_families(metric_data):
        family_name = family_data.name
        family = _ray_metric_families.get(family_name)
        if family:
            for sample in family_data.samples:
                key = gen_metric_key(family, sample.labels)
                value = sample.value
                metric_dict[key] += value

    return metric_dict


class RayMetricsMonitor:
    """
    Class for monitoring Ray system metrics that are documented in
    https://docs.ray.io/en/latest/ray-observability/reference/system-metrics.html .

    Args:
        sampling_interval: float, default to 10. The interval (in seconds) at which to pull system
            metrics.
    """
    def __init__(self, ray_head_ip: str, metrics_export_port: int, sampling_interval: float = 10):
        import mlflow
        from mlflow.tracking.client import MlflowClient
        from mlflow.utils.autologging_utils import BatchMetricsLogger
        from mlflow.system_metrics.system_metrics_monitor import SystemMetricsMonitor

        self._ray_head_ip = ray_head_ip
        self._metrics_export_port = metrics_export_port
        self._mlflow_experiment_id = mlflow.tracking.fluent._get_experiment_id()

        if mlflow.active_run() is not None:
            raise RuntimeError(
                "Before starting RayMetricsMonitor, ensure the current active MLflow run is "
                "terminated."
            )
        self._run_id = mlflow.start_run().info.run_id

        # export the MLflow run ID so that user code can log other data into
        # the same run.
        os.environ["MLFLOW_RUN_ID"] = self._run_id

        self._sampling_interval = sampling_interval
        self._logging_step = 0
        self.mlflow_logger = BatchMetricsLogger(self._run_id)
        self._shutdown_event = threading.Event()
        self._process = None

        os.environ["MLFLOW_SYSTEM_METRICS_NODE_ID"] = ray_head_ip
        self._mlflow_sys_metrics_monitor = SystemMetricsMonitor(run_id=self._run_id)

    @property
    def mlflow_experiment_id(self):
        return self._mlflow_experiment_id

    @property
    def run_id(self):
        return self._run_id

    def start(self):
        """Start monitoring system metrics."""
        try:
            self._process = threading.Thread(
                target=self.monitor,
                daemon=True,
                name="SystemMetricsMonitor",
            )
            self._process.start()
        except Exception as e:
            _logger.error(f"Failed to start RayMetricsMonitor thread: {e}")
            self._process = None

        # Beside `self.monitor` that collects Ray cluster specific metrics,
        # Using MLFlow builtin `SystemMetricsMonitor` can collect general
        # system metrics for CPU/GPU/disk/network ect.
        # Note the MLFlow builtin `SystemMetricsMonitor` only monitors
        # local node.
        self._mlflow_sys_metrics_monitor.start()

    def collect_metrics(self):
        try:
            return collect_ray_metrics(self._ray_head_ip, self._metrics_export_port)
        except ConnectionError:
            # Ray metrics exporter endpoint is not ready yet.
            pass
        except Exception as e:
            _logger.warning(f"Failed to collect Ray metrics: {e}")
            return {}

    def monitor(self):
        """Main monitoring loop, which consistently collect and log system metrics."""
        from mlflow.tracking.fluent import get_run

        while not self._shutdown_event.is_set():
            metrics = self.collect_metrics()
            try:
                # Get the MLflow run to check if the run is not RUNNING.
                run = get_run(self._run_id)
            except Exception as e:
                _logger.warning(f"Failed to get mlflow run: {e}.")
                return
            if run.info.status != "RUNNING" or self._shutdown_event.is_set():
                # If the mlflow run is terminated or receives the shutdown signal, stop
                # monitoring.
                return
            try:
                self.publish_metrics(metrics)
            except Exception as e:
                _logger.warning(
                    f"Failed to log system metrics: {e}, this is expected if the experiment/run is "
                    "already terminated."
                )
                return
            self._shutdown_event.wait(self._sampling_interval)

    def publish_metrics(self, metrics):
        """Log collected metrics to MLflow."""
        self.mlflow_logger.record_metrics(metrics, self._logging_step)
        self._logging_step += 1

    def finish(self):
        """Stop monitoring system metrics."""
        import mlflow

        if self._process is None:
            return
        _logger.info("Stopping system metrics monitoring...")
        self._shutdown_event.set()
        self._mlflow_sys_metrics_monitor.finish()
        del os.environ["MLFLOW_RUN_ID"]
        del os.environ["MLFLOW_SYSTEM_METRICS_NODE_ID"]
        try:
            self._process.join()
            self.mlflow_logger.flush()
            _logger.info("Successfully terminated system metrics monitoring!")
        except Exception as e:
            _logger.error(f"Error terminating system metrics monitoring process: {e}.")
        self._process = None

        if active_run := mlflow.active_run():
            if active_run.info.run_id == self._run_id:
                mlflow.end_run()
