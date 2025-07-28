import os
import threading
from collections import defaultdict
from dataclasses import dataclass
from requests.exceptions import ConnectionError
import logging


log = logging.getLogger(__name__)


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
    LOCATION = _RayMetricLabel("Location")
    OBJECT_STATE = _RayMetricLabel("ObjectState")
    COMPONENT = _RayMetricLabel("Component")
    TYPE = _RayMetricLabel("Type")


@dataclass
class _RayMetricFamily:
    name: str
    labels: list[_RayMetricLabel]

    # If True, this metric is global (cluster-wide) and the metric value must be scraped from
    # ray head node metric exporter endpoint. e.g.,
    # ray_tasks / ray_actors / ray_cluster_active_nodes are global metrics.
    # otherwise, this metric is node-local, and the metric value must be scraped from
    # the related Ray node's metric exporter endpoint.
    is_global_metric: bool


_ray_metric_families = {
    family.name: family
    for family in [
        _RayMetricFamily("ray_tasks", [_RayMetricLabels.NAME, _RayMetricLabels.STATE, _RayMetricLabels.IS_RETRY], True),
        _RayMetricFamily("ray_actors", [_RayMetricLabels.NAME, _RayMetricLabels.STATE], True),
        _RayMetricFamily(
            "ray_resources", [_RayMetricLabels.NAME, _RayMetricLabels.STATE], False
        ),
        _RayMetricFamily("ray_object_store_memory", [
            _RayMetricLabels.LOCATION, _RayMetricLabels.OBJECT_STATE,
        ],  False),
        _RayMetricFamily("ray_placement_groups", [_RayMetricLabels.STATE], True),
        _RayMetricFamily("ray_component_cpu_percentage",  [_RayMetricLabels.COMPONENT], False),
        _RayMetricFamily("ray_component_uss_mb", [_RayMetricLabels.COMPONENT], False),
        _RayMetricFamily("ray_cluster_active_nodes", [], True),
        _RayMetricFamily("ray_cluster_failed_nodes", [], True),
        _RayMetricFamily("ray_cluster_pending_nodes", [], True),
        _RayMetricFamily("ray_node_disk_io_write_speed", [], False),
        _RayMetricFamily("ray_node_disk_io_read_speed", [], False),
        _RayMetricFamily("ray_node_network_receive_speed", [], False),
        _RayMetricFamily("ray_node_network_send_speed", [], False),
    ]
}


def collect_ray_metrics(is_head_node, metrics_export_port, node_ip, ray_node_id):
    """
    Query ray metrics at current timestamp,
    return a dict of metric_key to metric_value
    For global Ray metrics (e.g. ray_tasks, ray_cluster_active_nodes), the metric key is like:
     `system/{metric_name}/{metric_label1}/{metric_label2}/...`
    For Ray metrics for certain Ray node
      (e.g., ray_component_cpu_percentage, ray_component_uss_mb),
      the metric key is like:
     `system/{node_ip}/{ray_node_id}/{metric_name}/{metric_label1}/{metric_label2}/...`
    depending on whether the metric values are grouped by nodes.
    """
    from prometheus_client.parser import text_string_to_metric_families
    import requests

    # Each Ray node starts a local Ray metric exporting endpoint
    # on port {_RAY_METRICS_EXPORT_PORT}
    response = requests.get(f"{node_ip}:{metrics_export_port}")
    metric_data = response.text

    def gen_metric_key(_family, label_data):
        key_parts = ["system"]

        if not _family.is_global_metric:
            # Set node rank as the metric key prefix.
            key_parts.extend([node_ip, f"ray_node_{ray_node_id}"])

        key_parts.append(_family.name)

        for label in _family.labels:
            label_value = label_data.get(label.name)
            if label_value:
                key_parts.append(label.to_str(label_value))

        return "/".join(key_parts)

    metric_dict = defaultdict(lambda: 0.0)
    for family_data in text_string_to_metric_families(metric_data):
        family_name = family_data.name
        family = _ray_metric_families.get(family_name)
        if family and (
            (is_head_node and family.is_global_metric)  # global metric
            or not family.is_global_metric  # local-node metric
        ):
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
        run_id: string, the MLflow run ID.
        is_head_node: bool, indicates whether the Ray metrics monitor is running on the head node
        metrics_export_port: int, the Ray node metrics export port.
        sampling_interval: float, default to 10. The interval (in seconds) at which to pull system
            metrics.
    """
    def __init__(
        self,
        run_id: str,
        is_head_node: str,
        node_ip: str,
        ray_node_id: int,
        metrics_export_port: int,
        sampling_interval: float = 10,
    ):
        from mlflow.utils.autologging_utils import BatchMetricsLogger
        from mlflow.system_metrics.system_metrics_monitor import SystemMetricsMonitor

        self._run_id = run_id
        self._is_head_node = is_head_node
        self._node_ip = node_ip
        self._ray_node_id = ray_node_id
        self._sampling_interval = sampling_interval
        self._metrics_export_port = metrics_export_port
        self._logging_step = 0
        self.mlflow_logger = BatchMetricsLogger(self._run_id)
        self._shutdown_event = threading.Event()
        self._process = None
        os.environ["MLFLOW_SYSTEM_METRICS_NODE_ID"] = node_ip
        self._mlflow_sys_metrics_monitor = SystemMetricsMonitor(run_id=self._run_id)

    def start(self):
        """Start monitoring system metrics."""
        try:
            self._process = threading.Thread(
                target=self.monitor,
                daemon=True,
                name="RayMetricsMonitor",
            )
            self._process.start()
        except Exception as e:
            log.warning(f"Start Ray monitoring process failed, error: {e}")
            self._process = None

    def collect_metrics(self):
        try:
            metrics = collect_ray_metrics(
                self._is_head_node, self._metrics_export_port,
                self._node_ip, self._ray_node_id,
            )
            return metrics
        except ConnectionError as e:
            # Ray metrics exporter endpoint is not ready yet.
            return {}
        except Exception as e:
            log.info(f"collect Ray metrics failed: {e}")
            return {}

    def monitor(self):
        """Main monitoring loop, which consistently collect and log system metrics."""
        from mlflow.tracking.fluent import get_run

        log.info("Started Ray metrics monitor.")
        while not self._shutdown_event.is_set():
            metrics = self.collect_metrics()
            self._shutdown_event.wait(self._sampling_interval)
            try:
                # Get the MLflow run to check if the run is not RUNNING.
                run = get_run(self._run_id)
            except Exception as e:
                log.warning(f"Failed to get mlflow run: {e}.")
                return
            if run.info.status != "RUNNING" or self._shutdown_event.is_set():
                # If the mlflow run is terminated or receives the shutdown signal, stop
                # monitoring.
                return
            try:
                self.publish_metrics(metrics)
            except Exception as e:
                log.warning(
                    f"Failed to log system metrics: {e}, this is expected if the experiment/run is "
                    "already terminated."
                )
                return

    def publish_metrics(self, metrics):
        """Log collected metrics to MLflow."""
        self.mlflow_logger.record_metrics(metrics, self._logging_step)
        self._logging_step += 1

    def finish(self):
        """Stop monitoring system metrics."""
        del os.environ["MLFLOW_SYSTEM_METRICS_NODE_ID"]
        if self._process is None:
            return
        log.info("Stopping Ray metrics monitoring...")
        self._shutdown_event.set()
        try:
            self._process.join()
            self.mlflow_logger.flush()
            log.info("Successfully terminated Ray metrics monitoring!")
        except Exception as e:
            log.warning(f"Error terminating system metrics monitoring process: {e}.")
        self._process = None
