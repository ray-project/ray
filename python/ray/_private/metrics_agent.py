import json
import logging
import os
import threading
import time
import traceback
from collections import namedtuple, defaultdict
from typing import List, Tuple

from opencensus.metrics.export.value import ValueDouble
from opencensus.stats import aggregation
from opencensus.stats import measure as measure_module
from opencensus.stats.view_manager import ViewManager
from opencensus.stats.stats_recorder import StatsRecorder
from opencensus.stats.base_exporter import StatsExporter
from opencensus.stats.aggregation_data import (
    CountAggregationData,
    DistributionAggregationData,
    LastValueAggregationData,
)
from opencensus.stats.view import View
from opencensus.stats.view_data import ViewData
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module

import ray
from ray._private.gcs_utils import GcsClient
from ray.core.generated.metrics_pb2 import Metric

logger = logging.getLogger(__name__)

# Env var key to decide worker timeout.
# If the worker doesn't report for more than
# this time, we treat workers as dead.
RAY_WORKER_TIMEOUT_S = "RAY_WORKER_TIMEOUT_S"


class Gauge(View):
    """Gauge representation of opencensus view.

    This class is used to collect process metrics from the reporter agent.
    Cpp metrics should be collected in a different way.
    """

    def __init__(self, name, description, unit, tags: List[str]):
        self._measure = measure_module.MeasureInt(name, description, unit)
        tags = [tag_key_module.TagKey(tag) for tag in tags]
        self._view = View(
            name, description, tags, self.measure, aggregation.LastValueAggregation()
        )

    @property
    def measure(self):
        return self._measure

    @property
    def view(self):
        return self._view

    @property
    def name(self):
        return self.measure.name


Record = namedtuple("Record", ["gauge", "value", "tags"])


class WorkerProxyExportState:
    def __init__(self):
        self._last_reported_time = time.monotonic()
        # view_name
        # -> set of tag_vals tuple that this worker genereated.
        # NOTE: We assume a tuple of tag_vals correspond to a
        # single worker because it always contain WorkerId.
        self._view_to_owned_tag_vals = defaultdict(set)

    @property
    def last_reported_time(self):
        return self._last_reported_time

    @property
    def view_to_owned_tag_vals(self):
        return self._view_to_owned_tag_vals

    def update_last_reported_time(self):
        self._last_reported_time = time.monotonic()

    def put_tag_vals(self, view_name: str, tag_vals: Tuple[str]):
        self._view_to_owned_tag_vals[view_name].add(tag_vals)


class MetricsAgent:
    def __init__(
        self,
        view_manager: ViewManager,
        stats_recorder: StatsRecorder,
        stats_exporter: StatsExporter = None,
    ):
        """A class to record and export metrics.

        The class exports metrics in 2 different ways.
        - Directly record and export metrics using OpenCensus.
        - Proxy metrics from other core components
            (e.g., raylet, GCS, core workers).

        This class is thread-safe.
        """
        # Lock required because gRPC server uses
        # multiple threads to process requests.
        self._lock = threading.Lock()

        #
        # Opencensus components to record metrics.
        #

        # Managing views to export metrics
        # If the stats_exporter is None, we disable all metrics export.
        self.view_manager = view_manager
        # A class that's used to record metrics
        # emitted from the current process.
        self.stats_recorder = stats_recorder
        # A class to export metrics.
        self.stats_exporter = stats_exporter

        if self.stats_exporter is None:
            # If the exporter is not given,
            # we disable metrics collection.
            self.view_manager = None
        else:
            self.view_manager.register_exporter(stats_exporter)

        # Below fields are used to clean up view data when workers are dead.
        # Note that Opencensus export metrics from
        # `view_data.tag_value_aggregation_data_map[tag_vals]`, which means
        # we can stop exporting metrics by deleting data in there.
        # Note that tag_vals is something like (<WorkerId>, <IP>, <tags..>).
        # Each worker metrics will have its own unique tag_vals because they
        # always contain WorkerId.
        # - When we proxy exports metrics, if they are from a worker, we store
        #   the corresponding {worker_id -> tag vals} and last reported time to
        #   `WorkerProxyExportState`.
        # - We periodically go over all `WorkerProxyExportState`. If the last
        #   reported time is bigger than the threashold, we treat
        #   the worker as dead.
        # - If the worker somehow reports metrics again, we starts reporting
        #   again. If workers are dead, they will stop reporting, so the dead
        #   worker metrics will eventually be cleaned up

        # {worker_id -> {view_name -> {tag_vals}}}
        # Used to clean up data created from a worker of worker id.
        self.worker_id_to_state = defaultdict(WorkerProxyExportState)
        # After the timeout, the worker is marked as dead.
        # The timeout is reset every time worker reports
        # new metrics (it happens every 2 seconds by default).
        # This value must be longer than the Prometheus
        # scraping interval (10s by default in Ray).
        self.worker_timeout_s = int(os.getenv(RAY_WORKER_TIMEOUT_S, 120))

    def _get_mutable_view_data(self, view_name: str) -> ViewData:
        """Return the current view data for a given view name."""
        assert self._lock.locked()
        return self.view_manager.measure_to_view_map._measure_to_view_data_list_map[
            view_name
        ][-1]

    def record_and_export(self, records: List[Record], global_tags=None):
        """Directly record and export stats from the same process."""
        global_tags = global_tags or {}
        with self._lock:
            if not self.view_manager:
                return

            for record in records:
                gauge = record.gauge
                value = record.value
                tags = record.tags
                self._record_gauge(gauge, value, {**tags, **global_tags})

    def _record_gauge(self, gauge: Gauge, value: float, tags: dict):
        view_data = self.view_manager.get_view(gauge.name)
        if not view_data:
            self.view_manager.register_view(gauge.view)
            # Reobtain the view.
        view = self.view_manager.get_view(gauge.name).view
        measurement_map = self.stats_recorder.new_measurement_map()
        tag_map = tag_map_module.TagMap()
        for key, tag_val in tags.items():
            tag_key = tag_key_module.TagKey(key)
            tag_value = tag_value_module.TagValue(tag_val)
            tag_map.insert(tag_key, tag_value)
        measurement_map.measure_float_put(view.measure, value)
        # NOTE: When we record this metric, timestamp will be renewed.
        measurement_map.record(tag_map)

    def proxy_export_metrics(self, metrics: List[Metric], worker_id_hex: str = None):
        """Proxy export metrics specified by a Opencensus Protobuf.

        This API is used to export metrics emitted from
        core components.

        Args:
            metrics: A list of protobuf Metric defined from OpenCensus.
            worker_id_hex: The worker ID it proxies metrics export. None
                if the metric is not from a worker (i.e., raylet, GCS).
        """
        with self._lock:
            if not self.view_manager:
                return

            self._proxy_export_metrics(metrics, worker_id_hex)

    def _proxy_export_metrics(self, metrics: List[Metric], worker_id_hex: str = None):
        assert self._lock.locked()
        # The list of view data is what we are going to use for the
        # final export to exporter.
        view_data_changed: List[ViewData] = []

        # Walk the protobufs and convert them to ViewData
        for metric in metrics:
            descriptor = metric.metric_descriptor
            timeseries = metric.timeseries

            if len(timeseries) == 0:
                continue

            columns = [label_key.key for label_key in descriptor.label_keys]
            start_time = timeseries[0].start_timestamp.seconds

            # Create the view and view_data
            measure = measure_module.BaseMeasure(
                descriptor.name, descriptor.description, descriptor.unit
            )
            view = self.view_manager.measure_to_view_map.get_view(descriptor.name, None)
            if not view:
                view = View(
                    descriptor.name,
                    descriptor.description,
                    columns,
                    measure,
                    aggregation=None,
                )
                self.view_manager.measure_to_view_map.register_view(view, start_time)
            view_data = self._get_mutable_view_data(measure.name)
            view_data_changed.append(view_data)

            # Create the aggregation and fill it in the our stats
            for series in timeseries:
                tag_vals = tuple(val.value for val in series.label_values)

                # If the metric is reported from a worker,
                # we update the states accordingly.
                if worker_id_hex:
                    state = self.worker_id_to_state[worker_id_hex]
                    state.update_last_reported_time()
                    state.put_tag_vals(descriptor.name, tag_vals)

                # Aggregate points.
                for point in series.points:
                    if point.HasField("int64_value"):
                        data = CountAggregationData(point.int64_value)
                    elif point.HasField("double_value"):
                        data = LastValueAggregationData(ValueDouble, point.double_value)
                    elif point.HasField("distribution_value"):
                        dist_value = point.distribution_value
                        counts_per_bucket = [
                            bucket.count for bucket in dist_value.buckets
                        ]
                        bucket_bounds = dist_value.bucket_options.explicit.bounds
                        data = DistributionAggregationData(
                            dist_value.sum / dist_value.count,
                            dist_value.count,
                            dist_value.sum_of_squared_deviation,
                            counts_per_bucket,
                            bucket_bounds,
                        )
                    else:
                        raise ValueError("Summary is not supported")
                    view_data.tag_value_aggregation_data_map[tag_vals] = data

        # Finally, export all the values.
        # When the view data is exported, they are hard-copied.
        self.view_manager.measure_to_view_map.export(view_data_changed)

    def clean_all_dead_worker_metrics(self):
        """Clean dead worker's metrics.

        Worker metrics are cleaned up if there was no metrics
        report for more than `worker_timeout_s`.

        This method has to be periodically called by a caller.
        """
        with self._lock:
            if not self.view_manager:
                return

            worker_ids_to_clean = []
            for worker_id_hex, state in self.worker_id_to_state.items():
                elapsed = time.monotonic() - state.last_reported_time
                if elapsed > self.worker_timeout_s:
                    logger.info(
                        "Metrics from a worker ({}) is cleaned up due to "
                        "timeout. Time since last report {}s".format(
                            worker_id_hex, elapsed
                        )
                    )
                    worker_ids_to_clean.append(worker_id_hex)

            for worker_id in worker_ids_to_clean:
                self._clean_worker_metrics(worker_id)

    def _clean_worker_metrics(self, worker_id_hex: str):
        assert self._lock.locked()
        assert worker_id_hex in self.worker_id_to_state

        state = self.worker_id_to_state[worker_id_hex]
        state.view_to_owned_tag_vals
        view_names_changed = set()

        for view_name, tag_vals_to_clean in state.view_to_owned_tag_vals.items():
            view_data = self._get_mutable_view_data(view_name)
            for tag_vals in tag_vals_to_clean:
                if tag_vals in view_data.tag_value_aggregation_data_map:
                    # If we remove tag_vals from here,
                    # exporter will stop exporting metrics.
                    del view_data.tag_value_aggregation_data_map[tag_vals]
            view_names_changed.add(view_name)

        # We need to re-export the view data so that prometheus
        # exporter updates its view data.
        view_data_changed = [
            self._get_mutable_view_data(view_name) for view_name in view_names_changed
        ]
        self.view_manager.measure_to_view_map.export(view_data_changed)

        # Clean up worker states.
        del self.worker_id_to_state[worker_id_hex]


class PrometheusServiceDiscoveryWriter(threading.Thread):
    """A class to support Prometheus service discovery.

    It supports file-based service discovery. Checkout
    https://prometheus.io/docs/guides/file-sd/ for more details.

    Args:
        gcs_address: Gcs address for this cluster.
        temp_dir: Temporary directory used by
            Ray to store logs and metadata.
    """

    def __init__(self, gcs_address, temp_dir):
        gcs_client_options = ray._raylet.GcsClientOptions.from_gcs_address(gcs_address)
        self.gcs_address = gcs_address

        ray._private.state.state._initialize_global_state(gcs_client_options)
        self.temp_dir = temp_dir
        self.default_service_discovery_flush_period = 5
        super().__init__()

    def get_file_discovery_content(self):
        """Return the content for Prometheus service discovery."""
        nodes = ray.nodes()
        metrics_export_addresses = [
            "{}:{}".format(node["NodeManagerAddress"], node["MetricsExportPort"])
            for node in nodes
            if node["alive"] is True
        ]
        gcs_client = GcsClient(address=self.gcs_address)
        autoscaler_addr = gcs_client.internal_kv_get(b"AutoscalerMetricsAddress", None)
        if autoscaler_addr:
            metrics_export_addresses.append(autoscaler_addr.decode("utf-8"))
        dashboard_addr = gcs_client.internal_kv_get(b"DashboardMetricsAddress", None)
        if dashboard_addr:
            metrics_export_addresses.append(dashboard_addr.decode("utf-8"))
        return json.dumps(
            [{"labels": {"job": "ray"}, "targets": metrics_export_addresses}]
        )

    def write(self):
        # Write a file based on https://prometheus.io/docs/guides/file-sd/
        # Write should be atomic. Otherwise, Prometheus raises an error that
        # json file format is invalid because it reads a file when
        # file is re-written. Note that Prometheus still works although we
        # have this error.
        temp_file_name = self.get_temp_file_name()
        with open(temp_file_name, "w") as json_file:
            json_file.write(self.get_file_discovery_content())
        # NOTE: os.replace is atomic on both Linux and Windows, so we won't
        # have race condition reading this file.
        os.replace(temp_file_name, self.get_target_file_name())

    def get_target_file_name(self):
        return os.path.join(
            self.temp_dir, ray._private.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE
        )

    def get_temp_file_name(self):
        return os.path.join(
            self.temp_dir,
            "{}_{}".format(
                "tmp", ray._private.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE
            ),
        )

    def run(self):
        while True:
            # This thread won't be broken by exceptions.
            try:
                self.write()
            except Exception as e:
                logger.warning(
                    "Writing a service discovery file, {},"
                    "failed.".format(self.get_target_file_name())
                )
                logger.warning(traceback.format_exc())
                logger.warning(f"Error message: {e}")
            time.sleep(self.default_service_discovery_flush_period)
