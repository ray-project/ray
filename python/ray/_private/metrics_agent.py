import json
import logging
import os
import threading
import time
import traceback
from collections import namedtuple
from typing import List

from opencensus.stats import aggregation
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.stats.view import View
from opencensus.stats.view_data import ViewData
from opencensus.stats.aggregation_data import (
    CountAggregationData,
    DistributionAggregationData,
    LastValueAggregationData,
)
from opencensus.metrics.export.value import ValueDouble
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module

import ray
from ray._private.gcs_utils import use_gcs_for_bootstrap, GcsClient
from ray._private import services

import ray._private.prometheus_exporter as prometheus_exporter
from ray.core.generated.metrics_pb2 import Metric

logger = logging.getLogger(__name__)


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


class MetricsAgent:
    def __init__(self, metrics_export_address, metrics_export_port):
        assert metrics_export_port is not None
        # OpenCensus classes.
        self.view_manager = stats_module.stats.view_manager
        self.stats_recorder = stats_module.stats.stats_recorder
        # Port where we will expose metrics.
        self.metrics_export_port = metrics_export_port
        # Lock required because gRPC server uses
        # multiple threads to process requests.
        self._lock = threading.Lock()

        # Configure exporter. (We currently only support prometheus).
        self.view_manager.register_exporter(
            prometheus_exporter.new_stats_exporter(
                prometheus_exporter.Options(
                    namespace="ray",
                    port=metrics_export_port,
                    address=metrics_export_address,
                )
            )
        )

    def record_reporter_stats(self, records: List[Record]):
        with self._lock:
            for record in records:
                gauge = record.gauge
                value = record.value
                tags = record.tags
                self._record_gauge(gauge, value, tags)

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

    def record_metric_points_from_protobuf(self, metrics: List[Metric]):
        """Record metrics from Opencensus Protobuf"""
        with self._lock:
            self._record_metrics(metrics)

    def _record_metrics(self, metrics):
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
            view_data = (
                self.view_manager.measure_to_view_map._measure_to_view_data_list_map[
                    measure.name
                ][-1]
            )
            view_data_changed.append(view_data)

            # Create the aggregation and fill it in the our stats
            for series in timeseries:
                tag_vals = tuple(val.value for val in series.label_values)
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

        # Finally, export all the values
        self.view_manager.measure_to_view_map.export(view_data_changed)


class PrometheusServiceDiscoveryWriter(threading.Thread):
    """A class to support Prometheus service discovery.

    It supports file-based service discovery. Checkout
    https://prometheus.io/docs/guides/file-sd/ for more details.

    Args:
        redis_address(str): Ray's redis address.
        redis_password(str): Ray's redis password.
        gcs_address(str): Gcs address for this cluster.
        temp_dir(str): Temporary directory used by
            Ray to store logs and metadata.
    """

    def __init__(self, redis_address, redis_password, gcs_address, temp_dir):
        if use_gcs_for_bootstrap():
            gcs_client_options = ray._raylet.GcsClientOptions.from_gcs_address(
                gcs_address
            )
            self.gcs_address = gcs_address
        else:
            gcs_client_options = ray._raylet.GcsClientOptions.from_redis_address(
                redis_address, redis_password
            )
            self.redis_address = redis_address
            self.redis_password = redis_password

        ray.state.state._initialize_global_state(gcs_client_options)
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
        if not use_gcs_for_bootstrap():
            redis_client = services.create_redis_client(
                self.redis_address, self.redis_password
            )
            autoscaler_addr = redis_client.get("AutoscalerMetricsAddress")
        else:
            gcs_client = GcsClient(address=self.gcs_address)
            autoscaler_addr = gcs_client.internal_kv_get(
                b"AutoscalerMetricsAddress", None
            )
        if autoscaler_addr:
            metrics_export_addresses.append(autoscaler_addr.decode("utf-8"))
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
            self.temp_dir, ray.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE
        )

    def get_temp_file_name(self):
        return os.path.join(
            self.temp_dir,
            "{}_{}".format("tmp", ray.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE),
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
