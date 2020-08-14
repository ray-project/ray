import json
import logging
import os
import threading
import time
import traceback

from collections import defaultdict
from typing import List

from opencensus.stats import aggregation
from opencensus.stats import measure as measure_module
from opencensus.stats.measurement_map import MeasurementMap
from opencensus.stats import stats as stats_module
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module
from opencensus.stats.view import View
from opencensus.stats.aggregation_data import CountAggregationData, DistributionAggregationData, LastValueAggregationData
from opencensus.metrics.export.value import ValueDouble

import ray

from ray import prometheus_exporter
from ray.core.generated.common_pb2 import MetricPoint
from ray.core.generated.metrics_pb2 import Metric

logger = logging.getLogger(__name__)


# We don't need counter, histogram, or sum because reporter just needs to
# collect momental values (gauge) that are already counted or sampled
# (histogram for example), or summed inside cpp processes.
class Gauge(View):
    def __init__(self, name, description, unit,
                 tags: List[tag_key_module.TagKey]):
        self._measure = measure_module.MeasureInt(name, description, unit)
        self._view = View(name, description, tags, self.measure,
                          aggregation.LastValueAggregation())

    @property
    def measure(self):
        return self._measure

    @property
    def view(self):
        return self._view

    @property
    def name(self):
        return self.measure.name

    @property
    def description(self):
        return self.measure.description

    @property
    def units(self):
        return self.measure.unit

    @property
    def tags(self):
        return self.view.columns

    def __dict__(self):
        return {
            "name": self.measure.name,
            "description": self.measure.description,
            "units": self.measure.unit,
            "tags": self.view.columns,
        }

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.__dict__())


class MetricsAgent:
    def __init__(self, metrics_export_port):
        assert metrics_export_port is not None
        # OpenCensus classes.
        self.view_manager = stats_module.stats.view_manager
        self.stats_recorder = stats_module.stats.stats_recorder
        # Port where we will expose metrics.
        self.metrics_export_port = metrics_export_port
        # metric name(str) -> view (view.View)
        self._registry = defaultdict(lambda: None)
        # Lock required because gRPC server uses
        # multiple threads to process requests.
        self._lock = threading.Lock()
        # Whether or not there are metrics that are missing description and
        # units information. This is used to dynamically update registry.
        self._missing_information = False

        # Configure exporter. (We currently only support prometheus).
        self.view_manager.register_exporter(
            prometheus_exporter.new_stats_exporter(
                prometheus_exporter.Options(
                    namespace="ray", port=metrics_export_port)))

    @property
    def registry(self):
        """Return metric definition registry.

        Metrics definition registry is dynamically updated
        by metrics reported by Ray processes.
        """
        return self._registry

    def record_metric_points_from_protobuf(self, metrics: List[Metric]):
        view_data_changed = []
        for metric in metrics:
            descriptor = metric.metric_descriptor
            timeseries = metric.timeseries

            columns = [label_key.key for label_key in descriptor.label_keys]
            start_time = timeseries[0].start_timestamp.seconds

            measure = measure_module.BaseMeasure(
                descriptor.name, descriptor.description, descriptor.unit)
            view = self.view_manager.measure_to_view_map.get_view(
                descriptor.name, None)
            if not view:
                view = View(
                    descriptor.name,
                    descriptor.description,
                    columns,
                    measure,
                    aggregation=None)
                self.view_manager.measure_to_view_map.register_view(
                    view, start_time)
            view_data = self.view_manager.measure_to_view_map._measure_to_view_data_list_map[
                measure.name][-1]

            view_data_changed.append(view_data)

            for series in timeseries:
                tag_vals = tuple([val.value for val in series.label_values])
                for point in series.points:
                    harvest_time = point.timestamp.seconds

                    data = None

                    if point.HasField("int64_value"):
                        data = CountAggregationData(point.int64_value)
                    elif point.HasField("double_value"):
                        data = LastValueAggregationData(
                            ValueDouble, point.double_value)
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
                            counts_per_bucket, bucket_bounds)
                    else:
                        raise ValueError("Summary is not supported")

                    view_data.tag_value_aggregation_data_map[tag_vals] = data
                    # print(
                    #     f"updated view_data's agg map {view_data.tag_value_aggregation_data_map}"
                    # )
        self.view_manager.measure_to_view_map.export(view_data_changed)


class PrometheusServiceDiscoveryWriter(threading.Thread):
    """A class to support Prometheus service discovery.

    It supports file-based service discovery. Checkout
    https://prometheus.io/docs/guides/file-sd/ for more details.

    Args:
        redis_address(str): Ray's redis address.
        redis_password(str): Ray's redis password.
        temp_dir(str): Temporary directory used by
            Ray to store logs and metadata.
    """

    def __init__(self, redis_address, redis_password, temp_dir):
        ray.state.state._initialize_global_state(
            redis_address=redis_address, redis_password=redis_password)
        self.temp_dir = temp_dir
        self.default_service_discovery_flush_period = 5
        super().__init__()

    def get_file_discovery_content(self):
        """Return the content for Prometheus serivce discovery."""
        nodes = ray.nodes()
        metrics_export_addresses = [
            "{}:{}".format(node["NodeManagerAddress"],
                           node["MetricsExportPort"]) for node in nodes
        ]
        return json.dumps([{
            "labels": {
                "job": "ray"
            },
            "targets": metrics_export_addresses
        }])

    def write(self):
        # Write a file based on https://prometheus.io/docs/guides/file-sd/
        # Write should be atomic. Otherwise, Prometheus raises an error that
        # json file format is invalid because it reads a file when
        # file is re-written. Note that Prometheus still works although we
        # have this error.
        temp_file_name = self.get_temp_file_name()
        with open(temp_file_name, "w") as json_file:
            json_file.write(self.get_file_discovery_content())
        # NOTE: os.rename is atomic, so we won't have race condition reading
        # this file.
        os.rename(temp_file_name, self.get_target_file_name())

    def get_target_file_name(self):
        return os.path.join(
            self.temp_dir, ray.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE)

    def get_temp_file_name(self):
        return os.path.join(
            self.temp_dir, "{}_{}".format(
                "tmp", ray.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE))

    def run(self):
        while True:
            # This thread won't be broken by exceptions.
            try:
                self.write()
            except Exception as e:
                logger.warning("Writing a service discovery file, {},"
                               "failed."
                               .format(self.writer.get_target_file_name()))
                logger.warning(traceback.format_exc())
                logger.warning("Error message: {}".format(e))
            time.sleep(self.default_service_discovery_flush_period)
