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
from opencensus.stats import view

import ray

from ray import prometheus_exporter
from ray.core.generated.common_pb2 import MetricPoint

logger = logging.getLogger(__name__)


# We don't need counter, histogram, or sum because reporter just needs to
# collect momental values (gauge) that are already counted or sampled
# (histogram for example), or summed inside cpp processes.
class Gauge(view.View):
    def __init__(self, name, description, unit,
                 tags: List[tag_key_module.TagKey]):
        self._measure = measure_module.MeasureInt(name, description, unit)
        self._view = view.View(name, description, tags, self.measure,
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

    def record_metrics_points(self, metrics_points: List[MetricPoint]):
        with self._lock:
            measurement_map = self.stats_recorder.new_measurement_map()
            for metric_point in metrics_points:
                self._register_if_needed(metric_point)
                self._record(metric_point, measurement_map)
            return self._missing_information

    def _record(self, metric_point: MetricPoint,
                measurement_map: MeasurementMap):
        """Record a single metric point to export.

        NOTE: When this method is called, the caller should acquire a lock.

        Args:
            metric_point(MetricPoint) metric point defined in common.proto
            measurement_map(MeasurementMap): Measurement map to record metrics.
        """
        metric_name = metric_point.metric_name
        tags = metric_point.tags

        metric = self._registry.get(metric_name)
        # Metrics should be always registered dynamically.
        assert metric

        tag_map = tag_map_module.TagMap()
        for key, value in tags.items():
            tag_key = tag_key_module.TagKey(key)
            tag_value = tag_value_module.TagValue(value)
            tag_map.insert(tag_key, tag_value)

        metric_value = metric_point.value
        measurement_map.measure_float_put(metric.measure, metric_value)
        # NOTE: When we record this metric, timestamp will be renewed.
        measurement_map.record(tag_map)

    def _register_if_needed(self, metric_point: MetricPoint):
        """Register metrics if they are not registered.

        NOTE: When this method is called, the caller should acquire a lock.

        Unseen metrics:
            Register it with Gauge type metrics. Note that all metrics in
            the agent will be gauge because sampling is already done
            within cpp processes.
        Metrics that are missing description & units:
            In this case, we will notify cpp proceses that we need this
            information. Cpp processes will then report description and units
            of all metrics they have.

        Args:
            metric_point metric point defined in common.proto
        Return:
            True if given metrics are missing description and units.
            False otherwise.
        """
        metric_name = metric_point.metric_name
        metric_description = metric_point.description
        metric_units = metric_point.units
        if self._registry[metric_name] is None:
            tags = metric_point.tags
            metric_tags = []
            for tag_key in tags:
                metric_tags.append(tag_key_module.TagKey(tag_key))

            metric = Gauge(metric_name, metric_description, metric_units,
                           metric_tags)
            self._registry[metric_name] = metric
            self.view_manager.register_view(metric.view)

            # If there are missing description & unit information,
            # we should notify cpp processes that we need them.
            if not metric_description or not metric_units:
                self._missing_information = True

        if metric_description and metric_units:
            self._registry[metric_name].view._description = metric_description
            self._registry[
                metric_name].view.measure._description = metric_description
            self._registry[metric_name].view.measure._unit = metric_units
            self._missing_information = False


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
