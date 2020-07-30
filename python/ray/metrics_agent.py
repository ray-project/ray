import logging
import threading

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
