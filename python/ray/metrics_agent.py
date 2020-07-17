import logging
import threading

from typing import List

from opencensus.stats import aggregation
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module
from opencensus.stats import view

from ray import prometheus_exporter

logger = logging.getLogger(__name__)

# Since metrics agent only collects the point data from cpp processes,
# we don't need any metrics types other than gauge.
class Gauge(view.View):
    def __init__(self, name, description, unit, tags: List[tag_key_module.TagKey]):
        self._measure = measure_module.MeasureInt(name, description, unit)
        self._view = view.View(
            name,
            description,
            tags,
            self.measure,
            aggregation.LastValueAggregation())

    @property
    def measure(self):
        return self._measure

    @property
    def view(self):
        return self._view


def get_metrics_info():
    """Get metrics name, description, and unit.

    This method should be in sync with metrics_def.h.
    We are using this method to dynamically create the same
    schema as cpp metrics def.
    """
    return {
        "task_count_received": {
            "description": "",
            "unit": "1pc"
        }}


class MetricsAgent:
    def __init__(self, metrics_export_port=8888):
        self.stats = stats_module.stats
        self.view_manager = self.stats.view_manager
        self.stats_recorder = self.stats.stats_recorder
        # Registry is updated dynamically.
        # Every metrics coming from cpp processes will be registered.
        self.registry = {}
        self.metrics_info = get_metrics_info()
        self.metrics_export_port = metrics_export_port
        self._measure_map_lock = threading.Lock()
        self.measure_map = self.stats_recorder.new_measurement_map()

        # Initialize all metrics.
        for metric in self.registry.values():
            self.view_manager.register_view(metric.view)

        # Configure exporter. (We currently only support prometheus).
        self.view_manager.register_exporter(
            prometheus_exporter.new_stats_exporter(
                prometheus_exporter.Options(
                    namespace="ray",
                    port=metrics_export_port)))

    def record_metrics_points(self, metrics_points):
        for metric_point in metrics_points:
            self._register_if_needed(metric_point)
            self.record(metric_point)
    
    def record(self, metric_point):
        # NOTE: When we record this metric, timestamp will be renewed.
        metric_name = metric_point.metric_name
        tags = metric_point.tags

        metric = self.registry.get(metric_name)
        # Q: The check is generous now. Should we assert here instead?
        if not metric:
            # logger.warning(
            # "metric of name {} is not processed. Please add "
            # "metric to MetricsAgent.registry".format(metric_name))
            return

        tag_map = tag_map_module.TagMap()
        for key, value in tags.items():
            tag_key = tag_key_module.TagKey(key)
            tag_value = tag_value_module.TagValue(value)
            tag_map.insert(tag_key, tag_value)

        metric_value = metric_point.value
        # The class should be thread-safe because GRPC server 
        # uses it with a threadpool.
        with self._measure_map_lock:
            self.measure_map.measure_float_put(metric.measure, metric_value)
            self.measure_map.record(tag_map)

    def _register_if_needed(self, metric_point):
        metric_name = metric_point.metric_name
        if metric_name not in self.registry:
            info = self.metrics_info.get(metric_name)
            # TODO(sang): We should assert this.
            if not info:
                return
            metric_description = info["description"]
            metric_unit = info["unit"]

            tags = metric_point.tags
            metric_tags = []
            for tag_key in tags:
                metric_tags.append(tag_key_module.TagKey(tag_key))

            metric = Gauge(metric_name,
                           metric_description,
                           metric_unit,
                           metric_tags)
            self.registry[metric_name] = metric
            self.view_manager.register_view(metric.view)
