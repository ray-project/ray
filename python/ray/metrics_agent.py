import logging

from opencensus.ext.prometheus import stats_exporter as prometheus
from opencensus.stats import aggregation
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module
from opencensus.stats import view

logger = logging.getLogger(__name__)

# Since metrics agent only collects the point data from cpp processes,
# we don't need any metrics types other than gauge.
class Gauge(view.View):
    def __init__(self, name, description, unit):
        self._measure = measure_module.MeasureInt(name, description, unit)
        self._view = view.View(
            name,
            description,
            [],
            self.measure,
            aggregation.LastValueAggregation())

    @property
    def measure(self):
        return self._measure

    @property
    def view(self):
        return self._view


def get_metrics_def():
    registry = {
        "task_count_received": Gauge("task_count_received", "", "1pc")
    }
    return registry


class MetricsAgent:
    def __init__(self, metrics_export_port=8888):
        self.stats = stats_module.stats
        self.view_manager = self.stats.view_manager
        self.stats_recorder = self.stats.stats_recorder
        self.registry = get_metrics_def()
        self.metrics_export_port = metrics_export_port

        # Initialize all metrics.
        for metric in self.registry.values():
            self.view_manager.register_view(metric.view)
        
        # Configure exporter. (We currently only support prometheus).
        self.view_manager.register_exporter(
            prometheus.new_stats_exporter(
                prometheus.Options(namespace="Ray", port=metrics_export_port)))

    def record_metrics_points(self, metrics_points):
        mmap = self.stats_recorder.new_measurement_map()
        tag_map = tag_map_module.TagMap()

        for metric_point in metrics_points:
            self.record(mmap, tag_map, metric_point)
        mmap.record(tag_map)
    
    def record(self, mmap, tag_map, metric_point):
        # NOTE: When we record this metric, timestamp will be renewed.
        metric_name = metric_point.metric_name
        value = metric_point.value
        tags = metric_point.tags

        metric = self.registry.get(metric_name)
        # Q: The check is generous now. Should we assert here instead?
        if not metric:
            logger.warning(
            "metric of name {} is not processed. Please add "
            "metric to MetricsAgent.registry".format(metric_name))
            return
        # We only have double type values.
        mmap.measure_float_put(metric.measure, value)

        for key, value in tags.items():
            tag_key = tag_key_module.TagKey(key)
            tag_value = tag_value_module.TagValue(value)
            tag_map.insert(tag_key, tag_value)
