from opencensus.stats import aggregation
from opencensus.stats import view
from opencensus.stats import stats
from opencensus.stats import measure as measure_module

# Since metrics agent only collects the point data from cpp processes,
# we don't need any metrics types other than gauge.
class Gauge(view.View):
    def __init__(self, name, description, unit):
        self._measure = measure_module.MeasureInt(name, description, unit)
        self._view = view.View(name,
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


class MetricsDef:
    def init(cls):
        registry = [
            Gauge("task_count_received", "", "1pc")
        ]
        return registry


class MetricsAgent:
    def __init__(self):
        self.stats = stats.Stats()
        self.view_manager = stats.view_manager
        self.stats_recorder = stats.stats_recorder
        self.registry = MetricsDef()
        for metric in self.registry.values():
            self.view_manager.register_view(metric.view)

    def record_metrics_points(self, metrics_points):
        mmap = self.stats_recorder.new_measurement_map()

        for metrics_point in metrics_points:
            metric_name = metrics_point.metric_name
            value = metrics_point.value
            tags = metrics_point.tags
            mmap.measure_float_put()
            mmap.measure_float_put(m_latency_ms, 17)
        mmap.measure_int_put(m_lines, 238)
        mmap.measure_int_put(m_bytes_in, 7000)

        # Record the measurements against tags in "tmap"
        mmap.record(tmap)