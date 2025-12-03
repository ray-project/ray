import logging
import threading
from collections import defaultdict
from enum import Enum
from typing import Dict, List

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

from ray._private.metrics_agent import Record
from ray._private.telemetry.metric_cardinality import MetricCardinality

logger = logging.getLogger(__name__)

NAMESPACE = "ray"


class MetricType(Enum):
    """Metric type for internal routing in set_metric_value."""

    GAUGE = "gauge"
    COUNTER = "counter"
    SUM = "sum"
    HISTOGRAM = "histogram"


class OpenTelemetryMetricRecorder:
    """
    A class to record OpenTelemetry metrics. This is the main entry point for exporting
    all ray telemetries to Prometheus server.
    It uses OpenTelemetry's Prometheus exporter to export metrics.

    Performance optimization: Gauges, counters, and sums use a "lazy observation"
    pattern where values are buffered in memory and only exported to OTEL SDK during
    Prometheus collection. This decouples hot-path metric recording from OTEL SDK
    overhead. Histograms use synchronous recording.
    """

    _metrics_initialized = False
    _metrics_initialized_lock = threading.Lock()

    def __init__(self):
        self._lock = threading.Lock()
        self._registered_instruments = {}
        # Maps metric name -> metric type for routing in set_metric_value
        self._metric_name_to_type: Dict[str, MetricType] = {}
        # Lazy observation storage for gauges, counters, and sums
        # For gauges: stores latest value per tag set (cleared on collection)
        # For counters/sums: stores accumulated value per tag set (persisted)
        self._observations_by_name: Dict[str, Dict[frozenset, float]] = defaultdict(
            dict
        )
        self._histogram_bucket_midpoints = defaultdict(list)
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)

    def _init_metrics(self):
        # Initialize the global metrics provider and meter. We only do this once on
        # the first initialization of the class, because re-setting the meter provider
        # can result in loss of metrics.
        with self._metrics_initialized_lock:
            if self._metrics_initialized:
                return
            prometheus_reader = PrometheusMetricReader()
            provider = MeterProvider(metric_readers=[prometheus_reader])
            metrics.set_meter_provider(provider)
            self._metrics_initialized = True

    def _create_observable_callback(self, name: str, is_cumulative: bool = False):
        """Create a callback for observable metrics (gauges and counters).

        Args:
            name: The metric name
            is_cumulative: If True, don't clear observations after collection (for counters/sums)
        """

        def callback(options):
            with self._lock:
                observations = self._observations_by_name.get(name, {})
                if not is_cumulative:
                    # For gauges, clear after collection to avoid stale data
                    self._observations_by_name[name] = {}

                # Drop high cardinality labels and aggregate
                aggregated_observations: Dict[frozenset, float] = defaultdict(float)
                high_cardinality_labels = (
                    MetricCardinality.get_high_cardinality_labels_to_drop(name)
                )
                for tag_set, val in observations.items():
                    tags_dict = dict(tag_set)
                    filtered_tags = {
                        k: v
                        for k, v in tags_dict.items()
                        if k not in high_cardinality_labels
                    }
                    filtered_key = frozenset(filtered_tags.items())
                    if is_cumulative:
                        # For counters/sums, take max (they report cumulative values)
                        aggregated_observations[filtered_key] = max(
                            aggregated_observations[filtered_key], val
                        )
                    else:
                        # For gauges, sum up values with same filtered tags
                        aggregated_observations[filtered_key] += val

                return [
                    Observation(val, attributes=dict(tag_set))
                    for tag_set, val in aggregated_observations.items()
                ]

        return callback

    def register_gauge_metric(self, name: str, description: str) -> None:
        """Register a gauge metric using ObservableGauge.

        Values are stored in memory and cleared after each Prometheus collection.
        """
        with self._lock:
            if name in self._registered_instruments:
                return

            callback = self._create_observable_callback(name, is_cumulative=False)
            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._metric_name_to_type[name] = MetricType.GAUGE
            self._observations_by_name[name] = {}

    def register_counter_metric(self, name: str, description: str) -> None:
        """Register a counter metric using ObservableCounter for lazy collection.

        Values are accumulated in memory and only exported during Prometheus scrape.
        This avoids synchronous OTEL SDK calls in the hot path.
        """
        with self._lock:
            if name in self._registered_instruments:
                return

            callback = self._create_observable_callback(name, is_cumulative=True)
            instrument = self.meter.create_observable_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._metric_name_to_type[name] = MetricType.COUNTER
            self._observations_by_name[name] = {}

    def register_sum_metric(self, name: str, description: str) -> None:
        """Register a sum metric (up-down counter) using ObservableUpDownCounter.

        Values are accumulated in memory and only exported during Prometheus scrape.
        """
        with self._lock:
            if name in self._registered_instruments:
                return

            callback = self._create_observable_callback(name, is_cumulative=True)
            instrument = self.meter.create_observable_up_down_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._metric_name_to_type[name] = MetricType.SUM
            self._observations_by_name[name] = {}

    def register_histogram_metric(
        self, name: str, description: str, buckets: List[float]
    ) -> None:
        """Register a histogram metric.

        Histograms use synchronous recording since OTEL doesn't support observable
        histograms.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Histogram with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            instrument = self.meter.create_histogram(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                explicit_bucket_boundaries_advisory=buckets,
            )
            self._registered_instruments[name] = instrument
            self._metric_name_to_type[name] = MetricType.HISTOGRAM

            # calculate the bucket midpoints; this is used for converting histogram
            # internal representation to approximated histogram data points.
            for i in range(len(buckets)):
                if i == 0:
                    lower_bound = 0.0 if buckets[0] > 0 else buckets[0] * 2.0
                    self._histogram_bucket_midpoints[name].append(
                        (lower_bound + buckets[0]) / 2.0
                    )
                else:
                    self._histogram_bucket_midpoints[name].append(
                        (buckets[i] + buckets[i - 1]) / 2.0
                    )
            # Approximated mid point for Inf+ bucket. Inf+ bucket is an implicit bucket
            # that is not part of buckets.
            self._histogram_bucket_midpoints[name].append(
                1.0 if buckets[-1] <= 0 else buckets[-1] * 2.0
            )

    def get_histogram_bucket_midpoints(self, name: str) -> List[float]:
        """
        Get the bucket midpoints for a histogram metric with the given name.
        """
        return self._histogram_bucket_midpoints[name]

    def set_metric_value(self, name: str, tags: dict, value: float):
        """Set the value of a metric.

        - Gauges: stores latest value, cleared on collection (lazy)
        - Counters/Sums: accumulates value, persisted across collections (lazy)
        - Histograms: records directly to OTEL SDK (synchronous)

        For gauges/counters/sums, this method is optimized for high-frequency calls -
        it only does dict operations under a brief lock, with no OTEL SDK calls.
        """
        tag_key = frozenset(tags.items())

        with self._lock:
            metric_type = self._metric_name_to_type.get(name)

            if metric_type == MetricType.GAUGE:
                # Gauge: store latest value (will be cleared on collection)
                self._observations_by_name[name][tag_key] = value
                return

            if metric_type in (MetricType.COUNTER, MetricType.SUM):
                # Counter/Sum: accumulate value (persisted across collections)
                current = self._observations_by_name[name].get(tag_key, 0.0)
                self._observations_by_name[name][tag_key] = current + value
                return

            if metric_type == MetricType.HISTOGRAM:
                # Histogram: record synchronously
                instrument = self._registered_instruments.get(name)
                if instrument is not None:
                    high_cardinality_labels = (
                        MetricCardinality.get_high_cardinality_labels_to_drop(name)
                    )
                    filtered_tags = {
                        k: v
                        for k, v in tags.items()
                        if k not in high_cardinality_labels
                    }
                    instrument.record(value, attributes=filtered_tags)
                return

        # Metric not registered
        logger.warning(f"Metric not registered: {name}")

    def record_and_export(self, records: List[Record], global_tags=None):
        """Record a list of telemetry records and export them to Prometheus."""
        global_tags = global_tags or {}

        for record in records:
            gauge = record.gauge
            value = record.value
            tags = {**record.tags, **global_tags}
            try:
                self.register_gauge_metric(gauge.name, gauge.description or "")
                self.set_metric_value(gauge.name, tags, value)
            except Exception as e:
                logger.error(
                    f"Failed to record metric {gauge.name} with value {value} "
                    f"with tags {tags!r} and global tags {global_tags!r} due to: {e!r}"
                )
