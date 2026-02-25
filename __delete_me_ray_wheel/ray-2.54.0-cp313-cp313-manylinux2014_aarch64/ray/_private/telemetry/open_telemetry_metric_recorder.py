import logging
import threading
from collections import defaultdict
from typing import Callable, List

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

from ray._private.metrics_agent import Record
from ray._private.telemetry.metric_cardinality import MetricCardinality
from ray._private.telemetry.metric_types import MetricType

logger = logging.getLogger(__name__)

NAMESPACE = "ray"


class OpenTelemetryMetricRecorder:
    """
    A class to record OpenTelemetry metrics. This is the main entry point for exporting
    all ray telemetries to Prometheus server.
    It uses OpenTelemetry's Prometheus exporter to export metrics.
    """

    _metrics_initialized = False
    _metrics_initialized_lock = threading.Lock()

    def __init__(self):
        self._lock = threading.Lock()
        self._registered_instruments = {}
        self._gauge_observations_by_name = defaultdict(dict)
        self._counter_observations_by_name = defaultdict(dict)
        self._sum_observations_by_name = defaultdict(dict)
        self._histogram_bucket_midpoints = defaultdict(list)
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)

    def _create_observable_callback(
        self, metric_name: str, metric_type: MetricType
    ) -> Callable[[dict], List[Observation]]:
        """
        Factory method to create callbacks for observable metrics.

        Args:
            metric_name: name of the metric for which the callback is being created
            metric_type: type of the metric for which the callback is being created

        Returns:
            Callable: A callback function that can be used to record observations for the metric.
        """

        def callback(options):
            with self._lock:
                # Select appropriate storage based on metric type
                if metric_type == MetricType.GAUGE:
                    observations = self._gauge_observations_by_name.get(metric_name, {})
                    # Clear after reading (gauges report last value)
                    self._gauge_observations_by_name[metric_name] = {}
                elif metric_type == MetricType.COUNTER:
                    observations = self._counter_observations_by_name.get(
                        metric_name, {}
                    )
                    # Don't clear - counters are cumulative
                elif metric_type == MetricType.SUM:
                    observations = self._sum_observations_by_name.get(metric_name, {})
                    # Don't clear - sums are cumulative
                else:
                    return []

                # Aggregate by filtered tags (drop high cardinality labels)
                high_cardinality_labels = (
                    MetricCardinality.get_high_cardinality_labels_to_drop(metric_name)
                )
                # First, collect all values that share the same filtered tag set
                values_by_filtered_tags = defaultdict(list)
                for tag_set, val in observations.items():
                    filtered = frozenset(
                        (k, v) for k, v in tag_set if k not in high_cardinality_labels
                    )
                    values_by_filtered_tags[filtered].append(val)

                # Then aggregate each group using the appropriate aggregation function
                agg_fn = MetricCardinality.get_aggregation_function(
                    metric_name, metric_type
                )
                return [
                    Observation(agg_fn(values), attributes=dict(filtered))
                    for filtered, values in values_by_filtered_tags.items()
                ]

        return callback

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

    def register_gauge_metric(self, name: str, description: str) -> None:
        with self._lock:
            if name in self._registered_instruments:
                # Gauge with the same name is already registered.
                return

            callback = self._create_observable_callback(name, MetricType.GAUGE)
            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._gauge_observations_by_name[name] = {}

    def register_counter_metric(self, name: str, description: str) -> None:
        """
        Register an observable counter metric with the given name and description.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Counter with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            callback = self._create_observable_callback(name, MetricType.COUNTER)
            instrument = self.meter.create_observable_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._counter_observations_by_name[name] = {}

    def register_sum_metric(self, name: str, description: str) -> None:
        """
        Register an observable sum metric with the given name and description.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Sum with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            callback = self._create_observable_callback(name, MetricType.SUM)
            instrument = self.meter.create_observable_up_down_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._sum_observations_by_name[name] = {}

    def register_histogram_metric(
        self, name: str, description: str, buckets: List[float]
    ) -> None:
        """
        Register a histogram metric with the given name and description.
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
        """
        Set the value of a metric with the given name and tags.

        For observable metrics (gauge, counter, sum), this stores the value internally
        and returns immediately. The value will be exported asynchronously when
        OpenTelemetry collects metrics.

        For histograms, this calls record() synchronously since there is no observable
        histogram in OpenTelemetry.

        If the metric is not registered, it lazily records the value for observable metrics or is a no-op for
        synchronous metrics.
        """
        with self._lock:
            tag_key = frozenset(tags.items())
            if self._gauge_observations_by_name.get(name) is not None:
                # Gauge - store the most recent value for the given tags.
                self._gauge_observations_by_name[name][tag_key] = value
            elif name in self._counter_observations_by_name:
                # Counter - increment the value for the given tags.
                self._counter_observations_by_name[name][tag_key] = (
                    self._counter_observations_by_name[name].get(tag_key, 0) + value
                )
            elif name in self._sum_observations_by_name:
                # Sum - add the value for the given tags.
                self._sum_observations_by_name[name][tag_key] = (
                    self._sum_observations_by_name[name].get(tag_key, 0) + value
                )
            else:
                # Histogram - record the value synchronously.
                instrument = self._registered_instruments.get(name)
                if isinstance(instrument, metrics.Histogram):
                    # Filter out high cardinality labels.
                    filtered_tags = {
                        k: v
                        for k, v in tags.items()
                        if k
                        not in MetricCardinality.get_high_cardinality_labels_to_drop(
                            name
                        )
                    }
                    instrument.record(value, attributes=filtered_tags)
                else:
                    logger.warning(
                        f"Metric {name} is not registered or unsupported type."
                    )

    def record_histogram_aggregated_batch(
        self,
        name: str,
        data_points: List[dict],
    ) -> None:
        """
        Record pre-aggregated histogram data for multiple data points in a single batch.

        This method takes pre-aggregated bucket counts and reconstructs individual
        observations using bucket midpoints. It acquires the lock once and performs
        all record() calls for ALL data points, minimizing lock contention.

        Note: The histogram sum value will be an approximation since we use bucket midpoints instead of actual values.
        """
        with self._lock:
            instrument = self._registered_instruments.get(name)
            if not isinstance(instrument, metrics.Histogram):
                logger.warning(
                    f"Metric {name} is not a registered histogram, skipping recording."
                )
                return

            bucket_midpoints = self._histogram_bucket_midpoints[name]
            high_cardinality_labels = (
                MetricCardinality.get_high_cardinality_labels_to_drop(name)
            )

            for dp in data_points:
                tags = dp["tags"]
                bucket_counts = dp["bucket_counts"]
                assert len(bucket_counts) == len(
                    bucket_midpoints
                ), "Number of bucket counts and midpoints must match"

                filtered_tags = {
                    k: v for k, v in tags.items() if k not in high_cardinality_labels
                }

                for i, bucket_count in enumerate(bucket_counts):
                    if bucket_count == 0:
                        continue
                    midpoint = bucket_midpoints[i]
                    for _ in range(bucket_count):
                        instrument.record(midpoint, attributes=filtered_tags)

    def record_and_export(self, records: List[Record], global_tags=None):
        """
        Record a list of telemetry records and export them to Prometheus.
        """
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
                    f"Failed to record metric {gauge.name} with value {value} with tags {tags!r} and global tags {global_tags!r} due to: {e!r}"
                )
