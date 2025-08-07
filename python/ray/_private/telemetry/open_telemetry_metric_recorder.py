import logging
import threading
from collections import defaultdict
from typing import List

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

from ray._private.metrics_agent import Record
from ray._private.telemetry.metric_cardinality import MetricCardinality

logger = logging.getLogger(__name__)

NAMESPACE = "ray"


class OpenTelemetryMetricRecorder:
    """
    A class to record OpenTelemetry metrics. This is the main entry point for exporting
    all ray telemetries to Prometheus server.
    It uses OpenTelemetry's Prometheus exporter to export metrics.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._registered_instruments = {}
        self._observations_by_name = defaultdict(dict)
        self._histogram_bucket_midpoints = defaultdict(list)

        prometheus_reader = PrometheusMetricReader()
        provider = MeterProvider(metric_readers=[prometheus_reader])
        metrics.set_meter_provider(provider)
        self.meter = metrics.get_meter(__name__)

    def register_gauge_metric(self, name: str, description: str) -> None:
        with self._lock:
            if name in self._registered_instruments:
                # Gauge with the same name is already registered.
                return

            # Register ObservableGauge with a dynamic callback. Callbacks are special
            # features in OpenTelemetry that allow you to provide a function that will
            # compute the telemetry at collection time.
            def callback(options):
                # Take snapshot of current observations.
                with self._lock:
                    observations = self._observations_by_name[name]
                    # Drop high cardinality from tag_set and sum up the value for
                    # same tag set after dropping
                    aggregated_observations = defaultdict(float)
                    high_cardinality_labels = (
                        MetricCardinality.get_high_cardinality_labels_to_drop(name)
                    )
                    for tag_set, val in observations.items():
                        # Convert frozenset back to dict
                        tags_dict = dict(tag_set)
                        # Filter out high cardinality labels
                        filtered_tags = {
                            k: v
                            for k, v in tags_dict.items()
                            if k not in high_cardinality_labels
                        }
                        # Create a key for aggregation
                        filtered_key = frozenset(filtered_tags.items())
                        # Sum up values for the same filtered tag set
                        aggregated_observations[filtered_key] += val

                    return [
                        Observation(val, attributes=dict(tag_set))
                        for tag_set, val in aggregated_observations.items()
                    ]

            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._observations_by_name[name] = {}

    def register_counter_metric(self, name: str, description: str) -> None:
        """
        Register a counter metric with the given name and description.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Counter with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            instrument = self.meter.create_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
            )
            self._registered_instruments[name] = instrument

    def register_sum_metric(self, name: str, description: str) -> None:
        """
        Register a sum metric with the given name and description.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Sum with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            instrument = self.meter.create_up_down_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
            )
            self._registered_instruments[name] = instrument

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
                        lower_bound + buckets[0] / 2.0
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
        Set the value of a metric with the given name and tags. If the metric is not
        registered, it lazily records the value for observable metrics or is a no-op for
        synchronous metrics.
        """
        with self._lock:
            if self._observations_by_name.get(name) is not None:
                # Set the value of an observable metric with the given name and tags. It
                # lazily records the metric value by storing it in a dictionary until
                # the value actually gets exported by OpenTelemetry.
                self._observations_by_name[name][frozenset(tags.items())] = value
            else:
                instrument = self._registered_instruments.get(name)
                tags = {
                    k: v
                    for k, v in tags.items()
                    if k
                    not in MetricCardinality.get_high_cardinality_labels_to_drop(name)
                }
                if isinstance(instrument, metrics.Counter):
                    instrument.add(value, attributes=tags)
                elif isinstance(instrument, metrics.UpDownCounter):
                    instrument.add(value, attributes=tags)
                elif isinstance(instrument, metrics.Histogram):
                    instrument.record(value, attributes=tags)
                else:
                    logger.warning(
                        f"Unsupported synchronous instrument type for metric: {name}."
                    )

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
