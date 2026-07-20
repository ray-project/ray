import bisect
import logging
import os
import threading
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional
from urllib.parse import unquote

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

import ray
from ray._private.metrics_agent import Record
from ray._private.telemetry.metric_cardinality import MetricCardinality
from ray._private.telemetry.metric_types import MetricType

logger = logging.getLogger(__name__)

NAMESPACE = "ray"


class _HistogramDef:
    """Static definition of a histogram metric: boundaries, midpoints, and the
    precomputed Prometheus ``le`` labels (boundaries plus the implicit +Inf)."""

    __slots__ = ("description", "boundaries", "midpoints", "le_labels")

    def __init__(self, description: str, boundaries: List[float]):
        self.description = description
        self.boundaries = list(boundaries)
        self.midpoints = _histogram_bucket_midpoints(boundaries)
        self.le_labels = [str(float(b)) for b in self.boundaries] + ["+Inf"]


class _HistogramData:
    """Accumulated histogram aggregation for one label set.

    ``bucket_counts`` holds per-bucket (non-cumulative) counts, with the
    implicit +Inf bucket as the last element.
    """

    __slots__ = ("bucket_counts", "sum")

    def __init__(self, num_buckets: int):
        self.bucket_counts = [0] * num_buckets
        self.sum = 0.0


def _histogram_bucket_midpoints(buckets: List[float]) -> List[float]:
    """Approximate midpoints for each histogram bucket, including +Inf."""
    midpoints = []
    for i in range(len(buckets)):
        if i == 0:
            lower_bound = 0.0 if buckets[0] > 0 else buckets[0] * 2.0
            midpoints.append((lower_bound + buckets[0]) / 2.0)
        else:
            midpoints.append((buckets[i] + buckets[i - 1]) / 2.0)
    # Approximated mid point for Inf+ bucket. Inf+ bucket is an implicit bucket
    # that is not part of buckets.
    midpoints.append(1.0 if buckets[-1] <= 0 else buckets[-1] * 2.0)
    return midpoints


class _HistogramPrometheusCollector:
    """prometheus_client collector rendering the accumulated histogram states.

    Histograms are aggregated directly into per-bucket counts (see
    ``record_histogram_aggregated_batch``) instead of going through
    OpenTelemetry SDK instruments, so this collector renders the same
    ``<ns>_<name>_bucket/_count/_sum`` families the SDK's Prometheus reader
    would have produced.
    """

    def collect(self):
        from prometheus_client.core import HistogramMetricFamily

        with OpenTelemetryMetricRecorder._histogram_lock:
            defs = dict(OpenTelemetryMetricRecorder._histogram_defs)
            states = {
                name: {
                    tag_set: (list(data.bucket_counts), data.sum)
                    for tag_set, data in by_tags.items()
                }
                for name, by_tags in OpenTelemetryMetricRecorder._histogram_states.items()
            }

        for name, hist_def in defs.items():
            by_tags = states.get(name)
            if not by_tags:
                continue
            # Keep a single label schema per metric, padding missing keys, to
            # match the observable-metric export behavior.
            label_keys = sorted({k for tag_set in by_tags for k, _ in tag_set})
            family = HistogramMetricFamily(
                f"{NAMESPACE}_{name}",
                hist_def.description,
                labels=label_keys,
            )
            for tag_set, (bucket_counts, sum_value) in by_tags.items():
                tags = dict(tag_set)
                cumulative = 0
                buckets = []
                for le, count in zip(hist_def.le_labels, bucket_counts):
                    cumulative += count
                    buckets.append((le, cumulative))
                family.add_metric(
                    [tags.get(k, "") for k in label_keys],
                    buckets=buckets,
                    sum_value=sum_value,
                )
            yield family


def _get_service_name(default_name: str) -> str:
    otel_service_name = os.environ.get("OTEL_SERVICE_NAME")
    if otel_service_name:
        return otel_service_name

    otel_resource_attributes = os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")

    for attribute in otel_resource_attributes.split(","):
        key, sep, value = attribute.partition("=")
        if sep and key.strip() == "service.name" and value.strip():
            return unquote(value.strip())

    return default_name


class OpenTelemetryMetricRecorder:
    """
    A class to record OpenTelemetry metrics. This is the main entry point for exporting
    all ray telemetries to Prometheus server.
    It uses OpenTelemetry's Prometheus exporter to export metrics.
    """

    _metrics_initialized = False
    _metrics_initialized_lock = threading.Lock()

    # Histogram aggregation state is shared across recorder instances (all
    # instances in a process share the global meter provider and Prometheus
    # registry), guarded by its own lock.
    _histogram_lock = threading.Lock()
    # metric name -> _HistogramDef
    _histogram_defs: Dict[str, _HistogramDef] = {}
    # metric name -> {frozenset(tag items) -> _HistogramData}
    _histogram_states: Dict[str, Dict[frozenset, _HistogramData]] = {}

    def __init__(self, gauge_metric_ttl_seconds: Optional[float] = None):
        self._lock = threading.Lock()
        self._registered_instruments = {}
        # Gauge observations are stored as tag_key -> (value, last_update_monotonic).
        # Unlike counters/sums, gauges are evicted once they have not been refreshed
        # within `_gauge_metric_ttl_s` (see the scrape callback below).
        self._gauge_observations_by_name = defaultdict(dict)
        self._counter_observations_by_name = defaultdict(dict)
        self._sum_observations_by_name = defaultdict(dict)
        self._gauge_metric_ttl_s = self._resolve_gauge_ttl_seconds(
            gauge_metric_ttl_seconds
        )
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)

    @staticmethod
    def _resolve_gauge_ttl_seconds(override: Optional[float]) -> float:
        """Returns how long (in seconds) a gauge observation is retained without a
        refresh before it is evicted on scrape.

        Emitters export their live gauge values to the agent every export interval.
        The effective export interval is ``max(metrics_report_interval_ms, 1000)`` --
        the emitter floors it at 1000ms via ``SetReportInterval`` in
        ``src/ray/stats/stats.h`` (``GetReportInterval()`` is what actually drives the
        OTLP export in ``InitOpenTelemetryExporter``). We apply the same floor here so
        the TTL is exactly 2x the true export cadence, even when the raw config value
        is below 1000ms. Retaining a value for 2 export intervals lets an
        actively-reported series survive a missed/late export, while a series that
        stops being reported (finished task, dead worker) ages out after ~2 intervals.

        NOTE: this mirrors the ``max(..., 1000)`` clamp and the export cadence in
        ``stats.h``. If either changes, update this derivation.

        Callers may pass an explicit ``override`` (used by tests and available for
        future injection from above).
        """
        if override is not None:
            return override
        # Mirror the emitter's SetReportInterval floor (stats.h): the export cadence is
        # max(metrics_report_interval_ms, 1000ms), never less than 1s.
        effective_report_interval_ms = max(
            ray._config.metrics_report_interval_ms(), 1000
        )
        return 2 * effective_report_interval_ms / 1000.0

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
                    # Gauges report the last value. Instead of clearing after each
                    # scrape (which drops a series between reports if the emitter
                    # hasn't re-reported in time), retain each value for a TTL and
                    # evict only observations that have gone stale.
                    stored = self._gauge_observations_by_name.get(metric_name, {})
                    now = time.monotonic()
                    retained = {}
                    observations = {}
                    for tag_set, (val, ts) in stored.items():
                        if now - ts <= self._gauge_metric_ttl_s:
                            retained[tag_set] = (val, ts)
                            observations[tag_set] = val
                    self._gauge_observations_by_name[metric_name] = retained
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
                # Keep a single label schema for each metric before passing
                # observations to the Prometheus exporter.
                all_keys = sorted(
                    {k for filtered in values_by_filtered_tags for k, _ in filtered}
                )

                observations = []
                for filtered, values in values_by_filtered_tags.items():
                    attrs = dict(filtered)
                    observations.append(
                        Observation(
                            agg_fn(values),
                            attributes={k: attrs.get(k, "") for k in all_keys},
                        )
                    )
                return observations

        return callback

    def _init_metrics(self):
        # Initialize the global metrics provider and meter. We only do this once on
        # the first initialization of the class, because re-setting the meter provider
        # can result in loss of metrics.
        with OpenTelemetryMetricRecorder._metrics_initialized_lock:
            if OpenTelemetryMetricRecorder._metrics_initialized:
                return
            from opentelemetry.sdk.resources import Resource

            prometheus_reader = PrometheusMetricReader()
            provider = MeterProvider(
                resource=Resource.create(
                    {
                        "service.name": _get_service_name("ray-dashboard-agent"),
                    }
                ),
                metric_readers=[prometheus_reader],
            )
            metrics.set_meter_provider(provider)

            from prometheus_client import REGISTRY

            REGISTRY.register(_HistogramPrometheusCollector())
            OpenTelemetryMetricRecorder._metrics_initialized = True

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

        Histograms are aggregated directly into shared per-bucket counts and
        rendered by ``_HistogramPrometheusCollector``. They do not go through
        an OpenTelemetry SDK instrument, so their registry is the process-wide
        ``_histogram_defs`` rather than the per-instance ``_registered_instruments``.
        """
        with OpenTelemetryMetricRecorder._histogram_lock:
            if name in OpenTelemetryMetricRecorder._histogram_defs:
                # Histogram with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return
            OpenTelemetryMetricRecorder._histogram_defs[name] = _HistogramDef(
                description, buckets
            )
            OpenTelemetryMetricRecorder._histogram_states[name] = {}

    def get_histogram_bucket_midpoints(self, name: str) -> List[float]:
        """
        Get the bucket midpoints for a histogram metric with the given name.
        """
        return OpenTelemetryMetricRecorder._histogram_defs[name].midpoints

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
                # Gauge - store the most recent value and its timestamp for the given
                # tags. The timestamp is used to evict stale observations on scrape.
                self._gauge_observations_by_name[name][tag_key] = (
                    value,
                    time.monotonic(),
                )
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
                # Histogram - accumulate the single observation into the shared
                # aggregation state.
                if name in OpenTelemetryMetricRecorder._histogram_defs:
                    # Filter out high cardinality labels.
                    filtered_tags = {
                        k: v
                        for k, v in tags.items()
                        if k
                        not in MetricCardinality.get_high_cardinality_labels_to_drop(
                            name
                        )
                    }
                    self._observe_histogram(name, filtered_tags, value)
                else:
                    logger.warning(
                        f"Metric {name} is not registered or unsupported type."
                    )

    @staticmethod
    def _get_or_create_histogram_data(states, filtered_tags, num_buckets):
        """Returns the accumulation for one label set, creating it if absent.

        Callers must hold ``_histogram_lock``.
        """
        tag_key = frozenset(filtered_tags.items())
        data = states.get(tag_key)
        if data is None:
            data = states[tag_key] = _HistogramData(num_buckets)
        return data

    def _observe_histogram(self, name: str, filtered_tags: dict, value: float) -> None:
        """Accumulate one observation with its true value into the shared state."""
        with OpenTelemetryMetricRecorder._histogram_lock:
            hist_def = OpenTelemetryMetricRecorder._histogram_defs[name]
            states = OpenTelemetryMetricRecorder._histogram_states[name]
            data = self._get_or_create_histogram_data(
                states, filtered_tags, len(hist_def.midpoints)
            )
            bucket_index = bisect.bisect_left(hist_def.boundaries, value)
            data.bucket_counts[bucket_index] += 1
            data.sum += value

    def record_histogram_aggregated_batch(
        self,
        name: str,
        data_points: List[dict],
    ) -> None:
        """
        Record pre-aggregated histogram data for multiple data points in a single batch.

        Each data point's per-bucket delta counts are added directly to the
        shared aggregation state, so the cost is O(data points x buckets)
        regardless of how many observations the buckets represent.

        Note: The histogram sum value will be an approximation since we use bucket midpoints instead of actual values.
        """
        with OpenTelemetryMetricRecorder._histogram_lock:
            hist_def = OpenTelemetryMetricRecorder._histogram_defs.get(name)
            if hist_def is None:
                logger.warning(
                    f"Metric {name} is not a registered histogram, skipping recording."
                )
                return

            bucket_midpoints = hist_def.midpoints
            states = OpenTelemetryMetricRecorder._histogram_states[name]
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
                data = self._get_or_create_histogram_data(
                    states, filtered_tags, len(bucket_midpoints)
                )

                for i, bucket_count in enumerate(bucket_counts):
                    if bucket_count == 0:
                        continue
                    data.bucket_counts[i] += bucket_count
                    data.sum += bucket_count * bucket_midpoints[i]

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
