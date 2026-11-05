import logging
import os
import threading
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional
from urllib.parse import unquote

from opentelemetry import metrics
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource

import ray
from ray._private.metrics_agent import Record
from ray._private.telemetry.aggregated_histogram import (
    _AggregatedHistogram,
    _AggregatedHistogramProducer,
    _PrometheusMetricReader,
)
from ray._private.telemetry.metric_cardinality import MetricCardinality
from ray._private.telemetry.metric_types import MetricType

logger = logging.getLogger(__name__)

NAMESPACE = "ray"


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
    _prometheus_reader: Optional[_PrometheusMetricReader] = None

    def __init__(self, gauge_metric_ttl_seconds: Optional[float] = None):
        # Lock-ordering contract:
        #   _registration_lock -> SDK meter locks (via meter.create_*)
        #   _registration_lock -> _lock
        #   SDK measurement-consumer lock -> _lock (the SDK holds its own lock
        #       while invoking our observable callbacks during collect(), and the
        #       callbacks acquire _lock)
        # _lock must therefore never be held across an SDK call that can acquire
        # the measurement-consumer lock — i.e. observable-instrument creation
        # (meter.create_observable_*, which registers the instrument with the
        # consumer). Holding _lock there deadlocks with a concurrent scrape:
        # registration holds _lock and waits on the consumer lock inside
        # create_*, while collect() holds the consumer lock and waits on _lock
        # inside a callback. _registration_lock exists so that instrument
        # registration stays serialized (no duplicate instruments on a race)
        # without _lock being held across those SDK entry points.
        #
        # Note: the synchronous histogram path (set_metric_value) does call
        # instrument.record() while holding _lock. That is safe from this
        # deadlock: record() only takes per-reader-storage locks, never the
        # measurement-consumer lock, and reader-storage code never calls back into
        # this recorder, so no lock cycle can form. Keep any new SDK call out of
        # _lock unless it has the same property.
        #
        # aggregated_histogram_producer takes _lock (to snapshot the aggregated
        # histograms) when the reader collects. The reader calls it after the SDK
        # has released the measurement-consumer lock, so it fits the order above.
        self._lock = threading.Lock()
        self._registration_lock = threading.Lock()
        self._registered_instruments = {}
        # Gauge observations are stored as tag_key -> (value, last_update_monotonic).
        # Unlike counters/sums, gauges are evicted once they have not been refreshed
        # within `_gauge_metric_ttl_s` (see the scrape callback below).
        self._gauge_observations_by_name = defaultdict(dict)
        self._counter_observations_by_name = defaultdict(dict)
        self._sum_observations_by_name = defaultdict(dict)
        self._aggregated_histograms: Dict[str, _AggregatedHistogram] = {}
        self._histogram_bucket_mismatch_warned = set()
        self._gauge_metric_ttl_s = self._resolve_gauge_ttl_seconds(
            gauge_metric_ttl_seconds
        )
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)
        self.aggregated_histogram_producer = _AggregatedHistogramProducer(
            self._snapshot_aggregated_histograms,
            scope_name=__name__,
            metric_name_prefix=NAMESPACE,
        )

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
            resource = Resource.create(
                {
                    "service.name": _get_service_name("ray-dashboard-agent"),
                }
            )
            prometheus_reader = _PrometheusMetricReader(resource)
            provider = MeterProvider(
                resource=resource,
                metric_readers=[prometheus_reader],
            )
            metrics.set_meter_provider(provider)
            OpenTelemetryMetricRecorder._prometheus_reader = prometheus_reader
            OpenTelemetryMetricRecorder._metrics_initialized = True

    def serve_aggregated_histograms(self) -> None:
        """Serve the histograms recorded with record_histogram_aggregated_batch
        through the process's Prometheus reader.

        Call it on the recorder that ingests them; calling it again is a no-op.

        Raises:
            RuntimeError: If the process's Prometheus reader was not created, i.e.
                the class was marked initialized without _init_metrics creating it.
        """
        reader = OpenTelemetryMetricRecorder._prometheus_reader
        if reader is None:
            raise RuntimeError(
                "Cannot serve aggregated histograms: OpenTelemetryMetricRecorder "
                "has no Prometheus reader. _metrics_initialized was set without "
                "_init_metrics creating the reader."
            )
        reader.add_metric_producer(self.aggregated_histogram_producer)

    def register_gauge_metric(self, name: str, description: str) -> None:
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Gauge with the same name is already registered.
                    return

            callback = self._create_observable_callback(name, MetricType.GAUGE)
            # Created without holding self._lock: create_observable_gauge acquires
            # SDK-internal locks that are also held around our callbacks during
            # collect() (see the lock-ordering contract in __init__).
            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            with self._lock:
                self._registered_instruments[name] = instrument
                self._gauge_observations_by_name[name] = {}

    def register_counter_metric(self, name: str, description: str) -> None:
        """
        Register an observable counter metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Counter with the same name is already registered. This is a
                    # common case when metrics are exported from multiple Ray
                    # components (e.g., raylet, worker, etc.) running in the same
                    # node. Since each component may export metrics with the same
                    # name, the same metric might be registered multiple times.
                    return

            callback = self._create_observable_callback(name, MetricType.COUNTER)
            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_observable_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            with self._lock:
                self._registered_instruments[name] = instrument
                self._counter_observations_by_name[name] = {}

    def register_sum_metric(self, name: str, description: str) -> None:
        """
        Register an observable sum metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Sum with the same name is already registered. This is a common
                    # case when metrics are exported from multiple Ray components
                    # (e.g., raylet, worker, etc.) running in the same node. Since
                    # each component may export metrics with the same name, the same
                    # metric might be registered multiple times.
                    return

            callback = self._create_observable_callback(name, MetricType.SUM)
            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_observable_up_down_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            with self._lock:
                self._registered_instruments[name] = instrument
                self._sum_observations_by_name[name] = {}

    def register_histogram_metric(
        self, name: str, description: str, buckets: List[float]
    ) -> None:
        """
        Register a histogram metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Histogram with the same name is already registered. This is a
                    # common case when metrics are exported from multiple Ray
                    # components (e.g., raylet, worker, etc.) running in the same
                    # node. Since each component may export metrics with the same
                    # name, the same metric might be registered multiple times.
                    return

            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_histogram(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                explicit_bucket_boundaries_advisory=buckets,
            )

            with self._lock:
                self._registered_instruments[name] = instrument

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
        description: str,
        data_points: List[dict],
    ) -> None:
        """Adds pre-aggregated histogram data points to the histogram with the given
        name.

        Emitters aggregate observations into per-bucket counts and a sum, and export
        the delta since their previous export. Each data point's counts and sum are
        added exactly to the cumulative series of its label set, which
        aggregated_histogram_producer serves (see serve_aggregated_histograms).

        The first data point reported for a name fixes the histogram's bucket
        bounds. A metric name is shared by every process on the node, but processes
        may emit it with different bounds (e.g. two vLLM engines configured with
        different max_model_len), whose counts cannot be added together. Data points
        whose bounds differ from the fixed ones are skipped, with a warning once per
        name, rather than raising, which would abort ingestion of the reporting
        component's other metrics.

        Args:
            name: Metric name, without the "ray_" namespace prefix.
            description: Metric description. Only the one reported first for a
                name is used.
            data_points: Delta data points, each a dict with:

                - "tags" (Dict[str, str]): Labels of the data point. Labels in
                  MetricCardinality's high-cardinality drop list are removed, and
                  empty values are treated as absent.
                - "explicit_bounds" (List[float]): Bucket upper bounds, excluding
                  the implicit +Inf bucket.
                - "bucket_counts" (List[int]): Count per bucket, one more than
                  explicit_bounds for the +Inf bucket.
                - "sum" (float): Sum of the observed values.
        """
        if not data_points:
            return
        with self._lock:
            histogram = self._aggregated_histograms.get(name)
            if histogram is None:
                histogram = _AggregatedHistogram(
                    description=description,
                    bounds=tuple(data_points[0]["explicit_bounds"]),
                )
                self._aggregated_histograms[name] = histogram

            high_cardinality_labels = (
                MetricCardinality.get_high_cardinality_labels_to_drop(name)
            )
            for dp in data_points:
                if not histogram.accepts(dp["explicit_bounds"], dp["bucket_counts"]):
                    if name not in self._histogram_bucket_mismatch_warned:
                        self._histogram_bucket_mismatch_warned.add(name)
                        logger.warning(
                            f"Metric {name} was reported with bucket bounds "
                            f"{list(dp['explicit_bounds'])} and "
                            f"{len(dp['bucket_counts'])} bucket counts, which differ "
                            f"from the bounds {list(histogram.bounds)} it was first "
                            "reported with; skipping the data point. Later "
                            "mismatches of this metric are not logged."
                        )
                    continue

                # Prometheus treats an empty label value as an absent label, so drop
                # empty values to merge data points that differ only in that.
                tag_key = frozenset(
                    (k, v)
                    for k, v in dp["tags"].items()
                    if k not in high_cardinality_labels and v != ""
                )
                histogram.add(tag_key, dp["bucket_counts"], dp["sum"])

    def _snapshot_aggregated_histograms(self) -> Dict[str, _AggregatedHistogram]:
        """Returns copies of the aggregated histograms, for
        aggregated_histogram_producer to convert without holding _lock (which
        ingestion and the observable callbacks also take)."""
        with self._lock:
            return {
                name: histogram.copy()
                for name, histogram in self._aggregated_histograms.items()
            }

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
