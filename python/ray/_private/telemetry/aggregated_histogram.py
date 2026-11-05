import logging
import threading
import time
from dataclasses import dataclass, field
from math import inf
from typing import Callable, Dict, FrozenSet, List, Sequence, Tuple

from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    Histogram,
    HistogramDataPoint,
    Metric,
    MetricsData,
    ResourceMetrics,
    ScopeMetrics,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope

logger = logging.getLogger(__name__)

# The labels of one series, as (key, value) pairs.
TagKey = FrozenSet[Tuple[str, str]]


@dataclass
class _HistogramSeries:
    """Cumulative bucket counts and sum of one histogram label set."""

    bucket_counts: List[int]
    sum: float = 0.0
    # When the series started accumulating, reported as the start time of its
    # cumulative data points.
    start_time_unix_nano: int = field(default_factory=time.time_ns)


@dataclass
class _AggregatedHistogram:
    """A histogram fed with pre-aggregated bucket counts (see
    OpenTelemetryMetricRecorder.record_histogram_aggregated_batch).

    Not thread-safe; the recorder guards every instance with its lock.
    """

    description: str
    # Upper bounds of the buckets, excluding the implicit +Inf bucket.
    bounds: Tuple[float, ...]
    series: Dict[TagKey, _HistogramSeries] = field(default_factory=dict)

    def accepts(self, bounds: Sequence[float], bucket_counts: Sequence[int]) -> bool:
        """Whether a data point's counts can be added to this histogram.

        Args:
            bounds: The data point's bucket upper bounds, excluding +Inf.
            bucket_counts: The data point's per-bucket counts.

        Returns:
            True if the data point has the same bounds as this histogram and one
            count per bucket, including the +Inf bucket.
        """
        return (
            tuple(bounds) == self.bounds and len(bucket_counts) == len(self.bounds) + 1
        )

    def add(self, tag_key: TagKey, bucket_counts: Sequence[int], total: float) -> None:
        """Adds a delta data point to the series of its label set.

        Args:
            tag_key: The labels of the series to add to. A new series starts with
                all counts at zero.
            bucket_counts: Per-bucket counts to add. Callers check them with
                accepts() first.
            total: Sum of the observed values to add.
        """
        series = self.series.get(tag_key)
        if series is None:
            series = _HistogramSeries(bucket_counts=[0] * (len(self.bounds) + 1))
            self.series[tag_key] = series
        for i, count in enumerate(bucket_counts):
            series.bucket_counts[i] += count
        series.sum += total

    def copy(self) -> "_AggregatedHistogram":
        """Returns a copy that later add() calls on this histogram don't change."""
        return _AggregatedHistogram(
            description=self.description,
            bounds=self.bounds,
            series={
                tag_key: _HistogramSeries(
                    bucket_counts=list(series.bucket_counts),
                    sum=series.sum,
                    start_time_unix_nano=series.start_time_unix_nano,
                )
                for tag_key, series in self.series.items()
            },
        )


def _to_metric(name: str, histogram: _AggregatedHistogram, now: int) -> Metric:
    """Converts a histogram into a cumulative OpenTelemetry histogram.

    Args:
        name: The exported metric name.
        histogram: The histogram to convert. Must have at least one series.
        now: Collection time, in nanoseconds since the epoch.

    Returns:
        A Metric with one cumulative data point per series. Every data point has
        the union of the series' label keys, with "" for labels a series lacks, so
        the exported series share one label schema.
    """
    label_keys = sorted({k for tag_key in histogram.series for k, _ in tag_key})
    explicit_bounds = list(histogram.bounds)
    data_points = []
    for tag_key, series in histogram.series.items():
        tags = dict(tag_key)
        data_points.append(
            HistogramDataPoint(
                attributes={k: tags.get(k, "") for k in label_keys},
                start_time_unix_nano=series.start_time_unix_nano,
                time_unix_nano=now,
                count=sum(series.bucket_counts),
                sum=series.sum,
                bucket_counts=series.bucket_counts,
                explicit_bounds=explicit_bounds,
                # Emitters don't report min and max. These are the values the SDK
                # reports when it doesn't record them.
                min=inf,
                max=-inf,
            )
        )
    return Metric(
        name=name,
        description=histogram.description,
        # The same unit as the instruments the recorder creates.
        unit="1",
        data=Histogram(
            data_points=data_points,
            aggregation_temporality=AggregationTemporality.CUMULATIVE,
        ),
    )


class _AggregatedHistogramProducer:
    """Bridges the recorder's pre-aggregated histograms into OpenTelemetry.

    The OpenTelemetry API has no instrument that accepts pre-aggregated bucket
    counts: a synchronous histogram only takes individual measurements. The
    OpenTelemetry metrics SDK specification defines MetricProducer for this case, a
    source of already aggregated data that a MetricReader collects alongside the
    SDK's own metrics. This implements that interface (produce() returning
    ScopeMetrics, see opentelemetry.sdk.metrics.export.MetricProducer in SDK
    versions that have it). The histograms then go through the same exporter as
    every other Ray metric, so they get the same Prometheus naming and labels.

    The recorder does not attach this producer itself. Whoever serves the
    recorder's metrics attaches it once, see
    OpenTelemetryMetricRecorder.serve_aggregated_histograms.
    """

    def __init__(
        self,
        snapshot: Callable[[], Dict[str, _AggregatedHistogram]],
        scope_name: str,
        metric_name_prefix: str,
    ):
        """Creates a producer for the histograms returned by ``snapshot``.

        Args:
            snapshot: Returns the histograms to serve, keyed by metric name. It
                must return copies (see _AggregatedHistogram.copy), since they are
                converted without holding the owner's lock. The producer keeps a
                reference to it, so whatever it is bound to stays alive for as
                long as a reader holds the producer.
            scope_name: Name of the instrumentation scope the metrics are reported
                in.
            metric_name_prefix: Prepended, with an underscore, to every metric
                name.
        """
        self._snapshot = snapshot
        self._scope_name = scope_name
        self._metric_name_prefix = metric_name_prefix

    def produce(self, timeout_millis: float = 10_000) -> List[ScopeMetrics]:
        """Returns the histograms as cumulative OpenTelemetry histograms.

        Args:
            timeout_millis: Time budget for producing the metrics, in milliseconds.
                Part of the MetricProducer interface; unused, since producing only
                copies in-memory series.

        Returns:
            A single ScopeMetrics holding one cumulative histogram per metric name
            that has data, or an empty list if no histogram has been recorded yet.
        """
        now = time.time_ns()
        metrics = [
            _to_metric(f"{self._metric_name_prefix}_{name}", histogram, now)
            for name, histogram in self._snapshot().items()
            if histogram.series
        ]
        if not metrics:
            return []
        # OTel nests metrics as Resource -> Scope -> Metric, so the producer returns
        # them wrapped in a ScopeMetrics.
        return [
            ScopeMetrics(
                scope=InstrumentationScope(name=self._scope_name),
                metrics=metrics,
                schema_url="",
            )
        ]


class _PrometheusMetricReader(PrometheusMetricReader):
    """PrometheusMetricReader that also allows us to pass in metrics following
    the OTEL MetricProducer interface.

    This backports MetricReader(metric_producers=...) from the OpenTelemetry SDK
    (open-telemetry/opentelemetry-python#5419), which adds support for custom
    metric producers. PrometheusMetricReader does not accept that argument yet.
    Once Ray's minimum SDK and exporter versions accept it, pass the producers
    to the reader and delete this class.
    """

    def __init__(self, resource: Resource):
        """Creates a reader with no metric producers attached.

        Args:
            resource: The resource of the MeterProvider this reader will be
                registered with. It is reported with the producers' metrics when
                the SDK has no metrics of its own to take a resource from.
        """
        super().__init__()
        # The resource of the MeterProvider this reader is registered with, used
        # when the SDK has no metrics of its own to take it from. The exporter
        # derives target_info from the first collection and keeps it, so it must be
        # the provider's resource rather than an empty one.
        self._resource = resource
        self._ray_metric_producers: List[_AggregatedHistogramProducer] = []
        self._producers_lock = threading.Lock()
        # Serializes collect() so _received_metrics belongs to a single collection.
        self._collect_lock = threading.Lock()
        self._received_metrics = False

    def add_metric_producer(self, producer: _AggregatedHistogramProducer) -> None:
        """Serves the producer's metrics on every later collection.

        Adding a producer that is already attached is a no-op.

        Args:
            producer: The producer to attach. The reader keeps a reference to it
                for the rest of its lifetime.
        """
        with self._producers_lock:
            if producer not in self._ray_metric_producers:
                self._ray_metric_producers.append(producer)

    def collect(self, timeout_millis: float = 10_000) -> None:
        """Collects the SDK's metrics and the producers' metrics, and hands them
        to the Prometheus exporter.

        Args:
            timeout_millis: Time budget for the collection, in milliseconds. Passed
                to the SDK's collection and to every producer.

        Raises:
            MetricsTimeoutError: If the SDK's observable-instrument callbacks run
                past ``timeout_millis``. Raised by the SDK; the producers' metrics
                are not served for that collection.
        """
        # The SDK only calls _receive_metrics when it has metrics of its own (e.g.
        # not before any gauge was reported, or once all gauges expired). Serve the
        # producers' metrics in that case too.
        with self._collect_lock:
            self._received_metrics = False
            super().collect(timeout_millis=timeout_millis)
            if not self._received_metrics:
                self._receive_metrics(
                    MetricsData(resource_metrics=[]), timeout_millis=timeout_millis
                )

    def _receive_metrics(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs,
    ) -> None:
        """Merges the producers' metrics into the SDK's and passes them on to the
        Prometheus exporter.

        Called by the SDK during collect() when it has metrics of its own, and by
        collect() itself, with empty MetricsData, when it does not.

        Args:
            metrics_data: The metrics the SDK collected. May have no
                resource_metrics.
            timeout_millis: Time budget for the producers, in milliseconds.
            **kwargs: Passed through to PrometheusMetricReader._receive_metrics.
        """
        self._received_metrics = True
        # Merge into the SDK's MetricsData rather than handing the exporter a
        # second one: the exporter re-yields the families of earlier MetricsData
        # for every one it translates in a collection, so a second one would
        # duplicate them. The SDK calls this method from collect() only after its
        # internal collection has returned and released the measurement-consumer
        # lock, so the producers add no lock ordering.
        producer_scope_metrics = self._produce(timeout_millis)
        if producer_scope_metrics:
            metrics_data = self._merge(metrics_data, producer_scope_metrics)
        if metrics_data.resource_metrics:
            super()._receive_metrics(metrics_data, timeout_millis, **kwargs)

    def _produce(self, timeout_millis: float) -> List[ScopeMetrics]:
        """Runs every attached producer.

        A producer that raises is logged and skipped, so its exception does not
        propagate and the other producers' and the SDK's metrics are still served.

        Args:
            timeout_millis: Time budget passed to each producer, in milliseconds.

        Returns:
            The ScopeMetrics of all producers that succeeded, in the order the
            producers were attached. Empty if no producer is attached or none
            produced data.
        """
        with self._producers_lock:
            producers = list(self._ray_metric_producers)
        scope_metrics = []
        for producer in producers:
            try:
                scope_metrics.extend(producer.produce(timeout_millis=timeout_millis))
            except Exception:
                # Isolate a failing producer so the SDK's metrics are still served.
                logger.exception(
                    f"Metric producer {producer} failed to produce metrics, skipping."
                )
        return scope_metrics

    def _merge(
        self, metrics_data: MetricsData, producer_scope_metrics: List[ScopeMetrics]
    ) -> MetricsData:
        """Adds the producers' scopes to the SDK's resource, as the SDK does.

        Args:
            metrics_data: The metrics the SDK collected. May have no
                resource_metrics.
            producer_scope_metrics: The producers' metrics, from _produce().

        Returns:
            New MetricsData. If the SDK reported a resource, the producers' scopes
            are appended to its first ResourceMetrics and any others are kept as
            they are. Otherwise it holds a single ResourceMetrics with this
            reader's resource and the producers' scopes.
        """
        if metrics_data.resource_metrics:
            sdk_resource_metrics = metrics_data.resource_metrics[0]
            merged = ResourceMetrics(
                resource=sdk_resource_metrics.resource,
                scope_metrics=[
                    *sdk_resource_metrics.scope_metrics,
                    *producer_scope_metrics,
                ],
                schema_url=sdk_resource_metrics.schema_url,
            )
            return MetricsData(
                resource_metrics=[merged, *metrics_data.resource_metrics[1:]]
            )
        return MetricsData(
            resource_metrics=[
                ResourceMetrics(
                    resource=self._resource,
                    scope_metrics=producer_scope_metrics,
                    schema_url=self._resource.schema_url,
                )
            ]
        )
