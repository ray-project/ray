import logging
import threading
from collections import defaultdict
from typing import List

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

from ray._private.metrics_agent import Gauge, Record

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
        self._observations_by_gauge_name = defaultdict(dict)

        prometheus_reader = PrometheusMetricReader()
        provider = MeterProvider(metric_readers=[prometheus_reader])
        metrics.set_meter_provider(provider)
        self.meter = metrics.get_meter(__name__)

    def record_and_export(self, records: List[Record], global_tags=None):
        """
        Record a list of telemetry records and export them to Prometheus.
        """
        global_tags = global_tags or {}

        with self._lock:
            for record in records:
                gauge = record.gauge
                value = record.value
                tags = {**record.tags, **global_tags}
                try:
                    self._record_gauge(gauge, value, tags)
                except Exception as e:
                    logger.error(
                        f"Failed to record metric {gauge.name} with value {value} with tags {tags!r} and global tags {global_tags!r} due to: {e!r}"
                    )

    def _record_gauge(self, gauge: Gauge, value: float, tags: dict):
        # Note: Gauge is a public interface to create a metric in Ray. Currently it is
        # wrapper of OpenCensus view. For backward compatibility with OpenCensus, we
        # are keeping the Gauge internal implementation as is. Once OpenCensus is
        # removed, we can simplify Gauge to only use OpenTelemetry.
        gauge_name = gauge.name
        # Store observation in our internal structure
        self._observations_by_gauge_name[gauge_name][frozenset(tags.items())] = value

        if gauge_name not in self._registered_instruments:
            # Register ObservableGauge with a dynamic callback. Callbacks are special
            # features in OpenTelemetry that allow you to provide a function that will
            # compute the telemetry at collection time.
            def callback(options):
                # Take snapshot of current observations.
                with self._lock:
                    observations = self._observations_by_gauge_name.get(
                        gauge_name, {}
                    ).items()
                return [
                    Observation(val, attributes=dict(tag_set))
                    for tag_set, val in observations
                ]

            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{gauge.name}",
                description=gauge.description or "",
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[gauge.name] = instrument
