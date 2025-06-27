import logging
import threading
from collections import defaultdict
from typing import List, Optional

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

from ray._private.metrics_agent import Record

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
                    observations = self._observations_by_name.get(name, {}).items()
                return [
                    Observation(val, attributes=dict(tag_set))
                    for tag_set, val in observations
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
                # Counter with the same name is already registered.
                return

            instrument = self.meter.create_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
            )
            self._registered_instruments[name] = instrument

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
                # Set the value of a synchronous metric with the given name and tags.
                # It is a no-op if the metric is not registered.
                instrument = self._registered_instruments.get(name)
                if isinstance(instrument, metrics.Counter):
                    instrument.add(value, attributes=tags)
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

    def _get_observable_metric_value(self, name: str, tags: dict) -> Optional[float]:
        """
        Get the value of a metric with the given name and tags. This method is mainly
        used for testing purposes.
        """
        return self._observations_by_name[name].get(frozenset(tags.items()), 0.0)
