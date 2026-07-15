"""Mirror any Prometheus /metrics exposition into Ray custom metrics.

`PrometheusCollector` is service-agnostic: give it a scrape URL and it
re-publishes every series it finds through :mod:`ray.util.metrics`, so they
surface in Ray's Prometheus export with a ``ray_`` prefix. Metric names,
types, labels, and HELP text are discovered from the exposition itself --
nothing is hard-coded, so it works unchanged against SGLang, vLLM, TGI,
node_exporter, or any endpoint that speaks the Prometheus text format.

    collector = PrometheusCollector("http://127.0.0.1:30000/metrics")
    collector.start()   # scrape every 2s in a daemon thread
    ...
    collector.stop()

`start()` scrapes immediately and retries failed scrapes with backoff, so
it is safe to call before the endpoint is up.

The exposition is parsed by the official ``prometheus_client`` parser, so
escaping, +Inf/NaN, exemplars, and histogram/summary sub-samples are all
handled correctly.

Translation notes:

- **Counters** are cumulative in Prometheus but Ray's ``Counter`` takes
  deltas, so the collector keeps the last cumulative value per series and
  records the difference. A value that goes *backwards* means the source
  restarted; the new reading is the increment since zero and is recorded
  as-is rather than dropped.
- **Histograms and summaries** are mirrored as gauges: their ``_bucket`` /
  ``_sum`` / ``_count`` sub-series are pre-aggregated cumulative values, and
  Ray's native ``Histogram`` wants raw ``observe()`` calls we don't have.
  Monotonically-set gauges still give correct results under ``rate()`` /
  ``histogram_quantile()``.
"""

import logging
import math
import threading
import urllib.request
from typing import Callable, Dict, Iterable, Optional, Protocol, Tuple, Union

from prometheus_client.parser import text_string_to_metric_families

from ray.util.annotations import DeveloperAPI
from ray.util.metric_registry import MetricKind, MetricRegistry

logger = logging.getLogger(__name__)

# A sample-level filter returns True to skip the sample. Applied both when
# declaring metrics and when writing values.
SampleFilter = Callable[..., bool]


def _skip_created(sample) -> bool:
    """Skip `_created` series (creation timestamps, not useful to mirror)."""
    return sample.name.endswith("_created")


def _skip_nan(sample) -> bool:
    """Skip NaN values (e.g. summary quantiles with no observations)."""
    return math.isnan(sample.value)


@DeveloperAPI
class Source(Protocol):
    """Anything that can produce one Prometheus text exposition."""

    def fetch(self) -> str:
        ...


@DeveloperAPI
class HTTPSource:
    """Fetch an exposition over HTTP(S) with a bounded timeout."""

    def __init__(self, url: str, timeout_s: float = 5.0):
        self.url = url
        self.timeout_s = timeout_s

    def fetch(self) -> str:
        with urllib.request.urlopen(self.url, timeout=self.timeout_s) as response:
            return response.read().decode()


@DeveloperAPI
class PrometheusCollector:
    """Mirror a Prometheus exposition endpoint into Ray custom metrics.

    The common case is two lines::

        PrometheusCollector("http://127.0.0.1:30000/metrics").start()

    `scrape` is pure with respect to the network, so it can be unit-tested
    against a captured /metrics fixture; `collect_once` pulls one exposition
    from the source and scrapes it; `start`/`stop` run `collect_once` on an
    interval in a daemon thread.

    Args:
        url: A ``/metrics`` URL to scrape over HTTP(S), or anything with a
            ``fetch() -> str`` method returning one Prometheus text
            exposition (e.g. :class:`HTTPSource`, a file reader, a test
            fixture).
        registry: Optional pre-built :class:`MetricRegistry` to mirror into
            (e.g. one shared with first-party metrics). Mutually exclusive
            with ``namespace``; when omitted, a registry is created from
            ``namespace``.
        namespace: Prefix for every mirrored metric name. Leave it empty for
            sources that already namespace their metrics (e.g. ``sglang:*``,
            ``vllm:*``); set it for ones that don't (e.g. node_exporter).
        timeout_s: HTTP timeout per scrape. Only used when ``url`` is a
            string; a ``Source`` object owns its own timeout.
        filters: Sample-level predicates; a sample is skipped when any
            returns True. Defaults to skipping ``_created`` series and NaN
            values.

    Raises:
        ValueError: if both ``registry`` and ``namespace`` are passed.
    """

    def __init__(
        self,
        url: Union[str, Source],
        registry: Optional[MetricRegistry] = None,
        *,
        namespace: str = "",
        timeout_s: float = 5.0,
        filters: Iterable[SampleFilter] = (),
    ):
        if registry is not None and namespace:
            raise ValueError("Pass `namespace` or a pre-built `registry`, not both.")
        self._source = HTTPSource(url, timeout_s) if isinstance(url, str) else url
        self._reg = registry if registry is not None else MetricRegistry(namespace)
        self._filters = tuple(filters) or (_skip_created, _skip_nan)
        # (sample name, sorted label items) -> last cumulative counter value.
        self._prev: Dict[Tuple, float] = {}
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def registry(self) -> MetricRegistry:
        """The registry mirrored metrics are created in."""
        return self._reg

    def collect_once(self) -> None:
        """Fetch one exposition from the source and mirror it."""
        self.scrape(self._source.fetch())

    def scrape(self, text: str) -> None:
        """Parse one exposition and mirror every sample."""
        families = list(text_string_to_metric_families(text))

        # Pass 1: declare each sample name once, with the union of the label
        # keys seen across its series and the HELP text as description.
        # Declaration is per *sample* name, not per family name: histogram /
        # summary sub-series (`_bucket`/`_sum`/`_count`, quantiles) record
        # under their own names, and declaring them lazily at write time
        # would fix their tag_keys to whatever the first series carried.
        for family in families:
            label_keys: Dict[str, set] = {}
            for sample in family.samples:
                if self._skip(sample):
                    continue
                label_keys.setdefault(sample.name, set()).update(sample.labels)
            for name, keys in label_keys.items():
                self._reg.declare(
                    name,
                    self._kind_for(family, name),
                    keys,
                    description=family.documentation,
                )

        # Pass 2: write values.
        for family in families:
            for sample in family.samples:
                if self._skip(sample):
                    continue
                self._write(family, sample)

    def start(self, interval_s: float = 2.0) -> "PrometheusCollector":
        """Scrape every ``interval_s`` seconds in a daemon thread.

        Idempotent while running; restartable after `stop`. Scrape errors are
        logged and retried with exponential backoff (1s, capped at 10s)
        rather than killing the loop. Returns ``self``.

        Args:
            interval_s: Seconds between scrapes.

        Returns:
            This collector, so ``PrometheusCollector(url).start()`` chains.
        """
        if self._thread is None:
            # Fresh event per run: reusing the event set by a prior stop()
            # would make the new thread exit before its first scrape, and
            # clear()-ing it instead would un-signal a previous thread still
            # draining its join timeout. The event is passed into _run so a
            # lingering old thread keeps watching its own (set) event.
            self._stop_event = threading.Event()
            self._thread = threading.Thread(
                target=self._run,
                args=(self._stop_event, interval_s),
                name="prometheus-collector",
                daemon=True,
            )
            self._thread.start()
        return self

    def stop(self, timeout_s: float = 5.0) -> None:
        """Signal the scrape loop to exit and wait for the thread to finish."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout_s)
            self._thread = None

    def _run(self, stop_event: threading.Event, interval_s: float) -> None:
        consecutive_errors = 0
        while not stop_event.is_set():
            try:
                self.collect_once()
                consecutive_errors = 0
                wait_s = interval_s
            except Exception:
                logger.exception("Error scraping Prometheus metrics.")
                # Exponential backoff starting at 1s and capping at 10s.
                wait_s = min(10, 2**consecutive_errors)
                consecutive_errors += 1
            stop_event.wait(wait_s)

    def _skip(self, sample) -> bool:
        return any(f(sample) for f in self._filters)

    @staticmethod
    def _kind_for(family, sample_name: str) -> MetricKind:
        """Counter families' cumulative sample maps to a Ray Counter;
        everything else (gauges, untyped, histogram/summary sub-series) is
        mirrored as a Gauge."""
        if family.type == "counter" and sample_name in (
            family.name,
            family.name + "_total",
        ):
            return MetricKind.COUNTER
        return MetricKind.GAUGE

    def _write(self, family, sample) -> None:
        if self._kind_for(family, sample.name) is MetricKind.COUNTER:
            handle = self._reg.counter(sample.name)
            series_key = (sample.name, tuple(sorted(sample.labels.items())))
            prev = self._prev.get(series_key)
            # Cumulative -> delta. If the counter went backwards the source
            # was reset, so the new reading IS the increment since zero;
            # dropping it (like a naive `delta > 0` check) would lose data.
            if prev is None or sample.value < prev:
                delta = sample.value
            else:
                delta = sample.value - prev
            self._prev[series_key] = sample.value
            handle.record(delta, sample.labels)  # no-ops on delta == 0
        else:
            self._reg.gauge(sample.name).record(sample.value, sample.labels)


__all__ = [
    "HTTPSource",
    "PrometheusCollector",
    "Source",
]
