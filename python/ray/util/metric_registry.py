"""Idempotent, thread-safe registry over :mod:`ray.util.metrics` primitives.

`MetricRegistry` is the single place that maps "a named Prometheus-style
metric with dynamic tags" onto Ray's fixed-``tag_keys`` primitives
(:class:`~ray.util.metrics.Counter`, :class:`~ray.util.metrics.Gauge`,
:class:`~ray.util.metrics.Histogram`). It owns the quirks every caller
otherwise re-solves by hand:

- **Dedup by name.** ``registry.counter("x")`` twice returns a handle to the
  same underlying Ray metric, so callers don't manage their own caches.
- **Name sanitizing.** ``:`` is illegal in a Ray metric name and is replaced
  with ``_``; an optional registry-wide namespace prefix is applied once.
- **The Counter ``_total`` quirk.** Ray's ``Counter`` re-appends ``_total``
  on export, so a trailing ``_total`` is folded out of both the registry key
  and the constructed name: ``counter("x_total")`` and ``counter("x")`` dedup
  to the same handle and the exported name comes out unchanged.
- **Tag-key superset, declared once.** Ray fixes ``tag_keys`` at metric
  construction and validates strictly at record time. Handles pad
  declared-but-missing keys with ``""`` and warn once (then drop) when a
  never-before-seen key shows up after creation -- Ray cannot widen
  ``tag_keys``, so this is a documented limitation rather than a crash.

Example:
    >>> from ray.util.metric_registry import MetricRegistry
    >>> reg = MetricRegistry("myapp")
    >>> reg.gauge("queue_depth", tag_keys=("shard",)).set(12, {"shard": "0"})
    >>> with reg.histogram("step_seconds", buckets=[0.1, 1, 10]).timer():
    ...     do_work()  # doctest: +SKIP
"""

import enum
import logging
import threading
import time
from typing import Dict, Iterable, List, Optional, Union

from ray.util.annotations import DeveloperAPI
from ray.util.metrics import Counter, Gauge, Histogram, Metric

logger = logging.getLogger(__name__)


@DeveloperAPI
class MetricKind(enum.Enum):
    """The three metric kinds supported by :mod:`ray.util.metrics`."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


class MetricHandle:
    """A recorded-through wrapper around one Ray metric.

    Handles are created by :class:`MetricRegistry` (never directly) and add
    tag normalization on top of the underlying metric: unknown tag keys are
    warned about once and dropped, and declared-but-missing keys are padded
    with ``""`` (unless covered by a default tag) so Ray's strict record-time
    validation never raises.
    """

    def __init__(self, metric: Metric, kind: MetricKind, tag_keys: tuple):
        self._metric = metric
        self._kind = kind
        self._tag_keys = tag_keys
        # Set mirror of _tag_keys for O(1) membership checks on the
        # per-record tag-normalization path.
        self._tag_keys_set = frozenset(tag_keys)
        self._default_tags: Dict[str, str] = {}
        # Tag keys we've already warned about, so post-creation surprises are
        # logged once per (metric, key) rather than once per record call.
        self._warned_keys = set()

    @property
    def kind(self) -> MetricKind:
        return self._kind

    @property
    def tag_keys(self) -> tuple:
        return self._tag_keys

    @property
    def info(self) -> dict:
        """Proxy of the underlying :attr:`Metric.info`."""
        return self._metric.info

    def set_default_tags(self, default_tags: Dict[str, str]) -> "MetricHandle":
        """Set default tags on the underlying metric. Returns ``self``."""
        self._metric.set_default_tags(default_tags)
        self._default_tags = default_tags
        return self

    def record(self, value: Union[int, float], tags: Optional[Dict[str, str]] = None):
        """Kind-agnostic record: counter ``inc``, gauge ``set``, histogram
        ``observe``.

        Unlike the kind-native methods, a counter ``record`` silently no-ops
        on ``value <= 0`` (Ray's ``Counter.inc`` rejects non-positive values;
        a zero delta from a mirrored cumulative counter is normal, not an
        error).
        """
        tags = self._normalize_tags(tags)
        if self._kind is MetricKind.COUNTER:
            if value > 0:
                self._metric.inc(value, tags)
        elif self._kind is MetricKind.GAUGE:
            self._metric.set(value, tags)
        else:
            self._metric.observe(value, tags)

    def _note_tag_keys(self, tag_keys: Iterable[str]) -> None:
        """Warn once per key that arrived after the metric was created."""
        for key in tag_keys:
            if key not in self._tag_keys_set and key not in self._warned_keys:
                self._warned_keys.add(key)
                logger.warning(
                    "Tag key %r appeared on metric %r after it was created "
                    "with tag_keys=%s. Ray cannot widen a metric's tag keys, "
                    "so this dimension will be dropped.",
                    key,
                    self._metric.info["name"],
                    self._tag_keys,
                )

    def _normalize_tags(self, tags: Optional[Dict[str, str]]) -> Dict[str, str]:
        tags = dict(tags) if tags else {}
        unknown = [k for k in tags if k not in self._tag_keys_set]
        if unknown:
            self._note_tag_keys(unknown)
            for key in unknown:
                del tags[key]
        # Pad declared keys that neither the call nor the default tags cover,
        # so Ray's strict missing-tag validation never raises. Keys covered by
        # default tags are left absent (padding "" would override the default).
        for key in self._tag_keys:
            if key not in tags and key not in self._default_tags:
                tags[key] = ""
        return tags


@DeveloperAPI
class CounterHandle(MetricHandle):
    """Handle to a registry-managed :class:`~ray.util.metrics.Counter`."""

    def inc(self, value: Union[int, float] = 1.0, tags: Dict[str, str] = None):
        """Increment by ``value``. Keeps Ray semantics: raises on ``<= 0``."""
        self._metric.inc(value, self._normalize_tags(tags))


@DeveloperAPI
class GaugeHandle(MetricHandle):
    """Handle to a registry-managed :class:`~ray.util.metrics.Gauge`."""

    def set(self, value: Optional[Union[int, float]], tags: Dict[str, str] = None):
        if value is None:
            # Ray's Gauge.set is a no-op on None; return before tag
            # normalization so a no-op call has no cost and no warning
            # side effects.
            return
        self._metric.set(value, self._normalize_tags(tags))


@DeveloperAPI
class HistogramHandle(MetricHandle):
    """Handle to a registry-managed :class:`~ray.util.metrics.Histogram`."""

    def observe(self, value: Union[int, float], tags: Dict[str, str] = None):
        self._metric.observe(value, self._normalize_tags(tags))

    def timer(self, tags: Dict[str, str] = None) -> "_Timer":
        """Context manager observing the elapsed wall-clock seconds."""
        return _Timer(self, tags)


class _Timer:
    def __init__(self, handle: HistogramHandle, tags: Optional[Dict[str, str]]):
        self._handle = handle
        self._tags = tags
        self._start = None

    def __enter__(self) -> "_Timer":
        self._start = time.monotonic()
        return self

    def __exit__(self, *exc_info) -> None:
        self._handle.observe(time.monotonic() - self._start, self._tags)


_HANDLE_CLASSES = {
    MetricKind.COUNTER: CounterHandle,
    MetricKind.GAUGE: GaugeHandle,
    MetricKind.HISTOGRAM: HistogramHandle,
}


@DeveloperAPI
class MetricRegistry:
    """Idempotent, thread-safe factory over :mod:`ray.util.metrics` primitives.

    One registry ~= one logical namespace. Calling :meth:`counter`,
    :meth:`gauge`, :meth:`histogram`, or :meth:`declare` with the same name
    returns a handle to the same underlying Ray metric.

    Args:
        namespace: Optional prefix applied to every metric name, joined with
            ``_`` (e.g. namespace ``"sglang"`` + name ``"num_requests"`` ->
            ``sglang_num_requests``). Leave empty for sources that already
            namespace their metrics.
    """

    def __init__(self, namespace: str = ""):
        self._namespace = namespace
        self._lock = threading.Lock()
        self._metrics: Dict[str, MetricHandle] = {}

    def counter(
        self,
        name: str,
        description: str = "",
        tag_keys: Iterable[str] = (),
    ) -> CounterHandle:
        """Get or create a Counter. The exported name gains a ``_total``
        suffix (a trailing ``_total`` on ``name`` is folded in, not doubled).
        """
        return self.declare(name, MetricKind.COUNTER, tag_keys, description)

    def gauge(
        self,
        name: str,
        description: str = "",
        tag_keys: Iterable[str] = (),
    ) -> GaugeHandle:
        """Get or create a Gauge."""
        return self.declare(name, MetricKind.GAUGE, tag_keys, description)

    def histogram(
        self,
        name: str,
        buckets: List[float],
        description: str = "",
        tag_keys: Iterable[str] = (),
    ) -> HistogramHandle:
        """Get or create a Histogram with the given bucket boundaries.

        ``buckets`` binds only on first creation; a later get with different
        buckets returns the existing metric unchanged.
        """
        return self.declare(
            name, MetricKind.HISTOGRAM, tag_keys, description, buckets=buckets
        )

    def declare(
        self,
        name: str,
        kind: Union[MetricKind, str],
        tag_keys: Iterable[str],
        description: str = "",
        buckets: Optional[List[float]] = None,
    ) -> MetricHandle:
        """Get or create a metric of ``kind`` with the full tag-key superset.

        This is the discovery-phase entry point: pass the union of all label
        keys the metric will ever carry, since Ray cannot widen ``tag_keys``
        after creation. ``description`` and ``buckets`` bind only on first
        creation.

        Args:
            name: Metric name (sanitized and namespaced by the registry).
            kind: A :class:`MetricKind` or its string value
                (``"counter"``/``"gauge"``/``"histogram"``).
            tag_keys: Union of all tag keys the metric will ever carry.
            description: Metric description; binds on first creation only.
            buckets: Histogram bucket boundaries; required for histograms,
                ignored otherwise. Binds on first creation only.

        Returns:
            The (possibly pre-existing) handle for the metric.

        Raises:
            ValueError: if the name already exists with a different kind, or
                a histogram is declared without ``buckets``.
        """
        kind = MetricKind(kind)
        sanitized = self._sanitize(name)
        if kind is MetricKind.COUNTER and sanitized.endswith("_total"):
            # Fold the `_total` before the registry lookup, not just before
            # construction: counter("x_total") and counter("x") must dedup to
            # the same handle (Ray's Counter re-appends `_total` on export
            # either way, so keying them separately would create two Ray
            # metrics exporting the same series).
            sanitized = sanitized[: -len("_total")]
        keys = tuple(sorted(set(tag_keys)))
        with self._lock:
            handle = self._metrics.get(sanitized)
            if handle is not None:
                if handle.kind is not kind:
                    raise ValueError(
                        f"Metric {sanitized!r} already declared as "
                        f"{handle.kind.value}, cannot redeclare as {kind.value}."
                    )
                handle._note_tag_keys(keys)
                return handle
            handle = self._create(sanitized, kind, description, keys, buckets)
            self._metrics[sanitized] = handle
            return handle

    @staticmethod
    def _create(
        name: str,
        kind: MetricKind,
        description: str,
        tag_keys: tuple,
        buckets: Optional[List[float]],
    ) -> MetricHandle:
        if kind is MetricKind.COUNTER:
            # `name` already has any trailing `_total` folded away by
            # declare(); Ray's Counter re-appends it on export.
            metric = Counter(name, description, tag_keys)
        elif kind is MetricKind.HISTOGRAM:
            if not buckets:
                raise ValueError(f"Histogram {name!r} requires non-empty `buckets`.")
            metric = Histogram(name, description, buckets, tag_keys)
        else:
            metric = Gauge(name, description, tag_keys)
        return _HANDLE_CLASSES[kind](metric, kind, tag_keys)

    def _sanitize(self, name: str) -> str:
        full = f"{self._namespace}_{name}" if self._namespace else name
        # ':' is legal in Prometheus (recording-rule convention) but illegal
        # in a Ray metric name.
        return full.replace(":", "_")


__all__ = [
    "MetricKind",
    "MetricRegistry",
    "CounterHandle",
    "GaugeHandle",
    "HistogramHandle",
]
