import re
import logging

from typing import Dict, Any, List, Optional, Tuple, Union

from ray._raylet import (
    Sum as CythonCount,
    Histogram as CythonHistogram,
    Gauge as CythonGauge,
)  # noqa: E402

# Sum is used for CythonCount because it allows incrementing by positive
# values that are different from one.
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
"""
    Validation logic is copied from prometheus_client.metrics_core
    https://github.com/prometheus/client_python/blob/2dcd17efd0ce2f0a1ad15cb3c150ffcdc42ced65/prometheus_client/metrics_core.py#L11
"""


def _validate_metric_name(name: str):

    if len(name) == 0:
        raise ValueError("Empty name is not allowed. Please provide a metric name.")

    if not METRIC_NAME_RE.match(name):
        raise ValueError(
            f"Metric name {name} is invalid. "
            "Please use metric names that match the regex "
            f"{METRIC_NAME_RE.pattern}"
        )


@DeveloperAPI
class Metric:
    """The parent class of custom metrics.

    Ray's custom metrics APIs are rooted from this class and share
    the same public methods.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        tag_keys: Optional[Tuple[str, ...]] = None,
    ):
        _validate_metric_name(name)

        self._name = name
        self._description = description
        # The default tags key-value pair.
        self._default_tags = {}
        # Keys of tags.
        self._tag_keys = tag_keys or tuple()
        # The Cython metric class. This should be set in the child class.
        self._metric = None

        if not isinstance(self._tag_keys, tuple):
            raise TypeError(
                "tag_keys should be a tuple type, got: " f"{type(self._tag_keys)}"
            )

        for key in self._tag_keys:
            if not isinstance(key, str):
                raise TypeError(f"Tag keys must be str, got {type(key)}.")

    def set_default_tags(self, default_tags: Dict[str, str]):
        """Set default tags of metrics.

        Example:
            >>> from ray.util.metrics import Counter
            >>> # Note that set_default_tags returns the instance itself.
            >>> counter = Counter("name", tag_keys=("a",))
            >>> counter2 = counter.set_default_tags({"a": "b"})
            >>> assert counter is counter2
            >>> # this means you can instantiate it in this way.
            >>> counter = Counter("name", tag_keys=("a",)).set_default_tags({"a": "b"})

        Args:
            default_tags: Default tags that are
                used for every record method.

        Returns:
            Metric: it returns the instance itself.
        """
        for key, val in default_tags.items():
            if key not in self._tag_keys:
                raise ValueError(f"Unrecognized tag key {key}.")
            if not isinstance(val, str):
                raise TypeError(f"Tag values must be str, got {type(val)}.")

        self._default_tags = default_tags
        return self

    def record(
        self,
        value: Union[int, float],
        tags: Optional[Dict[str, str]] = None,
        _internal=False,
    ) -> None:
        """Record the metric point of the metric.

        Tags passed in will take precedence over the metric's default tags.

        Args:
            value: The value to be recorded as a metric point.
        """
        assert self._metric is not None
        if isinstance(self._metric, CythonCount) and not _internal:
            logger.warning(
                "Counter.record() is deprecated in favor of "
                "Counter.inc() and will be removed in a future "
                "release. Please use Counter.inc() instead."
            )

        if isinstance(self._metric, CythonGauge) and not _internal:
            logger.warning(
                "Gauge.record() is deprecated in favor of "
                "Gauge.set() and will be removed in a future "
                "release. Please use Gauge.set() instead."
            )

        if isinstance(self._metric, CythonHistogram) and not _internal:
            logger.warning(
                "Histogram.record() is deprecated in favor of "
                "Histogram.observe() and will be removed in a "
                "future release. Please use Histogram.observe() "
                "instead."
            )

        final_tags = self._get_final_tags(tags)
        self._validate_tags(final_tags)
        self._metric.record(value, tags=final_tags)

    def _get_final_tags(self, tags):
        if not tags:
            return self._default_tags

        for val in tags.values():
            if not isinstance(val, str):
                raise TypeError(f"Tag values must be str, got {type(val)}.")

        return {**self._default_tags, **tags}

    def _validate_tags(self, final_tags):
        missing_tags = []
        for tag_key in self._tag_keys:
            # Prefer passed tags over default tags.
            if tag_key not in final_tags:
                missing_tags.append(tag_key)

        if missing_tags:
            raise ValueError(f"Missing value for tag key(s): {','.join(missing_tags)}.")

    @property
    def info(self) -> Dict[str, Any]:
        """Return the information of this metric.

        Example:
            >>> from ray.util.metrics import Counter
            >>> counter = Counter("name", description="desc")
            >>> print(counter.info)
            {'name': 'name', 'description': 'desc', 'tag_keys': (), 'default_tags': {}}
        """
        return {
            "name": self._name,
            "description": self._description,
            "tag_keys": self._tag_keys,
            "default_tags": self._default_tags,
        }


@DeveloperAPI
class Counter(Metric):
    """A cumulative metric that is monotonically increasing.

    This corresponds to Prometheus' counter metric:
    https://prometheus.io/docs/concepts/metric_types/#counter

    Args:
        name: Name of the metric.
        description: Description of the metric.
        tag_keys: Tag keys of the metric.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        tag_keys: Optional[Tuple[str, ...]] = None,
    ):
        super().__init__(name, description, tag_keys)
        self._metric = CythonCount(self._name, self._description, self._tag_keys)

    def __reduce__(self):
        deserializer = self.__class__
        serialized_data = (self._name, self._description, self._tag_keys)
        return deserializer, serialized_data

    def inc(self, value: Union[int, float] = 1.0, tags: Dict[str, str] = None):
        """Increment the counter by `value` (defaults to 1).

        Tags passed in will take precedence over the metric's default tags.

        Args:
            value(int, float): Value to increment the counter by (default=1).
            tags(Dict[str, str]): Tags to set or override for this counter.
        """
        if not isinstance(value, (int, float)):
            raise TypeError(f"value must be int or float, got {type(value)}.")
        if value <= 0:
            raise ValueError(f"value must be >0, got {value}")

        self.record(value, tags=tags, _internal=True)


@DeveloperAPI
class Count(Counter):
    """The count of the number of metric points.

    This corresponds to Prometheus' 'Count' metric.

    This class is DEPRECATED, please use ray.util.metrics.Counter instead.

    Args:
        name: Name of the metric.
        description: Description of the metric.
        tag_keys: Tag keys of the metric.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        tag_keys: Optional[Tuple[str, ...]] = None,
    ):
        logger.warning(
            "`metrics.Count` has been renamed to `metrics.Counter`. "
            "`metrics.Count` will be removed in a future release."
        )
        super().__init__(name, description, tag_keys)


@DeveloperAPI
class Histogram(Metric):
    """Tracks the size and number of events in buckets.

    Histograms allow you to calculate aggregate quantiles
    such as 25, 50, 95, 99 percentile latency for an RPC.

    This corresponds to Prometheus' histogram metric:
    https://prometheus.io/docs/concepts/metric_types/#histogram

    Args:
        name: Name of the metric.
        description: Description of the metric.
        boundaries: Boundaries of histogram buckets.
        tag_keys: Tag keys of the metric.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        boundaries: List[float] = None,
        tag_keys: Optional[Tuple[str, ...]] = None,
    ):
        super().__init__(name, description, tag_keys)
        if boundaries is None or len(boundaries) == 0:
            raise ValueError(
                "boundaries argument should be provided when using "
                "the Histogram class. e.g., "
                'Histogram("name", boundaries=[1.0, 2.0])'
            )
        for i, boundary in enumerate(boundaries):
            if boundary <= 0:
                raise ValueError(
                    "Invalid `boundaries` argument at index "
                    f"{i}, {boundaries}. Use positive values for the arguments."
                )

        self.boundaries = boundaries
        self._metric = CythonHistogram(
            self._name, self._description, self.boundaries, self._tag_keys
        )

    def observe(self, value: Union[int, float], tags: Dict[str, str] = None):
        """Observe a given `value` and add it to the appropriate bucket.

        Tags passed in will take precedence over the metric's default tags.

        Args:
            value(int, float): Value to set the gauge to.
            tags(Dict[str, str]): Tags to set or override for this gauge.
        """
        if not isinstance(value, (int, float)):
            raise TypeError(f"value must be int or float, got {type(value)}.")

        self.record(value, tags, _internal=True)

    def __reduce__(self):
        deserializer = Histogram
        serialized_data = (
            self._name,
            self._description,
            self.boundaries,
            self._tag_keys,
        )
        return deserializer, serialized_data

    @property
    def info(self):
        """Return information about histogram metric."""
        info = super().info
        info.update({"boundaries": self.boundaries})
        return info


@DeveloperAPI
class Gauge(Metric):
    """Gauges keep the last recorded value and drop everything before.

    Unlike counters, gauges can go up or down over time.

    This corresponds to Prometheus' gauge metric:
    https://prometheus.io/docs/concepts/metric_types/#gauge

    Args:
        name: Name of the metric.
        description: Description of the metric.
        tag_keys: Tag keys of the metric.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        tag_keys: Optional[Tuple[str, ...]] = None,
    ):
        super().__init__(name, description, tag_keys)
        self._metric = CythonGauge(self._name, self._description, self._tag_keys)

    def set(self, value: Union[int, float], tags: Dict[str, str] = None):
        """Set the gauge to the given `value`.

        Tags passed in will take precedence over the metric's default tags.

        Args:
            value(int, float): Value to set the gauge to.
            tags(Dict[str, str]): Tags to set or override for this gauge.
        """
        if not isinstance(value, (int, float)):
            raise TypeError(f"value must be int or float, got {type(value)}.")

        self.record(value, tags, _internal=True)

    def __reduce__(self):
        deserializer = Gauge
        serialized_data = (self._name, self._description, self._tag_keys)
        return deserializer, serialized_data


__all__ = [
    "Counter",
    "Histogram",
    "Gauge",
]
