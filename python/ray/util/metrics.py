from typing import Dict, Any, List, Optional

from ray._raylet import (
    Count as CythonCount,
    Histogram as CythonHistogram,
    Gauge as CythonGauge,
)  # noqa: E402


class Metric:
    """The parent class of custom metrics.

    Ray's custom metrics APIs are rooted from this class and sharing
    the same public methods.

    Recommended metric name pattern : ray.{component_name}.{module_name}, and
    name format must be in [0-9a-zA-Z].
    """

    def __init__(self,
                 name: str,
                 description: str = "",
                 tags: Dict[str, Optional[str]] = None):
        self._name = name
        self._description = description
        self._unit = ""
        # The default tags key-value pair.
        self._tags = tags if tags else {}
        # Keys of tags.
        self._tag_keys = self._tags.keys()
        # The Cython metric class. This should be set in the child class.
        self._metric = None
        # This field is used to temporarily store tags for record method.
        self._tag_cache = {}

    def with_tags(self, tags: Dict[str, Optional[str]]):
        """Chain method to specify tags.

        Example:
            >>> counter = Counter("name", tags={"key1": None})
                # This will override original key1 to "1".
                counter.with_tags({"key1": "1"}).record(3)

        Chaining multiple with_tags method is not allowed.

        Args:
            tags(dict): Dictionary of tags that overlap the default.
        """
        if len(self._tag_cache) > 0:
            raise ValueError(
                "Chaining multile with_tags method is not allowed. "
                "Please use record method after with_tags method. "
                "For example, Metrics.with_tags({'a': 1}).record(value)")
        self._tag_cache = tags
        return self

    def record(self, value: float,
               tags: Dict[str, Optional[str]] = None) -> None:
        """Record the metric point of the metric.

        Args:
            value(float): The value to be recorded as a metric point.
        """
        if tags is not None:
            self.with_tags(tags).record(value)
            return

        assert self._metric is not None
        default_tag_copy = self._tags.copy()
        tags = default_tag_copy.update(self._tag_cache)
        self._metric.record(value, tags=tags)
        # We should reset tag cache. Otherwise, with_tags will be corrupted.
        self._tag_cache = {}

    @property
    def info(self) -> Dict[str, Any]:
        """Return the information of this metric.

        Example:
            >>> counter = Counter("name", description="desc")
                print(counter.info)
                \"""
                {
                    "name": "name",
                    "description": "desc"
                    "tags": {}
                }
                \"""
        """
        return {
            "name": self._name,
            "description": self._description,
            "tags": self._tags
        }


class Count(Metric):
    """The count of the number of metric points.

    This is corresponding to Prometheus' Count metric.

    Args:
        name(str): Name of the metric.
        description(str): Description of the metric.
        tags(dict): Dictionary of tags of this metric.
            {"key": "value"}, Key is the tag key of the metric and
            Value is the default value of the tag. None indicates
            that there's no default value.
    """

    def __init__(self,
                 name: str,
                 description: str = "",
                 tags: Dict[str, Optional[str]] = None):
        super().__init__(name, description, tags)
        self._metric = CythonCount(self._name, self._description, self._unit,
                                   self._tag_keys)


class Histogram(Metric):
    """Histogram distribution of metric points.

    This is corresponding to Prometheus' Histogram metric.
    Recording metrics with histogram will enable you to import
    min, mean, max, 25, 50, 95, 99 percentile latency.

    Args:
        name(str): Name of the metric.
        description(str): Description of the metric.
        boundaries(list): Boundaries of histogram buckets.
        tags(dict): Dictionary of tags of this metric.
            {"key": "value"}, Key is the tag key of the metric and
            Value is the default value of the tag. None indicates
            that there's no default value.
    """

    def __init__(self,
                 name: str,
                 description: str = "",
                 boundaries: List[float] = None,
                 tags: Dict[str, Optional[str]] = None):
        super().__init__(name, description, tags)
        self.boundaries = boundaries if boundaries else []
        self._metric = CythonHistogram(self._name, self._description,
                                       self._unit, self.boundaries,
                                       self._tag_keys)

    @property
    def info(self):
        info = super().info
        info.update({"boundaries": self.boundaries})
        return info


class Gauge(Metric):
    """Gauge Keeps the last recorded value, drops everything before.

    This is corresponding to Prometheus' Gauge metric.

    Args:
        name(str): Name of the metric.
        description(str): Description of the metric.
        tags(dict): Dictionary of tags of this metric.
            {"key": "value"}, Key is the tag key of the metric and
            Value is the default value of the tag. None indicates
            that there's no default value.
    """

    def __init__(self,
                 name: str,
                 description: str = "",
                 tags: Dict[str, Optional[str]] = None):
        super().__init__(name, description, tags)
        self._metric = CythonGauge(self._name, self._description, self._unit,
                                   self._tag_keys)


__all__ = [
    "Count",
    "Histogram",
    "Gauge",
]
