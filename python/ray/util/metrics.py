import logging

from typing import Dict, Any, List, Optional, Tuple

from ray._raylet import (
    Count as CythonCount,
    Histogram as CythonHistogram,
    Gauge as CythonGauge,
)  # noqa: E402

logger = logging.getLogger(__name__)


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
                 tag_keys: Tuple[str] = None,
                 default_tags: Dict[str, Optional[str]] = None):
        self._name = name
        self._description = description
        self._unit = ""
        # The default tags key-value pair.
        self._default_tags = default_tags if default_tags else {}
        # Keys of tags.
        self._tag_keys = tag_keys if tag_keys else tuple()
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
            logger.warning(
                "Chaining multiple with_tags method is not recommended. EX) "
                "metrics.with_chain(tags1).with_chains(tags2). The first "
                f"tag {self._tag_cache} will be overwritten by the second "
                f"chained method. {tags}")
            self._tag_cache = {}

        self._tag_cache = tags
        return self

    def record(self, value: float) -> None:
        """Record the metric point of the metric.

        Args:
            value(float): The value to be recorded as a metric point.
        """
        assert self._metric is not None
        default_tag_copy = self._default_tags.copy()
        default_tag_copy.update(self._tag_cache)
        self._metric.record(value, tags=default_tag_copy)
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
                    "tag_keys": ("ray.key")
                    "default_tags": {"ray.key": "abc"}
                }
                \"""
        """
        return {
            "name": self._name,
            "description": self._description,
            "tag_keys": self._tag_keys,
            "default_tags": self._default_tags
        }


class Count(Metric):
    """The count of the number of metric points.

    This is corresponding to Prometheus' Count metric.

    Args:
        name(str): Name of the metric.
        description(str): Description of the metric.
        tag_keys(tuple): Tag keys of the metric.
        tags(dict): Dictionary of default tag values.
    """

    def __init__(self,
                 name: str,
                 description: str = "",
                 tag_keys: Tuple[str] = None,
                 default_tags: Dict[str, Optional[str]] = None):
        super().__init__(name, description, tag_keys, default_tags)
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
        tag_keys(tuple): Tag keys of the metric.
        tags(dict): Dictionary of default tag values.
    """

    def __init__(self,
                 name: str,
                 description: str = "",
                 boundaries: List[float] = None,
                 tag_keys: Tuple[str] = None,
                 default_tags: Dict[str, Optional[str]] = None):
        super().__init__(name, description, tag_keys, default_tags)
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
        tag_keys(tuple): Tag keys of the metric.
        tags(dict): Dictionary of default tag values.
    """

    def __init__(self,
                 name: str,
                 description: str = "",
                 tag_keys: Tuple[str] = None,
                 default_tags: Dict[str, Optional[str]] = None):
        super().__init__(name, description, tag_keys, default_tags)
        self._metric = CythonGauge(self._name, self._description, self._unit,
                                   self._tag_keys)


__all__ = [
    "Count",
    "Histogram",
    "Gauge",
]
