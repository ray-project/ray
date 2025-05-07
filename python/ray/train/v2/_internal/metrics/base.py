from dataclasses import dataclass
from typing import Any, Dict, List, Type, TypeVar

RUN_NAME_TAG_KEY = "ray_train_run_name"

T = TypeVar("T")


@dataclass
class Metric:
    name: str
    type: Type[T]
    default: T
    description: str
    tag_keys: List[str]

    def __init__(
        self,
        name: str,
        type: Type[T],
        default: T,
        description: str,
        tag_keys: List[str],
    ):
        self.name = name
        self.type = type
        self.default = default
        self.description = description
        self.tag_keys = tag_keys
        self._gauge = None
        self._values = {}

    def _validate_tags(self, tags: Dict[str, str]):
        """Validate that the provided tags match the expected tag keys.

        Args:
            tags: Dictionary of tag key-value pairs to validate

        Raises:
            ValueError: If the tag keys don't match the expected keys
        """
        if set(tags.keys()) != set(self.tag_keys):
            raise ValueError(
                f"Tag keys for metric '{self.name}' don't match expected keys. "
                f"Expected: {self.tag_keys}, got: {list(tags.keys())}"
            )

    def record(self, tags: Dict[str, str], value: T):
        """Update the metric value with the given tags.

        For numerical values (int, float), the value will be added to the current value.
        For non-numerical values, the current value will be replaced.

        Args:
            value: The value to update the metric with.
            tags: Dictionary of tag key-value pairs.
        """
        self._validate_tags(tags)

        key = frozenset(tags.items())

        # Initialize with default value for new tag combination
        if key not in self._values:
            self._values[key] = self.default

        # For numeric types, add the value; otherwise replace it
        if isinstance(self._values[key], (int, float)):
            self._values[key] += value
        else:
            self._values[key] = value

        # Update the gauge if it exists
        if self._gauge is not None:
            self._gauge.set(self._values[key], tags)

    def get_value(self, tags: Dict[str, str]) -> Any:
        """Get the value of the metric for the given tags."""
        self._validate_tags(tags)

        key = frozenset(tags.items())
        return self._values.get(key, self.default)

    def reset(self):
        """Reset all values to default for all tag combinations."""
        for key, _ in self._values.items():
            tags = dict(key)
            self._values[key] = self.default
            if self._gauge is not None:
                self._gauge.set(self.default, tags)

    def shutdown(self):
        """Reset values and clean up resources."""
        self.reset()
        self._values = {}


class MetricsTracker:
    """Tracks metric values for a set of defined metrics."""

    def __init__(self, metrics: Dict[str, Metric], base_tags: Dict[str, str]):
        self._metrics = metrics
        self._base_tags = base_tags

    def start(self):
        """Start all metrics."""
        for metric in self._metrics.values():
            metric.start()

    def shutdown(self):
        """Shutdown all metrics."""
        for metric in self._metrics.values():
            metric.shutdown()

    def reset(self):
        """Reset all metrics."""
        for metric in self._metrics.values():
            metric.reset()

    def record(
        self, metric_name: str, value: Any, additional_tags: Dict[str, str] = None
    ):
        """Record a value for a specific metric."""
        if additional_tags is None:
            additional_tags = {}
        metric = self._get_metric(metric_name)
        tags = self._base_tags | additional_tags
        metric.record(tags, value)

    def get_value(self, metric_name: str, additional_tags: Dict[str, str] = None):
        """Get the value of a specific metric."""
        if additional_tags is None:
            additional_tags = {}
        metric = self._get_metric(metric_name)
        tags = self._base_tags | additional_tags
        return metric.get_value(tags)

    def _get_metric(self, metric_name: str) -> Metric:
        """Get a specific metric."""
        if metric_name not in self._metrics:
            raise ValueError(f"Unknown metric: {metric_name}")
        return self._metrics[metric_name]
