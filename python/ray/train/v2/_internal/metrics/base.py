from dataclasses import dataclass
from enum import Enum
from typing import Dict, Generic, Tuple, Type, TypeVar

from ray.util.metrics import Gauge

RUN_NAME_TAG_KEY = "ray_train_run_name"

T = TypeVar("T")
E = TypeVar("E", bound=Enum)


@dataclass
class Metric:
    name: str
    type: Type[T]
    default: T
    description: str
    tag_keys: Tuple[str, ...]
    base_tags: Dict[str, str]

    def __init__(
        self,
        name: str,
        type: Type[T],
        default: T,
        description: str,
        tag_keys: Tuple[str, ...],
        base_tags: Dict[str, str],
    ):
        self.name = name
        self.type = type
        self.default = default
        self.description = description
        self.tag_keys = tag_keys
        self.base_tags = base_tags
        self._gauge = None
        self._values = {}
        self._started = False

    def start(self):
        """Initialize the gauge for this metric."""
        self._gauge = Gauge(
            self.name,
            description=self.description,
            tag_keys=self.tag_keys,
        )
        self._started = True

    def _assert_started(self):
        """Assert that the metric has been started.

        Raises:
            RuntimeError: If the metric has not been started.
        """
        if not self._started:
            raise RuntimeError(
                f"Metric '{self.name}' has not been started. "
                f"Call start() before using this method."
            )

    def record(self, value: T):
        """Update the metric value.

        For numerical values (int, float), the value will be added to the current value.
        For non-numerical values, the current value will be replaced.

        Args:
            value: The value to update the metric with.
        """
        self._assert_started()
        self._validate_tags(self.base_tags)
        key = frozenset(self.base_tags.items())

        # Initialize with default value for new tag combination
        if key not in self._values:
            self._values[key] = self.default

        # For numeric types, add the value; otherwise replace it
        if isinstance(self._values[key], (int, float)):
            self._values[key] += value
        else:
            self._values[key] = value

        self._gauge.set(self._values[key], self.base_tags)

    def get_value(self) -> T:
        """Get the value of the metric."""
        self._assert_started()
        self._validate_tags(self.base_tags)
        key = frozenset(self.base_tags.items())
        return self._values.get(key, self.default)

    def reset(self):
        """Reset all values to default for all tag combinations."""
        self._assert_started()
        for key, _ in self._values.items():
            tags = dict(key)
            self._values[key] = self.default
            self._gauge.set(self.default, tags)

    def shutdown(self):
        """Reset values and clean up resources."""
        if self._started:
            self.reset()
            self._values = {}
            self._started = False

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


class TimeMetric(Metric):
    """A metric for tracking elapsed time."""

    def __init__(
        self,
        name: str,
        description: str,
        tag_keys: Tuple[str, ...],
        base_tags: Dict[str, str],
    ):
        super().__init__(
            name=name,
            type=float,
            default=0.0,
            description=description,
            tag_keys=tag_keys,
            base_tags=base_tags,
        )


class EnumMetric(Metric, Generic[E]):
    """A metric for tracking enum values."""

    def __init__(
        self,
        name: str,
        description: str,
        tag_keys: Tuple[str, ...],
        base_tags: Dict[str, str],
        enum_type: Type[E],
        enum_tag_key: str,
    ):
        super().__init__(
            name=name,
            type=int,
            default=0,
            description=description,
            tag_keys=tag_keys,
            base_tags=base_tags,
        )
        self.enum_type = enum_type
        self.enum_tag_key = enum_tag_key

    def record(self, enum_value: E, value: int = 1):
        """Record a value for a specific enum value.

        Args:
            enum_value: The enum value to record for
            value: The value to record (default: 1)
        """
        self._assert_started()
        tags = dict(self.base_tags)
        tags[self.enum_tag_key] = enum_value.name
        self._validate_tags(tags)
        key = frozenset(tags.items())

        # Initialize with default value for new tag combination
        if key not in self._values:
            self._values[key] = self.default

        # Add the value
        self._values[key] += value

        # Update the gauge if it exists
        self._gauge.set(self._values[key], tags)

    def get_value(self, enum_value: E) -> int:
        """Get the value for a specific enum value.

        Args:
            enum_value: The enum value to get the value for

        Returns:
            The value for the enum value
        """
        self._assert_started()
        tags = dict(self.base_tags)
        tags[self.enum_tag_key] = enum_value.name
        self._validate_tags(tags)
        key = frozenset(tags.items())
        return self._values.get(key, self.default)
