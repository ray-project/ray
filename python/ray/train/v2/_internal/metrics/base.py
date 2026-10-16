from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Generic, Optional, Tuple, TypeVar

from ray.util.metrics import Gauge

RUN_NAME_TAG_KEY = "ray_train_run_name"
RUN_ID_TAG_KEY = "ray_train_run_id"

T = TypeVar("T")
E = TypeVar("E", bound=Enum)


class Metric(ABC):
    def __init__(
        self,
        name: str,
        default: T,
        description: str,
        base_tags: Dict[str, str],
    ):
        """
        Initialize a new metric.

        Args:
            name: The name of the metric.
            default: The default value of the metric.
            description: The description of the metric.
            base_tags: The base tags for the metric.
        """
        self._default = default
        self._base_tags = base_tags
        self._gauge = Gauge(
            name,
            description=description,
            tag_keys=self._get_tag_keys(),
        )

    @abstractmethod
    def record(self, value: T):
        """Update the metric value.

        Args:
            value: The value to update the metric with.
        """
        pass

    @abstractmethod
    def get_value(self) -> T:
        """Get the value of the metric.

        Returns:
            The value of the metric. If the metric has not been recorded,
            the default value is returned.
        """
        pass

    @abstractmethod
    def reset(self):
        """Reset values and clean up resources."""
        pass

    def _get_tag_keys(self) -> Tuple[str, ...]:
        return tuple(self._base_tags.keys())


class TimeMetric(Metric):
    """A metric for tracking elapsed time."""

    def __init__(
        self,
        name: str,
        description: str,
        base_tags: Dict[str, str],
    ):
        self._current_value = 0.0
        super().__init__(
            name=name,
            default=0.0,
            description=description,
            base_tags=base_tags,
        )

    def record(self, value: float):
        """Update the time metric value by accumulating the time.

        Args:
            value: The time value to increment the metric by.
        """
        self._current_value += value
        self._gauge.set(self._current_value, self._base_tags)

    def get_value(self) -> float:
        return self._current_value

    def reset(self):
        self._current_value = self._default
        self._gauge.set(self._default, self._base_tags)


class EnumMetric(Metric, Generic[E]):
    """A metric for tracking the current value of an enum as a numeric code.

    Each enum member is assigned a stable, non-zero integer code, and the gauge is
    set to the code of the current value. This produces a single time series per
    set of base tags, which is the shape Grafana's state timeline panel expects
    (one row per series, with value mappings translating codes back into names).
    A value of 0 means that no enum value is currently recorded.
    """

    DEFAULT_VALUE = 0

    def __init__(
        self,
        name: str,
        description: str,
        base_tags: Dict[str, str],
        enum_codes: Dict[E, int],
    ):
        """
        Initialize a new enum metric.

        Args:
            name: The name of the metric.
            description: The description of the metric.
            base_tags: The base tags for the metric.
            enum_codes: Mapping from each enum value to the code to record for it.
                Codes must be unique and non-zero, since 0 means "no value".
        """
        codes = list(enum_codes.values())
        if self.DEFAULT_VALUE in codes or len(set(codes)) != len(codes):
            raise ValueError(
                f"Enum codes must be unique and non-zero, got {enum_codes}."
            )
        self._enum_codes = enum_codes
        self._current_value: Optional[E] = None
        super().__init__(
            name=name,
            default=self.DEFAULT_VALUE,
            description=description,
            base_tags=base_tags,
        )

    def record(self, enum_value: E) -> None:
        """Record a specific enum value by setting the gauge to its code.

        Args:
            enum_value: The enum value to record.
        """
        self._gauge.set(self._enum_codes[enum_value], self._base_tags)
        self._current_value = enum_value

    def get_value(self) -> Optional[E]:
        """Get the currently recorded enum value, or None if nothing is recorded."""
        return self._current_value

    def reset(self):
        self._current_value = None
        self._gauge.set(self.DEFAULT_VALUE, self._base_tags)
