from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Generic, Literal, Optional, Tuple, TypeVar

from ray.util.metrics import Counter, Gauge

RUN_NAME_TAG_KEY = "ray_train_run_name"
RUN_ID_TAG_KEY = "ray_train_run_id"

EVENT_KIND_TAG_KEY = "kind"
EVENT_SEVERITY_TAG_KEY = "severity"

EventKind = Literal[
    "checkpoint_saved",
    "run_retry",
    "run_failed",
    "train_run_attempt",
]
EventSeverity = Literal["info", "warning", "error"]

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
    """A metric for tracking enum values."""

    DEFAULT_VALUE = 0
    RECORDED_VALUE = 1

    def __init__(
        self,
        name: str,
        description: str,
        base_tags: Dict[str, str],
        enum_tag_key: str,
    ):
        self._enum_tag_key = enum_tag_key
        self._current_value: Optional[E] = None
        super().__init__(
            name=name,
            default=self.DEFAULT_VALUE,
            description=description,
            base_tags=base_tags,
        )

    def record(self, enum_value: E) -> None:
        """Record a specific enum value.

        The metric will be reset to 0 for the previous value and set to 1 for the new value.

        Args:
            enum_value: The enum value to record for.
        """
        if enum_value == self._current_value:
            return

        if self._current_value is not None:
            previous_tags = self._get_tags(self._current_value)
            self._gauge.set(self._default, previous_tags)

        current_tags = self._get_tags(enum_value)
        self._gauge.set(self.RECORDED_VALUE, current_tags)

        self._current_value = enum_value

    def get_value(self, enum_value: E) -> int:
        """Get the value for a specific enum value.

        Args:
            enum_value: The enum value to get the value for

        Returns:
            The value for the enum value
        """
        return int(enum_value == self._current_value)

    def reset(self):
        if self._current_value is not None:
            tags = self._get_tags(self._current_value)
            self._gauge.set(self._default, tags)
        self._current_value = None

    def _get_tag_keys(self) -> Tuple[str, ...]:
        return tuple(self._base_tags.keys()) + (self._enum_tag_key,)

    def _get_tags(self, enum_value: E) -> Dict[str, str]:
        tags = self._base_tags.copy()
        tags[self._enum_tag_key] = enum_value.name
        return tags


class EventMetric(Counter):
    """A counter for emitting point-in-time annotation events.

    Increments a single Counter family per `event(kind, severity)` call. The
    `kind` and `severity` `Literal` types bound cardinality at the producer.
    Used for Grafana annotation queries; see the dashboard `annotations.list`
    entries for how these surface.

    The metric is exported as ``ray_train_annotation_event_total`` (the
    ``ray_`` namespace and ``_total`` suffix are appended by the metrics
    pipeline / Prometheus exporter).
    """

    METRIC_NAME = "train_annotation_event"

    def __init__(self, base_tags: Dict[str, str]):
        self._base_tags = base_tags
        super().__init__(
            self.METRIC_NAME,
            description="Annotation events emitted by Ray Train internals.",
            tag_keys=tuple(base_tags.keys())
            + (EVENT_KIND_TAG_KEY, EVENT_SEVERITY_TAG_KEY),
        )

    @classmethod
    def for_run(cls, run_name: str, run_id: str) -> "EventMetric":
        return cls(
            base_tags={RUN_NAME_TAG_KEY: run_name, RUN_ID_TAG_KEY: run_id},
        )

    def event(self, kind: EventKind, severity: EventSeverity = "info") -> None:
        self.inc(
            1,
            tags={
                **self._base_tags,
                EVENT_KIND_TAG_KEY: kind,
                EVENT_SEVERITY_TAG_KEY: severity,
            },
        )
