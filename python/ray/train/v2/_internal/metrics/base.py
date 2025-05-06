import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, TypeVar

from ray.util.metrics import Gauge

RUN_NAME_TAG_KEY = "ray_train_run_name"

T = TypeVar("T")


@dataclass
class Metric:
    name: str
    type: Type[T]
    default: T
    description: str
    tag_keys: List[str]


class MetricsTracker:
    """Tracks metric values for a set of defined metrics."""

    # Interval for pushing metrics to Prometheus.
    DEFAULT_METRICS_PUSH_INTERVAL_S: float = 5.0

    def __init__(self, metrics_definitions, base_tags: Dict[str, str]):
        self._metrics_definitions = {
            metric.name: metric for metric in metrics_definitions
        }
        self._base_tags = base_tags
        # Use a tuple of (metric_name, frozenset(tags.items())) as the key
        self._values = {}
        self._metrics_lock = threading.RLock()
        self._metrics_gauges = {}
        self._thread: Optional[threading.Thread] = None
        self._thread_stop_event: Optional[threading.Event] = None

    def start(self):
        """Create gauges and start the metrics thread."""
        self._create_gauges()
        self._start_metrics_thread()

    def shutdown(self):
        """Stop the metrics thread and reset gauges."""
        self._stop_metrics_thread()
        self._reset_gauges()

    def update(self, metric: Metric, additional_tags: Dict[str, str], value: Any):
        """Update a metric value with associated tags.

        Args:
            metric: A Metric instance
            additional_tags: Dictionary of tag key-value pairs to be merged with the base tags
            value: The value to update the metric with. The value will be added to the existing value
                for the metric-tags combination, or set if the metric-tags combination does not exist.
        """
        if metric.name not in self._metrics_definitions:
            raise ValueError(f"Unknown metric: {metric.name}")

        with self._metrics_lock:
            # Combine base tags with metric-specific tags
            combined_tags = {**self._base_tags, **additional_tags}
            key = self._get_key(metric.name, combined_tags)
            if key not in self._values:
                # Initialize with default value for new metric-tag combination
                self._values[key] = metric.default

            # For numeric types, add the value; otherwise replace it
            if isinstance(self._values[key], (int, float)):
                self._values[key] += value
            else:
                self._values[key] = value

    def get_value(self, metric: Metric, additional_tags: Dict[str, str]) -> Any:
        """Get the value of a metric for a given metric name and tags."""
        with self._metrics_lock:
            combined_tags = {**self._base_tags, **additional_tags}
            key = self._get_key(metric.name, combined_tags)
            return self._values.get(key, None)

    def _get_key(self, metric_name, tags):
        """Get the composite key for a metric and its tags."""
        return (metric_name, frozenset(tags.items()))

    def _create_gauges(self):
        """Create Prometheus gauges for all metrics."""
        with self._metrics_lock:
            self._metrics_gauges = {}
            for metric_name in self._metrics_definitions.keys():
                metric_def = self._metrics_definitions[metric_name]
                self._metrics_gauges[metric_name] = Gauge(
                    metric_name,
                    description=metric_def.description,
                    tag_keys=metric_def.tag_keys,
                )

    def _get_all(self):
        """Get all metric values and their associated tags.

        Returns:
            Dict mapping metric names to tuples of (tags, value).

            Example:
            {
                "metric_name": [
                    ({"tag1": "value1", "tag2": "value2"}, 1.0),
                    ({"tag1": "value1", "tag3": "value3"}, 2.0),
                ],
                "another_metric": [
                    ({"tag1": "value1"}, 5),
                    ({"tag2": "value2", "run": "experiment1"}, 10),
                ]
            }
        """
        with self._metrics_lock:
            result = {}
            for (metric_name, tag_items), value in self._values.items():
                tags = dict(tag_items)
                if metric_name not in result:
                    result[metric_name] = []
                result[metric_name].append((tags, value))
            return result

    def _push_metrics(self):
        """Push all metrics to their gauges."""
        with self._metrics_lock:
            metrics_dict = self._get_all()
            for metric_name, metric_values in metrics_dict.items():
                if metric_name in self._metrics_gauges:
                    for tags, value in metric_values:
                        self._metrics_gauges[metric_name].set(value, tags)

    def _reset_gauges(self):
        """Reset all gauges to their default values."""
        with self._metrics_lock:
            for metric_name, gauge in self._metrics_gauges.items():
                metric_def = self._metrics_definitions[metric_name]
                gauge.set(metric_def.default, self._base_tags)

    def _start_metrics_thread(
        self, push_interval_s: float = DEFAULT_METRICS_PUSH_INTERVAL_S
    ):
        """Start a thread to periodically push metrics to Prometheus.

        Args:
            push_interval_s: Interval in seconds between metrics pushes.
        """
        if self._thread is not None:
            return  # Thread already running

        self._thread_stop_event = threading.Event()

        def push_metrics_loop():
            while not self._thread_stop_event.is_set():
                self._push_metrics()
                time.sleep(push_interval_s)

        self._thread = threading.Thread(target=push_metrics_loop, daemon=True)
        self._thread.start()

    def _stop_metrics_thread(self):
        """Stop the metrics push thread."""
        if self._thread is None or self._thread_stop_event is None:
            return  # No thread running

        self._thread_stop_event.set()
        # Push metrics one final time before shutting down
        self._push_metrics()
        self._thread.join(timeout=1.0)  # Wait for thread to finish with timeout
        self._thread = None
        self._thread_stop_event = None
