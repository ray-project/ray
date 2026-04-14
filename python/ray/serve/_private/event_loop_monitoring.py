import asyncio
import logging
import time
from typing import Dict, Optional

from ray.serve._private.constants import (
    RAY_SERVE_EVENT_LOOP_MONITORING_INTERVAL_S,
    SERVE_EVENT_LOOP_LATENCY_HISTOGRAM_BOUNDARIES_MS,
    SERVE_LOGGER_NAME,
)
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)


def setup_event_loop_monitoring(
    loop: asyncio.AbstractEventLoop,
    scheduling_latency: metrics.Histogram,
    iterations: metrics.Counter,
    tasks: metrics.Gauge,
    tags: Dict[str, str],
    interval_s: Optional[float] = None,
) -> asyncio.Task:
    """Start monitoring an event loop and recording metrics.

    This function creates a background task that periodically measures:
    - How long it takes for the event loop to wake up after sleeping
      (scheduling latency / event loop lag)
    - The number of pending asyncio tasks

    Args:
        loop: The asyncio event loop to monitor.
        scheduling_latency: Histogram metric to record scheduling latency.
        iterations: Counter metric to track monitoring iterations.
        tasks: Gauge metric to track number of pending tasks.
        tags: Dictionary of tags to apply to all metrics.
        interval_s: Optional override for the monitoring interval.
            Defaults to RAY_SERVE_EVENT_LOOP_MONITORING_INTERVAL_S.

    Returns:
        The asyncio Task running the monitoring loop.
    """
    if interval_s is None:
        interval_s = RAY_SERVE_EVENT_LOOP_MONITORING_INTERVAL_S

    return loop.create_task(
        _run_monitoring_loop(
            loop=loop,
            schedule_latency=scheduling_latency,
            iterations=iterations,
            task_gauge=tasks,
            tags=tags,
            interval_s=interval_s,
        ),
        name="serve_event_loop_monitoring",
    )


async def _run_monitoring_loop(
    loop: asyncio.AbstractEventLoop,
    schedule_latency: metrics.Histogram,
    iterations: metrics.Counter,
    task_gauge: metrics.Gauge,
    tags: Dict[str, str],
    interval_s: float,
) -> None:
    """Internal monitoring loop that runs until the event loop stops.

    The scheduling latency is measured by comparing the actual elapsed time
    after sleeping to the expected sleep duration. In an ideal scenario
    with no blocking, the latency should be close to zero.
    """
    while loop.is_running():
        iterations.inc(1, tags)
        num_tasks = len(asyncio.all_tasks(loop))
        task_gauge.set(num_tasks, tags)
        yield_time = time.monotonic()
        await asyncio.sleep(interval_s)
        elapsed_time = time.monotonic() - yield_time

        # Historically, Ray's implementation of histograms are extremely finicky
        # with non-positive values (https://github.com/ray-project/ray/issues/26698).
        # Technically it shouldn't be possible for this to be negative, add the
        # max just to be safe.
        # Convert to milliseconds for the metric.
        latency_ms = max(0.0, (elapsed_time - interval_s) * 1000)
        schedule_latency.observe(latency_ms, tags)


class EventLoopMonitor:
    TAG_KEY_COMPONENT = "component"
    TAG_KEY_LOOP_TYPE = "loop_type"
    TAG_KEY_ACTOR_ID = "actor_id"

    # Component types
    COMPONENT_PROXY = "proxy"
    COMPONENT_REPLICA = "replica"
    COMPONENT_UNKNOWN = "unknown"

    # Loop types
    LOOP_TYPE_MAIN = "main"
    LOOP_TYPE_USER_CODE = "user_code"
    LOOP_TYPE_ROUTER = "router"

    def __init__(
        self,
        component: str,
        loop_type: str,
        actor_id: str,
        interval_s: float = RAY_SERVE_EVENT_LOOP_MONITORING_INTERVAL_S,
        extra_tags: Optional[Dict[str, str]] = None,
    ):
        """Initialize the event loop monitor.

        Args:
            component: The component type ("proxy" or "replica").
            loop_type: The type of event loop ("main", "user_code", or "router").
            actor_id: The ID of the actor where this event loop runs.
            interval_s: Optional override for the monitoring interval.
            extra_tags: Optional dictionary of additional tags to include in metrics.
        """
        self._interval_s = interval_s
        self._tags = {
            self.TAG_KEY_COMPONENT: component,
            self.TAG_KEY_LOOP_TYPE: loop_type,
            self.TAG_KEY_ACTOR_ID: actor_id,
        }
        if extra_tags:
            self._tags.update(extra_tags)
        self._tag_keys = tuple(self._tags.keys())

        # Create metrics
        self._scheduling_latency = metrics.Histogram(
            "serve_event_loop_scheduling_latency_ms",
            description=(
                "Latency of getting yielded control on the event loop in milliseconds. "
                "High values indicate the event loop is blocked."
            ),
            boundaries=SERVE_EVENT_LOOP_LATENCY_HISTOGRAM_BOUNDARIES_MS,
            tag_keys=self._tag_keys,
        )
        self._scheduling_latency.set_default_tags(self._tags)

        self._iterations = metrics.Counter(
            "serve_event_loop_monitoring_iterations",
            description=(
                "Number of times the event loop monitoring task has run. "
                "Can be used as a heartbeat."
            ),
            tag_keys=self._tag_keys,
        )
        self._iterations.set_default_tags(self._tags)

        self._tasks = metrics.Gauge(
            "serve_event_loop_tasks",
            description="Number of pending asyncio tasks on the event loop.",
            tag_keys=self._tag_keys,
        )
        self._tasks.set_default_tags(self._tags)

        self._monitoring_task: Optional[asyncio.Task] = None

    def start(self, loop: asyncio.AbstractEventLoop) -> asyncio.Task:
        """Start monitoring the given event loop.

        Args:
            loop: The asyncio event loop to monitor.

        Returns:
            The asyncio Task running the monitoring loop.
        """
        self._monitoring_task = setup_event_loop_monitoring(
            loop=loop,
            scheduling_latency=self._scheduling_latency,
            iterations=self._iterations,
            tasks=self._tasks,
            tags=self._tags,
            interval_s=self._interval_s,
        )
        logger.debug(
            f"Started event loop monitoring for {self._tags[self.TAG_KEY_COMPONENT]} "
            f"({self._tags[self.TAG_KEY_LOOP_TYPE]}) actor {self._tags[self.TAG_KEY_ACTOR_ID]}"
        )
        return self._monitoring_task

    def stop(self):
        if self._monitoring_task is not None and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            self._monitoring_task = None

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags.copy()
