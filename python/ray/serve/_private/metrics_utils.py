import asyncio
import bisect
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, DefaultDict, Dict, List, Optional

from ray.serve._private.constants import (
    METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class _MetricsTask:
    task_func: Callable
    interval_s: float


class MetricsPusher:
    """Periodically runs registered asyncio tasks."""

    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        async_sleep: Optional[Callable[[int], None]] = None,
    ):
        self._event_loop = event_loop
        self._async_sleep = async_sleep or asyncio.sleep
        self._tasks: Dict[str, _MetricsTask] = dict()
        self._async_tasks: Dict[str, asyncio.Task] = dict()
        self._stop_event = asyncio.Event(loop=event_loop)

    def start(self):
        self._stop_event.clear()

    async def metrics_task(self, name: str):
        """Periodically runs `task_func` every `interval_s` until `stop_event` is set.

        If `task_func` raises an error, an exception will be logged.
        """

        wait_for_stop_event = asyncio.create_task(self._stop_event.wait())
        while True:
            if wait_for_stop_event.done():
                return

            try:
                self._tasks[name].task_func()
            except Exception as e:
                logger.exception(f"Failed to run metrics task '{name}': {e}")

            await asyncio.wait(
                [
                    wait_for_stop_event,
                    self._async_sleep(self._tasks[name].interval_s),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

    def register_or_update_task(
        self,
        name: str,
        task_func: Callable,
        interval_s: int,
    ) -> None:
        """Register a task under the provided name, or update it.

        This method is idempotent - if a task is already registered with
        the specified name, it will update it with the most recent info.
        """

        self._tasks[name] = _MetricsTask(task_func, interval_s)
        if name not in self._async_tasks or self._async_tasks[name].done():
            self._async_tasks[name] = self._event_loop.create_task(
                self.metrics_task(name)
            )

    def stop_tasks(self):
        self._stop_event.set()
        self._tasks.clear()
        self._async_tasks.clear()

    async def graceful_shutdown(self):
        """Shutdown metrics pusher gracefully.

        This method will ensure idempotency of shutdown call.
        """

        self._stop_event.set()
        if self._async_tasks:
            await asyncio.wait(
                list(self._async_tasks.values()),
                timeout=METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S,
            )

        self._tasks.clear()
        self._async_tasks.clear()


@dataclass(order=True)
class TimeStampedValue:
    timestamp: float
    value: float = field(compare=False)


class InMemoryMetricsStore:
    """A very simple, in memory time series database"""

    def __init__(self):
        self.data: DefaultDict[str, List[TimeStampedValue]] = defaultdict(list)

    def add_metrics_point(self, data_points: Dict[str, float], timestamp: float):
        """Push new data points to the store.

        Args:
            data_points: dictionary containing the metrics values. The
              key should be a string that uniquely identifies this time series
              and to be used to perform aggregation.
            timestamp: the unix epoch timestamp the metrics are
              collected at.
        """
        for name, value in data_points.items():
            # Using in-sort to insert while maintaining sorted ordering.
            bisect.insort(a=self.data[name], x=TimeStampedValue(timestamp, value))

    def _get_datapoints(self, key: str, window_start_timestamp_s: float) -> List[float]:
        """Get all data points given key after window_start_timestamp_s"""

        datapoints = self.data[key]

        idx = bisect.bisect(
            a=datapoints,
            x=TimeStampedValue(
                timestamp=window_start_timestamp_s, value=0  # dummy value
            ),
        )
        return datapoints[idx:]

    def window_average(
        self, key: str, window_start_timestamp_s: float, do_compact: bool = True
    ) -> Optional[float]:
        """Perform a window average operation for metric `key`

        Args:
            key: the metric name.
            window_start_timestamp_s: the unix epoch timestamp for the
              start of the window. The computed average will use all datapoints
              from this timestamp until now.
            do_compact: whether or not to delete the datapoints that's
              before `window_start_timestamp_s` to save memory. Default is
              true.
        Returns:
            The average of all the datapoints for the key on and after time
            window_start_timestamp_s, or None if there are no such points.
        """
        points_after_idx = self._get_datapoints(key, window_start_timestamp_s)

        if do_compact:
            self.data[key] = points_after_idx

        if len(points_after_idx) == 0:
            return
        return sum(point.value for point in points_after_idx) / len(points_after_idx)

    def max(self, key: str, window_start_timestamp_s: float, do_compact: bool = True):
        """Perform a max operation for metric `key`.

        Args:
            key: the metric name.
            window_start_timestamp_s: the unix epoch timestamp for the
              start of the window. The computed average will use all datapoints
              from this timestamp until now.
            do_compact: whether or not to delete the datapoints that's
              before `window_start_timestamp_s` to save memory. Default is
              true.
        Returns:
            Max value of the data points for the key on and after time
            window_start_timestamp_s, or None if there are no such points.
        """
        points_after_idx = self._get_datapoints(key, window_start_timestamp_s)

        if do_compact:
            self.data[key] = points_after_idx

        return max((point.value for point in points_after_idx), default=None)
