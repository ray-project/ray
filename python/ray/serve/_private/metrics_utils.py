import asyncio
import bisect
import logging
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain
from typing import (
    Callable,
    DefaultDict,
    Dict,
    Hashable,
    Iterable,
    List,
    Optional,
    Tuple,
)

from ray.serve._private.constants import (
    METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)

QUEUED_REQUESTS_KEY = "queued"

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class _MetricsTask:
    task_func: Callable
    interval_s: float


class MetricsPusher:
    """Periodically runs registered asyncio tasks."""

    def __init__(
        self,
        *,
        async_sleep: Optional[Callable[[int], None]] = None,
    ):
        self._async_sleep = async_sleep or asyncio.sleep
        self._tasks: Dict[str, _MetricsTask] = dict()
        self._async_tasks: Dict[str, asyncio.Task] = dict()

        # The event needs to be lazily initialized because this class may be constructed
        # on the main thread but its methods called on a separate asyncio loop.
        self._stop_event: Optional[asyncio.Event] = None

    @property
    def stop_event(self) -> asyncio.Event:
        if self._stop_event is None:
            self._stop_event = asyncio.Event()

        return self._stop_event

    def start(self):
        self.stop_event.clear()

    async def metrics_task(self, name: str):
        """Periodically runs `task_func` every `interval_s` until `stop_event` is set.

        If `task_func` raises an error, an exception will be logged.
        """

        wait_for_stop_event = asyncio.create_task(self.stop_event.wait())
        while True:
            if wait_for_stop_event.done():
                return

            try:
                self._tasks[name].task_func()
            except Exception as e:
                logger.exception(f"Failed to run metrics task '{name}': {e}")

            sleep_task = asyncio.create_task(
                self._async_sleep(self._tasks[name].interval_s)
            )
            await asyncio.wait(
                [sleep_task, wait_for_stop_event],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not sleep_task.done():
                sleep_task.cancel()

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
            self._async_tasks[name] = asyncio.create_task(self.metrics_task(name))

    def stop_tasks(self):
        self.stop_event.set()
        self._tasks.clear()
        self._async_tasks.clear()

    async def graceful_shutdown(self):
        """Shutdown metrics pusher gracefully.

        This method will ensure idempotency of shutdown call.
        """

        self.stop_event.set()
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
        self.data: DefaultDict[Hashable, List[TimeStampedValue]] = defaultdict(list)

    def add_metrics_point(self, data_points: Dict[Hashable, float], timestamp: float):
        """Push new data points to the store.

        Args:
            data_points: dictionary containing the metrics values. The
              key should uniquely identify this time series
              and to be used to perform aggregation.
            timestamp: the unix epoch timestamp the metrics are
              collected at.
        """
        for name, value in data_points.items():
            # Using in-sort to insert while maintaining sorted ordering.
            bisect.insort(a=self.data[name], x=TimeStampedValue(timestamp, value))

    def prune_keys_and_compact_data(self, start_timestamp_s: float):
        """Prune keys and compact data that are outdated.

        For keys that haven't had new data recorded after the timestamp,
        remove them from the database.
        For keys that have, compact the datapoints that were recorded
        before the timestamp.
        """
        for key, datapoints in list(self.data.items()):
            if len(datapoints) == 0 or datapoints[-1].timestamp < start_timestamp_s:
                del self.data[key]
            else:
                self.data[key] = self._get_datapoints(key, start_timestamp_s)

    def _get_datapoints(
        self, key: Hashable, window_start_timestamp_s: float
    ) -> List[TimeStampedValue]:
        """Get all data points given key after window_start_timestamp_s"""

        datapoints = self.data[key]

        idx = bisect.bisect(
            a=datapoints,
            x=TimeStampedValue(
                timestamp=window_start_timestamp_s, value=0  # dummy value
            ),
        )
        return datapoints[idx:]

    def _aggregate_reduce(
        self,
        keys: Iterable[Hashable],
        aggregate_fn: Callable[[Iterable[float]], float],
    ) -> Optional[Tuple[float, int]]:
        """Reduce the entire set of timeseries values across the specified keys.

        Args:
            keys: Iterable of keys to aggregate across.
            aggregate_fn: Function to apply across all float values, e.g., sum, max.

        Returns:
            A tuple of (float, int) where the first element is the aggregated value
            and the second element is the number of valid keys used.
            Returns None if no values are available.
        """
        report_count = 0

        # This is used to count the number of valid keys that have data without
        # iterating over the keys multiple times.
        def values():
            nonlocal report_count
            for key in keys:
                series = self.data.get(key, ())
                if not series:
                    continue
                report_count += 1
                for ts in series:
                    yield ts.value

        it = values()
        _empty = object()
        first = next(it, _empty)
        if first is _empty:
            return None

        agg_result = aggregate_fn(chain((first,), it))
        return agg_result, report_count

    def get_latest(
        self,
        key: Hashable,
    ) -> Optional[float]:
        """Get the latest value for a given key."""
        if not self.data.get(key, None):
            return None
        return self.data[key][-1].value

    def aggregate_min(
        self,
        keys: Iterable[Hashable],
    ) -> Optional[Tuple[float, int]]:
        """Aggregate min value across the specified keys."""
        return self._aggregate_reduce(keys, min)

    def aggregate_max(
        self,
        keys: Iterable[Hashable],
    ) -> Optional[Tuple[float, int]]:
        """Aggregate max value across the specified keys."""
        return self._aggregate_reduce(keys, max)

    def aggregate_sum(
        self,
        keys: Iterable[Hashable],
    ) -> Optional[Tuple[float, int]]:
        """Aggregate sum value across the specified keys."""
        return self._aggregate_reduce(keys, sum)

    def aggregate_avg(
        self,
        keys: Iterable[Hashable],
    ) -> Optional[Tuple[float, int]]:
        """Aggregate average value across the specified keys."""
        return self._aggregate_reduce(keys, statistics.mean)


def consolidate_metrics_stores(*stores: InMemoryMetricsStore) -> InMemoryMetricsStore:
    merged = stores[0]
    if len(stores) == 1:
        return merged
    else:
        if QUEUED_REQUESTS_KEY not in merged.data:
            merged.data[QUEUED_REQUESTS_KEY] = [
                TimeStampedValue(timestamp=0, value=0.0)
            ]
        for store in stores[1:]:
            for key, timeseries in store.data.items():
                if key == QUEUED_REQUESTS_KEY:
                    # Sum queued requests across handle metrics.
                    merged.data[QUEUED_REQUESTS_KEY][-1].value += timeseries[-1].value
                elif key not in merged.data or (
                    timeseries
                    and (
                        not merged.data[key]
                        or timeseries[-1].timestamp > merged.data[key][-1].timestamp
                    )
                ):
                    # Replace if not present or if newer datapoints are available
                    merged.data[key] = timeseries.copy()

    return merged
