import asyncio
import bisect
import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass
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

from ray.serve._private.common import TimeStampedValue
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
    ) -> Tuple[Optional[float], int]:
        """Reduce the entire set of timeseries values across the specified keys.

        Args:
            keys: Iterable of keys to aggregate across.
            aggregate_fn: Function to apply across all float values, e.g., sum, max.

        Returns:
            A tuple of (float, int) where the first element is the aggregated value
            and the second element is the number of valid keys used.
            Returns (None, 0) if no valid keys have data.

        Example:
        Suppose the store contains:
        >>> store = InMemoryMetricsStore()
        >>> store.data.update({
        ...     "a": [TimeStampedValue(0, 1.0), TimeStampedValue(1, 2.0)],
        ...     "b": [],
        ...     "c": [TimeStampedValue(0, 10.0)],
        ... })

        Using sum across keys:

        >>> store._aggregate_reduce(keys=["a", "b", "c"], aggregate_fn=sum)
        (13.0, 2)

        Here:
        - The aggregated value is 1.0 + 2.0 + 10.0 = 13.0
        - Only keys "a" and "c" contribute values, so report_count = 2
        """
        valid_key_count = 0

        def _values_generator():
            """Generator that yields values from valid keys without storing them all in memory."""
            nonlocal valid_key_count
            for key in keys:
                series = self.data.get(key, [])
                if not series:
                    continue

                valid_key_count += 1
                for timestamp_value in series:
                    yield timestamp_value.value

        # Create the generator and check if it has any values
        values_gen = _values_generator()
        try:
            first_value = next(values_gen)
        except StopIteration:
            # No valid data found
            return None, 0

        # Apply aggregation to the generator (memory efficient)
        aggregated_result = aggregate_fn(chain([first_value], values_gen))
        return aggregated_result, valid_key_count

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
    ) -> Tuple[Optional[float], int]:
        """Find the min value across all timeseries values at the specified keys.

        Args:
            keys: Iterable of keys to aggregate across.
        Returns:
            A tuple of (float, int) where the first element is the min across
            all values found at `keys`, and the second is the number of valid
            keys used to compute the min.
            Returns (None, 0) if no valid keys have data.
        """
        return self._aggregate_reduce(keys, min)

    def aggregate_max(
        self,
        keys: Iterable[Hashable],
    ) -> Tuple[Optional[float], int]:
        """Find the max value across all timeseries values at the specified keys.

        Args:
            keys: Iterable of keys to aggregate across.
        Returns:
            A tuple of (float, int) where the first element is the max across
            all values found at `keys`, and the second is the number of valid
            keys used to compute the max.
            Returns (None, 0) if no valid keys have data.
        """
        return self._aggregate_reduce(keys, max)

    def aggregate_sum(
        self,
        keys: Iterable[Hashable],
    ) -> Tuple[Optional[float], int]:
        """Sum the entire set of timeseries values across the specified keys.

        Args:
            keys: Iterable of keys to aggregate across.
        Returns:
            A tuple of (float, int) where the first element is the sum across
            all values found at `keys`, and the second is the number of valid
            keys used to compute the sum.
            Returns (None, 0) if no valid keys have data.
        """
        return self._aggregate_reduce(keys, sum)

    def aggregate_avg(
        self,
        keys: Iterable[Hashable],
    ) -> Tuple[Optional[float], int]:
        """Average the entire set of timeseries values across the specified keys.

        Args:
            keys: Iterable of keys to aggregate across.
        Returns:
            A tuple of (float, int) where the first element is the mean across
            all values found at `keys`, and the second is the number of valid
            keys used to compute the mean.
            Returns (None, 0) if no valid keys have data.
        """
        return self._aggregate_reduce(keys, statistics.mean)


def _bucket_latest_by_window(
    series: List[TimeStampedValue],
    start: float,
    window_s: float,
) -> Dict[int, float]:
    """
    Map each window index -> latest value seen in that window.
    Assumes series is sorted by timestamp ascending.
    """
    buckets: Dict[int, float] = {}
    for p in series:
        w = int((p.timestamp - start) // window_s)
        buckets[w] = p.value  # overwrite keeps the latest within the window
    return buckets


def _merge_two_timeseries(
    t1: List[TimeStampedValue], t2: List[TimeStampedValue], window_s: float
) -> List[TimeStampedValue]:
    """
    Merge two ascending time series by summing values within a specified time window.
    If multiple values fall within the same window in a series, the latest value is used.
    The output contains one point per window that had at least one value, timestamped
    at the window center.
    """
    if window_s <= 0:
        raise ValueError(f"window_s must be positive, got {window_s}")

    if not t1 and not t2:
        return []

    # Align windows so each output timestamp sits at the start of its window.
    # start is snapped to window_s boundary for binning stability
    earliest = min(x[0].timestamp for x in (t1, t2) if x)
    start = earliest // window_s * window_s

    b1 = _bucket_latest_by_window(t1, start, window_s)
    b2 = _bucket_latest_by_window(t2, start, window_s)

    windows = sorted(set(b1.keys()) | set(b2.keys()))

    merged: List[TimeStampedValue] = []
    for w in windows:
        v = b1.get(w, 0.0) + b2.get(w, 0.0)
        ts_start = start + w * window_s
        merged.append(TimeStampedValue(timestamp=ts_start, value=v))
    return merged


def merge_timeseries_dicts(
    *timeseries_dicts: DefaultDict[Hashable, List[TimeStampedValue]],
    window_s: float,
) -> DefaultDict[Hashable, List[TimeStampedValue]]:
    """
    Merge multiple time-series dictionaries, typically contained within
    InMemoryMetricsStore().data. For the same key across stores, time series
    are merged with a windowed sum, where each series keeps only its latest
    value per window before summing.
    """
    merged: DefaultDict[Hashable, List[TimeStampedValue]] = defaultdict(list)
    for timeseries_dict in timeseries_dicts:
        for key, ts in timeseries_dict.items():
            if key in merged:
                merged[key] = _merge_two_timeseries(merged[key], ts, window_s)
            else:
                # Window the data, even if the key is unique.
                merged[key] = _merge_two_timeseries(ts, [], window_s)
    return merged
