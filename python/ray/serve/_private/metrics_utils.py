import asyncio
import bisect
import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass
from itertools import chain
from typing import (
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    Hashable,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from ray._raylet import (
    merge_instantaneous_total_cython,
    time_weighted_average_cython,
)
from ray.serve._private.common import TimeSeries, TimeStampedValue
from ray.serve._private.constants import (
    METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve.config import AggregationFunction

QUEUED_REQUESTS_KEY = "queued"

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class _MetricsTask:
    task_func: Union[Callable, Callable[[], Awaitable]]
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
        Supports both sync and async task functions.
        """

        wait_for_stop_event = asyncio.create_task(self.stop_event.wait())
        while True:
            if wait_for_stop_event.done():
                return

            try:
                task_func = self._tasks[name].task_func
                # Check if the function is a coroutine function
                if asyncio.iscoroutinefunction(task_func):
                    await task_func()
                else:
                    task_func()
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
        task_func: Union[Callable, Callable[[], Awaitable]],
        interval_s: int,
    ) -> None:
        """Register a sync or async task under the provided name, or update it.

        This method is idempotent - if a task is already registered with
        the specified name, it will update it with the most recent info.

        Args:
            name: Unique name for the task.
            task_func: Either a sync function or async function (coroutine function).
            interval_s: Interval in seconds between task executions.
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
        self.data: DefaultDict[Hashable, TimeSeries] = defaultdict(list)

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
    ) -> TimeSeries:
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

    def timeseries_count(
        self,
        key: Hashable,
    ) -> int:
        """Count the number of values across all timeseries values at the specified keys."""
        series = self.data.get(key, [])
        if not series:
            return 0
        return len(series)


def time_weighted_average(
    step_series: TimeSeries,
    window_start: Optional[float] = None,
    window_end: Optional[float] = None,
    last_window_s: float = 1.0,
) -> Optional[float]:
    """
    Compute time-weighted average of a step function over a time interval.

    This function uses a Cython-optimized implementation for improved performance.

    Args:
        step_series: Step function as list of (timestamp, value) points, sorted by time.
            Values are right-continuous (constant until next change).
        window_start: Start of averaging window (inclusive). If None, uses the start of the series.
        window_end: End of averaging window (exclusive). If None, uses the end of the series.
        last_window_s: when window_end is None, uses the last_window_s to compute the end of the window.
    Returns:
        Time-weighted average over the interval, or None if no data overlaps.
    """
    # Convert None to negative infinity for Cython (C doesn't have None)
    # Using -inf instead of a specific value like -1.0 ensures any valid float
    # (including -1.0) can be used as a window boundary.
    ws = window_start if window_start is not None else float("-inf")
    we = window_end if window_end is not None else float("-inf")
    return time_weighted_average_cython(step_series, ws, we, last_window_s)


def aggregate_timeseries(
    timeseries: TimeSeries,
    aggregation_function: AggregationFunction,
    last_window_s: float = 1.0,
    window_start: Optional[float] = None,
) -> Optional[float]:
    """Aggregate the values in a timeseries using a specified function."""
    if aggregation_function == AggregationFunction.MEAN:
        return time_weighted_average(
            timeseries, window_start=window_start, last_window_s=last_window_s
        )
    elif aggregation_function == AggregationFunction.MAX:
        values = (
            ts.value
            for ts in timeseries
            if window_start is None or ts.timestamp >= window_start
        )
        return max(values, default=None)
    elif aggregation_function == AggregationFunction.MIN:
        values = (
            ts.value
            for ts in timeseries
            if window_start is None or ts.timestamp >= window_start
        )
        return min(values, default=None)
    else:
        raise ValueError(f"Invalid aggregation function: {aggregation_function}")


def merge_instantaneous_total(
    replicas_timeseries: List[TimeSeries],
) -> TimeSeries:
    """
    Merge multiple gauge time series (right-continuous, LOCF) into an
    instantaneous total time series as a step function.

    This function uses a Cython-optimized implementation for 5-10x performance
    improvement over pure Python.

    This approach treats each replica's gauge as right-continuous, last-observation-
    carried-forward (LOCF), which matches gauge semantics. It produces an exact
    instantaneous total across replicas without bias from arbitrary windowing.

    Uses a k-way merge algorithm for O(n log k) complexity where k is the number
    of timeseries and n is the total number of events.

    Timestamps are rounded to 10ms precision (2 decimal places) and datapoints
    with the same rounded timestamp are combined, keeping the most recent value.

    Args:
        replicas_timeseries: List of time series, one per replica. Each time series
            is a list of TimeStampedValue objects sorted by timestamp.

    Returns:
        A list of TimeStampedValue representing the instantaneous total at event times.
        Between events, the total remains constant (step function). Timestamps are
        rounded to 10ms precision and duplicate timestamps are combined.
    """
    # Handle trivial cases in Python to avoid type conversion overhead
    active_series = [series for series in replicas_timeseries if series]
    if not active_series:
        return []
    if len(active_series) == 1:
        return active_series[0]

    # Cython returns list of (timestamp, value) tuples; convert to TimeStampedValue
    merged_tuples = merge_instantaneous_total_cython(active_series)
    return [TimeStampedValue(ts, val) for ts, val in merged_tuples]


def merge_timeseries_dicts(
    *timeseries_dicts: DefaultDict[Hashable, TimeSeries],
) -> DefaultDict[Hashable, TimeSeries]:
    """
    Merge multiple time-series dictionaries using instantaneous merge approach.
    """
    merged: DefaultDict[Hashable, TimeSeries] = defaultdict(list)

    for ts_dict in timeseries_dicts:
        for key, ts in ts_dict.items():
            merged[key].append(ts)

    return {key: merge_instantaneous_total(ts_list) for key, ts_list in merged.items()}
