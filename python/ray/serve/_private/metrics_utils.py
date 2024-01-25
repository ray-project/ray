import bisect
import logging
import math
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Union

import ray
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


class _MetricTask:
    def __init__(self, task_func, interval_s, callback_func):
        """
        Args:
            task_func: a callable that MetricsPusher will try to call in each loop.
            interval_s: the interval of each task_func is supposed to be called.
            callback_func: callback function is called when task_func is done, and
                the result of task_func is passed to callback_func as the first
                argument, and the timestamp of the call is passed as the second
                argument.
        """
        self.task_func: Callable = task_func
        self.interval_s: float = interval_s
        self.callback_func: Callable[[Any, float]] = callback_func
        self.last_ref: Optional[ray.ObjectRef] = None
        self.last_call_succeeded_time: Optional[float] = time.time()


class MetricsPusher:
    """
    Metrics pusher is a background thread that run the registered tasks in a loop.
    """

    def __init__(
        self,
    ):
        self.tasks: Dict[str, _MetricTask] = dict()
        self.pusher_thread: Union[threading.Thread, None] = None
        self.stop_event = threading.Event()

    def register_or_update_task(
        self,
        name: str,
        task_func: Callable,
        interval_s: int,
        process_func: Optional[Callable] = None,
    ) -> None:
        """Register a task under the provided name, or update it.

        This method is idempotent - if a task is already registered with
        the specified name, it will update it with the most recent info.
        """

        self.tasks[name] = _MetricTask(task_func, interval_s, process_func)

    def start(self):
        """Start a background thread to run the registered tasks in a loop.

        We use this background so it will be not blocked by user's code and ensure
        consistently metrics delivery. Python GIL will ensure that this thread gets
        fair timeshare to execute and run.
        """

        if len(self.tasks) == 0:
            raise ValueError("MetricsPusher has zero tasks registered.")

        if self.pusher_thread and self.pusher_thread.is_alive():
            return

        def send_forever():
            while True:
                if self.stop_event.is_set():
                    return

                start = time.time()
                for task in self.tasks.values():
                    try:
                        if start - task.last_call_succeeded_time >= task.interval_s:
                            if task.last_ref:
                                ready_refs, _ = ray.wait([task.last_ref], timeout=0)
                                if len(ready_refs) == 0:
                                    continue
                            data = task.task_func()
                            task.last_call_succeeded_time = time.time()
                            if task.callback_func and ray.is_initialized():
                                task.last_ref = task.callback_func(
                                    data, send_timestamp=time.time()
                                )
                    except Exception as e:
                        logger.warning(
                            f"MetricsPusher thread failed to run metric task: {e}"
                        )

                # For all tasks, check when the task should be executed
                # next. Sleep until the next closest time.
                least_interval_s = math.inf
                for task in self.tasks.values():
                    time_until_next_push = task.interval_s - (
                        time.time() - task.last_call_succeeded_time
                    )
                    least_interval_s = min(least_interval_s, time_until_next_push)

                time.sleep(max(least_interval_s, 0))

        self.pusher_thread = threading.Thread(target=send_forever)
        # Making this a daemon thread so it doesn't leak upon shutdown, and it
        # doesn't need to block the replica's shutdown.
        self.pusher_thread.setDaemon(True)
        self.pusher_thread.start()

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        """Shutdown metrics pusher gracefully.

        This method will ensure idempotency of shutdown call.
        """
        if not self.stop_event.is_set():
            self.stop_event.set()

        if self.pusher_thread:
            self.pusher_thread.join()

        self.tasks.clear()


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
