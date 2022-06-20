import bisect
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Event
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Tuple, Type

import ray
from ray.serve.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass(order=True)
class TimeStampedValue:
    value: float = field(compare=False)
    timestamp: float = field(default_factory=time.time)
    tags: Dict[Any, Any] = field(compare=False, default_factory=dict)


def start_metrics_pusher(
    interval_s: float,
    collection_callback: Callable[[], Tuple[str, TimeStampedValue]],
    metrics_process_func: Callable[[Dict[str, float], float], ray.ObjectRef],
    stop_event: Type[Event] = None,
):
    """Start a background thread to push metrics to controller.

    We use this background so it will be not blocked by user's code and ensure
    consistently metrics delivery. Python GIL will ensure that this thread gets
    fair timeshare to execute and run.

    Stop_event is passed in only when a RayServeHandle calls this function to
    push metrics for scale-to-zero. stop_event is set either when the handle
    is garbage collected or when the Serve application shuts down.

    Args:
        interval_s(float): the push interval.
        collection_callback: a callable that returns the metric data points to
          be sent to the the controller. The collection callback should take
          no argument and returns a dictionary of str_key -> float_value.
        metrics_process_func: actor handle function.
        stop_event: the backgroupd thread will be closed when this event is set
    Returns:
        timer: The background thread created by this function to push
               metrics to the controller
    """

    def send_once():
        (key, data) = collection_callback()

        # TODO(simon): maybe wait for ack or handle controller failure?
        data.timestamp = time.time()
        return metrics_process_func(key=key, data=data)

    def send_forever(stop_event):
        last_ref: Optional[ray.ObjectRef] = None
        last_send_succeeded: bool = True

        while True:
            start = time.time()
            if stop_event and stop_event.is_set():
                return

            if ray.is_initialized():
                try:
                    if last_ref:
                        ready_refs, _ = ray.wait([last_ref], timeout=0)
                        last_send_succeeded = len(ready_refs) == 1
                    if last_send_succeeded:
                        last_ref = send_once()
                except Exception as e:
                    logger.warning(
                        "Autoscaling metrics pusher thread "
                        "is failing to send metrics to the controller "
                        f": {e}"
                    )

            duration_s = time.time() - start
            remaining_time = interval_s - duration_s
            if remaining_time > 0:
                time.sleep(remaining_time)

    timer = threading.Thread(target=send_forever, args=[stop_event])
    # Making this a daemon thread so it doesn't leak upon shutdown, and it
    # doesn't need to block the replica's shutdown.
    timer.setDaemon(True)
    timer.start()
    return timer


class InMemoryMetricsStore:
    """A very simple, in memory time series database"""

    def __init__(self):
        self.data: DefaultDict[str, List[TimeStampedValue]] = defaultdict(list)

    def add_metrics_point(self, key: str, data: TimeStampedValue):
        """Push new data points to the store.

        Args:
            data_points(TimeStampedValue): dictionary containing the metrics values. The
              key should be a string that uniquely identifies this time series
              and to be used to perform aggregation.
            timestamp(float): the unix epoch timestamp the metrics are
              collected at.
            tags: the data tags used aggregate in different dimension
        """
        # Using in-sort to insert while maintaining sorted ordering.
        bisect.insort(a=self.data[key], x=data)

    def _get_datapoints(self, key: str, window_start_timestamp_s: float) -> List[float]:
        """Get all data points given key after window_start_timestamp_s"""

        datapoints = self.data[key]
        print("get points: ", datapoints)

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
            key(str): the metric name.
            window_start_timestamp_s(float): the unix epoch timestamp for the
              start of the window. The computed average will use all datapoints
              from this timestamp until now.
            do_compact(bool): whether or not to delete the datapoints that's
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

    def latest(
        self,
        key: str,
        window_start_timestamp_s: float,
        do_compact: bool = True,
        tag: Any = None,
    ):
        """Perform a latest operation for metric `key`. The function will return
            latest value given the key.

        Args:
            key(str): the metric name.
            window_start_timestamp_s(float): the unix epoch timestamp for the
              start of the window. The computed average will use all datapoints
              from this timestamp until now.
            do_compact(bool): whether or not to delete the datapoints that's
              before `window_start_timestamp_s` to save memory. Default is
              true.
            tag: if tag provided, the latest of data will be collected by the tag.
        Returns:
            Latest value of the data points for the key on and after time
            window_start_timestamp_s. if tag is provided, the return value will be
            a list, each element of the list represents each tag value. (Currently
            we don't need the tag information. If needed in the future, the function
            can directly return dict)
        """
        points_after_idx = self._get_datapoints(key, window_start_timestamp_s)

        if do_compact:
            self.data[key] = points_after_idx

        # tags aggregations
        if tag:
            datapoints_per_tags = defaultdict(TimeStampedValue)
            for point in points_after_idx[::-1]:
                if tag in point.tags and point.tags[tag] not in datapoints_per_tags:
                    datapoints_per_tags[point.tags[tag]] = point
            datapoints = list(datapoints_per_tags.values())
            print(self.data[key])
            return [point.value for point in datapoints]
        else:
            return [points_after_idx[-1].value] if points_after_idx else []
