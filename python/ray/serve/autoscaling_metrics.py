import threading
import bisect
from collections import defaultdict
import time
from typing import Callable, DefaultDict, Dict, List, Optional
from dataclasses import dataclass, field

import ray


def start_metrics_pusher(
    interval_s: float,
    collection_callback: Callable[[], Dict[str, float]],
    controller_handle,
):
    """Start a background thread to push metrics to controller.

    We use this background so it will be not blocked by user's code and ensure
    consistently metrics delivery. Python GIL will ensure that this thread gets
    fair timeshare to execute and run.

    Args:
        interval_s(float): the push interval.
        collection_callback: a callable that returns the metric data points to
          be sent to the the controller. The collection callback should take
          no argument and returns a dictionary of str_key -> float_value.
        controller_handle: actor handle to Serve controller.
    """

    def send_once():
        data = collection_callback()
        # TODO(simon): maybe wait for ack or handle controller failure?
        return controller_handle.record_autoscaling_metrics.remote(
            data=data, send_timestamp=time.time()
        )

    def send_forever():
        last_ref: Optional[ray.ObjectRef] = None
        last_send_succeeded: bool = True

        while True:
            start = time.time()

            if last_ref:
                ready_refs, _ = ray.wait([last_ref], timeout=0)
                last_send_succeeded = len(ready_refs) == 1
            if last_send_succeeded:
                last_ref = send_once()

            duration_s = time.time() - start
            remaining_time = interval_s - duration_s
            if remaining_time > 0:
                time.sleep(remaining_time)

    timer = threading.Thread(target=send_forever)
    # Making this a daemon thread so it doesn't leak upon shutdown, and it
    # doesn't need to block the replica's shutdown.
    timer.setDaemon(True)
    timer.start()


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
            data_points(dict): dictionary containing the metrics values. The
              key should be a string that uniquely identifies this time series
              and to be used to perform aggregation.
            timestamp(float): the unix epoch timestamp the metrics are
              collected at.
        """
        for name, value in data_points.items():
            # Using in-sort to insert while maintaining sorted ordering.
            bisect.insort(a=self.data[name], x=TimeStampedValue(timestamp, value))

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
        datapoints = self.data[key]

        idx = bisect.bisect(
            a=datapoints,
            x=TimeStampedValue(
                timestamp=window_start_timestamp_s, value=0  # dummy value
            ),
        )
        points_after_idx = datapoints[idx:]

        if do_compact:
            self.data[key] = points_after_idx

        if len(points_after_idx) == 0:
            return
        return sum(point.value for point in points_after_idx) / len(points_after_idx)
