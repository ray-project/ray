import datetime
import logging
import time
from typing import List, Tuple

import ray
from ray.util.metrics import Gauge, Histogram

from ray.experimental.raysort import constants
from ray.experimental.raysort import logging_utils

HISTOGRAM_BOUNDARIES = list(range(50, 200, 50))


def timeit(
    event: str,
    report_time=False,
    report_in_progress=False,
    report_completed=True,
):
    def decorator(f):
        def wrapped_f(*args, **kwargs):
            progress_tracker = ray.get_actor(constants.PROGRESS_TRACKER_ACTOR)
            progress_tracker.inc.remote(f"{event}_in_progress",
                                        echo=report_in_progress)
            try:
                start = time.time()
                ret = f(*args, **kwargs)
                end = time.time()
                duration = end - start
                progress_tracker.observe.remote(
                    f"{event}_time",
                    duration,
                    echo=report_time,
                )
                progress_tracker.inc.remote(f"{event}_completed",
                                            echo=report_completed)
                return ret
            finally:
                progress_tracker.dec.remote(f"{event}_in_progress",
                                            echo=report_in_progress)

        return wrapped_f

    return decorator


def get_metrics(_args):
    return {
        "gauges": [
            "mapper_in_progress",
            "reducer_in_progress",
            "sorting_in_progress",
            "mapper_completed",
            "reducer_completed",
            "sorting_completed",
        ],
        "histograms": [
            ("mapper_time", HISTOGRAM_BOUNDARIES),
            ("reducer_time", HISTOGRAM_BOUNDARIES),
            ("sorting_time", HISTOGRAM_BOUNDARIES),
        ],
    }


def create_progress_tracker(args):
    return ProgressTracker.options(
        name=constants.PROGRESS_TRACKER_ACTOR).remote(**get_metrics(args))


@ray.remote
class ProgressTracker:
    def __init__(
        self,
        gauges: List[str],
        histograms: List[Tuple[str, List[int]]],
    ):
        self.counts = {m: 0 for m in gauges}
        self.gauges = {m: Gauge(m) for m in gauges}
        self.reset_gauges()
        self.histograms = {
            m: Histogram(m, boundaries=b)
            for m, b in histograms
        }
        logging_utils.init()

    def reset_gauges(self):
        for g in self.gauges.values():
            g.set(0)

    def inc(self, metric_name, value=1, echo=False):
        gauge = self.gauges.get(metric_name)
        if gauge is None:
            logging.warning(f"No such Gauge: {metric_name}")
            return
        self.counts[metric_name] += value
        gauge.set(self.counts[metric_name])
        if echo:
            logging.info(f"{metric_name} {self.counts[metric_name]}")

    def dec(self, metric_name, value=1, echo=False):
        return self.inc(metric_name, -value, echo)

    def observe(self, metric_name, value, echo=False):
        histogram = self.histograms.get(metric_name)
        if histogram is None:
            logging.warning(f"No such Histogram: {metric_name}")
            return
        histogram.observe(value)
        if echo:
            logging.info(f"{metric_name} {value}")


def export_timeline():
    timestr = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"/tmp/ray-timeline-{timestr}.json"
    ray.timeline(filename=filename)
    logging.info(f"Exported Ray timeline to {filename}")
