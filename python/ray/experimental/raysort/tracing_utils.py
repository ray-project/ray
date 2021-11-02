import collections
import functools
import json
import logging
import os
import time
from typing import List

import pandas as pd
import ray
from ray.util.metrics import Gauge

from ray.experimental.raysort import constants
from ray.experimental.raysort import logging_utils

Span = collections.namedtuple(
    "Span",
    ["time", "duration", "event", "address", "pid"],
)


def timeit(
        event: str,
        report_time=False,
        report_in_progress=True,
        report_completed=True,
):
    def decorator(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            progress_tracker = get_progress_tracker()
            progress_tracker.inc.remote(
                f"{event}_in_progress", echo=report_in_progress)
            try:
                begin = time.time()
                ret = f(*args, **kwargs)
                duration = time.time() - begin
                progress_tracker.record_span.remote(
                    Span(begin, duration, event,
                         ray.util.get_node_ip_address(), os.getpid()),
                    echo=report_time,
                )
                progress_tracker.inc.remote(
                    f"{event}_completed", echo=report_completed)
                return ret
            finally:
                progress_tracker.dec.remote(f"{event}_in_progress")

        return wrapped_f

    return decorator


def record_value(metric_name, duration, echo=False):
    progress_tracker = get_progress_tracker()
    progress_tracker.record_value.remote(metric_name, duration, echo)


def get_metrics(_args):
    return {
        "gauges": [
            "map_in_progress",
            "merge_in_progress",
            "reduce_in_progress",
            "sort_in_progress",
            "map_completed",
            "merge_completed",
            "reduce_completed",
            "sort_completed",
        ],
    }


def get_progress_tracker():
    return ray.get_actor(constants.PROGRESS_TRACKER_ACTOR)


def create_progress_tracker(args):
    return ProgressTracker.options(
        name=constants.PROGRESS_TRACKER_ACTOR).remote(**get_metrics(args))


def _make_trace_event(span: Span):
    return {
        "cat": span.event,
        "name": span.event,
        "pid": span.address,
        "tid": span.pid,
        "ts": span.time * 1_000_000,
        "dur": span.duration * 1_000_000,
        "ph": "X",
        "args": {},
    }


@ray.remote
class ProgressTracker:
    def __init__(
            self,
            gauges: List[str],
    ):
        self.counts = {m: 0 for m in gauges}
        self.gauges = {m: Gauge(m) for m in gauges}
        self.series = collections.defaultdict(list)
        self.spans = []
        self.reset_gauges()
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

    def record_value(self, metric_name, value, echo=False):
        self.series[metric_name].append(value)
        if echo:
            logging.info(f"{metric_name} {value}")

    def record_span(self, span: Span, record_value=True, echo=False):
        self.spans.append(span)
        if echo:
            logging.info(f"{span}")
        if record_value:
            self.record_value(span.event, span.duration, echo=False)

    def report(self):
        ret = []
        for key, values in self.series.items():
            ss = pd.Series(values)
            ret.append(
                f"{key:20}mean={ss.mean():.2f}\tstd={ss.std():.2f}\t"
                f"max={ss.max():.2f}\tmin={ss.min():.2f}\tcount={ss.count()}")
        return "\n".join(ret)

    def create_trace(self):
        self.spans.sort(key=lambda span: span.time)
        ret = [_make_trace_event(span) for span in self.spans]
        return json.dumps(ret)


def export_timeline(args):
    filename = f"/tmp/ray-{args.run_id}.json"
    ray.timeline(filename=filename)
    logging.info(f"Exported Ray timeline to {filename}")


def performance_report(run_id: str, agent: ray.actor.ActorHandle):
    report = agent.report.remote()
    trace = agent.create_trace.remote()
    print(ray.get(report))
    filename = f"/tmp/raysort-{run_id}.json"
    with open(filename, "w") as fout:
        fout.write(ray.get(trace))
    logging.info(f"Exported RaySort timeline to {filename}")
