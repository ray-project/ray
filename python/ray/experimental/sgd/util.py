from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import filelock
import json
import logging
import numpy as np
import os
import pyarrow
import pyarrow.plasma as plasma
import time
import tensorflow as tf

import ray

logger = logging.getLogger(__name__)


def warmup():
    logger.info("Warming up object store")
    zeros = np.zeros(int(100e6 / 8), dtype=np.float64)
    start = time.time()
    for _ in range(10):
        ray.put(zeros)
    logger.info("Initial latency for 100MB put {}".format(
        (time.time() - start) / 10))
    for _ in range(5):
        for _ in range(100):
            ray.put(zeros)
        start = time.time()
        for _ in range(10):
            ray.put(zeros)
        logger.info("Warmed up latency for 100MB put {}".format(
            (time.time() - start) / 10))


def fetch(oids):
    raylet_client = ray.worker.global_worker.raylet_client
    for o in oids:
        ray_obj_id = ray.ObjectID(o)
        raylet_client.fetch_or_reconstruct([ray_obj_id], True)


def run_timeline(sess, ops, feed_dict=None, write_timeline=False, name=""):
    feed_dict = feed_dict or {}
    if write_timeline:
        run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
        run_metadata = tf.RunMetadata()
        fetches = sess.run(
            ops,
            options=run_options,
            run_metadata=run_metadata,
            feed_dict=feed_dict)
        trace = Timeline(step_stats=run_metadata.step_stats)
        outf = "timeline-{}-{}.json".format(name, os.getpid())
        trace_file = open(outf, "w")
        logger.info("wrote tf timeline to", os.path.abspath(outf))
        trace_file.write(trace.generate_chrome_trace_format())
    else:
        fetches = sess.run(ops, feed_dict=feed_dict)
    return fetches


class Timeline(object):
    def __init__(self, tid):
        self.events = []
        self.offset = 0
        self.start_time = self.time()
        self.tid = tid

    def patch_ray(self):
        orig_log = ray.worker.log

        def custom_log(event_type, kind, *args, **kwargs):
            orig_log(event_type, kind, *args, **kwargs)
            if kind == ray.worker.LOG_SPAN_START:
                self.start(event_type)
            elif kind == ray.worker.LOG_SPAN_END:
                self.end(event_type)
            elif kind == ray.worker.LOG_SPAN_POINT:
                self.event(event_type)

        ray.worker.log = custom_log

    def time(self):
        return time.time() + self.offset

    def reset(self):
        self.events = []
        self.start_time = self.time()

    def start(self, name):
        self.events.append((self.tid, "B", name, self.time()))

    def end(self, name):
        self.events.append((self.tid, "E", name, self.time()))

    def event(self, name):
        now = self.time()
        self.events.append((self.tid, "B", name, now))
        self.events.append((self.tid, "E", name, now + .0001))

    def merge(self, other):
        if other.start_time < self.start_time:
            self.start_time = other.start_time
        self.events.extend(other.events)
        self.events.sort(key=lambda e: e[3])

    def chrome_trace_format(self, filename):
        out = []
        for tid, ph, name, t in self.events:
            ts = int((t - self.start_time) * 1000000)
            out.append({
                "name": name,
                "tid": tid,
                "pid": tid,
                "ph": ph,
                "ts": ts,
            })
        with open(filename, "w") as f:
            f.write(json.dumps(out))
        logger.info("Wrote chrome timeline to", filename)


def ensure_plasma_tensorflow_op():
    base_path = os.path.join(pyarrow.__path__[0], "tensorflow")
    lock_path = os.path.join(base_path, "compile_op.lock")
    with filelock.FileLock(lock_path):
        if not os.path.exists(os.path.join(base_path, "plasma_op.so")):
            plasma.build_plasma_tensorflow_op()
        else:
            plasma.load_plasma_tensorflow_op()


if __name__ == "__main__":
    a = Timeline(1)
    b = Timeline(2)
    a.start("hi")
    time.sleep(.1)
    b.start("bye")
    a.start("hi3")
    time.sleep(.1)
    a.end("hi3")
    b.end("bye")
    time.sleep(.1)
    a.end("hi")
    b.start("b1")
    b.end("b1")
    a.merge(b)
    a.chrome_trace_format("test.json")
