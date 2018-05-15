from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import json
import time


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
        print("Wrote chrome timeline to", filename)


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
