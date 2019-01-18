from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.experimental.queue import Queue


def setup():
    if not hasattr(setup, "is_initialized"):
        ray.init(num_cpus=4)
        setup.is_initialized = True


class QueueSuite(object):
    def time_put(self):
        queue = Queue(1000)
        for i in range(1000):
            queue.put(i)

    def time_get(self):
        queue = Queue()
        for i in range(1000):
            queue.put(i)
        for _ in range(1000):
            queue.get()

    def time_qsize(self):
        queue = Queue()
        for _ in range(1000):
            queue.qsize()
