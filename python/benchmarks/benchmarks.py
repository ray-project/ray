from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


def setup():
    if not hasattr(setup, "is_initialized"):
        ray.init(num_cpus=4)
        setup.is_initialized = True


@ray.remote
def trivial_function():
    return 1


class TimeSuite(object):
    """An example benchmark."""

    def setup(self):
        self.d = {}
        for x in range(500):
            self.d[x] = None

    def time_keys(self):
        for key in self.d.keys():
            pass

    def time_range(self):
        d = self.d
        for key in range(500):
            d[key]


class MemSuite(object):
    def mem_list(self):
        return [0] * 256


class MicroBenchmarkSuite(object):
    def time_submit(self):
        trivial_function.remote()

    def time_submit_and_get(self):
        x = trivial_function.remote()
        ray.get(x)

    def time_put(self):
        ray.put(1)
