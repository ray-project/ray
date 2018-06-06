from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import ray.test.test_functions as test_functions


def setup():
    if not hasattr(setup, "is_initialized"):
        ray.init(num_workers=4, num_cpus=4)
        setup.is_initialized = True


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
        test_functions.empty_function.remote()

    def time_submit_and_get(self):
        x = test_functions.trivial_function.remote()
        ray.get(x)

    def time_put(self):
        ray.put(1)
