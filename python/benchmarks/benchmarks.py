from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import ray
import ray.test.test_functions as test_functions

def setup():
    if os.getenv("RAY_ASV_USE_RAYLET", 0):
        ray.init(num_workers=3, use_raylet=True)
    else:
        ray.init(num_workers=3)

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
