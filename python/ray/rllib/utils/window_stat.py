from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.utils.filter import RunningStat


class WindowStats(object):
    def __init__(self, name, n):
        self.name = name
        self.items = [None] * n
        self.idx = 0
        self.count = 0
        self.running = RunningStat(())

    def push(self, obj):
        self.items[self.idx] = obj
        self.idx += 1
        self.count += 1
        self.idx %= len(self.items)
        self.running.push(obj)

    def stats(self):
        if not self.count:
            quantiles = None
        else:
            quantiles = str(np.percentile(
                self.items[:self.count], [0, 10, 50, 90, 100]).tolist())
        return {
            self.name + "_count": int(self.running.n),
            self.name + "_mean": float(self.running.mean.tolist()),
            self.name + "_std": float(self.running.std.tolist()),
            self.name + "_quantiles": quantiles,
        }
