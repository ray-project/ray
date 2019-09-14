from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


class WindowStat(object):
    def __init__(self, name, n):
        self.name = name
        self.items = [None] * n
        self.idx = 0
        self.count = 0

    def push(self, obj):
        self.items[self.idx] = obj
        self.idx += 1
        self.count += 1
        self.idx %= len(self.items)

    def stats(self):
        if not self.count:
            quantiles = []
        else:
            quantiles = np.percentile(self.items[:self.count],
                                      [0, 10, 50, 90, 100]).tolist()
        return {
            self.name + "_count": int(self.count),
            self.name + "_mean": float(np.mean(self.items[:self.count])),
            self.name + "_std": float(np.std(self.items[:self.count])),
            self.name + "_quantiles": quantiles,
        }
