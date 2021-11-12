import numpy as np


class WindowStat:
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
            _quantiles = []
        else:
            _quantiles = np.nanpercentile(self.items[:self.count],
                                          [0, 10, 50, 90, 100]).tolist()
        return {
            self.name + "_count": int(self.count),
            self.name + "_mean": float(np.nanmean(self.items[:self.count])),
            self.name + "_std": float(np.nanstd(self.items[:self.count])),
            self.name + "_quantiles": _quantiles,
        }
