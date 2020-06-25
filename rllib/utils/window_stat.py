import numpy as np


class WindowStat:
    def __init__(self, name, n, count):
        self.name = name
        self.items = [None] * n
        self.idx = 0
        self.count = count

    def push(self, obj):
        self.items[self.idx] = obj
        self.idx += 1
        self.count += 1
        self.idx %= len(self.items)

    def stats(self):
        #Should I just try except all of these instead
        if np.isnan(self.count) or not self.count:
            _count = np.nan
        else:
            _count = int(self.count)

        if np.isnan(self.count) or not self.count or \
                np.isnan(self.items[:self.count]).any():
            _mean = np.nan
            _var = np.nan
            _quantiles = []
        else:
            _mean = float(np.mean(self.items[:self.count]))
            _var = float(np.std(self.items[:self.count]))
            _quantiles = np.percentile(self.items[:self.count],
                                       [0, 10, 50, 90, 100]).tolist()

        return {
            self.name + "_count": _count,
            self.name + "_mean": _mean,
            self.name + "_std": _var,
            self.name + "_quantiles": _quantiles,
        }
