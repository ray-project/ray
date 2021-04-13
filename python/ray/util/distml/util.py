import numpy as np
import cupy as cp

from ray.util.sgd.utils import TimerStat, TimerCollection, AverageMeterCollection


# some operation for this ml system.
def ones(shape, cpu=True):
    if cpu:
        return np.ones(shape)
    else:
        return cp.ones(shape)

def zeros_like(x, cpu=True):
    if cpu:
        return np.ones_like(x)
    else:
        return cp.ones_like(x)

# some operation for this ml system.
def zeros(shape, cpu=True):
    if cpu:
        return np.zeros(shape)
    else:
        return cp.zeros(shape)

def zeros_like(x, cpu=True):
    if cpu:
        return np.zeros_like(x)
    else:
        return cp.zeros_like(x)

def numel(v):
    return np.size(v)


class ThroughoutCollection(TimerCollection):
    def __init__(self, batch_size):
        self.batch_size = batch_size
        super(TimerCollection, self).__init__()

    def report(self, name):
        aggregates = {}

        k, t = name, self._timers[name]

        aggregates[f"mean_{k}_s"] = t.mean
        aggregates[f"last_{k}_s"] = t.last
        aggregates[f"total_{k}_s"] = t._total_time
        aggregates[f"pass_data_{k}"] = t.count*self.batch_size
        aggregates[f"throughout_{k}_d"] = t._total_time/(t.count*self.batch_size)

        return aggregates