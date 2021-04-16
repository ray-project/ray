from collections import defaultdict

import pandas as pd
import numpy as np
import cupy as cp

from ray.util.sgd.utils import TimerStat, TimerCollection, AverageMeterCollection

import time
from functools import wraps


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


class EmptyTimeState:
    def __enter__(self):
        pass
    def __exit__(self, type, value, tb):
        pass


class ThroughoutCollection(TimerCollection):
    def __init__(self, batch_size, num_workers=1, save_freq=50, job_name="default"):
        self.batch_size = batch_size * num_workers
        self.save_freq = save_freq
        self.job_name = job_name
        super(ThroughoutCollection, self).__init__()
        def defaultdict_list():
            return defaultdict(list)
        self.result_collection = defaultdict(defaultdict_list)
        self.key_steps = defaultdict(int)

    def record(self, key):
        if self._enabled:
            self.key_steps[key] += 1
            if self.key_steps[key]%self.save_freq == 0:
                self.save(key)
            return self._timers[key]
        else:
            return EmptyTimeState()

    def update(self, key, **kwargs):
        # call before this key record start.
        for k,v in kwargs.items():
            self.result_collection[key][k].append(v)

    def report(self, key):
        aggregates = {}
        k, t = key, self._timers[key]
        aggregates[f"count_{k}"] = t.count+1
        aggregates[f"mean_{k}_s"] = t.mean
        aggregates[f"last_{k}_s"] = t.last
        aggregates[f"total_{k}_s"] = t._total_time
        aggregates[f"pass_data_{k}"] = aggregates[f"count_{k}"]*self.batch_size
        aggregates[f"throughout_{k}_d"] = aggregates[f"pass_data_{k}"]/t._total_time
        for metric in aggregates.keys():
            self.result_collection[k][metric].append(aggregates[metric])
        return aggregates

    def save(self, key):
        self.report(key)
        df = pd.DataFrame.from_dict(self.result_collection[key], orient='columns')
        df.to_csv(f'{self.job_name}_{key}.csv', index=None)


def func_timer(function):
    '''A decorator to record time.'''
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print('[Function: {name} finished, spent time: {time:.5f}s]'.format(name = function.__name__,time = t1 - t0))
        return result
    return function_timer