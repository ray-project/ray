from collections import defaultdict
import os
import logging
import tracemalloc


import psutil
import ray
import numpy as np
import matplotlib.pyplot as plt
import objgraph
 
# list to store memory snapshots
snaps = []

logging.basicConfig(level=logging.INFO)

@ray.remote
class Actor:
    def __init__(self):
        self.index = 0
        self.process = psutil.Process(os.getpid())
        self.baseline = self.process.memory_full_info().rss / 1024**2
        self.baseline_uss = self.process.memory_full_info().uss / 1024**2
        self.something_to_store = None

    def set_something(self, something_to_store):
        self.something_to_store = something_to_store

    def compute(self):
        if self.index % 100 == 0:
            print(f'memory @{self.index} rss {self.process.memory_full_info().rss / 1024**2 - self.baseline} uss {self.process.memory_full_info().uss / 1024**2 - self.baseline_uss}')
        self.index = self.index + 1
        return 1

def simulate_call(workers, pass_object):
    if pass_object:
        ray.get([w.set_something.remote(something_to_store) for w in workers])
    results = ray.get([w.compute.remote() for w in workers])
    return results


if __name__ == "__main__":
    results = defaultdict(list)
    process = psutil.Process(os.getpid())
    num_samples = 200000
    scale = 1024**2
    something_to_store = {"data": np.random.random((100000, 2)).tolist()}
    
    num_cpu = 1
    local_mode = False
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=num_cpu, num_gpus=0, include_dashboard=False, local_mode=local_mode)
    
    workers = [Actor.remote() for _ in range(num_cpu)]
    baseline = process.memory_full_info().rss / scale
    baseline_uss = process.memory_full_info().uss / scale
    
    for v in range(num_samples):
        simulate_call(workers, True)
        if v % 100 == 0:
            print(f'memory @{v} rss {process.memory_full_info().rss / scale - baseline} uss {process.memory_full_info().uss / scale - baseline_uss}')
        
    print("done")