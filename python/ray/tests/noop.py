
from collections import defaultdict
import os
import logging


import psutil
import ray
import numpy as np
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)


@ray.remote
class Actor:
    def __init__(self):
        self.something_to_store = None

    def set_something(self, something_to_store):
        self.something_to_store = something_to_store

    def compute(self):
        return 1


def simulate_call(workers, pass_object):
    something_to_store = {"data": np.random.random((100000, 2)).tolist()}
    if pass_object:
        ray.get([w.set_something.remote(something_to_store) for w in workers])
    results = ray.get([w.compute.remote() for w in workers])
    return results


if __name__ == "__main__":

    print(f"Starting memory test with versio {ray.__version__}")
    results = defaultdict(list)
    process = psutil.Process(os.getpid())
    num_samples = 1000
    scale = 1024**2
    
    # This is just iterating through parameters so I can check the memory issue is consistent
    for num_cpu in [1]:
        for local_mode in [True]:
            if ray.is_initialized():
                ray.shutdown()
            ray.init(num_cpus=num_cpu, num_gpus=0, include_dashboard=False, local_mode=local_mode)
            for pass_object in [True, False]:
                workers = [Actor.remote() for _ in range(num_cpu)]
                baseline = process.memory_info().rss / scale
                key = f"{pass_object=} {num_cpu=} {local_mode=}"
                print(key)
                while len(results[key]) < num_samples:
                    simulate_call(workers, pass_object)
                    results[key].append(process.memory_info().rss / scale - baseline)

    print(results)
    # Plotting out the memory usage as a function of iteration
    fig, axes = plt.subplots(nrows=2, sharex=True)
    for k, v in results.items():
        p = "pass_object=True" in k
        l = "local_mode=True" in k
        ax = axes[0] if l else axes[1]
        ax.plot(v, label=k, ls="--" if p else "-")
        ax.text(num_samples * 1.05, v[-1], k)
    axes[1].set_xlabel("Iteration")
    for ax in axes:
        ax.set_ylabel("Delta Memory usage (MB)")
        ax.set_xscale("log")
    fig.savefig("memory_usage.png", dpi=300, bbox_inches="tight")