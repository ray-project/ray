"""This is the script for `ray clusterbenchmark`."""

import os
import time
import numpy as np
import multiprocessing
import ray

from ray.tests.cluster_utils import Cluster

# Only run tests matching this filter pattern.
filter_pattern = os.environ.get("TESTS_TO_RUN", "")

def main():
    print("Tip: set TESTS_TO_RUN='pattern' to run a subset of benchmarks")
    cluster = Cluster(initialize_head=True, connect=True, head_node_args={"object_store_memory":20*1024*1024*1024, "num_cpus":16})
    client_node = cluster.add_node(object_store_memory=20*1024*1024*1024, num_gpus=1, num_cpus=16)

    object_id_list = []
    for i in range(0,10):
        object_id = ray.put(np.random.rand(1024*128, 1024))
        object_id_list.append(object_id)

    @ray.remote(num_gpus=1)
    def f(object_id_list):
        diffs = []
        for object_id in object_id_list:
            before = time.time()
            x = ray.get(object_id)
            after = time.time()
            diffs.append(after - before)
            time.sleep(1)
        return np.mean(diffs), np.std(diffs)

    time.sleep(20)
    time_diff, time_diff_std = ray.get(f.remote(object_id_list))

    print ("latency to get an 1G object over network", round(time_diff, 2), "+-", round(time_diff_std, 2))

    ray.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
