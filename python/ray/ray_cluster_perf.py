"""This is the script for `ray clusterbenchmark`."""

import time
import numpy as np
import os
import ray

from ray.cluster_utils import Cluster
from ray.ray_perf import check_optimized_build, large_arg_and_value

# Only run tests matching this filter pattern.
filter_pattern = os.environ.get("TESTS_TO_RUN", "")


def timeit_latency(name, fn):
    if filter_pattern not in name:
        return
    # warmup
    start = time.time()
    while time.time() - start < 1:
        fn()
    # real run
    stats = []
    for _ in range(4):
        start = time.time()
        fn()
        end = time.time()
        stats.append(end - start)
    print(name, "mean latency", round(np.mean(stats), 2), "+-",
          round(np.std(stats), 2))


def main():
    check_optimized_build()

    print("Tip: set TESTS_TO_RUN='pattern' to run a subset of benchmarks")

    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "object_store_memory": 20 * 1024 * 1024 * 1024,
            "num_cpus": 16,
        })

    cluster.add_node(
        object_store_memory=20 * 1024 * 1024 * 1024, num_gpus=1, num_cpus=16)

    cluster.add_node(
        object_store_memory=20 * 1024 * 1024 * 1024, num_gpus=1, num_cpus=16)

    object_ref_list = []
    for i in range(0, 10):
        object_ref = ray.put(np.random.rand(1024 * 128, 1024))
        object_ref_list.append(object_ref)

    @ray.remote(num_gpus=1)
    def f(object_ref_list):
        diffs = []
        for object_ref in object_ref_list:
            before = time.time()
            ray.get(object_ref)
            after = time.time()
            diffs.append(after - before)
            time.sleep(1)
        return np.mean(diffs), np.std(diffs)

    time_diff, time_diff_std = ray.get(f.remote(object_ref_list))

    print("latency to get an 1G object over network", round(time_diff, 2),
          "+-", round(time_diff_std, 2))

    arr1 = np.ones((40, 1024, 1024))
    arr2 = np.ones((40, 1024, 1024))

    def data_intensive_task():
        ray.get(large_arg_and_value.remote(arr1, arr2))

    timeit_latency("single client data-intensive tasks sync",
                   data_intensive_task)

    def data_intensive_task_async():
        ray.get([large_arg_and_value.remote(arr1, arr2) for _ in range(10)])

    timeit_latency("single client data-intensive tasks async",
                   data_intensive_task_async)

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
