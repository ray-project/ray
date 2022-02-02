"""This is the script for `ray clusterbenchmark`."""

import time
import numpy as np
import ray

from ray.cluster_utils import Cluster


def main():
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={"object_store_memory": 20 * 1024 * 1024 * 1024, "num_cpus": 16},
    )
    cluster.add_node(
        object_store_memory=20 * 1024 * 1024 * 1024, num_gpus=1, num_cpus=16
    )

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

    print(
        "latency to get an 1G object over network",
        round(time_diff, 2),
        "+-",
        round(time_diff_std, 2),
    )

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
