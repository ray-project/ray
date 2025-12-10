import time

import ray

from benchmark import Benchmark
from cluster_resource_monitor import ClusterResourceMonitor


def main():
    """This tests check if the cluster doesn't scale up more than necessary."""
    ray.init()

    def sleep_task(row):
        # Only sleep for the first task so that some tasks finish.
        if row["id"] == 0:
            # By sleeping for 5 mins, we're able to test whether the autoscaler scales
            # up unnecessarily. This pipeline can use at most 2 CPUs, so the peak num of
            # CPUs should be at most 8 CPUs (1 node).
            time.sleep(300)

        return row

    with ClusterResourceMonitor() as monitor:
        ray.data.range(2, override_num_blocks=2).map(sleep_task).materialize()

        peak_resources = monitor.get_peak_cluster_resources()
        assert peak_resources.cpu == 8, f"Expected 8 CPUs, got {peak_resources.cpu}"
        assert peak_resources.gpu == 0, f"Expected 0 GPUs, got {peak_resources.gpu}"


if __name__ == "__main__":
    benchmark = Benchmark()
    benchmark.run_fn("main", main)
    benchmark.write_result()
