import time

import ray

from benchmark import Benchmark
from cluster_resource_monitor import ClusterResourceMonitor


def main():
    """This tests check if the cluster doesn't scale up more than necessary."""
    ray.init()

    def sleep_task(row):
        time.sleep(1)
        return row

    with ClusterResourceMonitor() as monitor:
        ray.data.range(1024, override_num_blocks=1024, concurrency=1).map(
            sleep_task
        ).materialize()

        peak_resources = monitor.get_peak_cluster_resources()
        # There are 8 CPUs on a single node. The autoscaler shouldn't provision more
        # than one node.
        assert peak_resources.cpu == 8, f"Expected 8 CPUs, got {peak_resources.cpu}"
        assert peak_resources.gpu == 0, f"Expected 0 GPUs, got {peak_resources.gpu}"


if __name__ == "__main__":
    benchmark = Benchmark()
    benchmark.run_fn("main", main)
    benchmark.write_result()
