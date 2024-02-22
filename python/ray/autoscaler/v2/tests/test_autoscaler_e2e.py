import os
import sys
import pytest

from ray.cluster_utils import AutoscalingCluster
import ray


def test_basic():
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 4},
                "node_config": {},
                "min_workers": 1,
                "max_workers": 30,
            },
        },
        autoscaler_v2=False,
        idle_timeout_minutes=999,
    )
    cluster.start()
    ray.init("auto")

    @ray.remote(num_cpus=4)
    def task():
        import time

        time.sleep(10)

    ray.get(task.remote())

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
