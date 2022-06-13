import time

import pytest
import ray

def test_task_summary(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=2)

    @ray.remote
    def run_long_time()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))