import numpy as np
import sys
import os

import pytest

import ray

from ray.tests.test_task_metrics import tasks_by_all, METRIC_CONFIG
from ray._private.test_utils import (
    wait_for_condition,
)


# Copied from similar test in test_reconstruction_2.py.
@pytest.mark.skipif(sys.platform == "win32", reason="No multi-node on Windows.")
def test_task_reconstruction(ray_start_cluster):
    cluster = ray_start_cluster

    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        **METRIC_CONFIG,
    )
    info = ray.init(address=cluster.address)

    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def large_object():
        print("RUN LARGE OBJECT")
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        print("RUN DEP TASK")
        return np.zeros(10**7, dtype=np.uint8)

    obj = large_object.remote()
    x = dependent_task.remote(obj)

    expected = {
        ("large_object", "FINISHED", "0"): 1.0,
        ("dependent_task", "FINISHED", "0"): 1.0,
    }
    wait_for_condition(
        lambda: tasks_by_all(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, object_store_memory=10**8)

    # Triggers reconstruction.
    ray.get(x)

    # No failures, yet IsRetry=1.
    expected = {
        ("large_object", "FINISHED", "0"): 1.0,
        ("large_object", "FINISHED", "1"): 1.0,
        ("dependent_task", "FINISHED", "0"): 1.0,
        ("dependent_task", "FINISHED", "1"): 1.0,
    }
    wait_for_condition(
        lambda: tasks_by_all(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
