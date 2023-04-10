import os
import signal
import sys

import numpy as np
import pytest

import ray

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


@pytest.fixture
def config(request):
    config = {
        "health_check_initial_delay_ms": 0,
        "health_check_period_ms": 100,
        "health_check_failure_threshold": 10,
        "object_timeout_milliseconds": 200,
    }
    yield config


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_reconstruction_stress(config, ray_start_cluster):
    config["task_retry_delay_ms"] = 100
    config["max_direct_call_object_size"] = 100
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0, _system_config=config, enable_object_reconstruction=True
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def large_object():
        return np.zeros(10**5, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return

    for _ in range(3):
        obj = large_object.options(resources={"node1": 1}).remote()
        ray.get(dependent_task.options(resources={"node2": 1}).remote(obj))

        outputs = [
            large_object.options(resources={"node1": 1}).remote() for _ in range(1000)
        ]
        outputs = [
            dependent_task.options(resources={"node2": 1}).remote(obj)
            for obj in outputs
        ]

        cluster.remove_node(node_to_kill, allow_graceful=False)
        node_to_kill = cluster.add_node(
            num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
        )

        i = 0
        while outputs:
            ray.get(outputs.pop(0))
            print(i)
            i += 1


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
