import pytest
import platform
import numpy as np

import ray
from ray._private.test_utils import wait_for_condition
from ray.cluster_utils import AutoscalingCluster


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_fake_autoscaler_basic_e2e(shutdown_only):
    # __example_begin__
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 4,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
            "gpu_node": {
                "resources": {
                    "CPU": 2,
                    "GPU": 1,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
    )

    try:
        cluster.start()
        ray.init("auto")

        # Triggers the addition of a GPU node.
        @ray.remote(num_gpus=1)
        def f():
            print("gpu ok")

        # Triggers the addition of a CPU node.
        @ray.remote(num_cpus=3)
        def g():
            print("cpu ok")

        ray.get(f.remote())
        ray.get(g.remote())
        ray.shutdown()
    finally:
        cluster.shutdown()
    # __example_end__


# Tests that we scale down even if secondary copies of objects are present on
# idle nodes: https://github.com/ray-project/ray/issues/21870
@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_scaledown_shared_objects(shutdown_only):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 1},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 1,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 4,
            },
        },
        idle_timeout_minutes=0.05,
    )

    try:
        cluster.start()
        ray.init("auto")

        # Triggers the addition of a GPU node.
        @ray.remote(num_cpus=1)
        class Actor:
            def f(self):
                pass

            def recv(self, obj):
                pass

        actors = [Actor.remote() for _ in range(5)]
        ray.get([a.f.remote() for a in actors])
        print("All five nodes launched")

        # Verify scale-up.
        wait_for_condition(lambda: ray.cluster_resources().get("CPU", 0) == 5)

        data = ray.put(np.zeros(1024 * 1024 * 100))
        ray.get([a.recv.remote(data) for a in actors])
        print("Data broadcast successfully, deleting actors.")
        del actors

        # Verify scale-down.
        wait_for_condition(
            lambda: ray.cluster_resources().get("CPU", 0) == 1, timeout=30
        )
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
