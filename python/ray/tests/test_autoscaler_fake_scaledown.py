import pytest
import platform
import numpy as np

import ray
from ray._private.test_utils import wait_for_condition
from ray.cluster_utils import AutoscalingCluster


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
                    "object_store_memory": 100 * 1024 * 1024,
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

        data = ray.put(np.zeros(1024 * 1024 * 5))
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
