"""A set of basic TPU tests on the fake autoscaler."""
import pytest
import platform

import ray
from ray.cluster_utils import AutoscalingCluster


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_fake_autoscaler_basic_e2e(shutdown_only):
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
            "tpu_node": {
                "resources": {
                    "CPU": 2,
                    "TPU": 4,
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

        # Triggers the addition of a TPU node.
        @ray.remote(num_tpus=1)
        def f():
            print("tpu ok")

        # Triggers the addition of a CPU node.
        @ray.remote(num_cpus=3)
        def g():
            print("cpu ok")

        #ray.get(f.remote())
        ray.get(g.remote())
        ray.shutdown()
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
