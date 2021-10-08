import pytest

import ray
from ray.cluster_utils import AutoscalingCluster


def test_fake_autoscaler_basic_e2e(shutdown_only):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 4},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 2,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 4,
            },
            "gpu_node": {
                "resources": {
                    "CPU": 4,
                    "GPU": 1,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        })

    try:
        cluster.start()
        ray.init("auto")

        @ray.remote(num_gpus=1)
        def f():
            print("gpu ok")

        ray.get(f.remote())
        ray.shutdown()
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
