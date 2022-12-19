import copy
import pytest
import platform

import ray
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


def test_zero_cpu_default_actor():
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 1,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 1,
            },
        },
    )

    try:
        cluster.start()
        ray.init("auto")

        @ray.remote
        class Actor:
            def ping(self):
                pass

        actor = Actor.remote()
        ray.get(actor.ping.remote())
        ray.shutdown()
    finally:
        cluster.shutdown()


def test_autoscaler_all_gpu_node_types():
    """Validates that CPU tasks still trigger upscaling
    when all available non-head node types have GPUs.
    """
    gpu_node_type_1 = (
        {
            "resources": {
                "CPU": 1,
                "GPU": 1,
            },
            "node_config": {},
            "min_workers": 0,
            "max_workers": 1,
        },
    )
    gpu_node_type_2 = copy.deepcopy(gpu_node_type_1)

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "gpu_node_type_1": gpu_node_type_1,
            "gpu_node_type_2": gpu_node_type_2,
        },
    )

    try:
        cluster.start()
        ray.init("auto")

        @ray.remote(num_cpus=1)
        def task():
            return True

        assert ray.get(task.remote(), timeout=60), "Failed to schedule CPU task."
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
