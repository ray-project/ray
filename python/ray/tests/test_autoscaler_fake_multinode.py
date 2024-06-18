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
            "acc_node": {
                "resources": {
                    "CPU": 2,
                    "ACC": 1,
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

        # Triggers the addition of a ACC node.
        @ray.remote(num_accs=1)
        def f():
            print("acc ok")

        # Triggers the addition of a CPU node.
        @ray.remote(num_cpus=3)
        def g():
            print("cpu ok")

        # Triggers the addition of a TPU node.
        @ray.remote(resources={"TPU": 4})
        def h():
            print("tpu ok")

        ray.get(f.remote())
        ray.get(g.remote())
        ray.get(h.remote())
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


def test_autoscaler_cpu_task_acc_node_up():
    """Validates that CPU tasks can trigger ACC upscaling.
    See https://github.com/ray-project/ray/pull/31202.
    """
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "acc_node_type": {
                "resources": {
                    "CPU": 1,
                    "ACC": 1,
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

        @ray.remote(num_cpus=1)
        def task():
            return True

        # Make sure the task can be scheduled.
        # Since the head has 0 CPUs, this requires upscaling a ACC worker.
        ray.get(task.remote(), timeout=30)
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
