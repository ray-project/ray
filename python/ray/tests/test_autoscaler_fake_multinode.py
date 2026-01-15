import platform
import sys
import time

import pytest

import ray
from ray.cluster_utils import AutoscalingCluster


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_fake_autoscaler_basic_e2e(autoscaler_v2, shutdown_only):
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
            "tpu_v5e_node": {
                "resources": {
                    "CPU": 4,
                    "TPU": 8,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
            "tpu_v6e_node": {
                "resources": {
                    "CPU": 4,
                    "TPU": 8,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        autoscaler_v2=autoscaler_v2,
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

        # Triggers the addition of a TPU node.
        @ray.remote(resources={"TPU": 4})
        def h():
            print("tpu ok")

        # Triggers the addition of a 8-chip TPU node.
        @ray.remote(resources={"TPU": 8})
        def i():
            print("8-chip tpu ok")

        ray.get(f.remote())
        ray.get(g.remote())
        ray.get(h.remote())
        ray.get(i.remote())
        ray.shutdown()
    finally:
        cluster.shutdown()
    # __example_end__


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_zero_cpu_default_actor(autoscaler_v2):
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
        autoscaler_v2=autoscaler_v2,
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


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_autoscaler_cpu_task_gpu_node_up(autoscaler_v2):
    """Validates that CPU tasks can trigger GPU upscaling.
    See https://github.com/ray-project/ray/pull/31202.
    """
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "gpu_node_type": {
                "resources": {
                    "CPU": 1,
                    "GPU": 1,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 1,
            },
        },
        autoscaler_v2=autoscaler_v2,
    )

    try:
        cluster.start()
        ray.init("auto")

        @ray.remote(num_cpus=1)
        def task():
            return True

        # Make sure the task can be scheduled.
        # Since the head has 0 CPUs, this requires upscaling a GPU worker.
        ray.get(task.remote(), timeout=30)
        ray.shutdown()

    finally:
        cluster.shutdown()


@pytest.fixture
def setup_cluster(request):
    autoscaler_v2 = request.param
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 5,
            },
        },
        idle_timeout_minutes=0.1,
        autoscaler_v2=autoscaler_v2,
    )
    try:
        cluster.start()
        ray.init("auto")
        yield cluster
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize(
    "setup_cluster", [False, True], ids=["v1", "v2"], indirect=True
)
def test_autoscaler_not_kill_blocking_node(setup_cluster):
    """Tests that the autoscaler does not kill a node that
    has worker in blocking state."""

    @ray.remote(num_cpus=1)
    def short_task():
        time.sleep(5)

    @ray.remote(num_cpus=1)
    def long_task():
        time.sleep(20)

    @ray.remote(num_cpus=1)
    def f():
        future_list = [short_task.remote(), long_task.remote()]
        ray.get(future_list)

    ray.get(f.remote(), timeout=30)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
