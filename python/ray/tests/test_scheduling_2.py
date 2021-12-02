import numpy as np
import platform
import pytest
import sys
import time

import ray
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows. Multi node.")
def test_load_balancing_under_constrained_memory(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 4
    object_size = 4e7
    num_tasks = 100
    for _ in range(num_nodes):
        cluster.add_node(
            num_cpus=num_cpus,
            memory=(num_cpus - 2) * object_size,
            object_store_memory=(num_cpus - 2) * object_size)
    cluster.add_node(
        num_cpus=0,
        resources={"custom": 1},
        memory=(num_tasks + 1) * object_size,
        object_store_memory=(num_tasks + 1) * object_size)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0, resources={"custom": 1})
    def create_object():
        return np.zeros(int(object_size), dtype=np.uint8)

    @ray.remote
    def f(i, x):
        print(i, ray.worker.global_worker.node.unique_id)
        time.sleep(0.1)
        return ray.worker.global_worker.node.unique_id

    deps = [create_object.remote() for _ in range(num_tasks)]
    for i, dep in enumerate(deps):
        print(i, dep)

    # TODO(swang): Actually test load balancing. Load balancing is currently
    # flaky on Travis, probably due to the scheduling policy ping-ponging
    # waiting tasks.
    deps = [create_object.remote() for _ in range(num_tasks)]
    tasks = [f.remote(i, dep) for i, dep in enumerate(deps)]
    for i, dep in enumerate(deps):
        print(i, dep)
    ray.get(tasks)


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_placement_group_strategy(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8, resources={"head": 1})
    cluster.add_node(num_cpus=8, num_gpus=8, resources={"worker": 1})
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)
    pg = ray.util.placement_group(bundles=[{
        "CPU": 1,
        "GPU": 1
    }, {
        "CPU": 1,
        "GPU": 1
    }])
    ray.get(pg.ready())

    with connect_to_client_or_not(connect_to_client):

        @ray.remote
        def get_node_id_1():
            return ray.worker.global_worker.current_node_id

        worker_node_id = ray.get(
            get_node_id_1.options(resources={
                "worker": 1
            }).remote())

        assert ray.get(
            get_node_id_1.options(
                num_cpus=1,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg)).remote()) == worker_node_id

        @ray.remote(
            num_cpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg))
        def get_node_id_2():
            return ray.worker.global_worker.current_node_id

        assert ray.get(get_node_id_2.remote()) == worker_node_id

        @ray.remote(
            num_cpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg))
        class Actor1():
            def get_node_id(self):
                return ray.worker.global_worker.current_node_id

        actor1 = Actor1.remote()
        assert ray.get(actor1.get_node_id.remote()) == worker_node_id

        @ray.remote
        class Actor2():
            def get_node_id(self):
                return ray.worker.global_worker.current_node_id

        actor2 = Actor2.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg)).remote()
        assert ray.get(actor2.get_node_id.remote()) == worker_node_id

    with pytest.raises(ValueError):

        @ray.remote(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg))
        def func():
            return 0

        func.options(placement_group=pg).remote()

    with pytest.raises(ValueError):

        @ray.remote
        def func():
            return 0

        func.options(scheduling_strategy="XXX").remote()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
