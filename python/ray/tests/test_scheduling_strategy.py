import sys
import time
import pytest

import ray
import ray.cluster_utils
import ray.experimental.internal_kv as internal_kv
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_placement_group_strategy(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8, resources={"head": 1})
    cluster.add_node(num_cpus=8, num_gpus=8, resources={"worker": 1})
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)
    pg = ray.util.placement_group(bundles=[{"CPU": 1, "GPU": 1}])
    ray.get(pg.ready())

    with connect_to_client_or_not(connect_to_client):

        @ray.remote
        def get_node_id():
            return ray.worker.global_worker.current_node_id

        worker_node_id = ray.get(
            get_node_id.options(resources={
                "worker": 1
            }).remote())

        assert ray.get(
            get_node_id.options(
                num_cpus=1,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg)).remote()) == worker_node_id

    with pytest.raises(ValueError):

        @ray.remote(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg))
        def func():
            return 0

        func.options(placement_group=pg).remote()


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_spread_scheduling_strategy(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    # Create a head node
    cluster.add_node(
        num_cpus=0, _system_config={
            "scheduler_spread_threshold": 1,
        })
    for i in range(2):
        cluster.add_node(num_cpus=8, resources={f"foo:{i}": 1})
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):

        @ray.remote
        def get_node_id():
            return ray.worker.global_worker.current_node_id

        worker_node_ids = {
            ray.get(get_node_id.options(resources={
                f"foo:{i}": 1
            }).remote())
            for i in range(2)
        }
        # Wait for updating driver raylet's resource view.
        time.sleep(5)

        @ray.remote(scheduling_strategy="SPREAD")
        def task1():
            internal_kv._internal_kv_put("test_task1", "task1")
            while internal_kv._internal_kv_exists("test_task1"):
                time.sleep(0.1)
            return ray.worker.global_worker.current_node_id

        @ray.remote
        def task2():
            internal_kv._internal_kv_put("test_task2", "task2")
            return ray.worker.global_worker.current_node_id

        locations = []
        locations.append(task1.remote())
        while not internal_kv._internal_kv_exists("test_task1"):
            time.sleep(0.1)
        # Wait for updating driver raylet's resource view.
        time.sleep(5)
        locations.append(task2.options(scheduling_strategy="SPREAD").remote())
        while not internal_kv._internal_kv_exists("test_task2"):
            time.sleep(0.1)
        internal_kv._internal_kv_del("test_task1")
        internal_kv._internal_kv_del("test_task2")
        assert set(ray.get(locations)) == worker_node_ids

        # Wait for updating driver raylet's resource view.
        time.sleep(5)

        @ray.remote(scheduling_strategy="SPREAD", num_cpus=1)
        class Actor1():
            def get_node_id(self):
                return ray.worker.global_worker.current_node_id

        @ray.remote(num_cpus=1)
        class Actor2():
            def get_node_id(self):
                return ray.worker.global_worker.current_node_id

        locations = []
        actor1 = Actor1.remote()
        locations.append(ray.get(actor1.get_node_id.remote()))
        # Wait for updating driver raylet's resource view.
        time.sleep(5)
        actor2 = Actor2.options(scheduling_strategy="SPREAD").remote()
        locations.append(ray.get(actor2.get_node_id.remote()))
        assert set(locations) == worker_node_ids

    with pytest.raises(TypeError):
        task1.options(scheduling_strategy="XXXX").remote()
    with pytest.raises(TypeError):
        Actor2.options(scheduling_strategy="XXXX").remote()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
