import pytest
import os
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.test_utils
import ray.cluster_utils


def test_placement_group_pack(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group_id = ray.experimental.placement_group(
        name="name", strategy="PACK", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 == node_of_actor_2


def test_placement_group_strict_pack(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group_id = ray.experimental.placement_group(
        name="name", strategy="STRICT_PACK", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 == node_of_actor_2


def test_placement_group_spread(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group_id = ray.experimental.placement_group(
        name="name", strategy="SPREAD", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 != node_of_actor_2


def test_placement_group_actor_resource_ids(ray_start_cluster):
    @ray.remote(num_cpus=1)
    class F:
        def f(self):
            return ray.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    g1 = ray.experimental.placement_group([{"CPU": 2}])
    a1 = F.options(placement_group_id=g1).remote()
    resources = ray.get(a1.f.remote())
    assert len(resources) == 1, resources
    assert "CPU_group_" in list(resources.keys())[0], resources


def test_placement_group_task_resource_ids(ray_start_cluster):
    @ray.remote(num_cpus=1)
    def f():
        return ray.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    g1 = ray.experimental.placement_group([{"CPU": 2}])
    o1 = f.options(placement_group_id=g1).remote()
    resources = ray.get(o1)
    assert len(resources) == 1, resources
    assert "CPU_group_" in list(resources.keys())[0], resources
    assert "CPU_group_0_" not in list(resources.keys())[0], resources

    # Now retry with a bundle index constraint.
    o1 = f.options(
        placement_group_id=g1, placement_group_bundle_index=0).remote()
    resources = ray.get(o1)
    assert len(resources) == 2, resources
    keys = list(resources.keys())
    assert "CPU_group_" in keys[0], resources
    assert "CPU_group_" in keys[1], resources
    assert "CPU_group_0_" in keys[0] or "CPU_group_0_" in keys[1], resources


def test_placement_group_hang(ray_start_cluster):
    @ray.remote(num_cpus=1)
    def f():
        return ray.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Warm workers up, so that this triggers the hang rice.
    ray.get(f.remote())

    g1 = ray.experimental.placement_group([{"CPU": 2}])
    # This will start out infeasible. The placement group will then be created
    # and it transitions to feasible.
    o1 = f.options(placement_group_id=g1).remote()

    resources = ray.get(o1)
    assert len(resources) == 1, resources
    assert "CPU_group_" in list(resources.keys())[0], resources


def test_placement_group_table(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Originally placement group creation should be pending because
    # there are no resources.
    name = "name"
    strategy = "PACK"
    bundles = [{"CPU": 2, "GPU": 1}, {"CPU": 2}]
    placement_group_id = ray.experimental.placement_group(
        name=name, strategy=strategy, bundles=bundles)
    result = ray.experimental.placement_group_table(placement_group_id)
    assert result["name"] == name
    assert result["strategy"] == strategy
    for i in range(len(bundles)):
        assert bundles[i] == result["bundles"][i]
    assert result["state"] == "PENDING"

    # Now the placement group should be scheduled.
    cluster.add_node(num_cpus=5, num_gpus=1)
    cluster.wait_for_nodes()
    actor_1 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=0).remote()
    ray.get(actor_1.value.remote())

    result = ray.experimental.placement_group_table(placement_group_id)
    assert result["state"] == "ALIVE"


def test_cuda_visible_devices(ray_start_cluster):
    @ray.remote(num_gpus=1)
    def f():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_gpus=1)
    ray.init(address=cluster.address)

    g1 = ray.experimental.placement_group([{"CPU": 1, "GPU": 1}])
    o1 = f.options(placement_group_id=g1).remote()

    devices = ray.get(o1)
    assert devices == "0", devices


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
