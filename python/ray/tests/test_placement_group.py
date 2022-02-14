import pytest
import sys

import ray
import ray.cluster_utils
from ray._private.test_utils import (
    placement_group_assert_no_leak,
)
from ray.util.client.ray_client_helpers import connect_to_client_or_not


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_placement_ready(ray_start_regular, connect_to_client):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def v(self):
            return 10

    # bundle is placement group reserved resources and can't be used in bundles
    with pytest.raises(Exception):
        ray.util.placement_group(bundles=[{"bundle": 1}])
    # This test is to test the case that even there all resource in the
    # bundle got allocated, we are still able to return from ready[I
    # since ready use 0 CPU
    with connect_to_client_or_not(connect_to_client):
        pg = ray.util.placement_group(bundles=[{"CPU": 1}])
        ray.get(pg.ready())
        a = Actor.options(num_cpus=1, placement_group=pg).remote()
        ray.get(a.v.remote())
        ray.get(pg.ready())

        placement_group_assert_no_leak([pg])


def test_placement_group_invalid_resource_request(shutdown_only):
    """
    Make sure exceptions are raised if
    requested resources don't fit any bundles.
    """
    ray.init(resources={"a": 1})
    pg = ray.util.placement_group(bundles=[{"a": 1}])

    #
    # Test an actor with 0 cpu.
    #
    @ray.remote
    class A:
        def ready(self):
            pass

    # The actor cannot be scheduled with the default because
    # it requires 1 cpu for the placement, but the pg doesn't have it.
    with pytest.raises(ValueError):
        a = A.options(placement_group=pg).remote()
    # Shouldn't work with 1 CPU because pg doesn't contain CPUs.
    with pytest.raises(ValueError):
        a = A.options(num_cpus=1, placement_group=pg).remote()
    # 0 CPU should work.
    a = A.options(num_cpus=0, placement_group=pg).remote()
    ray.get(a.ready.remote())
    del a

    #
    # Test an actor with non-0 resources.
    #
    @ray.remote(resources={"a": 1})
    class B:
        def ready(self):
            pass

    # When resources are given to the placement group,
    # it automatically adds 1 CPU to resources, so it should fail.
    with pytest.raises(ValueError):
        b = B.options(placement_group=pg).remote()
    # If 0 cpu is given, it should work.
    b = B.options(num_cpus=0, placement_group=pg).remote()
    ray.get(b.ready.remote())
    del b
    # If resources are requested too much, it shouldn't work.
    with pytest.raises(ValueError):
        # The actor cannot be scheduled with no resource specified.
        # Note that the default actor has 0 cpu.
        B.options(num_cpus=0, resources={"a": 2}, placement_group=pg).remote()

    #
    # Test a function with 1 CPU.
    #
    @ray.remote
    def f():
        pass

    # 1 CPU shouldn't work because the pg doesn't have CPU bundles.
    with pytest.raises(ValueError):
        f.options(placement_group=pg).remote()
    # 0 CPU should work.
    ray.get(f.options(placement_group=pg, num_cpus=0).remote())

    #
    # Test a function with 0 CPU.
    #
    @ray.remote(num_cpus=0)
    def g():
        pass

    # 0 CPU should work.
    ray.get(g.options(placement_group=pg).remote())

    placement_group_assert_no_leak([pg])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_pack(ray_start_cluster, connect_to_client):
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

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name",
            strategy="PACK",
            bundles=[
                {"CPU": 2, "GPU": 0},  # Test 0 resource spec doesn't break tests.
                {"CPU": 2},
            ],
        )
        ray.get(placement_group.ready())
        actor_1 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=0
        ).remote()
        actor_2 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=1
        ).remote()

        ray.get(actor_1.value.remote())
        ray.get(actor_2.value.remote())

        # Get all actors.
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are collocated in one node.
        actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
        actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

        assert actor_info_1 and actor_info_2

        node_of_actor_1 = actor_info_1["Address"]["NodeID"]
        node_of_actor_2 = actor_info_2["Address"]["NodeID"]
        assert node_of_actor_1 == node_of_actor_2
        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_strict_pack(ray_start_cluster, connect_to_client):
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

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name",
            strategy="STRICT_PACK",
            bundles=[
                {
                    "memory": 50
                    * 1024
                    * 1024,  # Test memory resource spec doesn't break tests.
                    "CPU": 2,
                },
                {"CPU": 2},
            ],
        )
        ray.get(placement_group.ready())
        actor_1 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=0
        ).remote()
        actor_2 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=1
        ).remote()

        ray.get(actor_1.value.remote())
        ray.get(actor_2.value.remote())

        # Get all actors.
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are collocated in one node.
        actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
        actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

        assert actor_info_1 and actor_info_2

        node_of_actor_1 = actor_info_1["Address"]["NodeID"]
        node_of_actor_2 = actor_info_2["Address"]["NodeID"]
        assert node_of_actor_1 == node_of_actor_2

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_spread(ray_start_cluster, connect_to_client):
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

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name", strategy="SPREAD", bundles=[{"CPU": 2}, {"CPU": 2}]
        )
        ray.get(placement_group.ready())
        actor_1 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=0
        ).remote()
        actor_2 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=1
        ).remote()

        ray.get(actor_1.value.remote())
        ray.get(actor_2.value.remote())

        # Get all actors.
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are located in separate nodes.
        actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
        actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

        assert actor_info_1 and actor_info_2

        node_of_actor_1 = actor_info_1["Address"]["NodeID"]
        node_of_actor_2 = actor_info_2["Address"]["NodeID"]
        assert node_of_actor_1 != node_of_actor_2

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_strict_spread(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name",
            strategy="STRICT_SPREAD",
            bundles=[{"CPU": 2}, {"CPU": 2}, {"CPU": 2}],
        )
        ray.get(placement_group.ready())
        actor_1 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=0
        ).remote()
        actor_2 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=1
        ).remote()
        actor_3 = Actor.options(
            placement_group=placement_group, placement_group_bundle_index=2
        ).remote()

        ray.get(actor_1.value.remote())
        ray.get(actor_2.value.remote())
        ray.get(actor_3.value.remote())

        # Get all actors.
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are located in separate nodes.
        actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
        actor_info_2 = actor_infos.get(actor_2._actor_id.hex())
        actor_info_3 = actor_infos.get(actor_3._actor_id.hex())

        assert actor_info_1 and actor_info_2 and actor_info_3

        node_of_actor_1 = actor_info_1["Address"]["NodeID"]
        node_of_actor_2 = actor_info_2["Address"]["NodeID"]
        node_of_actor_3 = actor_info_3["Address"]["NodeID"]
        assert node_of_actor_1 != node_of_actor_2
        assert node_of_actor_1 != node_of_actor_3
        assert node_of_actor_2 != node_of_actor_3

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_actor_resource_ids(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=1)
    class F:
        def f(self):
            return ray.worker.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        g1 = ray.util.placement_group([{"CPU": 2}])
        a1 = F.options(placement_group=g1).remote()
        resources = ray.get(a1.f.remote())
        assert len(resources) == 1, resources
        assert "CPU_group_" in list(resources.keys())[0], resources
        placement_group_assert_no_leak([g1])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_task_resource_ids(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=1)
    def f():
        return ray.worker.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        g1 = ray.util.placement_group([{"CPU": 2}])
        o1 = f.options(placement_group=g1).remote()
        resources = ray.get(o1)
        assert len(resources) == 1, resources
        assert "CPU_group_" in list(resources.keys())[0], resources
        assert "CPU_group_0_" not in list(resources.keys())[0], resources

        # Now retry with a bundle index constraint.
        o1 = f.options(placement_group=g1, placement_group_bundle_index=0).remote()
        resources = ray.get(o1)
        assert len(resources) == 2, resources
        keys = list(resources.keys())
        assert "CPU_group_" in keys[0], resources
        assert "CPU_group_" in keys[1], resources
        assert "CPU_group_0_" in keys[0] or "CPU_group_0_" in keys[1], resources

        placement_group_assert_no_leak([g1])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_hang(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=1)
    def f():
        return ray.worker.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        # Warm workers up, so that this triggers the hang rice.
        ray.get(f.remote())

        g1 = ray.util.placement_group([{"CPU": 2}])
        # This will start out infeasible. The placement group will then be
        # created and it transitions to feasible.
        o1 = f.options(placement_group=g1).remote()

        resources = ray.get(o1)
        assert len(resources) == 1, resources
        assert "CPU_group_" in list(resources.keys())[0], resources

        placement_group_assert_no_leak([g1])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
