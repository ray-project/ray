import pytest
import sys

import ray
import ray.cluster_utils
from ray._private.test_utils import (
    placement_group_assert_no_leak,
)
from ray.util.client.ray_client_helpers import connect_to_client_or_not


def are_pairwise_unique(g):
    s = set()
    for x in g:
        if x in s:
            return False
        s.add(x)
    return True


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_strict_spread(ray_start_cluster_enabled, connect_to_client):
    @ray.remote
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster_enabled
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
        actors = [
            Actor.options(
                placement_group=placement_group,
                placement_group_bundle_index=i,
                num_cpus=2,
            ).remote()
            for i in range(num_nodes)
        ]

        [ray.get(actor.value.remote()) for actor in actors]

        # Get all actors.
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are located in separate nodes.
        actor_info_objs = [actor_infos.get(actor._actor_id.hex()) for actor in actors]
        assert are_pairwise_unique(
            [info_obj["Address"]["NodeID"] for info_obj in actor_info_objs]
        )

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_actor_resource_ids(
    ray_start_cluster_enabled, connect_to_client
):
    @ray.remote(num_cpus=1)
    class F:
        def f(self):
            return ray.worker.get_resource_ids()

    cluster = ray_start_cluster_enabled
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
def test_placement_group_task_resource_ids(
    ray_start_cluster_enabled, connect_to_client
):
    @ray.remote(num_cpus=1)
    def f():
        return ray.worker.get_resource_ids()

    cluster = ray_start_cluster_enabled
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
def test_placement_group_hang(ray_start_cluster_enabled, connect_to_client):
    @ray.remote(num_cpus=1)
    def f():
        return ray.worker.get_resource_ids()

    cluster = ray_start_cluster_enabled
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
