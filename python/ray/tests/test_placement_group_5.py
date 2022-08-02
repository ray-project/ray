import sys

import pytest

import ray
from ray.tests.test_placement_group import are_pairwise_unique
from ray.util.client.ray_client_helpers import connect_to_client_or_not


@pytest.mark.parametrize("connect_to_client", [False, True])
@pytest.mark.parametrize("scheduling_strategy", ["SPREAD", "STRICT_SPREAD", "PACK"])
def test_placement_group_bin_packing_priority(
    ray_start_cluster, connect_to_client, scheduling_strategy
):
    @ray.remote
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    def index_to_actor(pg, index):
        if index < 2:
            return Actor.options(
                placement_group=pg, placement_group_bundle_index=index, num_cpus=1
            ).remote()
        else:
            return Actor.options(
                placement_group=pg, placement_group_bundle_index=index, num_gpus=1
            ).remote()

    def add_nodes_to_cluster(cluster):
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=2)
        cluster.add_node(num_gpus=1)

    default_bundles = [
        {"CPU": 1},
        {"CPU": 2},
        {"CPU": 1, "GPU": 1},
    ]

    default_num_nodes = len(default_bundles)
    cluster = ray_start_cluster
    add_nodes_to_cluster(cluster)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name",
            strategy=scheduling_strategy,
            bundles=default_bundles,
        )
        ray.get(placement_group.ready())

        actors = [index_to_actor(placement_group, i) for i in range(default_num_nodes)]

        [ray.get(actor.value.remote()) for actor in actors]

        # Get all actors.
        actor_infos = ray._private.state.actors()

        # Make sure all actors in counter_list are located in separate nodes.
        actor_info_objs = [actor_infos.get(actor._actor_id.hex()) for actor in actors]
        assert are_pairwise_unique(
            [info_obj["Address"]["NodeID"] for info_obj in actor_info_objs]
        )


@pytest.mark.parametrize("multi_bundle", [False, True])
@pytest.mark.parametrize("even_pack", [False, True])
@pytest.mark.parametrize("scheduling_strategy", ["SPREAD", "STRICT_PACK", "PACK"])
def test_placement_group_max_cpu_frac(
    ray_start_cluster, multi_bundle, even_pack, scheduling_strategy
):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    if multi_bundle:
        bundles = [{"CPU": 1}] * 3
    else:
        bundles = [{"CPU": 3}]

    # Input validation - max_cpu_fraction_per_node must be between 0 and 1.
    with pytest.raises(ValueError):
        ray.util.placement_group(bundles, _max_cpu_fraction_per_node=-1)
    with pytest.raises(ValueError):
        ray.util.placement_group(bundles, _max_cpu_fraction_per_node=2)

    pg = ray.util.placement_group(
        bundles, strategy=scheduling_strategy, _max_cpu_fraction_per_node=0.5
    )

    # Placement group will never be scheduled since it would violate the max CPU
    # fraction reservation.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=5)

    # Add new node with enough CPU cores to scheduled placement group bundle while
    # adhering to the max CPU fraction constraint.
    if even_pack:
        num_cpus = 6
    else:
        num_cpus = 8
    cluster.add_node(num_cpus=num_cpus)
    cluster.wait_for_nodes()
    # The placement group should be schedulable so this shouldn't raise.
    ray.get(pg.ready(), timeout=5)


def test_placement_group_max_cpu_frac_multiple_pgs(ray_start_cluster):
    """
    Make sure when there's more than 1 pg, they respect the fraction.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # This pg should be scheduable.
    pg = ray.util.placement_group([{"CPU": 4}], _max_cpu_fraction_per_node=0.5)
    ray.get(pg.ready())

    # When we schedule another placement group, it shouldn't be scheduled.
    pg2 = ray.util.placement_group([{"CPU": 4}], _max_cpu_fraction_per_node=0.5)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg2.ready(), timeout=5)

    cluster.add_node(num_cpus=8)
    ray.get(pg2.ready())

    """
    Make sure when the CPU * frac < 1, we can at least
    guarantee to have 1 CPU for pg.
    """
    ray.util.remove_placement_group(pg)
    ray.util.remove_placement_group(pg2)

    # We can reserve up to 0.8 CPU, but it should round up to 1, so this pg
    # is schedulable.
    pg = ray.util.placement_group([{"CPU": 1}], _max_cpu_fraction_per_node=0.1)
    ray.get(pg.ready())


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
