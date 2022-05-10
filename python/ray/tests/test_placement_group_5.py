import pytest
import sys
import ray

from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.tests.test_placement_group import are_pairwise_unique


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
        elif index < 3:
            return Actor.options(
                placement_group=pg, placement_group_bundle_index=index, num_gpus=1
            ).remote()
        else:
            return Actor.options(
                placement_group=pg,
                placement_group_bundle_index=index,
                object_store_memory=1024 * 1024 * 200,
            ).remote()

    def add_nodes_to_cluster(cluster):
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=2)
        cluster.add_node(num_gpus=1)
        cluster.add_node(object_store_memory=1024 * 1024 * 250)

    default_bundles = [
        {"CPU": 1},
        {"CPU": 2},
        {"CPU": 1, "GPU": 1},
        {"CPU": 1, "object_store_memory": 1024 * 1024 * 200},
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
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are located in separate nodes.
        actor_info_objs = [actor_infos.get(actor._actor_id.hex()) for actor in actors]
        assert are_pairwise_unique(
            [info_obj["Address"]["NodeID"] for info_obj in actor_info_objs]
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
