import pytest

import ray
import sys
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_scheduling_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, num_gpus=2, resources={"node_1": 1})
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, num_gpus=3, resources={"node_2": 1})

    @ray.remote(num_cpus=0)
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    @ray.remote
    class Actor:
        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

    node_1_id = ray.get(get_node_id.options(resources={"node_1": 1}).remote())
    node_2_id = ray.get(get_node_id.options(resources={"node_2": 1}).remote())

    scheduling_cluster = ray.util.scheduling_cluster([{"CPU": 2}, {"GPU": 3}], "PACK")
    ray.get(scheduling_cluster.ready())
    with scheduling_cluster:
        assert ray.get(get_node_id.options(num_cpus=1).remote()) == node_1_id
        assert ray.get(get_node_id.options(num_gpus=1).remote()) == node_2_id
        pg = ray.util.placement_group([{"CPU": 2}, {"GPU": 1}])
        ray.get(pg.ready())
        assert (
            ray.get(
                get_node_id.options(
                    num_cpus=1,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg, placement_group_bundle_index=0
                    ),
                ).remote()
            )
            == node_1_id
        )
        assert ray.get(get_node_id.options(num_cpus=1).remote()) == node_2_id


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
