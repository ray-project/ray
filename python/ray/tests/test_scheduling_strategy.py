import sys
import pytest

import ray
import ray.cluster_utils
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


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
