import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils

from ray.util.client.ray_client_helpers import connect_to_client_or_not


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_mini_integration(ray_start_cluster_enabled, connect_to_client):
    # Create bundles as many as number of gpus in the cluster.
    # Do some random work and make sure all resources are properly recovered.

    cluster = ray_start_cluster_enabled

    num_nodes = 5
    per_bundle_gpus = 2
    gpu_per_node = 4
    total_gpus = num_nodes * per_bundle_gpus * gpu_per_node
    per_node_gpus = per_bundle_gpus * gpu_per_node

    bundles_per_pg = 2
    total_num_pg = total_gpus // (bundles_per_pg * per_bundle_gpus)

    [
        cluster.add_node(num_cpus=2, num_gpus=per_bundle_gpus * gpu_per_node)
        for _ in range(num_nodes)
    ]
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):

        @ray.remote(num_cpus=0, num_gpus=1)
        def random_tasks():
            import time
            import random

            sleep_time = random.uniform(0.1, 0.2)
            time.sleep(sleep_time)
            return True

        pgs = []
        pg_tasks = []
        # total bundle gpu usage = bundles_per_pg*total_num_pg*per_bundle_gpus
        # Note this is half of total
        for index in range(total_num_pg):
            pgs.append(
                ray.util.placement_group(
                    name=f"name{index}",
                    strategy="PACK",
                    bundles=[{"GPU": per_bundle_gpus} for _ in range(bundles_per_pg)],
                )
            )

        # Schedule tasks.
        for i in range(total_num_pg):
            pg = pgs[i]
            pg_tasks.append(
                [
                    random_tasks.options(
                        placement_group=pg, placement_group_bundle_index=bundle_index
                    ).remote()
                    for bundle_index in range(bundles_per_pg)
                ]
            )

        # Make sure tasks are done and we remove placement groups.
        num_removed_pg = 0
        pg_indexes = [2, 3, 1, 7, 8, 9, 0, 6, 4, 5]
        while num_removed_pg < total_num_pg:
            index = pg_indexes[num_removed_pg]
            pg = pgs[index]
            assert all(ray.get(pg_tasks[index]))
            ray.util.remove_placement_group(pg)
            num_removed_pg += 1

        @ray.remote(num_cpus=2, num_gpus=per_node_gpus)
        class A:
            def ping(self):
                return True

        # Make sure all resources are properly returned by scheduling
        # actors that take up all existing resources.
        actors = [A.remote() for _ in range(num_nodes)]
        assert all(ray.get([a.ping.remote() for a in actors]))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
