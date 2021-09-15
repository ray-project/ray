import pytest
import sys
import time

from random import random

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils
from ray.util.placement_group import (placement_group, remove_placement_group)


@pytest.mark.parametrize("execution_number", range(3))
def test_placement_group_remove_stress(ray_start_cluster, execution_number):
    # This test checks the race condition between remove / creation.
    # This test shouldn't be flaky. If it fails on the last ray.get
    # that highly likely indicates a real bug.
    # It also runs 3 times to make sure the test consistently passes.
    # When 999 resource quantity is used, it fails about every other time
    # when the test was written.
    cluster = ray_start_cluster
    resource_quantity = 999
    num_nodes = 5
    custom_resources = {"pg_custom": resource_quantity}
    # Create pg that uses 1 resource of cpu & custom resource.
    num_pg = resource_quantity

    # TODO(sang): Cluster setup. Remove when running in real clusters.
    nodes = []
    for _ in range(num_nodes):
        nodes.append(
            cluster.add_node(
                num_cpus=3,
                num_gpus=resource_quantity,
                resources=custom_resources))
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)
    while not ray.is_initialized():
        time.sleep(0.1)
    bundles = [{"GPU": 1, "pg_custom": 1}] * num_nodes

    @ray.remote(num_cpus=0, num_gpus=1, max_calls=0)
    def mock_task():
        time.sleep(0.1)
        return True

    @ray.remote(num_cpus=0)
    def pg_launcher(num_pgs_to_create):
        print("Creating pgs")
        pgs = []
        for i in range(num_pgs_to_create):
            pgs.append(placement_group(bundles, strategy="STRICT_SPREAD"))

        pgs_removed = []
        pgs_unremoved = []
        # Randomly choose placement groups to remove.
        print("removing pgs")
        for pg in pgs:
            if random() < .5:
                pgs_removed.append(pg)
            else:
                pgs_unremoved.append(pg)

        tasks = []
        # Randomly schedule tasks or actors on placement groups that
        # are not removed.
        for pg in pgs_unremoved:
            tasks.append(mock_task.options(placement_group=pg).remote())
        # Remove the rest of placement groups.
        for pg in pgs_removed:
            remove_placement_group(pg)
        ray.get(tasks)
        # Since placement groups are scheduled, remove them.
        for pg in pgs_unremoved:
            remove_placement_group(pg)

    pg_launchers = []
    for _ in range(3):
        pg_launchers.append(pg_launcher.remote(num_pg // 3))

    ray.get(pg_launchers, timeout=120)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
