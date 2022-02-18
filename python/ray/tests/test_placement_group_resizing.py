import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils

from ray.util.placement_group import (placement_group)


def test_placement_group_add_bundles_for_created_bundles(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Create a placement group.
    pg = ray.util.placement_group(
        name="name", strategy="PACK", bundles=[{
            "CPU": 2
        }])
    ray.get(pg.ready(), timeout=10)

    # Now, Add a new bundle to this placement group and wait it to be created successfully.
    pg.add_bundles([{"CPU": 2}])
    import time
    ray.get(pg.ready(), timeout=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
