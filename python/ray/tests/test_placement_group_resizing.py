import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils
from ray._private.test_utils import (wait_for_condition, placement_group_assert_no_leak)

from ray.util.placement_group import (placement_group)


def test_placement_group_add_bundles_for_created_pg(ray_start_cluster):
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
    ray.get(pg.ready(), timeout=10)

    table = ray.util.placement_group_table(pg)
    assert len(list(table["bundles"].values())) == 2
    assert table["state"] == "CREATED"
    assert table["bundles_status"][0] == "VALID"
    assert table["bundles_status"][1] == "VALID"

    # Schedule an actor with the new bundle.
    actor = Actor.options(
        placement_group=pg,
        placement_group_bundle_index=1).remote()
    assert ray.get(actor.value.remote(), timeout=10) == 0

    placement_group_assert_no_leak([pg])


def test_placement_group_add_bundles_for_scheduling_pg(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    # Just one node at init.
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_SPREAD",
        bundles=[ {"CPU": 2}, {"CPU": 2} ] )

    table = ray.util.placement_group_table(placement_group)
    assert table["state"] == "PENDING"

    # Add two new bundle to this pending placement group.
    placement_group.add_bundles([{"CPU": 2}])
    placement_group.add_bundles([{"CPU": 2}])

    # Make sure its state will transform to `UPDATING`.
    table = ray.util.placement_group_table(placement_group)
    assert table["state"] == "UPDATING"

    # Add two new nodes to make sure the resources can satisficied the placement group.
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()
    ray.get(placement_group.ready(), timeout=10)

    # Check the current state.
    table = ray.util.placement_group_table(placement_group)
    assert len(list(table["bundles"].values())) == 4
    assert table["state"] == "CREATED"
    for index in range(4):
        assert table["bundles_status"][index] == "VALID"

    placement_group_assert_no_leak([placement_group])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
