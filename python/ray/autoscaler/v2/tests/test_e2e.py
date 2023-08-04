import os
import sys

import pytest

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking, wait_for_condition
from ray.autoscaler.v2.sdk import get_cluster_status
from ray.cluster_utils import AutoscalingCluster
from ray.util.state.api import list_placement_groups


def test_placement_group_consistent(shutdown_only):
    # Test that continuously creating and removing placement groups
    # does not leak pending resource requests.
    import time

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
    )
    cluster.start()
    ray.init("auto")

    driver_script = """

import ray
import time
# Import placement group APIs.
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group,
)

ray.init("auto")

# Reserve all the CPUs of nodes, X= num of cpus, N = num of nodes
while True:
    pg = placement_group([{"CPU": 1}])
    ray.get(pg.ready())
    time.sleep(0.5)
    remove_placement_group(pg)
    time.sleep(0.5)
"""
    run_string_as_driver_nonblocking(driver_script)

    def pg_created():
        pgs = list_placement_groups()
        assert len(pgs) > 0

        return True

    wait_for_condition(pg_created)

    for _ in range(30):
        # verify no pending request + resource used.
        status = get_cluster_status()
        has_pg_demand = len(status.resource_demands.placement_group_demand) > 0
        has_pg_usage = False
        for usage in status.cluster_resource_usage:
            has_pg_usage = has_pg_usage or "bundle" in usage.resource_name
        print(has_pg_demand, has_pg_usage)
        assert not (has_pg_demand and has_pg_usage), status
        time.sleep(0.1)

    cluster.shutdown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
