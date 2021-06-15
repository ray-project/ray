import pytest
import platform
import os
import sys
import time

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
from ray.test_utils import wait_for_condition


def test_actor_scheduling_not_block_with_placement_group(ray_start_cluster):
    """Tests the scheduling of lots of actors will not be blocked
       when using placement groups.

       For more detailed information please refer to:
       https://github.com/ray-project/ray/issues/15801.
    """

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    class A:
        def ready(self):
            pass

    actor_num = 1000
    pgs = [ray.util.placement_group([{"CPU": 1}]) for _ in range(actor_num)]
    actors = [A.options(placement_group=pg).remote() for pg in pgs]
    refs = [actor.ready.remote() for actor in actors]

    expected_created_num = 1

    def is_actor_created_number_correct():
        nonlocal expected_created_num
        ready, not_ready = ray.wait(refs, num_returns=len(refs), timeout=1)
        return len(ready) == expected_created_num

    def is_pg_created_number_correct():
        nonlocal expected_created_num
        created_pgs = [
            pg for _, pg in ray.util.placement_group_table().items()
            if pg["state"] == "CREATED"
        ]
        return len(created_pgs) == expected_created_num

    wait_for_condition(is_pg_created_number_correct, timeout=3)
    wait_for_condition(
        is_actor_created_number_correct, timeout=30, retry_interval_ms=0)

    # NOTE: we don't need to test all the actors create successfully.
    for _ in range(20):
        expected_created_num += 1
        cluster.add_node(num_cpus=1)

        wait_for_condition(is_pg_created_number_correct, timeout=10)
        # Make sure the node add event will cause a waiting actor
        # to create successfully in time.
        wait_for_condition(
            is_actor_created_number_correct, timeout=30, retry_interval_ms=0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
