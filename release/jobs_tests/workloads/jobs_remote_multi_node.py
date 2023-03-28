"""Job submission remote multinode test

This tests that Ray Job submission works when submitting jobs
to a remote cluster with multiple nodes.

This file is a driver script to be submitted to a Ray cluster via
the Ray Jobs API.  This is done by specifying `type: job` in
`release_tests.yaml` (as opposed to, say, `type: sdk_command`).

Test owner: architkulkarni
"""

import ray
from ray._private.test_utils import wait_for_condition

ray.init()

NUM_NODES = 5

# Spawn tasks that use get_runtime_context() to get their node_id
# and wait until all of the nodes are seen.


@ray.remote(num_cpus=1)
def get_node_id():
    return ray.get_runtime_context().node_id


# Allow one fewer node in case a node fails to come up.
num_expected_nodes = NUM_NODES - 1

node_ids = set(ray.get([get_node_id.remote() for _ in range(100)]))


def check_num_nodes_and_spawn_tasks():
    node_ids.update(ray.get([get_node_id.remote() for _ in range(10)]))
    return len(node_ids) >= num_expected_nodes


wait_for_condition(check_num_nodes_and_spawn_tasks)
