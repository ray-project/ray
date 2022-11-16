"""Job submission remote multinode test

This tests that Ray Job submission works when submitting jobs
to a remote cluster with multiple nodes.

This file is a driver script to be submitted to a Ray cluster via
the Ray Jobs API.  This is done by specifying `type: job` in
`release_tests.yaml` (as opposed to, say, `type: sdk_command`).

Test owner: architkulkarni
"""

import ray

ray.init()

NUM_NODES = 5

# Assert that we have 5 nodes
num_nodes = len(ray.nodes())
assert num_nodes == NUM_NODES, "Expected 5 nodes, got {}".format(num_nodes)
