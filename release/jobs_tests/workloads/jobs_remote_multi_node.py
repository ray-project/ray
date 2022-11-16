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

num_nodes = len(ray.nodes())
# Allow one fewer node in case a node fails to come up.
assert (
    num_nodes >= NUM_NODES - 1
), f"Expected at least {NUM_NODES - 1} nodes, got {num_nodes}"
