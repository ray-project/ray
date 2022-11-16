"""Job submission remote multinode test

This tests that Ray Job submission works when submitting jobs
to a remote cluster with multiple nodes.

This file is a driver script to be submitted to a Ray cluster via
the Ray Jobs API.  This is done by specifying `type: job` in
`release_tests.yaml` (as opposed to, say, `type: sdk_command`).

It runs several actors to verify that multiple nodes started up.
The number of actors is more than the number of CPUs on a single
node, so we expect that the actors will be spread across multiple
nodes.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import ray

ray.init()

NUM_NODES = 5

# Assert that we have 5 nodes
num_nodes = len(ray.nodes())
assert num_nodes == NUM_NODES, "Expected 5 nodes, got {}".format(num_nodes)

num_available_cpus = sum(
    ray.available_resources().get("CPU", 0) for node in ray.nodes()
)

assert (
    num_available_cpus >= 4 * num_nodes - 1
), "Expected at least 4 CPUs per node, got {}".format(num_available_cpus)


@ray.remote
class Actor:
    def __init__(self):
        pass

    def get_node_id(self):
        return ray.get_runtime_context().node_id


actors = [Actor.remote() for _ in range(num_available_cpus)]
node_ids = set(ray.get([actor.get_node_id.remote() for actor in actors]))

# Assert that actors are spread across all nodes
assert len(node_ids) == num_nodes, "Expected {} nodes, got {}".format(
    num_nodes, len(node_ids)
)
