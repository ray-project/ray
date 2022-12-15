"""Job Submission CUDA available test

Checks that GPU resources are available in the job submission
driver script.

This file is a driver script to be submitted to a Ray cluster via
the Ray Jobs API. This is done by specifying `type: job` in
`release_tests.yaml` (as opposed to, say, `type: sdk_command`).

Release test for https://github.com/ray-project/ray/issues/24455

Test owner: architkulkarni
"""

import ray
import torch
from ray._private.test_utils import wait_for_condition

ray.init()

# Assert that GPU resources are available in the driver script
assert torch.cuda.is_available(), "CUDA is not available in the driver script"


# For good measure, let's also check that we can use the GPU
# in a remote function.
@ray.remote(num_gpus=0.1)
def f():
    return ray.get_gpu_ids()


assert ray.get(f.remote()) == [0]

# Also check that non-GPU tasks can be scheduled across all nodes.
NUM_NODES = 2


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def get_node_id():
    return ray.get_runtime_context().node_id


node_ids = set(ray.get([get_node_id.remote() for _ in range(100)]))


def check_num_nodes_and_spawn_tasks():
    node_ids.update(ray.get([get_node_id.remote() for _ in range(10)]))
    return len(node_ids) >= NUM_NODES


wait_for_condition(check_num_nodes_and_spawn_tasks)
