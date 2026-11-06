"""Job Submission CUDA version test

Checks that GPU resources are available in the job submission driver script
*and* that the CUDA toolkit version torch was built against matches the
expected major version (default 13, for the cu130 images). This guards against
a cu130 image accidentally shipping a torch built for the wrong CUDA major.

This file is a driver script to be submitted to a Ray cluster via the Ray Jobs
API. This is done by specifying `type: job` in `release_tests.yaml`.

Companion to jobs_check_cuda_available.py.
"""

import argparse

import ray
import torch
from ray._common.test_utils import wait_for_condition

parser = argparse.ArgumentParser()
parser.add_argument(
    "--expected-cuda-version",
    type=str,
    required=True,
    help=(
        "Expected CUDA toolkit version torch is built with, e.g. '13' or "
        "'13.0'. Matched component-wise against torch.version.cuda, so '13' "
        "accepts any 13.x and '13.0' requires exactly 13.0."
    ),
)
args = parser.parse_args()


def check_cuda_version(expected: str) -> None:
    """Assert CUDA is available and built against the expected version.

    The expected version is matched component-wise, so passing "13" accepts any
    13.x while "13.0" requires exactly 13.0.
    """
    assert torch.cuda.is_available(), "CUDA is not available"
    actual = torch.version.cuda
    assert actual is not None, "torch was not built with CUDA support"
    expected_parts = expected.split(".")
    actual_parts = actual.split(".")
    assert actual_parts[: len(expected_parts)] == expected_parts, (
        f"Expected CUDA version starting with {expected!r}, but torch was built "
        f"with CUDA {actual!r}"
    )


ray.init()

# Assert GPU availability and the expected CUDA version in the driver script.
check_cuda_version(args.expected_cuda_version)


# Also confirm the same holds inside a GPU remote task (i.e. on the workers).
@ray.remote(num_gpus=0.1)
def f(expected: str):
    check_cuda_version(expected)
    return ray.get_gpu_ids()


assert ray.get(f.remote(args.expected_cuda_version)) == [0]

# Also check that non-GPU tasks can be scheduled across all nodes.
NUM_NODES = 2


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def get_node_id():
    return ray.get_runtime_context().get_node_id()


node_ids = set(ray.get([get_node_id.remote() for _ in range(100)]))


def check_num_nodes_and_spawn_tasks():
    node_ids.update(ray.get([get_node_id.remote() for _ in range(10)]))
    return len(node_ids) >= NUM_NODES


wait_for_condition(check_num_nodes_and_spawn_tasks)
