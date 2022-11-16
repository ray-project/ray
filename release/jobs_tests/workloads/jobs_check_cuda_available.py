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

ray.init()

# Assert that GPU resources are available in the driver script
assert torch.cuda.is_available()

# For good measure, let's also check that we can use the GPU
# in a remote function.
@ray.remote(num_gpus=0.1)
def f():
    return ray.get_gpu_ids()


assert ray.get(f.remote()) == [0]
