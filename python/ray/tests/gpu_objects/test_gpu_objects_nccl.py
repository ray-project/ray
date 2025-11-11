import sys

import pytest
import torch

import ray
from ray.experimental.collective import create_collective_group


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="nccl")
    def echo(self, data):
        return data.to("cuda")

    def sum(self, data):
        return data.sum().item()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_p2p(ray_start_regular):
    # TODO(swang): Add tests for mocked NCCL that can run on CPU-only machines.
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="nccl")

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    remote_sum = ray.get(dst_actor.sum.remote(gpu_ref))
    assert tensor.sum().item() == remote_sum


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
