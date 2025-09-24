import sys

import pytest
import torch

import ray
from ray.experimental.collective import create_collective_group


@ray.remote(enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="nccl")
    def echo(self, data):
        return data.to("cuda")

    def double(self, data):
        data.mul_(2)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_colocated_actors(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.options(num_gpus=0.5, num_cpus=0).remote() for _ in range(world_size)]
    create_collective_group(actors, backend="nccl")

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    ray.get(dst_actor.double.remote(gpu_ref))
    # Check that the tensor is modified in place, and is reflected on the source actor
    assert torch.equal(ray.get(gpu_ref), torch.tensor([2, 4, 6], device="cuda"))


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_ipc_fail(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.options(num_gpus=1, num_cpus=0).remote() for _ in range(world_size)]
    create_collective_group(actors, backend="nccl")

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    ray.get(dst_actor.double.remote(gpu_ref))
    assert torch.equal(ray.get(gpu_ref), torch.tensor([1, 2, 3], device="cuda"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))