import sys

import pytest
import torch

import ray
from ray.exceptions import ActorDiedError


@ray.remote(enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="cuda_ipc")
    def echo(self, data):
        return data.to("cuda")

    def double(self, data):
        data.mul_(2)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_colocated_actors(ray_start_regular):
    world_size = 2
    actors = [
        GPUTestActor.options(num_gpus=0.5, num_cpus=0).remote()
        for _ in range(world_size)
    ]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    ray.get(dst_actor.double.remote(gpu_ref), _tensor_transport="object_store")
    # Check that the tensor is modified in place, and is reflected on the source actor
    assert torch.equal(
        ray.get(gpu_ref, _tensor_transport="object_store"),
        torch.tensor([2, 4, 6], device="cuda"),
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_ipc_fail(ray_start_regular):
    world_size = 2
    actors = [
        GPUTestActor.options(num_gpus=1, num_cpus=0).remote() for _ in range(world_size)
    ]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    with pytest.raises(ActorDiedError):
        ray.get(dst_actor.double.remote(gpu_ref), _tensor_transport="object_store")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
