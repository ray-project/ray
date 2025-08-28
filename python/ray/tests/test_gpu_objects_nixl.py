import sys

import pytest
import torch

import ray


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="nixl")
    def echo(self, data):
        return data.to("cuda")

    def sum(self, data):
        return data.sum().item()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_p2p(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    result = dst_actor.sum.remote(ref)
    assert tensor.sum().item() == ray.get(result)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_intra_gpu_tensor_transfer(ray_start_regular):
    actor = GPUTestActor.remote()

    tensor = torch.tensor([1, 2, 3])

    # Intra-actor communication for pure GPU tensors
    ref = actor.echo.remote(tensor)
    result = actor.sum.remote(ref)
    assert tensor.sum().item() == ray.get(result)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
