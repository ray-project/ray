import sys
import random
import torch
import pytest
import ray
import torch.distributed as dist
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel import ChannelContext
from ray.experimental.collective import create_collective_group
from ray._private.custom_types import TensorTransportEnum


@ray.remote
class GPUTestActor:
    @ray.method(tensor_transport="nccl")
    def echo(self, data):
        return data.to("cuda")

    def sum(self, data):
        return data.sum().item()


def test_p2p(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="nccl")

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo_cuda.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    remote_sum = ray.get(dst_actor.sum.remote(gpu_ref))
    assert tensor.sum().item() == remote_sum


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))