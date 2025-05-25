import sys
import random
import torch
import pytest
import ray
import torch.distributed as dist
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel import ChannelContext


@ray.remote
class GPUTestActor:
    def register_custom_serializer(self):
        TorchTensorType().register_custom_serializer()

    def setup(self, world_size, rank):
        init_method = "tcp://localhost:8889"
        dist.init_process_group(
            backend="gloo", world_size=world_size, rank=rank, init_method=init_method
        )

    @ray.method(tensor_transport="gloo")
    def echo(self, data):
        return data

    def double(self, data):
        if isinstance(data, list):
            return [d * 2 for d in data]
        return data * 2


def init_process_group(actors):
    world_size = len(actors)
    ray.get([actor.setup.remote(world_size, i) for i, actor in enumerate(actors)])
    # Set up communicator so that the driver knows the actor-to-rank mapping.
    ctx = ChannelContext.get_current()
    ctx.communicators[0] = actors
    # Register custom serializer so that the serializer can retrieve tensors from
    # return values of actor methods.
    ray.get([actor.register_custom_serializer.remote() for actor in actors])


def test_inter_actor_gpu_tensor_transfer(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    init_process_group(actors)

    small_tensor = torch.randn((1,))
    sender = actors[0]
    receiver = actors[1]

    ref = sender.echo.remote(small_tensor)
    result = receiver.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)

    medium_tensor = torch.randn((500, 500))
    ref = sender.echo.remote(medium_tensor)
    result = receiver.double.remote(ref)
    assert ray.get(result) == pytest.approx(medium_tensor * 2)


def test_mix_cpu_gpu_data(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    init_process_group(actors)

    tensor = torch.randn((1,))
    cpu_data = random.randint(0, 100)
    data = [tensor, cpu_data]

    sender, receiver = actors[0], actors[1]
    ref = sender.echo.remote(data)
    ref = receiver.double.remote(ref)
    result = ray.get(ref)

    assert result[0] == pytest.approx(tensor * 2)
    assert result[1] == cpu_data * 2


def test_multiple_tensors(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    init_process_group(actors)

    tensor1 = torch.randn((1,))
    tensor2 = torch.randn((2,))
    cpu_data = random.randint(0, 100)
    data = [tensor1, tensor2, cpu_data]

    sender, receiver = actors[0], actors[1]
    ref = sender.echo.remote(data)
    ref = receiver.double.remote(ref)
    result = ray.get(ref)

    assert result[0] == pytest.approx(tensor1 * 2)
    assert result[1] == pytest.approx(tensor2 * 2)
    assert result[2] == cpu_data * 2


def test_invalid_tensor_transport(ray_start_regular):
    with pytest.raises(ValueError, match="Invalid tensor transport"):

        @ray.remote
        class InvalidActor:
            @ray.method(tensor_transport="invalid")
            def echo(self, data):
                return data


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
