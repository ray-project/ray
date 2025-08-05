import sys
import torch
import pytest
import ray
from ray.experimental.collective import create_collective_group


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="nccl")
    def echo(self, data):
        return data.to("cuda")

    def sum(self, data):
        return data.sum().item()

    def increment(self, data):
        return data + 1


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


def test_duplicate_objectref_transfer(ray_start_regular):
    """
    This test checks that passes a GPU object ref to the same actor and a different actor.
    """
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="nccl")

    actor0, actor1 = actors[0], actors[1]

    small_tensor = torch.randn((1,))

    ref = actor0.echo.remote(small_tensor)
    result1 = actor0.increment.remote(ref)
    result2 = actor1.increment.remote(ref)
    assert ray.get(result1) == pytest.approx(small_tensor + 2)
    assert ray.get(result2) == pytest.approx(small_tensor + 2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
