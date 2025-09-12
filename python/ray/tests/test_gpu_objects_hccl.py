import sys
import torch
import pytest
import ray
from ray.experimental.collective import create_collective_group


@ray.remote(num_cpus=0, resources={"NPU": 1}, enable_tensor_transport=True)
class NPUTestActor:
    @ray.method(tensor_transport="hccl")
    def echo(self, data):
        return data.to("npu")

    def sum(self, data):
        return data.sum().item()


@pytest.mark.parametrize(
    "ray_start_regular", [{"resources": {"NPU": 2}}], indirect=True
)
def test_hccl(ray_start_regular):
    world_size = 2
    actors = [NPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="hccl", name="test_hccl_group")

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    remote_sum = ray.get(dst_actor.sum.remote(gpu_ref))
    assert tensor.sum().item() == remote_sum


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
