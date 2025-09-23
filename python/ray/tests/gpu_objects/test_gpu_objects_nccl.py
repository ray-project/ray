import sys

import pytest
import torch
import torch.distributed as dist
from torch.distributed.device_mesh import init_device_mesh
from torch.distributed.tensor import (
    DTensor,
    Shard,
    distribute_tensor,
)

import ray
from ray.experimental.collective import create_collective_group


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="nccl")
    def echo(self, data):
        return data.to("cuda")

    def sum(self, data):
        return data.sum().item()


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class DTensorTestActor:
    def __init__(self, rank: int, world_size: int):
        self.rank = rank
        self.world_size = world_size
        torch.cuda.set_device(0)
        self.mesh = None

    def get_node_ip(self) -> str:
        return ray.util.get_node_ip_address()

    def setup_distributed(self, master_addr: str, master_port: int):
        dist.init_process_group(
            backend="nccl",
            init_method=f"tcp://{master_addr}:{master_port}",
            world_size=self.world_size,
            rank=self.rank,
        )
        self.mesh = init_device_mesh("cuda", (self.world_size,))

    # @ray.method(tensor_transport="nccl")
    def create_dtensor(self, M: int = 4, N: int = 2):
        """Create a distributed tensor sharded across the mesh."""
        global_tensor = torch.arange(M * N, dtype=torch.float32, device="cuda").view(
            M, N
        )
        dt = distribute_tensor(global_tensor, self.mesh, placements=[Shard(0)])
        return dt

    def sum_dtensor(self, dt):
        """Sum a distributed tensor and return the result."""
        assert isinstance(dt, DTensor)
        return dt.full_tensor().sum().item()

    def cleanup(self):
        if dist.is_initialized():
            dist.destroy_process_group()


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


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_dtensor_transport(ray_start_regular):
    """Test DTensor transport between actors using NCCL."""
    world_size = 2
    master_port = 29500

    # Create actors
    actors = [
        DTensorTestActor.remote(rank=i, world_size=world_size)
        for i in range(world_size)
    ]
    create_collective_group(actors, backend="nccl")

    # Setup distributed training
    master_addr = ray.get(actors[0].get_node_ip.remote())
    ray.get(
        [actor.setup_distributed.remote(master_addr, master_port) for actor in actors]
    )

    # Create DTensor on actors
    dtensor_ref = actors[0].create_dtensor.remote(4, 2)
    dtensor_ref1 = actors[1].create_dtensor.remote(4, 2)

    # Transfer DTensor to second actor and sum it
    remote_sum_ref = actors[1].sum_dtensor.remote(dtensor_ref)
    remote_sum_ref1 = actors[0].sum_dtensor.remote(dtensor_ref1)

    remote_sum = ray.get(remote_sum_ref)
    remote_sum1 = ray.get(remote_sum_ref1)
    # Verify the result
    assert remote_sum == remote_sum1
    expected_sum = float(torch.arange(4 * 2, dtype=torch.float32).sum().item())
    assert abs(expected_sum - remote_sum) < 1e-6

    # Cleanup
    ray.get([actor.cleanup.remote() for actor in actors])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
