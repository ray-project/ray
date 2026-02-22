# flake8: noqa

# __dtensor_gloo_example_start__
import torch
import torch.distributed as dist
from torch.distributed._tensor import DTensor, Shard, distribute_tensor
from torch.distributed.device_mesh import init_device_mesh

import ray
from ray._common.network_utils import find_free_port
from ray.experimental.collective import create_collective_group


@ray.remote(enable_tensor_transport=True)
class DTensorActor:
    def __init__(self, rank: int, world_size: int):
        self.rank = rank
        self.world_size = world_size
        self.mesh = None

    def get_node_ip(self) -> str:
        return ray.util.get_node_ip_address()

    def setup_distributed(self, master_addr: str, master_port: int):
        """Initialize PyTorch distributed with Gloo backend."""
        if not dist.is_initialized():
            dist.init_process_group(
                backend="gloo",
                init_method=f"tcp://{master_addr}:{master_port}",
                world_size=self.world_size,
                rank=self.rank,
            )
        self.mesh = init_device_mesh("cpu", (self.world_size,))

    @ray.method(tensor_transport="gloo")
    def create_dtensor(self):
        """Create a DTensor sharded across the mesh."""
        global_tensor = torch.arange(1, 9, dtype=torch.float32).reshape(4, 2)
        # Shard the tensor along dimension 0 across the mesh.
        # Each actor gets a different shard of the tensor.
        return distribute_tensor(global_tensor, self.mesh, placements=[Shard(0)])

    def sum_dtensor(self, dt):
        """Reconstruct the full tensor and compute its sum."""
        assert isinstance(dt, DTensor), f"Expected DTensor, got {type(dt)}"
        return dt.full_tensor().sum().item()

    def cleanup(self):
        if dist.is_initialized():
            dist.destroy_process_group()


ray.init()
world_size = 2
master_port = find_free_port()

# Create actors and collective group.
actors = [
    DTensorActor.remote(rank=i, world_size=world_size) for i in range(world_size)
]
create_collective_group(actors, backend="torch_gloo")

# Initialize PyTorch distributed on each actor.
master_addr = ray.get(actors[0].get_node_ip.remote())
ray.get(
    [actor.setup_distributed.remote(master_addr, master_port) for actor in actors]
)

# Create a DTensor on actor 0. The tensor is sharded, so actor 0 holds
# rows [1,2] and [3,4], while actor 1 holds rows [5,6] and [7,8].
dtensor_ref = actors[0].create_dtensor.remote()

# Transfer the DTensor to actor 1 via Gloo. Ray automatically serializes
# the DeviceMesh metadata and transfers the tensor data.
result = ray.get(actors[1].sum_dtensor.remote(dtensor_ref))
assert result == 36.0  # sum of 1..8

# Cleanup.
ray.get([actor.cleanup.remote() for actor in actors])
ray.shutdown()
# __dtensor_gloo_example_end__
