import torch

import ray
from ray.util.collective import (
    allreduce,
    create_collective_group,
    init_collective_group,
)
from ray.util.collective.backend_registry import get_backend_registry
from ray.util.collective.types import ReduceOp


def test_nccl_via_registry():
    ray.init(num_gpus=8)

    registry = get_backend_registry()
    assert "NCCL" in registry.list_backends()
    assert registry.check("NCCL")

    @ray.remote(num_gpus=1)
    class Worker:
        def __init__(self, rank):
            self.rank = rank
            self.tensor = None

        def setup(self, world_size):
            init_collective_group(
                world_size=world_size,
                rank=self.rank,
                backend="NCCL",
                group_name="default",
            )

        def compute(self):
            device = torch.cuda.current_device()
            self.tensor = torch.tensor([float(self.rank + 1)], device=device)
            allreduce(self.tensor, op=ReduceOp.SUM, group_name="default")
            return self.tensor.cpu().item()

    actors = [Worker.remote(rank=i) for i in range(2)]
    create_collective_group(
        actors=actors,
        world_size=2,
        ranks=[0, 1],
        backend="NCCL",
        group_name="default",
    )

    ray.get([a.setup.remote(2) for a in actors])
    results = ray.get([a.compute.remote() for a in actors])

    assert results == [3.0, 3.0], f"Expected [3.0, 3.0], got {results}"

    ray.shutdown()
