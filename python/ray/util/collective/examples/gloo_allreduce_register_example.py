import torch

import ray
from ray.util.collective import (
    allreduce,
    create_collective_group,
    init_collective_group,
)
from ray.util.collective.backend_registry import get_backend_registry
from ray.util.collective.types import ReduceOp


def test_gloo_via_registry():
    ray.init()

    registry = get_backend_registry()
    assert "GLOO" in registry.list_backends()
    assert registry.check("GLOO")

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank
            self.tensor = None

        def setup(self, world_size):
            init_collective_group(
                world_size=world_size,
                rank=self.rank,
                backend="GLOO",
                group_name="default",
                gloo_timeout=30000,
            )

        def compute(self):
            self.tensor = torch.tensor([self.rank + 1], dtype=torch.float32)
            allreduce(self.tensor, op=ReduceOp.SUM)
            return self.tensor.item()

    actors = [Worker.remote(rank=i) for i in range(2)]
    create_collective_group(
        actors=actors,
        world_size=2,
        ranks=[0, 1],
        backend="GLOO",
        group_name="default",
        gloo_timeout=30000,
    )

    ray.get([a.setup.remote(2) for a in actors])
    results = ray.get([a.compute.remote() for a in actors])

    assert results == [3.0, 3.0], f"Expected [3.0, 3.0], got {results}"

    ray.shutdown()
