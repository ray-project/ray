"""Test the allgather API on a distributed Ray cluster."""
import pytest
import torch
import torch_npu

import ray
from ray.util.collective.types import Backend
from ray.util.collective.tests.npu_util import (
    create_collective_multinpu_workers,
    init_tensors_for_gather_scatter_multinpu,
)


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("tensor_backend", ["torch"])
@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]]
)
def test_allgather_different_array_size(
    ray_start_distributed_multinpu_2_nodes_4_npus, array_size, tensor_backend, backend
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter_multinpu(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_allgather_multinpu.remote() for a in actors])
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            for k in range(actual_world_size):
                assert (
                    results[i][j][k]
                    == torch.ones(array_size, dtype=torch.float32).npu(j)
                ).all()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))