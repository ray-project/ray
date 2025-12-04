"""Test the collective reducescatter API."""
import pytest
import torch
import torch_npu

import ray
from ray.util.collective.tests.npu_util import (
    create_collective_workers,
    init_tensors_for_gather_scatter,
)
from ray.util.collective.types import Backend


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("tensor_backend", ["torch"])
@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]]
)
def test_reducescatter_different_array_size(
    ray_start_single_node_2_npus, array_size, tensor_backend, backend
):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i]
            == torch.ones(array_size, dtype=torch.float32).npu() * world_size
        ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("dtype", [torch.uint8, torch.float16, torch.float32, torch.float64])
def test_reducescatter_different_dtype(ray_start_single_node_2_npus, dtype, backend):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i] == torch.ones(10, dtype=dtype).npu() * world_size).all()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))