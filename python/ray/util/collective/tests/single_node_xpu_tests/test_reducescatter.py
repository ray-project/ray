"""Test the collective reducescatter API (XCCL)."""
import pytest
import torch

import ray
from ray.util.collective.tests.xpu_util import (
    create_collective_workers,
    init_tensors_for_gather_scatter,
)


@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]]
)
def test_reducescatter_different_array_size(ray_start_single_node_2_gpus, array_size):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, array_size=array_size)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == torch.ones(array_size, dtype=torch.float32) * world_size
        ).all()


@pytest.mark.parametrize(
    "dtype", [torch.uint8, torch.float16, torch.float32, torch.float64]
)
def test_reducescatter_different_dtype(ray_start_single_node_2_gpus, dtype):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i] == torch.ones(10, dtype=dtype) * world_size).all()


def test_reducescatter_torch_cupy(ray_start_single_node_2_gpus):
    # Upstream runs four phases mixing torch and cupy; with torch only they
    # collapse into this single phase.
    world_size = 2
    shape = [10, 10]
    actors, _ = create_collective_workers(world_size)

    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32) for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (results[i] == torch.ones(shape, dtype=torch.float32) * world_size).all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
