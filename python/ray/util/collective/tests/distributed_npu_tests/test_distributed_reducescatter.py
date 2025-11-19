"""Test the collective reducescatter API on a distributed Ray cluster."""
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
@pytest.mark.parametrize("tensor_backend", ["torch"])  # cupy removed, only torch_npu
@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]]
)
def test_reducescatter_different_array_size(
    ray_start_distributed_2_nodes_4_npus, array_size, tensor_backend, backend
):
    world_size = 4
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend="torch"
    )
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i]
            == torch.ones(array_size, dtype=torch.float32).npu() * world_size
        ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize(
    "dtype",
    [
        torch.uint8,
        torch.float16,
        torch.float32,
        torch.float64,
    ],
)
def test_reducescatter_different_dtype(ray_start_distributed_2_nodes_4_npus, dtype, backend):
    world_size = 4
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i] == torch.ones(10, dtype=dtype).npu() * world_size).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_reducescatter_torch_cupy(ray_start_distributed_2_nodes_4_npus, backend):
    world_size = 4
    shape = [10, 10]
    actors, _ = create_collective_workers(world_size, backend=backend)

    # tensor is pytorch, list is pytorch (formerly cupy)
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).npu() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [torch.ones(shape, dtype=torch.float32).npu() for _ in range(world_size)]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == torch.ones(shape, dtype=torch.float32).npu() * world_size
        ).all()

    # tensor list is pytorch
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).npu() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32).npu() for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == torch.ones(shape, dtype=torch.float32).npu() * world_size
        ).all()

    # mixed case, rewritten for torch_npu only
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).npu() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            list_buffer.append(torch.ones(shape, dtype=torch.float32).npu())
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == torch.ones(shape, dtype=torch.float32).npu() * world_size
        ).all()

    # mixed case 2, rewritten for NPUs only
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).npu() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            list_buffer.append(torch.ones(shape, dtype=torch.float32).npu())
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == torch.ones(shape, dtype=torch.float32).npu() * world_size
        ).all()


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
