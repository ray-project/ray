"""Test the collective reducescatter API on a distributed Ray cluster."""
import pytest
import ray

import cupy as cp
import torch

from ray.util.collective.tests.util import (
    create_collective_workers,
    init_tensors_for_gather_scatter,
)


@pytest.mark.parametrize("tensor_backend", ["cupy", "torch"])
@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]]
)
def test_reducescatter_different_array_size(
    ray_start_distributed_2_nodes_4_gpus, array_size, tensor_backend
):
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if tensor_backend == "cupy":
            assert (
                results[i] == cp.ones(array_size, dtype=cp.float32) * world_size
            ).all()
        else:
            assert (
                results[i]
                == torch.ones(array_size, dtype=torch.float32).cuda() * world_size
            ).all()


@pytest.mark.parametrize("dtype", [cp.uint8, cp.float16, cp.float32, cp.float64])
def test_reducescatter_different_dtype(ray_start_distributed_2_nodes_4_gpus, dtype):
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i] == cp.ones(10, dtype=dtype) * world_size).all()


def test_reducescatter_torch_cupy(ray_start_distributed_2_nodes_4_gpus):
    world_size = 4
    shape = [10, 10]
    actors, _ = create_collective_workers(world_size)

    # tensor is pytorch, list is cupy
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [cp.ones(shape, dtype=cp.float32) for _ in range(world_size)]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == torch.ones(shape, dtype=torch.float32).cuda() * world_size
        ).all()

    # tensor is cupy, list is pytorch
    for i, a in enumerate(actors):
        t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32).cuda() for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (results[i] == cp.ones(shape, dtype=cp.float32) * world_size).all()

    # some tensors in the list are pytorch, some are cupy
    for i, a in enumerate(actors):
        if i % 2 == 0:
            t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        else:
            t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(torch.ones(shape, dtype=torch.float32).cuda())
            else:
                list_buffer.append(cp.ones(shape, dtype=cp.float32))
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if i % 2 == 0:
            assert (
                results[i] == torch.ones(shape, dtype=torch.float32).cuda() * world_size
            ).all()
        else:
            assert (results[i] == cp.ones(shape, dtype=cp.float32) * world_size).all()

    # mixed case
    for i, a in enumerate(actors):
        if i % 2 == 0:
            t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        else:
            t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(cp.ones(shape, dtype=cp.float32))
            else:
                list_buffer.append(torch.ones(shape, dtype=torch.float32).cuda())
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if i % 2 == 0:
            assert (
                results[i] == torch.ones(shape, dtype=torch.float32).cuda() * world_size
            ).all()
        else:
            assert (results[i] == cp.ones(shape, dtype=cp.float32) * world_size).all()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
