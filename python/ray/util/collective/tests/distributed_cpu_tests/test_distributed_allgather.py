"""Test the allgather API on a distributed Ray cluster."""
import pytest
import ray

import numpy as np
import torch

from ray.util.collective.types import Backend
from ray.util.collective.tests.cpu_util import (
    create_collective_workers,
    init_tensors_for_gather_scatter,
)


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("tensor_backend", ["numpy", "torch"])
@pytest.mark.parametrize(
    "array_size", [2, 2 ** 5, 2 ** 10, 2 ** 15, 2 ** 20, [2, 2], [5, 5, 5]]
)
def test_allgather_different_array_size(
    ray_start_distributed_2_nodes, array_size, tensor_backend, backend
):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            if tensor_backend == "numpy":
                assert (
                    results[i][j] == np.ones(array_size, dtype=np.float32) * (j + 1)
                ).all()
            else:
                assert (
                    results[i][j]
                    == torch.ones(array_size, dtype=torch.float32) * (j + 1)
                ).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("dtype", [np.uint8, np.float16, np.float32, np.float64])
def test_allgather_different_dtype(ray_start_distributed_2_nodes, dtype, backend):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == np.ones(10, dtype=dtype) * (j + 1)).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("length", [0, 1, 3, 4, 7, 8])
def test_unmatched_tensor_list_length(ray_start_distributed_2_nodes, length, backend):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    list_buffer = [np.ones(10, dtype=np.float32) for _ in range(length)]
    ray.wait([a.set_list_buffer.remote(list_buffer, copy=True) for a in actors])
    if length != world_size:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("shape", [10, 20, [4, 5], [1, 3, 5, 7]])
def test_unmatched_tensor_shape(ray_start_distributed_2_nodes, shape, backend):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(actors, array_size=10)
    list_buffer = [np.ones(shape, dtype=np.float32) for _ in range(world_size)]
    ray.get([a.set_list_buffer.remote(list_buffer, copy=True) for a in actors])
    if shape != 10:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


@pytest.mark.parametrize("backend", [Backend.GLOO])
def test_allgather_torch_numpy(ray_start_distributed_2_nodes, backend):
    world_size = 8
    shape = [10, 10]
    actors, _ = create_collective_workers(world_size, backend=backend)

    # tensor is pytorch, list is numpy
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [np.ones(shape, dtype=np.float32) for _ in range(world_size)]
        ray.wait([a.set_list_buffer.remote(list_buffer, copy=True)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == np.ones(shape, dtype=np.float32) * (j + 1)).all()

    # tensor is numpy, list is pytorch
    for i, a in enumerate(actors):
        t = np.ones(shape, dtype=np.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32) for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (
                results[i][j] == torch.ones(shape, dtype=torch.float32) * (j + 1)
            ).all()

    # some tensors in the list are pytorch, some are numpy
    for i, a in enumerate(actors):
        t = np.ones(shape, dtype=np.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(torch.ones(shape, dtype=torch.float32))
            else:
                list_buffer.append(np.ones(shape, dtype=np.float32))
        ray.wait([a.set_list_buffer.remote(list_buffer, copy=True)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            if j % 2 == 0:
                assert (
                    results[i][j] == torch.ones(shape, dtype=torch.float32) * (j + 1)
                ).all()
            else:
                assert (
                    results[i][j] == np.ones(shape, dtype=np.float32) * (j + 1)
                ).all()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
