"""Test the collective reducescatter API on a distributed Ray cluster."""
import pytest
import ray

import numpy as np
import torch

from ray.util.collective.types import Backend
from ray.util.collective.tests.cpu_util import create_collective_workers, \
    init_tensors_for_gather_scatter


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("tensor_backend", ["numpy", "torch"])
@pytest.mark.parametrize("array_size",
                         [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]])
def test_reducescatter_different_array_size(
        ray_start_distributed_2_nodes, array_size, tensor_backend, backend):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if tensor_backend == "numpy":
            assert (results[i] == np.ones(array_size, dtype=np.float32) *
                    world_size).all()
        else:
            assert (results[i] == torch.ones(array_size, dtype=torch.float32) *
                    world_size).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("dtype",
                         [np.uint8, np.float16, np.float32, np.float64])
def test_reducescatter_different_dtype(ray_start_distributed_2_nodes, dtype,
                                       backend):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i] == np.ones(10, dtype=dtype) * world_size).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
def test_reducescatter_torch_numpy(ray_start_distributed_2_nodes, backend):
    world_size = 8
    shape = [10, 10]
    actors, _ = create_collective_workers(world_size, backend=backend)

    # tensor is pytorch, list is numpy
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            np.ones(shape, dtype=np.float32) for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (results[i] == torch.ones(shape, dtype=torch.float32) *
                world_size).all()

    # tensor is numpy, list is pytorch
    for i, a in enumerate(actors):
        t = np.ones(shape, dtype=np.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32) for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == np.ones(shape, dtype=np.float32) * world_size).all()

    # some tensors in the list are pytorch, some are numpy
    for i, a in enumerate(actors):
        if i % 2 == 0:
            t = torch.ones(shape, dtype=torch.float32) * (i + 1)
        else:
            t = np.ones(shape, dtype=np.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(torch.ones(shape, dtype=torch.float32))
            else:
                list_buffer.append(np.ones(shape, dtype=np.float32))
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if i % 2 == 0:
            assert (results[i] == torch.ones(shape, dtype=torch.float32) *
                    world_size).all()
        else:
            assert (results[i] == np.ones(shape, dtype=np.float32) *
                    world_size).all()

    # mixed case
    for i, a in enumerate(actors):
        if i % 2 == 0:
            t = torch.ones(shape, dtype=torch.float32) * (i + 1)
        else:
            t = np.ones(shape, dtype=np.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(np.ones(shape, dtype=np.float32))
            else:
                list_buffer.append(torch.ones(shape, dtype=torch.float32))
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if i % 2 == 0:
            assert (results[i] == torch.ones(shape, dtype=torch.float32) *
                    world_size).all()
        else:
            assert (results[i] == np.ones(shape, dtype=np.float32) *
                    world_size).all()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-x", __file__]))
