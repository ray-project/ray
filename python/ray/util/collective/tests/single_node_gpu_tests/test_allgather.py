"""Test the collective allgather API."""
import pytest
import ray

import cupy as cp
import torch

from ray.util.collective.tests.util import create_collective_workers, \
    init_tensors_for_gather_scatter


@pytest.mark.parametrize("tensor_backend", ["cupy", "torch"])
@pytest.mark.parametrize("array_size",
                         [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]])
def test_allgather_different_array_size(ray_start_single_node_2_gpus,
                                        array_size, tensor_backend):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend)
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            if tensor_backend == "cupy":
                assert (results[i][j] == cp.ones(array_size, dtype=cp.float32)
                        * (j + 1)).all()
            else:
                assert (results[i][j] == torch.ones(
                    array_size, dtype=torch.float32).cuda() * (j + 1)).all()


@pytest.mark.parametrize("dtype",
                         [cp.uint8, cp.float16, cp.float32, cp.float64])
def test_allgather_different_dtype(ray_start_single_node_2_gpus, dtype):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == cp.ones(10, dtype=dtype) * (j + 1)).all()


@pytest.mark.parametrize("length", [0, 1, 2, 3])
def test_unmatched_tensor_list_length(ray_start_single_node_2_gpus, length):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    list_buffer = [cp.ones(10, dtype=cp.float32) for _ in range(length)]
    ray.wait([a.set_list_buffer.remote(list_buffer) for a in actors])
    if length != world_size:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


@pytest.mark.parametrize("shape", [10, 20, [4, 5], [1, 3, 5, 7]])
def test_unmatched_tensor_shape(ray_start_single_node_2_gpus, shape):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, array_size=10)
    list_buffer = [cp.ones(shape, dtype=cp.float32) for _ in range(world_size)]
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])
    if shape != 10:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


def test_allgather_torch_cupy(ray_start_single_node_2_gpus):
    world_size = 2
    shape = [10, 10]
    actors, _ = create_collective_workers(world_size)

    # tensor is pytorch, list is cupy
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            cp.ones(shape, dtype=cp.float32) for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == cp.ones(shape, dtype=cp.float32) *
                    (j + 1)).all()

    # tensor is cupy, list is pytorch
    for i, a in enumerate(actors):
        t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32).cuda()
            for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == torch.ones(
                shape, dtype=torch.float32).cuda() * (j + 1)).all()

    # some tensors in the list are pytorch, some are cupy
    for i, a in enumerate(actors):
        t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(
                    torch.ones(shape, dtype=torch.float32).cuda())
            else:
                list_buffer.append(cp.ones(shape, dtype=cp.float32))
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            if j % 2 == 0:
                assert (results[i][j] == torch.ones(
                    shape, dtype=torch.float32).cuda() * (j + 1)).all()
            else:
                assert (results[i][j] == cp.ones(shape, dtype=cp.float32) *
                        (j + 1)).all()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-x", __file__]))
