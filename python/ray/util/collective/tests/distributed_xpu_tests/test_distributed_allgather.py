"""Test the allgather API on a distributed Ray cluster (XCCL)."""
import pytest
import torch

import ray
from ray.util.collective.tests.xpu_util import (
    cluster_world_size,
    create_collective_workers,
    init_tensors_for_gather_scatter,
)


@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]]
)
def test_allgather_different_array_size(
    ray_start_distributed_2_nodes_4_gpus, array_size
):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, array_size=array_size)
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (
                results[i][j] == torch.ones(array_size, dtype=torch.float32) * (j + 1)
            ).all()


@pytest.mark.parametrize(
    "dtype", [torch.uint8, torch.float16, torch.float32, torch.float64]
)
def test_allgather_different_dtype(ray_start_distributed_2_nodes_4_gpus, dtype):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == torch.ones(10, dtype=dtype) * (j + 1)).all()


@pytest.mark.parametrize("length", [0, 1, 3, 4, 7, 8])
def test_unmatched_tensor_list_length(ray_start_distributed_2_nodes_4_gpus, length):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    list_buffer = [torch.ones(10, dtype=torch.float32) for _ in range(length)]
    ray.wait([a.set_list_buffer.remote(list_buffer) for a in actors])
    if length != world_size:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


@pytest.mark.parametrize("shape", [10, 20, [4, 5], [1, 3, 5, 7]])
def test_unmatched_tensor_shape(ray_start_distributed_2_nodes_4_gpus, shape):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    init_tensors_for_gather_scatter(actors, array_size=10)
    list_buffer = [torch.ones(shape, dtype=torch.float32) for _ in range(world_size)]
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])
    if shape != 10:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


def test_allgather_torch_cupy(ray_start_distributed_2_nodes_4_gpus):
    # Upstream runs three phases mixing torch and cupy; with torch only they
    # collapse into this single phase.
    world_size = cluster_world_size(4)
    shape = [10, 10]
    actors, _ = create_collective_workers(world_size)

    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32) * (i + 1)
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
