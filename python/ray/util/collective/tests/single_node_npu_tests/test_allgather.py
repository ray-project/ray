"""Test the collective allgather API."""
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
def test_allgather_different_array_size(
    ray_start_single_node_2_npus, array_size, tensor_backend, backend
):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (
                results[i][j]
                == torch.ones(array_size, dtype=torch.float32).npu() * (j + 1)
            ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("dtype", [torch.uint8, torch.float16, torch.float32, torch.float64])
def test_allgather_different_dtype(ray_start_single_node_2_npus, dtype, backend):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == torch.ones(10, dtype=dtype).npu() * (j + 1)).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("length", [0, 1, 2, 3])
def test_unmatched_tensor_list_length(ray_start_single_node_2_npus, length, backend):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    list_buffer = [torch.ones(10, dtype=torch.float32).npu() for _ in range(length)]
    ray.wait([a.set_list_buffer.remote(list_buffer) for a in actors])
    if length != world_size:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("shape", [10, 20, [4, 5], [1, 3, 5, 7]])
def test_unmatched_tensor_shape(ray_start_single_node_2_npus, shape, backend):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    init_tensors_for_gather_scatter(actors, array_size=10)
    list_buffer = [torch.ones(shape, dtype=torch.float32).npu() for _ in range(world_size)]
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])
    if shape != 10:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


if __name__ == "__main__":
    print("the function is being called")
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))