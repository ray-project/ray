"""Test the reduce API."""
import torch
import torch_npu
import pytest

import ray
from ray.util.collective.tests.npu_util import create_collective_multinpu_workers
from ray.util.collective.types import ReduceOp, Backend


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("dst_npu_index", [0, 1])
def test_reduce_different_name(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    backend,
    group_name,
    dst_rank,
    dst_npu_index,
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker
    actors, _ = create_collective_multinpu_workers(
        num_workers=world_size, group_name=group_name, backend=backend
    )
    results = ray.get(
        [
            a.do_reduce_multinpu.remote(
                group_name, dst_rank=dst_rank, dst_npu_index=dst_npu_index
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            if i == dst_rank and j == dst_npu_index:
                assert (
                    results[i][j]
                    == torch.ones((10,), dtype=torch.float32).npu(j) * actual_world_size
                ).all()
            else:
                assert (
                    results[i][j] == torch.ones((10,), dtype=torch.float32).npu(j)
                ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("dst_npu_index", [0, 1])
def test_reduce_different_array_size(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    backend,
    array_size,
    dst_rank,
    dst_npu_index,
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker
    actors, _ = create_collective_multinpu_workers(
        num_workers=world_size, backend=backend
    )

    ray.get(actors[0].set_buffer.remote(array_size))
    ray.get(actors[1].set_buffer.remote(array_size))
    results = ray.get(
        [
            a.do_reduce_multinpu.remote(dst_rank=dst_rank, dst_npu_index=dst_npu_index)
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            if i == dst_rank and j == dst_npu_index:
                assert (
                    results[i][j]
                    == torch.ones((array_size,), dtype=torch.float32).npu(j)
                    * actual_world_size
                ).all()
            else:
                assert (
                    results[i][j]
                    == torch.ones((array_size,), dtype=torch.float32).npu(j)
                ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("dst_npu_index", [0, 1])
def test_reduce_different_op(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    backend,
    dst_rank,
    dst_npu_index,
):
    world_size = 2
    num_npu_per_worker = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)

    # check product
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get(
        [
            a.do_reduce_multinpu.remote(
                dst_rank=dst_rank, dst_npu_index=dst_npu_index, op=ReduceOp.PRODUCT
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            if i == dst_rank and j == dst_npu_index:
                assert (
                    results[i][j]
                    == torch.ones((10,), dtype=torch.float32).npu(j) * 120
                ).all()
            else:
                val = (i + 1) * 2 + j
                assert (
                    results[i][j]
                    == torch.ones((10,), dtype=torch.float32).npu(j) * val
                ).all()

    # check min
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get(
        [
            a.do_reduce_multinpu.remote(
                dst_rank=dst_rank, dst_npu_index=dst_npu_index, op=ReduceOp.MIN
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            if i == dst_rank and j == dst_npu_index:
                assert (
                    results[i][j]
                    == torch.ones((10,), dtype=torch.float32).npu(j) * 2
                ).all()
            else:
                val = (i + 1) * 2 + j
                assert (
                    results[i][j]
                    == torch.ones((10,), dtype=torch.float32).npu(j) * val
                ).all()

    # check max
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get(
        [
            a.do_reduce_multinpu.remote(
                dst_rank=dst_rank, dst_npu_index=dst_npu_index, op=ReduceOp.MAX
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            if i == dst_rank and j == dst_npu_index:
                assert (
                    results[i][j]
                    == torch.ones((10,), dtype=torch.float32).npu(j) * 5
                ).all()
            else:
                val = (i + 1) * 2 + j
                assert (
                    results[i][j]
                    == torch.ones((10,), dtype=torch.float32).npu(j) * val
                ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("dst_rank", [3, 4])
@pytest.mark.parametrize("dst_npu_index", [2, 3])
def test_reduce_invalid_rank(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    backend,
    dst_rank,
    dst_npu_index,
):
    world_size = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)
    with pytest.raises(ValueError):
        _ = ray.get(
            [
                a.do_reduce_multinpu.remote(
                    dst_rank=dst_rank, dst_npu_index=dst_npu_index
                )
                for a in actors
            ]
        )
