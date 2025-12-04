"""Test the broadcast API."""
import pytest
import torch
import torch_npu

import ray
from ray.util.collective.tests.npu_util import create_collective_multinpu_workers
from ray.util.collective.types import Backend


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("src_npu_index", [0, 1])
def test_broadcast_different_name(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    backend,
    group_name,
    src_rank,
    src_npu_index,
):
    world_size = 2
    num_npu_per_worker = 2
    actors, _ = create_collective_multinpu_workers(
        num_workers=world_size, group_name=group_name, backend=backend
    )
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))

    results = ray.get(
        [
            a.do_broadcast_multinpu.remote(
                group_name=group_name, src_rank=src_rank, src_npu_index=src_npu_index
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            val = (src_rank + 1) * 2 + src_npu_index
            assert (
                results[i][j] == torch.ones([10], dtype=torch.float32).npu(j) * val
            ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("src_npu_index", [0, 1])
def test_broadcast_different_array_size(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    backend,
    array_size,
    src_rank,
    src_npu_index,
):
    world_size = 2
    num_npu_per_worker = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)
    ray.get(actors[0].set_buffer.remote([array_size], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([array_size], value0=4, value1=5))
    results = ray.get(
        [
            a.do_broadcast_multinpu.remote(
                src_rank=src_rank, src_npu_index=src_npu_index
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_npu_per_worker):
            val = (src_rank + 1) * 2 + src_npu_index
            assert (
                results[i][j] == torch.ones((array_size,), dtype=torch.float32).npu(j) * val
            ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("src_rank", [3, 4])
@pytest.mark.parametrize("src_npu_index", [2, 3])
def test_broadcast_invalid_rank(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    backend,
    src_rank,
    src_npu_index,
):
    world_size = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)
    with pytest.raises(ValueError):
        _ = ray.get(
            [
                a.do_broadcast_multinpu.remote(
                    src_rank=src_rank, src_npu_index=src_npu_index
                )
                for a in actors
            ]
        )
