"""Test the broadcast API."""
import torch
import torch_npu
import pytest

import ray
from ray.util.collective.tests.npu_util import create_collective_workers
from ray.util.collective.types import Backend


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("src_rank", [0, 1, 2, 3])
def test_broadcast_different_name(
    ray_start_distributed_multinpu_2_nodes_4_npus, group_name, src_rank, backend
):
    world_size = 4
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name, backend=backend
    )
    ray.wait(
        [
            a.set_buffer.remote(
                torch.ones((10,), dtype=torch.float32).npu() * (i + 2)
            )
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [
            a.do_broadcast.remote(group_name=group_name, src_rank=src_rank)
            for a in actors
        ]
    )
    for i in range(world_size):
        assert torch.equal(
            results[i],
            torch.ones((10,), dtype=torch.float32).npu() * (src_rank + 2),
        )


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("src_rank", [0, 1, 2, 3])
def test_broadcast_different_array_size(
    ray_start_distributed_multinpu_2_nodes_4_npus, array_size, src_rank, backend
):
    world_size = 4
    actors, _ = create_collective_workers(world_size, backend=backend)
    ray.wait(
        [
            a.set_buffer.remote(
                torch.ones((array_size,), dtype=torch.float32).npu() * (i + 2)
            )
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])
    for i in range(world_size):
        assert torch.equal(
            results[i],
            torch.ones((array_size,), dtype=torch.float32).npu() * (src_rank + 2),
        )


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("src_rank", [0, 1])
def test_broadcast_torch_cupy(
    ray_start_distributed_multinpu_2_nodes_4_npus, src_rank, backend
):
    import torch

    world_size = 4
    actors, _ = create_collective_workers(world_size, backend=backend)

    ray.wait(
        [
            actors[1].set_buffer.remote(
                torch.ones(10).npu() * world_size
            )
        ]
    )

    results = ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])

    if src_rank == 0:
        assert torch.equal(results[0], torch.ones((10,)).npu())
        assert torch.equal(results[1], torch.ones((10,)).npu())
    else:
        assert torch.equal(results[0], torch.ones((10,)).npu() * world_size)
        assert torch.equal(results[1], torch.ones((10,)).npu() * world_size)


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_broadcast_invalid_rank(
    ray_start_distributed_multinpu_2_nodes_4_npus, src_rank=3, backend=None
):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    with pytest.raises(ValueError):
        ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])
