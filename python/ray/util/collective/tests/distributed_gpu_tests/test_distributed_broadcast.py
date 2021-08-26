"""Test the broadcast API."""
import pytest
import cupy as cp
import ray

from ray.util.collective.tests.util import create_collective_workers


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("src_rank", [0, 1, 2, 3])
def test_broadcast_different_name(ray_start_distributed_2_nodes_4_gpus,
                                  group_name, src_rank):
    world_size = 4
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name)
    ray.wait([
        a.set_buffer.remote(cp.ones((10, ), dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([
        a.do_broadcast.remote(group_name=group_name, src_rank=src_rank)
        for a in actors
    ])
    for i in range(world_size):
        assert (results[i] == cp.ones(
            (10, ), dtype=cp.float32) * (src_rank + 2)).all()


@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("src_rank", [0, 1, 2, 3])
def test_broadcast_different_array_size(ray_start_distributed_2_nodes_4_gpus,
                                        array_size, src_rank):
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    ray.wait([
        a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get(
        [a.do_broadcast.remote(src_rank=src_rank) for a in actors])
    for i in range(world_size):
        assert (results[i] == cp.ones(
            (array_size, ), dtype=cp.float32) * (src_rank + 2)).all()


@pytest.mark.parametrize("src_rank", [0, 1])
def test_broadcast_torch_cupy(ray_start_distributed_2_nodes_4_gpus, src_rank):
    import torch
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    ray.wait(
        [actors[1].set_buffer.remote(torch.ones(10, ).cuda() * world_size)])
    results = ray.get(
        [a.do_broadcast.remote(src_rank=src_rank) for a in actors])
    if src_rank == 0:
        assert (results[0] == cp.ones((10, ))).all()
        assert (results[1] == torch.ones((10, )).cuda()).all()
    else:
        assert (results[0] == cp.ones((10, )) * world_size).all()
        assert (results[1] == torch.ones((10, )).cuda() * world_size).all()


def test_broadcast_invalid_rank(ray_start_distributed_2_nodes_4_gpus,
                                src_rank=3):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    with pytest.raises(ValueError):
        ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])
