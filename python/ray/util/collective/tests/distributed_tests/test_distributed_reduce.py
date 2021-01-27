"""Test the reduce API."""
import pytest
import cupy as cp
import ray
from ray.util.collective.types import ReduceOp

from ray.util.collective.tests.util import create_collective_workers


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1, 2, 3])
def test_reduce_different_name(ray_start_distributed_2_nodes_4_gpus,
                               group_name, dst_rank):
    world_size = 4
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name)
    results = ray.get(
        [a.do_reduce.remote(group_name, dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones(
                (10, ), dtype=cp.float32) * world_size).all()
        else:
            assert (results[i] == cp.ones((10, ), dtype=cp.float32)).all()


@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("dst_rank", [0, 1, 2, 3])
def test_reduce_different_array_size(ray_start_distributed_2_nodes_4_gpus,
                                     array_size, dst_rank):
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    ray.wait([
        a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32))
        for a in actors
    ])
    results = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones(
                (array_size, ), dtype=cp.float32) * world_size).all()
        else:
            assert (results[i] == cp.ones((array_size, ),
                                          dtype=cp.float32)).all()


@pytest.mark.parametrize("dst_rank", [0, 1, 2, 3])
def test_reduce_different_op(ray_start_distributed_2_nodes_4_gpus, dst_rank):
    world_size = 4
    actors, _ = create_collective_workers(world_size)

    # check product
    ray.wait([
        a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([
        a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.PRODUCT)
        for a in actors
    ])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones(
                (10, ), dtype=cp.float32) * 120).all()
        else:
            assert (results[i] == cp.ones(
                (10, ), dtype=cp.float32) * (i + 2)).all()

    # check min
    ray.wait([
        a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([
        a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MIN) for a in actors
    ])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones((10, ), dtype=cp.float32) * 2).all()
        else:
            assert (results[i] == cp.ones(
                (10, ), dtype=cp.float32) * (i + 2)).all()

    # check max
    ray.wait([
        a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([
        a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MAX) for a in actors
    ])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones((10, ), dtype=cp.float32) * 5).all()
        else:
            assert (results[i] == cp.ones(
                (10, ), dtype=cp.float32) * (i + 2)).all()


@pytest.mark.parametrize("dst_rank", [0, 1])
def test_reduce_torch_cupy(ray_start_distributed_2_nodes_4_gpus, dst_rank):
    import torch
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    ray.wait([actors[1].set_buffer.remote(torch.ones(10, ).cuda())])
    results = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
    if dst_rank == 0:
        assert (results[0] == cp.ones((10, )) * world_size).all()
        assert (results[1] == torch.ones((10, )).cuda()).all()
    else:
        assert (results[0] == cp.ones((10, ))).all()
        assert (results[1] == torch.ones((10, )).cuda() * world_size).all()


def test_reduce_invalid_rank(ray_start_distributed_2_nodes_4_gpus, dst_rank=7):
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    with pytest.raises(ValueError):
        _ = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
