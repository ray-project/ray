"""Test the reduce API."""
import pytest
import numpy as np
import ray
from ray.util.collective.types import Backend, ReduceOp

from ray.util.collective.tests.cpu_util import create_collective_workers


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 2, 5, 6, 7])
def test_reduce_different_name(ray_start_distributed_2_nodes, group_name,
                               backend, dst_rank):
    world_size = 8
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name, backend=backend)
    results = ray.get(
        [a.do_reduce.remote(group_name, dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == np.ones(
                (10, ), dtype=np.float32) * world_size).all()
        else:
            assert (results[i] == np.ones((10, ), dtype=np.float32)).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("dst_rank", [0, 2, 5, 6, 7])
def test_reduce_different_array_size(ray_start_distributed_2_nodes, backend,
                                     array_size, dst_rank):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    ray.wait([
        a.set_buffer.remote(np.ones(array_size, dtype=np.float32))
        for a in actors
    ])
    results = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == np.ones(
                (array_size, ), dtype=np.float32) * world_size).all()
        else:
            assert (results[i] == np.ones((array_size, ),
                                          dtype=np.float32)).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("dst_rank", [0, 2, 5, 6, 7])
def test_reduce_different_op(ray_start_distributed_2_nodes, backend, dst_rank):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)

    # check product
    ray.wait([
        a.set_buffer.remote(np.ones(10, dtype=np.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([
        a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.PRODUCT)
        for a in actors
    ])

    product = 1
    for i in range(world_size):
        product = product * (i + 2)
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == np.ones(
                (10, ), dtype=np.float32) * product).all()
        else:
            assert (results[i] == np.ones(
                (10, ), dtype=np.float32) * (i + 2)).all()
    # check min
    ray.wait([
        a.set_buffer.remote(np.ones(10, dtype=np.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([
        a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MIN) for a in actors
    ])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == np.ones((10, ), dtype=np.float32) * 2).all()
        else:
            assert (results[i] == np.ones(
                (10, ), dtype=np.float32) * (i + 2)).all()

    # check max
    ray.wait([
        a.set_buffer.remote(np.ones(10, dtype=np.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([
        a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MAX) for a in actors
    ])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == np.ones(
                (10, ), dtype=np.float32) * (world_size + 1)).all()
        else:
            assert (results[i] == np.ones(
                (10, ), dtype=np.float32) * (i + 2)).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("dst_rank", [0, 2, 5, 6, 7])
def test_reduce_torch_numpy(ray_start_distributed_2_nodes, backend, dst_rank):
    import torch
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    ray.get([actors[1].set_buffer.remote(torch.ones(10, ))])
    results = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
    if dst_rank == 0:
        assert (results[0] == np.ones((10, )) * world_size).all()
        assert (results[1] == torch.ones((10, ))).all()
    else:
        assert (results[0] == np.ones((10, ))).all()
        assert (results[1] == torch.ones((10, ))).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
def test_reduce_invalid_rank(ray_start_distributed_2_nodes,
                             backend,
                             dst_rank=9):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    with pytest.raises(ValueError):
        _ = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-x", __file__]))
