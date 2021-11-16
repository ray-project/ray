"""Test the reduce API."""
import pytest
import cupy as cp
import ray
from ray.util.collective.types import ReduceOp

from ray.util.collective.tests.util import create_collective_multigpu_workers


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("dst_gpu_index", [0, 1])
def test_reduce_different_name(ray_start_distributed_multigpu_2_nodes_4_gpus,
                               group_name, dst_rank, dst_gpu_index):
    world_size = 2
    num_gpu_per_worker = 2
    actual_world_size = world_size * num_gpu_per_worker
    actors, _ = create_collective_multigpu_workers(
        num_workers=world_size, group_name=group_name)
    results = ray.get([
        a.do_reduce_multigpu.remote(
            group_name, dst_rank=dst_rank, dst_gpu_index=dst_gpu_index)
        for a in actors
    ])
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            if i == dst_rank and j == dst_gpu_index:
                assert (results[i][j] == cp.ones(
                    (10, ), dtype=cp.float32) * actual_world_size).all()
            else:
                assert (results[i][j] == cp.ones((10, ),
                                                 dtype=cp.float32)).all()


@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("dst_gpu_index", [0, 1])
def test_reduce_different_array_size(
        ray_start_distributed_multigpu_2_nodes_4_gpus, array_size, dst_rank,
        dst_gpu_index):
    world_size = 2
    num_gpu_per_worker = 2
    actual_world_size = world_size * num_gpu_per_worker
    actors, _ = create_collective_multigpu_workers(num_workers=world_size)

    ray.get(actors[0].set_buffer.remote(array_size))
    ray.get(actors[1].set_buffer.remote(array_size))
    results = ray.get([
        a.do_reduce_multigpu.remote(
            dst_rank=dst_rank, dst_gpu_index=dst_gpu_index) for a in actors
    ])
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            if i == dst_rank and j == dst_gpu_index:
                assert (results[i][j] == cp.ones(
                    (array_size, ), dtype=cp.float32) *
                        actual_world_size).all()
            else:
                assert (results[i][j] == cp.ones(
                    (array_size, ), dtype=cp.float32)).all()


@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("dst_gpu_index", [0, 1])
def test_reduce_different_op(ray_start_distributed_multigpu_2_nodes_4_gpus,
                             dst_rank, dst_gpu_index):
    world_size = 2
    num_gpu_per_worker = 2
    actors, _ = create_collective_multigpu_workers(world_size)

    # check product
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get([
        a.do_reduce_multigpu.remote(
            dst_rank=dst_rank,
            dst_gpu_index=dst_gpu_index,
            op=ReduceOp.PRODUCT) for a in actors
    ])
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            if i == dst_rank and j == dst_gpu_index:
                assert (results[i][j] == cp.ones(
                    (10, ), dtype=cp.float32) * 120).all()
            else:
                val = (i + 1) * 2 + j
                assert (results[i][j] == cp.ones(
                    (10, ), dtype=cp.float32) * val).all()

    # check min
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get([
        a.do_reduce_multigpu.remote(
            dst_rank=dst_rank, dst_gpu_index=dst_gpu_index, op=ReduceOp.MIN)
        for a in actors
    ])
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            if i == dst_rank and j == dst_gpu_index:
                assert (results[i][j] == cp.ones(
                    (10, ), dtype=cp.float32) * 2).all()
            else:
                val = (i + 1) * 2 + j
                assert (results[i][j] == cp.ones(
                    (10, ), dtype=cp.float32) * val).all()

    # check max
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get([
        a.do_reduce_multigpu.remote(
            dst_rank=dst_rank, dst_gpu_index=dst_gpu_index, op=ReduceOp.MAX)
        for a in actors
    ])
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            if i == dst_rank and j == dst_gpu_index:
                assert (results[i][j] == cp.ones(
                    (10, ), dtype=cp.float32) * 5).all()
            else:
                val = (i + 1) * 2 + j
                assert (results[i][j] == cp.ones(
                    (10, ), dtype=cp.float32) * val).all()


@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("dst_gpu_index", [0, 1])
def test_reduce_torch_cupy(ray_start_distributed_multigpu_2_nodes_4_gpus,
                           dst_rank, dst_gpu_index):
    import torch
    world_size = 2
    num_gpu_per_worker = 2
    actors, _ = create_collective_multigpu_workers(world_size)
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote(
        [10], value0=4, value1=5, tensor_type0="torch", tensor_type1="torch"))

    results = ray.get([
        a.do_reduce_multigpu.remote(
            dst_rank=dst_rank, dst_gpu_index=dst_gpu_index) for a in actors
    ])

    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            val = (i + 1) * 2 + j
            if dst_rank == i and dst_gpu_index == j:
                if i == 0:
                    assert (results[i][j] == cp.ones([10], dtype=cp.float32) *
                            14).all()
                else:
                    assert (
                        results[i][j] == torch.ones([10]).cuda(j) * 14).all()
            else:
                if i == 0:
                    assert (results[i][j] == cp.ones([10], dtype=cp.float32) *
                            val).all()
                else:
                    assert (
                        results[i][j] == torch.ones([10]).cuda(j) * val).all()


@pytest.mark.parametrize("dst_rank", [3, 4])
@pytest.mark.parametrize("dst_gpu_index", [2, 3])
def test_reduce_invalid_rank(ray_start_distributed_multigpu_2_nodes_4_gpus,
                             dst_rank, dst_gpu_index):
    world_size = 2
    actors, _ = create_collective_multigpu_workers(world_size)
    with pytest.raises(ValueError):
        _ = ray.get([
            a.do_reduce_multigpu.remote(
                dst_rank=dst_rank, dst_gpu_index=dst_gpu_index) for a in actors
        ])
