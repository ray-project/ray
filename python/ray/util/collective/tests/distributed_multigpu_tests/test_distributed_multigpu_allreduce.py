"""Test the collective allreduice API on a distributed Ray cluster."""
import pytest
import logging

import cupy as cp

import ray
from ray.util.collective.types import ReduceOp
from ray.util.collective.tests.util import create_collective_multigpu_workers

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
def test_allreduce_multigpu_different_name(
    ray_start_distributed_multigpu_2_nodes_4_gpus, group_name
):
    world_size = 2
    num_gpu_per_worker = 2
    actual_world_size = world_size * num_gpu_per_worker
    actors, _ = create_collective_multigpu_workers(
        num_workers=world_size, group_name=group_name
    )
    results = ray.get([a.do_allreduce_multigpu.remote(group_name) for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * actual_world_size).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * actual_world_size).all()


@pytest.mark.parametrize("array_size", [2, 2 ** 5, 2 ** 10, 2 ** 15, 2 ** 20])
def test_allreduce_multigpu_different_array_size(
    ray_start_distributed_multigpu_2_nodes_4_gpus, array_size
):
    world_size = 2
    num_gpu_per_worker = 2
    actual_world_size = world_size * num_gpu_per_worker
    actors, _ = create_collective_multigpu_workers(world_size)
    ray.get([a.set_buffer.remote(array_size) for a in actors])
    results = ray.get([a.do_allreduce_multigpu.remote() for a in actors])
    assert (
        results[0] == cp.ones((array_size,), dtype=cp.float32) * actual_world_size
    ).all()
    assert (
        results[1] == cp.ones((array_size,), dtype=cp.float32) * actual_world_size
    ).all()


def test_allreduce_multigpu_destroy(
    ray_start_distributed_multigpu_2_nodes_4_gpus, backend="nccl", group_name="default"
):
    world_size = 2
    num_gpu_per_worker = 2
    actual_world_size = world_size * num_gpu_per_worker
    actors, _ = create_collective_multigpu_workers(world_size)

    results = ray.get([a.do_allreduce_multigpu.remote() for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * actual_world_size).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * actual_world_size).all()

    # destroy the group and try do work, should fail
    ray.get([a.destroy_group.remote() for a in actors])
    with pytest.raises(RuntimeError):
        results = ray.get([a.do_allreduce_multigpu.remote() for a in actors])

    # reinit the same group and all reduce
    ray.get(
        [
            actor.init_group.remote(world_size, i, backend, group_name)
            for i, actor in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce_multigpu.remote() for a in actors])
    assert (
        results[0]
        == cp.ones((10,), dtype=cp.float32) * actual_world_size * actual_world_size
    ).all()
    assert (
        results[1]
        == cp.ones((10,), dtype=cp.float32) * actual_world_size * actual_world_size
    ).all()


def test_allreduce_multigpu_multiple_group(
    ray_start_distributed_multigpu_2_nodes_4_gpus, backend="nccl", num_groups=5
):
    world_size = 2
    num_gpu_per_worker = 2
    actual_world_size = world_size * num_gpu_per_worker
    actors, _ = create_collective_multigpu_workers(world_size)
    for group_name in range(1, num_groups):
        ray.get(
            [
                actor.init_group.remote(world_size, i, backend, str(group_name))
                for i, actor in enumerate(actors)
            ]
        )
    for i in range(num_groups):
        group_name = "default" if i == 0 else str(i)
        results = ray.get([a.do_allreduce_multigpu.remote(group_name) for a in actors])
        assert (
            results[0]
            == cp.ones((10,), dtype=cp.float32) * (actual_world_size ** (i + 1))
        ).all()


def test_allreduce_multigpu_different_op(ray_start_distributed_multigpu_2_nodes_4_gpus):
    world_size = 2
    actors, _ = create_collective_multigpu_workers(world_size)
    # check product
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get(
        [a.do_allreduce_multigpu.remote(op=ReduceOp.PRODUCT) for a in actors]
    )
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * 120).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * 120).all()

    # check min
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get([a.do_allreduce_multigpu.remote(op=ReduceOp.MIN) for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * 2).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * 2).all()

    # check max
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get([a.do_allreduce_multigpu.remote(op=ReduceOp.MAX) for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * 5).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * 5).all()


@pytest.mark.parametrize("dtype", [cp.uint8, cp.float16, cp.float32, cp.float64])
def test_allreduce_multigpu_different_dtype(
    ray_start_distributed_multigpu_2_nodes_4_gpus, dtype
):
    world_size = 2
    num_gpu_per_worker = 2
    actual_world_size = world_size * num_gpu_per_worker
    actors, _ = create_collective_multigpu_workers(world_size)
    ray.get([a.set_buffer.remote([10], dtype=dtype) for a in actors])
    results = ray.get([a.do_allreduce_multigpu.remote() for a in actors])
    assert (results[0] == cp.ones((10,), dtype=dtype) * actual_world_size).all()
    assert (results[1] == cp.ones((10,), dtype=dtype) * actual_world_size).all()


def test_allreduce_torch_cupy(ray_start_distributed_multigpu_2_nodes_4_gpus):
    # import torch
    world_size = 2
    actual_world_size = 4
    actors, _ = create_collective_multigpu_workers(world_size)
    ray.get(actors[0].set_buffer.remote([10]))
    ray.get(
        actors[1].set_buffer.remote([10], tensor_type0="torch", tensor_type1="torch")
    )
    results = ray.get([a.do_allreduce_multigpu.remote() for a in actors])
    assert (results[0] == cp.ones((10,)) * actual_world_size).all()

    ray.get(
        actors[0].set_buffer.remote([10], tensor_type0="cupy", tensor_type1="torch")
    )
    ray.get(
        actors[1].set_buffer.remote([10], tensor_type0="torch", tensor_type1="cupy")
    )
    results = ray.get([a.do_allreduce_multigpu.remote() for a in actors])
    assert (results[0] == cp.ones((10,)) * actual_world_size).all()
