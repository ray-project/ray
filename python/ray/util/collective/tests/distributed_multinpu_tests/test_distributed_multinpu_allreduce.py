"""Test the collective allreduce API on a distributed Ray cluster."""
import logging

import torch
import torch_npu
import pytest

import ray
from ray.util.collective.tests.npu_util import create_collective_multinpu_workers
from ray.util.collective.types import ReduceOp, Backend

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
def test_allreduce_multinpu_different_name(
    ray_start_distributed_multinpu_2_nodes_4_npus, backend, group_name
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker

    actors, _ = create_collective_multinpu_workers(
        num_workers=world_size, group_name=group_name, backend=backend
    )
    results = ray.get([a.do_allreduce_multinpu.remote(group_name) for a in actors])

    assert (results[0] == torch.ones((10,), dtype=torch.float32, device="npu") * actual_world_size).all()
    assert (results[1] == torch.ones((10,), dtype=torch.float32, device="npu") * actual_world_size).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
def test_allreduce_multinpu_different_array_size(
    ray_start_distributed_multinpu_2_nodes_4_npus, backend, array_size
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker

    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)
    ray.get([a.set_buffer.remote(array_size) for a in actors])
    results = ray.get([a.do_allreduce_multinpu.remote() for a in actors])

    assert (
        results[0] == torch.ones((array_size,), dtype=torch.float32, device="npu") * actual_world_size
    ).all()
    assert (
        results[1] == torch.ones((array_size,), dtype=torch.float32, device="npu") * actual_world_size
    ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_allreduce_multinpu_destroy(
    ray_start_distributed_multinpu_2_nodes_4_npus, backend, group_name="default"
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker

    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)

    results = ray.get([a.do_allreduce_multinpu.remote() for a in actors])
    assert (results[0] == torch.ones((10,), device="npu") * actual_world_size).all()
    assert (results[1] == torch.ones((10,), device="npu") * actual_world_size).all()

    # destroy the group and try do work, should fail
    ray.get([a.destroy_group.remote() for a in actors])
    with pytest.raises(RuntimeError):
        results = ray.get([a.do_allreduce_multinpu.remote() for a in actors])

    # reinit the same group and all reduce
    ray.get(
        [
            actor.init_group.remote(world_size, i, backend, group_name)
            for i, actor in enumerate(actors)
        ]
    )

    results = ray.get([a.do_allreduce_multinpu.remote() for a in actors])

    assert (
        results[0]
        == torch.ones((10,), device="npu") * actual_world_size * actual_world_size
    ).all()
    assert (
        results[1]
        == torch.ones((10,), device="npu") * actual_world_size * actual_world_size
    ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_allreduce_multinpu_multiple_group(
    ray_start_distributed_multinpu_2_nodes_4_npus, backend, num_groups=5
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker

    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)

    for group_name in range(1, num_groups):
        ray.get(
            [
                actor.init_group.remote(world_size, i, backend, str(group_name))
                for i, actor in enumerate(actors)
            ]
        )

    for i in range(num_groups):
        group_name = "default" if i == 0 else str(i)
        results = ray.get([a.do_allreduce_multinpu.remote(group_name) for a in actors])

        assert (
            results[0]
            == torch.ones((10,), device="npu") * (actual_world_size ** (i + 1))
        ).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_allreduce_multinpu_different_op(
    ray_start_distributed_multinpu_2_nodes_4_npus, backend
):
    world_size = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)

    # check product
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get(
        [a.do_allreduce_multinpu.remote(op=ReduceOp.PRODUCT) for a in actors]
    )
    assert (results[0] == torch.ones((10,), device="npu") * 120).all()
    assert (results[1] == torch.ones((10,), device="npu") * 120).all()

    # check min
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get([a.do_allreduce_multinpu.remote(op=ReduceOp.MIN) for a in actors])
    assert (results[0] == torch.ones((10,), device="npu") * 2).all()
    assert (results[1] == torch.ones((10,), device="npu") * 2).all()

    # check max
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))
    results = ray.get([a.do_allreduce_multinpu.remote(op=ReduceOp.MAX) for a in actors])
    assert (results[0] == torch.ones((10,), device="npu") * 5).all()
    assert (results[1] == torch.ones((10,), device="npu") * 5).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("dtype", [torch.uint8, torch.float16, torch.float32, torch.float64])
def test_allreduce_multinpu_different_dtype(
    ray_start_distributed_multinpu_2_nodes_4_npus, backend, dtype
):
    world_size = 2
    num_npu_per_worker = 2
    actual_world_size = world_size * num_npu_per_worker

    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)
    ray.get([a.set_buffer.remote([10], dtype=dtype) for a in actors])
    results = ray.get([a.do_allreduce_multinpu.remote() for a in actors])

    assert (results[0] == torch.ones((10,), dtype=dtype, device="npu") * actual_world_size).all()
    assert (results[1] == torch.ones((10,), dtype=dtype, device="npu") * actual_world_size).all()
