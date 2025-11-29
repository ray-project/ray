"""Test the reduce API."""
import pytest
import torch
import torch_npu

import ray
from ray.util.collective.tests.npu_util import create_collective_workers
from ray.util.collective.types import Backend, ReduceOp


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1])
def test_reduce_different_name(ray_start_single_node_2_npus, group_name, dst_rank, backend):
    world_size = 2
    actors, _ = create_collective_workers(num_workers=world_size, group_name=group_name, backend=backend)
    results = ray.get([a.do_reduce.remote(group_name, dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu() * world_size).all()
        else:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu()).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("dst_rank", [0, 1])
def test_reduce_different_array_size(
    ray_start_single_node_2_npus, array_size, dst_rank, backend
):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    ray.wait(
        [a.set_buffer.remote(torch.ones(array_size, dtype=torch.float32).npu()) for a in actors]
    )
    results = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (
                results[i] == torch.ones((array_size,), dtype=torch.float32).npu() * world_size
            ).all()
        else:
            assert (results[i] == torch.ones((array_size,), dtype=torch.float32).npu()).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("dst_rank", [0, 1])
def test_reduce_multiple_group(ray_start_single_node_2_npus, dst_rank, backend, num_groups=5):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    for group_name in range(1, num_groups):
        ray.get(
            [
                actor.init_group.remote(world_size, i, backend, str(group_name))
                for i, actor in enumerate(actors)
            ]
        )
    for i in range(num_groups):
        group_name = "default" if i == 0 else str(i)
        results = ray.get(
            [
                a.do_reduce.remote(dst_rank=dst_rank, group_name=group_name)
                for a in actors
            ]
        )
        for j in range(world_size):
            if j == dst_rank:
                assert (results[j] == torch.ones((10,), dtype=torch.float32).npu() * (i + 2)).all()
            else:
                assert (results[j] == torch.ones((10,), dtype=torch.float32).npu()).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("dst_rank", [0, 1])
def test_reduce_different_op(ray_start_single_node_2_npus, dst_rank, backend):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)

    # check product
    ray.wait(
        [
            a.set_buffer.remote(torch.ones(10, dtype=torch.float32).npu() * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.PRODUCT) for a in actors]
    )
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu() * 6).all()
        else:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu() * (i + 2)).all()

    # check min
    ray.wait(
        [
            a.set_buffer.remote(torch.ones(10, dtype=torch.float32).npu() * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MIN) for a in actors]
    )
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu() * 2).all()
        else:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu() * (i + 2)).all()

    # check max
    ray.wait(
        [
            a.set_buffer.remote(torch.ones(10, dtype=torch.float32).npu() * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MAX) for a in actors]
    )
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu() * 3).all()
        else:
            assert (results[i] == torch.ones((10,), dtype=torch.float32).npu() * (i + 2)).all()


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_reduce_invalid_rank(ray_start_single_node_2_npus, backend, dst_rank=3):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    with pytest.raises(ValueError):
        ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])