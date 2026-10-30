"""Test the collective allreduce API on a distributed Ray cluster (XCCL)."""
import pytest
import torch

import ray
from ray.util.collective.tests.xpu_util import (
    WORLD_SIZES,
    cluster_world_size,
    create_collective_workers,
    require_gpus,
)
from ray.util.collective.types import ReduceOp


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("world_size", WORLD_SIZES)
def test_allreduce_different_name(
    ray_start_distributed_2_nodes_4_gpus, group_name, world_size
):
    require_gpus(world_size)
    actors, _ = create_collective_workers(num_workers=world_size, group_name=group_name)
    results = ray.get([a.do_allreduce.remote(group_name) for a in actors])
    assert (results[0] == torch.ones((10,), dtype=torch.float32) * world_size).all()
    assert (results[1] == torch.ones((10,), dtype=torch.float32) * world_size).all()


@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
def test_allreduce_different_array_size(
    ray_start_distributed_2_nodes_4_gpus, array_size
):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    ray.wait(
        [
            a.set_buffer.remote(torch.ones(array_size, dtype=torch.float32))
            for a in actors
        ]
    )
    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (
        results[0] == torch.ones((array_size,), dtype=torch.float32) * world_size
    ).all()
    assert (
        results[1] == torch.ones((array_size,), dtype=torch.float32) * world_size
    ).all()


def test_allreduce_destroy(
    ray_start_distributed_2_nodes_4_gpus, backend="XCCL", group_name="default"
):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)

    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (results[0] == torch.ones((10,), dtype=torch.float32) * world_size).all()
    assert (results[1] == torch.ones((10,), dtype=torch.float32) * world_size).all()

    # destroy the group and try do work, should fail
    ray.get([a.destroy_group.remote() for a in actors])
    with pytest.raises(RuntimeError):
        results = ray.get([a.do_allreduce.remote() for a in actors])

    # reinit the same group and all reduce
    ray.get(
        [
            actor.init_group.remote(world_size, i, backend, group_name)
            for i, actor in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (
        results[0] == torch.ones((10,), dtype=torch.float32) * world_size * world_size
    ).all()
    assert (
        results[1] == torch.ones((10,), dtype=torch.float32) * world_size * world_size
    ).all()


def test_allreduce_multiple_group(
    ray_start_distributed_2_nodes_4_gpus, backend="XCCL", num_groups=5
):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    for group_name in range(1, num_groups):
        ray.get(
            [
                actor.init_group.remote(world_size, i, backend, str(group_name))
                for i, actor in enumerate(actors)
            ]
        )
    for i in range(num_groups):
        group_name = "default" if i == 0 else str(i)
        results = ray.get([a.do_allreduce.remote(group_name) for a in actors])
        assert (
            results[0]
            == torch.ones((10,), dtype=torch.float32) * (world_size ** (i + 1))
        ).all()


def test_allreduce_different_op(ray_start_distributed_2_nodes_4_gpus):
    world_size = cluster_world_size(4)
    # values are (rank + 2): product over ranks and max, as in upstream (120 / 5 for 4 ranks)
    expected_prod = 1
    for i in range(world_size):
        expected_prod *= i + 2
    expected_max = world_size + 1
    actors, _ = create_collective_workers(world_size)

    # check product
    ray.wait(
        [
            a.set_buffer.remote(torch.ones(10, dtype=torch.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote(op=ReduceOp.PRODUCT) for a in actors])
    assert (results[0] == torch.ones((10,), dtype=torch.float32) * expected_prod).all()
    assert (results[1] == torch.ones((10,), dtype=torch.float32) * expected_prod).all()

    # check min
    ray.wait(
        [
            a.set_buffer.remote(torch.ones(10, dtype=torch.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote(op=ReduceOp.MIN) for a in actors])
    assert (results[0] == torch.ones((10,), dtype=torch.float32) * 2).all()
    assert (results[1] == torch.ones((10,), dtype=torch.float32) * 2).all()

    # check max
    ray.wait(
        [
            a.set_buffer.remote(torch.ones(10, dtype=torch.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote(op=ReduceOp.MAX) for a in actors])
    assert (results[0] == torch.ones((10,), dtype=torch.float32) * expected_max).all()
    assert (results[1] == torch.ones((10,), dtype=torch.float32) * expected_max).all()


@pytest.mark.parametrize(
    "dtype", [torch.uint8, torch.float16, torch.float32, torch.float64]
)
def test_allreduce_different_dtype(ray_start_distributed_2_nodes_4_gpus, dtype):
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    ray.wait([a.set_buffer.remote(torch.ones(10, dtype=dtype)) for a in actors])
    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (results[0] == torch.ones((10,), dtype=dtype) * world_size).all()
    assert (results[1] == torch.ones((10,), dtype=dtype) * world_size).all()


def test_allreduce_torch_cupy(ray_start_distributed_2_nodes_4_gpus):
    # Upstream mixes torch and cupy tensors. Here every rank uses torch XPU
    # tensors; the mismatch half uses a CPU tensor, which the XCCL group cannot
    # run, in place of the cupy tensor.
    world_size = cluster_world_size(4)
    actors, _ = create_collective_workers(world_size)
    ray.wait([actors[1].set_buffer.remote(torch.ones(10))])
    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (results[0] == torch.ones((10,)) * world_size).all()

    ray.wait([actors[0].set_buffer.remote(torch.ones(10), device="cpu")])
    ray.wait([actors[1].set_buffer.remote(torch.ones(10))])
    with pytest.raises(RuntimeError):
        results = ray.get([a.do_allreduce.remote() for a in actors])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
