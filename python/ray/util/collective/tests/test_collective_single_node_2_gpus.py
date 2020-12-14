"""Test the collective group APIs."""
import pytest
import ray
from ray.util.collective.types import ReduceOp

import cupy as cp
import torch

from .util import Worker


def get_actors_group(num_workers=2, group_name="default", backend="nccl"):
    actors = [Worker.remote() for _ in range(num_workers)]
    world_size = num_workers
    init_results = ray.get([
        actor.init_group.remote(world_size, i, backend, group_name)
        for i, actor in enumerate(actors)
    ])
    return actors, init_results


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
def test_init_two_actors(ray_start_single_node_2_gpus, group_name):
    world_size = 2
    actors, results = get_actors_group(world_size, group_name)
    for i in range(world_size):
        assert (results[i])


def test_init_multiple_groups(ray_start_single_node_2_gpus):
    world_size = 2
    num_groups = 10
    actors = [Worker.remote() for i in range(world_size)]
    for i in range(num_groups):
        group_name = str(i)
        init_results = ray.get([
            actor.init_group.remote(world_size, i, group_name=group_name)
            for i, actor in enumerate(actors)
        ])
        for j in range(world_size):
            assert init_results[j]


def test_get_rank(ray_start_single_node_2_gpus):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    actor0_rank = ray.get(actors[0].report_rank.remote())
    assert actor0_rank == 0
    actor1_rank = ray.get(actors[1].report_rank.remote())
    assert actor1_rank == 1

    # create a second group with a different name,
    # and different order of ranks.
    new_group_name = "default2"
    _ = ray.get([
        actor.init_group.remote(
            world_size, world_size - 1 - i, group_name=new_group_name)
        for i, actor in enumerate(actors)
    ])
    actor0_rank = ray.get(actors[0].report_rank.remote(new_group_name))
    assert actor0_rank == 1
    actor1_rank = ray.get(actors[1].report_rank.remote(new_group_name))
    assert actor1_rank == 0


def test_get_world_size(ray_start_single_node_2_gpus):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    actor0_world_size = ray.get(actors[0].report_world_size.remote())
    actor1_world_size = ray.get(actors[1].report_world_size.remote())
    assert actor0_world_size == actor1_world_size == world_size


def test_availability(ray_start_single_node_2_gpus):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    actor0_nccl_availability = ray.get(
        actors[0].report_nccl_availability.remote())
    assert actor0_nccl_availability
    actor0_mpi_availability = ray.get(
        actors[0].report_mpi_availability.remote())
    assert not actor0_mpi_availability


def test_is_group_initialized(ray_start_single_node_2_gpus):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    # check group is_init
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor0_is_init
    actor0_is_init = ray.get(
        actors[0].report_is_group_initialized.remote("random"))
    assert not actor0_is_init
    actor0_is_init = ray.get(
        actors[0].report_is_group_initialized.remote("123"))
    assert not actor0_is_init
    actor1_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor1_is_init
    actor1_is_init = ray.get(
        actors[0].report_is_group_initialized.remote("456"))
    assert not actor1_is_init


def test_destroy_group(ray_start_single_node_2_gpus):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    # Now destroy the group at actor0
    ray.wait([actors[0].destroy_group.remote()])
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert not actor0_is_init

    # should go well as the group `random` does not exist at all
    ray.wait([actors[0].destroy_group.remote("random")])

    actor1_is_init = ray.get(actors[1].report_is_group_initialized.remote())
    assert actor1_is_init
    ray.wait([actors[1].destroy_group.remote("random")])
    actor1_is_init = ray.get(actors[1].report_is_group_initialized.remote())
    assert actor1_is_init
    ray.wait([actors[1].destroy_group.remote("default")])
    actor1_is_init = ray.get(actors[1].report_is_group_initialized.remote())
    assert not actor1_is_init

    # Now reconstruct the group using the same name
    init_results = ray.get([
        actor.init_group.remote(world_size, i)
        for i, actor in enumerate(actors)
    ])
    for i in range(world_size):
        assert init_results[i]
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor0_is_init
    actor1_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor1_is_init


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
# @pytest.mark.parametrize("group_name", ['123?34!'])
def test_allreduce_different_name(ray_start_single_node_2_gpus, group_name):
    world_size = 2
    actors, _ = get_actors_group(num_workers=world_size, group_name=group_name)
    results = ray.get([a.do_work.remote(group_name) for a in actors])
    assert (results[0] == cp.ones((10, ), dtype=cp.float32) * world_size).all()
    assert (results[1] == cp.ones((10, ), dtype=cp.float32) * world_size).all()


@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
def test_allreduce_different_array_size(ray_start_single_node_2_gpus,
                                        array_size):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    ray.wait([
        a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32))
        for a in actors
    ])
    results = ray.get([a.do_work.remote() for a in actors])
    assert (results[0] == cp.ones(
        (array_size, ), dtype=cp.float32) * world_size).all()
    assert (results[1] == cp.ones(
        (array_size, ), dtype=cp.float32) * world_size).all()


def test_allreduce_destroy(ray_start_single_node_2_gpus,
                           backend="nccl",
                           group_name="default"):
    world_size = 2
    actors, _ = get_actors_group(world_size)

    results = ray.get([a.do_work.remote() for a in actors])
    assert (results[0] == cp.ones((10, ), dtype=cp.float32) * world_size).all()
    assert (results[1] == cp.ones((10, ), dtype=cp.float32) * world_size).all()

    # destroy the group and try do work, should fail
    ray.wait([a.destroy_group.remote() for a in actors])
    with pytest.raises(RuntimeError):
        results = ray.get([a.do_work.remote() for a in actors])

    # reinit the same group and all reduce
    ray.get([
        actor.init_group.remote(world_size, i, backend, group_name)
        for i, actor in enumerate(actors)
    ])
    results = ray.get([a.do_work.remote() for a in actors])
    assert (results[0] == cp.ones(
        (10, ), dtype=cp.float32) * world_size * 2).all()
    assert (results[1] == cp.ones(
        (10, ), dtype=cp.float32) * world_size * 2).all()


def test_allreduce_multiple_group(ray_start_single_node_2_gpus,
                                  backend="nccl",
                                  num_groups=5):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    for group_name in range(1, num_groups):
        ray.get([
            actor.init_group.remote(world_size, i, backend, str(group_name))
            for i, actor in enumerate(actors)
        ])
    for i in range(num_groups):
        group_name = "default" if i == 0 else str(i)
        results = ray.get([a.do_work.remote(group_name) for a in actors])
        assert (results[0] == cp.ones(
            (10, ), dtype=cp.float32) * (world_size**(i + 1))).all()


def test_allreduce_different_op(ray_start_single_node_2_gpus):
    world_size = 2
    actors, _ = get_actors_group(world_size)

    # check product
    ray.wait([
        a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([a.do_work.remote(op=ReduceOp.PRODUCT) for a in actors])
    assert (results[0] == cp.ones((10, ), dtype=cp.float32) * 6).all()
    assert (results[1] == cp.ones((10, ), dtype=cp.float32) * 6).all()

    # check min
    ray.wait([
        a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([a.do_work.remote(op=ReduceOp.MIN) for a in actors])
    assert (results[0] == cp.ones((10, ), dtype=cp.float32) * 2).all()
    assert (results[1] == cp.ones((10, ), dtype=cp.float32) * 2).all()

    # check max
    ray.wait([
        a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
        for i, a in enumerate(actors)
    ])
    results = ray.get([a.do_work.remote(op=ReduceOp.MAX) for a in actors])
    assert (results[0] == cp.ones((10, ), dtype=cp.float32) * 3).all()
    assert (results[1] == cp.ones((10, ), dtype=cp.float32) * 3).all()


@pytest.mark.parametrize("dtype",
                         [cp.uint8, cp.float16, cp.float32, cp.float64])
def test_allreduce_different_dtype(ray_start_single_node_2_gpus, dtype):
    world_size = 2
    actors, _ = get_actors_group(world_size)
    ray.wait([a.set_buffer.remote(cp.ones(10, dtype=dtype)) for a in actors])
    results = ray.get([a.do_work.remote() for a in actors])
    assert (results[0] == cp.ones((10, ), dtype=dtype) * world_size).all()
    assert (results[1] == cp.ones((10, ), dtype=dtype) * world_size).all()


def test_allreduce_torch_cupy(ray_start_single_node_2_gpus):
    # import torch
    world_size = 2
    actors, _ = get_actors_group(world_size)
    ray.wait([actors[1].set_buffer.remote(torch.ones(10, ).cuda())])
    results = ray.get([a.do_work.remote() for a in actors])
    assert (results[0] == cp.ones((10, )) * world_size).all()

    ray.wait([actors[0].set_buffer.remote(torch.ones(10, ))])
    ray.wait([actors[1].set_buffer.remote(cp.ones(10, ))])
    with pytest.raises(RuntimeError):
        results = ray.get([a.do_work.remote() for a in actors])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-x", __file__]))
