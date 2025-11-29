"""Test the collective group APIs."""
from random import shuffle

import pytest

import ray
from ray.util.collective.tests.npu_util import create_collective_multinpu_workers
from ray.util.collective.types import Backend


@pytest.mark.parametrize("backend", [Backend.HCCL])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
def test_init_two_actors(
    ray_start_distributed_multinpu_2_nodes_4_npus, backend, group_name
):
    world_size = 2
    actors, results = create_collective_multinpu_workers(
        world_size, group_name, backend=backend
    )
    for i in range(world_size):
        assert results[i]


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_report_num_npus(ray_start_distributed_multinpu_2_nodes_4_npus, backend):
    world_size = 2
    actors, results = create_collective_multinpu_workers(
        world_size, backend=backend
    )
    num_npus = ray.get([actor.report_num_npus.remote() for actor in actors])
    assert num_npus == [2, 2]


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_get_rank(ray_start_distributed_multinpu_2_nodes_4_npus, backend):
    world_size = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)
    actor0_rank = ray.get(actors[0].report_rank.remote())
    assert actor0_rank == 0
    actor1_rank = ray.get(actors[1].report_rank.remote())
    assert actor1_rank == 1

    new_group_name = "default2"
    ranks = list(range(world_size))
    shuffle(ranks)
    ray.get(
        [
            actor.init_group.remote(
                world_size, ranks[i], group_name=new_group_name, backend=backend
            )
            for i, actor in enumerate(actors)
        ]
    )
    actor0_rank = ray.get(actors[0].report_rank.remote(new_group_name))
    assert actor0_rank == ranks[0]
    actor1_rank = ray.get(actors[1].report_rank.remote(new_group_name))
    assert actor1_rank == ranks[1]


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_is_group_initialized(ray_start_distributed_multinpu_2_nodes_4_npus, backend):
    world_size = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)

    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor0_is_init
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote("random"))
    assert not actor0_is_init
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote("123"))
    assert not actor0_is_init
    actor1_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor1_is_init
    actor1_is_init = ray.get(actors[0].report_is_group_initialized.remote("456"))
    assert not actor1_is_init


@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_destroy_group(ray_start_distributed_multinpu_2_nodes_4_npus, backend):
    world_size = 2
    actors, _ = create_collective_multinpu_workers(world_size, backend=backend)

    ray.wait([actors[0].destroy_group.remote()])
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert not actor0_is_init

    ray.wait([actors[0].destroy_group.remote("random")])

    actor1_is_init = ray.get(actors[1].report_is_group_initialized.remote())
    assert actor1_is_init
    ray.wait([actors[1].destroy_group.remote("random")])
    actor1_is_init = ray.get(actors[1].report_is_group_initialized.remote())
    assert actor1_is_init
    ray.wait([actors[1].destroy_group.remote("default")])
    actor1_is_init = ray.get(actors[1].report_is_group_initialized.remote())
    assert not actor1_is_init

    init_results = ray.get(
        [
            actor.init_group.remote(world_size, i, backend=backend)
            for i, actor in enumerate(actors)
        ]
    )
    for i in range(world_size):
        assert init_results[i]
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor0_is_init
    actor1_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor1_is_init


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
