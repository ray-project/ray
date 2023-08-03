"""Test the collective group APIs."""
import pytest
import ray
from random import shuffle

from ray.util.collective.tests.util import Worker, create_collective_workers


@pytest.mark.parametrize("world_size", [2, 3, 4])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
def test_init_two_actors(ray_start_distributed_2_nodes_4_gpus, world_size, group_name):
    actors, results = create_collective_workers(world_size, group_name)
    for i in range(world_size):
        assert results[i]


@pytest.mark.parametrize("world_size", [2, 3, 4])
def test_init_multiple_groups(ray_start_distributed_2_nodes_4_gpus, world_size):
    num_groups = 1
    actors = [Worker.remote() for _ in range(world_size)]
    for i in range(num_groups):
        group_name = str(i)
        init_results = ray.get(
            [
                actor.init_group.remote(world_size, k, group_name=group_name)
                for k, actor in enumerate(actors)
            ]
        )
        for j in range(world_size):
            assert init_results[j]


@pytest.mark.parametrize("world_size", [2, 3, 4])
def test_get_rank(ray_start_distributed_2_nodes_4_gpus, world_size):
    actors, _ = create_collective_workers(world_size)
    actor0_rank = ray.get(actors[0].report_rank.remote())
    assert actor0_rank == 0
    actor1_rank = ray.get(actors[1].report_rank.remote())
    assert actor1_rank == 1

    # create a second group with a different name, and different
    # orders of ranks.
    new_group_name = "default2"
    ranks = list(range(world_size))
    shuffle(ranks)
    ray.get(
        [
            actor.init_group.remote(world_size, ranks[i], group_name=new_group_name)
            for i, actor in enumerate(actors)
        ]
    )
    actor0_rank = ray.get(actors[0].report_rank.remote(new_group_name))
    assert actor0_rank == ranks[0]
    actor1_rank = ray.get(actors[1].report_rank.remote(new_group_name))
    assert actor1_rank == ranks[1]


@pytest.mark.parametrize("world_size", [2, 3, 4])
def test_get_collective_group_size(ray_start_distributed_2_nodes_4_gpus, world_size):
    actors, _ = create_collective_workers(world_size)
    actor0_world_size = ray.get(actors[0].report_world_size.remote())
    actor1_world_size = ray.get(actors[1].report_world_size.remote())
    assert actor0_world_size == actor1_world_size == world_size


def test_is_group_initialized(ray_start_distributed_2_nodes_4_gpus):
    world_size = 4
    actors, _ = create_collective_workers(world_size)
    # check group is_init
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


def test_destroy_group(ray_start_distributed_2_nodes_4_gpus):
    world_size = 4
    actors, _ = create_collective_workers(world_size)
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
    for i in [2, 3]:
        ray.wait([actors[i].destroy_group.remote("default")])

    # Now reconstruct the group using the same name
    init_results = ray.get(
        [actor.init_group.remote(world_size, i) for i, actor in enumerate(actors)]
    )
    for i in range(world_size):
        assert init_results[i]
    actor0_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor0_is_init
    actor1_is_init = ray.get(actors[0].report_is_group_initialized.remote())
    assert actor1_is_init


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
