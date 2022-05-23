"""Test the collective group APIs."""
import pytest
import ray
from random import shuffle

from ray.util.collective.tests.util import create_collective_multigpu_workers


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
def test_init_two_actors(ray_start_distributed_multigpu_2_nodes_4_gpus, group_name):
    world_size = 2
    actors, results = create_collective_multigpu_workers(world_size, group_name)
    for i in range(world_size):
        assert results[i]


def test_report_num_gpus(ray_start_distributed_multigpu_2_nodes_4_gpus):
    world_size = 2
    actors, results = create_collective_multigpu_workers(world_size)
    num_gpus = ray.get([actor.report_num_gpus.remote() for actor in actors])
    assert num_gpus == [2, 2]


def test_get_rank(ray_start_distributed_multigpu_2_nodes_4_gpus):
    world_size = 2
    actors, _ = create_collective_multigpu_workers(world_size)
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


def test_is_group_initialized(ray_start_distributed_multigpu_2_nodes_4_gpus):
    world_size = 2
    actors, _ = create_collective_multigpu_workers(world_size)
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


def test_destroy_group(ray_start_distributed_multigpu_2_nodes_4_gpus):
    world_size = 2
    actors, _ = create_collective_multigpu_workers(world_size)
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
