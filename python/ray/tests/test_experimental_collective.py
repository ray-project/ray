import pytest

import ray
import ray.experimental.collective


@ray.remote
class Actor:
    def __init__(self):
        pass


def test_basic(ray_start_regular):
    world_size = 3
    actors = [Actor.remote() for _ in range(world_size)]

    # Check no groups on start up.
    for actor in actors:
        groups = ray.experimental.collective.get_collective_groups([actor])
        assert groups == []

    groups = ray.experimental.collective.get_collective_groups(actors)
    assert groups == []

    # Check that the collective group is created with the correct actors and
    # ranks.
    group = ray.experimental.collective.create_collective_group(actors, name="test", backend="torch_gloo")
    assert group.name == "test"
    for i, actor in enumerate(actors):
        assert group.get_rank(actor) == i

    for actor in actors:
        groups = ray.experimental.collective.get_collective_groups([actor])
        assert groups == [group]

    groups = ray.experimental.collective.get_collective_groups(actors)
    assert groups == [group]

    # Check that the group is destroyed.
    ray.experimental.collective.destroy_collective_group(group)

    for actor in actors:
        groups = ray.experimental.collective.get_collective_groups([actor])
        assert groups == []

    groups = ray.experimental.collective.get_collective_groups(actors)
    assert groups == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
