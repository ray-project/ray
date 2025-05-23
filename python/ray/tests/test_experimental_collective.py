import pytest

import ray


@ray.remote
class Actor:
    def __init__(self):
        pass

def test_basic(ray_start_regular):
    world_size = 3
    actors = [Actor.remote() for _ in range(world_size)]

    # Check no groups on start up.
    for actor in actors:
        gps = ray.experimental.collective.get_collective_groups([actor])
        assert gps is []

    gps = ray.experimental.collective.get_collective_groups(actors)
    assert gps is []

    # Check that the collective group is created with the correct actors and
    # ranks.
    gp = ray.experimental.collective.create_collective_group(actors, "nccl", name="test")
    assert gp.name == "test"
    for i, actor in enumerate(actors):
        assert gp.get_rank(actor) == i

    for actor in actors:
        gps = ray.experimental.collective.get_collective_groups([actor])
        assert gps is [gp]

    gps = ray.experimental.collective.get_collective_groups(actors)
    assert gps is [gp]

    # Check that the group is destroyed.
    ray.experimental.collective.destroy_collective_group(gp)

    for actor in actors:
        gps = ray.experimental.collective.get_collective_groups([actor])
        assert gps is []

    gps = ray.experimental.collective.get_collective_groups(actors)
    assert gps is []


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
