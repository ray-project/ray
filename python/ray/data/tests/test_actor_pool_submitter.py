import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.data._internal.execution.operators.actor_pool_submitter import ActorPool


# TODO(Clark): Split into separate tests.
def test_actor_pool(ray_start_regular_shared):
    # Create actor pool.
    pool = ActorPool()
    assert pool.num_actors == 0

    @ray.remote
    class PoolWorker:
        def inc(self, x):
            return x + 1

    actors = [PoolWorker.remote() for _ in range(4)]
    for actor in actors:
        pool.add_actor(actor)
    # Check that pool count is as expected.
    assert pool.num_actors == 4
    # Check that picked actors are the same actors we gave it.
    picked_actors = []
    for _ in range(4):
        picked_actors.append(pool.pick_actor())
    assert set(picked_actors) == set(actors)
    # Check that picked actors each have 1 task in flight.
    for actor in picked_actors:
        assert pool.get_tasks_in_flight(actor) == 1
    # Check that no actors are killed since none are idle (all are currently picked
    # once).
    pool.kill_idle_actors()
    assert pool.num_actors == 4
    # Check that lone idle actor is the one picked.
    returned_actor = picked_actors.pop()
    pool.return_actor(returned_actor)
    assert pool.get_tasks_in_flight(returned_actor) == 0
    picked_actor = pool.pick_actor()
    assert picked_actor == returned_actor
    # Check that lone idle actor is killed.
    pool.return_actor(returned_actor)
    pool.kill_idle_actors()
    assert not pool.has_actor(returned_actor)
    assert pool.num_actors == 3
    # Check that repeated picking works, and that we always pick the least loaded actor.
    for _ in range(3):
        pool.pick_actor()
    for actor in picked_actors:
        # Each actor should have been picked one more time.
        assert pool.get_tasks_in_flight(actor) == 2
    # Check that asking the pool to kill future actors as they become idle works.
    pool.kill_future_idle_actors()
    for actor in picked_actors:
        # Actor was picked twice, so return it twice.
        for _ in range(2):
            pool.return_actor(actor)
        # Check that actor is no longer in pool.
        assert not pool.has_actor(actor)
        # Check that actor has been killed.
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.inc.remote(1))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
