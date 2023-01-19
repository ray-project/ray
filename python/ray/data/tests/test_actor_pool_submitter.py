import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.data._internal.execution.operators.actor_pool_submitter import ActorPool


@ray.remote
class PoolWorker:
    def inc(self, x):
        return x + 1


class TestActorPool:
    def test_pick_the_adds(self, ray_start_regular_shared):
        # Test that we can pick an actor that we add to the pool.
        pool = ActorPool()
        actor = PoolWorker.remote()
        pool.add_actor(actor)
        picked_actor = pool.pick_actor()
        assert picked_actor == actor

    def test_repeated_picking(self, ray_start_regular_shared):
        # Test that we can repeatedly pick the same actor.
        pool = ActorPool()
        actor = PoolWorker.remote()
        pool.add_actor(actor)
        for _ in range(10):
            picked_actor = pool.pick_actor()
            assert picked_actor == actor

    def test_return_actor(self, ray_start_regular_shared):
        # Test that we can return an actor as many times as we've picked it.
        pool = ActorPool()
        actor = PoolWorker.remote()
        pool.add_actor(actor)
        for _ in range(10):
            picked_actor = pool.pick_actor()
        # Return the actor as many times as it was picked.
        for _ in range(10):
            pool.return_actor(picked_actor)
        # Returning the actor more times than it has been picked should raise a
        # ValueError.
        with pytest.raises(ValueError):
            pool.return_actor(picked_actor)

    def test_pick_ordering_lone_idle(ray_start_regular_shared):
        # Test that a lone idle actor is the one that's picked.
        pool = ActorPool()
        actor1 = PoolWorker.remote()
        pool.add_actor(actor1)
        # Ensure that actor has been picked once.
        pool.pick_actor()
        # Add a new, idle actor.
        actor2 = PoolWorker.remote()
        pool.add_actor(actor2)
        # Check that picked actor is the idle newly added actor.
        picked_actor = pool.pick_actor()
        assert picked_actor == actor2

    def test_pick_ordering_full_order(ray_start_regular_shared):
        # Test that the least loaded actor is always picked.
        pool = ActorPool()
        # Add 4 actors to the pool.
        actors = [PoolWorker.remote() for _ in range(4)]
        for actor in actors:
            pool.add_actor(actor)
        # Pick 4 actors.
        picked_actors = [pool.pick_actor() for _ in range(4)]
        # Check that the 4 distinct actors that were added to the pool were all
        # returned.
        assert set(picked_actors) == set(actors)

    def test_pick_ordering_with_returns(ray_start_regular_shared):
        # Test that pick ordering works with returns.
        pool = ActorPool()
        actor1 = PoolWorker.remote()
        pool.add_actor(actor1)
        actor2 = PoolWorker.remote()
        pool.add_actor(actor2)
        picked_actors = [pool.pick_actor() for _ in range(2)]
        # Double-check that both actors were picked.
        assert set(picked_actors) == {actor1, actor2}
        # Return actor 2, implying that it's now idle.
        pool.return_actor(actor2)
        # Check that actor 2 is the next actor that's picked.
        assert pool.pick_actor() == actor2

    def test_idle_actor_killed(ray_start_regular_shared):
        # Test that idle actors are killed on the kill_idle_actors() call.
        pool = ActorPool()
        actor = PoolWorker.remote()
        pool.add_actor(actor)
        # Kill idle actors.
        pool.kill_idle_actors()
        # Check that actor is not in pool.
        with pytest.raises(ValueError):
            pool.pick_actor()
        # Check that actor was killed.
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.inc.remote(1))

    def test_active_actor_not_killed(ray_start_regular_shared):
        # Test that active actors are NOT killed on the kill_idle_actors() call.
        pool = ActorPool()
        actor = PoolWorker.remote()
        pool.add_actor(actor)
        picked_actor = pool.pick_actor()
        # Double-check that actor was picked.
        assert picked_actor == actor
        # Kill idle actors.
        pool.kill_idle_actors()
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor

    def test_future_idle_actors_killed(ray_start_regular_shared):
        # Test that future idle actors are killed after the kill_idle_actors() call.
        pool = ActorPool()
        actor = PoolWorker.remote()
        pool.add_actor(actor)
        picked_actor = pool.pick_actor()
        # Double-check that actor was picked.
        assert picked_actor == actor
        # Kill idle actors, of which there are currently none.
        pool.kill_idle_actors()
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor
        # Return the actor to the pool twice, which should set it as idle and cause it
        # to be killed.
        for _ in range(2):
            pool.return_actor(actor)
        # Check that actor is not in pool.
        with pytest.raises(ValueError):
            pool.pick_actor()
        # Check that actor was killed.
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.inc.remote(1))

    def test_all_actors_killed(ray_start_regular_shared):
        # Test that all actors are killed after the kill_all_actors() call.
        pool = ActorPool()
        active_actor = PoolWorker.remote()
        pool.add_actor(active_actor)
        picked_actor = pool.pick_actor()
        # Double-check that actor was picked.
        assert picked_actor == active_actor
        idle_actor = PoolWorker.remote()
        pool.add_actor(idle_actor)
        # Kill all actors, including idle actors.
        pool.kill_all_actors()
        # Check that the pool is empty.
        with pytest.raises(ValueError):
            pool.pick_actor()
        # Check that both actors were killed.
        for actor in [idle_actor, active_actor]:
            with pytest.raises(ray.exceptions.RayActorError):
                ray.get(actor.inc.remote(1))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
