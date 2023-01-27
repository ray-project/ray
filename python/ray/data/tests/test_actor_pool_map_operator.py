import time

import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool


@ray.remote
class PoolWorker:
    def ready(self) -> str:
        return "ok"


class TestActorPool:
    def _add_ready_worker(self, pool: _ActorPool) -> ray.actor.ActorHandle:
        actor = PoolWorker.remote()
        ready_ref = actor.ready.remote()
        pool.add_pending_actor(actor, ready_ref)
        # Wait until actor has started.
        ray.get(ready_ref)
        # Mark actor as running.
        has_actor = pool.pending_to_running(ready_ref)
        assert has_actor
        return actor

    def test_add_pending(self, ray_start_regular_shared):
        # Test that pending actor is added in the correct state.
        pool = _ActorPool()
        actor = PoolWorker.remote()
        ready_ref = actor.ready.remote()
        pool.add_pending_actor(actor, ready_ref)
        # Check that the pending actor is not pickable.
        assert pool.pick_actor() is None
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 1
        assert pool.num_pending_actors() == 1
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        # Check that ready future is returned.
        assert pool.get_pending_actor_refs() == [ready_ref]

    def test_pending_to_running(self, ray_start_regular_shared):
        # Test that pending actor is correctly transitioned to running.
        pool = _ActorPool()
        actor = self._add_ready_worker(pool)
        # Check that the actor is pickable.
        picked_actor = pool.pick_actor()
        assert picked_actor == actor
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 1
        assert pool.num_idle_actors() == 0

    def test_repeated_picking(self, ray_start_regular_shared):
        # Test that we can repeatedly pick the same actor.
        pool = _ActorPool()
        actor = self._add_ready_worker(pool)
        for _ in range(10):
            picked_actor = pool.pick_actor()
            assert picked_actor == actor

    def test_return_actor(self, ray_start_regular_shared):
        # Test that we can return an actor as many times as we've picked it.
        pool = _ActorPool()
        self._add_ready_worker(pool)
        for _ in range(10):
            picked_actor = pool.pick_actor()
        # Return the actor as many times as it was picked.
        for _ in range(10):
            pool.return_actor(picked_actor)
        # Returning the actor more times than it has been picked should raise an
        # AssertionError.
        with pytest.raises(AssertionError):
            pool.return_actor(picked_actor)
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 1  # Actor should now be idle.

    def test_pick_ordering_lone_idle(self, ray_start_regular_shared):
        # Test that a lone idle actor is the one that's picked.
        pool = _ActorPool()
        self._add_ready_worker(pool)
        # Ensure that actor has been picked once.
        pool.pick_actor()
        # Add a new, idle actor.
        actor2 = self._add_ready_worker(pool)
        # Check that picked actor is the idle newly added actor.
        picked_actor = pool.pick_actor()
        assert picked_actor == actor2

    def test_pick_ordering_full_order(self, ray_start_regular_shared):
        # Test that the least loaded actor is always picked.
        pool = _ActorPool()
        # Add 4 actors to the pool.
        actors = [self._add_ready_worker(pool) for _ in range(4)]
        # Pick 4 actors.
        picked_actors = [pool.pick_actor() for _ in range(4)]
        # Check that the 4 distinct actors that were added to the pool were all
        # returned.
        assert set(picked_actors) == set(actors)
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 4
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 4
        assert pool.num_active_actors() == 4
        assert pool.num_idle_actors() == 0

    def test_pick_ordering_with_returns(self, ray_start_regular_shared):
        # Test that pick ordering works with returns.
        pool = _ActorPool()
        actor1 = self._add_ready_worker(pool)
        actor2 = self._add_ready_worker(pool)
        picked_actors = [pool.pick_actor() for _ in range(2)]
        # Double-check that both actors were picked.
        assert set(picked_actors) == {actor1, actor2}
        # Return actor 2, implying that it's now idle.
        pool.return_actor(actor2)
        # Check that actor 2 is the next actor that's picked.
        assert pool.pick_actor() == actor2

    def test_kill_all_inactive_pending_actor_killed(self, ray_start_regular_shared):
        # Test that pending actors are killed on the kill_all_inactive_actors() call.
        pool = _ActorPool()
        actor = PoolWorker.remote()
        ready_ref = actor.ready.remote()
        pool.add_pending_actor(actor, ready_ref)
        # Kill inactive actors.
        pool.kill_all_inactive_actors()
        # Check that actor is not in pool.
        assert pool.get_pending_actor_refs() == []
        # Check that actor is no longer in the pool as pending, to protect against
        # ready/killed races.
        assert not pool.pending_to_running(ready_ref)
        # Check that actor was killed.
        # Wait a few seconds to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0

    def test_kill_all_inactive_idle_actor_killed(self, ray_start_regular_shared):
        # Test that idle actors are killed on the kill_all_inactive_actors() call.
        pool = _ActorPool()
        actor = self._add_ready_worker(pool)
        # Kill inactive actors.
        pool.kill_all_inactive_actors()
        # Check that actor is not in pool.
        assert pool.pick_actor() is None
        # Check that actor was killed.
        # Wait a few seconds to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0

    def test_kill_all_inactive_active_actor_not_killed(self, ray_start_regular_shared):
        # Test that active actors are NOT killed on the kill_all_inactive_actors() call.
        pool = _ActorPool()
        actor = self._add_ready_worker(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == actor
        # Kill inactive actors.
        pool.kill_all_inactive_actors()
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor

    def test_kill_all_inactive_future_idle_actors_killed(
        self, ray_start_regular_shared
    ):
        # Test that future idle actors are killed after the kill_all_inactive_actors()
        # call.
        pool = _ActorPool()
        actor = self._add_ready_worker(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == actor
        # Kill inactive actors, of which there are currently none.
        pool.kill_all_inactive_actors()
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor
        # Return the actor to the pool twice, which should set it as idle and cause it
        # to be killed.
        for _ in range(2):
            pool.return_actor(actor)
        # Check that actor is not in pool.
        assert pool.pick_actor() is None
        # Check that actor was killed.
        # Wait a few seconds to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0

    def test_kill_all_inactive_mixture(self, ray_start_regular_shared):
        # Test that in a mixture of pending, idle, and active actors, only the pending
        # and idle actors are killed on the kill_all_inactive_actors() call.
        pool = _ActorPool()
        # Add active actor.
        actor1 = self._add_ready_worker(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == actor1
        # Add idle actor.
        self._add_ready_worker(pool)
        # Add pending actor.
        actor3 = PoolWorker.remote()
        ready_ref = actor3.ready.remote()
        pool.add_pending_actor(actor3, ready_ref)
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 3
        assert pool.num_pending_actors() == 1
        assert pool.num_running_actors() == 2
        assert pool.num_active_actors() == 1
        assert pool.num_idle_actors() == 1
        # Kill inactive actors.
        pool.kill_all_inactive_actors()
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor1
        # Check that adding a pending actor raises an error.
        with pytest.raises(AssertionError):
            pool.add_pending_actor(actor3, ready_ref)
        # Check that kill_all_inactive_actors() is idempotent.
        pool.kill_all_inactive_actors()
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor1
        # Return the actor to the pool thrice, which should set it as idle and cause it
        # to be killed.
        for _ in range(3):
            pool.return_actor(actor1)
        # Check that actor is not in pool.
        assert pool.pick_actor() is None
        # Check that actor was killed.
        # Wait a few seconds to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor1.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0

    def test_all_actors_killed(self, ray_start_regular_shared):
        # Test that all actors are killed after the kill_all_actors() call.
        pool = _ActorPool()
        active_actor = self._add_ready_worker(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == active_actor
        idle_actor = self._add_ready_worker(pool)
        # Kill all actors, including active actors.
        pool.kill_all_actors()
        # Check that the pool is empty.
        assert pool.pick_actor() is None
        # Check that both actors were killed.
        # Wait a few seconds to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(idle_actor.ready.remote())
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(active_actor.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
