import collections
import time

import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    _ActorPool,
    AutoscalingConfig,
    AutoscalingPolicy,
)


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

    def test_pick_max_tasks_in_flight(self, ray_start_regular_shared):
        # Test that we can't pick an actor beyond the max_tasks_in_flight cap.
        pool = _ActorPool(max_tasks_in_flight=2)
        actor = self._add_ready_worker(pool)
        assert pool.pick_actor() == actor
        assert pool.pick_actor() == actor
        # Check that the 3rd pick doesn't return the actor.
        assert pool.pick_actor() is None

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

    def test_pick_all_max_tasks_in_flight(self, ray_start_regular_shared):
        # Test that max_tasks_in_flight cap applies to all actors in pool.
        pool = _ActorPool(max_tasks_in_flight=2)
        # Add 4 actors to the pool.
        actors = [self._add_ready_worker(pool) for _ in range(4)]
        picked_actors = [pool.pick_actor() for _ in range(8)]
        pick_counts = collections.Counter(picked_actors)
        # Check that picks were evenly distributed over the pool.
        assert len(pick_counts) == 4
        for actor, count in pick_counts.items():
            assert actor in actors
            assert count == 2
        # Check that the next pick doesn't return an ctor.
        assert pool.pick_actor() is None

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

    def test_kill_inactive_pending_actor(self, ray_start_regular_shared):
        # Test that a pending actor is killed on the kill_inactive_actor() call.
        pool = _ActorPool()
        actor = PoolWorker.remote()
        ready_ref = actor.ready.remote()
        pool.add_pending_actor(actor, ready_ref)
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that actor is not in pool.
        assert pool.get_pending_actor_refs() == []
        # Check that actor was killed.
        # Wait a second to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0

    def test_kill_inactive_idle_actor(self, ray_start_regular_shared):
        # Test that a idle actor is killed on the kill_inactive_actor() call.
        pool = _ActorPool()
        actor = self._add_ready_worker(pool)
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that actor is not in pool.
        assert pool.pick_actor() is None
        # Check that actor was killed.
        # Wait a second to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0

    def test_kill_inactive_active_actor_not_killed(self, ray_start_regular_shared):
        # Test that active actors are NOT killed on the kill_inactive_actor() call.
        pool = _ActorPool()
        actor = self._add_ready_worker(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == actor
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was NOT killed.
        assert not killed
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor

    def test_kill_inactive_pending_over_idle(self, ray_start_regular_shared):
        # Test that a killing pending actors is prioritized over killing idle actors on
        # the kill_inactive_actor() call.
        pool = _ActorPool()
        # Add pending worker.
        pending_actor = PoolWorker.remote()
        ready_ref = pending_actor.ready.remote()
        pool.add_pending_actor(pending_actor, ready_ref)
        # Add idle worker.
        idle_actor = self._add_ready_worker(pool)
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that the idle actor is still in the pool.
        assert pool.pick_actor() == idle_actor
        pool.return_actor(idle_actor)
        # Check that the pending actor is not in pool.
        assert pool.get_pending_actor_refs() == []
        # Check that actor was killed.
        # Wait a second to let actor killing happen.
        time.sleep(1)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(pending_actor.ready.remote())
        # Check that the per-state pool sizes are as expected.
        assert pool.num_total_actors() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 1

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


class TestAutoscalingConfig:
    def test_min_workers_validation(self):
        # Test min_workers positivity validation.
        with pytest.raises(ValueError):
            AutoscalingConfig(min_workers=0, max_workers=2)

    def test_max_workers_validation(self):
        # Test max_workers not being less than min_workers validation.
        with pytest.raises(ValueError):
            AutoscalingConfig(min_workers=3, max_workers=2)

    def test_max_tasks_in_flight_validation(self):
        # Test max_tasks_in_flight positivity validation.
        with pytest.raises(ValueError):
            AutoscalingConfig(min_workers=1, max_workers=2, max_tasks_in_flight=0)

    def test_full_specification(self):
        # Basic regression test for full specification.
        config = AutoscalingConfig(
            min_workers=2,
            max_workers=100,
            max_tasks_in_flight=3,
            ready_to_total_workers_ratio=0.8,
            idle_to_total_workers_ratio=0.25,
        )
        assert config.min_workers == 2
        assert config.max_workers == 100
        assert config.max_tasks_in_flight == 3
        assert config.ready_to_total_workers_ratio == 0.8
        assert config.idle_to_total_workers_ratio == 0.25

    def test_from_compute(self):
        # Test that construction from ActorPoolStrategy works as expected.
        compute = ActorPoolStrategy(
            min_size=2, max_size=5, max_tasks_in_flight_per_actor=3
        )
        config = AutoscalingConfig.from_compute_strategy(compute)
        assert config.min_workers == 2
        assert config.max_workers == 5
        assert config.max_tasks_in_flight == 3
        assert config.ready_to_total_workers_ratio == 0.8
        assert config.idle_to_total_workers_ratio == 0.5


class TestAutoscalingPolicy:
    def test_min_workers(self):
        # Test that the autoscaling policy forwards the config's min_workers.
        config = AutoscalingConfig(min_workers=1, max_workers=4)
        policy = AutoscalingPolicy(config)
        assert policy.min_workers == 1

    def test_max_workers(self):
        # Test that the autoscaling policy forwards the config's max_workers.
        config = AutoscalingConfig(min_workers=1, max_workers=4)
        policy = AutoscalingPolicy(config)
        assert policy.max_workers == 4

    def test_should_scale_up_over_max_workers(self):
        # Test that scale-up is blocked if the pool would go over the configured max
        # workers.
        config = AutoscalingConfig(min_workers=1, max_workers=4)
        policy = AutoscalingPolicy(config)
        num_total_workers = 4
        num_running_workers = 4
        # Shouldn't scale up due to pool max workers.
        assert not policy.should_scale_up(num_total_workers, num_running_workers)

        num_total_workers = 3
        num_running_workers = 3
        # Should scale up since under pool max workers.
        assert policy.should_scale_up(num_total_workers, num_running_workers)

    def test_should_scale_up_ready_to_total_ratio(self):
        # Test that scale-up is blocked if under the ready workers to total workers
        # ratio.
        config = AutoscalingConfig(
            min_workers=1, max_workers=4, ready_to_total_workers_ratio=0.5
        )
        policy = AutoscalingPolicy(config)

        num_total_workers = 2
        num_running_workers = 1
        # Shouldn't scale up due to being under ready workers to total workers ratio.
        assert not policy.should_scale_up(num_total_workers, num_running_workers)

        num_total_workers = 3
        num_running_workers = 2
        # Shouldn scale up due to being over ready workers to total workers ratio.
        assert policy.should_scale_up(num_total_workers, num_running_workers)

    def test_should_scale_down_min_workers(self):
        # Test that scale-down is blocked if the pool would go under the configured min
        # workers.
        config = AutoscalingConfig(min_workers=2, max_workers=4)
        policy = AutoscalingPolicy(config)
        num_total_workers = 2
        num_idle_workers = 2
        # Shouldn't scale down due to pool min workers.
        assert not policy.should_scale_down(num_total_workers, num_idle_workers)

        num_total_workers = 3
        num_idle_workers = 3
        # Should scale down since over pool min workers.
        assert policy.should_scale_down(num_total_workers, num_idle_workers)

    def test_should_scale_down_idle_to_total_ratio(self):
        # Test that scale-down is blocked if under the idle workers to total workers
        # ratio.
        config = AutoscalingConfig(
            min_workers=1, max_workers=4, idle_to_total_workers_ratio=0.5
        )
        policy = AutoscalingPolicy(config)
        num_total_workers = 4
        num_idle_workers = 1
        # Shouldn't scale down due to being under idle workers to total workers ratio.
        assert not policy.should_scale_down(num_total_workers, num_idle_workers)

        num_total_workers = 4
        num_idle_workers = 3
        # Should scale down due to being over idle workers to total workers ratio.
        assert policy.should_scale_down(num_total_workers, num_idle_workers)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
