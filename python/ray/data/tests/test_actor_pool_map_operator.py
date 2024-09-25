import collections
import unittest
from typing import Any, Optional, Tuple

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.actor import ActorHandle
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool
from ray.data._internal.execution.util import make_ref_bundles
from ray.tests.conftest import *  # noqa
from ray.types import ObjectRef


@ray.remote
class PoolWorker:
    def __init__(self, node_id: str = "node1"):
        self.node_id = node_id

    def get_location(self) -> str:
        return self.node_id


class TestActorPool(unittest.TestCase):
    def setup_class(self):
        self._last_created_actor_and_ready_ref: Optional[
            Tuple[ActorHandle, ObjectRef[Any]]
        ] = None
        self._actor_node_id = "node1"
        ray.init(num_cpus=4)

    def teardown_class(self):
        ray.shutdown()

    def _create_actor_fn(self) -> Tuple[ActorHandle, ObjectRef[Any]]:
        actor = PoolWorker.remote(self._actor_node_id)
        ready_ref = actor.get_location.remote()
        self._last_created_actor_and_ready_ref = actor, ready_ref
        return actor, ready_ref

    def _create_actor_pool(
        self,
        min_size=1,
        max_size=4,
        max_tasks_in_flight=4,
    ):
        pool = _ActorPool(
            compute_strategy=ActorPoolStrategy(
                min_size=min_size,
                max_size=max_size,
                max_tasks_in_flight_per_actor=max_tasks_in_flight,
            ),
            create_actor_fn=self._create_actor_fn,
        )
        return pool

    def _add_pending_actor(
        self, pool: _ActorPool, node_id="node1"
    ) -> Tuple[ActorHandle, ObjectRef[Any]]:
        self._actor_node_id = node_id
        assert pool.scale_up(1) == 1
        assert self._last_created_actor_and_ready_ref is not None
        actor, ready_ref = self._last_created_actor_and_ready_ref
        self._last_created_actor_and_ready_ref = None
        return actor, ready_ref

    def _wait_for_actor_ready(self, pool: _ActorPool, ready_ref):
        ray.get(ready_ref)
        pool.pending_to_running(ready_ref)

    def _add_ready_actor(self, pool: _ActorPool, node_id="node1") -> ActorHandle:
        actor, ready_ref = self._add_pending_actor(pool, node_id)
        self._wait_for_actor_ready(pool, ready_ref)
        return actor

    def _wait_for_actor_dead(self, actor_id: str):
        def _check_actor_dead():
            nonlocal actor_id
            actor_info = ray.state.actors(actor_id)
            return actor_info["State"] == "DEAD"

        wait_for_condition(_check_actor_dead)

    def test_basic_config(self):
        pool = self._create_actor_pool(
            min_size=1,
            max_size=4,
            max_tasks_in_flight=4,
        )
        assert pool.min_size() == 1
        assert pool.max_size() == 4
        assert pool.current_size() == 0
        assert pool.max_tasks_in_flight_per_actor() == 4

    def test_add_pending(self):
        # Test that pending actor is added in the correct state.
        pool = self._create_actor_pool()
        _, ready_ref = self._add_pending_actor(pool)
        # Check that the pending actor is not pickable.

        assert pool.pick_actor() is None
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 1
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0
        # Check that ready future is returned.
        assert pool.get_pending_actor_refs() == [ready_ref]

    def test_pending_to_running(self):
        # Test that pending actor is correctly transitioned to running.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Check that the actor is pickable.
        picked_actor = pool.pick_actor()
        assert picked_actor == actor
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 1
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 3

    def test_repeated_picking(self):
        # Test that we can repeatedly pick the same actor.
        pool = self._create_actor_pool(max_tasks_in_flight=999)
        actor = self._add_ready_actor(pool)
        for _ in range(10):
            picked_actor = pool.pick_actor()
            assert picked_actor == actor

    def test_return_actor(self):
        # Test that we can return an actor as many times as we've picked it.
        pool = self._create_actor_pool(max_tasks_in_flight=999)
        self._add_ready_actor(pool)
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
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 1  # Actor should now be idle.
        assert pool.num_free_slots() == 999

    def test_pick_max_tasks_in_flight(self):
        # Test that we can't pick an actor beyond the max_tasks_in_flight cap.
        pool = self._create_actor_pool(max_tasks_in_flight=2)
        actor = self._add_ready_actor(pool)
        assert pool.num_free_slots() == 2
        assert pool.pick_actor() == actor
        assert pool.num_free_slots() == 1
        assert pool.pick_actor() == actor
        assert pool.num_free_slots() == 0
        # Check that the 3rd pick doesn't return the actor.
        assert pool.pick_actor() is None

    def test_pick_ordering_lone_idle(self):
        # Test that a lone idle actor is the one that's picked.
        pool = self._create_actor_pool()
        self._add_ready_actor(pool)
        # Ensure that actor has been picked once.
        pool.pick_actor()
        # Add a new, idle actor.
        actor2 = self._add_ready_actor(pool)
        # Check that picked actor is the idle newly added actor.
        picked_actor = pool.pick_actor()
        assert picked_actor == actor2

    def test_pick_ordering_full_order(self):
        # Test that the least loaded actor is always picked.
        pool = self._create_actor_pool()
        # Add 4 actors to the pool.
        actors = [self._add_ready_actor(pool) for _ in range(4)]
        # Pick 4 actors.
        picked_actors = [pool.pick_actor() for _ in range(4)]
        # Check that the 4 distinct actors that were added to the pool were all
        # returned.
        assert set(picked_actors) == set(actors)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 4
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 4
        assert pool.num_active_actors() == 4
        assert pool.num_idle_actors() == 0

    def test_pick_all_max_tasks_in_flight(self):
        # Test that max_tasks_in_flight cap applies to all actors in pool.
        pool = self._create_actor_pool(max_tasks_in_flight=2)
        # Add 4 actors to the pool.
        actors = [self._add_ready_actor(pool) for _ in range(4)]
        picked_actors = [pool.pick_actor() for _ in range(8)]
        pick_counts = collections.Counter(picked_actors)
        # Check that picks were evenly distributed over the pool.
        assert len(pick_counts) == 4
        for actor, count in pick_counts.items():
            assert actor in actors
            assert count == 2
        # Check that the next pick doesn't return an actor.
        assert pool.pick_actor() is None

    def test_pick_ordering_with_returns(self):
        # Test that pick ordering works with returns.
        pool = self._create_actor_pool()
        actor1 = self._add_ready_actor(pool)
        actor2 = self._add_ready_actor(pool)
        picked_actors = [pool.pick_actor() for _ in range(2)]
        # Double-check that both actors were picked.
        assert set(picked_actors) == {actor1, actor2}
        # Return actor 2, implying that it's now idle.
        pool.return_actor(actor2)
        # Check that actor 2 is the next actor that's picked.
        assert pool.pick_actor() == actor2

    def test_kill_inactive_pending_actor(self):
        # Test that a pending actor is killed on the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        actor, _ = self._add_pending_actor(pool)
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that actor is not in pool.
        assert pool.get_pending_actor_refs() == []
        # Check that actor is dead.
        actor_id = actor._actor_id.hex()
        del actor
        self._wait_for_actor_dead(actor_id)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0

    def test_kill_inactive_idle_actor(self):
        # Test that a idle actor is killed on the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that actor is not in pool.
        assert pool.pick_actor() is None
        # Check that actor is dead.
        actor_id = actor._actor_id.hex()
        del actor
        self._wait_for_actor_dead(actor_id)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0

    def test_kill_inactive_active_actor_not_killed(self):
        # Test that active actors are NOT killed on the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == actor
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was NOT killed.
        assert not killed
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor

    def test_kill_inactive_pending_over_idle(self):
        # Test that a killing pending actors is prioritized over killing idle actors on
        # the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        # Add pending worker.
        pending_actor, _ = self._add_pending_actor(pool)
        # Add idle worker.
        idle_actor = self._add_ready_actor(pool)
        # Kill inactive actor.
        killed = pool.kill_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that the idle actor is still in the pool.
        assert pool.pick_actor() == idle_actor
        pool.return_actor(idle_actor)
        # Check that the pending actor is not in pool.
        assert pool.get_pending_actor_refs() == []
        # Check that actor is dead.
        actor_id = pending_actor._actor_id.hex()
        del pending_actor
        self._wait_for_actor_dead(actor_id)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 1
        assert pool.num_free_slots() == 4

    def test_kill_all_inactive_pending_actor_killed(self):
        # Test that pending actors are killed on the kill_all_inactive_actors() call.
        pool = self._create_actor_pool()
        actor, ready_ref = self._add_pending_actor(pool)
        # Kill inactive actors.
        pool.kill_all_inactive_actors()
        # Check that actor is not in pool.
        assert pool.get_pending_actor_refs() == []
        # Check that actor is no longer in the pool as pending, to protect against
        # ready/killed races.
        assert not pool.pending_to_running(ready_ref)
        # Check that actor is dead.
        actor_id = actor._actor_id.hex()
        del actor
        self._wait_for_actor_dead(actor_id)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0

    def test_kill_all_inactive_idle_actor_killed(self):
        # Test that idle actors are killed on the kill_all_inactive_actors() call.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Kill inactive actors.
        pool.kill_all_inactive_actors()
        # Check that actor is not in pool.
        assert pool.pick_actor() is None
        # Check that actor is dead.
        actor_id = actor._actor_id.hex()
        del actor
        self._wait_for_actor_dead(actor_id)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0

    def test_kill_all_inactive_active_actor_not_killed(self):
        # Test that active actors are NOT killed on the kill_all_inactive_actors() call.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == actor
        # Kill inactive actors.
        pool.kill_all_inactive_actors()
        # Check that the active actor is still in the pool.
        assert pool.pick_actor() == actor

    def test_kill_all_inactive_future_idle_actors_killed(self):
        # Test that future idle actors are killed after the kill_all_inactive_actors()
        # call.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
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
        # Check that actor is dead.
        actor_id = actor._actor_id.hex()
        del actor
        self._wait_for_actor_dead(actor_id)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0

    def test_kill_all_inactive_mixture(self):
        # Test that in a mixture of pending, idle, and active actors, only the pending
        # and idle actors are killed on the kill_all_inactive_actors() call.
        pool = self._create_actor_pool()
        # Add active actor.
        actor1 = self._add_ready_actor(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == actor1
        # Add idle actor.
        self._add_ready_actor(pool)
        # Add pending actor.
        actor3, ready_ref = self._add_pending_actor(pool)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 3
        assert pool.num_pending_actors() == 1
        assert pool.num_running_actors() == 2
        assert pool.num_active_actors() == 1
        assert pool.num_idle_actors() == 1
        assert pool.num_free_slots() == 7
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
        # Check that actor is dead.
        actor_id = actor1._actor_id.hex()
        del actor1
        self._wait_for_actor_dead(actor_id)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0

    def test_all_actors_killed(self):
        # Test that all actors are killed after the kill_all_actors() call.
        pool = self._create_actor_pool()
        active_actor = self._add_ready_actor(pool)
        # Pick actor (and double-check that the actor was picked).
        assert pool.pick_actor() == active_actor
        idle_actor = self._add_ready_actor(pool)
        # Kill all actors, including active actors.
        pool.kill_all_actors()
        # Check that the pool is empty.
        assert pool.pick_actor() is None

        # Check that both actors are dead
        actor_id = active_actor._actor_id.hex()
        del active_actor
        self._wait_for_actor_dead(actor_id)
        actor_id = idle_actor._actor_id.hex()
        del idle_actor
        self._wait_for_actor_dead(actor_id)

        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 0
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_slots() == 0

    def test_locality_manager_actor_ranking(self):
        pool = self._create_actor_pool(max_tasks_in_flight=2)

        # Setup bundle mocks.
        bundles = make_ref_bundles([[0] for _ in range(10)])
        fake_loc_map = {}
        for i, b in enumerate(bundles):
            fake_loc_map[b] = "node1"
        pool._get_location = lambda b: fake_loc_map[b]

        # Setup an actor on each node.
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        # Actors on node1 should be preferred.
        res1 = pool.pick_actor(bundles[0])
        assert res1 == actor1
        res2 = pool.pick_actor(bundles[1])
        assert res2 == actor1

        # Fallback to remote actors.
        res3 = pool.pick_actor(bundles[2])
        assert res3 == actor2
        res4 = pool.pick_actor(bundles[3])
        assert res4 == actor2
        res5 = pool.pick_actor(bundles[4])
        assert res5 is None

    def test_locality_manager_busyness_ranking(self):
        pool = self._create_actor_pool(max_tasks_in_flight=2)

        # Setup bundle mocks.
        bundles = make_ref_bundles([[0] for _ in range(10)])
        fake_loc_map = {}
        # Also test unknown location handling.
        for i, b in enumerate(bundles):
            fake_loc_map[b] = None
        pool._get_location = lambda b: fake_loc_map[b]

        # Setup two actors on the same node.
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        # Fake actor 2 as more busy.
        pool._num_tasks_in_flight[actor2] = 1
        res1 = pool.pick_actor(bundles[0])
        assert res1 == actor1

        # Fake actor 2 as more busy again.
        pool._num_tasks_in_flight[actor2] = 2
        res2 = pool.pick_actor(bundles[0])
        assert res2 == actor1

        # Nothing left
        res3 = pool.pick_actor(bundles[0])
        assert res3 is None


def test_start_actor_timeout(ray_start_regular_shared, restore_data_context):
    """Tests that ActorPoolMapOperator raises an exception on
    timeout while waiting for actors."""

    class UDFClass:
        def __call__(self, x):
            return x

    from ray.exceptions import GetTimeoutError

    ray.data.DataContext.get_current().wait_for_min_actors_s = 1

    with pytest.raises(
        GetTimeoutError,
        match=(
            "Timed out while starting actors. This may mean that the cluster "
            "does not have enough resources for the requested actor pool."
        ),
    ):
        # Specify an unachievable resource requirement to ensure
        # we timeout while waiting for actors.
        ray.data.range(10).map_batches(
            UDFClass,
            batch_size=1,
            compute=ray.data.ActorPoolStrategy(size=5),
            num_gpus=100,
        ).take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
