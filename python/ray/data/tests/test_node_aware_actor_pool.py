from unittest.mock import patch

import pytest

# Imported under a name that doesn't start with "Test" so pytest doesn't collect the
# base suite twice.
import ray.data.tests.test_actor_pool_map_operator as oss_test_module
from ray.core.generated import gcs_pb2
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    AutoscalingActorConfig,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    _NodeAwareActorPool,
)
from ray.data.tests.conftest import *  # noqa: F403

_MODULE = "ray.data._internal.execution.operators.actor_pool_map_operator"

_make_bundle_queue = oss_test_module._make_bundle_queue
_schedule_bundles = oss_test_module._schedule_bundles


class TestNodeAwareActorPool(oss_test_module.TestActorPool):
    """Re-runs the whole ``_ActorPool`` suite against ``_NodeAwareActorPool``, plus
    the draining-node cases that only the node-aware pool implements."""

    def _create_actor_pool(
        self,
        min_size=1,
        max_size=4,
        initial_size=1,
        max_tasks_in_flight=4,
        map_worker_cls_name="MapWorker",
    ):
        config = AutoscalingActorConfig(
            min_size=min_size,
            max_size=max_size,
            initial_size=initial_size,
            max_tasks_in_flight_per_actor=max_tasks_in_flight,
            max_actor_concurrency=1,
            per_actor_resource_usage=ExecutionResources(cpu=1),
        )
        return _NodeAwareActorPool(
            create_actor_fn=self._create_actor_fn,
            map_worker_cls_name=map_worker_cls_name,
            config=config,
        )

    @patch(f"{_MODULE}.get_draining_nodes")
    @patch(f"{_MODULE}.get_actor_locations")
    def test_selector_excludes_actors_on_draining_nodes(
        self, mock_get_actor_locations, mock_get_draining_nodes
    ):
        """Test that actors on draining nodes are excluded from scheduling.

        Verifies that:
            - can_schedule_task() returns False when all actors are on draining nodes
            - can_schedule_task() returns True when at least one actor is on non-draining node
            - select_actors() does not return an actor on a draining node
        """
        pool = self._create_actor_pool(max_tasks_in_flight=1)

        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        assert pool.num_running_actors() == 2

        mock_get_actor_locations.return_value = {
            pool._get_actor_logical_id(actor1): "node1",
            pool._get_actor_logical_id(actor2): "node2",
        }

        # Case 1: No draining nodes, both actors should be schedulable
        mock_get_draining_nodes.return_value = set()
        pool.refresh_actor_state()
        assert pool.can_schedule_task()

        results = _schedule_bundles(pool, _make_bundle_queue(2))
        assert len(results) == 2
        assert {actor1, actor2} == set(results)

        # Return the actors so we can schedule again
        for actor in results:
            pool.on_task_completed(actor)

        # Case 2: node1 is draining, only actor2 should be schedulable
        mock_get_draining_nodes.return_value = {"node1"}
        pool.refresh_actor_state()
        assert pool.can_schedule_task()

        results = _schedule_bundles(pool, _make_bundle_queue(1))
        assert results == [actor2]

        pool.on_task_completed(actor2)

        # Case 3: Both nodes draining - no actors should be schedulable
        mock_get_draining_nodes.return_value = {"node1", "node2"}
        pool.refresh_actor_state()
        assert not pool.can_schedule_task()

        bundle = _make_bundle_queue(1).get_next()
        assert pool.select_actors(bundle=bundle, actor_locality_enabled=False) is None

        # Case 4: node1 stops draining (actor1 becomes schedulable again)
        mock_get_draining_nodes.return_value = {"node2"}
        pool.refresh_actor_state()
        assert pool.can_schedule_task()

        results = _schedule_bundles(pool, _make_bundle_queue(1))
        assert results == [actor1]

    @patch(f"{_MODULE}.get_draining_nodes")
    @patch(f"{_MODULE}.get_actor_locations")
    def test_three_actors_one_node_draining(
        self, mock_get_actor_locations, mock_get_draining_nodes
    ):
        """With three actors on three nodes, draining one node leaves two schedulable."""
        pool = self._create_actor_pool(max_tasks_in_flight=2)

        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")
        actor3 = self._add_ready_actor(pool, node_id="node3")
        assert pool.num_running_actors() == 3

        mock_get_actor_locations.return_value = {
            pool._get_actor_logical_id(actor1): "node1",
            pool._get_actor_logical_id(actor2): "node2",
            pool._get_actor_logical_id(actor3): "node3",
        }
        mock_get_draining_nodes.return_value = {"node2"}
        pool.refresh_actor_state()

        assert pool.can_schedule_task()
        # Schedule 4 tasks (2 per actor max); only actor1 and actor3 should be used
        results = _schedule_bundles(pool, _make_bundle_queue(4))
        assert len(results) == 4
        assert actor2 not in results
        assert all(actor in (actor1, actor3) for actor in results)

    @patch(f"{_MODULE}.get_draining_nodes", return_value={})
    @patch(f"{_MODULE}.get_actor_locations", return_value={})
    def test_unknown_actor_location_falls_back(
        self, mock_get_actor_locations, mock_get_draining_nodes
    ):
        """An actor the location tracker hasn't recorded keeps its known location.

        ``ActorLocationTracker.update_actor_location`` is fire-and-forget, so a
        just-created actor can be missing from the snapshot. Losing its location
        would make it invisible to locality-based selection.
        """
        pool = self._create_actor_pool(max_tasks_in_flight=2)
        actor = self._add_ready_actor(pool, node_id="node1")

        pool.refresh_actor_state()

        assert pool.get_actor_location(actor) == "node1"
        assert "node1" in pool._alive_node_to_actor_heap
        assert actor in pool._alive_node_to_actor_heap["node1"]

    def test_select_actors_spreads_without_locality(self):
        """With locality disabled, select_actors spreads tasks across actors."""
        pool = self._create_actor_pool(max_tasks_in_flight=2)
        self._add_ready_actor(pool, node_id="node1")
        self._add_ready_actor(pool, node_id="node2")
        self._add_ready_actor(pool, node_id="node3")
        assert pool.num_running_actors() == 3

        results = _schedule_bundles(
            pool, _make_bundle_queue(6), actor_locality_enabled=False
        )

        assert len(results) == 6

        first_half = results[:3]
        last_half = results[3:]

        # The pool should evenly spread the load between actors, so
        # each half should have unique actors
        assert len(set(first_half)) == 3
        assert len(set(last_half)) == 3

    def test_can_schedule_task_false_when_all_slots_busy(self):
        """can_schedule_task returns False when every actor is at max_tasks_in_flight."""
        pool = self._create_actor_pool(max_tasks_in_flight=1)
        actor1 = self._add_ready_actor(pool)
        self._add_ready_actor(pool)

        assert pool.can_schedule_task()
        results = _schedule_bundles(pool, _make_bundle_queue(2))
        assert len(results) == 2
        assert not pool.can_schedule_task()
        pool.on_task_completed(actor1)
        assert pool.can_schedule_task()

    @patch(f"{_MODULE}.get_draining_nodes", return_value=set())
    @patch(f"{_MODULE}.get_actor_locations")
    def test_restarting_actor_excluded_from_scheduling(
        self, mock_get_actor_locations, mock_get_draining_nodes
    ):
        """Test that restarting actors are excluded from scheduling.

        When one actor is RESTARTING (mocked via _get_local_state), can_schedule_task
        and select_actors should use only the alive actor.
        """
        pool = self._create_actor_pool(max_tasks_in_flight=1)
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")
        assert pool.num_running_actors() == 2

        mock_get_actor_locations.return_value = {
            pool._get_actor_logical_id(actor1): "node1",
            pool._get_actor_logical_id(actor2): "node2",
        }
        pool.refresh_actor_state()
        assert pool.can_schedule_task()

        with patch.object(
            actor1,
            "_get_local_state",
            return_value=gcs_pb2.ActorTableData.ActorState.RESTARTING,
        ):
            pool.refresh_actor_state()

        assert pool.num_restarting_actors() == 1
        assert pool.num_alive_actors() == 1
        assert pool.num_running_actors() == 2
        # Should still be schedulable (actor2 is alive)
        assert pool.can_schedule_task()

        # select_actors should return actor2, not actor1
        bundle = _make_bundle_queue(1).get_next()
        actor = pool.select_actors(bundle=bundle, actor_locality_enabled=False)
        assert actor == actor2
        pool.on_task_submitted(actor)
        # No more schedulable actors
        assert not pool.can_schedule_task()
        assert pool.num_idle_actors() == 1
        assert pool.num_running_actors() == 2
        assert pool.num_restarting_actors() == 1

        with patch.object(
            actor1,
            "_get_local_state",
            return_value=gcs_pb2.ActorTableData.ActorState.ALIVE,
        ):
            pool.refresh_actor_state()
        assert pool.num_restarting_actors() == 0
        assert pool.num_alive_actors() == 2
        assert pool.num_running_actors() == 2
        assert pool.can_schedule_task()
        assert pool.num_idle_actors() == 1

        pool.on_task_completed(actor2)
        assert pool.num_idle_actors() == 2

    @patch(f"{_MODULE}.get_draining_nodes", return_value=set())
    @patch(f"{_MODULE}.get_actor_locations")
    def test_all_actors_restarting_not_schedulable(
        self, mock_get_actor_locations, mock_get_draining_nodes
    ):
        """When all actors are RESTARTING, can_schedule_task returns False."""
        pool = self._create_actor_pool(max_tasks_in_flight=1)
        actor = self._add_ready_actor(pool, node_id="node1")
        assert pool.num_running_actors() == 1

        mock_get_actor_locations.return_value = {
            pool._get_actor_logical_id(actor): "node1",
        }
        pool.refresh_actor_state()
        assert pool.can_schedule_task()

        with patch.object(
            actor,
            "_get_local_state",
            return_value=gcs_pb2.ActorTableData.ActorState.RESTARTING,
        ):
            pool.refresh_actor_state()

        assert pool.num_restarting_actors() == 1
        assert pool.num_alive_actors() == 0
        assert not pool.can_schedule_task()

        bundle = _make_bundle_queue(1).get_next()
        assert pool.select_actors(bundle=bundle, actor_locality_enabled=False) is None

    @patch(f"{_MODULE}.get_draining_nodes")
    @patch(f"{_MODULE}.get_actor_locations")
    def test_select_actor_none_probes_schedulability(
        self, mock_get_actor_locations, mock_get_draining_nodes
    ):
        """select_actors(bundle=None) agrees with can_schedule_task."""
        pool = self._create_actor_pool(max_tasks_in_flight=1)

        # Empty pool
        assert pool.select_actors() is None
        assert not pool.can_schedule_task()

        actor = self._add_ready_actor(pool, node_id="node1")
        mock_get_actor_locations.return_value = {
            pool._get_actor_logical_id(actor): "node1",
        }
        mock_get_draining_nodes.return_value = set()
        pool.refresh_actor_state()

        assert pool.select_actors() is not None
        assert pool.can_schedule_task()

        # Exhaust capacity
        pool.on_task_submitted(actor)
        assert pool.select_actors() is None
        assert not pool.can_schedule_task()

        # Free a slot
        pool.on_task_completed(actor)
        assert pool.select_actors() is not None
        assert pool.can_schedule_task()

        # Drain the node
        mock_get_draining_nodes.return_value = {"node1"}
        pool.refresh_actor_state()
        assert pool.select_actors() is None
        assert not pool.can_schedule_task()

    @patch.object(
        RefBundle,
        "get_preferred_object_locations",
        return_value={"node1": 1024, "node2": 512},
    )
    @patch(f"{_MODULE}.get_draining_nodes", return_value={"node1"})
    @patch(f"{_MODULE}.get_actor_locations")
    def test_locality_skips_draining_preferred_node(
        self, mock_get_actor_locations, mock_get_draining_nodes, _mock_locs
    ):
        """Locality falls through to the next node when the best one is draining.

        node1 holds the most bytes, so it wins locality, but it's draining. Every
        task should land on actor2 instead of waiting for or retrying on actor1.
        """
        pool = self._create_actor_pool(max_tasks_in_flight=2)

        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")
        mock_get_actor_locations.return_value = {
            pool._get_actor_logical_id(actor1): "node1",
            pool._get_actor_logical_id(actor2): "node2",
        }
        pool.refresh_actor_state()

        results = _schedule_bundles(
            pool, _make_bundle_queue(4), actor_locality_enabled=True
        )

        # Only actor2 is usable, so it fills to max_tasks_in_flight and stops.
        assert results == [actor2, actor2]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
