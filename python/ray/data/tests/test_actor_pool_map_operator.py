import asyncio
import collections
import datetime
import threading
import time
import unittest
from dataclasses import replace
from typing import Any, Dict, Optional, Tuple
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

import ray
from ray._common.test_utils import wait_for_condition
from ray.actor import ActorHandle
from ray.data._internal.execution.autoscaler.default_autoscaler import (
    ActorPoolScalingRequest,
)
from ray.data._internal.execution.bundle_queue import FIFOBundleQueue
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.physical_operator import _ActorPoolInfo
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
    _ActorPool,
    _ActorTaskSelector,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.tests.conftest import *  # noqa
from ray.types import ObjectRef


@ray.remote
class PoolWorker:
    def __init__(self, node_id: str = "node1"):
        self.node_id = node_id

    def get_location(self) -> str:
        return self.node_id

    def on_exit(self):
        pass


class TestActorPool(unittest.TestCase):
    def setup_class(self):
        self._last_created_actor_and_ready_ref: Optional[
            Tuple[ActorHandle, ObjectRef[Any]]
        ] = None
        self._actor_node_id = "node1"
        ray.init(num_cpus=4)

    def teardown_class(self):
        ray.shutdown()

    def _create_task_selector(self, pool: _ActorPool) -> _ActorTaskSelector:
        return ActorPoolMapOperator._create_task_selector(pool)

    def _pick_actor(
        self,
        pool: _ActorPool,
        bundle: Optional[RefBundle] = None,
        actor_locality_enabled: bool = False,
    ) -> ActorHandle:
        if bundle is None:
            bundles = make_ref_bundles([[0]])
        else:
            bundles = [bundle]
        queue = FIFOBundleQueue()
        for bundle in bundles:
            queue.add(bundle)
        actor_task_selector = self._create_task_selector(pool)
        it = actor_task_selector.select_actors(queue, actor_locality_enabled)
        try:
            actor = next(it)[1]
            pool.on_task_submitted(actor)
            return actor
        except StopIteration:
            return None

    def _create_actor_fn(
        self,
        labels: Dict[str, Any],
        logical_actor_id: str = "Actor1",
    ) -> Tuple[ActorHandle, ObjectRef[Any]]:
        actor = PoolWorker.options(_labels=labels).remote(self._actor_node_id)
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
            min_size=min_size,
            max_size=max_size,
            max_actor_concurrency=1,
            max_tasks_in_flight_per_actor=max_tasks_in_flight,
            create_actor_fn=self._create_actor_fn,
            per_actor_resource_usage=ExecutionResources(cpu=1),
        )
        return pool

    def _add_pending_actor(
        self, pool: _ActorPool, node_id="node1"
    ) -> Tuple[ActorHandle, ObjectRef[Any]]:
        self._actor_node_id = node_id
        num_actors = pool.scale(
            ActorPoolScalingRequest(delta=1, reason="adding pending actor")
        )

        assert num_actors == 1
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

    def test_can_scale_down(self):
        pool = self._create_actor_pool(min_size=1, max_size=4)

        downscaling_request = ActorPoolScalingRequest.downscale(
            delta=-1, reason="scaling down"
        )

        with freeze_time() as f:
            # Scale up
            pool.scale(ActorPoolScalingRequest(delta=1, reason="scaling up"))
            # Assert we can't scale down immediately after scale up
            assert not pool._can_apply(downscaling_request)
            assert pool._last_upscaled_at == time.time()

            # Check that we can still scale down if downscaling request
            # is a forced one
            assert pool._can_apply(replace(downscaling_request, force=True))

            # Advance clock
            f.tick(
                datetime.timedelta(
                    seconds=_ActorPool._ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S + 1
                )
            )

            # Assert can scale down after debounce period
            assert pool._can_apply(downscaling_request)

    def test_add_pending(self):
        # Test that pending actor is added in the correct state.
        pool = self._create_actor_pool()
        _, ready_ref = self._add_pending_actor(pool)

        # Check that the pending actor is not pickable.
        assert self._pick_actor(pool) is None

        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 1
        assert pool.num_running_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.num_free_task_slots() == 0
        # Check that ready future is returned.
        assert pool.get_pending_actor_refs() == [ready_ref]

    def test_pending_to_running(self):
        # Test that pending actor is correctly transitioned to running.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Check that the actor is pickable.
        picked_actor = self._pick_actor(pool)
        assert picked_actor == actor
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 1
        assert pool.num_idle_actors() == 0
        assert pool.num_free_task_slots() == 3

    def test_restarting_to_alive(self):
        # Test that actor is correctly transitioned from restarting to alive.
        pool = self._create_actor_pool(max_tasks_in_flight=1)
        actor = self._add_ready_actor(pool)

        # Mark the actor as restarting and test pick_actor fails
        pool.update_running_actor_state(actor, True)
        assert self._pick_actor(pool) is None
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_restarting_actors() == 1
        assert pool.num_alive_actors() == 0
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 1
        assert pool.num_free_task_slots() == 1
        assert pool.get_actor_info() == _ActorPoolInfo(
            running=0, pending=0, restarting=1
        )

        # Mark the actor as alive and test pick_actor succeeds
        pool.update_running_actor_state(actor, False)
        picked_actor = self._pick_actor(pool)
        assert picked_actor == actor
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_restarting_actors() == 0
        assert pool.num_alive_actors() == 1
        assert pool.num_active_actors() == 1
        assert pool.num_idle_actors() == 0
        assert pool.num_free_task_slots() == 0
        assert pool.get_actor_info() == _ActorPoolInfo(
            running=1, pending=0, restarting=0
        )

        # Return the actor
        pool.on_task_completed(picked_actor)
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_restarting_actors() == 0
        assert pool.num_alive_actors() == 1
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 1
        assert pool.num_free_task_slots() == 1
        assert pool.get_actor_info() == _ActorPoolInfo(
            running=1, pending=0, restarting=0
        )

    def test_repeated_picking(self):
        # Test that we can repeatedly pick the same actor.
        pool = self._create_actor_pool(max_tasks_in_flight=999)
        actor = self._add_ready_actor(pool)
        for _ in range(10):
            picked_actor = self._pick_actor(pool)
            assert picked_actor == actor

    def test_return_actor(self):
        # Test that we can return an actor as many times as we've picked it.
        pool = self._create_actor_pool(max_tasks_in_flight=999)
        self._add_ready_actor(pool)
        for _ in range(10):
            picked_actor = self._pick_actor(pool)
        # Return the actor as many times as it was picked.
        for _ in range(10):
            pool.on_task_completed(picked_actor)

        # Returning the actor more times than it has been picked should raise an
        # AssertionError.
        with pytest.raises(AssertionError):
            pool.on_task_completed(picked_actor)
        # Check that the per-state pool sizes are as expected.
        assert pool.current_size() == 1
        assert pool.num_pending_actors() == 0
        assert pool.num_running_actors() == 1
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 1  # Actor should now be idle.
        assert pool.num_free_task_slots() == 999

    def test_pick_max_tasks_in_flight(self):
        # Test that we can't pick an actor beyond the max_tasks_in_flight cap.
        pool = self._create_actor_pool(max_tasks_in_flight=2)
        actor = self._add_ready_actor(pool)
        assert pool.num_free_task_slots() == 2
        assert self._pick_actor(pool) == actor
        assert pool.num_free_task_slots() == 1
        assert self._pick_actor(pool) == actor
        assert pool.num_free_task_slots() == 0
        # Check that the 3rd pick doesn't return the actor.
        assert self._pick_actor(pool) is None

    def test_pick_ordering_lone_idle(self):
        # Test that a lone idle actor is the one that's picked.
        pool = self._create_actor_pool()
        self._add_ready_actor(pool)
        # Ensure that actor has been picked once.
        self._pick_actor(pool)
        # Add a new, idle actor.
        actor2 = self._add_ready_actor(pool)
        # Check that picked actor is the idle newly added actor.
        picked_actor = self._pick_actor(pool)
        assert picked_actor == actor2

    def test_pick_ordering_full_order(self):
        # Test that the least loaded actor is always picked.
        pool = self._create_actor_pool()
        # Add 4 actors to the pool.
        actors = [self._add_ready_actor(pool) for _ in range(4)]
        # Pick 4 actors.
        picked_actors = [self._pick_actor(pool) for _ in range(4)]
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
        picked_actors = [self._pick_actor(pool) for _ in range(8)]
        pick_counts = collections.Counter(picked_actors)
        # Check that picks were evenly distributed over the pool.
        assert len(pick_counts) == 4
        for actor, count in pick_counts.items():
            assert actor in actors
            assert count == 2
        # Check that the next pick doesn't return an actor.
        assert self._pick_actor(pool) is None

    def test_pick_ordering_with_returns(self):
        # Test that pick ordering works with returns.
        pool = self._create_actor_pool()
        actor1 = self._add_ready_actor(pool)
        actor2 = self._add_ready_actor(pool)
        picked_actors = [self._pick_actor(pool) for _ in range(2)]
        # Double-check that both actors were picked.
        assert set(picked_actors) == {actor1, actor2}
        # Return actor 2, implying that it's now idle.
        pool.on_task_completed(actor2)
        # Check that actor 2 is the next actor that's picked.
        picked_actor = self._pick_actor(pool)
        assert picked_actor == actor2

    def test_kill_inactive_pending_actor(self):
        # Test that a pending actor is killed on the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        actor, _ = self._add_pending_actor(pool)
        # Kill inactive actor.
        killed = pool._remove_inactive_actor()
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
        assert pool.num_free_task_slots() == 0

    def test_kill_inactive_idle_actor(self):
        # Test that a idle actor is killed on the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Kill inactive actor.
        killed = pool._remove_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that actor is not in pool.
        assert self._pick_actor(pool) is None
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
        assert pool.num_free_task_slots() == 0

    def test_kill_inactive_active_actor_not_killed(self):
        # Test that active actors are NOT killed on the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        actor = self._add_ready_actor(pool)
        # Pick actor (and double-check that the actor was picked).
        picked_actor = self._pick_actor(pool)
        assert picked_actor == actor
        # Kill inactive actor.
        killed = pool._remove_inactive_actor()
        # Check that an actor was NOT killed.
        assert not killed
        # Check that the active actor is still in the pool.
        picked_actor = self._pick_actor(pool)
        assert picked_actor == actor

    def test_kill_inactive_pending_over_idle(self):
        # Test that a killing pending actors is prioritized over killing idle actors on
        # the kill_inactive_actor() call.
        pool = self._create_actor_pool()
        # Add pending worker.
        pending_actor, _ = self._add_pending_actor(pool)
        # Add idle worker.
        idle_actor = self._add_ready_actor(pool)
        # Kill inactive actor.
        killed = pool._remove_inactive_actor()
        # Check that an actor was killed.
        assert killed
        # Check that the idle actor is still in the pool.
        picked_actor = self._pick_actor(pool)
        assert picked_actor == idle_actor
        pool.on_task_completed(idle_actor)
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
        assert pool.num_free_task_slots() == 4

    def test_all_actors_killed(self):
        # Test that all actors are killed after the kill_all_actors() call.
        pool = self._create_actor_pool()
        active_actor = self._add_ready_actor(pool)
        # Pick actor (and double-check that the actor was picked).
        assert self._pick_actor(pool) == active_actor
        idle_actor = self._add_ready_actor(pool)
        # Kill all actors, including active actors.
        pool.shutdown()
        # Check that the pool is empty.
        assert self._pick_actor(pool) is None

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
        assert pool.num_free_task_slots() == 0

    def test_locality_based_actor_ranking(self):
        pool = self._create_actor_pool(max_tasks_in_flight=2)

        # Setup bundle mocks.
        bundles = make_ref_bundles([[0] for _ in range(5)])

        # Patch all bundles to return mocked preferred locations
        def _get_preferred_locs():
            # Node1 is higher in priority
            return {"node1": 1024, "node2": 512}

        for b in bundles:
            # monkeypatch the get_preferred_object_locations method
            b.get_preferred_object_locations = _get_preferred_locs

        # Setup an actor on each node.
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        # Create the mock bundle queue
        bundle_queue = FIFOBundleQueue()
        for bundle in bundles:
            bundle_queue.add(bundle)

        # Create the mock task actor selector iterator
        task_selector = self._create_task_selector(pool)
        it = task_selector.select_actors(bundle_queue, actor_locality_enabled=True)

        # Actors on node1 should be preferred
        res1 = next(it)[1]
        pool.on_task_submitted(res1)
        assert res1 == actor1

        # Actors on node1 should be preferred still
        res2 = next(it)[1]
        pool.on_task_submitted(res2)
        assert res2 == actor1

        # Fallback to remote actors
        res3 = next(it)[1]
        pool.on_task_submitted(res3)
        assert res3 == actor2

        # NOTE: Actor 2 is selected (since Actor 1 is at capacity)
        res4 = next(it)[1]
        pool.on_task_submitted(res4)
        assert res4 == actor2

        # NOTE: Actor 2 is at max requests in-flight, hence excluded
        try:
            res5 = next(it)[1]
        except StopIteration:
            res5 = None
        assert res5 is None

    def test_locality_based_actor_ranking_no_locations(self):
        pool = self._create_actor_pool(max_tasks_in_flight=2)

        # Setup bundle mocks
        bundles = make_ref_bundles([[0] for _ in range(10)])

        # Patch all bundles to return mocked preferred locations
        for b in bundles:
            # monkeypatch the get_preferred_object_locations method
            b.get_preferred_object_locations = lambda: {}

        # Create the mock bundle queue
        bundle_queue = FIFOBundleQueue()
        for bundle in bundles:
            bundle_queue.add(bundle)

        # Add one actor to the pool
        actor1 = self._add_ready_actor(pool, node_id="node1")

        # Create the mock task actor selector iterator
        task_selector = self._create_task_selector(pool)
        it = task_selector.select_actors(bundle_queue, actor_locality_enabled=True)

        # Select one actor to schedule it on actor1
        res1 = next(it)[1]
        pool.on_task_submitted(res1)
        assert res1 == actor1

        # Add another actor to the pool
        actor2 = self._add_ready_actor(pool, node_id="node2")

        # Re-create the mock task actor selector iterator
        task_selector = self._create_task_selector(pool)
        it = task_selector.select_actors(bundle_queue, actor_locality_enabled=True)

        # Select and actor, it should be scheudled on actor2
        res2 = next(it)[1]
        pool.on_task_submitted(res2)
        assert res2 == actor2

        # Select another actor, it could be either actor1 or actor2
        res3 = next(it)[1]
        pool.on_task_submitted(res3)

        # Select another actor, it should be the other actor
        res4 = next(it)[1]
        pool.on_task_submitted(res4)
        if res3 == actor1:
            assert res4 == actor2
        else:
            assert res4 == actor1

        # Nothing left
        try:
            res5 = next(it)[1]
        except StopIteration:
            res5 = None
        assert res5 is None


def test_min_max_resource_requirements(restore_data_context):
    data_context = ray.data.DataContext.get_current()
    op = ActorPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        target_max_block_size=None,
        compute_strategy=ray.data.ActorPoolStrategy(
            min_size=1,
            max_size=2,
        ),
        ray_remote_args={"num_cpus": 1},
    )
    op._metrics = MagicMock(obj_store_mem_max_pending_output_per_task=3)

    (
        min_resource_usage_bound,
        max_resource_usage_bound,
    ) = op.min_max_resource_requirements()

    assert (
        min_resource_usage_bound == ExecutionResources(cpu=1, object_store_memory=3)
        and max_resource_usage_bound == ExecutionResources.for_limits()
    )


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


def test_actor_pool_fault_tolerance_e2e(ray_start_cluster, restore_data_context):
    """Test that a dataset with actor pools can finish, when
    all nodes in the cluster are removed and added back."""
    ray.shutdown()

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init()

    # Ensure block size is small enough to pass resource limits
    context = ray.data.DataContext.get_current()
    context.target_max_block_size = 1

    @ray.remote(num_cpus=0)
    class Signal:
        def __init__(self):
            self._node_id = ray.get_runtime_context().get_node_id()
            self._num_alive_actors = 0
            self._all_nodes_removed = False
            self._all_nodes_restarted = False

        async def notify_actor_alive(self):
            self._num_alive_actors += 1

        async def wait_for_actors_alive(self, value):
            while self._num_alive_actors != value:
                await asyncio.sleep(0.01)

        async def notify_nodes_removed(self):
            self._all_nodes_removed = True

        async def notify_nodes_restarted(self):
            self._all_nodes_restarted = True

        async def wait_for_nodes_removed(self):
            while not self._all_nodes_removed:
                await asyncio.sleep(0.01)

        async def wait_for_nodes_restarted(self):
            while not self._all_nodes_restarted:
                await asyncio.sleep(0.01)

    # Create the signal actor on the head node.
    signal_actor = Signal.remote()

    # Spin up nodes
    num_nodes = 1
    nodes = []
    for _ in range(num_nodes):
        nodes.append(cluster.add_node(num_cpus=10, num_gpus=1))
    cluster.wait_for_nodes()

    class MyUDF:
        def __init__(self, signal_actor):
            self._node_id = ray.get_runtime_context().get_node_id()
            self._signal_actor = signal_actor
            self._signal_sent = False

        def __call__(self, batch):
            if not self._signal_sent:
                # Notify the Actor is alive
                self._signal_actor.notify_actor_alive.remote()

                # Wait for the driver to remove nodes. This makes sure all
                # actors are running tasks when removing nodes.
                ray.get(self._signal_actor.wait_for_nodes_removed.remote())

                self._signal_sent = True

            return batch

    res = []
    num_items = 100

    def run_dataset():
        nonlocal res

        ds = ray.data.range(num_items, override_num_blocks=num_items)
        ds = ds.map_batches(
            MyUDF,
            fn_constructor_args=[signal_actor],
            concurrency=num_nodes,
            batch_size=1,
            num_gpus=1,
        )
        res = ds.take_all()

    # Kick off Actors
    thread = threading.Thread(target=run_dataset)
    thread.start()

    # Wait for all actors to start
    ray.get(signal_actor.wait_for_actors_alive.remote(num_nodes))

    # Remove all the nodes
    for node in nodes:
        cluster.remove_node(node)
    nodes.clear()
    ray.get(signal_actor.notify_nodes_removed.remote())

    # Add back all the nodes
    for _ in range(num_nodes):
        nodes.append(cluster.add_node(num_cpus=10, num_gpus=1))
    cluster.wait_for_nodes()
    ray.get(signal_actor.notify_nodes_restarted.remote())

    thread.join()
    assert sorted(res, key=lambda x: x["id"]) == [{"id": i} for i in range(num_items)]


def test_lazy_actor_starting_execution(ray_start_10_cpus_shared, restore_data_context):
    """Tests that actor pool operators use lazy starting execution.

    This test verifies that actors are not started immediately when the DAG is created,
    but only when the operator becomes eligible to run.
    """
    import time

    import ray

    # ------------------------------------------------------------------
    # Helper: lightweight counter actor to record when workers spin up.
    # ------------------------------------------------------------------

    @ray.remote(num_cpus=0)
    class _Counter:
        def __init__(self):
            self._n = 0

        def inc(self):
            self._n += 1

        def get(self):
            return self._n

    # Create (or get) the counter that workers will update.
    try:
        counter = ray.get_actor("tracking_counter")
    except ValueError:
        counter = _Counter.options(name="tracking_counter").remote()

    class TrackingActor:
        """Map-batch worker whose constructor touches `tracking_counter`."""

        def __init__(self):
            # Bump the shared counter so the driver knows an actor was created.
            ray.get_actor("tracking_counter").inc.remote()
            self.initialized = True
            print(f"TrackingActor initialized at {time.time()}")

        def __call__(self, batch):
            # Add a small delay to ensure this actor is actually needed
            time.sleep(0.01)
            # batch is a dict like {"id": [0, 1, 2, 3, 4]}
            # Return transformed batch in same format
            return {"id": [x * 2 for x in batch["id"]]}

    # Test 1: Verify DAG creation doesn't start actors
    dag_creation_start = time.time()
    print(f"Starting DAG creation at {dag_creation_start}")

    # Create a larger dataset to ensure the actor stage gets triggered
    ds = ray.data.range(100, override_num_blocks=10).map_batches(  # Larger dataset
        TrackingActor,
        batch_size=10,  # Reasonable batch size
        compute=ray.data.ActorPoolStrategy(
            min_size=1,  # Start with 1 actor to ensure scaling
            max_size=2,  # Allow scaling to verify execution starts
        ),
    )

    dag_creation_time = time.time() - dag_creation_start
    print(f"DAG creation completed in {dag_creation_time:.3f}s")
    # Query counter before execution – should be zero.
    assert ray.get(counter.get.remote()) == 0

    # DAG creation should be fast since actors haven't started
    assert (
        dag_creation_time < 1.0
    ), f"DAG creation took {dag_creation_time}s, should be fast"

    # Test 2: Execute and verify actors start when needed
    execution_start = time.time()
    print(f"Starting execution at {execution_start}")
    result = ds.take_all()
    execution_time = time.time() - execution_start
    print(f"Execution completed in {execution_time:.3f}s")
    actor_count = ray.get(counter.get.remote())
    print(f"Actors started: {actor_count}")

    # Verify correctness
    expected = [{"id": x * 2} for x in range(100)]
    assert sorted(result, key=lambda x: x["id"]) == sorted(
        expected, key=lambda x: x["id"]
    ), f"Expected {len(expected)} items, got {len(result)}"

    # At least one TrackingActor must have been created during execution.
    assert actor_count > 0, "No actors were launched during execution."

    print(f"✓ DAG creation: {dag_creation_time:.3f}s (no actors started)")
    print(f"✓ Execution: {execution_time:.3f}s")
    print(f"✓ Total actors initialized: {actor_count}")


def test_lazy_starting_with_slow_input(ray_start_10_cpus_shared, restore_data_context):
    """Tests lazy starting with a slow input operator to ensure timing is correct."""

    import time

    import ray

    # Get (or create) the shared tracking_counter actor.
    @ray.remote(num_cpus=0)
    class _Counter:
        def __init__(self):
            self._n = 0

        def inc(self):
            self._n += 1

        def get(self):
            return self._n

    try:
        counter = ray.get_actor("tracking_counter")
    except ValueError:
        counter = _Counter.options(name="tracking_counter").remote()

    before = ray.get(counter.get.remote())

    class DelayedActor:
        def __init__(self):
            # Record actor start via the shared counter.
            ray.get_actor("tracking_counter").inc.remote()
            print(f"DelayedActor initialized at {time.time()}")

        def __call__(self, batch):
            return {"id": [x + 1000 for x in batch["id"]]}

    # Create a dataset with a slow transformation first
    def slow_transform(batch):
        time.sleep(0.1)  # Make input stage slow
        return batch

    dag_start = time.time()

    ds = (
        ray.data.range(50, override_num_blocks=5)
        .map_batches(slow_transform, batch_size=10)  # Slow first stage
        .map_batches(
            DelayedActor,
            batch_size=10,
            compute=ray.data.ActorPoolStrategy(min_size=1, max_size=2),
        )
    )

    creation_time = time.time() - dag_start

    # Verify DAG creation is fast and no actors yet
    assert creation_time < 1.0, f"DAG creation took {creation_time}s"
    assert ray.get(counter.get.remote()) == before, "Actors started during DAG creation"

    # Execute the pipeline
    execution_start = time.time()
    result = ds.take_all()
    execution_time = time.time() - execution_start

    # Verify correctness
    expected = [{"id": x + 1000} for x in range(50)]
    assert sorted(result, key=lambda x: x["id"]) == sorted(
        expected, key=lambda x: x["id"]
    )

    # Verify some actors launched during execution
    after = ray.get(counter.get.remote())
    assert after - before > 0, "Actors should have been started during execution"

    print(f"✓ DAG with slow input created in {creation_time:.3f}s")
    print(f"✓ Executed in {execution_time:.3f}s")
    print(f"✓ Actors launched during execution: {after-before}")


def test_lazy_starting_with_blocking_operations(
    ray_start_10_cpus_shared, restore_data_context
):
    """Tests lazy starting behavior with blocking operations in the pipeline."""

    import time

    actor_start_times = []

    class TimedActor:
        def __init__(self):
            actor_start_times.append(time.time())
            print(f"TimedActor initialized at {time.time()}")

        def __call__(self, batch):
            return {"id": [x + 100 for x in batch["id"]]}

    # Test with a pipeline that has a repartition before the actor stage
    dag_start = time.time()

    ds = (
        # Larger dataset ⇒ more bundles to repartition ⇒ easier to see
        # the UI pause before the actor stage starts.
        ray.data.range(10_000, override_num_blocks=100)
        .repartition(num_blocks=20)  # Blocking boundary
        .map_batches(
            TimedActor,
            batch_size=500,  # Fewer actor invocations
            compute=ray.data.ActorPoolStrategy(size=1),
        )
    )

    creation_time = time.time() - dag_start

    # Verify DAG creation is fast
    assert creation_time < 0.5, f"DAG creation took {creation_time}s"
    assert len(actor_start_times) == 0, "Actors should not start during DAG creation"

    # Execute the pipeline
    result = ds.take_all()

    # Verify correctness
    expected = [{"id": x + 100} for x in range(10_000)]
    assert sorted(result, key=lambda x: x["id"]) == sorted(
        expected, key=lambda x: x["id"]
    )
    # Verify timing: actors should start after repartition begins, not at DAG creation
    assert len(actor_start_times) > 0, "Actors should have been started"

    first_actor_time = min(actor_start_times)
    delay = first_actor_time - dag_start

    # Actor should start well after DAG creation (after repartition becomes eligible)
    assert delay > 0.01, f"Actor started too early: {delay}s after DAG creation"


def test_lazy_starting_resource_efficiency(
    ray_start_10_cpus_shared, restore_data_context
):
    """Tests that lazy starting saves resources when pipelines are created but not executed."""

    import time

    expensive_actor_count = 0

    class ExpensiveActor:
        def __init__(self):
            nonlocal expensive_actor_count
            expensive_actor_count += 1
            print(f"ExpensiveActor #{expensive_actor_count} initialized")
            # Simulate some startup cost
            time.sleep(0.01)

        def __call__(self, batch):
            return {"id": [x * 10 for x in batch["id"]]}

    # Create multiple pipelines but only execute one
    creation_start = time.time()

    # Pipeline 1: Will be executed
    active_ds = ray.data.range(10, override_num_blocks=2).map_batches(
        lambda batch: {"id": [i + 1 for i in batch["id"]]}, batch_size=5
    )

    # Pipeline 2: Will NOT be executed (should not start expensive actors)
    _ = ray.data.range(50, override_num_blocks=5).map_batches(
        ExpensiveActor, batch_size=10, compute=ray.data.ActorPoolStrategy(size=2)
    )

    creation_time = time.time() - creation_start

    # Creating pipelines should be fast regardless of complexity
    assert creation_time < 1.0, f"Pipeline creation took {creation_time}s"
    assert (
        expensive_actor_count == 0
    ), "Expensive actors should not be started during DAG creation"

    # Execute only the simple pipeline
    result = active_ds.take_all()
    expected = [{"id": x + 1} for x in range(10)]
    assert sorted(result, key=lambda x: x["id"]) == sorted(
        expected, key=lambda x: x["id"]
    )

    # Expensive actors should still not be started since unused_ds was not executed
    assert (
        expensive_actor_count == 0
    ), f"Expensive actors were started: {expensive_actor_count}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
