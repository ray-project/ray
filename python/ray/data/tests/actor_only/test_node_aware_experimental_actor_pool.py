"""Tests for the node-aware experimental actor pool
(``_NodeAwareExperimentalActorPool``, actor-only backend): spot-preemption
node draining."""
import unittest
from typing import Any, Dict, List, Optional, Tuple, cast

import pyarrow as pa
import pytest
from typing_extensions import override

import ray
from ray.actor import ActorHandle
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    AutoscalingActorConfig,
)
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    NodeIdStr,
    RefBundle,
)
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry
from ray.data._internal.execution.resource_bank import LiveObjStore, ResourceBankBase
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _NodeAwareExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    DrainReason,
    ScaleUpRequest,
)
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef


@ray.remote
class ExperimentalPoolWorker:
    """Map-worker stand-in whose ready-ref resolves to a fake node id."""

    def __init__(self, node_id: str):
        self.node_id = node_id

    def get_location(self) -> str:
        return self.node_id


class _StubResourceBank(ResourceBankBase):
    """Minimal ``ResourceBankBase`` for the pool lifecycle tests."""

    @override
    def live_object_store(self, op=None, node_id=None, actor_id=None) -> LiveObjStore:
        # No outstanding outputs: the drain gate only waits on in-flight tasks.
        return LiveObjStore()

    @override
    def on_block_consumed(self, input_ref: ObjectRef[Any]) -> None:
        pass

    @override
    def node_capacity(self, node_id: NodeIdStr) -> ExecutionResources:
        return ExecutionResources(cpu=4, object_store_memory=1024**3)

    @override
    def deregister_actor(self, actor_id: str) -> None:
        pass


class _RecordingDrainMetrics:
    """Structural ``_ActorOnlyOpMetrics`` fake recording drain events as
    ``(kind, op_tag, reason, ...)``."""

    def __init__(self):
        self.events: List[Tuple[Any, ...]] = []

    def record_block_size_overshoot(self, op_tag: str) -> None:
        pass

    def record_output_limit_overshoot(self, op_tag: str) -> None:
        pass

    def record_drain_started(self, op_tag: str, reason: str) -> None:
        self.events.append(("started", op_tag, reason))

    def record_drain_completed(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        self.events.append(("completed", op_tag, reason))

    def record_drain_interrupted(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        self.events.append(("interrupted", op_tag, reason))

    def record_drain_cancelled(self, op_tag: str, reason: str, cause: str) -> None:
        self.events.append(("cancelled", op_tag, reason, cause))

    def count(self, kind: str, *tags: str) -> int:
        return sum(
            1
            for event in self.events
            if event[0] == kind and tuple(event[2 : 2 + len(tags)]) == tags
        )


def _located_bundle(node_id: str = "node1", size_bytes: int = 100) -> RefBundle:
    """A RefBundle carrying block location metadata, which the experimental pool
    reads on task submission."""
    block = pa.table({"col": [0] * max(1, size_bytes // 8)})
    metadata = BlockMetadata(
        num_rows=block.num_rows,
        size_bytes=size_bytes,
        exec_stats=None,
        node_id=node_id,
    )
    return RefBundle(
        blocks=(BlockEntry(cast("ObjectRef[Block]", ray.put(block)), metadata),),
        owns_blocks=False,
        schema=block.schema,
    )


class TestNodeAwareExperimentalActorPoolNodeDraining(unittest.TestCase):
    """Spot preemption: actors on a draining node are gracefully drained.

    Covers ``_NodeAwareExperimentalActorPool`` (actor-only backend), where the
    pool has a draining path the base pool lacks.
    """

    @classmethod
    def setUpClass(cls):
        cls._actor_node_id = "node1"
        cls._last_created_actor_and_ready_ref: Optional[Tuple[ActorHandle, Any]] = None
        # A prior test module may leak an auto-inited Ray session; this suite
        # needs its own sized cluster.
        ray.shutdown()
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def setUp(self):
        # What the pool's injected ``get_draining_nodes`` reports; tests
        # mutate it in place between refreshes.
        self._draining_nodes: Dict[str, int] = {}

    def _create_actor_fn(
        self,
        labels: Dict[str, Any],
        logical_actor_id: str = "Actor1",
        scheduling_strategy=None,
    ) -> Tuple[ActorHandle, ObjectRef[Any], ExecutionResources]:
        # pyrefly: ignore[missing-attribute]  # ray.remote-decorated class
        actor = ExperimentalPoolWorker.options(_labels=labels).remote(
            self._actor_node_id
        )
        ready_ref = actor.get_location.remote()
        self._last_created_actor_and_ready_ref = actor, ready_ref
        return actor, ready_ref, ExecutionResources(cpu=1)

    def _create_pool(
        self, min_tasks_in_flight: int = 4
    ) -> _NodeAwareExperimentalActorPool:
        config = AutoscalingActorConfig(
            min_size=1,
            max_size=4,
            initial_size=1,
            max_tasks_in_flight_per_actor=min_tasks_in_flight,
            max_actor_concurrency=1,
            per_actor_resource_usage=ExecutionResources(cpu=1),
            max_num_output_bytes_per_actor=10_000_000,
        )
        pool = _NodeAwareExperimentalActorPool(
            create_actor_fn=self._create_actor_fn,
            config=config,
            get_draining_nodes=lambda: dict(self._draining_nodes),
        )
        pool.set_resource_bank(_StubResourceBank())
        return pool

    def _add_ready_actor(
        self, pool: _NodeAwareExperimentalActorPool, node_id: str = "node1"
    ) -> ActorHandle:
        self._actor_node_id = node_id
        assert pool.scale(ScaleUpRequest.untargeted(1, reason="test")) == 1
        assert self._last_created_actor_and_ready_ref is not None
        actor, ready_ref = self._last_created_actor_and_ready_ref
        self._last_created_actor_and_ready_ref = None
        ray.get(ready_ref)
        pool.pending_to_running(ready_ref)
        return actor

    def test_drains_actors_on_draining_node(self):
        """Actors on a draining node start draining; actors elsewhere don't.

        The actor on the draining node must be non-reclaimable: its node is
        going away, so a later upscale must never reclaim it there.
        """
        metrics = _RecordingDrainMetrics()
        pool = self._create_pool()
        pool.set_metrics(metrics, "Map")
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        # No drain: nothing drains.
        self._draining_nodes.clear()
        pool.refresh_actor_state()
        assert pool.num_draining_actors() == 0
        assert pool.num_running_actors() == 2

        # node1 starts draining (spot preemption).
        self._draining_nodes["node1"] = 1
        pool.refresh_actor_state()

        # The drain is stamped with the preemption reason.
        assert metrics.count("started", "preemption") == 1
        assert metrics.count("started") == 1

        assert pool._running_actors[actor1].status == ActorStatus.DRAINING
        assert pool._running_actors[actor2].status != ActorStatus.DRAINING
        assert pool.num_draining_actors() == 1
        # The actor on the draining node no longer counts as capacity the sizer
        # can use, so it gets replaced on a node we keep.
        assert pool.num_running_actors() == 1
        # Never reclaimed: reclaim finds no eligible victim.
        assert pool.num_reclaimable_draining_actors() == 0
        assert pool.reclaim_draining_actors(limit=1) == 0
        # Only the actor on the healthy node still serves work.
        assert pool.select_actors() is actor2

    def test_actor_on_draining_node_finishes_in_flight_task_before_teardown(self):
        """An actor on a draining node is torn down only once it has nothing in
        flight, and repeated refreshes don't re-drain it."""
        pool = self._create_pool()
        actor = self._add_ready_actor(pool, node_id="node1")
        self._draining_nodes.clear()
        pool.refresh_actor_state()

        bundle = _located_bundle("node1")
        pool.on_task_submitted(actor, bundle)

        self._draining_nodes["node1"] = 1
        pool.refresh_actor_state()
        assert pool.num_draining_actors() == 1

        # Task still in flight -> the actor keeps its slot.
        assert pool.process_draining_actors() == 0
        assert actor in pool._running_actors

        # Draining is idempotent across refreshes.
        pool.refresh_actor_state()
        assert pool.num_draining_actors() == 1

        # Once the task lands, the actor is retired.
        pool.on_task_completed(actor, bundle)
        assert pool.process_draining_actors() == 1
        assert actor not in pool._running_actors
        assert pool.num_draining_actors() == 0

    def test_actor_ready_on_draining_node_drains_immediately(self):
        """An actor that becomes ready on a draining node is never schedulable.

        Actor-ready callbacks run before dispatch, so relying on the next
        ``refresh_actor_state()`` would leave a window where the actor can take
        tasks on a node that is going away.
        """
        pool = self._create_pool()
        self._draining_nodes["node1"] = 1

        # No refresh_actor_state(): this is the pending_to_running() path alone.
        draining = self._add_ready_actor(pool, node_id="node1")
        assert pool._running_actors[draining].status == ActorStatus.DRAINING
        assert pool.num_reclaimable_draining_actors() == 0
        assert pool.select_actors() is None

        # An actor on an active node is still schedulable via the same path.
        active = self._add_ready_actor(pool, node_id="node2")
        assert pool.select_actors() is active

    def test_draining_usage_by_node_is_the_draining_part_of_committed(self):
        """Draining actors keep their committed usage until they die; the sizer
        nets this part out when it decides what is left to shed."""
        pool = self._create_pool()
        self._draining_nodes.clear()
        self._add_ready_actor(pool, node_id="node1")
        leaving = self._add_ready_actor(pool, node_id="node1")
        assert pool.draining_usage_by_node() == {}

        pool.begin_draining(leaving, reason=DrainReason.RESIZE)

        per_actor = pool._actor_resource_usage[leaving]
        assert pool.draining_usage_by_node() == {"node1": per_actor}
        assert pool.committed_usage_by_node["node1"] == per_actor.scale(2)

    def test_already_draining_actor_becomes_non_reclaimable(self):
        """An actor already draining from an ordinary downscale stays reclaimable
        -- until its node starts draining, which must retract that."""
        pool = self._create_pool()
        actor = self._add_ready_actor(pool, node_id="node1")
        self._draining_nodes.clear()
        pool.refresh_actor_state()

        # A task in flight keeps the drain open across refreshes (a refresh
        # tears down a fully drained actor).
        pool.on_task_submitted(actor, _located_bundle("node1"))
        pool.begin_draining(actor, reason=DrainReason.RESIZE)
        assert pool.num_reclaimable_draining_actors() == 1

        self._draining_nodes["node1"] = 1
        pool.refresh_actor_state()

        assert pool.num_draining_actors() == 1
        assert pool.num_reclaimable_draining_actors() == 0
        assert pool.reclaim_draining_actors(limit=1) == 0

    def test_actor_on_draining_node_stays_out_of_heaps_after_task_completion(self):
        """Draining is what keeps the actor unschedulable, and it is sticky.

        ``on_task_completed`` re-syncs an actor into both scheduling heaps
        without consulting the draining snapshot, so a location-based heap gate
        would let the actor take new tasks until the next refresh. The
        DRAINING status survives that re-sync.
        """
        pool = self._create_pool()
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")
        self._draining_nodes.clear()
        pool.refresh_actor_state()

        bundle = _located_bundle("node1")
        pool.on_task_submitted(actor1, bundle)

        self._draining_nodes["node1"] = 1
        pool.refresh_actor_state()
        assert pool._running_actors[actor1].status == ActorStatus.DRAINING

        # The completion re-syncs actor1 to the heaps; it must stay out of both.
        pool.on_task_completed(actor1, bundle)
        assert actor1 not in pool._available_actors_to_tasks_fallback_heap
        assert actor1 not in pool._alive_node_to_available_actor_heap["node1"]
        # Even a node1-local bundle, which actor1 would win on locality, goes to
        # the actor on the healthy node.
        assert (
            pool.select_actors(
                bundle=_located_bundle("node1"), actor_locality_enabled=True
            )
            is actor2
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
