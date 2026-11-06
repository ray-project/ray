"""Tests for _ExperimentalActorPool heap-based scheduling."""

import unittest
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest import mock

import pyarrow as pa
import pytest
from typing_extensions import override

import ray
import ray.exceptions
from ray.actor import ActorHandle
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    AutoscalingActorConfig,
    AutoscalingActorPool,
)
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.execution_flags import (
    BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO,
    PER_ACTOR_INPUT_BACKPRESSURE_LIMIT,
)
from ray.data._internal.execution.interfaces import ExecutionResources, NodeIdStr
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.interfaces.physical_operator import TaskPullRequest
from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry, RefBundle
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.resource_bank import LiveObjStore, ResourceBankBase
from ray.data._internal.experimental.execution.operators import (
    actor_pool as pool_mod,
    actor_pool_map_operator as op_mod,
)
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    _ACTOR_STATE_ALIVE,
    _ACTOR_STATE_RESTARTING,
    DrainReason,
    ScaleDownRequest,
    ScaleUpRequest,
)
from ray.data._internal.util import MiB
from ray.data.block import UNKNOWN_NODE_ID, Block, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import (
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


@ray.remote
class PoolWorker:
    def __init__(self, node_id: str = "node1"):
        self.node_id = node_id

    def get_location(self) -> str:
        return self.node_id

    def __ray_shutdown__(self):
        pass


class _RecordingResourceBank(ResourceBankBase):
    """Minimal ``ResourceBankBase`` for the pool scheduling tests."""

    def __init__(self, node_resources: Optional[ExecutionResources] = None):
        self._node_resources = node_resources or ExecutionResources(
            cpu=4, object_store_memory=1024**3
        )
        # Per-actor live object store the drain gate reads; tests set an
        # actor's outstanding output blocks here.
        self.live_by_actor: Dict[str, LiveObjStore] = {}

    @override
    def live_object_store(self, op=None, node_id=None, actor_id=None) -> LiveObjStore:
        if actor_id is not None:
            return self.live_by_actor.get(actor_id, LiveObjStore())
        return LiveObjStore()

    @override
    def on_block_consumed(self, input_ref: ObjectRef[Any]) -> None:
        pass

    @override
    def node_capacity(self, node_id: NodeIdStr) -> ExecutionResources:
        return self._node_resources

    @override
    def deregister_actor(self, actor_id: str) -> None:
        pass


class _RecordingActorOnlyMetrics:
    """Structural ``_ActorOnlyOpMetrics`` fake that records every drain event
    as ``(kind, op_tag, reason, ...)``."""

    def __init__(self):
        self.events: List[Tuple[Any, ...]] = []

    def record_block_size_overshoot(self, op_tag: str) -> None:
        self.events.append(("block_size_overshoot", op_tag))

    def record_output_limit_overshoot(self, op_tag: str) -> None:
        self.events.append(("output_limit_overshoot", op_tag))

    def record_drain_started(self, op_tag: str, reason: str) -> None:
        self.events.append(("started", op_tag, reason))

    def record_drain_completed(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        self.events.append(("completed", op_tag, reason, duration_s))

    def record_drain_interrupted(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        self.events.append(("interrupted", op_tag, reason, duration_s))

    def record_drain_cancelled(self, op_tag: str, reason: str, cause: str) -> None:
        self.events.append(("cancelled", op_tag, reason, cause))

    def count(self, kind: str, *tags: str) -> int:
        """Events of ``kind`` whose ``(reason[, cause])`` start with ``tags``."""
        return sum(
            1
            for event in self.events
            if event[0] == kind and tuple(event[2 : 2 + len(tags)]) == tags
        )


def _make_bundle(size_bytes: int, node_id: Optional[str] = "node1") -> RefBundle:
    """Create a RefBundle with controlled size_bytes and node_id metadata.

    ``node_id=None`` marks the producing node as unknown, the shape a source
    operator's inputs have.
    """
    block = pa.table({"col": [0] * max(1, size_bytes // 8)})
    block_ref = ray.put(block)
    metadata = BlockMetadata(
        num_rows=block.num_rows,
        size_bytes=size_bytes,
        exec_stats=None,
        node_id=node_id if node_id is not None else UNKNOWN_NODE_ID,
    )
    return RefBundle(
        blocks=(BlockEntry(cast("ObjectRef[Block]", block_ref), metadata),),
        owns_blocks=False,
        schema=block.schema,
    )


class TestExperimentalActorPool(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._last_created_actor_and_ready_ref: Optional[Tuple[ActorHandle, Any]] = None
        cls._actor_node_id = "node1"
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        # Shut the shared cluster down so later test files (e.g. the
        # multi-node integration tests) can start their own.
        ray.shutdown()

    def _create_actor_fn(
        self,
        labels: Dict[str, Any],
        logical_actor_id: str = "Actor1",
        scheduling_strategy: NodeAffinitySchedulingStrategy | str | None = None,
    ) -> Tuple[ActorHandle, Any, ExecutionResources]:
        # pyrefly: ignore[missing-attribute]  # ray.remote-decorated class
        actor = PoolWorker.options(_labels=labels).remote(self._actor_node_id)
        ready_ref = actor.get_location.remote()
        self._last_created_actor_and_ready_ref = actor, ready_ref
        return actor, ready_ref, ExecutionResources(cpu=1)

    def _create_pool(
        self,
        max_tasks_in_flight=4,
        min_size=1,
        max_size=4,
        initial_size=1,
        max_num_output_bytes_per_actor: Optional[int] = 10_000_000,
        per_actor_resource_usage=None,
        node_resources=None,
    ) -> _ExperimentalActorPool:
        config = AutoscalingActorConfig(
            min_size=min_size,
            max_size=max_size,
            initial_size=initial_size,
            max_tasks_in_flight_per_actor=max_tasks_in_flight,
            max_actor_concurrency=1,
            per_actor_resource_usage=(
                per_actor_resource_usage or ExecutionResources(cpu=1)
            ),
            max_num_output_bytes_per_actor=max_num_output_bytes_per_actor,
        )
        pool = _ExperimentalActorPool(
            create_actor_fn=self._create_actor_fn, config=config
        )
        pool.set_resource_bank(_RecordingResourceBank(node_resources))
        return pool

    def _create_pool_with_metrics(
        self, metrics: _RecordingActorOnlyMetrics, **kwargs
    ) -> _ExperimentalActorPool:
        pool = self._create_pool(**kwargs)
        pool.set_metrics(metrics, "Map")
        return pool

    @staticmethod
    def _set_outstanding_outputs(
        pool: _ExperimentalActorPool, actor: ActorHandle, num_blocks: int
    ) -> None:
        """Make the fake bank report ``num_blocks`` pulled-but-unconsumed output
        blocks for ``actor``, which is what the drain gate reads."""
        bank = cast(_RecordingResourceBank, pool.resource_bank)
        bank.live_by_actor[pool.get_actor_logical_id(actor)] = LiveObjStore(
            num_pulled_output_blocks=num_blocks
        )

    def _add_pending_actor(
        self, pool: _ExperimentalActorPool, node_id="node1"
    ) -> Tuple[ActorHandle, Any]:
        self._actor_node_id = node_id
        num_actors = pool.scale(ScaleUpRequest.untargeted(1, reason="test"))
        assert num_actors == 1
        assert self._last_created_actor_and_ready_ref is not None
        actor, ready_ref = self._last_created_actor_and_ready_ref
        self._last_created_actor_and_ready_ref = None
        return actor, ready_ref

    def _add_ready_actor(
        self, pool: _ExperimentalActorPool, node_id="node1"
    ) -> ActorHandle:
        actor, ready_ref = self._add_pending_actor(pool, node_id)
        ray.get(ready_ref)
        pool.pending_to_running(ready_ref)
        return actor

    def _output_bytes_limit(
        self,
        *,
        node_resources: ExecutionResources,
        max_num_output_bytes_per_actor: Optional[int],
        per_actor_resource_usage: Optional[ExecutionResources] = None,
    ) -> float:
        """Build a one-actor pool, bring the actor up (which derives its budget
        in ``pending_to_running``), and return its output-bytes limit."""
        pool = self._create_pool(
            max_num_output_bytes_per_actor=max_num_output_bytes_per_actor,
            per_actor_resource_usage=per_actor_resource_usage,
            node_resources=node_resources,
        )
        actor = self._add_ready_actor(pool)
        return pool.output_bytes_limit(pool.get_actor_logical_id(actor))

    # ---- dynamic per-actor output-bytes limit tests ----

    def test_output_bytes_limit_derived_from_object_store_size(self):
        """With no explicit per-actor output limit, the limit is derived from the
        node's (coordinator-allocated) object store: a fraction of it, split by
        the actor's CPU share of the node, rounded to MiB. It scales with the
        object-store size. The fraction is pinned so expectations don't depend on
        the module default."""
        # 4-CPU node, 1-CPU actor, fraction 0.5 -> (object_store * 0.5) / 4 cpu.
        cases = [
            (1 * 1024**3, 128 * MiB),  # 0.5 GiB usable / 4 = 128 MiB
            (2 * 1024**3, 256 * MiB),
            (8 * 1024**3, 1024 * MiB),
        ]
        with mock.patch.object(pool_mod, "OBJECT_STORE_OUTPUT_FRACTION", 0.5):
            for object_store_bytes, expected in cases:
                with self.subTest(object_store_bytes=object_store_bytes):
                    limit = self._output_bytes_limit(
                        node_resources=ExecutionResources(
                            cpu=4, object_store_memory=object_store_bytes
                        ),
                        max_num_output_bytes_per_actor=None,
                    )
                    self.assertEqual(limit, expected)

    def test_output_bytes_limit_scales_with_per_actor_cpu_share(self):
        """The derived limit is the actor's CPU share of the node's usable object
        store: a 2-CPU actor on a 4-CPU node gets twice a 1-CPU actor's budget."""
        node = ExecutionResources(cpu=4, object_store_memory=8 * 1024**3)
        with mock.patch.object(pool_mod, "OBJECT_STORE_OUTPUT_FRACTION", 0.5):
            one_cpu = self._output_bytes_limit(
                node_resources=node,
                max_num_output_bytes_per_actor=None,
                per_actor_resource_usage=ExecutionResources(cpu=1),
            )
            two_cpu = self._output_bytes_limit(
                node_resources=node,
                max_num_output_bytes_per_actor=None,
                per_actor_resource_usage=ExecutionResources(cpu=2),
            )
        self.assertEqual(one_cpu, 1024 * MiB)  # 4 GiB usable / 4 cpu * 1
        self.assertEqual(two_cpu, 2048 * MiB)  # * 2

    def test_output_bytes_limit_normalizes_on_cpu_only(self):
        """The derived budget normalizes on CPU alone; memory and GPU are
        ignored. This avoids handing an actor nearly the whole object store when
        a non-CPU dimension is tiny relative to the node (e.g. a small memory
        reservation, or a single-GPU actor on a single-GPU node).

        Node: cpu=8, memory=512 GiB, 1 GPU, object_store=100 GiB (usable 50 GiB
        at fraction 0.5). The budget is the actor's CPU share (1/8) of 50 GiB
        regardless of how large its memory/GPU footprint is.
        """
        node = ExecutionResources(
            cpu=8,
            gpu=1,
            memory=512 * 1024**3,
            object_store_memory=100 * 1024**3,
        )
        # 50 GiB usable / 8 CPUs, floored to MiB (matches the implementation's
        # ``int(x / MiB) * MiB``). Raise the max bound so the clamp doesn't mask
        # the CPU-share derivation this test is exercising (6.25 GiB > 2 GiB).
        expected = int((50 * 1024**3 / 8) / MiB) * MiB
        with (
            mock.patch.object(pool_mod, "OBJECT_STORE_OUTPUT_FRACTION", 0.5),
            mock.patch.object(
                pool_mod, "PER_ACTOR_OBJECT_STORE_MAX_BYTES", 100 * 1024**3
            ),
        ):
            # A tiny memory reservation would, under the old cross-dimension
            # max, have claimed nearly the whole store; here it's ignored.
            for per_actor in (
                ExecutionResources(cpu=1),
                ExecutionResources(cpu=1, memory=4 * 1024**3),
                ExecutionResources(cpu=1, memory=1, gpu=1),
            ):
                with self.subTest(per_actor=per_actor):
                    limit = self._output_bytes_limit(
                        node_resources=node,
                        max_num_output_bytes_per_actor=None,
                        per_actor_resource_usage=per_actor,
                    )
                    self.assertEqual(limit, expected)

    def test_explicit_output_bytes_limit_overrides_derivation(self):
        """An explicit ``max_num_output_bytes_per_actor`` (within the system
        bounds) is used verbatim, so the node object-store size does not affect it
        (derivation is bypassed)."""
        explicit = 100 * MiB
        for object_store_bytes in (1 * 1024**3, 100 * 1024**3):
            with self.subTest(object_store_bytes=object_store_bytes):
                limit = self._output_bytes_limit(
                    node_resources=ExecutionResources(
                        cpu=4, object_store_memory=object_store_bytes
                    ),
                    max_num_output_bytes_per_actor=explicit,
                )
                self.assertEqual(limit, explicit)

    def test_output_bytes_limit_clamped_to_system_max(self):
        """A derived budget above the system ceiling is clamped down to it."""
        # 100 GiB object store, fraction 0.5, 1-CPU/4-CPU share -> ~12.5 GiB
        # derived, well above the system ceiling.
        with mock.patch.object(pool_mod, "OBJECT_STORE_OUTPUT_FRACTION", 0.5):
            limit = self._output_bytes_limit(
                node_resources=ExecutionResources(
                    cpu=4, object_store_memory=100 * 1024**3
                ),
                max_num_output_bytes_per_actor=None,
            )
        self.assertEqual(limit, pool_mod.PER_ACTOR_OBJECT_STORE_MAX_BYTES)

    def test_output_bytes_limit_clamped_to_system_min(self):
        """A derived budget below the system floor is clamped up to it (and no
        longer errors on a tiny node)."""
        # 4 MiB object store, fraction 0.5, 1-CPU/4-CPU share -> 0.5 MiB derived
        # (floors to 0), well below the system floor.
        with mock.patch.object(pool_mod, "OBJECT_STORE_OUTPUT_FRACTION", 0.5):
            limit = self._output_bytes_limit(
                node_resources=ExecutionResources(cpu=4, object_store_memory=4 * MiB),
                max_num_output_bytes_per_actor=None,
            )
        self.assertEqual(limit, pool_mod.PER_ACTOR_OBJECT_STORE_MIN_BYTES)

    def test_output_bytes_limit_zero_cpu_share_floored_to_min(self):
        """A zero per-actor CPU share (num_cpus=0), or a CPU-less landing node,
        can't yield a CPU-proportional budget -- it falls back to 0 (no divide-
        by-zero, no error) and the MIN floor pins it to the system minimum."""
        with mock.patch.object(pool_mod, "OBJECT_STORE_OUTPUT_FRACTION", 0.5):
            # num_cpus=0 actor on a CPU-bearing node.
            zero_cpu_actor = self._output_bytes_limit(
                node_resources=ExecutionResources(
                    cpu=4, object_store_memory=8 * 1024**3
                ),
                max_num_output_bytes_per_actor=None,
                per_actor_resource_usage=ExecutionResources(cpu=0),
            )
            # 1-CPU actor landing on a CPU:0 node (e.g. a GPU-only node).
            zero_cpu_node = self._output_bytes_limit(
                node_resources=ExecutionResources(
                    cpu=0, gpu=4, object_store_memory=8 * 1024**3
                ),
                max_num_output_bytes_per_actor=None,
                per_actor_resource_usage=ExecutionResources(cpu=1),
            )
        self.assertEqual(zero_cpu_actor, pool_mod.PER_ACTOR_OBJECT_STORE_MIN_BYTES)
        self.assertEqual(zero_cpu_node, pool_mod.PER_ACTOR_OBJECT_STORE_MIN_BYTES)

    def test_explicit_output_bytes_limit_escapes_bounds(self):
        """An explicit ``max_num_output_bytes_per_actor`` is an override used
        verbatim -- it is NOT clamped into the system [min, max] range (only the
        auto-derived budget is). So a user value above the ceiling or below the
        floor is honored as-is."""
        node = ExecutionResources(cpu=4, object_store_memory=8 * 1024**3)
        for explicit in (
            5 * 1024**3,  # above the system ceiling
            100,  # below the system floor
        ):
            with self.subTest(explicit=explicit):
                limit = self._output_bytes_limit(
                    node_resources=node,
                    max_num_output_bytes_per_actor=explicit,
                )
                self.assertEqual(limit, explicit)

    def test_output_bytes_limit_bounds_are_overridable(self):
        """The clamp on the derived budget reads the (env-backed) bound constants,
        so overriding them moves the ceiling/floor the derived budget is pinned
        to."""
        with (
            mock.patch.object(pool_mod, "OBJECT_STORE_OUTPUT_FRACTION", 0.5),
            mock.patch.object(pool_mod, "PER_ACTOR_OBJECT_STORE_MAX_BYTES", 256 * MiB),
            mock.patch.object(pool_mod, "PER_ACTOR_OBJECT_STORE_MIN_BYTES", 64 * MiB),
        ):
            # Derived is 4 GiB usable / 4 cpu = 1 GiB, clamped down to the 256 MiB
            # ceiling.
            self.assertEqual(
                self._output_bytes_limit(
                    node_resources=ExecutionResources(
                        cpu=4, object_store_memory=8 * 1024**3
                    ),
                    max_num_output_bytes_per_actor=None,
                ),
                256 * MiB,
            )
            # Derived is 128 MiB usable / 4 cpu = 32 MiB, clamped up to the 64 MiB
            # floor.
            self.assertEqual(
                self._output_bytes_limit(
                    node_resources=ExecutionResources(
                        cpu=4, object_store_memory=256 * MiB
                    ),
                    max_num_output_bytes_per_actor=None,
                ),
                64 * MiB,
            )

    def _select_and_submit(
        self,
        pool: _ExperimentalActorPool,
        bundle: RefBundle,
        actor_locality_enabled: bool = False,
    ) -> Optional[ActorHandle]:
        actor = pool.select_actors(
            bundle=bundle, actor_locality_enabled=actor_locality_enabled
        )
        if actor is not None:
            pool.on_task_submitted(actor, bundle)
        return actor

    # ---- max_tasks_in_flight tests ----

    def test_cap_blocks_actor_at_max_tasks_in_flight(self):
        """An actor takes tasks while under its cap and is unavailable at it;
        completing a task re-admits it."""
        pool = self._create_pool(max_tasks_in_flight=2)
        actor = self._add_ready_actor(pool)

        b1, b2, b3 = _make_bundle(1000), _make_bundle(1000), _make_bundle(1000)
        assert self._select_and_submit(pool, b1) == actor
        assert self._select_and_submit(pool, b2) == actor
        # At the cap: blocked, whatever the bundle sizes.
        assert pool.select_actors(bundle=b3) is None

        pool.on_task_completed(actor, b1)
        assert self._select_and_submit(pool, b3) == actor

    def test_admission_spreads_across_actors(self):
        pool = self._create_pool(max_tasks_in_flight=2)
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        assigned = []
        for _ in range(4):
            a = self._select_and_submit(pool, _make_bundle(1000))
            assert a is not None
            assigned.append(a)

        assert sum(1 for a in assigned if a == actor1) == 2
        assert sum(1 for a in assigned if a == actor2) == 2

        # Both actors at the cap.
        assert pool.select_actors(bundle=_make_bundle(1000)) is None

    # ---- scheduling priority tests ----

    def test_least_loaded_actor_preferred(self):
        pool = self._create_pool(max_tasks_in_flight=4)
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        # Submit four tasks; the pool always hands the next one to the
        # least-loaded actor, so picks must alternate. That the sequence is
        # A, B, A, B is the observable proof of an even 2/2 split -- no need to
        # reach into the pool's private per-actor state to confirm the counts.
        picks = [self._select_and_submit(pool, _make_bundle(100)) for _ in range(4)]

        assert None not in picks
        assert {picks[0], picks[1]} == {actor1, actor2}, picks
        # Rounds alternate: pick 3 returns to pick 1's actor, pick 4 to pick 2's.
        assert picks[2] == picks[0], picks
        assert picks[3] == picks[1], picks
        # Total load is what the pool reports publicly: 4 tasks, 2 per actor.
        assert pool.num_tasks_in_flight() == 4

    def test_completing_task_reprioritizes_actor(self):
        pool = self._create_pool(max_tasks_in_flight=4)
        actor1 = self._add_ready_actor(pool, node_id="node1")
        _actor2 = self._add_ready_actor(pool, node_id="node2")

        bundles = [_make_bundle(100) for _ in range(4)]
        for b in bundles[:4]:
            self._select_and_submit(pool, b)
        # Both actors have 2 tasks each

        # Complete both tasks on actor1
        for b in bundles[:2]:
            pool.on_task_completed(actor1, b)
        # actor1: 0 tasks, actor2: 2 tasks

        # Next 2 picks should both go to actor1 (lower rank)
        b5 = _make_bundle(100)
        a5 = self._select_and_submit(pool, b5)
        b6 = _make_bundle(100)
        a6 = self._select_and_submit(pool, b6)
        assert a5 == actor1
        assert a6 == actor1

    def test_evicting_restarting_actor_keeps_counters_consistent(self):
        """Draining a RESTARTING victim (e.g. a sizer eviction during chaos)
        must retire it from the restarting accounting: otherwise the
        restarting count leaks forever and num_alive_actors goes negative."""
        pool = self._create_pool()
        actor = self._add_ready_actor(pool)

        # GCS reports the actor RESTARTING (its node died).
        with mock.patch.object(
            ActorHandle, "_get_local_state", return_value=_ACTOR_STATE_RESTARTING
        ):
            pool.refresh_actor_state()
        assert pool.num_restarting_actors() == 1
        assert pool.pending_logical_usage() == ExecutionResources(cpu=1)

        # The sizer picks it as an eviction victim.
        pool.begin_draining(actor, reason=DrainReason.ALLOC_SHRINK)

        assert pool.num_restarting_actors() == 0
        assert pool.num_alive_actors() == 0
        assert pool.pending_logical_usage() == ExecutionResources.zero()

        # Full release retires it without double-decrementing.
        pool._release_running_actor(actor)
        assert pool.num_restarting_actors() == 0
        assert pool.num_alive_actors() == 0
        assert pool.num_draining_actors() == 0

    def test_relocated_actor_moves_node_keyed_bookkeeping(self):
        """When an actor lands on a new node, everything keyed by its location
        moves with it: the committed-usage ledger, the per-node actor index, and
        the per-node count of actors with in-flight tasks."""
        pool = self._create_pool()
        actor = self._add_ready_actor(pool, node_id="node1")
        pool.on_task_submitted(actor, _make_bundle(100))
        state = pool._running_actors[actor]

        assert set(pool.committed_usage_by_node) == {"node1"}
        assert set(pool._node_to_actor_states) == {"node1"}
        assert pool.active_nodes() == {"node1"}

        pool._relocate_actor(actor, state, "node2")

        assert set(pool.committed_usage_by_node) == {"node2"}
        assert pool.committed_usage_by_node["node2"] == ExecutionResources(cpu=1)
        assert set(pool._node_to_actor_states) == {"node2"}
        assert pool.active_nodes() == {"node2"}
        assert state.actor_location == "node2"
        assert pool._node_to_active_actor_count == {"node2": 1}

    def test_relocated_actor_leaves_no_residue_when_released(self):
        """The bug this guards: release debits whichever node the actor lives on
        now, so if the ledger wasn't moved the old node keeps a charge nothing
        can ever clear -- a permanent overdraft the sizer evicts against forever.
        """
        pool = self._create_pool()
        actor = self._add_ready_actor(pool, node_id="node1")
        state = pool._running_actors[actor]

        pool._relocate_actor(actor, state, "node2")
        pool._release_running_actor(actor)

        # Nothing charged to either node, and no empty-node residue.
        assert pool.committed_usage_by_node == {}
        assert pool._node_to_actor_states == {}
        assert pool.active_nodes() == set()

    def test_relocate_actor_is_noop_for_unchanged_location(self):
        """Called on every refresh, so the common case must not churn counters."""
        pool = self._create_pool()
        actor = self._add_ready_actor(pool, node_id="node1")
        pool.on_task_submitted(actor, _make_bundle(100))
        state = pool._running_actors[actor]
        before = dict(pool.committed_usage_by_node)

        for _ in range(3):
            pool._relocate_actor(actor, state, "node1")

        assert pool.committed_usage_by_node == before
        assert pool.active_nodes() == {"node1"}
        assert pool._node_to_active_actor_count == {"node1": 1}

    @staticmethod
    def _drain_metrics() -> _RecordingActorOnlyMetrics:
        return _RecordingActorOnlyMetrics()

    def test_downscale_on_node_picks_cheapest_drain(self):
        """A node-shaped downscale picks victims by drain cost: pendings
        first (cancelled outright), then fewest in-flight tasks (each must run
        to completion and adds more outstanding outputs when it finishes),
        then fewest output blocks the ResourceBank still counts against the
        actor (queued blocks drain fast). An actor with fewer outputs but MORE
        tasks in flight is the more expensive drain."""
        pool = self._create_pool(max_tasks_in_flight=4, max_size=5)
        idle = self._add_ready_actor(pool, node_id="node1")
        one_task_many_outputs = self._add_ready_actor(pool, node_id="node1")
        two_tasks_no_outputs = self._add_ready_actor(pool, node_id="node1")
        # A pending actor targeted at node1 (the request path validates hex
        # node ids, so register it directly).
        pending, ready_ref, usage = pool._create_actor()
        pool._add_pending_actor(pending, ready_ref, usage, target_node_id="node1")

        pool.on_task_submitted(one_task_many_outputs, _make_bundle(100))
        self._set_outstanding_outputs(pool, one_task_many_outputs, 5)
        for _ in range(2):
            pool.on_task_submitted(two_tasks_no_outputs, _make_bundle(100))

        def _downscale_one() -> Optional[int]:
            # force: bypass the post-upscale debounce (the capacity-transfer
            # donor path sets force=True for the same reason).
            return pool.scale(
                ScaleDownRequest.on_nodes(("node1",), force=True, reason="test")
            )

        def _status(actor) -> ActorStatus:
            return pool._running_actors[actor].status

        # 1st victim: the pending actor (cancelled, nothing to drain).
        assert _downscale_one() == -1
        assert pool.num_pending_actors() == 0
        assert pool.num_draining_actors() == 0

        # 2nd victim: the idle actor (0 tasks sorts first).
        assert _downscale_one() == -1
        assert _status(idle) == ActorStatus.DRAINING

        # 3rd victim: one task in flight beats two, despite its queued outputs.
        assert _downscale_one() == -1
        assert _status(one_task_many_outputs) == ActorStatus.DRAINING
        assert _status(two_tasks_no_outputs) != ActorStatus.DRAINING

    def test_scale_down_cancels_untargeted_pendings_by_id(self):
        """An untargeted pending (placement-group or initial-size actor) has no
        node, so no node-shaped shed reaches it; ``pending_ids`` cancels it
        directly, and the running actor is left alone."""
        pool = self._create_pool(max_size=4)
        running = self._add_ready_actor(pool, node_id="node1")
        pending, ready_ref, usage = pool._create_actor()
        pool._add_pending_actor(pending, ready_ref, usage, target_node_id=None)
        (pending_id,) = pool.untargeted_pending_ids()

        applied = pool.scale(
            ScaleDownRequest.on_nodes((), pending_ids=(pending_id,), force=True)
        )

        assert applied == -1
        assert pool.num_pending_actors() == 0
        assert pool.untargeted_pending_ids() == []
        assert pool._running_actors[running].status != ActorStatus.DRAINING

    def test_scale_down_reports_node_shortfall(self):
        """Asking a node for more actors than it holds sheds what is there;
        the applied delta reports the shortfall (the next tick re-requests)."""
        pool = self._create_pool(max_size=4)
        self._add_ready_actor(pool, node_id="node1")
        self._add_ready_actor(pool, node_id="node1")
        self._add_ready_actor(pool, node_id="node2")

        applied = pool.scale(
            ScaleDownRequest.on_nodes(("node1",) * 3, force=True, reason="test")
        )
        assert applied == -2
        assert pool.num_draining_actors() == 2
        assert pool.num_running_actors() == 1

    def test_reclaim_takes_busiest_draining_actor_first(self):
        """A reclaim takes the busiest draining actor first: it rescues the
        most in-flight work from a slow drain. ``reclaimable_node_ids`` reports
        the same order so the sizer's placement view matches apply."""
        pool = self._create_pool(max_tasks_in_flight=4, max_size=4)
        idle = self._add_ready_actor(pool, node_id="node1")
        busy = self._add_ready_actor(pool, node_id="node2")
        pool.on_task_submitted(busy, _make_bundle(100))
        pool.begin_draining(idle, reason=DrainReason.RESIZE)
        pool.begin_draining(busy, reason=DrainReason.RESIZE)

        assert pool.reclaimable_node_ids(limit=2) == ["node2", "node1"]
        assert pool.reclaim_draining_actors(limit=1) == 1
        assert pool._running_actors[busy].status == ActorStatus.ACTIVE
        assert pool._running_actors[idle].status == ActorStatus.DRAINING

    def test_drain_metrics_graceful_completion(self):
        metrics = self._drain_metrics()
        pool = self._create_pool_with_metrics(metrics)
        actor = self._add_ready_actor(pool)

        pool.begin_draining(actor, reason=DrainReason.RESIZE)
        assert metrics.count("started") == 1
        assert metrics.events[0][1] == "Map"  # tagged with the operator
        assert pool.num_draining_actors() == 1

        # Nothing in flight, nothing outstanding -> torn down on the next pass.
        assert pool.process_draining_actors() == 1
        assert metrics.count("completed") == 1
        assert pool.num_draining_actors() == 0
        assert metrics.count("interrupted") == 0

    def test_drain_metrics_death_mid_drain_counts_interrupted(self):
        metrics = self._drain_metrics()
        pool = self._create_pool_with_metrics(metrics)
        actor = self._add_ready_actor(pool)
        self._set_outstanding_outputs(pool, actor, 1)  # one outstanding block

        pool.begin_draining(actor, reason=DrainReason.ALLOC_SHRINK)
        # DEAD (not RESTARTING): nothing is coming back to serve the work, so
        # the drain is interrupted.
        with mock.patch.object(ActorHandle, "_get_local_state", return_value=None):
            pool.refresh_actor_state()

        assert metrics.count("interrupted") == 1
        assert pool.num_draining_actors() == 0
        assert metrics.count("completed") == 0

    def test_drain_metrics_death_after_fully_drained_counts_completed(self):
        metrics = self._drain_metrics()
        pool = self._create_pool_with_metrics(metrics)
        actor = self._add_ready_actor(pool)

        pool.begin_draining(actor, reason=DrainReason.ALLOC_SHRINK)
        # Fully drained when it died: this is the teardown the next pass would
        # have done anyway, so it counts as a completed drain, not a loss.
        with mock.patch.object(ActorHandle, "_get_local_state", return_value=None):
            pool.refresh_actor_state()

        assert metrics.count("completed") == 1
        assert metrics.count("interrupted") == 0
        assert pool.num_draining_actors() == 0

    def test_drain_metrics_cancelled_on_reclaim_and_restart(self):
        metrics = self._drain_metrics()
        pool = self._create_pool_with_metrics(metrics, max_tasks_in_flight=4)

        # Sizer reclaims a draining actor: drain cancelled.
        reclaimed = self._add_ready_actor(pool)
        pool.begin_draining(reclaimed, reason=DrainReason.RESIZE)
        assert pool.reclaim_draining_actors(limit=1) == 1
        assert metrics.count("cancelled") == 1
        assert metrics.count("cancelled", "resize", "reclaimed") == 1
        assert pool.num_draining_actors() == 0

        # A core restart of a draining actor that still owes work also
        # cancels the drain (the actor comes back to serve it).
        restarted = self._add_ready_actor(pool)
        pool.on_task_submitted(restarted, _make_bundle(100))
        pool.begin_draining(restarted, reason=DrainReason.ALLOC_SHRINK)
        with mock.patch.object(
            ActorHandle, "_get_local_state", return_value=_ACTOR_STATE_RESTARTING
        ):
            pool.refresh_actor_state()
        assert metrics.count("cancelled") == 2
        assert metrics.count("cancelled", "alloc_shrink", "core_restart") == 1
        assert metrics.count("started") == 2
        assert pool.num_draining_actors() == 0
        assert metrics.count("interrupted") == 0

    def test_drain_metrics_reason_per_entry_point(self):
        """Each drain entry point stamps its reason on the started counter,
        and the outcome inherits it."""
        cases = [
            (
                lambda pool, actor: pool.scale(
                    ScaleDownRequest.on_nodes(("node1",), force=True, reason="test")
                ),
                "resize",
            ),
            (
                lambda pool, actor: pool._downscale_actor_by_id(
                    pool.get_actor_logical_id(actor), DrainReason.RESIZE
                ),
                "resize",
            ),
            (
                lambda pool, actor: pool._downscale_actor_by_id(
                    pool.get_actor_logical_id(actor), DrainReason.ALLOC_SHRINK
                ),
                "alloc_shrink",
            ),
            (
                lambda pool, actor: pool.begin_draining(
                    actor, reason=DrainReason.PREEMPTION
                ),
                "preemption",
            ),
        ]
        for start_drain, expected_reason in cases:
            with self.subTest(expected_reason=expected_reason):
                metrics = self._drain_metrics()
                pool = self._create_pool_with_metrics(metrics)
                actor = self._add_ready_actor(pool)

                start_drain(pool, actor)
                assert metrics.count("started", expected_reason) == 1
                assert metrics.count("started") == 1

                # Idle actor -> torn down next pass; the outcome carries the
                # same reason.
                assert pool.process_draining_actors() == 1
                assert metrics.count("completed", expected_reason) == 1

    def test_status_counts_track_running_actors(self):
        """The ``num_*_actors`` views are served from the status histogram
        ``_status_counts``; after every lifecycle transition they must equal a
        brute-force scan of ``_running_actors``. IDLE means schedulable with
        nothing in flight, so DRAINING and RESTARTING actors are never idle."""

        def oracle(pool: _ExperimentalActorPool) -> int:
            return sum(
                1
                for state in pool._running_actors.values()
                if state.status not in (ActorStatus.DRAINING, ActorStatus.RESTARTING)
                and state.num_tasks_in_flight == 0
            )

        def check(pool: _ExperimentalActorPool, expected: int, label: str) -> None:
            assert oracle(pool) == expected, f"{label}: oracle {oracle(pool)}"
            assert (
                pool.num_idle_actors() == expected
            ), f"{label}: num_idle_actors {pool.num_idle_actors()}"
            states = list(pool._running_actors.values())
            assert sum(pool._status_counts.values()) == len(states), label
            assert pool.num_draining_actors() == len(pool._draining_actors), label
            assert pool.num_restarting_actors() == sum(
                1 for st in states if st.status is ActorStatus.RESTARTING
            ), label
            assert pool.num_active_actors() == sum(
                1 for st in states if st.num_tasks_in_flight > 0
            ), label

        pool = self._create_pool(max_tasks_in_flight=4)
        check(pool, 0, "empty pool")

        # Actor add: joins the pool idle.
        actor1 = self._add_ready_actor(pool, node_id="node1")
        check(pool, 1, "one ready actor")
        actor2 = self._add_ready_actor(pool, node_id="node2")
        check(pool, 2, "two ready actors")

        # Task submit: 0 -> 1 in flight leaves idle; 1 -> 2 must not
        # double-count.
        b1 = _make_bundle(100, node_id="node1")
        pool.on_task_submitted(actor1, b1)
        check(pool, 1, "actor1 first task")
        b2 = _make_bundle(100, node_id="node1")
        pool.on_task_submitted(actor1, b2)
        check(pool, 1, "actor1 second task")

        # Task completion: only the 1 -> 0 transition returns it to idle.
        pool.on_task_completed(actor1, b2)
        check(pool, 1, "actor1 one task left")
        pool.on_task_completed(actor1, b1)
        check(pool, 2, "actor1 drained")

        # RESTARTING actors are not schedulable, so they leave the idle count
        # and come back once ALIVE again.
        with mock.patch.object(
            ActorHandle, "_get_local_state", return_value=_ACTOR_STATE_RESTARTING
        ):
            pool.refresh_actor_state()
        check(pool, 0, "both restarting")
        with mock.patch.object(
            ActorHandle, "_get_local_state", return_value=_ACTOR_STATE_ALIVE
        ):
            pool.refresh_actor_state()
        check(pool, 2, "both recovered")

        # Draining an idle actor, then reclaiming it.
        pool.begin_draining(actor1, reason=DrainReason.RESIZE)
        check(pool, 1, "actor1 draining")
        assert pool.reclaim_draining_actors(limit=1) == 1
        check(pool, 2, "actor1 reclaimed")

        # Draining an ACTIVE actor does not change the idle count, and a
        # task completing on a draining actor must not return it to idle.
        b3 = _make_bundle(100, node_id="node2")
        pool.on_task_submitted(actor2, b3)
        check(pool, 1, "actor2 active")
        pool.begin_draining(actor2, reason=DrainReason.RESIZE)
        check(pool, 1, "actor2 draining while active")
        pool.on_task_completed(actor2, b3)
        check(pool, 1, "actor2 drained but draining")

        # Reclaiming the now-idle draining actor restores it.
        assert pool.reclaim_draining_actors(limit=1) == 1
        check(pool, 2, "actor2 reclaimed")

        # Release of an idle actor decrements; release of an active actor
        # (forced shutdown with tasks in flight) does not.
        b4 = _make_bundle(100, node_id="node1")
        pool.on_task_submitted(actor1, b4)
        check(pool, 1, "actor1 active again")
        pool._release_running_actor(actor1)
        check(pool, 1, "active actor1 released")
        pool._release_running_actor(actor2)
        check(pool, 0, "idle actor2 released")

    def test_active_nodes_tracks_actors_with_tasks(self):
        """``active_nodes`` is served from ``_node_to_active_actor_count`` and
        must match a scan of nodes hosting actors with in-flight tasks after
        every lifecycle transition."""

        def oracle(pool: _ExperimentalActorPool) -> set:
            return {
                state.actor_location
                for state in pool._running_actors.values()
                if state.num_tasks_in_flight > 0
            }

        def check(pool: _ExperimentalActorPool, expected: set, label: str) -> None:
            assert oracle(pool) == expected, f"{label}: oracle {oracle(pool)}"
            assert (
                pool.active_nodes() == expected
            ), f"{label}: active_nodes {pool.active_nodes()}"
            assert (
                set(pool._node_to_active_actor_count) == expected
            ), f"{label}: count keys {set(pool._node_to_active_actor_count)}"
            assert sum(pool._node_to_active_actor_count.values()) == sum(
                1
                for state in pool._running_actors.values()
                if state.num_tasks_in_flight > 0
            ), f"{label}: count total {pool._node_to_active_actor_count}"

        pool = self._create_pool(max_tasks_in_flight=4)
        check(pool, set(), "empty pool")

        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")
        check(pool, set(), "idle actors do not activate nodes")
        assert pool.nodes() == {"node1", "node2"}

        # 0 -> 1 in flight activates the node; 1 -> 2 must not double-count.
        b1 = _make_bundle(100, node_id="node1")
        pool.on_task_submitted(actor1, b1)
        check(pool, {"node1"}, "actor1 first task")
        b2 = _make_bundle(100, node_id="node1")
        pool.on_task_submitted(actor1, b2)
        check(pool, {"node1"}, "actor1 second task")
        assert pool._node_to_active_actor_count["node1"] == 1

        # A second actor on another node adds that node.
        b3 = _make_bundle(100, node_id="node2")
        pool.on_task_submitted(actor2, b3)
        check(pool, {"node1", "node2"}, "both nodes active")

        # Completing the last task on a node drops it from active_nodes.
        pool.on_task_completed(actor1, b2)
        check(pool, {"node1", "node2"}, "actor1 still has a task")
        pool.on_task_completed(actor1, b1)
        check(pool, {"node2"}, "node1 idle again")
        pool.on_task_completed(actor2, b3)
        check(pool, set(), "all drained")

        # Two active actors on the same node: releasing one must not drop
        # the node until the other finishes.
        actor3 = self._add_ready_actor(pool, node_id="node1")
        b4 = _make_bundle(100, node_id="node1")
        b5 = _make_bundle(100, node_id="node1")
        pool.on_task_submitted(actor1, b4)
        pool.on_task_submitted(actor3, b5)
        check(pool, {"node1"}, "two active actors on node1")
        assert pool._node_to_active_actor_count["node1"] == 2
        pool._release_running_actor(actor1)
        check(pool, {"node1"}, "one active actor remains on node1")
        assert pool._node_to_active_actor_count["node1"] == 1
        pool._release_running_actor(actor3)
        check(pool, set(), "last active actor released")

        # Draining with in-flight work still counts as active until the
        # task completes.
        b6 = _make_bundle(100, node_id="node2")
        pool.on_task_submitted(actor2, b6)
        pool.begin_draining(actor2, reason=DrainReason.RESIZE)
        check(pool, {"node2"}, "draining actor still active")
        pool.on_task_completed(actor2, b6)
        check(pool, set(), "draining actor drained")

    # ---- locality tests ----

    def test_locality_preferred_over_fallback(self):
        # Locality is derived from the bundle's own block metadata
        # (get_preferred_object_locations_from_metadata reads bm.node_id),
        # so building node1-local bundles is the seam -- no mocking needed.
        pool = self._create_pool(max_tasks_in_flight=4)
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        # Submit a task to actor2 first so it has lower rank
        b0 = _make_bundle(100, node_id="node1")
        pool.on_task_submitted(actor2, b0)  # manually push actor2 to rank 1

        b1 = _make_bundle(100, node_id="node1")
        a1 = pool.select_actors(bundle=b1, actor_locality_enabled=True)
        # Should pick actor1 (on preferred node) even though actor2
        # would be fallback choice at equal load
        assert a1 == actor1

    def test_locality_falls_back_when_local_at_capacity(self):
        pool = self._create_pool(max_tasks_in_flight=1)
        actor1 = self._add_ready_actor(pool, node_id="node1")
        actor2 = self._add_ready_actor(pool, node_id="node2")

        b1 = _make_bundle(100, node_id="node1")
        a1 = self._select_and_submit(pool, b1, actor_locality_enabled=True)
        assert a1 == actor1

        # actor1 at the floor and over the bytes cap: locality can't find it.
        b2 = _make_bundle(100, node_id="node1")
        a2 = self._select_and_submit(pool, b2, actor_locality_enabled=True)
        assert a2 == actor2

    def test_no_locality_info_uses_fallback(self):
        pool = self._create_pool(max_tasks_in_flight=4)
        actor1 = self._add_ready_actor(pool, node_id="node1")

        # A bundle with an unknown producing node reports no preferred
        # locations, so
        # selection must fall back to the global heap (the only actor).
        b1 = _make_bundle(100, node_id=None)
        a1 = pool.select_actors(bundle=b1, actor_locality_enabled=True)
        assert a1 == actor1

    def test_scale_up_reports_actual_reclaims_and_creations(self):
        """``scale()`` must report what actually happened, not the request:
        with a targeted upscale, a reclaim shortfall is not topped up with
        extra creations (the shortfall actors were planned onto specific
        nodes), so the applied delta can be smaller than requested. A caller
        keeping a ledger must see the real number."""
        pool = self._create_pool(max_size=10)
        draining = self._add_ready_actor(pool, node_id="node1")
        self._add_ready_actor(pool, node_id="node1")
        pool.begin_draining(draining, reason=DrainReason.RESIZE)

        # Ask for 3 total: reclaim 2 (only 1 is reclaimable) + create 1.
        # NodeAffinitySchedulingStrategy validates hex node ids.
        target_node = ray.NodeID.from_random().hex()
        applied = pool.scale(
            ScaleUpRequest.on_nodes((target_node,), num_to_reclaim=2, reason="test")
        )
        assert applied == 2
        assert pool.num_draining_actors() == 0
        assert pool.num_pending_actors() == 1

    def test_refresh_disabled_still_reaps_dead_draining_actor(self):
        """With RAY_DATA_REFRESH_ACTOR_STATE off, the pool skips the full
        (expensive) per-actor refresh -- but draining actors must still be
        observed, or a dead one holds its DRAINING slot and its node's
        capacity accounting for the rest of the run."""
        pool = self._create_pool()
        actor = self._add_ready_actor(pool)
        pool.begin_draining(actor, reason=DrainReason.ALLOC_SHRINK)

        with mock.patch.object(
            pool_mod, "REFRESH_ACTOR_STATE", False
        ), mock.patch.object(ActorHandle, "_get_local_state", return_value=None):
            pool.refresh_actor_state()
        assert actor not in pool._running_actors
        assert pool.num_draining_actors() == 0

    def test_estimate_bytes_per_output_is_node_scoped(self):
        """The estimate only looks at the actors on the requested node (budgets
        are derived per node), and an actor with an unbounded budget (explicit
        override or a raised cap) carries no block-size information, so it is
        skipped rather than poisoning the estimate (or crashing on ``inf``)."""
        limit = 10_000_000
        pool = self._create_pool(max_num_output_bytes_per_actor=limit)
        a1 = self._add_ready_actor(pool, node_id="node1")
        a2 = self._add_ready_actor(pool, node_id="node1")
        b1 = self._add_ready_actor(pool, node_id="node2")
        pool._running_actors[b1].max_num_output_bytes = 2 * limit

        expected = limit * BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO
        assert pool.estimate_bytes_per_output(node_id="node1") == pytest.approx(
            expected
        )
        assert pool.estimate_bytes_per_output(node_id="node2") == pytest.approx(
            2 * expected
        )
        assert pool.estimate_bytes_per_output(node_id="node3") is None

        pool._running_actors[a1].max_num_output_bytes = float("inf")
        assert pool.estimate_bytes_per_output(node_id="node1") == pytest.approx(
            expected
        )

        pool._running_actors[a2].max_num_output_bytes = float("inf")
        assert pool.estimate_bytes_per_output(node_id="node1") is None
        assert pool.estimate_bytes_per_output(node_id="node2") == pytest.approx(
            2 * expected
        )


def _make_actor_op(
    ray_remote_args: Dict[str, Any],
    ray_remote_args_fn=None,
    compute_strategy: Optional[ActorPoolStrategy] = None,
    generator_backpressure_disabled: bool = False,
    is_read_op: bool = False,
) -> ExperimentalActorPoolMapOperator:
    """Build a real ExperimentalActorPoolMapOperator the way the planner does."""
    ctx = DataContext.get_current()
    input_op = InputDataBuffer(ctx, input_data=[])
    transformer = MapTransformer(
        [BlockMapTransformFn(lambda blocks, _ctx: iter(blocks))]
    )
    return ExperimentalActorPoolMapOperator(
        map_transformer=transformer,
        input_op=input_op,
        data_context=ctx,
        compute_strategy=compute_strategy or ActorPoolStrategy(),
        ray_remote_args=ray_remote_args,
        ray_remote_args_fn=ray_remote_args_fn,
        name="Infer",
        generator_backpressure_disabled=generator_backpressure_disabled,
        is_read_op=is_read_op,
    )


@pytest.mark.parametrize("uses_fn", [False, True])
def test_only_a_ray_remote_args_fn_mints_a_pg_per_actor(uses_fn):
    """An op mints a PG per actor only when it has a ``ray_remote_args_fn``;
    otherwise the sizer must place its actors itself."""
    op = _make_actor_op({}, ray_remote_args_fn=(lambda: {}) if uses_fn else None)
    assert op.placement_constraint().mints_pg_per_actor is uses_fn


def test_system_output_bytes_limit_env_setting():
    """The system default (RAY_DATA_PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT,
    surfaced as PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT) is used as the actor
    output-bytes limit when the ActorPoolStrategy doesn't set one; an explicit
    strategy value wins over it.

    There is no public getter for the computed pool config, so the assertions
    read ``_config`` directly."""
    env_limit = 123 * MiB
    with mock.patch.object(
        op_mod, "PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT", env_limit
    ):
        # Strategy leaves it unset -> the system env value is used.
        op = _make_actor_op({"num_cpus": 1})
        assert op.actor_pool._config.max_num_output_bytes_per_actor == env_limit

        # Explicit strategy value takes precedence over the system env value.
        strategy_limit = 7 * MiB
        op_override = _make_actor_op(
            {"num_cpus": 1},
            compute_strategy=ActorPoolStrategy(
                max_num_output_bytes_per_actor=strategy_limit
            ),
        )
        assert (
            op_override.actor_pool._config.max_num_output_bytes_per_actor
            == strategy_limit
        )


def test_generator_backpressure_disabled_suppresses_actor_wide_default():
    """The opt-out kwarg must suppress the experimental operator's actor-wide
    generator cap: no backpressure keys at all (absent means disabled)."""
    op = _make_actor_op({"num_cpus": 1})
    assert "_actor_generator_backpressure_num_objects" in op._ray_remote_args

    op = _make_actor_op({"num_cpus": 1}, generator_backpressure_disabled=True)
    assert "_actor_generator_backpressure_num_objects" not in op._ray_remote_args
    assert "_generator_backpressure_num_objects" not in op._ray_actor_task_remote_args


def test_max_tasks_in_flight_defaults_and_overrides():
    """With nothing configured the per-actor task cap scales with actor
    concurrency; an ActorPoolStrategy value wins over the DataContext value,
    which wins over the derived default."""
    op = _make_actor_op({"num_cpus": 1})
    assert (
        op.actor_pool._config.max_tasks_in_flight_per_actor
        == PER_ACTOR_INPUT_BACKPRESSURE_LIMIT
    )
    op = _make_actor_op({"num_cpus": 1, "max_concurrency": 4})
    assert (
        op.actor_pool._config.max_tasks_in_flight_per_actor
        == 4 * PER_ACTOR_INPUT_BACKPRESSURE_LIMIT
    )

    ctx = DataContext.get_current()
    original = ctx.max_tasks_in_flight_per_actor
    ctx.max_tasks_in_flight_per_actor = 7
    try:
        op = _make_actor_op({"num_cpus": 1})
        assert op.actor_pool._config.max_tasks_in_flight_per_actor == 7
        op = _make_actor_op(
            {"num_cpus": 1},
            compute_strategy=ActorPoolStrategy(max_tasks_in_flight_per_actor=3),
        )
        assert op.actor_pool._config.max_tasks_in_flight_per_actor == 3
    finally:
        ctx.max_tasks_in_flight_per_actor = original


def test_scale_to_initial_size_is_a_no_op():
    """Pools must NOT scale themselves to their initial size at start(): those
    actors would be created untargeted (Ray core places them) before the
    sizer's first tick, bypassing constraint-aware placement -- a busy
    unconstrained actor landing on a labeled/pinned node is never evicted and
    can wedge a constrained pool below its target for the whole run. The
    sizer's initial_sizing_request owns initial sizing instead."""
    op = _make_actor_op({"num_cpus": 1})
    calls = []

    class _RecordingPool:
        def initial_size(self):
            return 3

        def scale(self, request):
            calls.append(request)

    op._actor_pool = cast(AutoscalingActorPool, _RecordingPool())

    op._scale_to_initial_size()
    assert calls == []


def _patch_actor_wait(monkeypatch, op, refs, ready_schedule):
    """Drive ``wait_for_min_actors`` deterministically.

    ``ready_schedule`` is an iterable of per-``ray.wait``-call lists of refs to
    resolve (``[]`` means "nothing became ready this poll"); it is padded with
    ``[]`` forever. A fake clock advances by the timeout each ``ray.wait`` asks
    for, so no test actually sleeps.
    """
    import ray as ray_mod

    class _PendingPool:
        def get_pending_actor_refs(self):
            return list(refs)

    op._actor_pool = _PendingPool()

    clock = {"t": 0.0}
    calls = []
    schedule = list(ready_schedule)

    def _wait(pending, num_returns=1, timeout=None):
        assert timeout is not None
        calls.append((list(pending), timeout))
        clock["t"] += timeout
        ready = schedule.pop(0) if schedule else []
        return list(ready), [r for r in pending if r not in ready]

    monkeypatch.setattr(ray_mod, "wait", _wait)
    monkeypatch.setattr(ray_mod, "get", lambda refs, **kw: None)
    monkeypatch.setattr(op_mod.time, "perf_counter", lambda: clock["t"])
    return calls, clock


@pytest.mark.parametrize(
    "num_actors,expected",
    [(1, 315), (5, 375), (20, 600), (100, 1800), (1000, 1800)],
)
def test_wait_for_min_actors_budget_scales_with_pool_size(num_actors, expected):
    """The default (unset ``wait_for_min_actors_s``) budget grows with the
    number of actors waited on and saturates at the max, instead of using one
    fixed 5-minute deadline for every pool size."""
    assert op_mod.wait_for_min_actors_budget_s(num_actors) == expected


def test_wait_for_min_actors_waits_on_pending_refs(monkeypatch):
    """Under the sizer, initial actors are created by the sizer's initial
    sizing pass (not op.start()), which then runs this wait. Semantics:
    explicitly-set wait_for_min_actors_s (> 0) wins as a hard deadline; unset
    uses the size-derived budget (the wait is always on for this backend)."""
    op = _make_actor_op({"num_cpus": 1})

    # Explicitly configured: returns as soon as both refs resolve.
    calls, _ = _patch_actor_wait(monkeypatch, op, ["ref1", "ref2"], [["ref1", "ref2"]])
    monkeypatch.setattr(op.data_context, "wait_for_min_actors_s", 5)
    op.wait_for_min_actors()
    assert len(calls) == 1

    # Unset: still waits (size-derived budget), and keeps polling across a
    # poll where nothing became ready.
    calls, _ = _patch_actor_wait(
        monkeypatch, op, ["ref1", "ref2"], [[], ["ref1"], ["ref2"]]
    )
    monkeypatch.setattr(op.data_context, "wait_for_min_actors_s", -1)
    op.wait_for_min_actors()
    assert len(calls) == 3

    # No pending refs: nothing to wait on regardless of configuration.
    calls, _ = _patch_actor_wait(monkeypatch, op, [], [])
    op.wait_for_min_actors()
    assert calls == []


def test_wait_for_min_actors_timeout_raises_with_guidance(monkeypatch):
    """Expiry fails the dataset with guidance: tune the DAG's per-operator
    min concurrency or raise RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S."""
    op = _make_actor_op({"num_cpus": 1})
    _patch_actor_wait(monkeypatch, op, ["ref1"], [])
    monkeypatch.setattr(op.data_context, "wait_for_min_actors_s", 5)
    with pytest.raises(
        ray.exceptions.GetTimeoutError,
        match="RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S",
    ):
        op.wait_for_min_actors()


def test_wait_for_min_actors_aborts_on_stalled_startup(monkeypatch):
    """On the default path the wait gives up once startup stalls, well before
    the (size-derived) overall budget expires -- a pool that can't be placed
    fails fast no matter how large its budget is."""
    op = _make_actor_op({"num_cpus": 1})
    monkeypatch.setattr(op.data_context, "wait_for_min_actors_s", -1)
    monkeypatch.setattr(op_mod, "WAIT_FOR_MIN_ACTORS_STALL_S", 10)
    # 100 actors -> a 1800s budget, but only one ever starts.
    refs = [f"ref{i}" for i in range(100)]
    calls, clock = _patch_actor_wait(monkeypatch, op, refs, [["ref0"]])

    with pytest.raises(ray.exceptions.GetTimeoutError, match="1/100"):
        op.wait_for_min_actors()

    # Failed ~10s after the last actor came up, not after the 1800s budget.
    assert clock["t"] < 30


def test_create_task_context_clamps_block_size_to_configured_target(
    restore_data_context,
):
    """``_create_task_context`` sizes blocks as
    ``min(output_limit * ratio, configured target)``: the actor's output
    budget may shrink the target block size, but must not raise it above the
    configured DataContext / op override.
    """
    configured = 10 * MiB
    restore_data_context.target_max_block_size = configured
    op = _make_actor_op({"num_cpus": 1})
    actor = mock.Mock(name="actor")

    # Large output budget: ratio-derived size would exceed the configured
    # target; keep the configured target.
    large_limit = 128 * MiB
    with (
        mock.patch.object(
            op.actor_pool, "get_actor_logical_id", return_value="actor-1"
        ),
        mock.patch.object(
            op.actor_pool, "output_bytes_limit", return_value=large_limit
        ),
    ):
        ctx = op._create_task_context(actor)
    assert ctx.target_max_block_size_override == configured

    # Small output budget: ratio-derived size is below the configured target;
    # shrink to fit under the actor's output-bytes limit.
    small_limit = 4 * MiB
    expected = int(small_limit * BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO)
    assert expected < configured
    with (
        mock.patch.object(
            op.actor_pool, "get_actor_logical_id", return_value="actor-1"
        ),
        mock.patch.object(
            op.actor_pool, "output_bytes_limit", return_value=small_limit
        ),
    ):
        ctx = op._create_task_context(actor)
    assert ctx.target_max_block_size_override == expected


def test_ray_remote_args_fn_must_only_assign_a_placement_group():
    """With the sizer on, a ray_remote_args_fn is allowed ONLY if
    it assigns a placement group and nothing else. A fn that assigns a PG merges
    cleanly (the PG strategy is preserved); a fn that overrides the resource
    footprint or sets a non-PG strategy would break the sizer's accounting and is
    rejected at merge time (the one point the fn's realized output is
    available)."""
    # (a) fn assigns a PG -> merge OK, PG strategy preserved.
    ok = _make_actor_op(
        {"num_gpus": 0},
        ray_remote_args_fn=lambda: {
            "scheduling_strategy": PlacementGroupSchedulingStrategy(
                placement_group=cast(PlacementGroup, object())
            )
        },
    )
    merged = ok._merge_ray_remote_args()
    assert isinstance(merged["scheduling_strategy"], PlacementGroupSchedulingStrategy)

    # (b) fn overrides the resource footprint -> rejected (takeover).
    takeover = _make_actor_op(
        {"num_gpus": 0},
        ray_remote_args_fn=lambda: {
            "scheduling_strategy": PlacementGroupSchedulingStrategy(
                placement_group=cast(PlacementGroup, object())
            ),
            "num_gpus": 4,
        },
    )
    with pytest.raises(NotImplementedError, match="num_gpus"):
        takeover._merge_ray_remote_args()

    # (c) fn assigns a non-PG strategy -> rejected.
    non_pg = _make_actor_op(
        {"num_gpus": 0}, ray_remote_args_fn=lambda: {"scheduling_strategy": "SPREAD"}
    )
    with pytest.raises(NotImplementedError, match="placement group"):
        non_pg._merge_ray_remote_args()


def test_data_default_scheduling_strategy_never_reaches_actors():
    """Under the actor-only backend the sizer owns placement, so Ray Data's own
    default ``scheduling_strategy`` ("SPREAD", stamped by read_api on every
    read) must never reach ``ray.remote`` -- neither on a sizer-targeted
    creation nor on an untargeted one.
    """
    op = _make_actor_op(
        {"num_cpus": 1, "scheduling_strategy": "SPREAD"}, is_read_op=True
    )
    assert op._user_set_ray_remote_args["scheduling_strategy"] == "SPREAD"
    assert "scheduling_strategy" not in op._ray_remote_args
    op.validate_ray_remote_args()  # Data's own stamp on a read: not rejected
    # Untargeted creation: nothing leaks through.
    assert "scheduling_strategy" not in op._merge_ray_remote_args()
    # Sizer-targeted creation: the resolved placement is what ray.remote sees.
    pin = NodeAffinitySchedulingStrategy(node_id="ab" * 28, soft=False)
    assert (
        op._merge_ray_remote_args(scheduling_strategy=pin)["scheduling_strategy"] is pin
    )


@pytest.mark.parametrize("strategy", ["SPREAD", "DEFAULT", "PACK"])
def test_user_scheduling_strategy_string_is_rejected(strategy):
    """The same string on a non-read op can only have come from the user, and
    this backend does not honor it -- so say so instead of ignoring it."""
    op = _make_actor_op({"num_cpus": 1, "scheduling_strategy": strategy})
    with pytest.raises(NotImplementedError, match="scheduling_strategy"):
        op.validate_ray_remote_args()


def test_user_custom_scheduling_strategy_is_rejected():
    custom = NodeLabelSchedulingStrategy(hard={"k": In("v")})
    user_op = _make_actor_op({"num_cpus": 1, "scheduling_strategy": custom})
    assert user_op._user_set_ray_remote_args["scheduling_strategy"] is custom
    with pytest.raises(NotImplementedError, match="scheduling_strategy"):
        user_op.validate_ray_remote_args()


def test_node_active_counter_follows_actor_relocation():
    # Regression: the active-actor counter is keyed by actor_location, which
    # changes when Ray core restarts the actor on another node. Decrementing
    # the completion-time location raised KeyError (and leaked the dispatch
    # node's count). _relocate_actor is the only writer of actor_location and
    # must move the count with it, so the paired decrement finds the node
    # that was incremented.
    from ray.data._internal.experimental.execution.operators.actor_pool_models import (  # noqa: E501
        _ExperimentalActorState,
    )

    pool = cast(Any, object.__new__(_ExperimentalActorPool))
    pool._node_to_active_actor_count = {}
    pool.committed_usage_by_node = {}
    pool._actor_resource_usage = {}
    pool._node_to_actor_states = {}
    state = _ExperimentalActorState(
        logical_id="la-1",
        num_tasks_in_flight=1,
        num_input_bytes_in_flight=0,
        actor_location="nodeA",
        max_num_outputs=float("inf"),
        max_num_output_bytes=float("inf"),
        max_tasks_in_flight=4,
        latest_task_submission_ts=0.0,
        status=ActorStatus.ACTIVE,
    )

    pool._mark_node_actor_active(state)
    assert pool._node_to_active_actor_count == {"nodeA": 1}

    # Actor restarts elsewhere while its task is still in flight: the count
    # moves with it.
    pool._relocate_actor(cast(ActorHandle, object()), state, "nodeB")
    assert pool._node_to_active_actor_count == {"nodeB": 1}

    # The completion-time decrement hits nodeB, not nodeA.
    state.num_tasks_in_flight = 0
    pool._mark_node_actor_inactive(state)
    assert pool._node_to_active_actor_count == {}


def test_is_output_backpressured_counts_blocked_tasks_not_actors():
    # is_output_backpressured() is the deadlock hatch's trigger and asks "is
    # every task that is running blocked on its output budget?". It must count
    # TASKS: at the pipeline tail 2 tasks are blocked on a 38-actor pool that
    # is otherwise idle, so (blocked, active) = (2, 2) over tasks reads True,
    # while the actor-denominated (2, 38) would read False and switch the
    # hatch off in exactly the state it exists for.
    from types import SimpleNamespace

    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

    op = cast(Any, object.__new__(ExperimentalActorPoolMapOperator))
    op.get_actor_info = lambda: SimpleNamespace(running=38, pending=0)
    op._data_tasks = {0: object(), 1: object()}
    op.output_backpressured_fraction = lambda: (2, 2)
    op.output_backpressured_actors = lambda: (2, 38)
    assert op.is_output_backpressured() is True


def test_expired_pending_actor_is_released_and_killed(monkeypatch):
    # A pending actor whose ready-ref never resolves (e.g. an actor wedged in
    # failed restarts, ray#53727) must not hold the pool's pending count -- and
    # with it every sizer arm -- forever. After the expiry bound the pool kills
    # the handle, unwinds the bookkeeping, and frees the pending slot.

    import ray
    from ray.data._internal.execution.interfaces import ExecutionResources
    from ray.data._internal.experimental.execution.operators.actor_pool import (
        _ExperimentalActorPool,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_models import (  # noqa: E501
        _PendingActorInfo,
    )

    killed = []
    monkeypatch.setattr(ray, "kill", lambda a: killed.append(a))

    class _Handle:
        pass

    pool = cast(Any, object.__new__(_ExperimentalActorPool))
    actor = _Handle()
    ref = object()
    usage = ExecutionResources(cpu=1, gpu=1, memory=0)
    pool._pending_actors = {ref: actor}
    pool._pending_actor_info = {
        ref: _PendingActorInfo(
            logical_id="la-1", target_node_id="nodeA", created_at=1000.0
        )
    }
    pool._actor_resource_usage = {actor: usage}
    pool._total_usage = usage
    pool._pending_or_restarting_usage = usage
    pool._actor_to_logical_id = {actor: "la-1"}
    removed_committed = []
    pool._remove_committed_usage = lambda node, u: removed_committed.append((node, u))

    # Before the bound: nothing happens.
    assert pool.release_expired_pending_actors(expiry_s=300.0, now=1200.0) == 0
    assert pool.num_pending_actors() == 1

    # Past the bound: released, killed, bookkeeping unwound.
    assert pool.release_expired_pending_actors(expiry_s=300.0, now=1301.0) == 1
    assert pool.num_pending_actors() == 0
    assert killed == [actor]
    assert removed_committed == [("nodeA", usage)]
    assert pool._total_usage.cpu == 0
    assert pool._pending_or_restarting_usage.cpu == 0
    assert actor not in pool._actor_to_logical_id


def test_input_queue_bytes_by_node_tracks_queued_bundles():
    """The per-node byte accounting of the internal task-submission queue
    increments when a bundle is enqueued and decrements when it is dispatched;
    a node drops out once its queued bytes reach zero."""
    op = _make_actor_op({"num_cpus": 1})
    bundle = _make_bundle(100, node_id="node1")

    # Through the real input path so the metrics inqueue mirror stays
    # consistent (_maybe_dispatch_next_bundle enqueues rather than dispatching).
    op._add_input_inner(bundle, 0)
    assert op.input_queue_bytes_by_node() == {"node1": 100}

    queued = op._bundle_queue.peek_next()
    assert queued is not None
    op._dequeue_bundle_for_dispatch(queued)
    assert op.input_queue_bytes_by_node() == {}


def test_backpressure_progress_str_reports_blocked_share():
    """The progress tag reports the share of the pool blocked on output
    budget (actionable), not just the tripped policy name (boolean)."""

    op = cast(Any, object.__new__(ExperimentalActorPoolMapOperator))
    op._output_backpressured_actors_cache_ts = float("-inf")  # cache stale
    op.output_backpressured_actors = lambda: (2, 5)

    class _Pool:
        def current_size(self):
            return 5

    with mock.patch.object(
        ExperimentalActorPoolMapOperator, "actor_pool", property(lambda self: _Pool())
    ):
        assert op.backpressure_progress_str() == "[blocked=2/5]"

        # Nothing blocked: no tag at all.
        op._output_backpressured_actors_cache_ts = float("-inf")
        op.output_backpressured_actors = lambda: (0, 5)
        assert op.backpressure_progress_str() == ""


def test_backpressure_progress_str_reads_write_through_cache():
    """``backpressure_progress_str`` must never pay for the full scan itself
    while a regular caller keeps the write-through cache fresh; a real call to
    ``output_backpressured_actors`` refreshes it (write-through)."""
    import time as _time

    op = cast(Any, object.__new__(ExperimentalActorPoolMapOperator))

    # A fresh cache is served as-is: the scan (stubbed to fail) must not run.
    op._output_backpressured_actors_cache = (3, 5)
    op._output_backpressured_actors_cache_ts = _time.monotonic()
    op.output_backpressured_actors = lambda: pytest.fail(
        "progress str must not recompute a fresh cache"
    )

    class _Pool:
        def current_size(self):
            return 5

    with mock.patch.object(
        ExperimentalActorPoolMapOperator, "actor_pool", property(lambda self: _Pool())
    ):
        assert op.backpressure_progress_str() == "[blocked=3/5]"

        # A stale cache falls back to recomputing.
        op._output_backpressured_actors_cache_ts = float("-inf")
        op.output_backpressured_actors = lambda: (1, 5)
        assert op.backpressure_progress_str() == "[blocked=1/5]"


def test_output_backpressured_actors_counts_serving_actors_only():
    """``blocked`` and ``running`` cover the same actors: a draining or
    restarting actor with an exhausted budget is not a stuck serving actor,
    so it must not make ``blocked == running`` while a serving actor still
    has budget."""
    from types import SimpleNamespace

    exhausted = {"draining", "restarting", "stuck"}
    states = {
        "free": SimpleNamespace(status=ActorStatus.ACTIVE),
        "stuck": SimpleNamespace(status=ActorStatus.IDLE),
        "draining": SimpleNamespace(status=ActorStatus.DRAINING),
        "restarting": SimpleNamespace(status=ActorStatus.RESTARTING),
    }
    op = cast(Any, object.__new__(ExperimentalActorPoolMapOperator))
    # ``running`` excludes draining and restarting actors.
    op.get_actor_info = lambda: SimpleNamespace(running=2, pending=0)
    op._actor_pull_budget = lambda actor_id: TaskPullRequest(
        bytes_to_read=0 if actor_id in exhausted else 1,
        blocks_to_read=0 if actor_id in exhausted else 1,
    )
    pool = SimpleNamespace(alive_actor_states_by_node=lambda: {"n1": states})

    with mock.patch.object(
        ExperimentalActorPoolMapOperator, "actor_pool", property(lambda self: pool)
    ):
        assert op.output_backpressured_actors() == (1, 2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
