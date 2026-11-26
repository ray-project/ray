"""Unit tests for ``OperatorSizer`` and the phase components it sequences:
bootstrap validation and initial sizing, the sizer's phases and per-op how_many,
capacity granting (the ledger), scarcity ordering, constraint matching and
placement-group caps, allocation-shrink reclaim, and the operator's
placement-view surface. Duck-typed operator/pool doubles over a fake
reporter and clock; no cluster."""
import logging
import sys
from collections import Counter
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple, cast

import pytest

from ray.data._internal.actor_autoscaler import (
    ActorPoolScalingRequest,
    AutoscalingActorPool,
)
from ray.data._internal.execution.execution_flags import SIZER_CADENCE_S
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.interfaces.physical_operator import (
    PhysicalOperator,
)
from ray.data._internal.execution.resource_bank import LiveObjStore
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    ActorOnlyResourceReporter,
    BundleKey,
    DatasetReservationView,
    PlacementGroupSlots,
)
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
    SlotUsage,
    next_actor_pool_ops,
    prev_actor_pool_ops,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    DrainReason,
    _DrainingActorInfo,
)
from ray.data._internal.experimental.execution.operators.placement_constraints import (
    PlacementConstraint,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    OpPlacementView,
)
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    OpShareWindow,
)
from ray.data._internal.experimental.execution.sizer.capacity import (
    committed_by_node,
    nodes_to_reclaim,
    pg_free_by_bundle,
)
from ray.data._internal.experimental.execution.sizer.capacity_ledger import (
    ConstraintAwareCapacityLedger,
)
from ray.data._internal.experimental.execution.sizer.locality_actor_placement import (
    LocalityBasedActorPlacement,
)
from ray.data._internal.experimental.execution.sizer.models import (
    ClusterStateSnapshot,
    PlacedRequests,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
    build_op_profile,
    critical_ops,
)
from ray.data._internal.experimental.execution.sizer.op_status import (
    Active,
    Held,
    OpStatusKind,
)
from ray.data._internal.experimental.execution.sizer.operator_ordering import (
    OpOrderingSignals,
    ScarcityFirstOrdering,
    ordered_actor_pool_ops,
)
from ray.data._internal.experimental.execution.sizer.operator_sizer import OperatorSizer
from ray.data._internal.experimental.execution.sizer.policy.ceiling import (
    backlog_actor_target,
    upscale_ceiling,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    OpDecision,
    OpRule,
    SizerPhase,
)
from ray.data._internal.experimental.execution.sizer.policy.warmup import (
    WarmupShares,
)
from ray.data._internal.experimental.execution.sizer.scale_where import (
    compute_favored_nodes,
    place_downscales,
    resolve_evictions,
)
from ray.data._internal.experimental.execution.sizer.telemetry import (
    SizerTelemetry,
    timed,
)
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.util import GiB, MiB
from ray.data.context import DataContext
from ray.data.tests.actor_only.actor_only_test_utils import FakeClock, fake_pg
from ray.util.scheduling_strategies import (
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

_NODE_ID_KEY = "ray.io/node-id"

# The real builder, before the fixture below swaps it out.
_build_op_placement_view = placement_view.build_op_placement_view


@pytest.fixture(autouse=True)
def _views_from_fake_ops(monkeypatch):
    """The doubles serve canned placement views from ``build_placement_view``;
    route the sizer's view builder there."""
    monkeypatch.setattr(
        placement_view,
        "build_op_placement_view",
        lambda op, *, rank_victims: op.build_placement_view(rank_victims=rank_victims),
    )


# --- doubles -------------------------------------------------------------------


class _FakeMetrics:
    def __init__(self):
        self.num_task_outputs_generated = 0
        self.num_task_inputs_processed = 0
        self.num_outputs_taken = 0
        self.num_tasks_finished = 0
        # Inputs consumed per task; the demand target divides enqueued by this.
        self.average_num_inputs_per_task = 1


class _FakeNonPoolOp(PhysicalOperator):
    """A pass-through operator (limit, union, mix, the input buffer).

    Launches no tasks, so ``num_task_outputs_generated`` stays 0 for the whole
    run -- which is exactly why the warmup latch must walk past it.
    """

    def __init__(self, name, inputs=()):
        self._fake_name = name
        self._name = name
        self._input_dependencies = list(inputs)
        self._output_dependencies = []
        self._fake_metrics = _FakeMetrics()
        for dep in inputs:
            dep._output_dependencies.append(self)

    @property
    # pyrefly: ignore[bad-override]
    def name(self):
        return self._fake_name

    @property
    # pyrefly: ignore[bad-override]
    def metrics(self):
        return self._fake_metrics

    @property
    def input_dependencies(self):
        return self._input_dependencies

    @property
    def output_dependencies(self):
        return self._output_dependencies

    def has_completed(self):
        return False


class _FakePool:
    def __init__(
        self,
        *,
        min_size,
        max_size,
        cpu=0.0,
        gpu=0.0,
        memory=0.0,
        running=0,
        pending=0,
        in_flight=0,
        concurrency=1,
    ):
        self._min = min_size
        self._max = max_size
        self._per_actor = ExecutionResources(cpu=cpu, gpu=gpu, memory=memory)
        self.running = running
        self.pending = pending
        self.in_flight = in_flight
        self.concurrency = concurrency
        self.scaled = []
        # Set by the owning _FakeOp: where its actors are committed, the
        # draining part of that, and how many draining actors are reclaimable.
        self.node: Optional[str] = None
        self.committed_override: Optional[Dict[str, ExecutionResources]] = None
        self.draining_usage: Dict[str, ExecutionResources] = {}
        self.reclaimable = 0
        self.upkeep_calls = 0

    def scale(self, request):
        self.scaled.append(request)

    @property
    def committed_usage_by_node(self):
        if self.committed_override is not None:
            return self.committed_override
        if self.node is None:
            return {}
        return {self.node: self._per_actor.scale(self.total_size())}

    def draining_usage_by_node(self):
        return self.draining_usage

    def num_reclaimable_draining_actors(self):
        return self.reclaimable

    def release_expired_pending_actors(self):
        self.upkeep_calls += 1
        return 0

    def total_size(self):
        return self.running + self.pending

    def serving_size(self):
        # The fake has no draining actors, so serving == current.
        return self.running + self.pending

    def min_size(self):
        return self._min

    def initial_size(self):
        return self._min

    def max_size(self):
        return self._max

    bounded_max_size = AutoscalingActorPool.bounded_max_size

    def num_pending_actors(self):
        return self.pending

    def num_draining_actors(self):
        # The fake has no draining actors (see serving_size).
        return 0

    def num_running_actors(self):
        return self.running

    def per_actor_resource_usage(self):
        return self._per_actor

    def num_tasks_in_flight(self):
        return self.in_flight

    def max_actor_concurrency(self):
        return self.concurrency

    def get_resource_requests(self):
        # One shape per actor, as the real pool records at creation.
        return [{"CPU": 1, "GPU": 1}] * self.total_size()


class _FakeOp(ExperimentalActorPoolMapOperator):
    """An actor-pool operator double exposing only what the sizer reads.

    A real subclass (so ``isinstance`` checks hold) that skips the operator's
    ``__init__``: the sizer only touches the accessors overridden below plus
    the real ``placement_constraint`` /
    ``validate_ray_remote_args``, which read the attributes set here.
    """

    def __init__(
        self,
        name,
        pool,
        *,
        completed=False,
        throttling_disabled=False,
        execution_finished=False,
        logical_ops=(),
        inputs=(),
        # Node the fake's actors are committed to (all of them, at
        # per_actor x total_size); None commits nothing.
        node: Optional[str] = "node0",
        ray_remote_args=None,
        user_ray_remote_args=None,
        uses_ray_remote_args_fn=False,
        reclaimable=0,
        # Overrides for the default single-node picture below.
        view: Optional[OpPlacementView] = None,
        committed: Optional[Dict[str, ExecutionResources]] = None,
        # The part of ``committed`` held by draining actors.
        draining: Optional[Dict[str, ExecutionResources]] = None,
        # No input bundle queued yet (the sizer starts no actors until one).
        awaiting_first_input=False,
    ):
        self._fake_name = name
        self.pending_first_input = awaiting_first_input
        self._name = name
        self._id = name
        self._user_set_ray_remote_args = dict(user_ray_remote_args or {})
        self._ray_remote_args = dict(ray_remote_args or {})
        self._user_set_ray_remote_args_fn = uses_ray_remote_args_fn
        self._fn_placement_groups = {}
        self._placement_constraint = None
        self._data_context = DataContext.get_current()
        self._pool = pool
        # Read by the real ``get_resource_requests``.
        self._actor_pool = pool
        self._node = node
        self.completed = completed
        self._throttling_disabled = throttling_disabled
        self._execution_finished = execution_finished
        self._fake_metrics = _FakeMetrics()
        self._logical_operators = list(logical_ops)
        self._canned_view = view
        pool.node = node
        pool.committed_override = committed
        pool.draining_usage = draining or {}
        pool.reclaimable = reclaimable
        # (blocked, active) tasks / (blocked, running) actors for the two
        # backpressure reads; tests set the actor pair when it matters.
        self.out_backpressure = (0, 0)
        self.out_backpressure_actors = None
        self.actor_counts: Dict[str, Counter] = {}
        self.waited_for_min_actors = False
        self._input_dependencies = list(inputs)
        self._output_dependencies = []
        for dep in inputs:
            dep._output_dependencies.append(self)

    @property
    # pyrefly: ignore[bad-override]
    def name(self):
        return self._fake_name

    @property
    # pyrefly: ignore[bad-override]
    def metrics(self):
        return self._fake_metrics

    @property
    # pyrefly: ignore[bad-override]
    def input_dependencies(self):
        return self._input_dependencies

    @property
    # pyrefly: ignore[bad-override]
    def output_dependencies(self):
        return self._output_dependencies

    @property
    # pyrefly: ignore[bad-override]
    def actor_pool(self):
        return self._pool

    # pyrefly: ignore[bad-override]
    def get_autoscaling_actor_pools(self):
        return [self._pool]

    def build_placement_view(self, rank_victims=False):
        if self._canned_view is not None:
            return self._canned_view
        return _placement_view(
            per_actor=self._pool.per_actor_resource_usage(),
            actors_by_node=(
                {self._node: self._pool.total_size()} if self._node else {}
            ),
        )

    def actor_counts_by_node(self):
        # Default to the same picture the pool's committed usage paints -- the
        # pool's actors, all started, on this fake's node -- so the two cannot
        # disagree. Tests that care about a status breakdown set actor_counts.
        if self.actor_counts or self._node is None:
            return self.actor_counts
        running = self._pool.total_size() - self._pool.num_pending_actors()
        counts: Dict[str, Counter] = {}
        if running > 0:
            counts[self._node] = Counter({ActorStatus.ACTIVE: running})
        if self._pool.num_pending_actors() > 0:
            counts.setdefault(self._node, Counter())[
                ActorStatus.PENDING
            ] = self._pool.num_pending_actors()
        return counts

    def wait_for_min_actors(self):
        self.waited_for_min_actors = True

    def awaiting_first_input(self):
        return self.pending_first_input

    def process_draining_actors(self):
        return 0

    def has_completed(self):
        return self.completed

    def throttling_disabled(self):
        return self._throttling_disabled

    def has_execution_finished(self):
        return self._execution_finished

    def output_backpressured_fraction(self):
        return self.out_backpressure

    def productive_task_slots(self):
        return max(0, self._pool.num_tasks_in_flight() - self.out_backpressure[0])

    def slot_usage(self):
        # From the same knobs the other accessors read: blocked actors are
        # output-backpressured; the fake keeps no slot integrals.
        serving = self._pool.num_running_actors()
        conc = self._pool.max_actor_concurrency()
        blocked = min(serving, self.out_backpressure[0])
        return SlotUsage(
            serving_actors=serving,
            slots=serving * conc,
            slot_seconds=0.0,
            obp_slots=blocked * conc,
            obp_busy_slots=0,
            busy_slots=0,
            busy_slot_seconds=0.0,
        )

    def output_backpressured_actors(self):
        # Mirror the real contract: a pool with no running actors or with
        # pending (mid-resize) actors reports (0, 0).
        if self._pool.num_running_actors() == 0 or self._pool.num_pending_actors() > 0:
            return 0, 0
        if self.out_backpressure_actors is not None:
            return self.out_backpressure_actors
        return self.out_backpressure

    def min_max_resource_requirements(self):
        per = self._pool.per_actor_resource_usage()
        return (
            per.scale(self._pool.min_size()),
            per.scale(self._pool.max_size()).copy(object_store_memory=float("inf")),
        )


class _FakeOpState:
    def __init__(self, enqueued_blocks=0):
        self._enqueued = enqueued_blocks

    def total_enqueued_input_blocks(self):
        return self._enqueued


class _FakeReporter(ActorOnlyResourceReporter):
    """Serves a fixed cluster view; tests replace ``view`` to model a changing
    grant. Skips the real ``__init__`` (no coordinator, no ``ray.nodes()``).
    Like the real reporter, measures the topology's PG slots from the
    placement groups' bundle shapes (``pg_bundles_by_id``)."""

    def __init__(
        self, view: DatasetReservationView, topology=None, pg_bundles_by_id=None
    ):
        self.view = view
        self.topology = topology or {}
        self.pg_bundles_by_id = dict(pg_bundles_by_id or {})

    def get_reserved_resources_by_node(self):
        return dict(self.view.reserved_by_node)

    def get_cluster_view(self):
        return replace(
            self.view,
            pg_slots=PlacementGroupSlots.measure(
                (
                    op
                    for op in self.topology
                    if isinstance(op, ExperimentalActorPoolMapOperator)
                ),
                self.pg_bundles_by_id,
            ),
        )


class _TestSizer(OperatorSizer):
    """An ``OperatorSizer`` over fakes, plus the seams tests drive it by: one
    phase at a time, off the fake reporter's view and the fake clock."""

    def __init__(self, topo, reporter: _FakeReporter, clock: FakeClock, **sizer_kwargs):
        super().__init__(dataset_id="test-ds", clock=clock, **sizer_kwargs)
        self.reporter = reporter
        self.clock = clock
        self.bootstrap(topo)

    def _scale_to_initial_size(self) -> None:
        """Skipped by ``bootstrap``, so tests start from the fakes' sizes;
        tests that check initial sizing call ``initial_sizing_request``."""

    def initial_sizing_request(self) -> None:
        OperatorSizer._scale_to_initial_size(self)

    def next_tick(self, *, force: bool = True) -> Optional[ClusterStateSnapshot]:
        """This step's snapshot, or None inside the cadence window; ``force``
        steps the clock past the cadence. Runs the step's lifecycle
        transitions, as the scheduling loop's call does."""
        if force:
            self.clock.advance(SIZER_CADENCE_S)
        return self.maybe_build_cluster_state_snapshot(
            cluster_view=self.reporter.get_cluster_view(),
            seconds_since_allocation_change=0.0,
        )

    def held_snapshot(self, *, force: bool = True) -> ClusterStateSnapshot:
        """The next tick's snapshot with the open transfers advanced and the
        room for held ops and transfers already reserved, as every later step
        of a tick sees it."""
        snapshot = self.next_tick(force=force)
        assert snapshot is not None
        landed = self._statuses.on_tick(now=self._clock())
        self._policy.record_landed_upscales(landed=landed)
        self._reserve_for_held_ops(snapshot=snapshot)
        return snapshot

    def how_many(self, *, force: bool = True) -> Dict[Any, ActorPoolScalingRequest]:
        """One how-many step's counts (the decisions WHERE places), after the
        step's status transitions, the holds and the tick's phase update."""
        snapshot = self.held_snapshot(force=force)
        self._maybe_transition_to_new_phase()
        return _counts(
            self._per_op_how_many(snapshot=snapshot, bottleneck_snapshot=None)
        )

    @property
    def statuses(self):
        return self._statuses


def _placement_view(
    *, per_actor, victim_ids_by_node=None, pending_ids_by_node=None, actors_by_node=None
):
    return OpPlacementView(
        op_id="op",
        is_source=False,
        per_actor_usage=per_actor,
        input_bytes_per_actor=None,
        actors_by_node=dict(actors_by_node or {}),
        input_queue_bytes_by_node={},
        upstream_actors_by_node={},
        downstream_actors_by_node={},
        victim_ids_by_node=dict(victim_ids_by_node or {}),
        idle_actors_by_node={},
        pending_ids_by_node=dict(pending_ids_by_node or {}),
    )


def _topology(*ops: PhysicalOperator, enqueued: int = 0) -> Any:
    # Source-first insertion order, like build_streaming_topology.
    return {op: _FakeOpState(enqueued_blocks=enqueued) for op in ops}


def _single_node(cpu=100.0, gpu=4.0) -> DatasetReservationView:
    return DatasetReservationView(
        reserved_by_node={"node0": ExecutionResources(cpu=cpu, gpu=gpu)},
        labels_by_node={"node0": {}},
    )


def _make_sizer(
    topo,
    view: Optional[DatasetReservationView] = None,
    pg_bundles_by_id=None,
    *,
    sizer_kwargs: Optional[Dict[str, Any]] = None,
    **cluster,
) -> _TestSizer:
    """A sizer over ``topo`` whose reporter serves the given cluster view (a
    single ``node0`` by default) and placement-group bundle shapes. Free
    capacity is the tick's real math: allocation minus what the fakes commit
    (per_actor x total_size). ``sizer_kwargs`` go to ``OperatorSizer``."""
    return _TestSizer(
        topo,
        _FakeReporter(view or _single_node(**cluster), topo, pg_bundles_by_id),
        FakeClock(now=100.0),
        **(sizer_kwargs or {}),
    )


def _bootstrapped(
    *ops, enqueued=0, view=None, pg_bundles_by_id=None, warm=False, **cluster
):
    """Sizer over ``ops``; ``warm=True`` has every op report output and
    starts the sizer in STEADY, so the bottleneck snapshot decides from the
    first tick. Returns (sizer, topology)."""
    topo = _topology(*ops, enqueued=enqueued)
    sizer = _make_sizer(topo, view, pg_bundles_by_id, **cluster)
    if warm:
        for op in topo:
            op.metrics.num_task_outputs_generated = 1
        sizer.phase = SizerPhase.STEADY
    return sizer, topo


def _counts(decisions) -> Dict[Any, ActorPoolScalingRequest]:
    """The decisions that carry a count, by op."""
    return {op: d.request for op, d in decisions.items() if d.request is not None}


def _tick(sizer: _TestSizer) -> Dict[Any, ActorPoolScalingRequest]:
    return sizer.how_many()


def _warmup_probe(topo, **sizer_kwargs):
    """A sizer over ``topo``, and a callable that runs its next tick and
    reports whether the sizer still waits for the pipeline to flow.
    ``sizer_kwargs`` go to ``OperatorSizer``."""
    sizer = _make_sizer(topo, sizer_kwargs=sizer_kwargs)

    def waits_for_flow_after_tick() -> bool:
        sizer.how_many()
        return sizer._flowing_since_t is None

    return sizer, waits_for_flow_after_tick


def _snapshot_of(sizer: _TestSizer) -> ClusterStateSnapshot:
    return sizer.held_snapshot()


def _write_op(name, pool, **kwargs) -> _FakeOp:
    return _FakeOp(name, pool, logical_ops=(Write.__new__(Write),), **kwargs)


def _linear_pipeline(running=1):
    """A -> B -> infer(GPU, fixed 4) -> write. Returns (ops dict, topology)."""
    a = _FakeOp(
        "ReadFiles", _FakePool(min_size=1, max_size=500, cpu=1, running=running)
    )
    b = _FakeOp(
        "Map(preprocess)",
        _FakePool(min_size=1, max_size=500, cpu=1, running=running),
        inputs=(a,),
    )
    infer = _FakeOp(
        "MapBatches(Infer)",
        _FakePool(min_size=4, max_size=4, gpu=1, running=4),
        inputs=(b,),
    )
    write = _write_op(
        "Write",
        _FakePool(min_size=1, max_size=500, cpu=1, running=running),
        inputs=(infer,),
    )
    return {"a": a, "b": b, "infer": infer, "write": write}, _topology(
        a, b, infer, write
    )


# --- bootstrap and validation --------------------------------------------------


def test_op_awaiting_first_input_gets_no_actors_until_it_arrives():
    # Creating an actor serializes the MapTransformer, so an actor created
    # before ``on_write_start`` ships a datasink without the state it sets
    # (Iceberg: table metadata is None). Such an op stays at zero actors,
    # through initial sizing and how_many, until its first input runs it.
    read = _FakeOp("read", _FakePool(min_size=1, max_size=10, cpu=1))
    write = _write_op(
        "write",
        _FakePool(min_size=1, max_size=10, cpu=1),
        inputs=[read],
        awaiting_first_input=True,
    )
    sizer, _ = _bootstrapped(read, write)

    sizer.initial_sizing_request()
    assert len(read._pool.scaled) == 1
    assert write._pool.scaled == []
    assert not write.waited_for_min_actors
    assert write not in _tick(sizer)

    write.pending_first_input = False
    assert _tick(sizer)[write].delta >= 1


def test_op_awaiting_first_input_has_its_share_held_until_it_arrives():
    # An op held at zero actors until its first input arrives must still find
    # room when it does. Without a hold, upstream ops can take every node's space
    # first and the op (e.g. a Write sink) can then never grow past its first
    # actor. Its warmup share stays out of the tick's free capacity, with
    # memory held as well as CPU, so every phase sizes and places around it;
    # once its input arrives it grows straight to that share.
    read = _FakeOp(
        "read", _FakePool(min_size=1, max_size=10, cpu=1, memory=1, running=1)
    )
    write = _write_op(
        "write",
        _FakePool(min_size=1, max_size=10, cpu=1, memory=1),
        inputs=[read],
        awaiting_first_input=True,
    )
    view = DatasetReservationView(
        reserved_by_node={"node0": ExecutionResources(cpu=16, memory=20)},
        labels_by_node={"node0": {}},
    )
    sizer, _ = _bootstrapped(read, write, view=view)
    # Write's share: (16 - 2 floors) x 0.8 / 2 = 5.6 -> 5; memory allows 8.
    target = 5

    assert write not in _tick(sizer)
    free = _snapshot_of(sizer).free_by_node["node0"]
    # Minus read's running actor, minus write's held share.
    assert (free.cpu, free.memory) == (16 - 1 - target, 20 - 1 - target)

    # Write's first input arrives, so read has produced output. Write
    # becomes active and grows into the room held for it, in one step.
    read.metrics.num_task_outputs_generated = 1
    write.pending_first_input = False
    request = _tick(sizer)
    assert (request[write].delta, request[write].reason) == (
        target,
        "room held until its first input",
    )
    write._pool.pending = target
    assert write not in _tick(sizer)


def _memory_bound_gpu_pipeline():
    """Read -> resize -> a 1-GPU feature stage -> Write, one actor each, on
    one 32 CPU / 8 GPU / 61.6 GiB node. The CPU stages want 2.4 GiB per
    actor and the GPU stage 3.3 GiB, so memory, not CPU, bounds the shares.
    Returns (ops dict, view)."""
    read = _FakeOp(
        "Read",
        _FakePool(min_size=1, max_size=100, cpu=1, memory=2.4 * GiB, running=1),
    )
    resize = _FakeOp(
        "resize",
        _FakePool(min_size=1, max_size=100, cpu=1, memory=2.4 * GiB, running=1),
        inputs=(read,),
    )
    features = _FakeOp(
        "ExtractImageFeatures",
        _FakePool(min_size=1, max_size=8, cpu=1, gpu=1, memory=3.3 * GiB, running=1),
        inputs=(resize,),
    )
    write = _write_op(
        "Write",
        _FakePool(min_size=1, max_size=100, cpu=1, memory=2.4 * GiB, running=1),
        inputs=(features,),
    )
    view = DatasetReservationView(
        reserved_by_node={
            "node0": ExecutionResources(cpu=32, gpu=8, memory=61.6 * GiB)
        },
        labels_by_node={"node0": {}},
    )
    return {"read": read, "resize": resize, "features": features, "write": write}, view


def test_warmup_share_respects_memory_after_gpu_ops():
    # A CPU-only share, (32 - 3 floors) x 0.8 / 3 = 7.7 -> 7 per CPU stage,
    # would take 21 x 2.4 = 50.4 memory; with the GPU stage's 5 x 3.3 that
    # exceeds the usable 49.3 and leaves the GPU stage unplaceable. GPU stage
    # first: 5 x 3.3 = 16.5; (49.3 - 16.5) / 3 / 2.4 = 4.6 -> 4.
    cpu_target = 4
    ops, view = _memory_bound_gpu_pipeline()
    warmup = WarmupShares(profiles=OpProfiles(_topology(*ops.values())))
    capacity = view.reserved_by_node["node0"]
    targets = {
        name: warmup.warmup_target(
            op=op, cluster_capacity=capacity, pg_slots=PlacementGroupSlots()
        )
        for name, op in ops.items()
    }

    # GPU share: (8 - 1 floor) x 0.8 = 5.6 -> 5.
    assert targets == {
        "read": cpu_target,
        "resize": cpu_target,
        "features": 5,
        "write": cpu_target,
    }


def test_memory_aware_share_leaves_cpu_only_actors_alone():
    # Actors that request no memory are bounded by CPU alone.
    read = _FakeOp("read", _FakePool(min_size=1, max_size=100, cpu=1))
    write = _write_op(
        "write", _FakePool(min_size=1, max_size=100, cpu=1), inputs=(read,)
    )
    warmup = WarmupShares(profiles=OpProfiles(_topology(read, write)))
    # (32 - 2 floors) x 0.8 / 2 = 12.
    assert (
        warmup.warmup_target(
            op=read,
            cluster_capacity=ExecutionResources(cpu=32, gpu=4),
            pg_slots=PlacementGroupSlots(),
        )
        == 12
    )


def _held(sizer: _TestSizer, op) -> Held:
    """The room held for ``op`` this tick (it must be held)."""
    state = sizer.statuses.status(op)
    assert isinstance(state, Held), state
    return state


def test_ops_awaiting_first_input_each_reserve_their_share():
    # Every stage downstream of read waits on its first input: each holds its
    # warmup share (no actors started), and each grows to it once its own
    # input arrives.
    ops, view = _memory_bound_gpu_pipeline()
    for name in ("resize", "features", "write"):
        ops[name].pending_first_input = True
    sizer, _ = _bootstrapped(*ops.values(), view=view)

    _snapshot_of(sizer)
    held = sizer.statuses.ops_in(kind=OpStatusKind.HELD)
    assert held == {ops["resize"], ops["features"], ops["write"]}
    assert _held(sizer, ops["features"]).reserved == 5
    resize_reserved = _held(sizer, ops["resize"]).reserved

    # resize's first input arrives, so read has produced output.
    ops["read"].metrics.num_task_outputs_generated = 1
    ops["resize"].pending_first_input = False
    request = _tick(sizer)
    # resize becomes active and upscales into the room held for it (it
    # already has 1 actor); the others stay held.
    assert request[ops["resize"]].delta == resize_reserved - 1
    assert request[ops["resize"]].reason == "room held until its first input"
    assert sizer.statuses.kind(ops["features"]) is OpStatusKind.HELD


def test_warmup_lasts_one_window_after_the_pipeline_flows():
    # Warmup ends one bottleneck window after the critical op's first
    # output, and the observer drops what it measured before then: the
    # first window the policy decides from holds no startup time.
    ops, topo = _linear_pipeline(running=1)
    # Critical op = deepest task-launching op, i.e. the last non-sink map -> infer.
    assert critical_ops(topo) == (ops["infer"],)
    sizer = _make_sizer(topo)
    observer = sizer._bottleneck_observer
    _tick(sizer)
    assert sizer._flowing_since_t is None

    observer._windows[ops["infer"]] = OpShareWindow(observer.window_s)
    ops["infer"].metrics.num_task_outputs_generated = 1
    _tick(sizer)
    assert sizer._flowing_since_t is not None
    assert observer._windows == {}

    sizer.clock.advance(observer.window_s - 2 * SIZER_CADENCE_S)
    _tick(sizer)
    assert sizer.phase is SizerPhase.WARMUP
    # The next tick is one full window past the first output. Without a
    # bottleneck snapshot, the first STEADY tick holds every size.
    assert _tick(sizer) == {}
    # _tick moves the phase; pyrefly keeps the earlier narrowing.
    assert sizer.phase is SizerPhase.STEADY  # pyrefly: ignore[unnecessary-comparison]


def test_warmup_times_out_when_a_critical_op_never_produces(caplog, propagate_logs):
    # A critical op that never gets an actor (say, starved by a memory
    # reservation) must not hold the sizer in warmup forever: after the
    # timeout it moves to the warm phase, and says which op it gave up on.
    ops, topo = _linear_pipeline(running=1)
    _, waits_for_flow_after_tick = _warmup_probe(
        topo, warmup_timeout_s=10 * SIZER_CADENCE_S
    )

    # The timeout counts from the first tick.
    assert all(waits_for_flow_after_tick() for _ in range(10))
    with caplog.at_level(logging.WARNING):
        assert not waits_for_flow_after_tick()
    assert any(
        "warmup timed out" in r.getMessage() and ops["infer"].name in r.getMessage()
        for r in caplog.records
    )


def test_initial_sizing_request_creates_untargeted_in_scarcity_order():
    # Pools never scale themselves to their initial size under the sizer; the
    # executor asks it right after building it. Initial actors are created
    # UNTARGETED (Ray core places them; constrained actors carry their
    # label_selector), in the sizer's scarcity order -- a tiebreaker, not a
    # placement guarantee.
    read = _FakeOp("read", _FakePool(min_size=1, max_size=10, cpu=1))
    infer = _FakeOp("infer", _FakePool(min_size=4, max_size=4, gpu=1), inputs=[read])
    write = _write_op(
        "write", _FakePool(min_size=1, max_size=10, cpu=1), inputs=[infer]
    )
    sizer, topo = _bootstrapped(read, infer, write)
    sizer.initial_sizing_request()

    expected_order = [
        cast(_FakeOp, op)
        for op in ordered_actor_pool_ops(topo, OpProfiles(topo).actor_pool_ops)
    ]
    scaled_ops = [op for op in expected_order if op._pool.scaled]
    assert scaled_ops == expected_order
    for op in scaled_ops:
        (req,) = op._pool.scaled
        assert req.delta == op._pool.initial_size()
        assert not getattr(req, "nodes", ())
        # The optional wait_for_min_actors_s wait runs here, since start() no
        # longer creates the actors it would wait on.
        assert op.waited_for_min_actors

    # Pools already at their initial size get no scale call.
    _, at_size = _linear_pipeline()
    sizer = _make_sizer(at_size)
    sizer.initial_sizing_request()
    assert all(op._pool.scaled == [] for op in at_size)


@pytest.mark.parametrize(
    "pool_kwargs,uses_fn,expected_type",
    [
        # A GPU pool that also reserves CPU is allowed and sizes as GPU.
        pytest.param(dict(cpu=1, gpu=1), False, "gpu", id="mixed-cpu-gpu"),
        # Per-actor memory is modeled, not rejected.
        pytest.param(dict(cpu=1, memory=128), False, "cpu", id="memory"),
        # A ray_remote_args_fn op (vLLM "ray" backend) declares no CPU/GPU --
        # its GPUs live in the fn's placement group -- and is self-placed, so
        # the shape guard does not apply.
        pytest.param(dict(cpu=0, gpu=0), True, "cpu", id="self-placed-fn-op"),
    ],
)
def test_resource_shape_accepted(pool_kwargs, uses_fn, expected_type):
    op = _FakeOp(
        "Op",
        _FakePool(min_size=1, max_size=10, **pool_kwargs),
        uses_ray_remote_args_fn=uses_fn,
    )
    _bootstrapped(op)  # does not raise
    assert build_op_profile(op).resource_type == expected_type


def test_resource_shape_rejects_actor_requesting_neither_cpu_nor_gpu():
    op = _FakeOp("Op", _FakePool(min_size=1, max_size=10, cpu=0, gpu=0))
    # Construction validates the topology, so a bad shape fails before sizing.
    with pytest.raises(ValueError, match="requests neither"):
        _make_sizer(_topology(op))


# placement_group / placement_group_bundle_index are ACCEPTED (the sizer caps
# such pools at the PG's bundle capacity and lets Ray core place into it);
# placement_group_capture_child_tasks is always rejected.
@pytest.mark.parametrize(
    "user_remote_args",
    [
        {"scheduling_strategy": NodeLabelSchedulingStrategy(hard={"k": In("v")})},
        {"placement_group_capture_child_tasks": True},
        {"resources": {"custom": 1}},
        # Actors are never restarted by Ray core in this backend.
        {"max_restarts": -1},
        {"max_restarts": 3},
    ],
)
def test_validate_ray_remote_args_rejects(user_remote_args):
    op = _FakeOp(
        "Bad",
        _FakePool(min_size=1, max_size=10, cpu=1),
        user_ray_remote_args=user_remote_args,
    )
    with pytest.raises(NotImplementedError):
        op.validate_ray_remote_args()
    # Building the sizer runs the same check, so a bad op fails before sizing.
    with pytest.raises(NotImplementedError):
        _make_sizer(_topology(op))


@pytest.mark.parametrize(
    "max_concurrency,rejected", [(0, True), (1, False), (4, False)]
)
def test_validate_ray_remote_args_requires_positive_max_concurrency(
    max_concurrency, rejected
):
    # Ray's own option check accepts max_concurrency=0, but sizing divides by
    # it, so it is rejected up front.
    op = _FakeOp(
        "Op",
        _FakePool(min_size=1, max_size=10, cpu=1),
        ray_remote_args={"max_concurrency": max_concurrency},
    )
    if rejected:
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            op.validate_ray_remote_args()
    else:
        op.validate_ray_remote_args()


@pytest.mark.parametrize(
    "user_remote_args,uses_fn",
    [
        # A ray_remote_args_fn is allowed here: the PG-only contract is
        # enforced per actor in _merge_ray_remote_args.
        pytest.param({}, True, id="ray-remote-args-fn"),
        pytest.param({"placement_group": object()}, False, id="placement-group"),
        pytest.param(
            {
                "num_cpus": 1,
                "num_gpus": 1,
                "memory": 100,
                "max_restarts": 0,
                "runtime_env": {"env_vars": {"X": "1"}},
            },
            False,
            id="resources-and-benign-knobs",
        ),
        pytest.param(
            {"label_selector": {_NODE_ID_KEY: "n1"}}, False, id="label-selector"
        ),
    ],
)
def test_validate_ray_remote_args_allows(user_remote_args, uses_fn):
    op = _FakeOp(
        "Ok",
        _FakePool(min_size=1, max_size=10, cpu=1, gpu=1),
        user_ray_remote_args=user_remote_args,
        uses_ray_remote_args_fn=uses_fn,
    )
    op.validate_ray_remote_args()  # does not raise


@pytest.mark.parametrize(
    "ray_remote_args,user_args,expected_pin,expected_selector,expect_pg",
    [
        # A NodeAffinity pin is a node-id selector on the constraint.
        pytest.param(
            {
                "scheduling_strategy": NodeAffinitySchedulingStrategy(
                    "a" * 56, soft=False
                )
            },
            {},
            "a" * 56,
            {_NODE_ID_KEY: "a" * 56},
            False,
            id="node-affinity",
        ),
        # A node-id label_selector (local-scheme reads) pins too, even
        # alongside other keys.
        pytest.param(
            {"label_selector": {_NODE_ID_KEY: "n1", "region": "us"}},
            {},
            "n1",
            {_NODE_ID_KEY: "n1", "region": "us"},
            False,
            id="node-id-selector",
        ),
        pytest.param(
            {"label_selector": {"region": "us"}},
            {},
            None,
            {"region": "us"},
            False,
            id="generic-selector-no-pin",
        ),
        pytest.param({"num_cpus": 1}, {}, None, None, False, id="unconstrained"),
        # A user PG strategy surfaces as the constraint's placement group.
        pytest.param(
            {},
            {
                "scheduling_strategy": PlacementGroupSchedulingStrategy(
                    cast(Any, object()), 2
                )
            },
            None,
            None,
            True,
            id="placement-group-strategy",
        ),
    ],
)
def test_placement_constraint_and_pin(
    ray_remote_args, user_args, expected_pin, expected_selector, expect_pg
):
    op = _FakeOp(
        "Op",
        _FakePool(min_size=1, max_size=10, cpu=1),
        ray_remote_args=ray_remote_args,
        user_ray_remote_args=user_args,
    )
    constraint = op.placement_constraint()
    # A node pin, however it was spelled, surfaces as a node-id selector entry.
    assert (constraint.label_selector or {}).get(_NODE_ID_KEY) == expected_pin
    assert constraint.label_selector == expected_selector
    assert constraint.has_placement_group is expect_pg
    if expect_pg:
        assert constraint.placement_group_bundle_index == 2
    assert constraint.has_label_selector is (expected_selector is not None)
    # Parsed once: constraints are fixed for the op's lifetime.
    assert op.placement_constraint() is constraint


def _label_view():
    alloc = {
        "n1": ExecutionResources(cpu=16),
        "g1": ExecutionResources(gpu=4),
        "g2": ExecutionResources(gpu=4),
        "g3": ExecutionResources(gpu=4),
    }
    labels = {
        "n1": {_NODE_ID_KEY: "n1"},
        "g1": {_NODE_ID_KEY: "g1", "accelerator-type": "A100"},
        "g2": {_NODE_ID_KEY: "g2", "accelerator-type": "A100"},
        "g3": {_NODE_ID_KEY: "g3", "accelerator-type": "L4"},
    }
    return DatasetReservationView(reserved_by_node=alloc, labels_by_node=labels)


# n1 is CPU-only and g1/g2/g3 are GPU-only. Nodes must satisfy the label
# selector AND (when per_actor is given) carry every resource the op requests.
@pytest.mark.parametrize(
    "selector,per_actor,expected",
    [
        pytest.param(
            {"accelerator-type": "A100"}, ExecutionResources(), {"g1", "g2"}, id="exact"
        ),
        pytest.param(
            {"accelerator-type": "!A100"},
            ExecutionResources(),
            {"n1", "g3"},
            id="negation",
        ),
        pytest.param(
            {"accelerator-type": "in(A100,L4)"},
            ExecutionResources(),
            {"g1", "g2", "g3"},
            id="in-set",
        ),
        pytest.param(
            {"accelerator-type": "H100"},
            ExecutionResources(),
            set(),
            id="matches-nothing",
        ),
        pytest.param(
            None, ExecutionResources(), {"n1", "g1", "g2", "g3"}, id="unconstrained"
        ),
        pytest.param({_NODE_ID_KEY: "n1"}, ExecutionResources(), {"n1"}, id="node-pin"),
        # A value Ray core's matcher rejects degrades to "eligible everywhere"
        # with a warning, never an empty set.
        pytest.param(
            {"accelerator-type": "in()"},
            ExecutionResources(),
            {"n1", "g1", "g2", "g3"},
            id="unparseable-degrades-to-all",
        ),
        pytest.param(
            None, ExecutionResources(gpu=1), {"g1", "g2", "g3"}, id="gpu-actor-filter"
        ),
        pytest.param(None, ExecutionResources(cpu=1), {"n1"}, id="cpu-actor-filter"),
        pytest.param(
            {"accelerator-type": "A100"},
            ExecutionResources(gpu=1),
            {"g1", "g2"},
            id="label-and-resource-intersect",
        ),
    ],
)
def test_matching_nodes(selector, per_actor, expected: set):
    c = PlacementConstraint(label_selector=selector)
    assert set(c.matching_nodes(_label_view(), per_actor)) == expected


def _pg_op(name, pg, *, bundle_index=None, inputs=(), **pool_kwargs) -> _FakeOp:
    kwargs = dict(min_size=1, max_size=10)
    kwargs.update(pool_kwargs)
    op = _FakeOp(name, _FakePool(**kwargs), node=None, inputs=inputs)
    op._placement_constraint = PlacementConstraint(
        placement_group=cast(Any, pg), placement_group_bundle_index=bundle_index
    )
    return op


@pytest.mark.parametrize(
    "running, pending, awaiting_first_input, completed, expected_requests",
    [
        # Autoscale from zero: no actor exists, so the min-size shapes are the
        # only GPU demand the cluster autoscaler sees.
        pytest.param(0, 0, False, False, 2, id="no-actors"),
        pytest.param(1, 0, False, False, 2, id="one-running"),
        pytest.param(0, 1, False, False, 2, id="one-pending"),
        pytest.param(3, 0, False, False, 3, id="above-min"),
        # An op held until its first input still asks for its min size.
        pytest.param(0, 0, True, False, 2, id="held"),
        # A completed op's actors drain to zero, so it asks only for those left.
        pytest.param(1, 0, False, True, 1, id="completed"),
    ],
)
def test_resource_requests_cover_the_min_size(
    running, pending, awaiting_first_input, completed, expected_requests
):
    """On a cluster with no GPU node, the sizer finds no room for a GPU op
    and creates no actors for it. The op still requests its min-size shapes;
    otherwise the cluster autoscaler never sees GPU demand and the pipeline
    hangs."""
    infer = _FakeOp(
        "infer",
        _FakePool(
            min_size=2, max_size=8, cpu=1, gpu=1, running=running, pending=pending
        ),
        node=None,
        ray_remote_args={"num_cpus": 1, "num_gpus": 1},
        awaiting_first_input=awaiting_first_input,
        completed=completed,
    )

    assert infer.get_resource_requests() == [{"CPU": 1, "GPU": 1}] * expected_requests


def test_pg_managed_op_requests_only_its_actors():
    op = _pg_op("infer", fake_pg("pg1"), min_size=2, gpu=1)

    assert op.get_resource_requests() == []


def test_pg_bundles_are_per_bundle_slots(caplog, propagate_logs):
    """Each bundle is its own slot (a 4-GPU actor fits no 2-GPU bundle, even
    though the PG totals 8 GPU); a bundle index narrows the op to one bundle,
    an out-of-range one is warned about and widened to every bundle; an
    unresolved PG is left out (its capacity is unknown)."""
    pg = fake_pg("pg1")
    pg_bundles_by_id = {"pg1": [ExecutionResources(gpu=2)] * 4}
    anywhere = _pg_op("anywhere", pg, gpu=1, running=3)
    pinned = _pg_op("pinned", pg, bundle_index=1, gpu=1, running=1, inputs=[anywhere])
    big = _pg_op("big", pg, gpu=4, inputs=[pinned])
    unresolved = _pg_op("unresolved", fake_pg("missing"), gpu=1, inputs=[big])
    bad_index = _pg_op("bad_index", pg, bundle_index=9, gpu=1, inputs=[unresolved])
    with caplog.at_level(logging.WARNING):
        bundles = PlacementGroupSlots.measure(
            [anywhere, pinned, big, unresolved, bad_index], pg_bundles_by_id
        )

    every_bundle = tuple(BundleKey("pg1", i) for i in range(4))
    assert bundles.static_bundles_by_op[anywhere] == every_bundle
    assert bundles.static_bundles_by_op[pinned] == (BundleKey("pg1", 1),)
    assert unresolved not in bundles.static_bundles_by_op
    assert bundles.unknown_cap_by_op[unresolved] == unresolved._pool.max_size()
    assert bundles.static_bundles_by_op[bad_index] == every_bundle
    assert any(
        "placement_group_bundle_index=9" in r.getMessage() for r in caplog.records
    )
    # Held actors pack first-fit, most constrained op first: pinned's actor
    # takes half of bundle 1, then anywhere's 3 fill bundle 0 and the rest of 1.
    free = pg_free_by_bundle(bundles)
    assert [free[BundleKey("pg1", i)].gpu for i in range(4)] == [0, 0, 2, 2]

    snapshot = _snapshot(
        {},
        {},
        pg_free=free,
        bundles=bundles.static_bundles_by_op,
    )
    ledger = ConstraintAwareCapacityLedger(snapshot)
    assert ledger.max_upscale_delta(anywhere) == 4
    assert ledger.max_upscale_delta(big) == 0  # 4 GPU free, but split in two


def test_placement_group_bundles_are_not_free_for_non_pg_ops():
    """The slice includes the placement group's bundles on the nodes Ray placed
    them, but only PG actors can use them: non-PG ops see that node's capacity
    without them, read from each tick's view, so a group Ray places after the
    sizer is built is taken out on the next tick."""
    pg = fake_pg("pg1")
    reserved_by_node = {
        "g1": ExecutionResources(cpu=8, gpu=4),
        "c1": ExecutionResources(cpu=8),
    }
    pg_bundles_by_id = {"pg1": [ExecutionResources(cpu=1, gpu=1)] * 2}
    view = DatasetReservationView(
        reserved_by_node=reserved_by_node,
        labels_by_node={"g1": {}, "c1": {}},
    )
    infer = _pg_op("infer", pg, cpu=1, gpu=1, running=2)
    write = _FakeOp(
        "write",
        _FakePool(min_size=1, max_size=10, cpu=1, running=1),
        node="g1",
        inputs=[infer],
    )
    # Upstream is producing, so write holds no share and free shows only the
    # bundles taken out.
    infer.metrics.num_task_outputs_generated = 1
    sizer, _ = _bootstrapped(infer, write, view=view, pg_bundles_by_id=pg_bundles_by_id)

    # Pending group: it holds nothing yet.
    assert _snapshot_of(sizer).reserved_by_node["g1"] == reserved_by_node["g1"]

    sizer.reporter.view = replace(
        view, pg_reserved_by_node={"g1": ExecutionResources(cpu=2, gpu=2)}
    )
    capacity = _snapshot_of(sizer)
    assert capacity.reserved_by_node["g1"] == ExecutionResources(cpu=6, gpu=2)
    assert capacity.free_by_node["g1"] == ExecutionResources(cpu=5, gpu=2)
    assert capacity.free_by_node["c1"] == ExecutionResources(cpu=8)


@pytest.mark.parametrize(
    "pg,uses_fn,pool_kwargs,expected_cap",
    [
        # A static PG op grows into what its bundles hold.
        pytest.param(
            fake_pg("pg1"), False, dict(min_size=2, max_size=10, gpu=1), 4, id="pg"
        ),
        # A ray_remote_args_fn op mints a PG per actor the sizer cannot inspect,
        # so it is capped at its user concurrency (max_size) instead.
        pytest.param(None, True, dict(min_size=1, max_size=1, gpu=0), 1, id="fn-op"),
        # A static PG with no bundles in the view yet is just as unknown:
        # capped at max_size too, rather than held at zero.
        pytest.param(
            fake_pg("missing"),
            False,
            dict(min_size=1, max_size=3, gpu=1),
            3,
            id="pg-not-resolved",
        ),
    ],
)
def test_sizer_measures_core_placed_caps(pg, uses_fn, pool_kwargs, expected_cap):
    pg_bundles_by_id = {"pg1": [ExecutionResources(gpu=4)]}
    view = DatasetReservationView(
        reserved_by_node={"g1": ExecutionResources(gpu=4)},
    )
    op = _FakeOp(
        "embed", _FakePool(**pool_kwargs), node=None, uses_ray_remote_args_fn=uses_fn
    )
    if pg is not None:
        op._placement_constraint = PlacementConstraint(placement_group=cast(Any, pg))
    sizer, _ = _bootstrapped(op, view=view, pg_bundles_by_id=pg_bundles_by_id)
    # Nothing running yet, so the whole cap is free.
    ledger = ConstraintAwareCapacityLedger(_snapshot_of(sizer))
    assert ledger.max_upscale_delta(op) == expected_cap
    assert build_op_profile(op).pg_managed


# --- warmup --------------------------------------------------------------------


def test_warmup_equal_share_targets():
    ops, topo = _linear_pipeline(running=1)
    # a has output, so b is not held at min. infer has none (its first output
    # ends warmup), so write, downstream of it, is held at min.
    ops["a"].metrics.num_task_outputs_generated = 1
    sizer = _make_sizer(topo, cpu=100.0, gpu=4.0)
    result = _tick(sizer)
    # min_reserved_cpu = 3 (a, b, write at min 1 x 1 cpu); 3 autoscaled cpu
    # ops. ceiling = (100 - 3) * 0.8 / 3 = 25.87 -> target 25 -> delta 24.
    for key in ("a", "b"):
        assert result[ops[key]].delta == 24, key
    assert ops["write"] not in result
    # Fixed GPU pool at size: no resize request.
    assert ops["infer"] not in result


@pytest.mark.parametrize("upstream_has_output", [False, True])
def test_warmup_holds_ops_at_min_until_upstream_produces_output(upstream_has_output):
    source = _FakeOp("ListFiles", _FakePool(min_size=1, max_size=500, cpu=1, running=1))
    op = _FakeOp(
        "MapBatches(count_rows)",
        _FakePool(min_size=1, max_size=500, cpu=1, running=1),
        inputs=(source,),
    )
    if upstream_has_output:
        source.metrics.num_task_outputs_generated = 1
    sizer, topo = _bootstrapped(source, op, cpu=100.0, gpu=0.0)

    result = _tick(sizer)

    # The source is never gated. Share: (100 - 2) * 0.8 / 2 = 39.2 -> 39.
    assert result[source].delta == 38
    if upstream_has_output:
        assert result[op].delta == 38
    else:
        assert op not in result


def test_upscale_is_capped_at_remaining_work_once_upstream_completes():
    """No more input can arrive: ceil(40 / 1.75) = 23 actors cover the
    backlog, so the op grows by 3, not to its full share of
    (100 - 1) x 0.8 = 79."""
    upstream = _FakeOp(
        "ListFiles",
        _FakePool(min_size=1, max_size=200, cpu=1, running=1),
        completed=True,
        execution_finished=True,
    )
    upstream.metrics.num_task_outputs_generated = 1
    op = _FakeOp(
        "MapBatches(count_rows)",
        _FakePool(min_size=1, max_size=500, cpu=1, running=20),
        inputs=(upstream,),
    )
    sizer, _ = _bootstrapped(upstream, op, enqueued=40, cpu=100.0, gpu=0.0)

    assert _tick(sizer)[op].delta == 3


@pytest.mark.parametrize(
    "upstream_completed,max_size,expected",
    [
        pytest.param(False, 500, 500, id="more-input-can-come"),
        # ceil(40 / 1.75) actors cover the backlog.
        pytest.param(True, 500, 23, id="backlog-bounds-it"),
        pytest.param(True, 10, 10, id="pool-max-is-lower"),
    ],
)
def test_upscale_ceiling_is_the_lower_of_pool_max_and_remaining_work(
    upstream_completed, max_size, expected
):
    """One ceiling bounds every upscale, bottleneck or per-op."""
    upstream = _FakeOp(
        "ListFiles",
        _FakePool(min_size=1, max_size=200, cpu=1, running=1),
        completed=upstream_completed,
        execution_finished=upstream_completed,
    )
    op = _FakeOp(
        "Map",
        _FakePool(min_size=1, max_size=max_size, cpu=1, running=20),
        inputs=(upstream,),
    )
    topo = _topology(upstream, op, enqueued=40)
    assert upscale_ceiling(op=op, op_state=topo[op]) == expected


def test_upscale_ceiling_without_op_state_is_the_pool_max():
    upstream = _FakeOp("ListFiles", _FakePool(min_size=1, max_size=200, cpu=1))
    upstream.completed = True
    op = _FakeOp("Map", _FakePool(min_size=1, max_size=500, cpu=1), inputs=(upstream,))
    assert upscale_ceiling(op=op, op_state=None) == 500


def test_warmup_share_survives_float_division():
    # (4.1 - 0.1) * 0.8 / 0.1 is 31.999... in floating point; truncating it
    # would leave the pool one actor short of its 32-actor share.
    op = _FakeOp("op", _FakePool(min_size=1, max_size=100, cpu=0.1, running=1))
    sizer, topo = _bootstrapped(op, cpu=4.1, gpu=0.0)
    assert _tick(sizer)[op].delta == 31


def test_warmup_pg_ops_share_their_placement_group():
    """PG ops size against their placement group, not the allocation: two
    autoscaled ops in one PG split its bundles like non-PG ops split the
    allocation. They also stay out of the non-PG equal share, so the non-PG
    GPU op gets the whole slice."""
    pg = fake_pg("pg1")
    pg_bundles_by_id = {"pg1": [ExecutionResources(gpu=10)]}
    view = DatasetReservationView(
        reserved_by_node={"node0": ExecutionResources(cpu=8, gpu=8)},
        labels_by_node={"node0": {}},
    )
    pg_ops = []
    for name in ("embed", "rerank"):
        op = _FakeOp(
            name,
            _FakePool(min_size=1, max_size=40, gpu=1, running=1),
            node=None,
            inputs=pg_ops[-1:],
        )
        op._placement_constraint = PlacementConstraint(placement_group=cast(Any, pg))
        pg_ops.append(op)
    plain = _FakeOp(
        "plain",
        _FakePool(min_size=1, max_size=40, gpu=1, running=1),
        inputs=pg_ops[-1:],
    )
    # Upstreams have output (no op held at min); plain keeps warmup going.
    for op in pg_ops:
        op.metrics.num_task_outputs_generated = 1
    sizer, _ = _bootstrapped(
        *pg_ops, plain, view=view, pg_bundles_by_id=pg_bundles_by_id
    )
    result = sizer.how_many()
    # (10 GPU - 2 floors) * 0.8 / 2 = 3.2 -> target 3 -> +2 each.
    for op in pg_ops:
        assert result[op].delta == 2, op.name
    # Non-PG share: (8 - 1) * 0.8 / 1 = 5.6 -> target 5 -> +4.
    assert result[plain].delta == 4


def test_warmup_is_upscale_only_and_holds_while_pending():
    # Above the warmup target: warmup must not downscale.
    big = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, running=90))
    sizer, topo = _bootstrapped(big, cpu=100.0)
    assert big not in _tick(sizer)

    # A granted upscale that is still pending is not re-requested.
    op = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, running=1))
    sizer, topo = _bootstrapped(op, cpu=100.0)
    delta = _tick(sizer)[op].delta
    assert delta > 0
    op._pool.pending = delta
    assert op not in _tick(sizer)


def test_critical_op_discovery_walks_past_ops_that_never_report_task_outputs():
    """Regression: the walk used to stop at the first non-sink operator,
    whatever it was. Limit, union, mix and the input buffer launch no tasks, so
    their num_task_outputs_generated is always 0 -- latching on one pinned the
    sizer in feedforward warmup for the entire run."""
    source = _FakeOp("Map", _FakePool(min_size=1, max_size=4, cpu=1))
    limit = _FakeNonPoolOp("Limit", inputs=[source])
    topo = _topology(source, limit)
    _, waits_for_flow_after_tick = _warmup_probe(topo)

    assert critical_ops(topo) == (source,)
    assert waits_for_flow_after_tick()
    source.metrics.num_task_outputs_generated = 1
    assert not waits_for_flow_after_tick()


def test_latch_waits_for_every_branch_of_a_union():
    """A union has two inputs, so there is no single deepest operator. Ending
    warmup while one branch is still cold would size that branch by feedback
    against an empty queue and drain it."""
    left = _FakeOp("Left", _FakePool(min_size=1, max_size=4, cpu=1))
    right = _FakeOp("Right", _FakePool(min_size=1, max_size=4, cpu=1))
    union = _FakeNonPoolOp("Union", inputs=[left, right])
    topo = _topology(left, right, union)
    _, waits_for_flow_after_tick = _warmup_probe(topo)

    assert set(critical_ops(topo)) == {left, right}
    left.metrics.num_task_outputs_generated = 1
    assert waits_for_flow_after_tick()  # right is still cold
    right.metrics.num_task_outputs_generated = 1
    assert not waits_for_flow_after_tick()


def _first_input_arrives(op: _FakeOp) -> None:
    op.pending_first_input = False


def _completes(op: _FakeOp) -> None:
    op.completed = True


@pytest.mark.parametrize(
    "release",
    [
        pytest.param(_first_input_arrives, id="input-arrives"),
        pytest.param(_completes, id="op-completes"),
    ],
)
def test_latch_waits_for_ops_awaiting_first_input(release):
    """A write held at zero actors until its first input is not a critical
    op, but warmup must not end before it can scale: otherwise the warm phase
    sizes its upstream from feedback while the write has no actors at all. An
    op that completes without ever getting input does not hold warmup open."""
    read = _FakeOp("Read", _FakePool(min_size=1, max_size=4, cpu=1))
    write = _write_op(
        "Write",
        _FakePool(min_size=1, max_size=4, cpu=1),
        inputs=[read],
        awaiting_first_input=True,
    )
    topo = _topology(read, write)
    _, waits_for_flow_after_tick = _warmup_probe(topo)

    assert critical_ops(topo) == (read,)
    read.metrics.num_task_outputs_generated = 1
    assert waits_for_flow_after_tick()  # write has no input yet
    release(write)
    assert not waits_for_flow_after_tick()


def test_tick_cadence_gate_and_idle_phases():
    ops, topo = _linear_pipeline(running=1)
    sizer = _make_sizer(topo)
    snapshot = sizer.next_tick(force=False)  # the first tick is never gated
    assert snapshot is not None
    assert snapshot.nodes_to_reclaim == {}
    # Every real tick's snapshot carries the order every step walks
    # operators in.
    assert snapshot.ordered_ops == tuple(
        ordered_actor_pool_ops(
            topo,
            OpProfiles(topo).actor_pool_ops,
            snapshot.schedulable_nodes_by_op,
        )
    )
    # Inside the cadence window there is no snapshot, so the scheduling loop
    # makes no sizing decision. Nothing is resized, and pool upkeep is the
    # executor's job, not the sizer's.
    assert sizer.next_tick(force=False) is None
    assert all(op._pool.scaled == [] for op in topo)
    assert all(op.actor_pool.upkeep_calls == 0 for op in topo)


# --- below-min, fixed and completed pools --------------------------------------


@pytest.mark.parametrize(
    "running,expected_delta",
    [
        pytest.param(2, 2, id="below-min-replenished"),
        pytest.param(4, None, id="at-size-never-resized"),
    ],
)
def test_fixed_pool_replenished_below_min_only(running: int, expected_delta):
    fixed = _FakeOp("Infer", _FakePool(min_size=4, max_size=4, gpu=1, running=running))
    sizer, topo = _bootstrapped(fixed)
    result = _tick(sizer)
    if expected_delta is None:
        assert fixed not in result
    else:
        req = result[fixed]
        assert (req.delta, req.reason) == (expected_delta, "below min size")


def test_below_min_grants_full_min_in_order():
    # Each below-min op is granted its FULL min (clamped to placeable
    # capacity), in the sizer's ordering. On a full cluster a large downstream
    # pool processed first can consume everything and leave the source at 0.
    src = _FakeOp("ReadRange", _FakePool(min_size=1, max_size=1, cpu=1, running=0))
    producer = _FakeOp(
        "MapBatches(producer)",
        _FakePool(min_size=30, max_size=30, cpu=1, running=0),
        inputs=(src,),
    )
    consumer = _FakeOp(
        "MapBatches(consumer)",
        _FakePool(min_size=1, max_size=1, cpu=1, running=0),
        inputs=(producer,),
    )
    sizer, topo = _bootstrapped(src, producer, consumer, cpu=16)
    result = _tick(sizer)
    # Consumer (sink-most) then producer are granted; producer's min 30 is
    # clamped to the remaining capacity, exhausting the cluster.
    assert result[consumer].delta == 1
    assert result[producer].delta == 15
    assert src not in result or result[src].delta == 0


@pytest.mark.parametrize(
    "cluster_cpu,expected_placed",
    [
        # Autoscale-from-zero: nothing placeable.
        pytest.param(0, 0, id="zero-capacity"),
        # Part of the min fits: that part is requested.
        pytest.param(2, 2, id="partial-placement"),
    ],
)
def test_below_min_requests_what_fits(cluster_cpu: float, expected_placed: int):
    src = _FakeOp("ReadRange", _FakePool(min_size=1, max_size=1, cpu=1, running=0))
    mapper = _FakeOp(
        "Map(sleep_task)",
        _FakePool(min_size=4, max_size=8, cpu=1, running=0),
        inputs=(src,),
    )
    sizer, topo = _bootstrapped(src, mapper, cpu=cluster_cpu, gpu=0)
    result = _tick(sizer)
    placed = sum(max(0, req.delta) for req in result.values())
    assert placed == expected_placed


def test_below_min_constrained_op_granted_full_min():
    """A label-constrained below-min op is granted its FULL min from its
    eligible nodes; unstarted unconstrained peers must not be reserved a slot
    out of the constrained op's private (scarce) capacity."""
    alloc = {
        "picked": ExecutionResources(cpu=8),
        "n2": ExecutionResources(cpu=8),
        "n3": ExecutionResources(cpu=8),
    }
    labels = {"picked": {_NODE_ID_KEY: "picked"}, "n2": {}, "n3": {}}
    view = DatasetReservationView(reserved_by_node=alloc, labels_by_node=labels)

    # Pinned op: min 4 x 2 CPUs = exactly the picked node.
    peer1 = _FakeOp("peer1", _FakePool(min_size=1, max_size=10, cpu=1), node=None)
    pinned = _FakeOp(
        "pinned",
        _FakePool(min_size=4, max_size=4, cpu=2),
        inputs=[peer1],
        node=None,
        ray_remote_args={"label_selector": {_NODE_ID_KEY: "picked"}},
    )
    peer2 = _FakeOp(
        "peer2", _FakePool(min_size=1, max_size=10, cpu=1), inputs=[pinned], node=None
    )
    peer3 = _FakeOp(
        "peer3", _FakePool(min_size=1, max_size=10, cpu=1), inputs=[peer2], node=None
    )
    sizer, topo = _bootstrapped(peer1, pinned, peer2, peer3, view=view)
    req = _tick(sizer)
    assert req[pinned].delta == 4
    for peer in (peer1, peer2, peer3):
        assert req[peer].delta >= 1


def test_completed_op_released_with_force():
    pool = _FakePool(min_size=1, max_size=500, cpu=1, running=10)
    done = _FakeOp("ReadFiles", pool, completed=True, execution_finished=True)
    sizer, topo = _bootstrapped(done)
    for _ in range(3):
        # The request keeps firing so attrition continues.
        req = _tick(sizer)[done]
        assert (req.delta, req.force) == (-10, True)
    pool.running = 4  # attrition removed 6 idle actors
    assert _tick(sizer)[done].delta == -4


def test_source_op_not_released_at_startup():
    # A source op with an empty input queue must warm up, never be treated as
    # done and force-released.
    src = _FakeOp("ReadFiles", _FakePool(min_size=1, max_size=500, cpu=1, running=4))
    sizer, topo = _bootstrapped(src, cpu=100.0)
    req = _tick(sizer)[src]
    assert req.delta > 0 and not req.force


# --- how_many after warmup -------------------------------------------------------


def test_backlog_actor_target_formula():
    # ceil((in_flight + ceil(enqueued/avg)) / (max_concurrency * 1.75)).
    op = _FakeOp(
        "A", _FakePool(min_size=1, max_size=500, cpu=1, running=10, in_flight=20)
    )
    op._fake_metrics.average_num_inputs_per_task = 2
    topo = _topology(op, enqueued=40)  # 20 tasks; total 40
    assert backlog_actor_target(op=op, op_state=topo[op]) == 23


def test_label_aware_caps_constrained_op_to_its_eligible_nodes():
    alloc = {
        "n1": ExecutionResources(cpu=16),
        "g1": ExecutionResources(gpu=4),
        "g2": ExecutionResources(gpu=4),
        "g3": ExecutionResources(gpu=4),  # L4 -- must NOT be used by the A100 op
    }
    labels = {
        "n1": {},
        "g1": {"accelerator-type": "A100"},
        "g2": {"accelerator-type": "A100"},
        "g3": {"accelerator-type": "L4"},
    }
    view = DatasetReservationView(reserved_by_node=alloc, labels_by_node=labels)

    read = _FakeOp(
        "read", _FakePool(min_size=1, max_size=500, cpu=1, running=1), node=None
    )
    embed = _FakeOp(
        "embed",
        _FakePool(min_size=10, max_size=500, gpu=1, running=0),
        inputs=[read],
        node=None,
        ray_remote_args={"label_selector": {"accelerator-type": "A100"}},
    )
    sizer, topo = _bootstrapped(read, embed, enqueued=10_000, view=view, warm=True)
    read._pool.in_flight = 50

    # embed matches only the 2 A100 nodes; read requests CPU, so it is only
    # eligible where CPU is allocated (the GPU-only nodes have none).
    snap = _snapshot_of(sizer)
    assert set(snap.schedulable_nodes_by_op[embed]) == {"g1", "g2"}
    assert set(snap.schedulable_nodes_by_op[read]) == {"n1"}

    # embed wants its min of 10 but only 8 A100 GPUs are free -> capped at 8.
    req = _tick(sizer)
    assert req[embed].delta == 8


def test_warns_once_when_no_node_can_hold_an_op_with_queued_input(
    caplog, propagate_logs
):
    """An op with queued input and no actors, whose label selector no node
    matches, gets nothing from the sizer and stalls the pipeline. The sizer
    warns about it once, and again only if it gets stuck a second time."""

    def view(labels):
        return DatasetReservationView(
            reserved_by_node={"n1": ExecutionResources(cpu=8, memory=32 * GiB)},
            labels_by_node={"n1": labels},
        )

    read = _FakeOp(
        "read", _FakePool(min_size=1, max_size=4, cpu=1, running=1), node=None
    )
    gen = _FakeOp(
        "gen_data",
        _FakePool(min_size=1, max_size=4, cpu=1, running=0),
        inputs=[read],
        node=None,
        ray_remote_args={"label_selector": {"node-pool": "cpu"}},
    )
    sizer, topo = _bootstrapped(read, gen, enqueued=177, view=view({}))

    def warnings():
        return [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING and "gen_data" in r.getMessage()
        ]

    with caplog.at_level(logging.WARNING):
        assert gen not in _tick(sizer)
        _tick(sizer)
    (message,) = warnings()
    assert "177 queued input block(s)" in message
    assert "{'node-pool': 'cpu'}" in message
    # read has an actor, so it is not warned about.
    assert not any("read has" in r.getMessage() for r in caplog.records)

    # The label appears: gen_data can be placed again, and is.
    sizer.reporter.view = view({"node-pool": "cpu"})
    assert _tick(sizer)[gen].delta >= 1

    # It disappears again: a second warning.
    sizer.reporter.view = view({})
    with caplog.at_level(logging.WARNING):
        _tick(sizer)
    assert len(warnings()) == 2


def test_reclaimable_actors_do_not_debit_free_capacity_for_later_ops():
    # A reclaim-heavy op processed first must not over-debit the per-node
    # ledger: only genuinely-new actors consume free capacity, so a later op
    # contending for the same node still gets its full grant.
    view = DatasetReservationView(
        reserved_by_node={"n1": ExecutionResources(cpu=10)}, labels_by_node={"n1": {}}
    )
    up = _FakeOp(
        "up",
        _FakePool(min_size=24, max_size=500, cpu=1, running=16, in_flight=1),
        node=None,
    )
    sink = _FakeOp(
        "sink",
        _FakePool(min_size=15, max_size=500, cpu=1, running=10, in_flight=1),
        inputs=[up],
        node=None,
        reclaimable=3,  # sink (processed first, sink-most) reclaims these for free
    )
    sizer, topo = _bootstrapped(up, sink, enqueued=100_000, view=view, warm=True)
    req = _tick(sizer)
    # sink: 5 below min, of which 3 are reclaimed -> only 2 consume free,
    # leaving 8 CPU. up: 8 below min, and it gets all 8.
    assert req[sink].delta == 5
    assert req[up].delta == 8


# --- capacity ledger -----------------------------------------------------------


class _LedgerOp:
    """Duck-typed op the ledger reads its per-actor shape and reclaimable
    count off."""

    def __init__(self, per_actor=None, reclaimable=0):
        shape = per_actor or ExecutionResources(cpu=1)
        self.actor_pool = SimpleNamespace(
            per_actor_resource_usage=lambda: shape,
            num_reclaimable_draining_actors=lambda: reclaimable,
        )


def _snapshot(
    free,
    schedulable_nodes_by_op,
    *,
    unknown_pg_headroom=None,
    pg_free=None,
    bundles=None,
):
    # None match = unconstrained = all nodes (matching_nodes never returns None).
    schedulable_nodes_by_op = {
        op: (frozenset(free) if match is None else match)
        for op, match in schedulable_nodes_by_op.items()
    }
    return ClusterStateSnapshot(
        free_by_node=free,
        reserved_by_node=dict(free),
        schedulable_nodes_by_op=schedulable_nodes_by_op,
        pg_free_by_bundle=dict(pg_free or {}),
        schedulable_bundles_by_op=dict(bundles or {}),
        unknown_pg_headroom_by_op=unknown_pg_headroom or {},
    )


def test_ledger_is_per_node_and_constraint_aware():
    # 2 nodes x 3 CPU, actor needs 2 CPU: per-node floor (1+1=2), NOT a global
    # floor(6/2)=3 -- fragmentation is visible.
    op = cast(Any, _LedgerOp(ExecutionResources(cpu=2)))
    free = {"a": ExecutionResources(cpu=3), "b": ExecutionResources(cpu=3)}
    ledger = ConstraintAwareCapacityLedger(_snapshot(free, {op: None}))
    assert ledger.max_upscale_delta(op) == 2

    # op1 eligible on all; op2 only on the shared GPU nodes. Granting op2
    # first consumes the shared nodes, so op1 then sees less.
    op1 = cast(Any, _LedgerOp(ExecutionResources(gpu=1)))
    op2 = cast(Any, _LedgerOp(ExecutionResources(gpu=1)))
    free = {
        "n1": ExecutionResources(cpu=8),
        "g1": ExecutionResources(gpu=4),
        "g2": ExecutionResources(gpu=4),
    }
    match = {op1: None, op2: frozenset({"g1", "g2"})}
    ledger = ConstraintAwareCapacityLedger(_snapshot(free, match))
    assert ledger.max_upscale_delta(op2) == 8  # 2 GPU nodes x 4
    ledger.grant(op2, 8)
    assert ledger.max_upscale_delta(op2) == 0
    assert ledger.max_upscale_delta(op1) == 0  # the GPU nodes are drained


def test_ledger_reclaimable_accounting():
    # A node with room for 4 actors + 3 draining actors this op can reclaim:
    # max_upscale_delta = 4 + 3. Granting the 3 reclaimable takes them back from
    # their held slots and consumes ZERO free, so a peer contending for the
    # same node still sees the full 4; granting past them debits free.
    op = cast(Any, _LedgerOp(ExecutionResources(cpu=1), reclaimable=3))
    peer = cast(Any, _LedgerOp(ExecutionResources(cpu=1)))
    free = {"n1": ExecutionResources(cpu=4)}
    ledger = ConstraintAwareCapacityLedger(_snapshot(free, {op: None, peer: None}))
    assert ledger.max_upscale_delta(op) == 7
    ledger.grant(op, 3)
    assert ledger.max_upscale_delta(peer) == 4
    ledger.grant(op, 5)  # 3 reclaimed + 2 new
    assert ledger.max_upscale_delta(peer) == 2


def test_ledger_pg_per_actor_ops_size_against_headroom():
    op = cast(Any, _LedgerOp(ExecutionResources(gpu=1)))
    ledger = ConstraintAwareCapacityLedger(
        _snapshot(
            {"n1": ExecutionResources(cpu=8)},
            {op: None},
            unknown_pg_headroom={op: 3},
        )
    )
    assert ledger.max_upscale_delta(op) == 3
    ledger.grant(op, 5)  # capped at remaining
    assert ledger.max_upscale_delta(op) == 0


def test_ledger_unknown_pg_ops_revive_draining_actors_on_top_of_headroom():
    # 2 revivable draining actors and room for 1 more under max_size (whose
    # headroom already counts the draining ones): reviving needs no new room.
    op = cast(Any, _LedgerOp(ExecutionResources(gpu=1), reclaimable=2))
    ledger = ConstraintAwareCapacityLedger(
        _snapshot({}, {op: None}, unknown_pg_headroom={op: 1})
    )
    assert ledger.max_upscale_delta(op) == 3
    ledger.grant(op, 2)  # both revived: the headroom is untouched
    assert ledger.unknown_pg_headroom[op] == 1
    ledger.grant(op, 3)  # 2 revived + 1 new: spends it
    assert ledger.unknown_pg_headroom[op] == 0


def test_ledger_ops_sharing_a_pg_spend_the_same_bundles():
    """4 bundles of {1 GPU, 4 CPU}. Alone, each op fits 4. Jointly they do
    not: every GPU actor also takes a CPU, leaving no bundle with the 4 CPU
    the other op's actor needs -- which a per-op actor cap cannot see."""
    gpu_op = cast(Any, _LedgerOp(ExecutionResources(cpu=1, gpu=1)))
    cpu_op = cast(Any, _LedgerOp(ExecutionResources(cpu=4)))
    keys = tuple(BundleKey("pg1", i) for i in range(4))
    ledger = ConstraintAwareCapacityLedger(
        _snapshot(
            {},
            {},
            pg_free={key: ExecutionResources(cpu=4, gpu=1) for key in keys},
            bundles={gpu_op: keys, cpu_op: keys},
        )
    )
    assert ledger.max_upscale_delta(gpu_op) == 4
    assert ledger.max_upscale_delta(cpu_op) == 4
    ledger.grant(gpu_op, 2)
    assert ledger.max_upscale_delta(cpu_op) == 2  # bundles 2 and 3 untouched
    ledger.grant(gpu_op, 2)
    assert ledger.max_upscale_delta(cpu_op) == 0


def test_ledger_grants_do_not_reach_where():
    """A grant is a count WHERE places later, so only the ledger's own map
    loses it; the snapshot's free map, which WHERE places into, keeps it.
    Later grants in the same tick see less room."""
    op = cast(Any, _LedgerOp(ExecutionResources(cpu=1)))
    snapshot = _snapshot({"n1": ExecutionResources(cpu=3)}, {op: None})
    ledger = ConstraintAwareCapacityLedger(snapshot)

    ledger.grant(op, 1)
    assert ledger.max_upscale_delta(op) == 2
    assert snapshot.free_by_node["n1"] == ExecutionResources(cpu=3)


def test_next_and_prev_actor_pool_ops_look_through_non_pool_ops():
    # read -> limit -> write: write is next for read, read is previous for
    # write; the limit in between has no actors and is skipped.
    read = _FakeOp("read", _FakePool(min_size=1, max_size=4, cpu=1))
    limit = _FakeNonPoolOp("limit", inputs=[read])
    write = _write_op("write", _FakePool(min_size=1, max_size=4, cpu=1), inputs=[limit])
    assert next_actor_pool_ops(read) == (write,)
    assert prev_actor_pool_ops(write) == (read,)
    assert next_actor_pool_ops(write) == ()
    assert prev_actor_pool_ops(read) == ()


# --- ordering and the tick snapshot -------------------------------------------


def _feat(
    i,
    selector=False,
    pg=False,
    gpu=False,
    fixed=False,
    large_memory=False,
    below_min=False,
    eligible=0,
):
    return OpOrderingSignals(
        topo_index=i,
        has_placement_group=pg,
        has_label_selector=selector,
        is_gpu=gpu,
        is_fixed_concurrency=fixed,
        is_large_memory=large_memory,
        below_min_size=below_min,
        num_schedulable_nodes=eligible,
    )


@pytest.mark.parametrize(
    "feats,expected",
    [
        # pg(tier 0) < embed(tier 1, selector) < gpu(tier 2) < fixed(tier 3)
        # < decode(tier 4, large memory) < write(tier 5, sink-most) < read.
        pytest.param(
            {
                "read": _feat(0),
                "decode": _feat(1, large_memory=True),
                "embed": _feat(2, selector=True, gpu=True),
                "fixed": _feat(3, fixed=True),
                "write": _feat(4),
                "pg": _feat(5, pg=True),
                "gpu": _feat(6, gpu=True),
            },
            ["pg", "embed", "gpu", "fixed", "decode", "write", "read"],
            id="tiers",
        ),
        # Below min_size goes ahead of placement groups; within it, ops keep
        # their tier order.
        pytest.param(
            {
                "pg": _feat(0, pg=True),
                "gpu": _feat(1, gpu=True),
                "starved_gpu": _feat(2, gpu=True, below_min=True),
                "starved_cpu": _feat(3, below_min=True),
            },
            ["starved_gpu", "starved_cpu", "pg", "gpu"],
            id="below-min-size-first",
        ),
        # Initial sizing: every op is below min. A labeled op still goes ahead
        # of a sink-most unconstrained op, which would otherwise take its nodes.
        pytest.param(
            {
                "read": _feat(0, below_min=True),
                "labeled": _feat(1, selector=True, fixed=True, below_min=True),
                "hog": _feat(2, below_min=True),
                "write": _feat(3, below_min=True),
            },
            ["labeled", "write", "hog", "read"],
            id="all-below-min-keeps-tiers",
        ),
        pytest.param(
            {"a": _feat(0, gpu=True), "b": _feat(1, gpu=True)},
            ["b", "a"],
            id="sink-most-within-tier",
        ),
        # Within the label tier, more specific (fewer eligible nodes) goes
        # first, overriding sink-most.
        pytest.param(
            {
                "in_ab": _feat(3, selector=True, eligible=4),
                "pin": _feat(0, selector=True, eligible=1),
                "pool": _feat(2, selector=True, eligible=2),
            },
            ["pin", "pool", "in_ab"],
            id="specificity-within-label-tier",
        ),
        # Specificity only breaks ties WITHIN a tier.
        pytest.param(
            {
                "pg": _feat(0, pg=True, eligible=100),
                "pin": _feat(1, selector=True, eligible=1),
                "gpu": _feat(2, gpu=True, eligible=1),
            },
            ["pg", "pin", "gpu"],
            id="specificity-does-not-cross-tiers",
        ),
    ],
)
def test_scarcity_first_ordering(feats, expected):
    assert ScarcityFirstOrdering().order(feats) == expected


@pytest.mark.parametrize(
    "pool_kwargs,large_memory,below_min",
    [
        # Default reservation is 4 GiB per CPU less the system's share: not large.
        pytest.param(dict(cpu=1, memory=3 * GiB), False, False, id="default-memory"),
        pytest.param(dict(cpu=1, memory=8 * GiB), True, False, id="large-memory"),
        pytest.param(dict(cpu=2, memory=8 * GiB), False, False, id="4gib-per-cpu"),
        pytest.param(dict(cpu=1, running=1), False, True, id="below-min"),
    ],
)
def test_ordering_signals_from_operator(pool_kwargs, large_memory, below_min):
    kwargs = dict(min_size=2, max_size=10, running=2)
    kwargs.update(pool_kwargs)
    op = _FakeOp("op", _FakePool(**kwargs))
    signals = OpOrderingSignals.from_operator(op, topo_index=0)
    assert signals.is_large_memory == large_memory
    assert signals.below_min_size == below_min


def test_ordered_ops_use_snapshot_eligibility_for_specificity():
    """With a tick snapshot, a node-pinned op (1 eligible node) is ordered
    before an overlapping pool selector (2 eligible nodes) even though
    sink-most alone would reverse them. Without a snapshot (bootstrap /
    initial sizing), ordering falls back to sink-most within the tier."""
    alloc = {"a1": ExecutionResources(cpu=8), "a2": ExecutionResources(cpu=8)}
    labels = {
        "a1": {_NODE_ID_KEY: "a1", "ray-pool": "pool_a"},
        "a2": {_NODE_ID_KEY: "a2", "ray-pool": "pool_a"},
    }
    view = DatasetReservationView(reserved_by_node=alloc, labels_by_node=labels)

    # pinned is UPSTREAM of pool: sink-most within the tier would put pool first.
    pinned = _FakeOp(
        "pinned",
        _FakePool(min_size=1, max_size=4, cpu=1),
        node=None,
        ray_remote_args={"label_selector": {_NODE_ID_KEY: "a1"}},
    )
    pool = _FakeOp(
        "pool",
        _FakePool(min_size=1, max_size=4, cpu=1),
        inputs=[pinned],
        node=None,
        ray_remote_args={"label_selector": {"ray-pool": "pool_a"}},
    )
    sizer, topo = _bootstrapped(pinned, pool, view=view)
    ops = OpProfiles(topo).actor_pool_ops

    assert ordered_actor_pool_ops(topo, ops) == [pool, pinned]
    snap = _snapshot_of(sizer)
    assert ordered_actor_pool_ops(topo, ops, snap.schedulable_nodes_by_op) == [
        pinned,
        pool,
    ]


def _labelled_cpu_nodes_and_gpu_nodes(num_cpu_nodes):
    """8-CPU nodes labelled ``node-pool: cpu``, where a label-selected op
    holds 7 of 8 CPUs, plus two 4-CPU / 4-GPU nodes. Returns (view, ops dict)
    with a GPU op (fixed 8) and a Write, both still awaiting their first
    input."""
    cpu_nodes = [f"c{i}" for i in range(num_cpu_nodes)]
    reserved = {n: ExecutionResources(cpu=8) for n in cpu_nodes}
    reserved.update({g: ExecutionResources(cpu=4, gpu=4) for g in ("g1", "g2")})
    labels = {n: {"node-pool": "cpu"} for n in cpu_nodes}
    labels.update({"g1": {}, "g2": {}})
    view = DatasetReservationView(reserved_by_node=reserved, labels_by_node=labels)
    gen = _FakeOp(
        "gen_data",
        _FakePool(min_size=1, max_size=100, cpu=1, running=7 * num_cpu_nodes),
        committed={n: ExecutionResources(cpu=7) for n in cpu_nodes},
        ray_remote_args={"label_selector": {"node-pool": "cpu"}},
    )
    gpu = _FakeOp(
        "FakeGPUInference",
        _FakePool(min_size=8, max_size=8, cpu=1, gpu=1),
        inputs=[gen],
        node=None,
        awaiting_first_input=True,
    )
    write = _write_op(
        "Write",
        _FakePool(min_size=1, max_size=100, cpu=1),
        inputs=[gpu],
        node=None,
        awaiting_first_input=True,
    )
    return view, {"gen": gen, "gpu": gpu, "write": write}


def test_contention_weighs_ops_by_how_few_nodes_they_can_use():
    """Two label-selected ops share 10 CPU nodes (2/10 each); only the GPU op
    can use the 2 GPU nodes (1/2 each). The GPU nodes are the scarce ones,
    though fewer ops can use them."""
    view, ops = _labelled_cpu_nodes_and_gpu_nodes(num_cpu_nodes=10)
    cpu_process = _FakeOp(
        "cpu_process",
        _FakePool(min_size=1, max_size=100, cpu=1),
        inputs=[ops["gen"]],
        node=None,
        ray_remote_args={"label_selector": {"node-pool": "cpu"}},
    )
    ops["gpu"]._input_dependencies = [cpu_process]
    sizer, _ = _bootstrapped(ops["gen"], cpu_process, ops["gpu"], view=view)
    contention = _snapshot_of(sizer).contention_by_node
    assert contention["c0"] == pytest.approx(0.2)
    assert contention["g1"] == pytest.approx(0.5)


def test_room_held_until_first_input_goes_to_its_op():
    """A GPU op holds the GPU nodes until its first input arrives. On that
    tick, a held Write must not top up its hold with those nodes' CPUs, or
    the GPU op never places an actor. The room goes to the GPU op, and Write
    keeps the nodes it held.

    If the GPU op's pending actors then expire, it is below its min size
    while Write is still held and short. Its min size comes before Write's
    hold, so Write still cannot take the GPU nodes' CPUs."""
    view, ops = _labelled_cpu_nodes_and_gpu_nodes(num_cpu_nodes=4)
    gpu, write = ops["gpu"], ops["write"]
    sizer, _ = _bootstrapped(*ops.values(), view=view)

    request = _tick(sizer)
    assert gpu not in request and write not in request
    assert _held(sizer, gpu).by_node == {"g1": 4, "g2": 4}
    write_held = dict(_held(sizer, write).by_node)
    # Only the CPU nodes' free CPU is left for Write.
    assert set(write_held) <= {"c0", "c1", "c2", "c3"}

    ops["gen"].metrics.num_task_outputs_generated = 1
    gpu.pending_first_input = False
    request = _tick(sizer)
    assert (request[gpu].delta, request[gpu].reason) == (
        8,
        "room held until its first input",
    )
    assert _held(sizer, write).by_node == write_held

    # The 8 pending actors expired without starting: the pool is still empty.
    request = _tick(sizer)
    assert (request[gpu].delta, request[gpu].reason) == (8, "below min size")
    assert _held(sizer, write).by_node == write_held


def test_held_ops_pack_scarcest_first():
    """Two held ops, the unconstrained one earlier in the topology. Packing in
    topology order let it spill onto the GPU nodes, leaving the GPU op no room
    to hold; packing scarcest first lets the GPU op claim them."""
    view, ops = _labelled_cpu_nodes_and_gpu_nodes(num_cpu_nodes=4)
    gen, gpu, write = ops["gen"], ops["gpu"], ops["write"]
    write._input_dependencies = [gen]
    sizer, _ = _bootstrapped(gen, write, gpu, view=view)

    _tick(sizer)
    assert _held(sizer, gpu).by_node == {"g1": 4, "g2": 4}
    assert set(_held(sizer, write).by_node) <= {"c0", "c1", "c2", "c3"}


def test_gpu_nodes_are_scarce_in_node_pressure():
    # A GPU op with NO label_selector still marks GPU nodes as scarce, so a
    # plain CPU op deprioritizes them; a CPU-only node stays unpressured.
    view = DatasetReservationView(
        reserved_by_node={
            "n1": ExecutionResources(cpu=16),
            "g1": ExecutionResources(cpu=8, gpu=4),
        },
        labels_by_node={"n1": {}, "g1": {}},
    )
    cpu_op = _FakeOp(
        "read", _FakePool(min_size=1, max_size=10, cpu=1, running=1), node=None
    )
    gpu_op = _FakeOp(
        "embed",
        _FakePool(min_size=1, max_size=10, gpu=1, running=1),
        inputs=[cpu_op],
        node=None,
    )
    sizer, topo = _bootstrapped(cpu_op, gpu_op, view=view)
    snap = _snapshot_of(sizer)
    assert snap.contention_by_node.get("g1", 0) == 1
    assert snap.contention_by_node.get("n1", 0) == 0


# --- reclaiming a shrunk allocation ------------------------------------------


def test_committed_and_free_by_node_exclude_pg_managed_ops():
    a = _FakeOp("a", _FakePool(min_size=1, max_size=10, cpu=1, running=2), node="n1")
    b = _FakeOp(
        "b", _FakePool(min_size=1, max_size=10, cpu=1, running=1), node="n1", inputs=[a]
    )
    c = _FakeOp(
        "c", _FakePool(min_size=1, max_size=10, cpu=1, running=3), node="n2", inputs=[b]
    )
    # These two are PG-managed, so their actors live outside our allocation.
    pg = _FakeOp(
        "pg",
        _FakePool(min_size=1, max_size=10, gpu=1, running=1),
        node="n1",
        inputs=[c],
    )
    pg._placement_constraint = PlacementConstraint(
        placement_group=cast(Any, fake_pg("pg1"))
    )
    fn = _FakeOp(
        "fn",
        _FakePool(min_size=1, max_size=10, gpu=1, running=1),
        node="n1",
        inputs=[pg],
        uses_ray_remote_args_fn=True,
    )
    labels = {"n1": {}, "n2": {}}
    view = DatasetReservationView(
        reserved_by_node={
            "n1": ExecutionResources(cpu=4),
            "n2": ExecutionResources(cpu=4),
        },
        labels_by_node=labels,
    )
    sizer, topo = _bootstrapped(a, b, c, pg, fn, view=view)

    committed = committed_by_node(OpProfiles(topo))
    assert committed["n1"].cpu == 3 and committed["n1"].gpu == 0
    assert committed["n2"].cpu == 3

    free = _snapshot_of(sizer).free_by_node
    assert free["n1"].cpu == 1 and free["n2"].cpu == 1

    # Over-commit clamps free at 0, never negative.
    tight = DatasetReservationView(
        reserved_by_node={
            "n1": ExecutionResources(cpu=1),
            "n2": ExecutionResources(cpu=4),
        },
        labels_by_node=labels,
    )
    # Over-committed n1 is a node to reclaim: it keeps its reserved slice
    # (accounting), but is not offered as free (anything placed there would be
    # evicted next tick).
    sizer.reporter.view = tight
    tight_snapshot = _snapshot_of(sizer)
    assert "n1" in tight_snapshot.nodes_to_reclaim
    assert set(tight_snapshot.reserved_by_node) == {"n1", "n2"}
    assert set(tight_snapshot.free_by_node) == {"n2"}


@pytest.mark.parametrize(
    "draining_cpu,expected_over",
    [
        # Our slice shrank 8 -> 6 and 2 actors are already draining for it:
        # nothing more to shed, on this tick or any later one.
        pytest.param(2, None, id="shrink-already-draining"),
        pytest.param(0, 2, id="shrink-not-yet-shed"),
    ],
)
def test_nodes_to_reclaim_nets_out_draining_actors(draining_cpu, expected_over):
    op = _FakeOp(
        "Map",
        _FakePool(min_size=1, max_size=10, cpu=1),
        committed={"n1": ExecutionResources(cpu=8)},
        draining={"n1": ExecutionResources(cpu=draining_cpu)} if draining_cpu else None,
    )
    alloc = {"n1": ExecutionResources(cpu=6)}
    reclaim = nodes_to_reclaim(alloc, OpProfiles(_topology(op)))
    if expected_over is None:
        assert reclaim == {}
    else:
        assert reclaim["n1"].cpu == expected_over


@pytest.mark.parametrize(
    "reserved_cpu, draining_cpu, expected_after_drains_cpu",
    [
        pytest.param(8, 2, 2, id="drain-frees-room"),
        # Over-committed: the drain only brings the node back to its slice,
        # so it frees nothing, not the 2 CPUs a plain `free + draining` gives.
        pytest.param(6, 2, 0, id="drain-covers-an-overage"),
        pytest.param(8, 0, 0, id="nothing-draining"),
    ],
)
def test_snapshot_free_after_drains(
    reserved_cpu, draining_cpu, expected_after_drains_cpu
):
    """``free_after_drains_by_node`` is a node's free capacity once its
    draining actors finish: the slice minus what stays committed."""
    op = _FakeOp(
        "Map",
        _FakePool(min_size=1, max_size=10, cpu=1),
        committed={"n1": ExecutionResources(cpu=8)},
        draining={"n1": ExecutionResources(cpu=draining_cpu)} if draining_cpu else None,
    )
    snapshot = ClusterStateSnapshot.build(
        DatasetReservationView(
            reserved_by_node={"n1": ExecutionResources(cpu=reserved_cpu)}
        ),
        OpProfiles(_topology(op)),
        seconds_since_allocation_change=0.0,
    )
    assert snapshot.free_by_node["n1"].cpu == 0
    assert snapshot.free_after_drains_by_node["n1"].cpu == expected_after_drains_cpu


def test_replacements_for_draining_actors_are_not_read_as_an_overage():
    """Draining actors and the pendings that replace them both count as
    committed until the drain finishes; netting out the draining ones keeps the
    replacements from being reclaimed (cancelled)."""
    donor = _FakeOp(
        "donor",
        _FakePool(min_size=1, max_size=10, cpu=1),
        committed={"n1": ExecutionResources(cpu=8)},
        draining={"n1": ExecutionResources(cpu=2)},
    )
    recipient = _FakeOp(
        "recipient",
        _FakePool(min_size=1, max_size=10, cpu=1),
        committed={"n1": ExecutionResources(cpu=2)},  # 2 targeted pendings
        inputs=[donor],
    )
    alloc = {"n1": ExecutionResources(cpu=8)}
    profiles = OpProfiles(_topology(donor, recipient))
    assert nodes_to_reclaim(alloc, profiles) == {}


def test_nodes_to_reclaim_over_allocation():
    """Per node, reclaim = committed - alloc, clamped >= 0. A node absent from
    alloc (our slice lost it) is fully over-allocated."""
    op = _FakeOp(
        "Map",
        _FakePool(min_size=1, max_size=10, cpu=1),
        committed={
            "n1": ExecutionResources(cpu=4),
            "n2": ExecutionResources(cpu=1),
            "n3": ExecutionResources(cpu=3),
        },
    )
    alloc = {"n1": ExecutionResources(cpu=2), "n2": ExecutionResources(cpu=4)}
    reclaim = nodes_to_reclaim(alloc, OpProfiles(_topology(op)))
    assert set(reclaim) == {"n1", "n3"}
    assert reclaim["n1"].cpu == 2
    assert reclaim["n3"].cpu == 3


def test_empty_allocation_reclaim_skepticism():
    # The coordinator client's first poll returns its INITIAL EMPTY cache while
    # the real RPC is in flight, so an empty allocation that has never been
    # non-empty is not "allocation shrank". After a real grant, a single empty
    # read may be transient: evictions fire only on the second consecutive
    # empty tick, and a non-empty read in between resets the counter.
    op = _FakeOp("Map", _FakePool(min_size=1, max_size=10, cpu=1, running=44))
    topo = _topology(op)
    sizer = _make_sizer(topo, cpu=100.0)
    grant = sizer.reporter.view
    empty = replace(grant, reserved_by_node={})

    def reclaim_for(view):
        sizer.reporter.view = view
        return _snapshot_of(sizer).nodes_to_reclaim

    assert reclaim_for(empty) == {}  # empty before the first grant
    reclaim_for(grant)  # non-empty grant seen
    assert reclaim_for(empty) == {}  # first empty read: skeptical
    assert reclaim_for(grant) == {}  # recovers; counter resets
    assert reclaim_for(empty) == {}
    assert "node0" in reclaim_for(empty)  # second consecutive empty


def _evictable_ops(*, cpu_running, cpu_pending=(), gpu_running=()):
    """A CPU op and a GPU op, both with actors on n1."""
    cpu_op = _FakeOp(
        "Cpu",
        _FakePool(min_size=1, max_size=10, cpu=1),
        view=_placement_view(
            per_actor=ExecutionResources(cpu=1),
            pending_ids_by_node={"n1": list(cpu_pending)} if cpu_pending else {},
            victim_ids_by_node={"n1": list(cpu_running)},
        ),
    )
    gpu_op = _FakeOp(
        "Gpu",
        _FakePool(min_size=1, max_size=10, gpu=1),
        inputs=[cpu_op],
        view=_placement_view(
            per_actor=ExecutionResources(gpu=1),
            victim_ids_by_node={"n1": list(gpu_running)},
        ),
    )
    return cpu_op, gpu_op, OpProfiles(_topology(cpu_op, gpu_op))


def test_resolve_evictions_cheapest_first_cross_op():
    """Actors are shed pending-then-running, across ops, only until each
    over-allocated node fits again; ops whose shape cannot reduce the overage
    (wrong dimension) are skipped."""
    cpu_op, gpu_op, profiles = _evictable_ops(
        cpu_running=["r1", "r2"], cpu_pending=["p1"], gpu_running=["g1"]
    )
    shed = resolve_evictions(
        nodes_to_reclaim={"n1": ExecutionResources(cpu=2)}, profiles=profiles
    )
    # 2 CPU over -> shed two of cpu_op's three on n1 (the pool takes the
    # pending first); gpu_op skipped.
    assert shed == {cpu_op: ["n1", "n1"]}


class _PickFirstNodes:
    """Downscale placement that sheds from "n1"."""

    def pick_downscale_nodes(self, view, *, count, favored_nodes):
        return ["n1"] * count


@pytest.mark.parametrize(
    "completed,delta,expected_pending,expected_nodes",
    [
        # Untargeted pendings hold no node's capacity: shed them first, then
        # let placement pick the node for the rest.
        pytest.param(False, -3, ("p1", "p2"), ("n1",), id="untargeted-first"),
        pytest.param(False, -1, ("p1",), (), id="untargeted-cover-it"),
        # A completed op releases everything, untargeted pendings included.
        pytest.param(True, -4, ("p1", "p2"), ("n1", "n1"), id="completed-release"),
    ],
)
def test_place_downscales_sheds_untargeted_pendings_by_id(
    completed, delta, expected_pending, expected_nodes
):
    op = _FakeOp(
        "pg_op", _FakePool(min_size=1, max_size=10, cpu=1), completed=completed
    )
    view = replace(
        _placement_view(
            per_actor=ExecutionResources(cpu=1),
            victim_ids_by_node={"n1": ["r1", "r2"]},
        ),
        untargeted_pending_ids=["p1", "p2"],
    )
    placed: Dict[Any, Any] = {}
    place_downscales(
        placed=placed,
        request={op: ActorPoolScalingRequest(delta=delta)},
        views={op: view},
        favored=set(),
        free={},
        placement=cast(Any, _PickFirstNodes()),
        evicted_ops=set(),
    )
    req = placed[op]
    assert req.delta == delta
    assert req.pending_ids == expected_pending
    assert req.nodes == expected_nodes


def _favored_nodes_on_gpu_nodes(unmet: int):
    """Four nodes with 8 CPUs and 1 GPU each. A GPU op (1 CPU + 1 GPU per
    actor) is ``unmet`` actors short. On g3 and g4 its GPU is free but CPU
    actors hold every CPU, so the ledger has no room for it there. CPUs
    freed on g1 or g2, whose GPU it already uses, never help it. Returns the
    nodes a CPU op's 4-actor downscale should favor, and the CPU op's view."""
    nodes = ("g1", "g2", "g3", "g4")
    free = {
        "g1": ExecutionResources(cpu=3, gpu=0),
        "g2": ExecutionResources(cpu=2, gpu=0),
        "g3": ExecutionResources(cpu=0, gpu=1),
        "g4": ExecutionResources(cpu=0, gpu=1),
    }
    gpu_op = _FakeOp(
        "Predictor",
        _FakePool(min_size=1, max_size=10, cpu=1, gpu=1),
        view=_placement_view(per_actor=ExecutionResources(cpu=1, gpu=1)),
    )
    cpu_view = _placement_view(
        per_actor=ExecutionResources(cpu=1),
        victim_ids_by_node={
            "g1": ["a", "b"],
            "g2": ["c"],
            "g3": ["d", "e"],
            "g4": ["f", "g"],
        },
    )
    cpu_op = _FakeOp(
        "preprocess", _FakePool(min_size=1, max_size=40, cpu=1), view=cpu_view
    )
    snapshot = ClusterStateSnapshot(
        schedulable_nodes_by_op={op: frozenset(nodes) for op in (gpu_op, cpu_op)},
        reserved_by_node={n: ExecutionResources(cpu=8, gpu=1) for n in nodes},
        free_by_node=free,
    )
    favored = compute_favored_nodes(
        request=cast(Any, {cpu_op: ActorPoolScalingRequest.downscale(delta=-4)}),
        # As the sizer passes it: only ops that are short.
        unmet=cast(Any, {gpu_op: unmet} if unmet > 0 else {}),
        views=cast(Any, {cpu_op: cpu_view}),
        capacity=snapshot,
        free=free,
        placement=LocalityBasedActorPlacement(),
        profiles=OpProfiles(_topology(cpu_op, gpu_op)),
        ordered_ops=(gpu_op, cpu_op),
    )
    return favored, cpu_view


def test_downscales_free_capacity_where_a_short_upscaler_fits():
    """Downscales remove CPU actors on g3 and g4, where the freed CPUs let
    the short GPU op place."""
    favored, cpu_view = _favored_nodes_on_gpu_nodes(unmet=2)
    assert favored == {"g3", "g4"}
    victims = LocalityBasedActorPlacement().pick_downscale_nodes(
        cpu_view, count=4, favored_nodes=favored
    )
    assert sorted(victims) == ["g3", "g3", "g4", "g4"]


def test_downscales_favor_no_node_when_nobody_is_short():
    favored, _ = _favored_nodes_on_gpu_nodes(unmet=0)
    assert favored == set()


def test_resolve_evictions_mixed_overage_takes_one_actor_per_dimension():
    """Over by {1 CPU, 1 GPU} with three CPU actors and one GPU actor: one of
    each goes. Once the CPU dimension is covered, the other CPU actors no
    longer free anything still over budget."""
    cpu_op, gpu_op, profiles = _evictable_ops(
        cpu_running=["r1", "r2", "r3"], gpu_running=["g1"]
    )
    shed = resolve_evictions(
        nodes_to_reclaim={"n1": ExecutionResources(cpu=1, gpu=1)}, profiles=profiles
    )
    assert shed == {cpu_op: ["n1"], gpu_op: ["n1"]}


# --- profiles and telemetry ----------------------------------------------------


@pytest.mark.parametrize(
    "pool_kwargs,is_write,has_pg,pinned,expected",
    [
        # A Write sink has no special cap: its pool cap is its max_size.
        pytest.param(
            dict(max_size=500, cpu=1),
            True,
            False,
            None,
            dict(pool_cap=500, resource_type="cpu", pg_managed=False),
            id="write-sink-uses-max-size",
        ),
        # A GPU pool is GPU-currency and keeps its own max_size as the cap.
        pytest.param(
            dict(max_size=8, gpu=1),
            False,
            False,
            None,
            dict(pool_cap=8, resource_type="gpu"),
            id="gpu-resource-type",
        ),
        # An unbounded pool keeps an integral cap.
        pytest.param(
            dict(max_size=float("inf"), cpu=1),
            False,
            False,
            None,
            dict(pool_cap=sys.maxsize),
            id="unbounded-pool",
        ),
        pytest.param(
            dict(max_size=4, cpu=1),
            False,
            True,
            None,
            dict(pg_managed=True),
            id="pg-op",
        ),
    ],
)
def test_build_op_profile(pool_kwargs, is_write, has_pg, pinned, expected):
    op = _FakeOp(
        "Op",
        _FakePool(min_size=1, running=1, **pool_kwargs),
        logical_ops=(Write.__new__(Write),) if is_write else (),
        ray_remote_args={"label_selector": {_NODE_ID_KEY: pinned}} if pinned else None,
    )
    if has_pg:
        op._placement_constraint = PlacementConstraint(
            placement_group=cast(Any, fake_pg("pg1"))
        )
    profile = build_op_profile(op)
    for field, value in expected.items():
        assert getattr(profile, field) == value, field
    if pinned:
        assert op.placement_constraint().label_selector == {_NODE_ID_KEY: pinned}
    # One profile per op, cached.
    profiles = OpProfiles(_topology(op))
    assert profiles.profile(op) is profiles.profile(op)


class _PhaseRecorder:
    """Stands in for ``SizerMetrics``: keeps each tick phase's duration."""

    def __init__(self):
        self.phases: List[Tuple[str, float]] = []

    def record_tick_duration(self, phase: str, seconds: float) -> None:
        self.phases.append((phase, seconds))

    def record_actor_delta(self, *, operator: str, phase: str, delta: int) -> None:
        pass


def _recording_telemetry() -> Tuple[SizerTelemetry, List[Tuple[str, float]]]:
    """A telemetry whose metrics land in the returned list."""
    recorder = _PhaseRecorder()
    return SizerTelemetry("test-ds", metrics=cast(Any, recorder)), recorder.phases


def test_log_decisions_names_each_ops_status_rule_and_reason(caplog, propagate_logs):
    """One debug line per tick says why each op is sized as it is."""
    grown = _FakeOp("Grown", _FakePool(min_size=1, max_size=10, cpu=1))
    held = _FakeOp("Held", _FakePool(min_size=1, max_size=10, cpu=1))
    shed = _FakeOp("Shed", _FakePool(min_size=1, max_size=10, cpu=1))
    short = _FakeOp("Short", _FakePool(min_size=1, max_size=10, cpu=1))
    decisions = {
        grown: OpDecision(
            OpRule.UPSCALE_BOTTLENECK,
            request=ActorPoolScalingRequest.upscale(delta=2, reason="bottleneck"),
            unmet=1,
        ),
        short: OpDecision(OpRule.UPSCALE_BUSY, unmet=3),
        held: OpDecision(OpRule.NONE),
        shed: OpDecision(
            OpRule.DOWNSCALE_IDLE,
            request=ActorPoolScalingRequest.downscale(delta=-2, reason="idle"),
        ),
    }
    statuses = {
        grown: Active(),
        short: Active(),
        held: Held(reserved=4),
        shed: Active(),
    }
    with caplog.at_level(logging.DEBUG):
        SizerTelemetry("test-ds").log_decisions(
            phase=SizerPhase.STEADY,
            decisions=cast(Any, decisions),
            statuses=cast(Any, statuses),
        )
    (line,) = [r.getMessage() for r in caplog.records if "tick:" in r.getMessage()]
    assert line == (
        "OperatorSizer tick: phase=steady | "
        "Grown[active]: upscale_bottleneck +2 (bottleneck) (no room for 1); "
        "Short[active]: upscale_busy (no room for 3); Held[held]: none; "
        "Shed[active]: downscale_idle -2 (idle)"
    )


def test_record_tick_emits_and_clears_collected_timings():
    """Timings add up per phase until ``record_tick``, which emits them even
    on a tick that sized nothing, then starts the next tick empty."""
    telemetry, observed = _recording_telemetry()
    telemetry.add_timing_phase("build", 0.001)
    telemetry.add_timing_phase("how_many", 0.002)
    telemetry.add_timing_phase("build", 0.003)  # a later step's build adds up

    telemetry.record_tick(topology={}, decisions={}, where=PlacedRequests(by_op={}))
    assert observed == [("build", pytest.approx(0.004)), ("how_many", 0.002)]

    observed.clear()
    telemetry.record_tick(topology={}, decisions={}, where=PlacedRequests(by_op={}))
    assert observed == []


def test_timed_adds_each_call_to_its_phase():
    """``@timed`` adds every call's wall time to the owner's telemetry under
    its phase, returns the method's result, and records a call that raises
    too."""
    telemetry, observed = _recording_telemetry()

    class _Owner:
        def __init__(self):
            self._telemetry = telemetry

        @timed("work")
        def work(self, x):
            return x * 2

        @timed("fail")
        def fail(self):
            raise ValueError("boom")

    owner = _Owner()
    assert owner.work(2) == 4
    owner.work(3)
    with pytest.raises(ValueError):
        owner.fail()
    telemetry.record_tick(topology={}, decisions={}, where=PlacedRequests(by_op={}))
    assert [phase for phase, _ in observed] == ["work", "fail"]
    assert all(seconds >= 0 for _, seconds in observed)


# --- the operator's placement surface -----------------------------------------


def _actor_state(in_flight, *, ts=0.0, status=ActorStatus.ACTIVE):
    return SimpleNamespace(
        num_tasks_in_flight=in_flight,
        latest_task_submission_ts=ts,
        status=status,
    )


def test_build_op_placement_view():
    """Counts are identical whether or not victim order is requested; only the
    full build carries the cheapest-drain-first victim lists. Neighbor counts
    exclude only DRAINING actors: pendings (during warmup every neighbor
    is still pending) stay counted for collocation."""
    upstream = _FakeOp("up", _FakePool(min_size=1, max_size=4, cpu=1))
    upstream.actor_counts = {
        "n1": Counter(
            {
                ActorStatus.ACTIVE: 1,
                ActorStatus.IDLE: 1,
                ActorStatus.DRAINING: 1,
            }
        ),
        "n2": Counter({ActorStatus.PENDING: 2}),
    }
    pool = SimpleNamespace(
        alive_actor_states_by_node=lambda: {
            # n1: one idle, one busy, one draining (excluded everywhere).
            "n1": {
                "a": _actor_state(0),
                "b": _actor_state(3),
                "t": _actor_state(1, status=ActorStatus.DRAINING),
            },
            "n2": {"c": _actor_state(0), "d": _actor_state(0)},
            # n3: only draining actors -> absent from every map.
            "n3": {"t2": _actor_state(2, status=ActorStatus.DRAINING)},
        },
        pending_ids_by_target_node=lambda: {"n2": ["p1"]},
        untargeted_pending_ids=lambda: ["u1"],
        per_actor_resource_usage=lambda: ExecutionResources(cpu=1),
        max_tasks_in_flight_per_actor=lambda: 4,
    )
    op = SimpleNamespace(
        actor_pool=pool,
        # Drain cost reads the bank's outstanding output blocks: none here.
        resource_bank=SimpleNamespace(live_object_store=lambda **kw: LiveObjStore()),
        id="op1",
        data_context=SimpleNamespace(target_max_block_size=10 * MiB),
        metrics=SimpleNamespace(
            bytes_inputs_received=300, bytes_task_outputs_generated=100
        ),
        input_dependencies=[upstream],
        output_dependencies=[],
        input_queue_bytes_by_node=lambda: {"n1": 42},
    )

    full = _build_op_placement_view(cast(Any, op), rank_victims=True)
    counts = _build_op_placement_view(cast(Any, op), rank_victims=False)

    assert counts.actors_by_node == full.actors_by_node == {"n1": 2, "n2": 3}
    assert counts.idle_actors_by_node == full.idle_actors_by_node == {"n1": 1, "n2": 2}
    assert counts.pending_ids_by_node == full.pending_ids_by_node == {"n2": ["p1"]}
    # Untargeted pendings belong to no node: listed by id, not counted per node.
    assert counts.untargeted_pending_ids == full.untargeted_pending_ids == ["u1"]
    assert full.victim_ids_by_node == {"n1": ["a", "b"], "n2": ["c", "d"]}
    assert full.victim_drain_cost_by_node == {
        "n1": [(0, 0), (3, 0)],
        "n2": [(0, 0), (0, 0)],
    }
    assert counts.victim_ids_by_node == {}
    # Neighbors: the upstream actor-pool op makes this a non-source, and its
    # alive count per node excludes only the draining actor.
    assert full.is_source is False
    # Bytes-per-actor for collocation = tasks-in-flight cap x target block size.
    assert full.input_bytes_per_actor == 4 * 10 * MiB
    assert dict(full.upstream_actors_by_node) == {"n1": 2, "n2": 2}
    assert dict(full.downstream_actors_by_node) == {}
    # Edge weights follow the observed byte flow: 300 in / 100 out.
    assert (full.upstream_edge_weight, full.downstream_edge_weight) == (0.75, 0.25)


def test_reclaimable_node_ids_follow_reclaim_order():
    """reclaimable_node_ids(limit=k) returns the nodes of the first k draining
    actors in the order reclaim_draining_actors reclaims them -- busiest first,
    then drain-start order -- so the sizer's pre-placement view adjustment
    matches what apply does."""
    pool = SimpleNamespace(
        _draining_actors={
            "a1": _DrainingActorInfo(since=0.0, reason=DrainReason.RESIZE),
            "a2": _DrainingActorInfo(since=0.0, reason=DrainReason.RESIZE),
            "a3": _DrainingActorInfo(since=0.0, reason=DrainReason.RESIZE),
        },
        _running_actors={
            "a1": SimpleNamespace(actor_location="n1", num_tasks_in_flight=0),
            "a2": SimpleNamespace(actor_location="n2", num_tasks_in_flight=0),
            "a3": SimpleNamespace(actor_location="n3", num_tasks_in_flight=2),
        },
        _num_outstanding_outputs=lambda state: 0,
    )
    pool._reclaimable_draining_actors = (
        lambda: _ExperimentalActorPool._reclaimable_draining_actors(cast(Any, pool))
    )
    reclaimable_node_ids = _ExperimentalActorPool.reclaimable_node_ids
    # a3 is busiest; a1 and a2 tie and keep drain-start order.
    assert reclaimable_node_ids(cast(Any, pool), limit=2) == ["n3", "n1"]
    assert reclaimable_node_ids(cast(Any, pool), limit=99) == ["n3", "n1", "n2"]
    # Actors losing their node (non-reclaimable) are skipped.
    pool._draining_actors["a3"] = _DrainingActorInfo(
        since=0.0, reason=DrainReason.ALLOC_SHRINK
    )
    assert reclaimable_node_ids(cast(Any, pool), limit=99) == ["n1", "n2"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
