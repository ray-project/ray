"""Unit tests for ``OperatorSizer`` and the phase components it sequences:
bootstrap validation and initial sizing, the warmup and warm sizing arms,
capacity granting (the ledger), scarcity ordering, constraint matching and
placement-group caps, allocation-shrink reclaim, and the operator's
placement-view surface. Duck-typed operator/pool doubles over a fake
reporter and clock; no cluster."""
import logging
import sys
from collections import Counter
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, Dict, Optional, cast

import pytest

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import (
    SIZER_CADENCE_S,
    SIZER_DOWNSCALE_HYSTERESIS,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.interfaces.physical_operator import (
    PhysicalOperator,
)
from ray.data._internal.execution.resource_bank import LiveObjStore
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    ActorOnlyResourceReporter,
    DatasetAllocationView,
)
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
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
    pack_actors,
)
from ray.data._internal.experimental.execution.sizer.capacity_ledger import (
    ConstraintAwareCapacityLedger,
)
from ray.data._internal.experimental.execution.sizer.locality_actor_placement import (
    LocalityBasedActorPlacement,
)
from ray.data._internal.experimental.execution.sizer.models import (
    OpShortfall,
    PerOpSizingRequest,
    PlacedSizingRequest,
    TickCapacitySnapshot,
    TickContext,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
    build_op_profile,
)
from ray.data._internal.experimental.execution.sizer.operator_ordering import (
    OpOrderingSignals,
    ScarcityFirstOrdering,
    ordered_actor_pool_ops,
)
from ray.data._internal.experimental.execution.sizer.operator_sizer import OperatorSizer
from ray.data._internal.experimental.execution.sizer.optimizer import PipelineOptimizer
from ray.data._internal.experimental.execution.sizer.optimizer.rule import (
    OptimizationRule,
)
from ray.data._internal.experimental.execution.sizer.pg_bundles import (
    PlacementGroupBundles,
)
from ray.data._internal.experimental.execution.sizer.scale_how_many import (
    OpSizingPolicy,
)
from ray.data._internal.experimental.execution.sizer.scale_where import (
    place_downscales,
    resolve_evictions,
)
from ray.data._internal.experimental.execution.sizer.scaling_guard import OpScalingGuard
from ray.data._internal.experimental.execution.sizer.signals import BoolStreak
from ray.data._internal.experimental.execution.sizer.telemetry import (
    SizerTelemetry,
)
from ray.data._internal.experimental.execution.sizer.tick import (
    SizingTicker,
    committed_by_node,
)
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.util import GiB, MiB
from ray.data.context import DataContext
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

    def num_pending_actors(self):
        return self.pending

    def num_running_actors(self):
        return self.running

    def per_actor_resource_usage(self):
        return self._per_actor

    def num_tasks_in_flight(self):
        return self.in_flight

    def max_actor_concurrency(self):
        return self.concurrency


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
    ):
        self._fake_name = name
        self._name = name
        self._id = name
        self._user_set_ray_remote_args = dict(user_ray_remote_args or {})
        self._ray_remote_args = dict(ray_remote_args or {})
        self._user_set_ray_remote_args_fn = uses_ray_remote_args_fn
        self._placement_constraint = None
        self._data_context = DataContext.get_current()
        self._pool = pool
        self._node = node
        self._completed = completed
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

    def process_draining_actors(self):
        return 0

    def has_completed(self):
        return self._completed

    def throttling_disabled(self):
        return self._throttling_disabled

    def has_execution_finished(self):
        return self._execution_finished

    def output_backpressured_fraction(self):
        return self.out_backpressure

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
    grant. Skips the real ``__init__`` (no coordinator, no ``ray.nodes()``)."""

    def __init__(self, view: DatasetAllocationView):
        self.view = view

    def get_reserved_resources_by_node(self):
        return dict(self.view.reserved_by_node)

    def get_cluster_view(self, **kwargs):
        return self.view

    def prefetch_pg_bundles(self, placement_groups):
        pass


class _FakeClock:
    def __init__(self, now: float = 100.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _TestSizer(OperatorSizer):
    """An ``OperatorSizer`` over fakes, plus the seams tests drive it by: one
    phase at a time, off the fake reporter's view and the fake clock."""

    def __init__(self, topo, reporter: _FakeReporter, clock: _FakeClock):
        super().__init__(
            dataset_id="test-ds", topology=topo, resource_reporter=reporter, clock=clock
        )
        self.reporter = reporter
        self.clock = clock

    def next_tick(self, *, force: bool = True) -> Optional[TickContext]:
        """The next TickContext; ``force`` steps the clock past the cadence."""
        if force:
            self.clock.advance(SIZER_CADENCE_S)
        return self._ticker.next_tick(self.reporter.view)

    def how_many(self, *, force: bool = True) -> PerOpSizingRequest:
        """One scale_how_many evaluation."""
        ctx = self.next_tick(force=force)
        assert ctx is not None
        return self._per_op_how_many(ctx)

    @property
    def guard(self):
        return self._guard

    def shortfalls(self) -> Dict[Any, OpShortfall]:
        """Unplaced asks the last pass reported to the optimizer."""
        return cast(
            Dict[Any, OpShortfall], self._optimizer._signals.signal_dict()["shortfalls"]
        )

    def shortfall_streak(self, op) -> bool:
        streaks = cast(
            Dict[Any, BoolStreak],
            self._optimizer._signals.signal_dict()["shortfall_streak"],
        )
        return streaks[op].is_streak()


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


def _single_node(cpu=100.0, gpu=4.0) -> DatasetAllocationView:
    return DatasetAllocationView(
        reserved_by_node={"node0": ExecutionResources(cpu=cpu, gpu=gpu)},
        labels_by_node={"node0": {}},
    )


def _make_sizer(
    topo, view: Optional[DatasetAllocationView] = None, **cluster
) -> _TestSizer:
    """A sizer over ``topo`` whose reporter serves the given cluster view (a
    single ``node0`` by default). Free capacity is the tick's real math:
    allocation minus what the fakes commit (per_actor x total_size)."""
    return _TestSizer(
        topo, _FakeReporter(view or _single_node(**cluster)), _FakeClock()
    )


def _bootstrapped(*ops, enqueued=0, view=None, warm=False, **cluster):
    """Sizer over ``ops``; ``warm=True`` has every op report output, so the
    first tick is already past warmup. Returns (sizer, topology)."""
    topo = _topology(*ops, enqueued=enqueued)
    sizer = _make_sizer(topo, view, **cluster)
    if warm:
        for op in topo:
            op.metrics.num_task_outputs_generated = 1
    return sizer, topo


def _tick(sizer: _TestSizer, topo=None) -> PerOpSizingRequest:
    return sizer.how_many()


def _ticker(topo):
    """A standalone ticker on a fake clock, and a callable that runs its next
    tick and reports whether that tick is still in warmup."""
    clock = _FakeClock()
    ticker = SizingTicker(OpProfiles(topo), cadence_s=SIZER_CADENCE_S, clock=clock)

    def in_warmup_after_tick() -> bool:
        clock.advance(SIZER_CADENCE_S)
        ctx = ticker.next_tick(_single_node())
        assert ctx is not None
        return ctx.warmup

    return ticker, in_warmup_after_tick


def _snapshot_of(sizer: _TestSizer) -> TickCapacitySnapshot:
    ctx = sizer.next_tick()
    assert ctx is not None
    return ctx.capacity


def _ticks_until_idle_drain(sizer: _TestSizer, op, limit: int = 20) -> int:
    """Ticks until ``op`` gets an idle-drain request. Equal to the hysteresis
    exactly when its idle streak starts from zero."""
    for n in range(1, limit + 1):
        req = sizer.how_many().request.get(op)
        if req is not None and req.reason == "drain (idle)":
            return n
    raise AssertionError(f"{op.name} never drained")


def _shortfall_actors(sizer: _TestSizer, op=None) -> int:
    """Unplaced actors the last tick reported to the optimizer: for ``op``, or
    summed across every op."""
    shortfalls = sizer.shortfalls()
    if op is not None:
        return shortfalls[op].num_actors if op in shortfalls else 0
    return sum(sf.num_actors for sf in shortfalls.values())


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


def test_warmup_latches_off_on_critical_op_first_output():
    ops, topo = _linear_pipeline(running=1)
    # Critical op = deepest task-launching op, i.e. the last non-sink map -> infer.
    assert _ticker(topo)[0].critical_ops == (ops["infer"],)
    sizer = _make_sizer(topo)
    ctx = sizer.next_tick()
    assert ctx is not None and ctx.warmup
    ops["infer"].metrics.num_task_outputs_generated = 1
    ctx = sizer.next_tick()
    assert ctx is not None and not ctx.warmup
    # The first warm tick holds sizes; the warm arms need a signal to act on.
    assert sizer._per_op_how_many(ctx).request == {}


def test_warmup_times_out_when_a_critical_op_never_produces(caplog, propagate_logs):
    # A critical op that never gets an actor (say, starved by a memory
    # reservation) must not hold the sizer in warmup forever: after the
    # timeout it moves to the warm phase, and says which op it gave up on.
    ops, topo = _linear_pipeline(running=1)
    clock = _FakeClock()
    ticker = SizingTicker(
        OpProfiles(topo),
        cadence_s=SIZER_CADENCE_S,
        clock=clock,
        warmup_timeout_s=10 * SIZER_CADENCE_S,
    )

    def in_warmup_after_tick() -> bool:
        clock.advance(SIZER_CADENCE_S)
        ctx = ticker.next_tick(_single_node())
        assert ctx is not None
        return ctx.warmup

    # The timeout counts from the first tick.
    assert all(in_warmup_after_tick() for _ in range(10))
    with caplog.at_level(logging.WARNING):
        assert not in_warmup_after_tick()
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
    return DatasetAllocationView(reserved_by_node=alloc, labels_by_node=labels)


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


class _FakePG:
    """Minimal placement-group stand-in exposing ``.id.hex()``."""

    def __init__(self, id_hex):
        self.id = SimpleNamespace(hex=lambda: id_hex)


def _pg_op(name, pg, *, bundle_index=None, inputs=(), **pool_kwargs) -> _FakeOp:
    kwargs = dict(min_size=1, max_size=10)
    kwargs.update(pool_kwargs)
    op = _FakeOp(name, _FakePool(**kwargs), node=None, inputs=inputs)
    op._placement_constraint = PlacementConstraint(
        placement_group=cast(Any, pg), placement_group_bundle_index=bundle_index
    )
    return op


def test_pg_bundles_are_per_bundle_slots(caplog, propagate_logs):
    """Each bundle is its own slot (a 4-GPU actor fits no 2-GPU bundle, even
    though the PG totals 8 GPU); a bundle index narrows the op to one bundle,
    an out-of-range one is warned about and widened to every bundle; an
    unresolved PG is left out (its capacity is unknown)."""
    pg = _FakePG("pg1")
    view = DatasetAllocationView(
        reserved_by_node={},
        pg_bundles={"pg1": [ExecutionResources(gpu=2)] * 4},
    )
    anywhere = _pg_op("anywhere", pg, gpu=1, running=3)
    pinned = _pg_op("pinned", pg, bundle_index=1, gpu=1, running=1, inputs=[anywhere])
    big = _pg_op("big", pg, gpu=4, inputs=[pinned])
    unresolved = _pg_op("unresolved", _FakePG("missing"), gpu=1, inputs=[big])
    bad_index = _pg_op("bad_index", pg, bundle_index=9, gpu=1, inputs=[unresolved])
    with caplog.at_level(logging.WARNING):
        bundles = PlacementGroupBundles.measure(
            [anywhere, pinned, big, unresolved, bad_index], view
        )

    every_bundle = tuple(("pg1", i) for i in range(4))
    assert bundles.bundles_by_op[anywhere] == every_bundle
    assert bundles.bundles_by_op[pinned] == (("pg1", 1),)
    assert unresolved not in bundles.bundles_by_op
    assert bundles.bundles_by_op[bad_index] == every_bundle
    assert any(
        "placement_group_bundle_index=9" in r.getMessage() for r in caplog.records
    )
    # Held actors pack first-fit, most constrained op first: pinned's actor
    # takes half of bundle 1, then anywhere's 3 fill bundle 0 and the rest of 1.
    free = bundles.free_by_bundle()
    assert [free[("pg1", i)].gpu for i in range(4)] == [0, 0, 2, 2]

    snapshot = _snapshot(
        {},
        {},
        {op: op._pool.per_actor_resource_usage() for op in (anywhere, big)},
        pg_free=free,
        bundles=bundles.bundles_by_op,
    )
    ledger = ConstraintAwareCapacityLedger(snapshot)
    assert ledger.max_upscale_delta(anywhere) == 4
    assert ledger.max_upscale_delta(big) == 0  # 4 GPU free, but split in two


@pytest.mark.parametrize(
    "pg,uses_fn,pool_kwargs,expected_cap",
    [
        # A static PG op grows into what its bundles hold.
        pytest.param(
            _FakePG("pg1"), False, dict(min_size=2, max_size=10, gpu=1), 4, id="pg"
        ),
        # A ray_remote_args_fn op mints a PG per actor the sizer cannot inspect,
        # so it is capped at its user concurrency (max_size) instead.
        pytest.param(None, True, dict(min_size=1, max_size=1, gpu=0), 1, id="fn-op"),
        # A static PG with no bundles in the view yet is just as unknown:
        # capped at max_size too, rather than held at zero.
        pytest.param(
            _FakePG("missing"),
            False,
            dict(min_size=1, max_size=3, gpu=1),
            3,
            id="pg-not-resolved",
        ),
    ],
)
def test_sizer_measures_core_placed_caps(pg, uses_fn, pool_kwargs, expected_cap):
    view = DatasetAllocationView(
        reserved_by_node={"g1": ExecutionResources(gpu=4)},
        pg_bundles={"pg1": [ExecutionResources(gpu=4)]},
    )
    op = _FakeOp(
        "embed", _FakePool(**pool_kwargs), node=None, uses_ray_remote_args_fn=uses_fn
    )
    if pg is not None:
        op._placement_constraint = PlacementConstraint(placement_group=cast(Any, pg))
    sizer, _ = _bootstrapped(op, view=view)
    # Nothing running yet, so the whole cap is free.
    ledger = ConstraintAwareCapacityLedger(_snapshot_of(sizer))
    assert ledger.max_upscale_delta(op) == expected_cap
    assert build_op_profile(op).pg_managed


# --- warmup --------------------------------------------------------------------


def test_warmup_equal_share_targets():
    ops, topo = _linear_pipeline(running=1)
    sizer = _make_sizer(topo, cpu=100.0, gpu=4.0)
    result = _tick(sizer, topo)
    # min_reserved_cpu = 3 (a, b, write at min 1 x 1 cpu); 3 autoscaled cpu
    # ops. ceiling = (100 - 3) * 0.8 / 3 = 25.87 -> target 25 -> delta 24.
    for key in ("a", "b", "write"):
        assert result.request[ops[key]].delta == 24, key
    # Fixed GPU pool at size: no resize request.
    assert ops["infer"] not in result.request

    # GPU ops size against the GPU share: min_reserved_gpu = 1;
    # ceiling = (8 - 1) * 0.8 / 1 = 5.6 -> target 5 -> delta 4.
    gpu_op = _FakeOp("GpuMap", _FakePool(min_size=1, max_size=40, gpu=1, running=1))
    sizer, topo = _bootstrapped(gpu_op, cpu=100.0, gpu=8.0)
    assert _tick(sizer, topo).request[gpu_op].delta == 4


def test_warmup_share_survives_float_division():
    # (4.1 - 0.1) * 0.8 / 0.1 is 31.999... in floating point; truncating it
    # would leave the pool one actor short of its 32-actor share.
    op = _FakeOp("op", _FakePool(min_size=1, max_size=100, cpu=0.1, running=1))
    sizer, topo = _bootstrapped(op, cpu=4.1, gpu=0.0)
    assert _tick(sizer, topo).request[op].delta == 31


def test_warmup_pg_ops_share_their_placement_group():
    """PG ops size against their placement group, not the allocation: two
    autoscaled ops in one PG split its bundles like non-PG ops split the
    allocation. They also stay out of the non-PG equal share, so the non-PG
    GPU op gets the whole slice."""
    pg = _FakePG("pg1")
    view = DatasetAllocationView(
        reserved_by_node={"node0": ExecutionResources(cpu=8, gpu=8)},
        labels_by_node={"node0": {}},
        pg_bundles={"pg1": [ExecutionResources(gpu=10)]},
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
    sizer, _ = _bootstrapped(*pg_ops, plain, view=view)
    result = sizer.how_many()
    # (10 GPU - 2 floors) * 0.8 / 2 = 3.2 -> target 3 -> +2 each.
    for op in pg_ops:
        assert result.request[op].delta == 2, op.name
    # Non-PG share: (8 - 1) * 0.8 / 1 = 5.6 -> target 5 -> +4.
    assert result.request[plain].delta == 4


def test_guard_blocked_op_gets_no_want():
    # An op an optimizer rule has claimed is left alone by every arm: no idle
    # drain, no below-min replenish -- a replenish would add pendings that
    # ``_record_transfer_pendings`` mistakes for a transfer's claim. Released,
    # both arms act again.
    idle = _FakeOp("idle", _FakePool(min_size=1, max_size=10, cpu=1, running=5))
    short = _FakeOp(
        "short", _FakePool(min_size=3, max_size=10, cpu=1, running=1), inputs=[idle]
    )
    sizer, topo = _bootstrapped(idle, short, warm=True)
    for op in (idle, short):
        sizer.guard.block(op, "claimed by a rule")
    for _ in range(5):  # longer than the idle-drain hysteresis
        assert _tick(sizer, topo).request == {}

    for op in (idle, short):
        sizer.guard.release(op)
    requests = [_tick(sizer, topo).request for _ in range(5)]
    assert any(r.get(idle) and r[idle].delta < 0 for r in requests)
    assert requests[0][short].delta == 2


def test_below_min_op_granted_nothing_builds_the_transfer_streak():
    # A below-min op that gets zero actors is ungranted like any other ask, so
    # it can become a capacity-transfer recipient.
    op = _FakeOp("op", _FakePool(min_size=3, max_size=10, cpu=1, running=1))
    sizer, topo = _bootstrapped(op, warm=True, cpu=1.0, gpu=0.0)
    for _ in range(3):
        assert op not in _tick(sizer, topo).request
    assert _shortfall_actors(sizer, op) == 2
    assert sizer.shortfall_streak(op)


def test_warmup_is_upscale_only_and_holds_while_pending():
    # Above the warmup target: warmup must not downscale.
    big = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, running=90))
    sizer, topo = _bootstrapped(big, cpu=100.0)
    assert big not in _tick(sizer, topo).request

    # A granted upscale that is still pending is not re-requested.
    op = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, running=1))
    sizer, topo = _bootstrapped(op, cpu=100.0)
    delta = _tick(sizer, topo).request[op].delta
    assert delta > 0
    op._pool.pending = delta
    assert op not in _tick(sizer, topo).request


def test_critical_op_discovery_walks_past_ops_that_never_report_task_outputs():
    """Regression: the walk used to stop at the first non-sink operator,
    whatever it was. Limit, union, mix and the input buffer launch no tasks, so
    their num_task_outputs_generated is always 0 -- latching on one pinned the
    sizer in feedforward warmup for the entire run."""
    source = _FakeOp("Map", _FakePool(min_size=1, max_size=4, cpu=1))
    limit = _FakeNonPoolOp("Limit", inputs=[source])
    ticker, in_warmup_after_tick = _ticker(_topology(source, limit))

    assert ticker.critical_ops == (source,)
    assert in_warmup_after_tick()
    source.metrics.num_task_outputs_generated = 1
    assert not in_warmup_after_tick()


def test_latch_waits_for_every_branch_of_a_union():
    """A union has two inputs, so there is no single deepest operator. Ending
    warmup while one branch is still cold would size that branch by feedback
    against an empty queue and drain it."""
    left = _FakeOp("Left", _FakePool(min_size=1, max_size=4, cpu=1))
    right = _FakeOp("Right", _FakePool(min_size=1, max_size=4, cpu=1))
    union = _FakeNonPoolOp("Union", inputs=[left, right])
    ticker, in_warmup_after_tick = _ticker(_topology(left, right, union))

    assert set(ticker.critical_ops) == {left, right}
    left.metrics.num_task_outputs_generated = 1
    assert in_warmup_after_tick()  # right is still cold
    right.metrics.num_task_outputs_generated = 1
    assert not in_warmup_after_tick()


def test_tick_cadence_gate_and_idle_phases():
    ops, topo = _linear_pipeline(running=1)
    sizer = _make_sizer(topo)
    ctx = sizer.next_tick(force=False)  # the first tick is never gated
    assert ctx is not None
    assert ctx.capacity.nodes_to_reclaim == {}
    # Every real tick carries the cluster snapshot the phases share, and the
    # order they all walk operators in.
    assert ctx.ordered_ops == tuple(
        ordered_actor_pool_ops(
            topo,
            OpProfiles(topo).actor_pool_ops,
            ctx.capacity.schedulable_nodes_by_op,
        )
    )
    # Inside the cadence window there is no tick, so a scheduling-loop step
    # only samples signals. Nothing is resized, and pool upkeep is the
    # executor's job, not the sizer's.
    assert sizer.next_tick(force=False) is None
    sizer.perform_sizing_decision(
        cluster_view=sizer.reporter.view,
        seconds_since_allocation_change=0.0,
    )
    assert all(op._pool.scaled == [] for op in topo)
    assert all(op.actor_pool.upkeep_calls == 0 for op in topo)


def test_loop_lag_warning_on_slow_tick(caplog, propagate_logs):
    # When the gap between evaluations exceeds 1.5x the cadence, the control
    # loop is running slow and a lag warning is emitted; an on-cadence tick
    # must not warn.
    op = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, running=20))
    sizer, topo = _bootstrapped(op, warm=True)

    def lag_records():
        return [r for r in caplog.records if "scheduling-loop lag" in r.getMessage()]

    with caplog.at_level(logging.WARNING):
        sizer.how_many(force=False)  # first eval, no prior gap
        sizer.clock.advance(4.0)  # 4.0s gap > 1.5 * 2s cadence -> lag
        sizer.how_many(force=False)
    assert len(lag_records()) == 1

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        sizer.clock.advance(2.2)  # past the gate but under the threshold
        sizer.how_many(force=False)
    assert not lag_records()


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
    result = _tick(sizer, topo)
    if expected_delta is None:
        assert fixed not in result.request
    else:
        req = result.request[fixed]
        assert (req.delta, req.reason) == (expected_delta, "below min size")


def test_below_min_grants_full_min_in_order_and_reports_shortfall():
    # Each below-min op is granted its FULL min (clamped to placeable
    # capacity), in the sizer's ordering. On a full cluster a large downstream
    # pool processed first can consume everything and leave the source at 0
    # -- that shortfall must be reported so the optimizer can qualify the op
    # for a capacity transfer.
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
    result = _tick(sizer, topo)
    # Consumer (sink-most) then producer are granted; producer's min 30 is
    # clamped to the remaining capacity, exhausting the cluster.
    assert result.request[consumer].delta == 1
    assert result.request[producer].delta == 15
    assert src not in result.request or result.request[src].delta == 0
    assert _shortfall_actors(sizer, src) == 1
    assert _shortfall_actors(sizer, producer) == 15


@pytest.mark.parametrize(
    "cluster_cpu,expected_placed,expected_short_actors",
    [
        # Autoscale-from-zero: nothing placeable, the whole min reported short.
        pytest.param(0, 0, 5, id="zero-capacity-full-shortfall"),
        # Part of the min fits: that part is requested, the rest reported.
        pytest.param(2, 2, 3, id="partial-placement-remaining-shortfall"),
    ],
)
def test_below_min_shortfall_is_reported(
    cluster_cpu: float, expected_placed: int, expected_short_actors: int
):
    src = _FakeOp("ReadRange", _FakePool(min_size=1, max_size=1, cpu=1, running=0))
    mapper = _FakeOp(
        "Map(sleep_task)",
        _FakePool(min_size=4, max_size=8, cpu=1, running=0),
        inputs=(src,),
    )
    sizer, topo = _bootstrapped(src, mapper, cpu=cluster_cpu, gpu=0)
    result = _tick(sizer, topo)
    # Placed + reported-short covers the full min (src 1 + mapper 4 = 5).
    placed = sum(max(0, req.delta) for req in result.request.values())
    assert placed == expected_placed
    assert _shortfall_actors(sizer) == expected_short_actors


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
    view = DatasetAllocationView(reserved_by_node=alloc, labels_by_node=labels)

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
    req = _tick(sizer, topo)
    assert req.request[pinned].delta == 4
    for peer in (peer1, peer2, peer3):
        assert req.request[peer].delta >= 1


def test_completed_op_released_with_force_and_logged_once_per_size(
    caplog, propagate_logs
):
    pool = _FakePool(min_size=1, max_size=500, cpu=1, running=10)
    done = _FakeOp("ReadFiles", pool, completed=True, execution_finished=True)
    sizer, topo = _bootstrapped(done)
    with caplog.at_level(logging.DEBUG):
        for _ in range(3):
            # The request keeps firing so attrition continues.
            req = _tick(sizer, topo).request[done]
            assert (req.delta, req.force) == (-10, True)
        pool.running = 4  # attrition removed 6 idle actors
        assert _tick(sizer, topo).request[done].delta == -4
    releases = [r for r in caplog.records if "releasing" in r.getMessage()]
    # One line at size 10 and one at size 4 -- not one per tick.
    assert len(releases) == 2


def test_source_op_not_released_at_startup():
    # A source op with an empty input queue must warm up, never be treated as
    # done and force-released.
    src = _FakeOp("ReadFiles", _FakePool(min_size=1, max_size=500, cpu=1, running=4))
    sizer, topo = _bootstrapped(src, cpu=100.0)
    req = _tick(sizer, topo).request[src]
    assert req.delta > 0 and not req.force


# --- warm loop -----------------------------------------------------------------


@pytest.mark.parametrize(
    "pool_kwargs,bp_kind,bp,expected_full",
    [
        # A pool with pending actors (mid-resize) must not read as output-full:
        # cross-op queries have no other guard, so a resizing neighbour would
        # flip them.
        pytest.param(
            dict(running=4, pending=2), "tasks", (8, 8), False, id="mid-resize-guard"
        ),
        # 1 blocked actor out of 10 running (9 idle) must not read as full:
        # idle actors are unblocked capacity, not absent from the denominator.
        pytest.param(
            dict(running=10), "actors", (1, 10), False, id="idle-actors-in-denominator"
        ),
        pytest.param(dict(running=10), "actors", (10, 10), True, id="all-blocked"),
    ],
)
def test_outqueue_full(pool_kwargs, bp_kind, bp, expected_full):
    op = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, **pool_kwargs))
    if bp_kind == "tasks":
        op.out_backpressure = bp
    else:
        op.out_backpressure_actors = bp
    policy = OpSizingPolicy(OpProfiles(_topology(op)))
    full, blocked, running = policy.is_output_backpressured(op)
    assert full is expected_full
    if bp_kind == "actors":
        assert (blocked, running) == bp


# Warm-loop grow/hold arms for a single fed op: the +50% growth step bounded
# by backlog demand, max_size, and the "not output-full" gate.
@pytest.mark.parametrize(
    "pool_kwargs,enqueued,out_backpressure,expected_delta",
    [
        # want = ceil(0.5*10) = 5; demand ceil(40/1.75)=23 and max_size both
        # leave it -> grows multiplicatively.
        pytest.param(dict(running=10), 40, (0, 5), 5, id="grow-multiplicative"),
        # demand = ceil(21/1.75) = 12 sits between cur and cur+want, so demand
        # (not the +50% rate) caps: min(want=5, demand-cur=2, max-cur) = 2.
        pytest.param(dict(running=10), 21, (0, 5), 2, id="grow-capped-by-demand"),
        # Cold start: fed and NOT output-backpressured, but already at/above
        # the work it has (demand = ceil((10+5)/1.75) = 9 << cur=100) -> held
        # at demand, no unbounded growth.
        pytest.param(
            dict(running=100, in_flight=10), 5, (0, 0), None, id="hold-at-demand"
        ),
        # max_size clamps the step: min(want=5, 12-10) = 2.
        pytest.param(
            dict(max_size=12, running=10), 40, (0, 5), 2, id="grow-clamped-by-max"
        ),
        pytest.param(dict(max_size=10, running=10), 40, (0, 5), None, id="hold-at-max"),
        # An idle pool (zero active tasks) must not read as output-full; fed ->
        # upscales min(want=3, demand ceil(10/1.75)=6 - cur=5) = 1.
        pytest.param(dict(running=5), 10, (0, 0), 1, id="grow-zero-active-not-full"),
    ],
)
def test_warm_grow_hold_arms(pool_kwargs, enqueued, out_backpressure, expected_delta):
    kwargs = dict(min_size=1, max_size=500, cpu=1)
    kwargs.update(pool_kwargs)
    op = _FakeOp("A", _FakePool(**kwargs))
    sizer, topo = _bootstrapped(op, enqueued=enqueued, warm=True, cpu=1000.0, gpu=8.0)
    op.out_backpressure = out_backpressure
    result = _tick(sizer, topo)
    if expected_delta is None:
        assert op not in result.request
    else:
        assert result.request[op].delta == expected_delta


def test_warm_demand_target_formula():
    # ceil((in_flight + ceil(enqueued/avg)) / (max_concurrency * 1.75)).
    op = _FakeOp(
        "A", _FakePool(min_size=1, max_size=500, cpu=1, running=10, in_flight=20)
    )
    op._fake_metrics.average_num_inputs_per_task = 2
    topo = _topology(op, enqueued=40)  # 20 tasks; total 40
    assert OpSizingPolicy(OpProfiles(topo)).backlog_actor_target(op, topo[op]) == 23


def test_warm_same_tick_grants_do_not_overcommit_cluster():
    # Two fed pools, 5 free CPU slots: downstream-first grant must debit the
    # tick-local budget so the second op cannot claim the same slots.
    a = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, running=10))
    b = _FakeOp(
        "B", _FakePool(min_size=1, max_size=500, cpu=1, running=10), inputs=(a,)
    )
    sizer, topo = _bootstrapped(a, b, enqueued=40, warm=True, cpu=25.0)
    for op in (a, b):
        op.out_backpressure = (0, 5)
    result = _tick(sizer, topo)
    granted = sum(req.delta for req in result.request.values() if req.delta > 0)
    assert granted <= 5
    # Downstream-first: B gets its full step (want=5), A gets nothing.
    assert result.request[b].delta == 5
    assert a not in result.request


# An output-backpressured pool must HOLD (never shed) when draining it would
# starve a downstream bottleneck or kill in-flight work, however long the
# blocked streak runs.
@pytest.mark.parametrize(
    "pool_kwargs,enqueued,out_backpressure,extra_ticks",
    [
        # Output fully backpressured but a large input backlog (demand =
        # ceil(200/1.75) = 115 >> current 18): the DOWNSTREAM op is the
        # bottleneck, not this stage over-provisioned.
        pytest.param(dict(cpu=1, running=18), 200, (38, 38), 2, id="backlogged-feeder"),
        # Every actor mid-task, outputs merely alive downstream: a pool with
        # zero idle actors has nothing to shed without killing in-flight work.
        pytest.param(
            dict(gpu=1, max_size=8, running=2, in_flight=2),
            0,
            (2, 2),
            3,
            id="all-actors-busy",
        ),
    ],
)
def test_warm_backpressured_pool_never_shed(
    pool_kwargs, enqueued, out_backpressure, extra_ticks
):
    kwargs = dict(min_size=1, max_size=500)
    kwargs.update(pool_kwargs)
    op = _FakeOp("A", _FakePool(**kwargs))
    sizer, topo = _bootstrapped(op, enqueued=enqueued, warm=True, cpu=1000.0, gpu=8.0)
    op.out_backpressure = out_backpressure
    for _ in range(SIZER_DOWNSCALE_HYSTERESIS + extra_ticks):
        req = _tick(sizer, topo).request.get(op)
        assert req is None or req.delta >= 0, f"shed a backpressured pool: {req}"


# An output-backpressured producer must HOLD in every variant where acting on
# it would hurt: shedding would kill in-flight work or starve a bottleneck,
# and growing would pile up unconsumed output. Each op spec is
# dict(pool=..., enq=..., bp=...); expect: "hold" (no request at all),
# "no_shed" (delta never negative), "no_grow" (delta never positive).
_PRODUCER_HOLD_CHECKS = {
    "hold": lambda req: req is None,
    "no_shed": lambda req: req is None or req.delta >= 0,
    "no_grow": lambda req: req is None or req.delta <= 0,
}


@pytest.mark.parametrize(
    "producer,consumer,cpu,ticks_fn,expect",
    [
        # The producer's actors are ALL busy: donating capacity would kill
        # in-flight work, so nothing is shed.
        pytest.param(
            dict(pool=dict(running=50, in_flight=50), enq=200, bp=(50, 50)),
            dict(pool=dict(running=20), enq=200, bp=(0, 20)),
            70.0,
            lambda hysteresis: hysteresis + 2,
            "no_shed",
            id="donor-requires-idle-actors",
        ),
        # The consumer is a SATURATED bottleneck pinned at its max_size on
        # ample free CPU: no placeable unmet demand -- draining OR growing the
        # producer would just starve/spill.
        pytest.param(
            dict(pool=dict(running=40), enq=200, bp=(40, 40)),
            dict(
                pool=dict(min_size=10, max_size=10, running=10, in_flight=10),
                enq=200,
                bp=(0, 10),
            ),
            200.0,
            lambda hysteresis: hysteresis + 1,
            "hold",
            id="consumer-cannot-grow",
        ),
        # The consumer is saturated AND has a genuine input backlog: it truly
        # cannot absorb more, so growing the producer would pile up output.
        pytest.param(
            dict(pool=dict(running=10), enq=200, bp=(10, 10)),
            dict(pool=dict(running=8, in_flight=8), enq=50, bp=(0, 8)),
            1000.0,
            lambda hysteresis: 1,
            "hold",
            id="downstream-saturated",
        ),
        # The consumer is itself output-blocked: the chain is stuck
        # downstream, so the producer's out_full must not become growth.
        pytest.param(
            dict(pool=dict(running=10), enq=200, bp=(10, 10)),
            dict(pool=dict(running=8, in_flight=8), enq=0, bp=(8, 8)),
            1000.0,
            lambda hysteresis: 1,
            "no_grow",
            id="consumer-output-blocked",
        ),
    ],
)
def test_warm_backpressured_producer_holds(producer, consumer, cpu, ticks_fn, expect):
    def build(name: str, spec: dict, **op_kwargs) -> _FakeOp:
        kwargs = dict(min_size=1, max_size=500, cpu=1)
        kwargs.update(spec["pool"])
        op = _FakeOp(name, _FakePool(**kwargs), **op_kwargs)
        op.out_backpressure = spec["bp"]
        return op

    producer_op = build("Producer", producer)
    consumer_op = build("Consumer", consumer, inputs=(producer_op,))
    sizer, topo = _bootstrapped(producer_op, consumer_op, warm=True, cpu=cpu, gpu=8.0)
    topo[producer_op]._enqueued = producer["enq"]
    topo[consumer_op]._enqueued = consumer["enq"]
    for _ in range(ticks_fn(SIZER_DOWNSCALE_HYSTERESIS)):
        req = _tick(sizer, topo).request.get(producer_op)
        assert _PRODUCER_HOLD_CHECKS[expect](req), f"{expect} violated: {req}"


# A saturated op braked by its consumer holds at demand in the warm loop: the
# warm loop never lifts it, whatever its producer does -- that is the
# optimizer's silent-bottleneck rule (see test_sizer_pipeline_optimizer).
@pytest.mark.parametrize(
    "producer_bp,bottleneck_pool",
    [
        pytest.param(
            (0, 8), dict(max_size=500, in_flight=10), id="upstream-not-backpressured"
        ),
        pytest.param((8, 8), dict(max_size=500, in_flight=3), id="not-saturated"),
        pytest.param((8, 8), dict(max_size=10, in_flight=10), id="capped-at-max-size"),
    ],
)
def test_warm_loop_holds_saturated_op_at_demand(producer_bp, bottleneck_pool):
    producer = _FakeOp(
        "Producer", _FakePool(min_size=1, max_size=500, cpu=1, running=8, in_flight=8)
    )
    kwargs = dict(min_size=1, max_size=500, cpu=1, running=10, concurrency=1)
    kwargs.update(bottleneck_pool)
    bottleneck = _FakeOp("Bottleneck", _FakePool(**kwargs), inputs=(producer,))
    sizer, topo = _bootstrapped(producer, bottleneck, warm=True, cpu=1000.0, gpu=8.0)
    topo[bottleneck]._enqueued = 2  # shallow queue: demand 7 <= current 10
    bottleneck.out_backpressure = (0, 10)
    producer.out_backpressure = producer_bp
    assert bottleneck not in _tick(sizer, topo).request


# The drain arm fires only on a TRULY idle input: an empty queue with busy
# in-flight tasks holds at demand and the drain streak never builds.
@pytest.mark.parametrize(
    "in_flight,out_backpressure,expect_drain",
    [
        pytest.param(0, (0, 0), True, id="idle-input-drains"),
        # Queue drained but actors busy: demand = ceil(30/1.75) = 18 < cur=20
        # -> hold at demand, never the drain arm.
        pytest.param(30, (0, 30), False, id="in-flight-work-prevents-drain"),
    ],
)
def test_warm_drain_arm_requires_truly_idle_input(
    in_flight, out_backpressure, expect_drain
):
    op = _FakeOp(
        "A", _FakePool(min_size=1, max_size=500, cpu=1, running=20, in_flight=in_flight)
    )
    sizer, topo = _bootstrapped(op, warm=True, cpu=1000.0, gpu=8.0)
    op.out_backpressure = out_backpressure
    if expect_drain:
        assert _ticks_until_idle_drain(sizer, op) == SIZER_DOWNSCALE_HYSTERESIS
        return
    for _ in range(SIZER_DOWNSCALE_HYSTERESIS + 1):  # past hysteresis, no drain
        assert op not in _tick(sizer, topo).request
    # The streak never built: once truly idle, the drain takes a full streak.
    op._pool.in_flight = 0
    assert _ticks_until_idle_drain(sizer, op) == SIZER_DOWNSCALE_HYSTERESIS


def test_warm_pending_gate_blocks_all_arms(caplog, propagate_logs):
    op = _FakeOp("A", _FakePool(min_size=1, max_size=500, cpu=1, running=20, pending=3))
    sizer, topo = _bootstrapped(op, warm=True, cpu=1000.0, gpu=8.0)
    with caplog.at_level(logging.DEBUG):
        results = [_tick(sizer, topo) for _ in range(SIZER_DOWNSCALE_HYSTERESIS + 2)]
    assert all(op not in r.request for r in results)
    assert any("sizing in flight" in r.getMessage() for r in caplog.records)
    # Frozen, not building: once the pendings land, the drain takes a full
    # streak.
    op._pool.pending = 0
    assert _ticks_until_idle_drain(sizer, op) == SIZER_DOWNSCALE_HYSTERESIS


# other -> op. `other` occupies most of the 100 cluster CPUs; op's fed upscale
# is clamped to what is free, and whatever cannot be placed is reported short.
@pytest.mark.parametrize(
    "other_running,expected_delta,expected_short_actors",
    [
        # free = 100 - (10 + 85) = 5 -> placeable = min(want=5, free=5).
        pytest.param(85, 5, 0, id="fits-free-capacity"),
        # free = 0 -> want = ceil(0.5 * 10) = 5 actors, all reported short.
        pytest.param(90, 0, 5, id="unplaced-want-is-reported-short"),
    ],
)
def test_warm_upscale_clamped_by_free_cluster(
    other_running, expected_delta, expected_short_actors
):
    other = _FakeOp(
        "B", _FakePool(min_size=1, max_size=500, cpu=1, running=other_running)
    )
    op = _FakeOp(
        "A", _FakePool(min_size=1, max_size=500, cpu=1, running=10), inputs=(other,)
    )
    sizer, topo = _bootstrapped(other, op, warm=True, cpu=100.0)
    topo[op]._enqueued = 200
    op.out_backpressure = (0, 5)
    other.out_backpressure = (0, 40)
    result = _tick(sizer, topo)
    if expected_delta == 0:
        assert op not in result.request or result.request[op].delta == 0
    else:
        assert result.request[op].delta == expected_delta
    assert _shortfall_actors(sizer, op) == expected_short_actors


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
    view = DatasetAllocationView(reserved_by_node=alloc, labels_by_node=labels)

    read = _FakeOp(
        "read", _FakePool(min_size=1, max_size=500, cpu=1, running=1), node=None
    )
    embed = _FakeOp(
        "embed",
        _FakePool(min_size=1, max_size=500, gpu=1, running=20, in_flight=50),
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

    # embed wants ceil(0.5*20)=10 but only 8 A100 GPUs are free -> capped at
    # 8; the other 2 are reported short.
    req = _tick(sizer, topo)
    assert req.request[embed].delta == 8
    assert _shortfall_actors(sizer, embed) == 2


def test_reclaimable_actors_do_not_debit_free_capacity_for_later_ops():
    # A reclaim-heavy op processed first must not over-debit the per-node
    # ledger: only genuinely-new actors consume free capacity, so a later op
    # contending for the same node still gets its full grant.
    view = DatasetAllocationView(
        reserved_by_node={"n1": ExecutionResources(cpu=10)}, labels_by_node={"n1": {}}
    )
    up = _FakeOp(
        "up",
        _FakePool(min_size=1, max_size=500, cpu=1, running=16, in_flight=1),
        node=None,
    )
    sink = _FakeOp(
        "sink",
        _FakePool(min_size=1, max_size=500, cpu=1, running=10, in_flight=1),
        inputs=[up],
        node=None,
        reclaimable=3,  # sink (processed first, sink-most) reclaims these for free
    )
    sizer, topo = _bootstrapped(up, sink, enqueued=100_000, view=view, warm=True)
    req = _tick(sizer, topo)
    # sink: want=ceil(0.5*10)=5, of which 3 are reclaimed -> only 2 consume
    # free, leaving 8 CPU. up: want=ceil(0.5*16)=8, and it gets all 8.
    assert req.request[sink].delta == 5
    assert req.request[up].delta == 8


# --- capacity ledger -----------------------------------------------------------


class _LedgerOp:
    """Duck-typed op the ledger reads its reclaimable count off."""

    def __init__(self, reclaimable=0):
        self.actor_pool = SimpleNamespace(
            num_reclaimable_draining_actors=lambda: reclaimable
        )


def _snapshot(
    free,
    schedulable_nodes_by_op,
    per_actor_resources_by_op,
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
    return TickCapacitySnapshot(
        free_by_node=free,
        reserved_by_node=dict(free),
        schedulable_nodes_by_op=schedulable_nodes_by_op,
        per_actor_resources_by_op=per_actor_resources_by_op,
        pg_free_by_bundle=dict(pg_free or {}),
        schedulable_bundles_by_op=dict(bundles or {}),
        unknown_pg_headroom_by_op=unknown_pg_headroom or {},
    )


def test_ledger_is_per_node_and_constraint_aware():
    # 2 nodes x 3 CPU, actor needs 2 CPU: per-node floor (1+1=2), NOT a global
    # floor(6/2)=3 -- fragmentation is visible.
    op = cast(Any, _LedgerOp())
    free = {"a": ExecutionResources(cpu=3), "b": ExecutionResources(cpu=3)}
    ledger = ConstraintAwareCapacityLedger(
        _snapshot(free, {op: None}, {op: ExecutionResources(cpu=2)})
    )
    assert ledger.max_upscale_delta(op) == 2

    # op1 eligible on all; op2 only on the shared GPU nodes. Granting op2
    # first consumes the shared nodes, so op1 then sees less.
    op1, op2 = cast(Any, _LedgerOp()), cast(Any, _LedgerOp())
    free = {
        "n1": ExecutionResources(cpu=8),
        "g1": ExecutionResources(gpu=4),
        "g2": ExecutionResources(gpu=4),
    }
    match = {op1: None, op2: frozenset({"g1", "g2"})}
    per = {op1: ExecutionResources(gpu=1), op2: ExecutionResources(gpu=1)}
    ledger = ConstraintAwareCapacityLedger(_snapshot(free, match, per))
    assert ledger.max_upscale_delta(op2) == 8  # 2 GPU nodes x 4
    ledger.grant(op2, 8)
    assert ledger.max_upscale_delta(op2) == 0
    assert ledger.max_upscale_delta(op1) == 0  # the GPU nodes are drained


def test_ledger_reclaimable_accounting():
    # A node with room for 4 actors + 3 draining actors this op can reclaim:
    # max_upscale_delta = 4 + 3. Granting the 3 reclaimable takes them back from
    # their held slots and consumes ZERO free, so a peer contending for the
    # same node still sees the full 4; granting past them debits free.
    op = cast(Any, _LedgerOp(reclaimable=3))
    peer = cast(Any, _LedgerOp())
    free = {"n1": ExecutionResources(cpu=4)}
    per = {op: ExecutionResources(cpu=1), peer: ExecutionResources(cpu=1)}
    ledger = ConstraintAwareCapacityLedger(_snapshot(free, {op: None, peer: None}, per))
    assert ledger.max_upscale_delta(op) == 7
    ledger.grant(op, 3)
    assert ledger.max_upscale_delta(peer) == 4
    ledger.grant(op, 5)  # 3 reclaimed + 2 new
    assert ledger.max_upscale_delta(peer) == 2


def test_ledger_pg_per_actor_ops_size_against_headroom():
    op = cast(Any, _LedgerOp())
    ledger = ConstraintAwareCapacityLedger(
        _snapshot(
            {"n1": ExecutionResources(cpu=8)},
            {op: None},
            {op: ExecutionResources(gpu=1)},
            unknown_pg_headroom={op: 3},
        )
    )
    assert ledger.max_upscale_delta(op) == 3
    ledger.grant(op, 5)  # capped at remaining
    assert ledger.max_upscale_delta(op) == 0


def test_ledger_unknown_pg_ops_revive_draining_actors_on_top_of_headroom():
    # 2 revivable draining actors and room for 1 more under max_size (whose
    # headroom already counts the draining ones): reviving needs no new room.
    op = cast(Any, _LedgerOp(reclaimable=2))
    ledger = ConstraintAwareCapacityLedger(
        _snapshot(
            {}, {op: None}, {op: ExecutionResources(gpu=1)}, unknown_pg_headroom={op: 1}
        )
    )
    assert ledger.max_upscale_delta(op) == 3
    ledger.grant(op, 2)  # both revived: the headroom is untouched
    assert ledger._unknown_pg_headroom[op] == 1
    ledger.grant(op, 3)  # 2 revived + 1 new: spends it
    assert ledger._unknown_pg_headroom[op] == 0


def test_ledger_ops_sharing_a_pg_spend_the_same_bundles():
    """4 bundles of {1 GPU, 4 CPU}. Alone, each op fits 4. Jointly they do
    not: every GPU actor also takes a CPU, leaving no bundle with the 4 CPU
    the other op's actor needs -- which a per-op actor cap cannot see."""
    gpu_op, cpu_op = cast(Any, _LedgerOp()), cast(Any, _LedgerOp())
    keys = tuple(("pg1", i) for i in range(4))
    ledger = ConstraintAwareCapacityLedger(
        _snapshot(
            {},
            {},
            {
                gpu_op: ExecutionResources(cpu=1, gpu=1),
                cpu_op: ExecutionResources(cpu=4),
            },
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


def test_rule_claims_reach_the_per_op_passes():
    """Rules debit the tick snapshot's own free maps, so the ledger that
    phase 2 builds afterwards sees what phase 1 claimed."""
    op = cast(Any, _LedgerOp())
    bundle = ("pg1", 0)
    capacity = _snapshot(
        {"n1": ExecutionResources(cpu=4)},
        {op: None},
        {op: ExecutionResources(cpu=1)},
        pg_free={bundle: ExecutionResources(cpu=4)},
    )

    class _Claim(OptimizationRule[None]):
        name = "claim"

        def apply(self, ctx, state):
            ctx.claim_free("n1", ExecutionResources(cpu=1), 3)
            pack_actors(ctx.pg_free, [bundle], ExecutionResources(cpu=1), 1)
            return []

    optimizer = PipelineOptimizer(
        "ds",
        LocalityBasedActorPlacement(),
        _FakeClock(),
        lambda _op: (False, 0, 0),
        OpScalingGuard(),
    )
    optimizer.register(_Claim())
    optimizer.optimize_pipeline(
        cast(Any, {}),
        warmup=True,  # the built-in rules stand down
        capacity=capacity,
        profiles=cast(
            Any, SimpleNamespace(actor_pool_ops=(), non_pg_ops=(), next_ops={})
        ),
        growth_delta={},
        seconds_since_allocation_change=0.0,
    )
    assert ConstraintAwareCapacityLedger(capacity).max_upscale_delta(op) == 1
    assert capacity.pg_free_by_bundle[bundle].cpu == 3


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
    view = DatasetAllocationView(reserved_by_node=alloc, labels_by_node=labels)

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


def test_gpu_nodes_are_scarce_in_node_pressure():
    # A GPU op with NO label_selector still marks GPU nodes as scarce, so a
    # plain CPU op deprioritizes them; a CPU-only node stays unpressured.
    view = DatasetAllocationView(
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
    assert snap.constrained_op_count_by_node.get("g1", 0) == 1
    assert snap.constrained_op_count_by_node.get("n1", 0) == 0


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
        placement_group=cast(Any, _FakePG("pg1"))
    )
    fn = _FakeOp(
        "fn",
        _FakePool(min_size=1, max_size=10, gpu=1, running=1),
        node="n1",
        inputs=[pg],
        uses_ray_remote_args_fn=True,
    )
    labels = {"n1": {}, "n2": {}}
    view = DatasetAllocationView(
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
    tight = DatasetAllocationView(
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
def test_compute_nodes_to_reclaim_nets_out_draining_actors(draining_cpu, expected_over):
    op = _FakeOp(
        "Map",
        _FakePool(min_size=1, max_size=10, cpu=1),
        committed={"n1": ExecutionResources(cpu=8)},
        draining={"n1": ExecutionResources(cpu=draining_cpu)} if draining_cpu else None,
    )
    alloc = {"n1": ExecutionResources(cpu=6)}
    reclaim = SizingTicker.compute_nodes_to_reclaim(alloc, OpProfiles(_topology(op)))
    if expected_over is None:
        assert reclaim == {}
    else:
        assert reclaim["n1"].cpu == expected_over


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
    assert SizingTicker.compute_nodes_to_reclaim(alloc, profiles) == {}


def test_compute_nodes_to_reclaim_over_allocation():
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
    reclaim = SizingTicker.compute_nodes_to_reclaim(alloc, OpProfiles(_topology(op)))
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

    def nodes_to_reclaim(view):
        sizer.reporter.view = view
        return _snapshot_of(sizer).nodes_to_reclaim

    assert nodes_to_reclaim(empty) == {}  # empty before the first grant
    nodes_to_reclaim(grant)  # non-empty grant seen
    assert nodes_to_reclaim(empty) == {}  # first empty read: skeptical
    assert nodes_to_reclaim(grant) == {}  # recovers; counter resets
    assert nodes_to_reclaim(empty) == {}
    assert "node0" in nodes_to_reclaim(empty)  # second consecutive empty


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
            placement_group=cast(Any, _FakePG("pg1"))
        )
    profile = build_op_profile(op)
    for field, value in expected.items():
        assert getattr(profile, field) == value, field
    if pinned:
        assert op.placement_constraint().label_selector == {_NODE_ID_KEY: pinned}
    # One profile per op, cached.
    profiles = OpProfiles(_topology(op))
    assert profiles.profile(op) is profiles.profile(op)


def test_record_tick_emits_every_phase(monkeypatch):
    """An idle tick -- nothing sized, nothing placed -- still reports every
    phase: the optimizer does most of its work on ticks where no operator is
    resized, so a phase set that only appeared on sizing ticks would miss it."""
    observed = []
    telemetry = SizerTelemetry("test-ds")
    monkeypatch.setattr(
        telemetry._metrics,
        "record_tick_duration",
        lambda phase, seconds: observed.append((phase, seconds)),
    )
    telemetry.record_tick(
        {},
        PerOpSizingRequest(request={}),
        PlacedSizingRequest(request={}),
        prepare_s=0.0005,
        cross_op_optimize_s=0.001,
        per_op_how_many_s=0.002,
        per_op_where_s=0.004,
        apply_s=0.008,
        e2e_s=0.016,
    )
    assert [phase for phase, _ in observed] == [
        "prepare",
        "cross_op_optimize",
        "per_op_how_many",
        "per_op_where",
        "apply",
        "e2e",
    ]
    assert dict(observed)["e2e"] == 0.016


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
