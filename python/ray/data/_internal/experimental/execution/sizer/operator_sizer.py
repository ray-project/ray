"""The actor-only backend's operator sizer.

The sizer owns every actor-pool sizing decision under the actor-only backend:
initial pool sizes, per-tick up/downscale deltas, and placement-resolved
requests (which nodes gain actors, which nodes shed them -- see
``ScaleUpRequest`` / ``ScaleDownRequest``). Operators never size themselves:
``ExperimentalActorPoolMapOperator._scale_to_initial_size`` is a no-op for
exactly this reason.

``ExperimentalStreamingExecutor`` builds the sizer and calls
``initial_sizing_request`` once before the scheduling loop starts, then
``perform_sizing_decision`` once per scheduling-loop step (after completed
tasks are processed, before new tasks are launched).
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from typing import TYPE_CHECKING, Callable, Dict, Optional, Sequence, Set, Tuple

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import SIZER_CADENCE_S
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    PlacedScalingRequest,
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    ActorPlacementStrategy,
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
    TickContext,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
    bounded_max_size,
)
from ray.data._internal.experimental.execution.sizer.operator_ordering import (
    ordered_actor_pool_ops,
)
from ray.data._internal.experimental.execution.sizer.optimizer import PipelineOptimizer
from ray.data._internal.experimental.execution.sizer.pg_bundles import (
    PlacementGroupBundles,
)
from ray.data._internal.experimental.execution.sizer.scale_how_many import (
    OpSizingPolicy,
)
from ray.data._internal.experimental.execution.sizer.scale_where import (
    build_placement_views,
    compute_favored_nodes,
    evict_from_reclaimed_nodes,
    place_downscales,
    place_upscales,
)
from ray.data._internal.experimental.execution.sizer.scaling_guard import OpScalingGuard
from ray.data._internal.experimental.execution.sizer.telemetry import SizerTelemetry
from ray.data._internal.experimental.execution.sizer.tick import SizingTicker

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.actor_only_resource_reporter import (  # noqa: E501
        ActorOnlyResourceReporter,
        DatasetAllocationView,
    )

logger = logging.getLogger(__name__)


class OperatorSizer:
    """Owns how many actors every operator in the topology has, and where.

    One evaluation per sizing tick, in four phases:

    1. CROSS-OP OPTIMIZE  Topology-wide corrections (``PipelineOptimizer``): move
                 capacity between operators, and grow an operator whose own
                 signals cannot ask for it -- one that reads output
                 backpressured while its downstream is in fact keeping up
                 (``SilentBottleneckGrant``).
    2. PER-OP HOW MANY    Per-operator greedy up/downscale deltas from queue
                 depth and backpressure (``OpSizingPolicy``), clamped to what
                 the cluster can place (``ConstraintAwareCapacityLedger``).
    3. PER-OP WHERE       Each delta resolved to nodes -- where new actors land,
                 where actors are shed -- by the ``ActorPlacementStrategy``.
                 The pool picks the actors on each node.
    4. APPLY     The resolved requests handed to the pools.

    Phase 1 is the odd one out: it emits requests that already name their
    nodes, and applies them itself, so phases 2 and 3 never see them.

    Alongside the phases it gives back capacity when the coordinator shrinks
    this dataset's slice: the tick reports where we now hold more than we are
    allowed, and WHERE sheds off those nodes before it places anything new.

    Everything a phase decides lives in the phase's own component; this class
    only sequences them.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        topology: "Topology",
        resource_reporter: "ActorOnlyResourceReporter",
        clock: Callable[[], float] = time.monotonic,
    ):
        """Validate the topology and build the per-run state.

        ``resource_reporter`` is read here, once, for placement-group capacity
        and to warm the allocation cache. After this the sizer never
        touches it: every tick is handed the cluster view the scheduling loop
        already read (see ``perform_sizing_decision``).
        """
        self._dataset_id = dataset_id
        self._topology = topology
        self._clock = clock
        self._placement: ActorPlacementStrategy = LocalityBasedActorPlacement()

        self._validate_topology(topology)

        # Static per-op traits (resource type, pool cap, PG-managed or not), and
        # the operator listings every phase shares.
        self._profiles = OpProfiles(topology)
        # Keeps the optimizer's and the warm loop's decisions off each other.
        # One clock for all three, so a faked clock in a test moves guard
        # expiry, tick cadence and rule deadlines together.
        self._guard = OpScalingGuard(clock)
        # Warm the non-blocking allocation cache, so the first ticks read a
        # real allocation instead of the cold-cache empty value (the client
        # submits an async RPC and serves the answer on a later call).
        resource_reporter.get_reserved_resources_by_node()
        pg_bundles, unknown_pg_cap_by_op = self._read_pg_capacity(resource_reporter)
        self._policy = OpSizingPolicy(
            self._profiles,
            pg_bundles=pg_bundles,
            unknown_pg_cap_by_op=unknown_pg_cap_by_op,
        )
        self._telemetry = SizerTelemetry(dataset_id)
        self._optimizer = PipelineOptimizer(
            dataset_id,
            self._placement,
            clock,
            self._policy.is_output_backpressured,
            self._guard,
        )
        self._ticker = SizingTicker(
            self._profiles,
            cadence_s=SIZER_CADENCE_S,
            clock=clock,
            pg_bundles=pg_bundles,
            unknown_pg_cap_by_op=unknown_pg_cap_by_op,
        )

    # --- lifecycle ------------------------------------------------------------

    def initial_sizing_request(self) -> None:
        """Bring every pool up to its initial (min) size, untargeted."""
        for op in ordered_actor_pool_ops(self._topology, self._profiles.actor_pool_ops):
            pool = op.actor_pool
            delta = pool.initial_size() - pool.serving_size()
            if delta > 0:
                pool.scale(
                    ScaleUpRequest.untargeted(
                        delta, reason="scaling to initial size (sizer-ordered)"
                    )
                )
            op.wait_for_min_actors()

    # --- one sizing evaluation ------------------------------------------------

    def perform_sizing_decision(
        self,
        *,
        cluster_view: "DatasetAllocationView",
        seconds_since_allocation_change: float,
    ) -> None:
        """Called every scheduling-loop step. Signal sampling runs on every
        call; everything else is gated to ``SIZER_CADENCE_S`` by the ticker.

        The cluster view is read once per step by the scheduling loop and
        handed in, so the bank and every phase of a tick agree on it.
        """
        t0 = time.perf_counter()
        self._observe()
        ctx = self._ticker.next_tick(cluster_view)
        t1 = time.perf_counter()
        if ctx is None:
            return

        self._cross_op_optimize(ctx, seconds_since_allocation_change)
        t2 = time.perf_counter()
        how_many = self._per_op_how_many(ctx)
        t3 = time.perf_counter()
        where = self._per_op_where(how_many, ctx)
        t4 = time.perf_counter()
        self._apply(where)
        t5 = time.perf_counter()
        self._telemetry.record_tick(
            self._topology,
            how_many,
            where,
            prepare_s=t1 - t0,
            cross_op_optimize_s=t2 - t1,
            per_op_how_many_s=t3 - t2,
            per_op_where_s=t4 - t3,
            apply_s=t5 - t4,
            e2e_s=t5 - t0,
        )

    def _observe(self) -> None:
        """Sample the signals the optimizer decides on."""
        self._optimizer.observe(self._profiles.actor_pool_ops)

    def _cross_op_optimize(
        self, ctx: TickContext, seconds_since_allocation_change: float
    ) -> None:
        """Phase 1: apply the cross-operator corrections.

        These land before the per-op passes on purpose: a correction's claims
        are targeted pendings in committed accounting, so the passes below
        already see the space they reserved. Operators a rule claimed are
        blocked on the guard, so the warm loop leaves them alone this tick.
        """
        for placed in self._optimizer.optimize_pipeline(
            self._topology,
            warmup=ctx.warmup,
            capacity=ctx.capacity,
            profiles=self._profiles,
            growth_delta={
                op: self._policy.growth_delta(op, self._topology[op])
                for op in self._profiles.actor_pool_ops
            },
            seconds_since_allocation_change=seconds_since_allocation_change,
        ):
            self._apply(placed)

    def _per_op_how_many(self, ctx: TickContext) -> PerOpSizingRequest:
        """Phase 2: how many actors each operator gains or loses this tick.

        Two passes over the operators, in the tick's shared order:

        - WANT    each op's desired delta from its own signals alone (see
                  OpSizingPolicy). Capacity is not consulted, so order does
                  not matter here.
        - GRANT   clamp each want to what the cluster can hold and debit it.
                  Grant order is placement order, so anything granted here is
                  placeable in phase 3 by construction; what does not fit is
                  recorded as a shortfall for the optimizer.

        Capacity this tick's downscales will free is not credited: a running
        victim holds its resources until it is dead, so the ledger sees it
        on a later tick.
        """
        ledger = ConstraintAwareCapacityLedger(ctx.capacity)
        request: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest] = {}
        # How much each op asked for and did not get. The optimizer cannot
        # observe this: from pool state alone, "did not want to grow" and
        # "could not grow" look the same, so the ask is only knowable here.
        shortfalls: Dict[ExperimentalActorPoolMapOperator, OpShortfall] = {}
        # Ops that asked to grow and were granted zero. Only these build the
        # transfer streak, so an op that is still growing doesn't pull
        # capacity from others.
        ungranted_ops: Set[ExperimentalActorPoolMapOperator] = set()
        # Ops backpressured while below demand: no evidence either way, so the
        # streak freezes rather than resets.
        backpressured_with_demand: Set[ExperimentalActorPoolMapOperator] = set()

        wants = self._collect_wants(ctx, backpressured_with_demand)
        self._grant_within_capacity(
            ctx.ordered_ops, wants, ledger, request, shortfalls, ungranted_ops
        )

        self._optimizer.record_sizing_results(
            ordered_ops=list(ctx.ordered_ops),
            ungranted_ops=ungranted_ops,
            shortfalls=shortfalls,
            backpressured_with_demand=backpressured_with_demand,
        )
        self._telemetry.log_request_summary(request)
        return PerOpSizingRequest(request=request)

    def _collect_wants(
        self,
        ctx: TickContext,
        backpressured_with_demand: Set[ExperimentalActorPoolMapOperator],
    ) -> Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest]:
        """WANT pass: each op's desired delta from its own signals.

        A pool below min_size wants at least enough to reach it (fixed pools
        included). Completed ops are left to GRANT, which releases them.
        """
        wants: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest] = {}
        for op in ctx.ordered_ops:
            pool = op.actor_pool
            if op.has_completed():
                continue
            if not self._guard.can_scale(op):
                # An optimizer rule has claimed this op (e.g. mid-transfer).
                # Leave it alone so our pendings aren't mistaken for its claim.
                continue
            want: Optional[ActorPoolScalingRequest] = None
            if pool.min_size() == pool.max_size():
                pass  # fixed pool: only ever replenished to min
            elif ctx.warmup:
                want = self._policy.warmup_delta(op, ctx.capacity.get_global_limits())
            else:
                want = self._policy.warm_delta(
                    op, self._topology[op], backpressured_with_demand
                )
            below_min = pool.min_size() - pool.serving_size()
            if below_min > 0 and (want is None or want.delta < below_min):
                want = ActorPoolScalingRequest.upscale(
                    delta=below_min, reason="below min size"
                )
            if want is not None:
                wants[op] = want
        return wants

    def _grant_within_capacity(
        self,
        ordered_ops: Sequence[ExperimentalActorPoolMapOperator],
        wants: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest],
        ledger: ConstraintAwareCapacityLedger,
        request: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest],
        shortfalls: Dict[ExperimentalActorPoolMapOperator, OpShortfall],
        ungranted_ops: Set[ExperimentalActorPoolMapOperator],
    ) -> None:
        """GRANT pass: clamp each want to what fits, in placement order."""
        for op in ordered_ops:
            pool = op.actor_pool

            # Completed: release the whole pool, pending and running alike
            # (force=True skips the debounce). Running actors drain before
            # they are released, so in-flight work still finishes.
            if op.has_completed():
                if pool.serving_size() > 0:
                    self._telemetry.log_completed_release(op, pool.serving_size())
                    request[op] = ActorPoolScalingRequest.downscale(
                        delta=-pool.serving_size(),
                        force=True,
                        reason="operator completed",
                    )
                continue

            want = wants.get(op)
            if want is None:
                continue
            if want.delta < 0:
                request[op] = want
                continue
            # Upscale: clamp to schedulable capacity. Warmup shares are already
            # bounded, but clamping them too keeps a constrained op from
            # getting an all-cluster share it cannot place.
            delta = min(want.delta, ledger.max_upscale_delta(op))
            unplaced = want.delta - max(0, delta)
            if unplaced > 0:
                shortfalls[op] = OpShortfall(num_actors=unplaced)
                if delta <= 0:
                    ungranted_ops.add(op)
            if delta > 0:
                request[op] = ActorPoolScalingRequest.upscale(
                    delta=delta, reason=want.reason
                )
                ledger.grant(op, delta)

    def _per_op_where(
        self, request: PerOpSizingRequest, ctx: TickContext
    ) -> PlacedSizingRequest:
        """Phase 3: resolve each delta to nodes.

        1. shed off nodes being removed from the dataset's allocation
        2. resolve downscales to the nodes they shed from
        3. resolve upscales to the nodes new actors land on

        Stateless across ticks: placement is re-derived from current state
        every time, so a failed placement or a slow drain simply retries when
        scale_how_many re-emits its delta next tick.
        """
        if not request.request and not ctx.capacity.nodes_to_reclaim:
            return PlacedSizingRequest(request={})

        cluster = ctx.capacity
        reserved = dict(cluster.reserved_by_node)
        # Copy: place_upscales spends from this map; the snapshot stays intact.
        free = {node_id: res.copy() for node_id, res in cluster.free_by_node.items()}
        placed: Dict[ExperimentalActorPoolMapOperator, PlacedScalingRequest] = {}

        # Give back nodes we are losing first -- that takes precedence over
        # this tick's sizing for the same op, which re-requests next tick.
        evicted_ops = evict_from_reclaimed_nodes(
            placed=placed,
            nodes_to_reclaim=ctx.capacity.nodes_to_reclaim,
            profiles=self._profiles,
        )
        views = build_placement_views(request=request.request, capacity=cluster)
        # Nodes the capacity-short upscalers want freed, so downscales shed
        # where the freed capacity actually helps.
        favored = compute_favored_nodes(
            request=request.request,
            views=views,
            reserved=reserved,
            free=free,
            placement=self._placement,
            profiles=self._profiles,
            ordered_ops=ctx.ordered_ops,
        )
        # Downscales first: cancelling a pending refunds capacity this tick's
        # upscales can immediately reuse.
        place_downscales(
            placed=placed,
            request=request.request,
            views=views,
            favored=favored,
            free=free,
            placement=self._placement,
            evicted_ops=evicted_ops,
        )
        place_upscales(
            placed=placed,
            request=request.request,
            views=views,
            free=free,
            placement=self._placement,
            profiles=self._profiles,
            ordered_ops=ctx.ordered_ops,
            evicted_ops=evicted_ops,
            telemetry=self._telemetry,
        )

        self._telemetry.log_placement(reserved, free, placed, self._placement)
        return PlacedSizingRequest(request=placed)

    def _apply(self, request: PlacedSizingRequest) -> None:
        """Phase 4: hand the resolved requests to the pools."""
        # Downscales before upscales, deterministically.
        for op, req in sorted(
            request.request.items(), key=lambda entry: entry[1].delta
        ):
            logger.debug(
                "scale: %s delta=%d nodes=%s",
                op.name,
                req.delta,
                dict(Counter(req.nodes)),
            )
            op.actor_pool.scale(req)

    # --- construction-time validation -----------------------------------------

    def _read_pg_capacity(
        self, resource_reporter: "ActorOnlyResourceReporter"
    ) -> Tuple[PlacementGroupBundles, Dict[ExperimentalActorPoolMapOperator, int]]:
        """Capacity for PG-managed ops, read once: bundle shapes are fixed at
        PG creation.

        An op's PG capacity is unknown when it mints a PG per actor (no bundles
        to inspect) or its PG has no bundles in the view yet (not created, or
        the PG table unreadable). Either way it is capped at its pool's max
        size, and Ray core still places its actors into the PG.
        """
        ops = self._profiles.actor_pool_ops
        resource_reporter.prefetch_pg_bundles(
            [
                pg
                for op in ops
                if (pg := op.placement_constraint().placement_group) is not None
            ]
        )
        view = resource_reporter.get_cluster_view(include_pg_bundles=True)
        pg_bundles = PlacementGroupBundles.measure(ops, view)
        unknown_pg_caps = {
            op: bounded_max_size(op.actor_pool)
            for op in ops
            if op.placement_constraint().is_pg_managed
            and op not in pg_bundles.bundles_by_op
        }
        return pg_bundles, unknown_pg_caps

    @staticmethod
    def _validate_topology(topology: "Topology") -> None:
        """Reject operator configurations the sizer cannot manage.

        Two rejections: ``ray_remote_args`` it cannot reason about (the sizer
        owns placement, so a custom scheduling strategy or a custom resource
        would be mis-placed), and an actor requesting neither CPU nor GPU,
        which warmup cannot size against and Ray cannot schedule. PG-managed
        ops are exempt from the second: their placement group places them.
        """
        for op in topology:
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            op.validate_ray_remote_args()
            if op.placement_constraint().is_pg_managed:
                continue
            per_actor = op.actor_pool.per_actor_resource_usage()
            if not (per_actor.cpu > 0 or per_actor.gpu > 0):
                raise ValueError(
                    f"OperatorSizer requires each actor to request CPU or "
                    f"GPU; {op.name} requests neither "
                    f"(cpu={per_actor.cpu}, gpu={per_actor.gpu})."
                )
