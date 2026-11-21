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
tasks are processed, before new tasks are launched), and
``observe_after_launch`` once per step after it launches tasks.
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from typing import TYPE_CHECKING, Callable, Dict, Optional, Sequence, Set

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
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    BottleneckObserver,
    BottleneckTick,
)
from ray.data._internal.experimental.execution.sizer.capacity_ledger import (
    ConstraintAwareCapacityLedger,
)
from ray.data._internal.experimental.execution.sizer.lifecycle import OpLifecycles
from ray.data._internal.experimental.execution.sizer.locality_actor_placement import (
    LocalityBasedActorPlacement,
)
from ray.data._internal.experimental.execution.sizer.metrics import (
    OptimizerMetrics,
)
from ray.data._internal.experimental.execution.sizer.models import (
    PerOpSizingRequest,
    PlacedSizingRequest,
    TickContext,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
)
from ray.data._internal.experimental.execution.sizer.operator_ordering import (
    ordered_actor_pool_ops,
)
from ray.data._internal.experimental.execution.sizer.policy.bottleneck_policy import (  # noqa: E501
    BottleneckPolicy,
)
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.decision import (
    classify_phase,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    Decision,
    SizerPhase,
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
from ray.data._internal.experimental.execution.sizer.telemetry import SizerTelemetry
from ray.data._internal.experimental.execution.sizer.tick import SizingTicker

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.actor_only_resource_reporter import (  # noqa: E501
        DatasetAllocationView,
    )

logger = logging.getLogger(__name__)


class OperatorSizer:
    """Owns how many actors every operator in the topology has, and where.

    A state machine, evaluated once per sizing tick:

    - ``SizerPhase`` (``classify_phase``): WARMUP until every critical op has
      produced output, MODEL_WARMING until the bottleneck model has seen a
      full window of every unfinished op, then STEADY.
    - ``Decision`` (``policy.decision.classify``): the tick's one primary
      action from the bottleneck model -- grow a bottleneck into free
      capacity, grow balanced stages, fund the recipient from donors, shrink
      blocked surplus, or hold.
    - ``TransferPhase``: whether a transfer is open (``TransferBook``).
    - ``OpLifecycle`` per op: whether it may be resized now.

    Each tick runs in four steps:

    1. DECIDE    Advance open transfers, classify the decision and carry it
                 out (``BottleneckPolicy``). Its requests already name their
                 nodes and are applied at once, so the steps below see the
                 capacity they claimed.
    2. HOW MANY  Per-op wants that do not need the model (``OpSizingPolicy``):
                 equal-share growth before the model has data for an op,
                 idle shedding, min-size refill, the grant to a released
                 held op, and releasing completed pools -- clamped to what
                 the cluster can place (``ConstraintAwareCapacityLedger``).
    3. WHERE     Each how-many delta resolved to nodes by the
                 ``ActorPlacementStrategy``; the pool picks the actors.
    4. APPLY     The resolved requests handed to the pools.

    Alongside, it gives back capacity when the coordinator shrinks this
    dataset's slice: the tick reports where we now hold more than we are
    allowed, and WHERE sheds off those nodes before it places anything new.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        clock: Callable[[], float] = time.monotonic,
    ):
        """State that needs no topology. ``bootstrap`` builds the rest. The
        sizer never reads the resource reporter: every tick is handed the
        cluster view the scheduling loop already read (see
        ``perform_sizing_decision``).
        """
        self.phase = SizerPhase.WARMUP

        self._dataset_id = dataset_id
        self._clock = clock
        self._placement: ActorPlacementStrategy = LocalityBasedActorPlacement()

        self._held_actors_by_op: Dict[ExperimentalActorPoolMapOperator, int] = {}
        self._grow_to_held: Dict[ExperimentalActorPoolMapOperator, int] = {}
        self._modeled_ops: Set[ExperimentalActorPoolMapOperator] = set()
        self._bottleneck: Optional[BottleneckTick] = None
        self._decision: Optional[Decision] = None
        self._telemetry = SizerTelemetry(dataset_id)

    # --- lifecycle ------------------------------------------------------------

    def bootstrap(self, topology: "Topology") -> None:
        """Build the per-run state over ``topology``, then bring every pool up
        to its initial (min) size."""
        self._validate_topology(topology)
        self._topology = topology
        self._profiles = OpProfiles(topology)
        self._lifecycles = OpLifecycles(self._profiles.actor_pool_ops)

        self._policy = BottleneckPolicy(
            metrics=OptimizerMetrics(self._dataset_id),
            placement=self._placement,
            lifecycles=self._lifecycles,
        )
        self._how_many = OpSizingPolicy(profiles=self._profiles)
        self._bottleneck_observer = BottleneckObserver(
            profiles=self._profiles, clock=self._clock
        )
        self._ticker = SizingTicker(
            profiles=self._profiles,
            cadence_s=SIZER_CADENCE_S,
            clock=self._clock,
            # Held ops reserve their warmup share.
            hold_target=self._how_many.warmup_target,
        )
        self._scale_to_initial_size()

    def _scale_to_initial_size(self) -> None:
        """Bring every pool up to its initial (min) size, untargeted, in
        scarcity order."""
        for op in ordered_actor_pool_ops(self._topology, self._profiles.actor_pool_ops):
            if not self._lifecycles.can_scale(op):
                continue
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
        """Called every scheduling-loop step. Lifecycle transitions run on
        every call; everything else is gated to ``SIZER_CADENCE_S`` by the
        ticker.

        The cluster view is read once per step by the scheduling loop and
        handed in, so the bank and every step of a tick agree on it.
        """
        t0 = time.perf_counter()
        self._lifecycles.on_step()
        ctx = self._ticker.next_tick(cluster_view)
        t1 = time.perf_counter()
        if ctx is None:
            return
        self._note_held_ops(ctx)
        # The bottleneck model, once per tick, before any decision debits
        # free capacity: the policy acts on it and the log line shows it.
        self._bottleneck = self._bottleneck_observer.tick(
            ctx.capacity, self._policy.held_capacity_by_node()
        )
        self._bottleneck_observer.log_tick(self._bottleneck)
        self._advance_phase(ctx)

        self._decide(ctx, seconds_since_allocation_change)
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
            decide_s=t2 - t1,
            per_op_how_many_s=t3 - t2,
            per_op_where_s=t4 - t3,
            apply_s=t5 - t4,
            e2e_s=t5 - t0,
        )

    def _advance_phase(self, ctx: TickContext) -> None:
        """Fold this tick's model into the ops it has seen, and move the
        phase forward."""
        if self._bottleneck is not None:
            self._modeled_ops.update(self._bottleneck.snapshot.shares)
        unmodeled = {
            op
            for op in self._profiles.actor_pool_ops
            if op not in self._modeled_ops and not op.has_completed()
        }
        phase = classify_phase(ctx.warmup, unmodeled)
        if phase is not self.phase:
            logger.info("OperatorSizer: phase %s -> %s", self.phase.value, phase.value)
            self.phase = phase

    def _awaits_model(self, op: ExperimentalActorPoolMapOperator) -> bool:
        """Whether ``op`` still grows toward its equal share: in warmup, or
        after it while the model has not had a full window of it."""
        return self.phase is SizerPhase.WARMUP or (
            self.phase is SizerPhase.MODEL_WARMING and op not in self._modeled_ops
        )

    def _note_held_ops(self, ctx: TickContext) -> None:
        """Queue a one-time grant up to the held count for every op whose
        hold released since last tick, then remember this tick's holds."""
        held_now = ctx.capacity.held_actors_by_op
        for op, held in self._held_actors_by_op.items():
            if op not in held_now and not op.has_completed():
                self._grow_to_held[op] = held
        self._held_actors_by_op = dict(held_now)

    def observe_after_launch(self) -> None:
        """Sample the bottleneck observer's slot shares. Called by the
        scheduling loop once per step after it launches tasks, not at the
        start of ``perform_sizing_decision``.

        The observer reads each op's slots at one instant. Before the launch,
        every slot a task freed this step is still empty, though the launch
        refills it moments later: a consumer looks like it has an idle slot,
        and its producer's output-backpressured time counts as self-limited
        instead of blocked by the consumer. Consumers whose tasks finish every
        few seconds would almost always look that way. After the launch, a
        slot is empty only if the consumer had no work for it.
        """
        self._bottleneck_observer.observe(self._topology)

    def _decision_context(
        self, ctx: TickContext, seconds_since_allocation_change: float
    ) -> DecisionContext:
        capacity = ctx.capacity
        return DecisionContext(
            topology=self._topology,
            phase=self.phase,
            # Shared, not copied (here and the PG maps below): the policy
            # debits what it claims, so the how-many pass later this tick
            # (which copies from the same snapshot) doesn't hand it out again.
            free=capacity.free_by_node,
            pg_free=capacity.pg_free_by_bundle,
            schedulable_bundles=capacity.schedulable_bundles_by_op,
            unknown_pg_headroom=capacity.unknown_pg_headroom_by_op,
            actor_pool_ops=self._profiles.actor_pool_ops,
            non_pg_ops=self._profiles.non_pg_ops,
            schedulable_nodes=capacity.schedulable_nodes_by_op,
            growth_ceiling={
                op: self._growth_ceiling(op) for op in self._profiles.actor_pool_ops
            },
            next_ops=self._profiles.next_ops,
            seconds_since_allocation_change=seconds_since_allocation_change,
            now=self._clock(),
            bottleneck=self._bottleneck,
        )

    def _growth_ceiling(self, op: ExperimentalActorPoolMapOperator) -> int:
        """The pool's bounded max size, or less once every upstream op has
        completed and the remaining work needs fewer actors."""
        ceiling = op.actor_pool.bounded_max_size()
        remaining = self._how_many.remaining_work_cap(op, self._topology[op])
        return ceiling if remaining is None else min(ceiling, remaining)

    def _decide(self, ctx: TickContext, seconds_since_allocation_change: float) -> None:
        """Step 1: classify this tick's decision and carry it out.

        This lands before how_many on purpose: a decision debits what it claims
        (or holds for a later tick) off the shared free map, and what it
        places becomes targeted pendings in committed accounting, so how_many
        already sees the space it reserved. Ops a transfer claimed are no
        longer ACTIVE, so how_many leaves them alone this tick.
        """
        self._decision, requests = self._policy.step(
            self._decision_context(ctx, seconds_since_allocation_change)
        )
        for placed in requests:
            self._apply(placed)

    def _per_op_how_many(self, ctx: TickContext) -> PerOpSizingRequest:
        """Step 2: the per-op wants that do not need the model.

        Two passes over the operators, in the tick's shared order:

        - WANT    each op's desired delta from its own state alone (see
                  OpSizingPolicy). Capacity is not consulted, so order does not
                  matter here.
        - GRANT   clamp each want to what the cluster can hold and debit it.
                  Grant order is placement order, so anything granted here is
                  placeable in step 3 by construction.

        Capacity this tick's downscales will free is not credited: a running
        victim holds its resources until it is dead, so the ledger sees it
        on a later tick.
        """
        ledger = ConstraintAwareCapacityLedger(ctx.capacity)
        request: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest] = {}
        wants = self._collect_wants(ctx)
        self._grant_within_capacity(ctx.ordered_ops, wants, ledger, request)
        self._telemetry.log_request_summary(request)
        return PerOpSizingRequest(request=request)

    def _collect_wants(
        self, ctx: TickContext
    ) -> Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest]:
        """WANT pass: each op's desired delta from its own state.

        A pool below min_size wants at least enough to reach it (fixed pools
        included). Completed ops are left to GRANT, which releases them.
        """
        wants: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest] = {}
        for op in ctx.ordered_ops:
            pool = op.actor_pool
            if op.has_completed():
                continue
            if not self._lifecycles.can_scale(op):
                # A transfer holds this op, or its ``on_start`` hasn't run
                # yet. Leave it alone so our pendings aren't mistaken for
                # the transfer's.
                continue
            want: Optional[ActorPoolScalingRequest] = None
            if pool.min_size() == pool.max_size():
                pass  # fixed pool: only ever replenished to min
            elif self._awaits_model(op):
                want = self._how_many.warmup_delta(
                    op, ctx.capacity.get_global_limits(), ctx.capacity.pg_slots
                )
            else:
                # Growth comes only from the bottleneck policy; per op, only
                # idle actors are shed.
                want = self._how_many.shrink_idle_delta(op, self._topology[op])
            # Released from a hold: grow straight into the room held for it
            # while it waited (its warmup share), once. Capped below, like any
            # want, at the work left once upstream is done.
            held = self._grow_to_held.pop(op, 0)
            to_held = held - pool.serving_size()
            if to_held > 0 and (want is None or want.delta < to_held):
                want = ActorPoolScalingRequest.upscale(
                    delta=to_held, reason="held share (hold released)"
                )
            want = self._cap_at_remaining_work(op, want)
            below_min = pool.min_size() - pool.serving_size()
            if below_min > 0 and (want is None or want.delta < below_min):
                want = ActorPoolScalingRequest.upscale(
                    delta=below_min, reason="below min size"
                )
            if want is not None:
                wants[op] = want
        return wants

    def _cap_at_remaining_work(
        self,
        op: ExperimentalActorPoolMapOperator,
        want: Optional[ActorPoolScalingRequest],
    ) -> Optional[ActorPoolScalingRequest]:
        """Trim an upscale to the actors ``op``'s remaining work can use."""
        if want is None or want.delta <= 0:
            return want
        cap = self._how_many.remaining_work_cap(op, self._topology[op])
        if cap is None:
            return want
        allowed = cap - op.actor_pool.serving_size()
        if allowed <= 0:
            return None
        return ActorPoolScalingRequest.upscale(
            delta=min(want.delta, allowed), reason=want.reason
        )

    def _grant_within_capacity(
        self,
        ordered_ops: Sequence[ExperimentalActorPoolMapOperator],
        wants: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest],
        ledger: ConstraintAwareCapacityLedger,
        request: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest],
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
            if delta > 0:
                request[op] = ActorPoolScalingRequest.upscale(
                    delta=delta, reason=want.reason
                )
                ledger.grant(op, delta)

    def _per_op_where(
        self, request: PerOpSizingRequest, ctx: TickContext
    ) -> PlacedSizingRequest:
        """Step 3: resolve each delta to nodes.

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
        """Step 4: hand the resolved requests to the pools."""
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
