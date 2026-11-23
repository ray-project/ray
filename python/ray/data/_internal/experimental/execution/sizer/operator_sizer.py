"""The actor-only backend's operator sizer.

The sizer owns every actor-pool sizing decision under the actor-only backend:
initial pool sizes, per-tick up/downscale deltas, and placement-resolved
requests (which nodes gain actors, which nodes shed them -- see
``ScaleUpRequest`` / ``ScaleDownRequest``). Operators never size themselves:
``ExperimentalActorPoolMapOperator._scale_to_initial_size`` is a no-op for
exactly this reason.

``ExperimentalStreamingExecutor`` builds the sizer and calls ``bootstrap``
once before the scheduling loop starts. Then, once per scheduling-loop step
(after completed tasks are processed, before new tasks are launched), it
calls ``maybe_build_cluster_state_snapshot``, and ``perform_sizing_decision``
with the snapshot when one is due; and ``observe_after_launch`` once per step
after it launches tasks.
"""

from __future__ import annotations

import logging
import math
import time
from collections import Counter
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Set

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import (
    SIZER_CADENCE_S,
    SIZER_WARMUP_TIMEOUT_S,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    PlacedScalingRequest,
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    ActorPlacementStrategy,
    pack_actors,
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
    ClusterStateSnapshot,
    PerOpSizingRequest,
    PlacedSizingRequest,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
    critical_ops,
    has_produced_output,
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

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.actor_only_resource_reporter import (  # noqa: E501
        DatasetReservationView,
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
        cadence_s: float = SIZER_CADENCE_S,
        warmup_timeout_s: float = SIZER_WARMUP_TIMEOUT_S,
    ):
        """State that needs no topology. ``bootstrap`` builds the rest. The
        sizer never reads the resource reporter: every tick is handed the
        cluster view the scheduling loop already read (see
        ``perform_sizing_decision``).
        """
        self.phase = SizerPhase.WARMUP

        self._dataset_id = dataset_id
        self._clock = clock
        # Sizing cadence: the executor calls in every scheduling-loop step
        # (~100ms), but queue signals are meaningless below ~1s and actor
        # startup dominates anything we decide, so a snapshot is built (and a
        # decision made) at most once per ``cadence_s``.
        self.cadence_s = cadence_s
        # -inf rather than 0: time.monotonic()'s epoch is arbitrary, so only
        # this reads unambiguously as "never evaluated".
        self._last_tick_t: float = float("-inf")
        self._warned_loop_lag = False
        # WARMUP ends once every critical op has produced output, or after
        # this long since the first tick (``_warmup_complete``).
        self._warmup_timeout_s = warmup_timeout_s
        self._warmup_started_t: Optional[float] = None
        # An empty reservation is usually a cold coordinator cache, not a
        # revoked slice (``_trust_empty_reservation``).
        self._reservation_seen_nonempty = False
        self._consecutive_empty_reservations = 0
        # How long the last snapshot took to build (telemetry).
        self._build_s = 0.0
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
        self._critical_ops = critical_ops(topology)
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

    def maybe_build_cluster_state_snapshot(
        self,
        *,
        cluster_view: "DatasetReservationView",
        seconds_since_allocation_change: float,
    ) -> Optional[ClusterStateSnapshot]:
        """Called every scheduling-loop step. Runs the per-step lifecycle
        transitions (counted in steps, so they must not wait for the
        cadence), then builds this tick's snapshot, or returns None while
        inside the cadence window.

        The cluster view is read once per step by the scheduling loop and
        handed in, so the bank and every step of a tick agree on it.
        """
        self._lifecycles.on_step()
        now = self._clock()
        if now - self._last_tick_t < self.cadence_s:
            return None
        self._warn_if_loop_lagging(now)
        self._last_tick_t = now
        t0 = time.perf_counter()
        snapshot = ClusterStateSnapshot.build(
            cluster_view,
            self._profiles,
            seconds_since_allocation_change=seconds_since_allocation_change,
            trust_empty_reservation=self._trust_empty_reservation(cluster_view),
        )
        self._build_s = time.perf_counter() - t0
        return snapshot

    def perform_sizing_decision(
        self, *, cluster_state_snapshot: ClusterStateSnapshot
    ) -> None:
        """One sizing tick against the snapshot: hold room for held ops, run
        the bottleneck model, advance the phase, then decide, how many,
        where, apply. The snapshot's free maps are debited along the way."""
        snapshot = cluster_state_snapshot
        t0 = time.perf_counter()
        self._note_held_ops(self._hold_for_held_ops(snapshot))
        # The bottleneck model, once per tick, before any decision debits
        # free capacity: the policy acts on it and the log line shows it.
        self._bottleneck = self._bottleneck_observer.tick(
            snapshot, self._policy.held_capacity_by_node()
        )
        self._bottleneck_observer.log_tick(self._bottleneck)
        self._advance_phase()
        t1 = time.perf_counter()

        self._decide(snapshot)
        t2 = time.perf_counter()
        how_many = self._per_op_how_many(snapshot)
        t3 = time.perf_counter()
        where = self._per_op_where(how_many, snapshot)
        t4 = time.perf_counter()
        self._apply(where)
        t5 = time.perf_counter()
        self._telemetry.record_tick(
            self._topology,
            how_many,
            where,
            prepare_s=self._build_s + (t1 - t0),
            decide_s=t2 - t1,
            per_op_how_many_s=t3 - t2,
            per_op_where_s=t4 - t3,
            apply_s=t5 - t4,
            e2e_s=self._build_s + (t5 - t0),
        )

    # --- cadence and reservation trust ------------------------------------------

    def _warn_if_loop_lagging(self, now: float) -> None:
        """The gap between ticks measures the control loop, since the executor
        calls in every step. A slow loop delays every sizing decision, so make
        it visible."""
        since = now - self._last_tick_t
        if not math.isfinite(since):
            return  # first tick of the run
        if since <= 1.5 * self.cadence_s or self._warned_loop_lag:
            return
        self._warned_loop_lag = True
        logger.warning(
            "OperatorSizer: scheduling-loop lag -- %.1fs since last sizer tick "
            "(cadence %.1fs); a slow control loop delays every sizing decision. "
            "Logged once.",
            since,
            self.cadence_s,
        )

    def _trust_empty_reservation(self, view: "DatasetReservationView") -> bool:
        """Whether this tick may reclaim on an empty reservation.

        An empty reservation usually means the coordinator cache has not been
        filled yet, not that our slice was revoked, so we do not evict on one
        until it has persisted for two ticks. A non-empty read is always acted
        on -- a partial shrink is real.
        """
        if view.reserved_by_node:
            self._reservation_seen_nonempty = True
            self._consecutive_empty_reservations = 0
            return True
        if not self._reservation_seen_nonempty:
            logger.debug(
                "OperatorSizer: reservation empty before first grant -- "
                "skipping reclaim this tick."
            )
            return False
        self._consecutive_empty_reservations += 1
        if self._consecutive_empty_reservations < 2:
            logger.debug(
                "OperatorSizer: reservation read empty after a prior grant -- "
                "holding evictions one tick in case it is transient."
            )
            return False
        return True

    # --- phase ------------------------------------------------------------------

    def _advance_phase(self) -> None:
        """Fold this tick's model into the ops it has seen, and move the
        phase forward. WARMUP is left for good once ``_warmup_complete``."""
        if self._bottleneck is not None:
            self._modeled_ops.update(self._bottleneck.snapshot.shares)
        unmodeled = {
            op
            for op in self._profiles.actor_pool_ops
            if op not in self._modeled_ops and not op.has_completed()
        }
        in_warmup = self.phase is SizerPhase.WARMUP and not self._warmup_complete(
            self._clock()
        )
        phase = classify_phase(in_warmup, unmodeled)
        if phase is not self.phase:
            logger.info("OperatorSizer: phase %s -> %s", self.phase.value, phase.value)
            self.phase = phase

    def _warmup_complete(self, now: float) -> bool:
        """Whether WARMUP ends this tick: every critical op has produced its
        first output, so the pipeline is flowing end to end and feedback
        beats feedforward from here on.

        A critical op that finished without producing (an empty branch of a
        union) does not hold warmup open. Nor does one that never produces
        within the warmup timeout: leaving warmup early only hands sizing to
        the bottleneck policy, which can still move capacity to it.
        """
        if self._warmup_started_t is None:
            self._warmup_started_t = now
        waiting_on = [op for op in self._critical_ops if not has_produced_output(op)]
        if not waiting_on:
            logger.info(
                "OperatorSizer: warmup complete (%s produced first output)",
                ", ".join(op.name for op in self._critical_ops)
                or "no actor-pool operators",
            )
            return True
        if now - self._warmup_started_t < self._warmup_timeout_s:
            return False
        logger.warning(
            "OperatorSizer: warmup timed out after %.0fs with no output yet "
            "from %s; leaving warmup.",
            now - self._warmup_started_t,
            ", ".join(op.name for op in waiting_on),
        )
        return True

    def _awaits_model(self, op: ExperimentalActorPoolMapOperator) -> bool:
        """Whether ``op`` still grows toward its equal share: in warmup, or
        after it while the model has not had a full window of it."""
        return self.phase is SizerPhase.WARMUP or (
            self.phase is SizerPhase.MODEL_WARMING and op not in self._modeled_ops
        )

    # --- held ops -----------------------------------------------------------------

    def _held_ops(self) -> List[ExperimentalActorPoolMapOperator]:
        """Non-PG ops kept from growing into their share for now: waiting on
        their ``on_start`` (no actor may start before it runs), and ops kept
        at min until every upstream actor-pool op has produced output."""
        return [
            op
            for op in self._profiles.non_pg_ops
            if not op.has_completed()
            and (
                op.awaiting_on_start()
                or not all(
                    has_produced_output(prev) for prev in self._profiles.prev_ops[op]
                )
            )
        ]

    def _hold_for_held_ops(
        self, snapshot: ClusterStateSnapshot
    ) -> Dict[ExperimentalActorPoolMapOperator, int]:
        """Take each held op's room out of ``snapshot.free_by_node``; return
        how many actors each got held.

        A held op (e.g. a Write sink before its first input) gets no room of
        its own, so upstream ops can take every node's space first; getting it
        back then means draining actors whose outputs only clear through the
        held op. Holding its room here keeps it out of every step's free map
        without starting an actor. Every held op is held for its warmup share
        (``OpSizingPolicy.warmup_target``), and once released the sizer grows
        it straight to the held count. Held whole per actor, every resource
        the actor needs, on the least contended nodes, as the ledger would
        place it; what does not fit is not held.
        """
        free = snapshot.free_by_node
        global_limits = snapshot.get_global_limits()
        held_actors_by_op: Dict[ExperimentalActorPoolMapOperator, int] = {}
        for op in self._held_ops():
            pool = op.actor_pool
            target = self._how_many.warmup_target(op, global_limits, snapshot.pg_slots)
            if target is None:
                target = pool.min_size()
            # An op awaiting on_start may request nothing yet, so its min is
            # held too. Any other held op's shortfall below min is requested
            # this tick ("below min size"); holding it as well would keep that
            # room from the op itself.
            floor = pool.serving_size()
            if not op.awaiting_on_start():
                floor = max(floor, pool.min_size())
            num_to_hold = target - floor
            if num_to_hold <= 0:
                continue
            pressure = snapshot.constrained_op_count_by_node
            nodes = sorted(
                (n for n in free if n in snapshot.schedulable_nodes_by_op[op]),
                key=lambda n: (pressure.get(n, 0), n),
            )
            held = pack_actors(
                free, nodes, snapshot.per_actor_resources_by_op[op], num_to_hold
            )
            held_actors_by_op[op] = floor + len(held)
            logger.debug(
                "OperatorSizer: holding %d/%d actor(s) for %s until its hold "
                "releases",
                len(held),
                num_to_hold,
                op.name,
            )
        return held_actors_by_op

    def _note_held_ops(
        self, held_now: Dict[ExperimentalActorPoolMapOperator, int]
    ) -> None:
        """Queue a one-time grant up to the held count for every op whose
        hold released since last tick, then remember this tick's holds."""
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

    def _decision_context(self, snapshot: ClusterStateSnapshot) -> DecisionContext:
        return DecisionContext.from_snapshot(
            snapshot,
            topology=self._topology,
            phase=self.phase,
            actor_pool_ops=self._profiles.actor_pool_ops,
            non_pg_ops=self._profiles.non_pg_ops,
            now=self._clock(),
            bottleneck=self._bottleneck,
            growth_ceiling={
                op: self._growth_ceiling(op) for op in self._profiles.actor_pool_ops
            },
            next_ops=self._profiles.next_ops,
        )

    def _growth_ceiling(self, op: ExperimentalActorPoolMapOperator) -> int:
        """The pool's bounded max size, or less once every upstream op has
        completed and the remaining work needs fewer actors."""
        ceiling = op.actor_pool.bounded_max_size()
        remaining = self._how_many.remaining_work_cap(op, self._topology[op])
        return ceiling if remaining is None else min(ceiling, remaining)

    def _decide(self, snapshot: ClusterStateSnapshot) -> None:
        """Step 1: classify this tick's decision and carry it out.

        This lands before how_many on purpose: a decision debits what it claims
        (or holds for a later tick) off the shared free map, and what it
        places becomes targeted pendings in committed accounting, so how_many
        already sees the space it reserved. Ops a transfer claimed are no
        longer ACTIVE, so how_many leaves them alone this tick.
        """
        self._decision, requests = self._policy.step(self._decision_context(snapshot))
        for placed in requests:
            self._apply(placed)

    def _per_op_how_many(self, snapshot: ClusterStateSnapshot) -> PerOpSizingRequest:
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
        ledger = ConstraintAwareCapacityLedger(snapshot)
        request: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest] = {}
        wants = self._collect_wants(snapshot)
        self._grant_within_capacity(snapshot.ordered_ops, wants, ledger, request)
        self._telemetry.log_request_summary(request)
        return PerOpSizingRequest(request=request)

    def _collect_wants(
        self, snapshot: ClusterStateSnapshot
    ) -> Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest]:
        """WANT pass: each op's desired delta from its own state.

        A pool below min_size wants at least enough to reach it (fixed pools
        included). Completed ops are left to GRANT, which releases them.
        """
        wants: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest] = {}
        for op in snapshot.ordered_ops:
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
                    op, snapshot.get_global_limits(), snapshot.pg_slots
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
        self, request: PerOpSizingRequest, snapshot: ClusterStateSnapshot
    ) -> PlacedSizingRequest:
        """Step 3: resolve each delta to nodes.

        1. shed off nodes being removed from the dataset's allocation
        2. resolve downscales to the nodes they shed from
        3. resolve upscales to the nodes new actors land on

        Stateless across ticks: placement is re-derived from current state
        every time, so a failed placement or a slow drain simply retries when
        scale_how_many re-emits its delta next tick.
        """
        if not request.request and not snapshot.nodes_to_reclaim:
            return PlacedSizingRequest(request={})

        reserved = dict(snapshot.reserved_by_node)
        # Copy: place_upscales spends from this map; the snapshot stays intact.
        free = {node_id: res.copy() for node_id, res in snapshot.free_by_node.items()}
        placed: Dict[ExperimentalActorPoolMapOperator, PlacedScalingRequest] = {}

        # Give back nodes we are losing first -- that takes precedence over
        # this tick's sizing for the same op, which re-requests next tick.
        evicted_ops = evict_from_reclaimed_nodes(
            placed=placed,
            nodes_to_reclaim=snapshot.nodes_to_reclaim,
            profiles=self._profiles,
        )
        views = build_placement_views(request=request.request, capacity=snapshot)
        # Nodes the capacity-short upscalers want freed, so downscales shed
        # where the freed capacity actually helps.
        favored = compute_favored_nodes(
            request=request.request,
            views=views,
            reserved=reserved,
            free=free,
            placement=self._placement,
            profiles=self._profiles,
            ordered_ops=snapshot.ordered_ops,
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
            ordered_ops=snapshot.ordered_ops,
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
