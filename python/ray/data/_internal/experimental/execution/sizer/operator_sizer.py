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
import time
from collections import Counter
from typing import TYPE_CHECKING, Callable, Dict, Optional, Set, Tuple

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
from ray.data._internal.experimental.execution.sizer.locality_actor_placement import (
    LocalityBasedActorPlacement,
)
from ray.data._internal.experimental.execution.sizer.metrics import (
    OptimizerMetrics,
)
from ray.data._internal.experimental.execution.sizer.models import (
    ClusterStateSnapshot,
    PlacedRequests,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
    critical_ops,
    has_produced_output,
)
from ray.data._internal.experimental.execution.sizer.op_status import (
    OpStatusKind,
    OpStatusMachine,
)
from ray.data._internal.experimental.execution.sizer.operator_ordering import (
    ordered_actor_pool_ops,
)
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.sizing_policy import (
    SizingPolicy,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    Decision,
    OpDecision,
    SizerPhase,
)
from ray.data._internal.experimental.execution.sizer.scale_where import (
    build_placement_views,
    compute_favored_nodes,
    evict_from_reclaimed_nodes,
    place_downscales,
    place_upscales,
)
from ray.data._internal.experimental.execution.sizer.telemetry import (
    SizerTelemetry,
    timed,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.actor_only_resource_reporter import (  # noqa: E501
        DatasetReservationView,
    )

logger = logging.getLogger(__name__)


class OperatorSizer:
    """Owns how many actors every operator in the topology has, and where.

    A state machine, evaluated once per sizing tick:

    - ``SizerPhase`` (``_advance_phase``): WARMUP until every op has received
      input and every critical op has produced output, MODEL_WARMING until
      the bottleneck model has seen a full window of every unfinished op,
      then STEADY.
    - ``OpStatus`` per op (``op_status``): what the sizer may do to it now.
    - ``OpRule`` per op: the one action that sized it this tick.

    Each tick runs in three steps:

    1. HOW MANY  ``SizingPolicy.decide`` picks one action per op (``OpRule``).
                 It acts on the bottleneck model first: grow a bottleneck or
                 balanced stages into free capacity, fund the recipient from
                 donors, or shed blocked surplus. Those requests already name
                 their nodes and take their room from the snapshot. Every
                 other op gets a per-op action: release a completed pool,
                 grow to held room, grow toward the warmup share, shed idle
                 actors, or refill to min size. Every action checks and
                 spends capacity in one ledger
                 (``ConstraintAwareCapacityLedger``), so each per-op upscale
                 is already cut to what the cluster can still place.
    2. WHERE     The ``ActorPlacementStrategy`` picks nodes for each per-op
                 delta; the pool picks the actors.
    3. APPLY     We hand every placed request to the pools.

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
        self._cadence_s = cadence_s
        self._last_tick_t: float = float("-inf")

        self._warmup_timeout_s = warmup_timeout_s
        self._warmup_started_t: Optional[float] = None

        self._seen_nonempty_reservation = False
        self._consecutive_empty_reservations = 0
        self._placement: ActorPlacementStrategy = LocalityBasedActorPlacement()

        # these are the ops that are currently modeled by the bottleneck observer
        # eventually all ops are, but in the beginning the observer needs
        # to gather data before classifying as bottleneck or not
        self._modeled_ops: Set[ExperimentalActorPoolMapOperator] = set()
        self._decision: Optional[Decision] = None
        self._telemetry = SizerTelemetry(dataset_id)

    # --- bootstrap ------------------------------------------------------------

    def bootstrap(self, topology: "Topology") -> None:
        """Build the per-run state over ``topology``, then bring every pool up
        to its initial (min) size."""
        self._validate_topology(topology)
        self._topology = topology
        self._profiles = OpProfiles(topology)
        self._statuses = OpStatusMachine(self._profiles.actor_pool_ops)

        self._policy = SizingPolicy(
            metrics=OptimizerMetrics(self._dataset_id),
            placement=self._placement,
            statuses=self._statuses,
            profiles=self._profiles,
        )
        self._bottleneck_observer = BottleneckObserver(
            profiles=self._profiles, clock=self._clock
        )
        self._critical_ops = critical_ops(topology)
        self._scale_to_initial_size()

    def _scale_to_initial_size(self) -> None:
        """Bring every pool up to its initial (min) size, untargeted, in
        scarcity order."""
        for op in ordered_actor_pool_ops(self._topology, self._profiles.actor_pool_ops):
            if not self._statuses.can_size_per_op(op):
                continue  # held until its first input: no actors yet
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

    def _trust_empty_reservation(self, view: "DatasetReservationView") -> bool:
        """Whether this tick may reclaim on an empty reservation.

        An empty reservation can come in 2 places:
            - cold cache from autoscaling coordinator
            - the autoscaling coordinator decided to hand out 0 nodes. This can
            happen in node preemption, for example
        """
        if view.reserved_by_node:
            self._seen_nonempty_reservation = True
            self._consecutive_empty_reservations = 0
            return True
        if not self._seen_nonempty_reservation:
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

    @timed("build")
    def maybe_build_cluster_state_snapshot(
        self,
        *,
        cluster_view: "DatasetReservationView",
        seconds_since_allocation_change: float,
    ) -> Optional[ClusterStateSnapshot]:
        """Called every scheduling-loop step. Runs the per-step status
        transitions (counted in steps, so they must not wait for the
        cadence), then builds this tick's snapshot, or returns None while
        inside the cadence window.

        The cluster view is read once per step by the scheduling loop and
        handed in, so the bank and every step of a tick agree on it.
        """
        self._statuses.on_step()
        now = self._clock()
        if now - self._last_tick_t < self._cadence_s:
            return None
        self._last_tick_t = now
        snapshot = ClusterStateSnapshot.build(
            cluster_view,
            self._profiles,
            seconds_since_allocation_change=seconds_since_allocation_change,
            trust_empty_reservation=self._trust_empty_reservation(cluster_view),
        )
        return snapshot

    def perform_sizing_decision(
        self, *, cluster_state_snapshot: ClusterStateSnapshot
    ) -> None:
        """One sizing tick against the snapshot, then its telemetry. The
        snapshot's free maps are debited along the way."""
        decisions, where = self._run_tick(cluster_state_snapshot)
        self._telemetry.record_tick(self._topology, decisions, where)

    @timed("tick")
    def _run_tick(
        self, snapshot: ClusterStateSnapshot
    ) -> Tuple[Dict[ExperimentalActorPoolMapOperator, OpDecision], PlacedRequests]:
        """Hold room for held ops, run the bottleneck model, advance the
        phase, then how many, where, apply."""
        bottleneck_tick = self._prepare_tick(snapshot)
        decisions = self._per_op_how_many(
            snapshot=snapshot, bottleneck_tick=bottleneck_tick
        )
        where = self._per_op_where(decisions, snapshot)
        self._apply(where)
        return decisions, where

    @timed("prepare")
    def _prepare_tick(self, snapshot: ClusterStateSnapshot) -> BottleneckTick | None:
        """Advance the op statuses, take held ops' room out of the snapshot,
        run the bottleneck model on what is left, and move the phase
        forward."""
        self._statuses.on_tick()
        self._exclude_held_ops_from_free_capacity(snapshot)
        bottleneck_tick = self._bottleneck_observer.tick(
            capacity=snapshot,
            held_by_node=self._policy.held_capacity_by_node(),
        )
        self._bottleneck_observer.log_tick(bottleneck_tick)
        self._advance_phase(bottleneck_tick=bottleneck_tick)
        return bottleneck_tick

    @timed("how_many")
    def _per_op_how_many(
        self,
        *,
        snapshot: ClusterStateSnapshot,
        bottleneck_tick: Optional[BottleneckTick],
    ) -> Dict[ExperimentalActorPoolMapOperator, OpDecision]:
        """Step 1: pick one action/decision per op.

        Each decision is either a count (``request``) or requests that
        already name their nodes (``placed``): a decision whose outcome
        depends on the node, like a transfer, picks the nodes itself. Every
        action checks and spends capacity in the tick's ledger, in the tick's
        op order. That is also WHERE's placement order, so step 2 can place
        every count.
        """
        decision_ctx = DecisionContext.from_snapshot(
            snapshot,
            topology=self._topology,
            phase=self.phase,
            apmo_profiles=self._profiles,
            now=self._clock(),
            bottleneck=bottleneck_tick,
            modeled_ops=frozenset(self._modeled_ops),
        )
        decisions = self._policy.decide(decision_ctx)
        self._decision = self._policy.last_decision
        self._telemetry.log_decisions(
            self.phase,
            self._decision,
            decisions,
            {op: self._statuses.status(op) for op in decisions},
        )
        return decisions

    @timed("per_op_where")
    def _per_op_where(
        self,
        decisions: Dict[ExperimentalActorPoolMapOperator, OpDecision],
        snapshot: ClusterStateSnapshot,
    ) -> PlacedRequests:
        """Step 2: give every decision its nodes. Decisions that already
        name theirs pass through; each count is placed here:

        1. shed off nodes being removed from the dataset's allocation
        2. resolve downscales to the nodes they shed from
        3. resolve upscales to the nodes new actors land on

        Stateless across ticks: placement is re-derived from current state
        every time, so a failed placement or a slow drain retries when the
        per-op rules ask for the delta again next tick.
        """
        counts: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest] = {}
        for op in snapshot.ordered_ops:
            want = decisions[op].request if op in decisions else None
            if want is not None:
                counts[op] = want
        placed = self._place_counts(counts, snapshot)
        by_op = {
            op: decision.placed for op, decision in decisions.items() if decision.placed
        }
        for op, req in placed.items():
            by_op[op] = (*by_op.get(op, ()), req)
        return PlacedRequests(by_op=by_op)

    def _place_counts(
        self,
        request: Dict[ExperimentalActorPoolMapOperator, ActorPoolScalingRequest],
        snapshot: ClusterStateSnapshot,
    ) -> Dict[ExperimentalActorPoolMapOperator, PlacedScalingRequest]:
        """Nodes for each count, plus the sheds off nodes being reclaimed."""
        if not request and not snapshot.nodes_to_reclaim:
            return {}

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
        views = build_placement_views(request=request, capacity=snapshot)
        # Nodes the capacity-short upscalers want freed, so downscales shed
        # where the freed capacity actually helps.
        favored = compute_favored_nodes(
            request=request,
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
            request=request,
            views=views,
            favored=favored,
            free=free,
            placement=self._placement,
            evicted_ops=evicted_ops,
        )
        place_upscales(
            placed=placed,
            request=request,
            views=views,
            free=free,
            placement=self._placement,
            profiles=self._profiles,
            ordered_ops=snapshot.ordered_ops,
            evicted_ops=evicted_ops,
            telemetry=self._telemetry,
        )

        self._telemetry.log_placement(reserved, free, placed, self._placement)
        return placed

    @timed("apply")
    def _apply(self, where: PlacedRequests) -> None:
        """Step 3: hand every placed request to the pools."""
        requests = [(op, req) for op, reqs in where.by_op.items() for req in reqs]
        # Downscales before upscales. The sort is stable, so an op's own
        # requests keep their order.
        for op, req in sorted(requests, key=lambda entry: entry[1].delta):
            logger.debug(
                "scale: %s delta=%d nodes=%s",
                op.name,
                req.delta,
                dict(Counter(req.nodes)),
            )
            op.actor_pool.scale(req)

    # --- phase ------------------------------------------------------------------

    def _advance_phase(self, bottleneck_tick: BottleneckTick | None):
        """Fold this tick's model into the ops it has seen, and move the
        phase forward. WARMUP is left for good once ``_warmup_complete``."""
        if bottleneck_tick is not None:
            self._modeled_ops.update(bottleneck_tick.snapshot.shares)
        unmodeled = {
            op
            for op in self._profiles.actor_pool_ops
            if op not in self._modeled_ops and not op.has_completed()
        }
        in_warmup = self.phase is SizerPhase.WARMUP and not self._warmup_complete()
        if in_warmup:
            phase = SizerPhase.WARMUP
        elif unmodeled:
            phase = SizerPhase.MODEL_WARMING
        else:
            phase = SizerPhase.STEADY

        if phase is not self.phase:
            logger.info("OperatorSizer: phase %s -> %s", self.phase.value, phase.value)
            self.phase = phase

    def _warmup_complete(self) -> bool:
        """Whether WARMUP ends this tick: every actor-pool op has received its
        first input and every critical op has produced its first output, so
        the pipeline is flowing end to end and feedback beats feedforward
        from here on.

        An op that finished without input or output (an empty branch of a
        union) does get stuck in warmup, because we enforce a timeout. After
        leaving warmup, more flexible op scaling decisions can be made.
        """
        now = self._clock()
        if self._warmup_started_t is None:
            self._warmup_started_t = now
        waiting_on = [op for op in self._critical_ops if not has_produced_output(op)]
        waiting_on += [
            op
            for op in self._profiles.actor_pool_ops
            if op.awaiting_first_input()
            and not op.has_completed()
            and op not in waiting_on
        ]
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
            "OperatorSizer: warmup timed out after %.0fs still waiting on "
            "first input or output from %s; leaving warmup.",
            now - self._warmup_started_t,
            ", ".join(op.name for op in waiting_on),
        )
        return True

    def _exclude_held_ops_from_free_capacity(
        self, snapshot: ClusterStateSnapshot
    ) -> None:
        """Take each held op's room out of ``snapshot.free_by_node``, and
        record it on its ``Held`` state.

        A held op (e.g. a Write sink before its first input) has no actors, so
        nothing else would hold room for it: upstream ops can take every
        node's space first, and getting it back then means draining actors
        whose outputs only clear through the held op. Holding its room here
        keeps it out of every step's free map without starting an actor.
        PG-managed ops hold nothing here: their placement group is their room.
        """
        free = snapshot.free_by_node
        global_limits = snapshot.get_global_limits()
        held_ops = self._statuses.ops_in(OpStatusKind.HELD)
        for op in self._profiles.non_pg_ops:
            if op not in held_ops or op.has_completed():
                continue
            pool = op.actor_pool
            target = self._policy.per_op.warmup_target(
                op, global_limits, snapshot.pg_slots
            )
            if target is None:
                target = pool.min_size()
            floor = pool.serving_size()
            num_to_hold = target - floor
            if num_to_hold <= 0:
                self._statuses.set_reserved(op, floor)
                continue
            pressure = snapshot.constrained_op_count_by_node
            nodes = sorted(
                (n for n in free if n in snapshot.schedulable_nodes_by_op[op]),
                key=lambda n: (pressure.get(n, 0), n),
            )
            held = pack_actors(
                free, nodes, op.actor_pool.per_actor_resource_usage(), num_to_hold
            )
            self._statuses.set_reserved(op, floor + len(held))
            logger.debug(
                "OperatorSizer: holding %d/%d actor(s) for %s until its first "
                "input arrives",
                len(held),
                num_to_hold,
                op.name,
            )

    def observe_after_launch(self) -> None:
        """Sample the bottleneck observer's slot shares. Called by the
        scheduling loop once per step after it launches tasks, not at the
        start of ``perform_sizing_decision``.

        We observe after launch because we want to consider truly IDLE slots,
        ie, slots that even after launching tasks are still free. This truly
        indicates no work for the sizer.
        """
        self._bottleneck_observer.observe(self._topology)

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
