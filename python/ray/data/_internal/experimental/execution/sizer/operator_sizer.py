"""The actor-only backend's operator sizer.

The sizer owns every actor-pool sizing decision under the actor-only backend:
initial pool sizes, per-tick up/downscale deltas, and placement-resolved
requests (which nodes gain actors, which nodes lose them -- see
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
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from ray.data._internal.execution.execution_flags import (
    SIZER_CADENCE_S,
    SIZER_WARMUP_TIMEOUT_S,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
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
    Snapshot,
)
from ray.data._internal.experimental.execution.sizer.common import (
    CountByNode,
    ScalingRequests,
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
    critical_ops,
    has_produced_output,
)
from ray.data._internal.experimental.execution.sizer.op_status import (
    Held,
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
    OpDecisions,
    SizingPolicy,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
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

    - ``SizerPhase`` (``_maybe_transition_to_new_phase``): WARMUP until the pipeline flows
      (every op has received input and every critical op has produced
      output) and the bottleneck observer has then measured one full window,
      then STEADY.
    - ``OpStatus`` per op (``op_status``): what the sizer may do to it now.
    - ``OpRule`` per op: the one action that sized it this tick.

    Each tick runs in three steps:

    1. HOW MANY  ``SizingPolicy.decide`` picks one rule per op (``OpRule``)
                 from its status and signals, and a count to upscale or
                 downscale it by. Upscales take free capacity from one
                 ledger (``ConstraintAwareCapacityLedger``) in priority
                 order, so each is already cut to what the cluster can
                 still place.
    2. WHERE     The ``ActorPlacementStrategy`` picks nodes for each count;
                 the pool picks the actors.
    3. APPLY     We hand every placed request to the pools.

    Alongside, it gives back capacity when the coordinator downscales this
    dataset's slice: the tick reports where we now hold more than we are
    allowed, and WHERE downscales off those nodes before it places anything
    new.
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
        # When the pipeline first flowed end to end, or None until then.
        self._flowing_since_t: Optional[float] = None

        self._seen_nonempty_reservation = False
        self._consecutive_empty_reservations = 0
        self._placement: ActorPlacementStrategy = LocalityBasedActorPlacement()

        self._telemetry = SizerTelemetry(dataset_id)

    # --- bootstrap ------------------------------------------------------------

    def bootstrap(self, topology: "Topology") -> None:
        """Build the per-run state over ``topology``, then bring every pool up
        to its initial (min) size."""
        self._validate_topology(topology)
        self._topology = topology
        self._profiles = OpProfiles(topology)
        self._statuses = OpStatusMachine(ops=self._profiles.actor_pool_ops)

        self._policy = SizingPolicy(statuses=self._statuses, profiles=self._profiles)
        self._bottleneck_observer = BottleneckObserver(
            profiles=self._profiles, clock=self._clock
        )
        self._critical_ops = critical_ops(topology)
        self._scale_to_initial_size()

    def _scale_to_initial_size(self) -> None:
        """Bring every pool up to its initial (min) size, untargeted, in
        scarcity order."""
        for op in ordered_actor_pool_ops(self._topology, self._profiles.actor_pool_ops):
            if self._statuses.kind(op) is OpStatusKind.HELD:
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
        """Called every scheduling-loop step. Builds this tick's snapshot, or
        returns None while inside the cadence window.

        The cluster view is read once per step by the scheduling loop and
        handed in, so the bank and every step of a tick agree on it.
        """
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
        self._telemetry.record_tick(
            topology=self._topology, decisions=decisions, where=where
        )

    @timed("tick")
    def _run_tick(
        self, snapshot: ClusterStateSnapshot
    ) -> Tuple[OpDecisions, PlacedRequests]:
        """Reserve room, take the bottleneck snapshot, advance the phase, then
        how many, where, apply."""
        bottleneck_snapshot = self._prepare_tick(snapshot=snapshot)
        decisions = self._per_op_how_many(
            snapshot=snapshot, bottleneck_snapshot=bottleneck_snapshot
        )
        where = self._per_op_where(decisions=decisions, snapshot=snapshot)
        self._apply(where=where)
        return decisions, where

    @timed("prepare")
    def _prepare_tick(self, *, snapshot: ClusterStateSnapshot) -> Snapshot:
        """Advance the op statuses, reserve room for held ops, move the phase
        forward, and take the bottleneck snapshot.
        """
        landed = self._statuses.on_tick(now=self._clock())
        self._policy.record_landed_upscales(landed=landed)
        self._reserve_for_held_ops(snapshot=snapshot)
        self._maybe_transition_to_new_phase()
        return self._bottleneck_observer.tick()

    @timed("how_many")
    def _per_op_how_many(
        self,
        *,
        snapshot: ClusterStateSnapshot,
        bottleneck_snapshot: Optional[Snapshot],
    ) -> OpDecisions:
        """Step 1: pick one rule and count per op. Upscales take capacity
        from the tick's ledger in priority order, which is also the order
        the decisions come back in and WHERE places them in, so step 2 can
        place every count.
        """
        decision_ctx = DecisionContext.from_snapshot(
            snapshot=snapshot,
            topology=self._topology,
            phase=self.phase,
            apmo_profiles=self._profiles,
            now=self._clock(),
            bottleneck_snapshot=bottleneck_snapshot,
        )
        decisions = self._policy.decide(ctx=decision_ctx)
        self._telemetry.warn_unplaceable_ops(topology=self._topology, snapshot=snapshot)
        self._telemetry.log_decisions(
            phase=self.phase,
            decisions=decisions,
            statuses={op: self._statuses.status(op) for op in decisions},
        )
        return decisions

    @timed("per_op_where")
    def _per_op_where(
        self,
        *,
        decisions: OpDecisions,
        snapshot: ClusterStateSnapshot,
    ) -> PlacedRequests:
        """Step 2: give every count its nodes, in the order of
        ``decisions``:

        1. downscale off nodes being removed from the dataset's allocation
        2. resolve downscales to the nodes they remove actors from
        3. resolve upscales to the nodes new actors land on

        Stateless across ticks: placement is re-derived from current state
        every time, so a failed placement or a slow drain retries when the
        policy asks for the delta again next tick.
        """
        counts: ScalingRequests = {
            op: decision.request
            for op, decision in decisions.items()
            if decision.request is not None
        }
        unmet = {op: d.unmet for op, d in decisions.items() if d.unmet > 0}
        return PlacedRequests(
            by_op=self._place_counts(counts, snapshot=snapshot, unmet=unmet)
        )

    def _place_counts(
        self,
        request: ScalingRequests,
        *,
        snapshot: ClusterStateSnapshot,
        unmet: Optional[Dict[ExperimentalActorPoolMapOperator, int]] = None,
    ) -> Dict[ExperimentalActorPoolMapOperator, PlacedScalingRequest]:
        """Nodes for each count, in its order, plus the downscales off nodes
        being reclaimed. ``unmet`` is each upscale's part that free capacity
        could not hold; downscales free room on the nodes where it would."""
        unmet = unmet or {}
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
        # Nodes the capacity-short upscalers want freed, so downscales remove
        # actors where the freed capacity actually helps.
        favored = compute_favored_nodes(
            request=request,
            unmet=unmet,
            views=views,
            capacity=snapshot,
            free=free,
            placement=self._placement,
            profiles=self._profiles,
            ordered_ops=list(snapshot.ordered_ops),
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
            ordered_ops=list(request),
            evicted_ops=evicted_ops,
            telemetry=self._telemetry,
        )

        self._telemetry.log_placement(reserved, free, placed, self._placement)
        return placed

    @timed("apply")
    def _apply(self, *, where: PlacedRequests) -> None:
        """Step 3: hand every placed request to the pools."""
        # Downscales before upscales.
        for op, req in sorted(where.by_op.items(), key=lambda entry: entry[1].delta):
            logger.debug(
                "scale: %s delta=%d nodes=%s",
                op.name,
                req.delta,
                dict(Counter(req.nodes)),
            )
            op.actor_pool.scale(req)

    # --- phase ------------------------------------------------------------------

    def _maybe_transition_to_new_phase(self) -> None:
        if self.phase is SizerPhase.WARMUP and self._warmup_complete():
            logger.info("OperatorSizer: phase warmup -> steady")
            self.phase = SizerPhase.STEADY

    def _warmup_complete(self) -> bool:
        """Whether the bottleneck observer has measured one full window since
        the pipeline started to flow. A "window" just means a range of values
        observed over a period of time.

        When the pipeline first flows, the observer drops what it measured
        before then. Until the first output, downstream ops sit starved, and
        a window holding that time would downscale them as idle the moment
        warmup ends.
        """
        now = self._clock()
        if self._flowing_since_t is None:
            if not self._pipeline_flowing():
                return False
            # Here, the critical op has finally produced an output. Record the
            # timestamp that it is flowing, and then wait for a window of observations
            self._flowing_since_t = now
            self._bottleneck_observer.reset_windows()
        return now - self._flowing_since_t >= self._bottleneck_observer.window_s

    def _pipeline_flowing(self) -> bool:
        """Whether every actor-pool op has received its first input and every
        critical op has produced its first output, so the pipeline flows end
        to end.

        An op that finished without input or output (an empty branch of a
        union) does not keep the sizer in warmup forever, because warmup has
        a timeout.
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
                "OperatorSizer: pipeline flowing (%s produced first output); "
                "warmup ends after one bottleneck window",
                ", ".join(op.name for op in self._critical_ops)
                or "no actor-pool operators",
            )
            return True
        if now - self._warmup_started_t < self._warmup_timeout_s:
            return False
        logger.warning(
            "OperatorSizer: warmup timed out after %.0fs still waiting on "
            "first input or output from %s; treating the pipeline as flowing.",
            now - self._warmup_started_t,
            ", ".join(op.name for op in waiting_on),
        )
        return True

    def _reserve_for_held_ops(self, *, snapshot: ClusterStateSnapshot) -> None:
        """Take each held op's room out of ``snapshot.free_by_node``, and
        record it on its ``Held`` state.

        A held op (e.g. a Write sink before its first input) has no actors, so
        nothing else would hold room for it: upstream ops can take every
        node's space first, and getting it back then means draining actors
        whose outputs only clear through the held op. Holding its room here
        keeps it out of every step's free map without starting an actor.
        PG-managed ops hold nothing here: their placement group is their room.

        1. Room an op held until it left ``Held`` this tick is kept out of the
           free map until the ops still held have packed theirs, then given
           back, so the policy grants it to that op first
           (``OpRule.UPSCALE_TO_HELD_ROOM``). Otherwise a held op could take it
           the moment it frees: in backpressure_heterogeneous_memory_nodes, a
           held Write took the CPUs of the GPU nodes the GPU op had held, and
           the GPU op never placed an actor.
        2. Room for each op below its min size that is not held is kept out
           the same way, and the policy grants it with
           ``OpRule.UPSCALE_TO_MIN``. An op cannot run below its min size,
           while a held op cannot use its room until data reaches it, so a
           min size comes before a hold. Step 1 covers only the tick an op
           leaves ``Held``; this covers an op that falls below its min size
           later, e.g. when its pending actors expire.
        3. Each held op keeps the room it held last tick, on the same nodes,
           where it still fits, so held ops do not trade nodes every tick.
        4. Each tops up what it still lacks on the least contended nodes
           (``ClusterStateSnapshot.contention_by_node``).

        Steps 2-4 go scarcest op first (``ClusterStateSnapshot.ordered_ops``),
        so an op with few nodes to choose from claims them before an op that
        could go anywhere.

        Example: CPU nodes c0, c1 with 1 free CPU each; GPU nodes g1, g2 with
        4 CPU / 4 GPU each. Last tick, GPU (fixed 8) held {g1: 4, g2: 4} and
        Write (wants 10) held {c0: 1, c1: 1}. GPU's first input arrived.
        ``free`` shows free CPUs; each actor of either op takes one.

            start:       free = {c0: 1, c1: 1, g1: 4, g2: 4}
                         released = {GPU: {g1: 4, g2: 4}}
            1. shield:   free = {c0: 1, c1: 1, g1: 0, g2: 0}
            3. keep:     Write holds {c0: 1, c1: 1}
                         free = {c0: 0, c1: 0, g1: 0, g2: 0}
            4. top up:   Write lacks 8, nothing free; holds 2 of 10
            1-2. return: free = {c0: 0, c1: 0, g1: 4, g2: 4}
            policy:      GPU UPSCALE_TO_HELD_ROOM +8 -> g1, g2

        Without step 1, Write's top-up takes g1's and g2's 8 CPUs, and GPU,
        needing 1 CPU per actor, never places one. If GPU's pending actors
        expire a tick later, it is below its min size and no longer leaving
        ``Held``: step 2 sets its 8 aside before Write packs, the same way.
        """
        free = snapshot.free_by_node
        contention = snapshot.contention_by_node

        # 1. Keep room released this tick out of the free map for now.
        shielded: List[Tuple[NodeIdStr, ExecutionResources]] = []
        for op, by_node in self._statuses.released_holds().items():
            per_actor = op.actor_pool.per_actor_resource_usage()
            for node_id in sorted(by_node):
                if node_id not in free:
                    continue
                kept = pack_actors(free, [node_id], per_actor, by_node[node_id])
                if kept:
                    shielded.append((node_id, per_actor.scale(len(kept))))

        # 2. Keep room for each op below its min size, and not held, out of the
        # free map for now. Room released to it in step 1 counts toward it.
        held_ops = self._statuses.ops_in(kind=OpStatusKind.HELD)
        non_pg_ops = set(self._profiles.non_pg_ops)
        released = self._statuses.released_holds()
        for op in snapshot.ordered_ops:
            if op in held_ops or op not in non_pg_ops or op.has_completed():
                continue
            pool = op.actor_pool
            below_min = (
                pool.min_size()
                - pool.serving_size()
                - sum(released.get(op, {}).values())
            )
            if below_min <= 0:
                continue
            nodes = sorted(
                (n for n in free if n in snapshot.schedulable_nodes_by_op[op]),
                key=lambda n: (contention.get(n, 0.0), n),
            )
            per_actor = pool.per_actor_resource_usage()
            for node_id in pack_actors(free, nodes, per_actor, below_min):
                shielded.append((node_id, per_actor))

        # How many actors' worth each held op holds this tick, scarcest op
        # first.
        global_limits = snapshot.get_global_limits()
        to_hold: Dict[ExperimentalActorPoolMapOperator, int] = {}
        for op in snapshot.ordered_ops:
            if op not in held_ops or op not in non_pg_ops or op.has_completed():
                continue
            pool = op.actor_pool
            target = self._policy.warmup.warmup_target(
                op=op,
                cluster_capacity=global_limits,
                pg_slots=snapshot.pg_slots,
            )
            if target is None:
                target = pool.min_size()
            num_to_hold = target - pool.serving_size()
            if num_to_hold <= 0:
                self._statuses.set_reserved(op=op, n=pool.serving_size())
                continue
            to_hold[op] = num_to_hold

        # 3. Keep last tick's room where it still fits.
        held_by_op: Dict[ExperimentalActorPoolMapOperator, CountByNode] = {}
        for op, num_to_hold in to_hold.items():
            per_actor = op.actor_pool.per_actor_resource_usage()
            schedulable = snapshot.schedulable_nodes_by_op[op]
            status = self._statuses.status(op)
            previous = status.by_node if isinstance(status, Held) else {}
            held: CountByNode = {}
            for node_id in sorted(previous):
                wanted = min(previous[node_id], num_to_hold - sum(held.values()))
                if wanted <= 0:
                    break
                if node_id not in free or node_id not in schedulable:
                    continue
                kept = pack_actors(free, [node_id], per_actor, wanted)
                if kept:
                    held[node_id] = len(kept)
            held_by_op[op] = held

        # 4. Top up on the least contended nodes.
        for op, num_to_hold in to_hold.items():
            held = held_by_op[op]
            lacking = num_to_hold - sum(held.values())
            if lacking > 0:
                nodes = sorted(
                    (n for n in free if n in snapshot.schedulable_nodes_by_op[op]),
                    key=lambda n: (contention.get(n, 0.0), n),
                )
                per_actor = op.actor_pool.per_actor_resource_usage()
                for node_id in pack_actors(free, nodes, per_actor, lacking):
                    held[node_id] = held.get(node_id, 0) + 1
            num_held = sum(held.values())
            self._statuses.set_reserved(
                op=op, n=op.actor_pool.serving_size() + num_held, by_node=held
            )
            logger.debug(
                "OperatorSizer: holding %d/%d actor(s) for %s until its first "
                "input arrives",
                num_held,
                num_to_hold,
                op.name,
            )

        # 1-2. (cont.) Give the room back for the policy to grant.
        for node_id, room in shielded:
            free[node_id] = free[node_id].add(room)

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
