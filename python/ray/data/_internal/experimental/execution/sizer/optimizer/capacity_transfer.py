"""Move actors from operators with provable surplus to ones short on capacity."""

from __future__ import annotations

import logging
import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Callable,
    DefaultDict,
    Deque,
    Dict,
    FrozenSet,
    List,
    Optional,
    Set,
    Tuple,
)

from typing_extensions import override

from ray.data._internal.execution.execution_flags import (
    SIZER_CADENCE_S,
    SIZER_CLUSTER_SETTLE_S,
    SIZER_TRANSFER_THRASH_WINDOW_TICKS,
    SIZER_TRANSFER_TIMEOUT_S,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import LogicalActorId, NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    PlacedScalingRequest,
    ScaleDownRequest,
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.operators.placement_constraints import (
    is_constrained,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    FIT_EPS,
    PLACEMENT_RESOURCE_DIMS,
    OpPlacementView,
    max_placeable_actors,
)
from ray.data._internal.experimental.execution.sizer.metrics import (
    OptimizerMetrics,
)
from ray.data._internal.experimental.execution.sizer.models import (
    PlacedSizingRequest,
)
from ray.data._internal.experimental.execution.sizer.optimizer.context import (
    OptimizationContext,
)
from ray.data._internal.experimental.execution.sizer.optimizer.rule import (
    OptimizationRule,
)
from ray.data._internal.experimental.execution.sizer.optimizer.utils import (
    free_schedulable_nodes,
    pending_actor_ids,
)
from ray.data._internal.experimental.execution.sizer.scaling_guard import OpScalingGuard

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
        ActorPlacementStrategy,
    )

logger = logging.getLogger(__name__)

# How long after a transfer closes its inverse is refused as thrashing.
THRASH_WINDOW_S = SIZER_TRANSFER_THRASH_WINDOW_TICKS * SIZER_CADENCE_S


@dataclass
class RecentTransfer:
    """A fired transfer, remembered so its inverse can be refused for a while.

    The window runs from when the transfer closes, not when it fires: until
    its actors land, nobody can judge whether it was right.
    """

    recipient: "ExperimentalActorPoolMapOperator"
    donors: FrozenSet["ExperimentalActorPoolMapOperator"]
    # None while the transfer is still in flight.
    closed_t: Optional[float] = None

    def inverted_by(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        donors: FrozenSet["ExperimentalActorPoolMapOperator"],
        now: float,
    ) -> bool:
        """Whether moving capacity from ``donors`` to ``recipient`` now would
        undo this transfer inside the thrash window."""
        if self.closed_t is not None and now - self.closed_t > THRASH_WINDOW_S:
            return False
        return recipient in self.donors and self.recipient in donors


@dataclass
class TransferInFlight:
    """One transfer, from the moment it fires until its actors land (or it
    times out and the claim is cancelled).

    The donors are downscaled when it fires; the recipient's actors are only
    created as the donors' capacity actually frees up, so the dataset never
    asks for more than its slice of a node. Until then the transfer holds that
    node's free capacity (see ``CapacityTransfer._hold_owed_capacity``) so
    nothing else in the sizer takes it.
    """

    recipient: "ExperimentalActorPoolMapOperator"
    donors: FrozenSet["ExperimentalActorPoolMapOperator"]
    started_t: float
    # Recipient actors still to place, per node.
    owed_by_node: Dict[NodeIdStr, int] = field(default_factory=dict)
    # Recipient actors to bring up this tick (their capacity is free and
    # held), per node; ``apply`` creates them.
    to_bring_up_by_node: Dict[NodeIdStr, int] = field(default_factory=dict)
    # The recipient's actors (pending and running) when the transfer fired.
    preexisting_ids: FrozenSet[LogicalActorId] = frozenset()
    # The recipient's actors created since, accumulated as they are placed:
    # the transfer's (the guard blocks every other decision on the recipient
    # while it is in flight).
    claimed_ids: Tuple[LogicalActorId, ...] = ()
    # This transfer's entry in ``TransferState.recent_transfers``, stamped
    # with the close time when it closes.
    record: Optional[RecentTransfer] = None


@dataclass
class TransferState:
    """What CapacityTransfer carries between ticks; owned by the optimizer."""

    in_flight: Dict["ExperimentalActorPoolMapOperator", TransferInFlight] = field(
        default_factory=dict
    )
    # Recent transfers. One that a new transfer inverts within THRASH_WINDOW_S
    # of closing is thrashing the same capacity back and forth.
    recent_transfers: Deque[RecentTransfer] = field(
        default_factory=lambda: deque(maxlen=8)
    )
    # Donors blocked until their signal window refills (see _close_transfer).
    cooling_donors: Set["ExperimentalActorPoolMapOperator"] = field(default_factory=set)
    # Edge-triggered so "no donor surplus" logs once per episode, not per tick.
    logged_no_donors: bool = False
    # Recipients whose thrashing refusal was logged since they last received.
    logged_thrashing: Set["ExperimentalActorPoolMapOperator"] = field(
        default_factory=set
    )


@dataclass
class DonorCapacity:
    """How much an operator can give up without losing throughput.

        work_floor = ceil(peak productive tasks / max_actor_concurrency)
        protected  = max(min_size, work_floor)
        removable  = serving_size - protected

    Productive means in-flight minus output-blocked, taken at its windowed
    peak: blocked actors are not producing, so they are not protected. Whether
    the op wants to grow is deliberately ignored -- a backpressured feeder's
    demand is phantom exactly when its consumer is the bottleneck.
    """

    op: "ExperimentalActorPoolMapOperator"
    view: OpPlacementView
    per_actor: ExecutionResources
    removable: int


def assess_donor(
    op: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
) -> Optional[DonorCapacity]:
    """How many actors ``op`` can give up without losing throughput, or None
    if it has none to give."""
    pool = op.actor_pool
    cur = pool.serving_size()
    conc = max(1, pool.max_actor_concurrency())
    signal = ctx.productive_tasks_signal.get(op)
    peak = signal.peak() if signal is not None else None
    if peak is None:
        protected = cur
    else:
        protected = max(pool.min_size(), math.ceil(peak / conc))
    removable = cur - protected
    # The no_donor_surplus refusal counter says a transfer was blocked but not
    # why. ``protected`` is a MAX over the productive window, so one early
    # burst can hold a whole pool protected long after it stops producing;
    # without these inputs that is indistinguishable from a busy donor.
    logger.debug(
        "PipelineOptimizer donor %s: cur=%d peak=%s protected=%d removable=%d "
        "(conc=%d min_size=%d)",
        op.name,
        cur,
        "none" if peak is None else f"{peak:g}",
        protected,
        removable,
        conc,
        pool.min_size(),
    )
    if removable <= 0:
        return None
    return DonorCapacity(
        op=op,
        view=placement_view.build_op_placement_view(op, rank_victims=True),
        per_actor=pool.per_actor_resource_usage(),
        removable=removable,
    )


# --- planning -----------------------------------------------------------------


@dataclass(frozen=True, order=True)
class VictimCandidate:
    """One actor a donor could give up. Sorts cheapest to evict first."""

    eviction_rank: int
    unconsumed_outputs: int
    tasks_in_flight: int
    donor: DonorCapacity = field(compare=False)
    actor_id: LogicalActorId = field(compare=False)

    @classmethod
    def build(
        cls,
        donor: DonorCapacity,
        actor_id: LogicalActorId,
        *,
        pending: bool,
        tasks_in_flight: int,
        unconsumed_outputs: int,
        recipient_constrained: bool,
    ) -> "VictimCandidate":
        # How soon evicting frees the slot: pending now, idle soon, busy after
        # its tasks finish.
        rank = 0 if pending else 1 if tasks_in_flight == 0 else 2
        # A constrained donor (GPU or label selector) has few nodes to regrow
        # on, so an unconstrained recipient takes its actors last.
        if is_constrained(donor.op) and not recipient_constrained:
            rank += 3
        return cls(rank, unconsumed_outputs, tasks_in_flight, donor, actor_id)


@dataclass
class NodePlan:
    """What one node gives a transfer: recipient actors placed there, and the
    victims evicted for them."""

    node: NodeIdStr
    num_recipient_actors: int
    victims: List[VictimCandidate]
    # Only when nothing fits: per resource, how much one recipient actor still
    # lacks after evicting every victim on the node (for the refusal log).
    resource_shortfall: Dict[str, float] = field(default_factory=dict)

    def victims_by_donor(self) -> Dict["ExperimentalActorPoolMapOperator", int]:
        return dict(Counter(victim.donor.op for victim in self.victims))


def _victim_candidates(
    node: NodeIdStr,
    donors: List[DonorCapacity],
    recipient_constrained: bool,
    num_removable_actors_by_op: Dict["ExperimentalActorPoolMapOperator", int],
) -> List[VictimCandidate]:
    candidates: List[VictimCandidate] = []
    for donor in donors:
        if num_removable_actors_by_op[donor.op] <= 0:
            continue
        for actor_id in donor.view.pending_ids_by_node.get(node, []):
            candidates.append(
                VictimCandidate.build(
                    donor,
                    actor_id,
                    pending=True,
                    tasks_in_flight=0,
                    unconsumed_outputs=0,
                    recipient_constrained=recipient_constrained,
                )
            )
        for actor_id, (in_flight, unconsumed) in zip(
            donor.view.victim_ids_by_node.get(node, []),
            donor.view.victim_drain_cost_by_node.get(node, []),
        ):
            candidates.append(
                VictimCandidate.build(
                    donor,
                    actor_id,
                    pending=False,
                    tasks_in_flight=in_flight,
                    unconsumed_outputs=unconsumed,
                    recipient_constrained=recipient_constrained,
                )
            )
    return sorted(candidates)


def _plan_node(
    node: NodeIdStr,
    max_actors: int,
    free: Dict[NodeIdStr, ExecutionResources],
    recipient_per_actor: ExecutionResources,
    candidates: List[VictimCandidate],
    num_removable_actors_by_op: Dict["ExperimentalActorPoolMapOperator", int],
) -> NodePlan:
    """Choose victims on ``node`` so up to ``max_actors`` recipient actors fit.

    A candidate is taken only if it frees a resource the next recipient actor
    is still short of, so a GPU recipient never evicts CPU-only actors whose
    CPU it doesn't need.
    """
    freed = ExecutionResources.zero()
    taken_by_op: DefaultDict["ExperimentalActorPoolMapOperator", int] = defaultdict(int)
    victims: List[VictimCandidate] = []
    best_fit, victims_needed = 0, 0
    for candidate in candidates:
        op = candidate.donor.op
        if taken_by_op[op] >= num_removable_actors_by_op[op]:
            continue
        if not _frees_short_dim(
            candidate.donor.per_actor,
            available=free[node].add(freed),
            recipient_per_actor=recipient_per_actor,
            num_recipient_actors=best_fit + 1,
        ):
            continue
        victims.append(candidate)
        taken_by_op[op] += 1
        freed = freed.add(candidate.donor.per_actor)
        fit = min(
            max_actors,
            int(max_placeable_actors(free[node].add(freed), recipient_per_actor)),
        )
        if fit > best_fit:
            best_fit, victims_needed = fit, len(victims)
        if fit >= max_actors:
            break
    if best_fit == 0:
        return NodePlan(
            node,
            0,
            [],
            resource_shortfall=_resource_shortfall_for_one_actor(
                recipient_per_actor, free[node].add(freed)
            ),
        )
    return NodePlan(node, best_fit, victims[:victims_needed])


def _frees_short_dim(
    victim_per_actor: ExecutionResources,
    *,
    available: ExecutionResources,
    recipient_per_actor: ExecutionResources,
    num_recipient_actors: int,
) -> bool:
    """True if evicting one victim of this shape frees a dimension that
    ``available`` lacks for ``num_recipient_actors`` recipient actors."""
    return any(
        getattr(victim_per_actor, dim) > 0
        and getattr(recipient_per_actor, dim) * num_recipient_actors
        > getattr(available, dim) + FIT_EPS
        for dim in PLACEMENT_RESOURCE_DIMS
    )


def _resource_shortfall_for_one_actor(
    per_actor: ExecutionResources, available: ExecutionResources
) -> Dict[str, float]:
    """Per dimension, how much more ``available`` needs to fit one actor (for
    logging)."""
    shortfall = {}
    for dim in PLACEMENT_RESOURCE_DIMS:
        need = (getattr(per_actor, dim) or 0) - (getattr(available, dim) or 0)
        if need > 0:
            shortfall[dim] = need
    return shortfall


def plan_transfer(
    recipient: "ExperimentalActorPoolMapOperator",
    recipient_per_actor: ExecutionResources,
    want: int,
    nodes: List[NodeIdStr],
    free: Dict[NodeIdStr, ExecutionResources],
    donors: List[DonorCapacity],
    node_rank: Dict[NodeIdStr, Tuple],
) -> Tuple[Optional[List[NodePlan]], Optional[str]]:
    """Plan how to give ``recipient`` up to ``want`` more actors by evicting
    victims from donors.

    Tries nodes where the most recipient actors fit first, breaking ties by
    the recipient's locality rank. No donor loses more than its removable
    count across the whole plan.

    Returns (plan, None), or (None, refusal reason).
    """
    if any(max_placeable_actors(free[n], recipient_per_actor) >= 1 for n in nodes):
        # The shortfall is from last tick's sizing pass. Capacity has freed
        # since, so normal sizing grants it this tick.
        return None, "capacity_already_free"
    num_removable_actors_by_op = {donor.op: donor.removable for donor in donors}
    recipient_constrained = is_constrained(recipient)

    def plan_node(node: NodeIdStr, max_actors: int) -> NodePlan:
        candidates = _victim_candidates(
            node, donors, recipient_constrained, num_removable_actors_by_op
        )
        return _plan_node(
            node,
            max_actors,
            free,
            recipient_per_actor,
            candidates,
            num_removable_actors_by_op,
        )

    ranked = sorted(
        (plan_node(node, want) for node in nodes),
        key=lambda p: (-p.num_recipient_actors, node_rank.get(p.node, ())),
    )
    plan: List[NodePlan] = []
    actors_still_needed = want
    for probe in ranked:
        if actors_still_needed <= 0:
            break
        if probe.num_recipient_actors == 0:
            continue
        # Plan again: earlier nodes may have used up some donors' removable
        # counts.
        node_plan = plan_node(probe.node, actors_still_needed)
        if node_plan.num_recipient_actors == 0:
            continue
        for victim in node_plan.victims:
            num_removable_actors_by_op[victim.donor.op] -= 1
        plan.append(node_plan)
        actors_still_needed -= node_plan.num_recipient_actors
    if plan:
        return plan, None
    _log_closest_miss(recipient, recipient_per_actor, ranked)
    return None, "victims_insufficient"


def _log_closest_miss(
    recipient: "ExperimentalActorPoolMapOperator",
    recipient_per_actor: ExecutionResources,
    ranked: List[NodePlan],
) -> None:
    misses = [p for p in ranked if p.resource_shortfall]
    if not misses:
        return
    closest = min(misses, key=lambda p: sum(p.resource_shortfall.values()))
    logger.debug(
        "PipelineOptimizer transfer refused: %s needs %s; best node %s "
        "still short by %s after all eligible victims.",
        recipient.name,
        {dim: getattr(recipient_per_actor, dim) for dim in PLACEMENT_RESOURCE_DIMS},
        closest.node[:8],
        {dim: round(v, 2) for dim, v in closest.resource_shortfall.items()},
    )


def _consume_victims_from_view(
    view: OpPlacementView, node: NodeIdStr, count: int
) -> None:
    """Remove planned victims from a donor placement view."""
    remaining = count
    removed = 0

    pending = view.pending_ids_by_node.get(node)
    if pending:
        take = min(remaining, len(pending))
        del pending[:take]
        remaining -= take
        removed += take
        if not pending:
            view.pending_ids_by_node.pop(node, None)

    actor_ids = view.victim_ids_by_node.get(node)
    if remaining > 0 and actor_ids:
        costs = view.victim_drain_cost_by_node.get(node)
        take = min(remaining, len(actor_ids))
        removed_costs = costs[:take] if costs is not None else []

        del actor_ids[:take]
        if costs is not None:
            del costs[:take]
        removed += take

        if not actor_ids:
            view.victim_ids_by_node.pop(node, None)
        if costs is not None and not costs:
            view.victim_drain_cost_by_node.pop(node, None)

        idle_removed = (
            sum(1 for in_flight, _ in removed_costs if in_flight == 0)
            if removed_costs
            else min(view.idle_actors_by_node.get(node, 0), take)
        )
        idle = view.idle_actors_by_node.get(node, 0) - idle_removed
        if idle > 0:
            view.idle_actors_by_node[node] = idle
        else:
            view.idle_actors_by_node.pop(node, None)

    if removed:
        actors = view.actors_by_node.get(node, 0) - removed
        if actors > 0:
            view.actors_by_node[node] = actors
        else:
            view.actors_by_node.pop(node, None)


def _consume_plan_from_donors(
    plan: List[NodePlan],
    donors_by_op: Dict["ExperimentalActorPoolMapOperator", DonorCapacity],
) -> None:
    """Take a fired plan's victims out of the donors, so later recipients in
    the same tick plan against what is left."""
    for node_plan in plan:
        for op, count in node_plan.victims_by_donor().items():
            donor = donors_by_op[op]
            donor.removable = max(0, donor.removable - count)
            _consume_victims_from_view(donor.view, node_plan.node, count)


# --- the rule -----------------------------------------------------------------


class CapacityTransfer(OptimizationRule[TransferState]):
    """Claim, evict, land -- per node and in one request: the recipient's
    targeted pendings are created alongside the victims' eviction, so
    committed accounting reserves the space and nothing else can take it in
    between."""

    name = "capacity_transfer"

    def __init__(
        self,
        metrics: OptimizerMetrics,
        placement: "ActorPlacementStrategy",
        guard: OpScalingGuard,
        reset_signals: Callable[["ExperimentalActorPoolMapOperator"], None],
    ):
        self._metrics = metrics
        self._placement = placement
        self._guard = guard
        self._reset_signals = reset_signals

    @override
    def new_state(self) -> TransferState:
        return TransferState()

    # --- transfers already fired ----------------------------------------------

    @override
    def advance(self, ctx: OptimizationContext, state: TransferState) -> None:
        """Move earlier transfers along: note which new pendings belong to each
        one, close it once every owed actor is placed and running (or cancel it
        after the timeout), hold the capacity the open ones are still owed,
        and release donors whose cooldown is over.

        Recording the pendings here rather than right after the requests were
        applied is safe: the guard blocks the recipient for the whole life of
        the transfer, so no other decision can add pendings to it in between.
        """
        self._record_transfer_pendings(state)
        self._close_finished_transfers(ctx, state)
        self._hold_owed_capacity(ctx, state)
        self._release_cooled_donors(ctx, state)

    @staticmethod
    def _record_transfer_pendings(state: TransferState) -> None:
        """The recipient's actors created since the transfer fired are the
        transfer's (see ``claimed_ids``), whether still pending or already
        running."""
        for transfer in state.in_flight.values():
            new_ids = (
                set(transfer.recipient.actor_pool.get_logical_ids())
                - transfer.preexisting_ids
                - set(transfer.claimed_ids)
            )
            transfer.claimed_ids += tuple(sorted(new_ids))

    @staticmethod
    def _hold_owed_capacity(ctx: OptimizationContext, state: TransferState) -> None:
        """Debit what each open transfer is still owed off ``ctx.free``.

        Every rule advances before any rule applies, and the optimizer runs
        before the per-op passes, so this hold comes before anything else this
        tick can spend the capacity. As many owed actors as the node's free
        capacity fits are brought up by ``apply``; the rest of the hold keeps
        the node's partial free capacity for the recipient while donors drain.

        ``ctx.free`` is rebuilt every tick, so a hold lasts one tick and is
        re-taken here while the transfer is open: closing a transfer (landed,
        lost or timed out) releases it.
        """
        for transfer in state.in_flight.values():
            per_actor = transfer.recipient.actor_pool.per_actor_resource_usage()
            transfer.to_bring_up_by_node = {}
            for node, owed in transfer.owed_by_node.items():
                if owed <= 0:
                    continue  # every actor owed here is already placed
                if node not in ctx.free:
                    # A node we are losing: nothing to place there; the
                    # timeout cancels whatever stays owed.
                    continue
                num_to_bring_up = min(
                    owed, int(max_placeable_actors(ctx.free[node], per_actor))
                )
                # Claims the whole owed amount, not just what fits now: the
                # part that doesn't fit yet is a reservation for actors this
                # transfer places on a later tick, as donors free their slots.
                ctx.claim_free(node, per_actor, owed)
                if num_to_bring_up > 0:
                    transfer.to_bring_up_by_node[node] = num_to_bring_up
                    transfer.owed_by_node[node] = owed - num_to_bring_up

    def _close_finished_transfers(
        self, ctx: OptimizationContext, state: TransferState
    ) -> None:
        """Close transfers with no claimed actor still pending (landed if all
        of them are running, lost otherwise); cancel the ones that timed
        out."""
        for recipient, transfer in list(state.in_flight.items()):
            pool = recipient.actor_pool
            pending = pending_actor_ids(pool)
            still_pending = [a for a in transfer.claimed_ids if a in pending]
            still_owed = sum(transfer.owed_by_node.values())
            if not still_pending and not still_owed:
                # No longer pending is not the same as landed: a claimed actor
                # may have been cancelled or died instead of starting.
                # Nothing claimed means the recipient never grew (its upscale
                # was refused), so the donors gave up actors for nothing.
                landed = bool(transfer.claimed_ids) and all(
                    pool.has_actor(a) for a in transfer.claimed_ids
                )
                logger.debug(
                    "PipelineOptimizer: transfer to %s closed %s (%.0fs after claim)",
                    recipient.name,
                    "landed" if landed else "lost",
                    ctx.now - transfer.started_t,
                )
                self._close_transfer(state, transfer, ctx.now)
                self._metrics.record_outcome(
                    self.name, recipient.name, "landed" if landed else "lost"
                )
                continue
            if ctx.now - transfer.started_t > SIZER_TRANSFER_TIMEOUT_S:
                # Cancel the claim node by node. The pool cancels pendings
                # first, and every pending on a guarded recipient is ours
                # (see ``TransferInFlight.claimed_ids``), so a per-node count
                # is exact.
                claimed = set(still_pending)
                cancel_on = [
                    node
                    for node, ids in pool.pending_ids_by_target_node().items()
                    for actor_id in ids
                    if actor_id in claimed
                ]
                # A transfer still waiting on its donors has nothing pending
                # to cancel; dropping it releases the held capacity.
                if cancel_on:
                    pool.scale(
                        ScaleDownRequest.on_nodes(
                            cancel_on,
                            force=True,
                            reason="optimizer: transfer timed out",
                        )
                    )
                logger.warning(
                    "PipelineOptimizer: transfer to %s timed out after %.0fs "
                    "with %d actor(s) never placed; claim cancelled.",
                    recipient.name,
                    SIZER_TRANSFER_TIMEOUT_S,
                    still_owed,
                )
                # Closing also drops its capacity hold: holds are re-taken
                # each tick only for open transfers.
                self._close_transfer(state, transfer, ctx.now)
                self._metrics.record_outcome(self.name, recipient.name, "timeout")

    def _release_cooled_donors(
        self, ctx: OptimizationContext, state: TransferState
    ) -> None:
        for donor in list(state.cooling_donors):
            window = ctx.productive_tasks_signal.get(donor)
            if window is not None and window.covered():
                self._guard.release(donor)
                state.cooling_donors.discard(donor)

    def _close_transfer(
        self, state: TransferState, transfer: TransferInFlight, now: float
    ) -> None:
        """Free the recipient; start each donor's cooldown and record completion time to block thrashing.

        A donor with surplus to spare can fund several recipients in one pass,
        so a donation closing does not mean the donor is done: it keeps its
        in-flight block until its LAST donation closes, and only then starts
        the cooldown. Otherwise the cooldown could end while another transfer
        is still draining it.
        """
        del state.in_flight[transfer.recipient]
        self._guard.release(transfer.recipient)
        if transfer.record is not None:
            transfer.record.closed_t = now
        still_donating_to = {
            donor: other.recipient
            for other in state.in_flight.values()
            for donor in other.donors
        }
        for donor in transfer.donors:
            if donor in still_donating_to:
                self._guard.block(donor, f"donating to {still_donating_to[donor].name}")
                continue
            # Wait until the donor's signal window is full of samples taken at
            # its smaller size (SIZER_SIGNAL_WINDOW_SAMPLES scheduling-loop
            # steps). Until then its signals still describe the old pool.
            self._reset_signals(donor)
            self._guard.block(donor, "post-donation cooldown")
            state.cooling_donors.add(donor)

    # --- new transfers --------------------------------------------------------

    @override
    def apply(
        self, ctx: OptimizationContext, state: TransferState
    ) -> List[PlacedSizingRequest]:
        # Open transfers place first, into the capacity ``advance`` held.
        requests = self._bring_up_held_actors(state)
        refusal = self._precondition_refusal(ctx)
        if refusal is not None:
            self._metrics.record_refusal(self.name, refusal)
            return requests
        recipients = self._pick_recipients(ctx)
        if not recipients:
            return requests
        donors = self._collect_donors(ctx, recipients, state)
        if not donors:
            return requests
        donors_by_op = {donor.op: donor for donor in donors}
        for recipient in recipients:
            requests.extend(
                self._try_transfer(recipient, donors, donors_by_op, ctx, state)
            )
        return requests

    @staticmethod
    def _bring_up_held_actors(state: TransferState) -> List[PlacedSizingRequest]:
        """Targeted upscales for the recipient actors whose capacity
        ``_hold_owed_capacity`` found free this tick."""
        requests: List[PlacedSizingRequest] = []
        for transfer in state.in_flight.values():
            nodes = [
                node
                for node, count in sorted(transfer.to_bring_up_by_node.items())
                for _ in range(count)
            ]
            transfer.to_bring_up_by_node = {}
            if nodes:
                requests.append(
                    PlacedSizingRequest(
                        request={
                            transfer.recipient: ScaleUpRequest.on_nodes(
                                nodes,
                                reason=(
                                    "optimizer: capacity transfer from "
                                    + "/".join(sorted(d.name for d in transfer.donors))
                                ),
                            )
                        }
                    )
                )
        return requests

    @staticmethod
    def _precondition_refusal(ctx: OptimizationContext) -> Optional[str]:
        if ctx.warmup:
            return "warmup"
        # Transfers correct a settled allocation; while the cluster is still
        # resizing, growth is the autoscaler's move.
        if ctx.seconds_since_allocation_change < SIZER_CLUSTER_SETTLE_S:
            return "cluster_settling"
        return None

    def _pick_recipients(
        self, ctx: OptimizationContext
    ) -> List["ExperimentalActorPoolMapOperator"]:
        """Non-PG ops whose ask has gone ungranted for a full streak. Real asks
        come before silent ones: an op that asked and was clamped is
        evidence; a silent want is an inference, so when donors are scarce
        the real asks get the budget."""
        non_pg = set(ctx.non_pg_ops)
        recipients = []
        for op, streak in ctx.shortfall_streak.items():
            if (
                op not in non_pg
                or not streak.is_streak()
                or not self._guard.can_scale(op)
            ):
                continue
            if any(c in ctx.shortfalls for c in ctx.next_ops.get(op, ())):
                # The op's own consumer is short, so this op's demand is
                # phantom: growing it would pile output onto a consumer that
                # cannot keep up. Serve the consumer first -- this op is donor
                # material, not a recipient.
                continue
            if op.actor_pool.num_pending_actors() > 0:
                # Sizing already in flight; judge again once it lands.
                continue
            recipients.append(op)
        recipients.sort(
            key=lambda op: ctx.shortfalls[op].silent if op in ctx.shortfalls else False
        )
        return recipients

    def _collect_donors(
        self,
        ctx: OptimizationContext,
        recipients: List["ExperimentalActorPoolMapOperator"],
        state: TransferState,
    ) -> List[DonorCapacity]:
        donors: List[DonorCapacity] = []
        for op in ctx.non_pg_ops:
            # A blocked op's signals predate whatever blocked it, so its
            # removable count cannot be trusted; it sits out this round.
            if op in recipients or not self._guard.can_scale(op):
                continue
            donor = assess_donor(op, ctx)
            if donor is not None:
                donors.append(donor)
        if donors:
            state.logged_no_donors = False
            return donors
        # Every non-PG op is short, blocked, or has no provable surplus: there
        # is nothing to move. Normal while demand exceeds the cluster (e.g.
        # during an autoscaling ramp) -- adding capacity is the autoscaler's
        # job, not this rule's.
        self._metrics.record_refusal(self.name, "no_donor_surplus")
        if not state.logged_no_donors:
            logger.debug(
                "PipelineOptimizer: %d capacity-short op(s) but no donor has surplus; "
                "nothing to move.",
                len(recipients),
            )
            state.logged_no_donors = True
        return []

    def _try_transfer(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        donors: List[DonorCapacity],
        donors_by_op: Dict["ExperimentalActorPoolMapOperator", DonorCapacity],
        ctx: OptimizationContext,
        state: TransferState,
    ) -> List[PlacedSizingRequest]:
        """Plan and fire one transfer to ``recipient``; [] if it is refused."""
        nodes = free_schedulable_nodes(ctx, recipient)
        if not nodes:
            return []
        pool = recipient.actor_pool
        shortfall = ctx.shortfalls.get(recipient)
        want = max(1, shortfall.num_actors if shortfall else 1)
        recipient_view = placement_view.build_op_placement_view(
            recipient, rank_victims=False
        )
        node_rank = {n: self._placement.rank_node(recipient_view, n) for n in nodes}
        plan, refusal = plan_transfer(
            recipient,
            pool.per_actor_resource_usage(),
            want,
            nodes,
            ctx.free,
            [d for d in donors if d.op is not recipient],
            node_rank,
        )
        if plan is None:
            self._metrics.record_refusal(self.name, refusal or "unknown")
            return []
        donor_ops = frozenset(op for p in plan for op in p.victims_by_donor())
        if self._is_thrashing(state, recipient, donor_ops, ctx.now):
            self._metrics.record_refusal(self.name, "thrashing")
            if recipient not in state.logged_thrashing:
                state.logged_thrashing.add(recipient)
                logger.debug(
                    "PipelineOptimizer: transfer to %s from %s refused: it would "
                    "undo a transfer still open or closed under %.0fs ago.",
                    recipient.name,
                    "/".join(sorted(d.name for d in donor_ops)),
                    THRASH_WINDOW_S,
                )
            return []
        _consume_plan_from_donors(plan, donors_by_op)

        recipient_per_actor = pool.per_actor_resource_usage()
        requests: List[PlacedSizingRequest] = []
        owed_by_node: Dict[NodeIdStr, int] = {}
        for node_plan in plan:
            # The recipient's actors are owed, not created: they are placed on
            # a later tick, once the victims' capacity is actually free. Hold
            # the node's existing free capacity for them meanwhile.
            ctx.claim_free(
                node_plan.node, recipient_per_actor, node_plan.num_recipient_actors
            )
            owed_by_node[node_plan.node] = node_plan.num_recipient_actors
            req: Dict["ExperimentalActorPoolMapOperator", PlacedScalingRequest] = {}
            for donor, count in node_plan.victims_by_donor().items():
                req[donor] = ScaleDownRequest.on_nodes(
                    (node_plan.node,) * count,
                    force=True,
                    reason=f"optimizer: donate to {recipient.name}",
                )
                self._metrics.record_capacity_transfer(
                    donor.name, recipient.name, count
                )
            requests.append(PlacedSizingRequest(request=req))
        record = RecentTransfer(recipient=recipient, donors=donor_ops)
        state.in_flight[recipient] = TransferInFlight(
            recipient=recipient,
            donors=donor_ops,
            started_t=ctx.now,
            owed_by_node=owed_by_node,
            preexisting_ids=frozenset(pool.get_logical_ids()),
            record=record,
        )
        # The recipient and its donors are ours until the transfer closes, so
        # nothing else resizes them mid-move.
        self._guard.block(recipient, "receiving a capacity transfer")
        for donor in donor_ops:
            self._guard.block(donor, f"donating to {recipient.name}")
        state.recent_transfers.append(record)
        state.logged_thrashing.discard(recipient)
        self._metrics.record_outcome(self.name, recipient.name, "fired")
        logger.debug(
            "PipelineOptimizer: transfer +%d %s across %d node(s) from %s",
            sum(p.num_recipient_actors for p in plan),
            recipient.name,
            len(plan),
            "/".join(sorted(d.name for d in donor_ops)),
        )
        return requests

    @staticmethod
    def _is_thrashing(
        state: TransferState,
        recipient: "ExperimentalActorPoolMapOperator",
        donors: FrozenSet["ExperimentalActorPoolMapOperator"],
        now: float,
    ) -> bool:
        """True if this transfer would undo a recent one (A->B then B->A)
        that is still open or closed within ``THRASH_WINDOW_S``."""
        return any(
            past.inverted_by(recipient, donors, now) for past in state.recent_transfers
        )
