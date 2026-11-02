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
    SIZER_CLUSTER_SETTLE_S,
    SIZER_TRANSFER_TIMEOUT_S,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import LogicalActorId, NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ExperimentalActorPoolScalingRequest,
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


@dataclass
class TransferInFlight:
    """One transfer, from the moment it fires until its actors land (or it
    times out and the claim is cancelled)."""

    recipient: "ExperimentalActorPoolMapOperator"
    donors: FrozenSet["ExperimentalActorPoolMapOperator"]
    started_t: float
    # The recipient's actors (pending and running) when the transfer fired.
    preexisting_ids: FrozenSet[LogicalActorId] = frozenset()
    # The recipient's actors created since: the transfer's (the guard blocks
    # every other decision on the recipient while it is in flight).
    claimed_ids: Tuple[LogicalActorId, ...] = ()


@dataclass
class TransferState:
    """What CapacityTransfer carries between ticks; owned by the optimizer."""

    in_flight: Dict["ExperimentalActorPoolMapOperator", TransferInFlight] = field(
        default_factory=dict
    )
    # Recent (recipient, donors) pairs. A transfer that inverts one of these is
    # thrashing the same capacity back and forth.
    recent_transfers: Deque[
        Tuple[
            "ExperimentalActorPoolMapOperator",
            FrozenSet["ExperimentalActorPoolMapOperator"],
        ]
    ] = field(default_factory=lambda: deque(maxlen=8))
    # Donors blocked until their signal window refills (see _close_transfer).
    cooling_donors: Set["ExperimentalActorPoolMapOperator"] = field(default_factory=set)
    # Edge-triggered so "no donor surplus" logs once per episode, not per tick.
    logged_no_donors: bool = False


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
        one, close it once they are all ready (or cancel them after the
        timeout), and release donors whose cooldown is over.

        Recording the pendings here rather than right after the requests were
        applied is safe: the guard blocks the recipient for the whole life of
        the transfer, so no other decision can add pendings to it in between.
        """
        self._record_transfer_pendings(state)
        self._close_finished_transfers(ctx, state)
        self._release_cooled_donors(ctx, state)

    @staticmethod
    def _record_transfer_pendings(state: TransferState) -> None:
        """The recipient's actors created since the transfer fired are the
        transfer's (see ``claimed_ids``), whether still pending or already
        running."""
        for transfer in state.in_flight.values():
            if transfer.claimed_ids:
                continue
            transfer.claimed_ids = tuple(
                sorted(
                    set(transfer.recipient.actor_pool.get_logical_ids())
                    - transfer.preexisting_ids
                )
            )

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
            if not still_pending:
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
                self._close_transfer(state, transfer)
                self._metrics.record_outcome(
                    self.name, recipient.name, "landed" if landed else "lost"
                )
                continue
            if ctx.now - transfer.started_t > SIZER_TRANSFER_TIMEOUT_S:
                pool.scale(
                    ExperimentalActorPoolScalingRequest(
                        delta=-len(still_pending),
                        force=True,
                        reason="optimizer: transfer timed out",
                        actor_ids_to_downscale=tuple(still_pending),
                    )
                )
                logger.warning(
                    "PipelineOptimizer: transfer to %s timed out after %.0fs; "
                    "claim cancelled.",
                    recipient.name,
                    SIZER_TRANSFER_TIMEOUT_S,
                )
                self._close_transfer(state, transfer)
                self._metrics.record_outcome(self.name, recipient.name, "timeout")

    def _release_cooled_donors(
        self, ctx: OptimizationContext, state: TransferState
    ) -> None:
        for donor in list(state.cooling_donors):
            window = ctx.productive_tasks_signal.get(donor)
            if window is not None and window.covered():
                self._guard.release(donor)
                state.cooling_donors.discard(donor)

    def _close_transfer(self, state: TransferState, transfer: TransferInFlight) -> None:
        """Free the recipient; start each donor's cooldown.

        A donor with surplus to spare can fund several recipients in one pass,
        so a donation closing does not mean the donor is done: it keeps its
        in-flight block until its LAST donation closes, and only then starts
        the cooldown. Otherwise the cooldown could end while another transfer
        is still draining it.
        """
        del state.in_flight[transfer.recipient]
        self._guard.release(transfer.recipient)
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
        refusal = self._precondition_refusal(ctx)
        if refusal is not None:
            self._metrics.record_refusal(self.name, refusal)
            return []
        recipients = self._pick_recipients(ctx)
        if not recipients:
            return []
        donors = self._collect_donors(ctx, recipients, state)
        if not donors:
            return []
        donors_by_op = {donor.op: donor for donor in donors}
        requests: List[PlacedSizingRequest] = []
        for recipient in recipients:
            requests.extend(
                self._try_transfer(recipient, donors, donors_by_op, ctx, state)
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
        if self._is_thrashing(state, recipient, donor_ops):
            self._metrics.record_refusal(self.name, "thrashing")
            return []
        _consume_plan_from_donors(plan, donors_by_op)

        recipient_per_actor = pool.per_actor_resource_usage()
        requests: List[PlacedSizingRequest] = []
        for node_plan in plan:
            # Victims' capacity is not credited (they hold it until drained),
            # so this only takes back what the recipient uses of existing free.
            ctx.claim_free(
                node_plan.node, recipient_per_actor, node_plan.num_recipient_actors
            )
            req: Dict[
                "ExperimentalActorPoolMapOperator", ExperimentalActorPoolScalingRequest
            ] = {
                recipient: ExperimentalActorPoolScalingRequest(
                    delta=node_plan.num_recipient_actors,
                    reason="optimizer: capacity transfer in",
                    target_nodes_to_scale_actors_on=(node_plan.node,)
                    * node_plan.num_recipient_actors,
                )
            }
            for donor, count in node_plan.victims_by_donor().items():
                req[donor] = ExperimentalActorPoolScalingRequest(
                    delta=-count,
                    force=True,
                    reason=f"optimizer: donate to {recipient.name}",
                    victim_node=node_plan.node,
                )
                self._metrics.record_capacity_transfer(
                    donor.name, recipient.name, count
                )
            requests.append(PlacedSizingRequest(request=req))
        state.in_flight[recipient] = TransferInFlight(
            recipient=recipient,
            donors=donor_ops,
            started_t=ctx.now,
            preexisting_ids=frozenset(pool.get_logical_ids()),
        )
        # The recipient and its donors are ours until the transfer closes, so
        # nothing else resizes them mid-move.
        self._guard.block(recipient, "receiving a capacity transfer")
        for donor in donor_ops:
            self._guard.block(donor, f"donating to {recipient.name}")
        state.recent_transfers.append((recipient, donor_ops))
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
    ) -> bool:
        """True if this transfer would undo a recent one (A->B then B->A)."""
        return any(
            recipient in past_donors and past_recipient in donors
            for past_recipient, past_donors in state.recent_transfers
        )
