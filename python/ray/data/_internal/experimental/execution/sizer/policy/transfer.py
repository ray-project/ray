"""Transfer mechanics: move actors from donors to a recipient.

A transfer fires by draining victims on the recipient's nodes and holding
those nodes' capacity for the recipient's owed actors; it places them as the
victims free their slots, and closes once they run (or times out). Who
receives and who donates is the bottleneck policy's call
(``sizing_policy``); this module only plans and carries out the move.
"""

from __future__ import annotations

import logging
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

from ray.data._internal.execution.execution_flags import (
    SIZER_CADENCE_S,
    SIZER_TRANSFER_THRASH_WINDOW_TICKS,
    SIZER_TRANSFER_TIMEOUT_S,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorId, NodeIdStr
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
from ray.data._internal.experimental.execution.sizer.op_status import OpStatusMachine
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.growth import (
    pending_actor_ids,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    TransferPhase,
)

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
    asks for more than its slice of a node. Until then the transfer reserves
    that node's free capacity each tick (see ``TransferBook.reserve_owed``),
    so nothing else in the sizer takes it.
    """

    recipient: "ExperimentalActorPoolMapOperator"
    donors: FrozenSet["ExperimentalActorPoolMapOperator"]
    started_t: float
    # Last time the transfer placed recipient actors (initially when it
    # fired). The timeout runs from here: a donor that drains slowly delays
    # placement, and that delay must not eat into the placed actors' startup.
    # Defaults to ``started_t``.
    last_progress_t: Optional[float] = None
    # Recipient actors still to place, per node.
    owed_by_node: Dict[NodeIdStr, int] = field(default_factory=dict)
    # Recipient actors to bring up this tick (their capacity is free and
    # held), per node; ``apply`` creates them.
    to_bring_up_by_node: Dict[NodeIdStr, int] = field(default_factory=dict)
    # The recipient's actors (pending and running) when the transfer fired.
    preexisting_ids: FrozenSet[ActorId] = frozenset()
    # The recipient's actors created since, accumulated as they are placed:
    # the transfer's (the recipient is RECEIVING, so no other decision
    # resizes it while it is in flight).
    claimed_ids: Tuple[ActorId, ...] = ()
    # This transfer's entry in ``TransferState.recent_transfers``, stamped
    # with the close time when it closes.
    record: Optional[RecentTransfer] = None
    # Seconds without progress before the claim is cancelled.
    timeout_s: float = SIZER_TRANSFER_TIMEOUT_S

    def __post_init__(self) -> None:
        if self.last_progress_t is None:
            self.last_progress_t = self.started_t

    def seconds_without_progress(self, now: float) -> float:
        return now - (
            self.started_t if self.last_progress_t is None else self.last_progress_t
        )


@dataclass
class TransferState:
    """What the transfer book carries between ticks."""

    # # NOTE: Keyed by recipients, not donors here
    in_flight: Dict["ExperimentalActorPoolMapOperator", TransferInFlight] = field(
        default_factory=dict
    )
    # Recent transfers. One that a new transfer inverts within THRASH_WINDOW_S
    # of closing is thrashing the same capacity back and forth.
    recent_transfers: Deque[RecentTransfer] = field(
        default_factory=lambda: deque(maxlen=8)
    )
    # Recipients whose thrashing refusal was logged since they last received.
    logged_thrashing: Set["ExperimentalActorPoolMapOperator"] = field(
        default_factory=set
    )


@dataclass
class DonorCapacity:
    """An operator a transfer may take actors from: its placement view (the
    victims on each node) and how many actors it can give up."""

    op: "ExperimentalActorPoolMapOperator"
    view: OpPlacementView
    per_actor: ExecutionResources
    removable: int


# --- planning -----------------------------------------------------------------


@dataclass(frozen=True, order=True)
class VictimCandidate:
    """One actor a donor could give up. Sorts cheapest to evict first."""

    eviction_rank: int
    unconsumed_outputs: int
    tasks_in_flight: int
    donor: DonorCapacity = field(compare=False)
    actor_id: ActorId = field(compare=False)

    @classmethod
    def build(
        cls,
        donor: DonorCapacity,
        actor_id: ActorId,
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


# Orders victim candidates; a key of None excludes the candidate.
VictimKey = Callable[["VictimCandidate"], Optional[Tuple]]


def _victim_candidates(
    node: NodeIdStr,
    donors: List[DonorCapacity],
    recipient_constrained: bool,
    num_removable_actors_by_op: Dict["ExperimentalActorPoolMapOperator", int],
    victim_key: Optional[VictimKey] = None,
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
    if victim_key is None:
        return sorted(candidates)
    keyed = [(victim_key(c), c) for c in candidates]
    return [
        c
        for key, c in sorted(
            ((key, c) for key, c in keyed if key is not None), key=lambda kc: kc[0]
        )
    ]


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
    victim_key: Optional[VictimKey] = None,
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
    if recipient_per_actor.gpu == 0 and any(d.per_actor.gpu > 0 for d in donors):
        # Evicting a GPU actor for its CPU leaves a GPU idle, so a CPU-only
        # recipient takes GPU actors only when CPU-only donors can't cover it.
        cpu_only = [donor for donor in donors if donor.per_actor.gpu == 0]
        if cpu_only:
            cpu_only_plan, refusal = plan_transfer(
                recipient,
                recipient_per_actor,
                want,
                nodes,
                free,
                cpu_only,
                node_rank,
                victim_key,
            )
            if cpu_only_plan is not None:
                return cpu_only_plan, refusal
    num_removable_actors_by_op = {donor.op: donor.removable for donor in donors}
    recipient_constrained = is_constrained(recipient)

    def plan_node(node: NodeIdStr, max_actors: int) -> NodePlan:
        candidates = _victim_candidates(
            node, donors, recipient_constrained, num_removable_actors_by_op, victim_key
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
        "SizingPolicy: transfer refused: %s needs %s; best node %s "
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


# --- the transfer book --------------------------------------------------------


class TransferBook:
    """Claim, evict, land -- per node and in one request: the recipient's
    actors are owed when a transfer fires and created once the victims'
    capacity is free, with that capacity held for them meanwhile, so nothing
    else can take it in between.

    Owns every open transfer (``TransferState``) and moves each through its
    life: fired, placing as donors free capacity, then closed as landed,
    lost or timed out. Recipient and donors are held in ``OpStatusMachine``
    from firing until close; donors then cool down there.
    """

    def __init__(
        self,
        name: str,
        metrics: OptimizerMetrics,
        placement: "ActorPlacementStrategy",
        statuses: OpStatusMachine,
    ):
        self._name = name
        self._metrics = metrics
        self._placement = placement
        self._status_machine = statuses
        self.state = TransferState()

    @property
    def phase(self) -> TransferPhase:
        return TransferPhase.IN_FLIGHT if self.state.in_flight else TransferPhase.IDLE

    # --- transfers already fired ----------------------------------------------

    def advance(self, now: float) -> None:
        """Move the open transfers along: note which new pending actors belong
        to each one, and close each one whose owed actors are all placed and
        running (or cancel it after the timeout).

        Recording the pendings here rather than right after the requests were
        applied is safe: the recipient is ``RECEIVING`` for the whole life of
        the transfer, so no other decision can add pendings to it in between.
        """
        self._record_transfer_pendings()
        self._close_finished_transfers(now)

    def _record_transfer_pendings(self) -> None:
        """The recipient's actors created since the transfer fired are the
        transfer's (see ``claimed_ids``), whether still pending or already
        running."""
        for transfer in self.state.in_flight.values():
            new_ids = (
                set(transfer.recipient.actor_pool.get_logical_ids())
                - transfer.preexisting_ids
                - set(transfer.claimed_ids)
            )
            transfer.claimed_ids += tuple(sorted(new_ids))

    def reserve_owed(
        self, free_by_node: Dict[NodeIdStr, ExecutionResources], now: float
    ) -> None:
        """Take each open transfer's owed room out of ``free_by_node``, in
        the order the transfers fired, and note how many owed actors fit now.

        Each transfer reserves the room for every actor it still owes on a
        node, not only the ones that fit now. The rest of the reservation
        keeps the node's partial free room for the recipient while its donors'
        actors go. ``bring_up_held_actors`` brings up the actors that fit.

        The sizer calls this every tick before anything else reads free
        capacity, so a reservation lasts one tick. Closing a transfer (landed,
        lost or timed out) ends its reservation.
        """
        for transfer in self.state.in_flight.values():
            per_actor = transfer.recipient.actor_pool.per_actor_resource_usage()
            transfer.to_bring_up_by_node = {}
            for node, owed in transfer.owed_by_node.items():
                if owed <= 0:
                    continue  # every actor owed here is already placed
                if node not in free_by_node:
                    # A node we are losing: nothing to place there; the
                    # timeout cancels whatever stays owed.
                    continue
                num_to_bring_up = min(
                    owed, int(max_placeable_actors(free_by_node[node], per_actor))
                )
                free_by_node[node] = (
                    free_by_node[node]
                    .subtract(per_actor.scale(owed))
                    .max(ExecutionResources.zero())
                )
                if num_to_bring_up > 0:
                    transfer.to_bring_up_by_node[node] = num_to_bring_up
                    transfer.owed_by_node[node] = owed - num_to_bring_up
                    transfer.last_progress_t = now

    def _close_finished_transfers(self, now: float) -> None:
        """Close transfers with no claimed actor still pending (landed if all
        of them are running, lost otherwise); cancel the ones that timed
        out."""
        for recipient, transfer in list(self.state.in_flight.items()):
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
                    "SizingPolicy: transfer to %s closed %s (%.0fs after claim)",
                    recipient.name,
                    "landed" if landed else "lost",
                    now - transfer.started_t,
                )
                self._close_transfer(transfer, now)
                self._metrics.record_outcome(
                    self._name, recipient.name, "landed" if landed else "lost"
                )
                continue
            if transfer.seconds_without_progress(now) > transfer.timeout_s:
                # Cancel the claim node by node. The pool cancels pendings
                # first, and every pending on a receiving recipient is ours
                # (see ``TransferInFlight.claimed_ids``), so a per-node count
                # is exact.
                cancel_on = [
                    node
                    for node, ids in pool.pending_ids_by_target_node().items()
                    for actor_id in ids
                    if actor_id in set(still_pending)
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
                    "SizingPolicy: transfer to %s made no progress for %.0fs "
                    "with %d actor(s) never placed; claim cancelled.",
                    recipient.name,
                    transfer.timeout_s,
                    still_owed,
                )
                # Closing also drops its capacity hold: holds are re-taken
                # each tick only for open transfers.
                self._close_transfer(transfer, now)
                self._metrics.record_outcome(self._name, recipient.name, "timeout")

    def _close_transfer(self, transfer: TransferInFlight, now: float) -> None:
        """Free the recipient; start each donor's cooldown and record the
        close time to block thrashing.

        A donor can fund several recipients, so a donation closing does not
        mean the donor is done: it stays ``DONATING`` until its LAST donation
        closes, and only then cools down. Otherwise the cooldown could end
        while another transfer is still draining it.
        """
        del self.state.in_flight[transfer.recipient]
        self._status_machine.activate(transfer.recipient)
        if transfer.record is not None:
            transfer.record.closed_t = now
        still_donating_to = {
            donor: transfer.recipient
            for transfer in self.state.in_flight.values()
            for donor in transfer.donors
        }
        for donor in transfer.donors:
            if donor in still_donating_to:
                self._status_machine.start_donating(donor, still_donating_to[donor])
                continue
            # Wait until the donor's signals describe its smaller size
            # (SIZER_DONOR_COOLDOWN_STEPS scheduling-loop steps).
            self._status_machine.start_cooling(donor)

    # --- new transfers --------------------------------------------------------

    def bring_up_held_actors(self) -> List[PlacedSizingRequest]:
        """Targeted upscales for the recipient actors that ``reserve_owed``
        found room for this tick."""
        requests: List[PlacedSizingRequest] = []
        for transfer in self.state.in_flight.values():
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

    def transfer_nodes(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        ctx: DecisionContext,
    ) -> List[NodeIdStr]:
        """Return the nodes the recipient may run on, minus any node where an
        open transfer has not yet brought up all of its recipient's actors.
        """
        busy_nodes = {
            node
            for transfer in self.state.in_flight.values()
            for node, owed in transfer.owed_by_node.items()
            if owed > 0
        }
        return [n for n in ctx.capacity.free_nodes(recipient) if n not in busy_nodes]

    def plan(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        want: int,
        nodes: List[NodeIdStr],
        donors: List[DonorCapacity],
        ctx: DecisionContext,
        victim_key: Optional[VictimKey] = None,
    ) -> Tuple[Optional[List["NodePlan"]], Optional[str]]:
        """``plan_transfer`` on ``nodes``, ranked by the recipient's locality."""
        recipient_view = placement_view.build_op_placement_view(
            recipient, rank_victims=False
        )
        node_rank = {n: self._placement.rank_node(recipient_view, n) for n in nodes}
        return plan_transfer(
            recipient,
            recipient.actor_pool.per_actor_resource_usage(),
            want,
            nodes,
            ctx.capacity.free,
            [d for d in donors if d.op is not recipient],
            node_rank,
            victim_key,
        )

    def fire(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        plan: List["NodePlan"],
        donors_by_op: Dict["ExperimentalActorPoolMapOperator", DonorCapacity],
        ctx: DecisionContext,
        *,
        timeout_s: float = SIZER_TRANSFER_TIMEOUT_S,
    ) -> List[PlacedSizingRequest]:
        """Fire a planned transfer: drain its victims, hold the recipient's
        capacity, open it in flight. [] if it would thrash."""
        state = self.state
        pool = recipient.actor_pool
        donor_ops = frozenset(op for p in plan for op in p.victims_by_donor())
        if self._is_thrashing(recipient, donor_ops, ctx.now):
            self._metrics.record_refusal(self._name, "thrashing")
            if recipient not in state.logged_thrashing:
                state.logged_thrashing.add(recipient)
                logger.debug(
                    "SizingPolicy: transfer to %s from %s refused: it would "
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
            ctx.capacity.claim(
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
            last_progress_t=ctx.now,
            owed_by_node=owed_by_node,
            preexisting_ids=frozenset(pool.get_logical_ids()),
            record=record,
            timeout_s=timeout_s,
        )
        # The recipient and its donors are the transfer's until it closes, so
        # nothing else resizes them mid-move.
        self._status_machine.start_receiving(recipient)
        for donor in donor_ops:
            self._status_machine.start_donating(donor, recipient)
        state.recent_transfers.append(record)
        state.logged_thrashing.discard(recipient)
        self._metrics.record_outcome(self._name, recipient.name, "fired")
        logger.debug(
            "SizingPolicy: transfer +%d %s across %d node(s) from %s",
            sum(p.num_recipient_actors for p in plan),
            recipient.name,
            len(plan),
            "/".join(sorted(d.name for d in donor_ops)),
        )
        return requests

    def _is_thrashing(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        donors: FrozenSet["ExperimentalActorPoolMapOperator"],
        now: float,
    ) -> bool:
        """True if this transfer would undo a recent one (A->B then B->A)
        that is still open or closed within ``THRASH_WINDOW_S``."""
        return any(
            past.inverted_by(recipient, donors, now)
            for past in self.state.recent_transfers
        )
