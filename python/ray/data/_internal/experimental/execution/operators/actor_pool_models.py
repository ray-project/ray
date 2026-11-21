"""Data model for the actor-only backend's actor pool.

Scaling requests, per-actor scheduling state and rank, and drain bookkeeping
shared by ``_ExperimentalActorPool`` and ``ExperimentalActorPoolMapOperator``.

Scaling is node-shaped: the sizer says how many actors each node gains or
loses (``ScaleUpRequest`` / ``ScaleDownRequest``), the pool picks the actors
from live state with one fixed ranking.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Optional, Tuple, Union

from ray.core.generated import gcs_pb2
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    ActorPoolScalingRequest,
)
from ray.data._internal.execution.interfaces import (
    NodeIdStr,
)
from ray.data._internal.execution.interfaces.common import ActorStatus, LogicalActorId

# NOTE: generated protos have no type stubs.
# pyrefly: ignore[missing-attribute]
_ACTOR_STATE_DEAD = gcs_pb2.ActorTableData.ActorState.DEAD
# pyrefly: ignore[missing-attribute]
_ACTOR_STATE_ALIVE = gcs_pb2.ActorTableData.ActorState.ALIVE
# pyrefly: ignore[missing-attribute]
_ACTOR_STATE_RESTARTING = gcs_pb2.ActorTableData.ActorState.RESTARTING


class DrainReason(str, Enum):
    # A downscale, or a completed operator's release.
    RESIZE = "resize"
    # The actor's node is being taken away (Ray reports it DRAINING, e.g.
    # spot preemption).
    PREEMPTION = "preemption"
    # The dataset's allocation shrank; the actor is evicted from a reclaimed
    # node.
    ALLOC_SHRINK = "alloc_shrink"

    @property
    def reclaimable(self) -> bool:
        """Only a resize leaves the actor's node ours to keep using."""
        return self is DrainReason.RESIZE


@dataclass(frozen=True)
class ScaleUpRequest(ActorPoolScalingRequest):
    """Add ``delta`` actors.

    ``num_to_reclaim`` of them are draining actors reclaimed in place (busiest
    draining actors are reclaimed first). The rest are created, one
    per entry of ``nodes`` (soft node affinity; a repeated node stacks actors
    on it), or Ray-core-scheduled when ``nodes`` is empty. in case of shortfalls
    (unable to reclaim, node is full, etc) the next tick re-requests.
    """

    nodes: Tuple[NodeIdStr, ...] = ()
    num_to_reclaim: int = 0

    def __post_init__(self):
        assert self.delta > 0, f"ScaleUpRequest needs delta > 0, got {self.delta}"
        assert 0 <= self.num_to_reclaim <= self.delta, (
            f"num_to_reclaim must be within [0, delta]: "
            f"num_to_reclaim={self.num_to_reclaim}, delta={self.delta}"
        )
        assert not self.nodes or len(self.nodes) == self.delta - self.num_to_reclaim, (
            f"One node per created actor: delta={self.delta}, "
            f"num_to_reclaim={self.num_to_reclaim}, nodes={self.nodes}"
        )

    @classmethod
    def on_nodes(
        cls,
        nodes: Iterable[NodeIdStr],
        *,
        num_to_reclaim: int = 0,
        reason: Optional[str] = None,
    ) -> "ScaleUpRequest":
        nodes = tuple(nodes)
        return cls(
            delta=len(nodes) + num_to_reclaim,
            reason=reason,
            nodes=nodes,
            num_to_reclaim=num_to_reclaim,
        )

    @classmethod
    def untargeted(
        cls, delta: int, *, num_to_reclaim: int = 0, reason: Optional[str] = None
    ) -> "ScaleUpRequest":
        return cls(delta=delta, reason=reason, num_to_reclaim=num_to_reclaim)


@dataclass(frozen=True)
class ScaleDownRequest(ActorPoolScalingRequest):
    """Remove ``-delta`` actors.

    ``pending_ids`` names pendings to cancel outright; it is how untargeted
    pendings (placement-group or initial-size actors, which belong to no node)
    are shed. One entry of ``nodes`` per further actor to shed: on each node
    the pool picks from live state, pendings targeted there first, then
    running actors cheapest drain first (fewest tasks in flight, then fewest
    outstanding outputs). A node with fewer eligible actors than asked sheds
    what it has; the applied delta reports the shortfall and the next tick
    re-requests. ``drain_reason`` tags the drain and decides whether a later
    upscale may reclaim the victims (``DrainReason.reclaimable``).
    """

    nodes: Tuple[NodeIdStr, ...] = ()
    pending_ids: Tuple[LogicalActorId, ...] = ()
    drain_reason: DrainReason = DrainReason.RESIZE

    def __post_init__(self):
        assert self.delta < 0, f"ScaleDownRequest needs delta < 0, got {self.delta}"
        assert len(self.nodes) + len(self.pending_ids) == -self.delta, (
            f"One node or pending id per shed actor: delta={self.delta}, "
            f"nodes={self.nodes}, pending_ids={self.pending_ids}"
        )

    @classmethod
    def on_nodes(
        cls,
        nodes: Iterable[NodeIdStr],
        *,
        pending_ids: Iterable[LogicalActorId] = (),
        drain_reason: DrainReason = DrainReason.RESIZE,
        force: bool = False,
        reason: Optional[str] = None,
    ) -> "ScaleDownRequest":
        nodes = tuple(nodes)
        pending_ids = tuple(pending_ids)
        return cls(
            delta=-(len(nodes) + len(pending_ids)),
            force=force,
            reason=reason,
            nodes=nodes,
            pending_ids=pending_ids,
            drain_reason=drain_reason,
        )


# What ``scale_where`` hands the pools.
PlacedScalingRequest = Union[ScaleUpRequest, ScaleDownRequest]


@dataclass
class _PendingActorInfo:
    """Placement bookkeeping for a not-yet-ready actor."""

    logical_id: LogicalActorId
    # Node the actor was pinned to via NodeAffinitySchedulingStrategy if
    # passed, else None.
    target_node_id: Optional[NodeIdStr]
    # When the actor was added as pending; drives the expiry watchdog.
    created_at: float = field(default_factory=time.time)


@dataclass
class _DrainingActorInfo:
    """Bookkeeping for one draining actor."""

    # When the actor began draining.
    since: float
    # Why the drain started (see DrainReason).
    reason: DrainReason
    # Whether a later upscale may reclaim the actor. Starts from the reason;
    # the node-draining hook clears it once the actor's node is going away.
    reclaimable: bool = field(init=False)

    def __post_init__(self):
        self.reclaimable = self.reason.reclaimable


@dataclass
class _ExperimentalActorState:
    """Actor state. Not to be confused with Ray Core actor state that tracks
    DEAD, RESTARTING, or ALIVE statuses, but rather, tracks additional info
    in order to inform Ray Data scheduling decisions."""

    # Pool-assigned logical id (carried in actor labels). Stable for the
    # actor's lifetime; lets us go from state -> logical id without a
    # reverse lookup.
    logical_id: LogicalActorId

    # Number of tasks in flight per actor
    num_tasks_in_flight: int

    # Total bytes of inputs currently in flight on this actor.
    num_input_bytes_in_flight: int

    # Node id of each ready actor
    actor_location: NodeIdStr

    # Per-actor output backpressure limits. ``max_num_output_bytes`` is the
    # live cap: it starts at ``default_max_num_output_bytes`` (below), is
    # raised by ``upgrade_output_limit`` (the deadlock hatch) and restored to
    # the default by ``reset_output_limit``.
    max_num_outputs: float
    max_num_output_bytes: float

    # Per-actor input admission (see
    # ``_ExperimentalActorPool._is_available_for_scheduling``): the actor
    # accepts another task while ``num_tasks_in_flight < max_tasks_in_flight``.
    max_tasks_in_flight: int

    latest_task_submission_ts: float

    status: ActorStatus

    # Baseline output-bytes cap derived when the actor became ready: the
    # configured ``max_num_output_bytes_per_actor``, else the actor's share of
    # its node's object store. ``reset_output_limit`` restores
    # ``max_num_output_bytes`` to this once a raised cap is no longer needed.
    default_max_num_output_bytes: float = float("inf")


# Scheduling rank used by both the per-node (locality) heap and the global
# fallback heap. Lower is preferred. Sort order: ascending
# ``num_tasks_in_flight``, ``latest_task_submission_ts`` (less-loaded actor
# first, where tiebreaks are handled by the earliest latest_task_submission_ts).
@dataclass(order=True)
class _ExperimentalActorRank:
    num_tasks_in_flight: int
    latest_task_submission_ts: float
