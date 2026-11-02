"""Data model for the actor-only backend's actor pool.

Dataclasses and enums shared by ``_ExperimentalActorPool`` and
``ExperimentalActorPoolMapOperator``: placement-resolved scaling requests,
per-actor scheduling state and rank, and drain bookkeeping.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import (
    Optional,
    Tuple,
)

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


@dataclass(frozen=True)
class ExperimentalActorPoolScalingRequest(ActorPoolScalingRequest):
    """Placement-resolved scaling request for ``_ExperimentalActorPool``.

    Supports scaling actors on specific nodes and killing specific actors on
    scale down."""

    # Upscale: one node id PER NEW ACTOR, repeat node id if scaling up multiple
    # actors on the same node; len == delta - num_to_reclaim.
    target_nodes_to_scale_actors_on: Tuple[NodeIdStr, ...] = ()
    # Downscale: exact victim actors; len == -delta. For actor-shaped
    # decisions: an ordinary downscale whose caller already picked victims,
    # or cancelling specific still-pending actors.
    actor_ids_to_downscale: Tuple[LogicalActorId, ...] = ()
    # Downscale: pick ``-delta`` victims ON THIS NODE at apply time from live
    # actor state, cheapest drain first (fewest in-flight tasks, then fewest
    # unconsumed outputs). For node-shaped decisions -- the caller needs capacity freed
    # on one specific node (e.g. a donor evicted from where another pool's
    # actor is about to land) -- where deferring the pick to apply time keeps
    # a snapshot-time choice from racing task dispatch.
    victim_node: Optional[NodeIdStr] = None
    # Upscale: revive this many still-DRAINING actors (from a prior downscale
    # that hasn't finished) back into the running pool instead of creating
    # fresh ones. They already hold their node's resources, so this is free
    # capacity-wise and skips actor startup. ``delta`` is the TOTAL upscale;
    # ``len(target_nodes_to_scale_actors_on) == delta - num_to_reclaim``
    # (only the genuinely-new actors carry target nodes).
    num_to_reclaim: int = 0
    # Downscale only: when False, the victims are being EVICTED because our
    # allocation shrank -- they drain gracefully but must never be revived by a
    # later upscale (their resource is no longer ours). Default True (ordinary
    # downscale victims stay revivable).
    reclaimable: bool = True

    @classmethod
    def from_base(
        cls, req: ActorPoolScalingRequest, **overrides
    ) -> "ExperimentalActorPoolScalingRequest":
        if isinstance(req, cls):
            return replace(req, **overrides) if overrides else req
        kwargs = dict(delta=req.delta, force=req.force, reason=req.reason)
        kwargs.update(overrides)
        return cls(**kwargs)


@dataclass
class _PendingActorInfo:
    """Placement bookkeeping for a not-yet-ready actor."""

    logical_id: LogicalActorId
    # Node the actor was pinned to via NodeAffinitySchedulingStrategy if
    # passed, else None.
    target_node_id: Optional[NodeIdStr]
    # When the actor was added as pending; drives the expiry watchdog.
    created_at: float = field(default_factory=time.time)


class DrainReason(str, Enum):
    """Why a drain started. Values are the ``reason`` tag on the actor-only
    drain metrics."""

    # A resize: downscale, or a completed operator's release.
    RESIZE = "resize"
    # The actor's node is being taken away (Ray reports it DRAINING, e.g.
    # spot preemption).
    PREEMPTION = "preemption"
    # The dataset's allocation shrank; the actor is evicted from a reclaimed
    # node.
    ALLOC_SHRINK = "alloc_shrink"
    # The caller did not say.
    UNKNOWN = "unknown"


@dataclass
class _DrainingActorInfo:
    """Bookkeeping for one draining actor."""

    # When the actor began draining.
    since: float
    # Whether this actor may be revived by reclaim_draining_actors. False for
    # actors evicted because our allocation shrank -- their node is no longer
    # ours, so a later upscale must never revive them (they drain and die).
    reclaimable: bool = True
    # Why the drain started (see DrainReason).
    reason: DrainReason = DrainReason.UNKNOWN


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
