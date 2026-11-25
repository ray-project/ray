"""Type aliases and small value types shared across the actor-only sizer.

A leaf module: it imports nothing from the sizer, so any sizer module can
import it without a cycle.
"""

from typing import TYPE_CHECKING, Dict, NamedTuple

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

# Resources per node (free, reserved, committed, to reclaim, ...). Unlike
# ``NodeResources`` in the autoscaling coordinator, values are
# ``ExecutionResources``, not raw resource dicts.
ExecutionResourcesByNode = Dict[NodeIdStr, ExecutionResources]

# A count per node (actors, ops, ...).
CountByNode = Dict[NodeIdStr, int]

# The scaling request each op hands its pool this tick.
ScalingRequests = Dict["ExperimentalActorPoolMapOperator", "ActorPoolScalingRequest"]


class SizeChange(NamedTuple):
    """An op's pool size before an upscale and once it landed."""

    before: int
    after: int


# Each upscale that landed this tick.
LandedUpscales = Dict["ExperimentalActorPoolMapOperator", SizeChange]


class UpscaleSplit(NamedTuple):
    """How one upscale's actors split between placement phases."""

    # Placed next to an upstream or downstream neighbour's actors.
    collocated: int
    # Spread over the remaining nodes.
    spread: int


class DrainCost(NamedTuple):
    """What must reach zero before a draining actor can exit. Lower frees
    capacity sooner."""

    tasks_in_flight: int
    # Output blocks the ResourceBank still counts against the actor.
    unconsumed_outputs: int
