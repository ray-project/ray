import logging
from typing import TYPE_CHECKING, List

from .actor_pool_drain_policy import ActorPoolDrainPolicy
from .backpressure_policy import BackpressurePolicy
from .concurrency_cap_backpressure_policy import ConcurrencyCapBackpressurePolicy
from .downstream_capacity_backpressure_policy import (
    DownstreamCapacityBackpressurePolicy,
)
from .resource_budget_backpressure_policy import ResourceBudgetBackpressurePolicy
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)

# Default enabled backpressure policies and its config key.
# Use `DataContext.set_config` to config it.
# NOTE: `ActorPoolDrainPolicy` is inert unless
# `DataContext.actor_pool_drain_window_s` is set, and must stay *first*:
# `get_eligible_operators` short-circuits on the first policy that returns False,
# and this one has to see every operator on every iteration to keep its
# per-operator stall window rolling. `get_backpressure_policies` enforces that
# position for custom lists too.
ENABLED_BACKPRESSURE_POLICIES = [
    ActorPoolDrainPolicy,
    ConcurrencyCapBackpressurePolicy,
    ResourceBudgetBackpressurePolicy,
    DownstreamCapacityBackpressurePolicy,
]
ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY = "backpressure_policies.enabled"


def _with_drain_policy_first(policies: List[type]) -> List[type]:
    """Return ``policies`` with ``ActorPoolDrainPolicy`` moved to the front.

    The policy is stateful across iterations -- it keeps a rolling stall window
    per operator -- so it has to be consulted every time. `get_eligible_operators`
    stops at the first policy that returns False, so a drain policy sitting behind
    another one would both freeze its window and skip its own release conditions
    (an eligible downstream running dry, an external consumer waiting), leaving
    `actor_pool_drain_in_progress` set while the actor autoscaler keeps acting on
    it. A custom list may name it anywhere; honour the requirement here rather
    than trusting the order.
    """
    if ActorPoolDrainPolicy not in policies:
        return list(policies)

    reordered = [ActorPoolDrainPolicy] + [
        policy for policy in policies if policy is not ActorPoolDrainPolicy
    ]
    if policies[0] is not ActorPoolDrainPolicy:
        logger.debug(
            "Moved ActorPoolDrainPolicy to the front of the enabled backpressure "
            "policies: it has to be consulted before any policy that can "
            "short-circuit the chain."
        )
    return reordered


def get_backpressure_policies(
    data_context: DataContext,
    topology: "Topology",
    resource_manager: "ResourceManager",
) -> List[BackpressurePolicy]:
    policies = data_context.get_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY, ENABLED_BACKPRESSURE_POLICIES
    )

    policies = _with_drain_policy_first(policies)

    return [policy(data_context, topology, resource_manager) for policy in policies]


__all__ = [
    "BackpressurePolicy",
    "ConcurrencyCapBackpressurePolicy",
    "DownstreamCapacityBackpressurePolicy",
    "ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY",
    "ActorPoolDrainPolicy",
    "get_backpressure_policies",
]
