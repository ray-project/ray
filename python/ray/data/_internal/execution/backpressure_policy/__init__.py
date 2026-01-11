from typing import TYPE_CHECKING, List

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

# Default enabled backpressure policies and its config key.
# Use `DataContext.set_config` to config it.
ENABLED_BACKPRESSURE_POLICIES = [
    ConcurrencyCapBackpressurePolicy,
    ResourceBudgetBackpressurePolicy,
    DownstreamCapacityBackpressurePolicy,
]
ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY = "backpressure_policies.enabled"


def get_backpressure_policies(
    data_context: DataContext,
    topology: "Topology",
    resource_manager: "ResourceManager",
) -> List[BackpressurePolicy]:
    policies = data_context.get_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY, ENABLED_BACKPRESSURE_POLICIES
    )

    return [policy(data_context, topology, resource_manager) for policy in policies]


__all__ = [
    "BackpressurePolicy",
    "ConcurrencyCapBackpressurePolicy",
    "DownstreamCapacityBackpressurePolicy",
    "ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY",
    "get_backpressure_policies",
]
