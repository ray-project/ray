from typing import TYPE_CHECKING

import ray
from .backpressure_policy import BackpressurePolicy
from .concurrency_cap_backpressure_policy import ConcurrencyCapBackpressurePolicy
from .streaming_output_backpressure_policy import StreamingOutputBackpressurePolicy

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology

# Default enabled backpressure policies and its config key.
# Use `DataContext.set_config` to config it.
# TODO(hchen): Enable StreamingOutputBackpressurePolicy by default.
ENABLED_BACKPRESSURE_POLICIES = [ConcurrencyCapBackpressurePolicy]
ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY = "backpressure_policies.enabled"


def get_backpressure_policies(topology: "Topology"):
    data_context = ray.data.DataContext.get_current()
    policies = data_context.get_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY, ENABLED_BACKPRESSURE_POLICIES
    )

    return [policy(topology) for policy in policies]


__all__ = [
    "BackpressurePolicy",
    "ConcurrencyCapBackpressurePolicy",
    "StreamingOutputBackpressurePolicy",
    "ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY",
    "get_backpressure_policies",
]
