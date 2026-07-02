import logging
from typing import TYPE_CHECKING

from ray.data._internal.usage.collector import (
    EnvInfo,
    Issue,
    LogicalOp,
    OpConfig,
    PipelinePerf,
    PlanNode,
    UsageInfo,
    WorkloadInfo,
    record_usage_info,
)
from ray.data._internal.usage.execution_callback import UsageCallback

if TYPE_CHECKING:
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)


def create_usage_callback(logical_plan: "LogicalPlan") -> UsageCallback:
    """Create the usage callback for an execution.

    Factory method to return a ``UsageCallback`` object.
    """
    try:
        from ray.anyscale.data._internal.usage import RayTurboUsageCallback
    except ImportError:
        return UsageCallback(logical_plan)
    return RayTurboUsageCallback(logical_plan)


__all__ = [
    "EnvInfo",
    "Issue",
    "LogicalOp",
    "OpConfig",
    "PipelinePerf",
    "PlanNode",
    "UsageCallback",
    "UsageInfo",
    "WorkloadInfo",
    "create_usage_callback",
    "record_usage_info",
]
