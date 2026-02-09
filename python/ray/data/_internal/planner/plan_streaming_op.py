from typing import List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.unbounded_data_operator import (
    UnboundedDataOperator,
)
from ray.data._internal.logical.operators.unbound_data_operator import (
    UnboundedData,
)
from ray.data.context import DataContext
from ray.data.datasource.unbound_datasource import UnboundDatasource


def plan_unbounded_streaming_op(
    logical_op: UnboundedData,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Plan the UnboundedData logical operator.

    This function converts the logical streaming operator into a physical
    operator that can be executed by Ray Data's execution engine.

    Args:
        logical_op: The logical streaming operator
        physical_children: Should be empty for source operators
        data_context: Ray Data context

    Returns:
        Physical streaming operator
    """
    assert len(physical_children) == 0, "Streaming source operators have no children"

    ds = logical_op.datasource
    trigger = logical_op.trigger

    # Validate: only UnboundDatasource allowed.
    if not isinstance(ds, UnboundDatasource):
        raise ValueError(
            "Streaming triggers require an UnboundDatasource. "
            f"Got datasource type: {type(ds)}"
        )

    # Ensure trigger fields exist and are sane (Spark-like semantics).
    if trigger.trigger_type not in ("once", "available_now", "continuous", "fixed_interval"):
        raise ValueError(f"Unsupported trigger_type: {trigger.trigger_type}")

    # Extract ray_remote_args from data_context if available
    ray_remote_args = {}
    if hasattr(data_context, "scheduling_strategy"):
        ray_remote_args["scheduling_strategy"] = data_context.scheduling_strategy

    return UnboundedDataOperator(
        data_context=data_context,
        datasource=ds,
        trigger=trigger,
        parallelism=logical_op.parallelism,
        ray_remote_args=ray_remote_args,
    )
