from typing import TYPE_CHECKING

from ray.data._internal.execution.operators.limit_operator import LimitOperator

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.logical.operators.one_to_one_operator import Limit


def plan_limit_op(
    op: "Limit", input_physical_dag: "PhysicalOperator"
) -> "PhysicalOperator":
    """Get the corresponding DAG of physical operators for Limit.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    return LimitOperator(op._limit, input_physical_dag)
