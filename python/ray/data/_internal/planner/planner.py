from typing import Callable, Dict, List, Tuple, Type, TypeVar

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    PhysicalPlan,
)
from ray.util.annotations import DeveloperAPI

LogicalOperatorType = TypeVar("LogicalOperatorType", bound=LogicalOperator)

PlanLogicalOpFn = Callable[
    [LogicalOperatorType, List[PhysicalOperator]], PhysicalOperator
]

# A list of registered plan functions for logical operators.
PLAN_LOGICAL_OP_FNS: List[Tuple[Type[LogicalOperator], PlanLogicalOpFn]] = []


@DeveloperAPI
def register_plan_logical_op_fn(
    logical_op_type: Type[LogicalOperator],
    plan_fn: PlanLogicalOpFn,
):
    """Register a plan function for a logical operator type."""
    PLAN_LOGICAL_OP_FNS.append((logical_op_type, plan_fn))


def _register_default_plan_logical_op_fns():
    from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
    from ray.data._internal.execution.operators.limit_operator import LimitOperator
    from ray.data._internal.execution.operators.union_operator import UnionOperator
    from ray.data._internal.execution.operators.zip_operator import ZipOperator
    from ray.data._internal.logical.operators.all_to_all_operator import (
        AbstractAllToAll,
    )
    from ray.data._internal.logical.operators.from_operators import AbstractFrom
    from ray.data._internal.logical.operators.input_data_operator import InputData
    from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
    from ray.data._internal.logical.operators.n_ary_operator import Union, Zip
    from ray.data._internal.logical.operators.one_to_one_operator import Limit
    from ray.data._internal.logical.operators.read_operator import Read
    from ray.data._internal.logical.operators.write_operator import Write
    from ray.data._internal.planner.plan_all_to_all_op import plan_all_to_all_op
    from ray.data._internal.planner.plan_read_op import plan_read_op
    from ray.data._internal.planner.plan_udf_map_op import plan_udf_map_op
    from ray.data._internal.planner.plan_write_op import plan_write_op

    register_plan_logical_op_fn(Read, plan_read_op)

    def plan_input_data_op(
        logical_op: InputData, physical_children: List[PhysicalOperator]
    ) -> PhysicalOperator:
        """Get the corresponding DAG of physical operators for InputData."""
        assert len(physical_children) == 0

        return InputDataBuffer(
            input_data=logical_op.input_data,
            input_data_factory=logical_op.input_data_factory,
        )

    register_plan_logical_op_fn(InputData, plan_input_data_op)
    register_plan_logical_op_fn(Write, plan_write_op)

    def plan_from_op(
        op: AbstractFrom, physical_children: List[PhysicalOperator]
    ) -> PhysicalOperator:
        assert len(physical_children) == 0
        return InputDataBuffer(op.input_data)

    register_plan_logical_op_fn(AbstractFrom, plan_from_op)
    register_plan_logical_op_fn(AbstractUDFMap, plan_udf_map_op)
    register_plan_logical_op_fn(AbstractAllToAll, plan_all_to_all_op)

    def plan_zip_op(_, physical_children):
        assert len(physical_children) == 2
        return ZipOperator(physical_children[0], physical_children[1])

    register_plan_logical_op_fn(Zip, plan_zip_op)

    def plan_union_op(_, physical_children):
        assert len(physical_children) >= 2
        return UnionOperator(*physical_children)

    register_plan_logical_op_fn(Union, plan_union_op)

    def plan_limit_op(logical_op, physical_children):
        assert len(physical_children) == 1
        return LimitOperator(logical_op._limit, physical_children[0])

    register_plan_logical_op_fn(Limit, plan_limit_op)


_register_default_plan_logical_op_fns()


class Planner:
    """The planner to convert optimized logical to physical operators.

    Note that planner is only doing operators conversion. Physical optimization work is
    done by physical optimizer.
    """

    def __init__(self):
        self._physical_op_to_logical_op: Dict[PhysicalOperator, LogicalOperator] = {}

    def plan(self, logical_plan: LogicalPlan) -> PhysicalPlan:
        """Convert logical to physical operators recursively in post-order."""
        physical_dag = self._plan(logical_plan.dag)
        return PhysicalPlan(physical_dag, self._physical_op_to_logical_op)

    def _plan(self, logical_op: LogicalOperator) -> PhysicalOperator:
        # Plan the input dependencies first.
        physical_children = []
        for child in logical_op.input_dependencies:
            physical_children.append(self._plan(child))

        physical_op = None
        for op_type, plan_fn in PLAN_LOGICAL_OP_FNS:
            if isinstance(logical_op, op_type):
                physical_op = plan_fn(logical_op, physical_children)
                break

        if physical_op is None:
            raise ValueError(
                f"Found unknown logical operator during planning: {logical_op}"
            )

        self._physical_op_to_logical_op[physical_op] = logical_op
        return physical_op
