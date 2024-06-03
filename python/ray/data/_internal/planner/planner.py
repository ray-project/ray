from typing import Callable, ClassVar, Dict, Generic, List, Tuple, Type, TypeVar

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.union_operator import UnionOperator
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    PhysicalPlan,
)
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll
from ray.data._internal.logical.operators.from_operators import AbstractFrom
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.logical.operators.n_ary_operator import Union, Zip
from ray.data._internal.logical.operators.one_to_one_operator import Limit
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.planner.plan_all_to_all_op import plan_all_to_all_op
from ray.data._internal.planner.plan_from_op import plan_from_op
from ray.data._internal.planner.plan_input_data_op import plan_input_data_op
from ray.data._internal.planner.plan_read_op import plan_read_op
from ray.data._internal.planner.plan_udf_map_op import plan_udf_map_op
from ray.data._internal.planner.plan_write_op import plan_write_op

LogicalOperatorType = TypeVar("LogicalOperatorType", bound=LogicalOperator)

CallbackFn = Callable[[LogicalOperatorType, List[PhysicalOperator]], PhysicalOperator]


class PlanLogicalOpFn(Generic[LogicalOperatorType]):
    def __init__(
        self, logical_op_type: Type[LogicalOperatorType], callback: CallbackFn
    ):
        self._logical_op_type = logical_op_type
        self._callback = callback

    def should_handle(self, logical_op: LogicalOperator):
        return isinstance(logical_op, self._logical_op_type)

    def __call__(
        self, logical_op: LogicalOperator, physical_children: List[PhysicalOperator]
    ) -> PhysicalOperator:
        return self._callback(logical_op, physical_children)


PLAN_LOGICAL_OP_FNS: List[PlanLogicalOpFn] = []


def init_plan_logical_op_fns():
    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(Read, plan_read_op))
    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(InputData, plan_input_data_op))
    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(Write, plan_write_op))
    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(AbstractFrom, plan_from_op))
    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(AbstractUDFMap, plan_udf_map_op))
    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(AbstractAllToAll, plan_all_to_all_op))

    def plan_zip_op(_, physical_children):
        assert len(physical_children) == 2
        return ZipOperator(physical_children[0], physical_children[1])

    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(Zip, plan_zip_op))

    def plan_union_op(_, physical_children):
        assert len(physical_children) >= 2
        return UnionOperator(*physical_children)

    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(Union, plan_union_op))

    def plan_limit_op(logical_op, physical_children):
        return LimitOperator(logical_op._limit, physical_children[0])

    PLAN_LOGICAL_OP_FNS.append(PlanLogicalOpFn(Limit, plan_limit_op))


init_plan_logical_op_fns()


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
        for plan_fn in PLAN_LOGICAL_OP_FNS:
            if plan_fn.should_handle(logical_op):
                physical_op = plan_fn(logical_op, physical_children)
                break

        if physical_op is None:
            raise ValueError(
                f"Found unknown logical operator during planning: {logical_op}"
            )

        self._physical_op_to_logical_op[physical_op] = logical_op
        return physical_op
