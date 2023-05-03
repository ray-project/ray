from typing import Dict

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    PhysicalPlan,
)
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll
from ray.data._internal.logical.operators.n_ary_operator import Zip
from ray.data._internal.logical.operators.from_arrow_operator import FromArrowRefs
from ray.data._internal.logical.operators.from_items_operator import FromItems
from ray.data._internal.logical.operators.from_numpy_operator import FromNumpyRefs
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.planner.plan_all_to_all_op import _plan_all_to_all_op
from ray.data._internal.planner.plan_from_arrow_op import _plan_from_arrow_refs_op
from ray.data._internal.planner.plan_from_items_op import _plan_from_items_op
from ray.data._internal.planner.plan_from_numpy_op import _plan_from_numpy_refs_op
from ray.data._internal.planner.plan_from_pandas_op import (
    FromPandasRefsOperators,
    _plan_from_pandas_refs_op,
)
from ray.data._internal.planner.plan_udf_map_op import _plan_udf_map_op
from ray.data._internal.planner.plan_read_op import _plan_read_op
from ray.data._internal.planner.plan_write_op import _plan_write_op


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

        if isinstance(logical_op, Read):
            assert not physical_children
            physical_op = _plan_read_op(logical_op)
        elif isinstance(logical_op, Write):
            assert len(physical_children) == 1
            physical_op = _plan_write_op(logical_op, physical_children[0])
        elif isinstance(logical_op, FromItems):
            assert not physical_children
            physical_op = _plan_from_items_op(logical_op)
        # Use __args__ because isinstance() check doesn't work with
        # subscripted generics. https://stackoverflow.com/a/45959000
        elif isinstance(logical_op, FromPandasRefsOperators.__args__):
            assert not physical_children
            physical_op = _plan_from_pandas_refs_op(logical_op)
        elif isinstance(logical_op, FromNumpyRefs):
            assert not physical_children
            physical_op = _plan_from_numpy_refs_op(logical_op)
        elif isinstance(logical_op, FromArrowRefs):
            assert not physical_children
            physical_op = _plan_from_arrow_refs_op(logical_op)
        elif isinstance(logical_op, AbstractUDFMap):
            assert len(physical_children) == 1
            physical_op = _plan_udf_map_op(logical_op, physical_children[0])
        elif isinstance(logical_op, AbstractAllToAll):
            assert len(physical_children) == 1
            physical_op = _plan_all_to_all_op(logical_op, physical_children[0])
        elif isinstance(logical_op, Zip):
            assert len(physical_children) == 2
            physical_op = ZipOperator(physical_children[0], physical_children[1])
        else:
            raise ValueError(
                f"Found unknown logical operator during planning: {logical_op}"
            )
        self._physical_op_to_logical_op[physical_op] = logical_op
        return physical_op
