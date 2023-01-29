from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data._internal.planner.plan_all_to_all_op import _plan_all_to_all_op
from ray.data._internal.planner.plan_map_op import _plan_map_op
from ray.data._internal.planner.plan_read_op import _plan_read_op


class Planner:
    """The planner to convert optimized logical to physical operators.

    Note that planner is only doing operators conversion. Physical optimization work is
    done by physical optimizer.
    """

    def plan(self, logical_dag: LogicalOperator) -> PhysicalOperator:
        """Convert logical to physical operators recursively in post-order."""
        # Plan the input dependencies first.
        physical_children = []
        for child in logical_dag.input_dependencies:
            physical_children.append(self.plan(child))

        if isinstance(logical_dag, Read):
            assert not physical_children
            physical_dag = _plan_read_op(logical_dag)
        elif isinstance(logical_dag, AbstractMap):
            assert len(physical_children) == 1
            physical_dag = _plan_map_op(logical_dag, physical_children[0])
        elif isinstance(logical_dag, AbstractAllToAll):
            assert len(physical_children) == 1
            physical_dag = _plan_all_to_all_op(logical_dag, physical_children[0])
        else:
            raise ValueError(
                f"Found unknown logical operator during planning: {logical_dag}"
            )
        return physical_dag
