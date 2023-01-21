from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.read_operator import Read, plan_read_op
from ray.data._internal.logical.operators.map_operator import (
    MapBatches,
    plan_map_batches_op,
)


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
            physical_dag = plan_read_op(logical_dag)
        elif isinstance(logical_dag, MapBatches):
            assert len(physical_children) == 1
            physical_dag = plan_map_batches_op(logical_dag, physical_children[0])
        else:
            raise ValueError(
                f"Found unknown logical operator during planning: {logical_dag}"
            )
        return physical_dag
