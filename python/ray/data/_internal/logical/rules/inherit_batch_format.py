import copy

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators import AbstractAllToAll, MapBatches
from ray.data.context import DataContext

__all__ = [
    "InheritBatchFormatRule",
]


class InheritBatchFormatRule(Rule):
    """For AbstractAllToAll based operator, apply this rule
    to inherit batch_format from upstream operator by traversing
    the entire DAG."""

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        if (
            DataContext.get_current().batch_to_block_arrow_format
        ):  # Disable the InheritBatchFormatRule if batch_to_block_arrow_format is True to prevent arrow to pandas round trip list/ndarray conversion errors
            return plan

        optimized_dag: LogicalOperator = self._apply(plan.dag)
        new_plan = LogicalPlan(dag=optimized_dag, context=plan.context)
        return new_plan

    def _apply(self, op: LogicalOperator):
        def transform(node: LogicalOperator) -> LogicalOperator:
            if not isinstance(node, AbstractAllToAll):
                return node

            # traversal up the DAG until we find MapBatches with batch_format
            # or we reach to source op and do nothing
            upstream_op = node.input_dependencies[0]
            while upstream_op.input_dependencies:
                if isinstance(upstream_op, MapBatches) and upstream_op.batch_format:
                    new_op = copy.copy(node)
                    new_op.batch_format = upstream_op.batch_format
                    return new_op
                upstream_op = upstream_op.input_dependencies[0]

            return node

        return op._apply_transform(transform)
