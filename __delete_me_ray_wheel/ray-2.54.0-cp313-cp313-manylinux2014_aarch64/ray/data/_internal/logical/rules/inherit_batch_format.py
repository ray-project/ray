import copy

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators import AbstractAllToAll, MapBatches

__all__ = [
    "InheritBatchFormatRule",
]


class InheritBatchFormatRule(Rule):
    """For AbstractAllToAll based operator, apply this rule
    to inherit batch_format from upstream operator by traversing
    the entire DAG."""

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
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
                    new_op._output_dependencies = []
                    return new_op
                upstream_op = upstream_op.input_dependencies[0]

            return node

        return op._apply_transform(transform)
