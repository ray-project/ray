from collections import deque
from typing import Iterable

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import Aggregate, Sort


class InheritBatchFormatRule(Rule):
    """For Sort and Aggregate logicla operator, apply this rule
    to inherit batch_format from upstream operator by traversing
    the entire DAG."""

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        optimized_dag: LogicalOperator = self._apply(plan.dag)
        new_plan = LogicalPlan(dag=optimized_dag, context=plan.context)
        return new_plan

    def _apply(self, op: LogicalOperator):
        # Post-order traversal.
        nodes: Iterable[LogicalOperator] = deque()
        for node in op.post_order_iter():
            nodes.appendleft(node)

        while len(nodes) > 0:
            current_op = nodes.pop()

            if isinstance(current_op, (Sort, Aggregate)):
                # traversal up the DAG until we find first operator with batch_format
                # or we reach to source op and do nothing
                upstream_op = current_op.input_dependencies[0]
                while (
                    upstream_op.input_dependencies
                    and getattr(upstream_op, "_batch_format", None) is None
                ):
                    upstream_op = upstream_op.input_dependencies[0]
                if getattr(upstream_op, "_batch_format", None):
                    current_op._batch_format = upstream_op._batch_format

        # just return the default op
        return op
