from collections import deque
from typing import Iterable

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll
from ray.data._internal.logical.operators.limit_operator import Limit
from ray.data._internal.logical.operators.read_operator import Read


class LimitPushdownRule(Rule):
    """Rule for pushing down the limit operator.

    When a limit operator is present, we apply the limit on the
    most upstream operator that supports it. Notably, this means that
    the Sort operator blocks the limit from being pushed down.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        optimized_dag: LogicalOperator = self._apply(plan.dag)
        return LogicalPlan(dag=optimized_dag)

    def _apply(self, op: LogicalOperator) -> LogicalOperator:
        # Post-order traversal.
        nodes: Iterable[LogicalOperator] = deque()
        for node in op.post_order_iter():
            nodes.appendleft(node)

        while len(nodes) > 0:
            current_op = nodes.pop()
            upstream_ops = current_op.input_dependencies

            # If we encounter a Limit op, move it downstream from its first upstream
            # AllToAllOperator (which requires all blocks to be materialized).
            if isinstance(current_op, Limit):
                has_upstream_all_to_all = any(
                    isinstance(upstream_op, (AbstractAllToAll, Read))
                    for upstream_op in upstream_ops
                )
                if not has_upstream_all_to_all:
                    assert len(upstream_ops) == 1
                    for i in range(len(upstream_ops)):
                        upstream_op = current_op.input_dependencies[i]
                        upstream_op_inputs = upstream_op.input_dependencies
                        upstream_op._input_dependencies = [current_op]
                        current_op._input_dependencies = upstream_op_inputs

                        # do we need to update output dependencies too?
                        # current_op._output_dependencies = [upstream_op]
                        # for upstream_input in upstream_op_inputs:
                        #     upstream_input._output_dependencies = [current_op]
                        nodes.append(upstream_op)
        return current_op
