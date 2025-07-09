import copy
from collections import deque
from typing import Iterable, List

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.operators.one_to_one_operator import (
    Limit,
)


class LimitPushdownRule(Rule):
    """Rule for pushing down the limit operator.

    When a limit operator is present, we apply the limit on the
    most upstream operator that supports it. We are conservative and only
    push through operators that we know for certain do not modify row counts:
    - Project operations (column selection)

    We stop at:
    - Any operator that can modify the number of output rows (Sort, Shuffle, Aggregate, Read etc.)
    - MapRows and MapBatches operations (to be conservative)
    - Union operations (limits should not be pushed through unions as it changes semantics)

    In addition, we also fuse consecutive Limit operators into a single
    Limit operator, i.e. `Limit[n] -> Limit[m]` becomes `Limit[min(n, m)]`.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        optimized_dag = self._apply_limit_pushdown(plan.dag)
        optimized_dag = self._apply_limit_fusion(optimized_dag)
        return LogicalPlan(dag=optimized_dag, context=plan.context)

    def _apply_limit_pushdown(self, op: LogicalOperator) -> LogicalOperator:
        """Given a DAG of LogicalOperators, traverse the DAG and push down
        Limit operators conservatively.

        Returns a new LogicalOperator with the Limit operators pushed down."""
        # Post-order traversal.
        nodes: Iterable[LogicalOperator] = deque()
        for node in op.post_order_iter():
            nodes.appendleft(node)

        current_dag_root = op
        while len(nodes) > 0:
            current_op = nodes.pop()

            if isinstance(current_op, Limit):
                new_op = self._push_limit_down(current_op)
                if new_op != current_op:
                    nodes.append(new_op)
                    # If this is the DAG root, update it
                    if current_op == current_dag_root:
                        current_dag_root = new_op
                    # Continue processing instead of returning early

        return current_dag_root

    def _push_limit_down(self, limit_op: Limit) -> LogicalOperator:
        """Push a single limit down through compatible operators conservatively.

        Only pushes through Project operations.
        """
        limit_op_copy = copy.copy(limit_op)

        # Traverse up the DAG until we reach the first operator that meets
        # one of the stopping conditions
        new_input_into_limit = limit_op.input_dependency
        ops_between_new_input_and_limit: List[LogicalOperator] = []

        while isinstance(new_input_into_limit, Project):
            new_input_into_limit_copy = copy.copy(new_input_into_limit)
            ops_between_new_input_and_limit.append(new_input_into_limit_copy)
            new_input_into_limit = new_input_into_limit.input_dependency

        # If we couldn't push through any operators, return original
        if not ops_between_new_input_and_limit:
            return limit_op

        # Link the Limit operator and its newly designated input op from above.
        limit_op_copy._input_dependencies = [new_input_into_limit]
        new_input_into_limit._output_dependencies = [limit_op_copy]

        # Build the chain of operator dependencies between the new
        # input and the Limit operator, using copies of traversed operators.
        ops_between_new_input_and_limit.append(limit_op_copy)
        for idx in range(len(ops_between_new_input_and_limit) - 1):
            curr_op, up_op = (
                ops_between_new_input_and_limit[idx],
                ops_between_new_input_and_limit[idx + 1],
            )
            curr_op._input_dependencies = [up_op]
            up_op._output_dependencies = [curr_op]

        # Link the first operator in the chain to the limit's original output
        for limit_output_op in limit_op.output_dependencies:
            limit_output_op._input_dependencies = [ops_between_new_input_and_limit[0]]
        last_op = ops_between_new_input_and_limit[0]
        last_op._output_dependencies = limit_op.output_dependencies

        return last_op

    def _apply_limit_fusion(self, op: LogicalOperator) -> LogicalOperator:
        """Given a DAG of LogicalOperators, traverse the DAG and fuse all
        back-to-back Limit operators, i.e.
        Limit[n] -> Limit[m] becomes Limit[min(n, m)].

        Returns a new LogicalOperator with the Limit operators fusion applied."""

        # Post-order traversal.
        nodes: Iterable[LogicalOperator] = deque()
        for node in op.post_order_iter():
            nodes.appendleft(node)

        while len(nodes) > 0:
            current_op = nodes.pop()

            # If we encounter two back-to-back Limit operators, fuse them.
            if isinstance(current_op, Limit):
                upstream_op = current_op.input_dependency
                if isinstance(upstream_op, Limit):
                    new_limit = min(current_op._limit, upstream_op._limit)
                    fused_limit_op = Limit(upstream_op.input_dependency, new_limit)

                    # Link the fused Limit operator to its input and output ops, i.e.:
                    # `upstream_input -> limit_upstream -> limit_downstream -> downstream_output`  # noqa: E501
                    # becomes `upstream_input -> fused_limit -> downstream_output`
                    fused_limit_op._input_dependencies = upstream_op.input_dependencies
                    fused_limit_op._output_dependencies = current_op.output_dependencies

                    # Replace occurrences of the upstream Limit operator in
                    # output_dependencies with the newly fused Limit operator.
                    upstream_input = upstream_op.input_dependency
                    upstream_input._output_dependencies = [fused_limit_op]

                    for current_output in current_op.output_dependencies:
                        current_output._input_dependencies = [fused_limit_op]
                    nodes.append(fused_limit_op)
        return current_op
