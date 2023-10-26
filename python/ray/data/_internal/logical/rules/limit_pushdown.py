import copy
from collections import deque
from typing import Iterable, List

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.one_to_one_operator import (
    AbstractOneToOne,
    Limit,
)
from ray.data._internal.logical.operators.read_operator import Read


class LimitPushdownRule(Rule):
    """Rule for pushing down the limit operator.

    When a limit operator is present, we apply the limit on the
    most upstream operator that supports it. Notably, we move the
    Limit operator downstream from Read op, any other non-OneToOne operator,
    or any operator which could potentially change the number of output rows.

    In addition, we also fuse consecutive Limit operators into a single
    Limit operator, i.e. `Limit[n] -> Limit[m]` becomes `Limit[min(n, m)]`.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        optimized_dag = self._apply_limit_pushdown(plan.dag)
        optimized_dag = self._apply_limit_fusion(optimized_dag)
        return LogicalPlan(dag=optimized_dag)

    def _apply_limit_pushdown(self, op: LogicalOperator) -> LogicalOperator:
        """Given a DAG of LogicalOperators, traverse the DAG and push down
        Limit operators, i.e. move Limit operators as far upstream as possible.

        Returns a new LogicalOperator with the Limit operators pushed down."""
        # Post-order traversal.
        nodes: Iterable[LogicalOperator] = deque()
        for node in op.post_order_iter():
            nodes.appendleft(node)

        while len(nodes) > 0:
            current_op = nodes.pop()

            # If we encounter a Limit op, move it upstream until it reaches:
            # - Read operator
            # - A non-AbstractOneToOne operator (e.g. AbstractAllToAll)
            # - An AbstractOneToOne operator that could change the number of output rows

            # TODO(scottjlee): in our current abstraction, we have Read extend
            # AbstractMap (with no input dependency), which extends AbstractOneToOne.
            # So we have to explicitly separate the Read op in its own check.
            # We should remove this case once we refactor Read op to no longer
            # be an AbstractOneToOne op.
            if isinstance(current_op, Limit):
                limit_op_copy = copy.copy(current_op)

                # Traverse up the DAG until we reach the first operator that meets
                # one of the conditions above, which will serve as the new input
                # into the Limit operator.
                new_input_into_limit = current_op.input_dependency
                ops_between_new_input_and_limit: List[LogicalOperator] = []
                while (
                    isinstance(new_input_into_limit, AbstractOneToOne)
                    and not isinstance(new_input_into_limit, Read)
                    and not getattr(new_input_into_limit, "can_modify_num_rows", False)
                ):
                    new_input_into_limit_copy = copy.copy(new_input_into_limit)
                    ops_between_new_input_and_limit.append(new_input_into_limit_copy)
                    new_input_into_limit = new_input_into_limit.input_dependency

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
                    # Add the copied operator to the list of nodes to be traversed.
                    nodes.append(curr_op)

                # Link the Limit operator to its new input operator.
                for limit_output_op in current_op.output_dependencies:
                    limit_output_op._input_dependencies = [
                        ops_between_new_input_and_limit[0]
                    ]
                last_op = ops_between_new_input_and_limit[0]
                last_op._output_dependencies = current_op.output_dependencies

        return current_op

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
