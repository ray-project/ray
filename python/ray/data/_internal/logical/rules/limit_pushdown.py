import copy
from collections import deque
from typing import Iterable, List

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll
from ray.data._internal.logical.operators.limit_operator import Limit
from ray.data._internal.logical.operators.read_operator import Read


class LimitPushdownRule(Rule):
    """Rule for pushing down the limit operator.

    When a limit operator is present, we apply the limit on the
    most upstream operator that supports it. Notably, we move the
    Limit operator downstream from Read, AbstractAllToAll, or any
    operator which could potentially change the number of output rows.
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

            # If we encounter a Limit op, move it upstream until it reaches:
            # - Read operator
            # - AbstractAllToAll operator
            # - Operator that could change the number of output rows
            if isinstance(current_op, Limit):
                limit_op_copy = copy.copy(current_op)

                # Find the first operator that meets one of the conditions above
                assert (
                    len(limit_op_copy.input_dependencies) == 1
                ), limit_op_copy.input_dependencies
                new_input_into_limit = current_op.input_dependencies[0]
                ops_between_new_input_and_limit: List[LogicalOperator] = []
                while (
                    not (
                        isinstance(new_input_into_limit, (Read, AbstractAllToAll))
                        or getattr(new_input_into_limit, "can_modify_num_rows", False)
                    )
                    and len(new_input_into_limit.input_dependencies) == 1
                ):
                    new_input_into_limit_copy = copy.copy(new_input_into_limit)
                    ops_between_new_input_and_limit.append(new_input_into_limit_copy)
                    new_input_into_limit = new_input_into_limit.input_dependencies[0]

                # Set the new input to the Limit operator.
                limit_op_copy._input_dependencies = [new_input_into_limit]

                # Build the new copy of operator dependencies
                # between the new input and the Limit operator.
                ops_between_new_input_and_limit.append(limit_op_copy)
                for idx in range(len(ops_between_new_input_and_limit) - 1):
                    ops_between_new_input_and_limit[idx]._input_dependencies = [
                        ops_between_new_input_and_limit[idx + 1]
                    ]
                    nodes.append(ops_between_new_input_and_limit[idx])

                # Set the final operator between the input and Limit op to
                # have the Limit op as its new input.
                for op_ in limit_op_copy.output_dependencies:
                    op_._input_dependencies = [ops_between_new_input_and_limit[0]]

        return current_op
