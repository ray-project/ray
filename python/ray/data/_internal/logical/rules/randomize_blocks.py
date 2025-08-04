import copy
from collections import deque

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import (
    AbstractAllToAll,
    RandomizeBlocks,
)
from ray.data._internal.logical.operators.streaming_split_operator import StreamingSplit
from ray.data._internal.logical.operators.write_operator import Write


class ReorderRandomizeBlocksRule(Rule):
    """Rule for reordering RandomizeBlocks logical operator.

    Reordering RandomizeBlocks operators is to help fuse multiple
    AbstractUDFMap operators together for better performance.

    1. Dedupes multiple RandomizeBlocks operators if they are not seeded.
    2. Moves RandomizeBlocks operator to the end of a sequence of AbstractUDFMap
    operators. RandomizeBlocks operators are not moved across boundary operators.

    A boundary operator is one that RandomizeBlocks is not allowed to be pushed
    past to retain an equivalent logical plan.
    This includes AllToAll, Write, and StreamingSplit operators.

    Example 1: RandomizeBlocks is not allowed to be pushed past Write/StreamingSplit.

    Input: Read -> Map1 -> RandomizeBlocks -> Map2 -> Write/StreamingSplit
    Optimized: (Read->Map1->Map2) -> RandomizeBlocks -> Write/StreamingSplit

    Example 2: RandomizeBlocks is not allowed to be pushed past Sort (AllToAll).

    Input: Read -> Map1 -> RandomizeBlocks -> Sort -> Map2 -> Write
    Optimized: (Read->Map1) -> RandomizeBlocks -> Sort -> Map2 -> Write
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        optimized_dag: LogicalOperator = self._apply(plan.dag)
        new_plan = LogicalPlan(dag=optimized_dag, context=plan.context)
        return new_plan

    @staticmethod
    def _is_boundary_operator(op: LogicalOperator) -> bool:
        """Returns True if the operator is a boundary operator."""
        # Only consider AllToAll operators that are not RandomizeBlocks,
        # since we will already deduplicate RandomizeBlocks operators.
        is_all_to_all = isinstance(op, AbstractAllToAll) and not isinstance(
            op, RandomizeBlocks
        )
        is_write = isinstance(op, Write)
        is_streaming_split = isinstance(op, StreamingSplit)
        return is_all_to_all or is_write or is_streaming_split

    def _apply(self, op: LogicalOperator) -> LogicalOperator:
        operators = []

        # Post-order traversal.
        nodes = deque()
        for node in op.post_order_iter():
            nodes.appendleft(node)

        while len(nodes) > 0:
            current_op = nodes.pop()
            upstream_ops = current_op.input_dependencies

            # Iterate through all upstream ops, and remove all RandomizeBlocks
            # operators.
            for i in range(len(upstream_ops)):
                if isinstance(upstream_ops[i], RandomizeBlocks):
                    # If no seeds are provided, then collapse into a single
                    # RandomizeBlocks operator.
                    current_seed = upstream_ops[i]._seed
                    if not operators or current_seed or operators[-1]._seed:
                        # We need to make a copy of the operator.
                        # Because the operator instance may be shared by multiple
                        # Datasets. We shouldn't modify it in place.
                        operators.append(copy.copy(upstream_ops[i]))

                    # Remove RandomizeBlocks operator from the dag and wire in new input
                    # dependencies.
                    assert len(upstream_ops[i].input_dependencies) == 1
                    upstream_ops[i] = upstream_ops[i].input_dependencies[0]

            if self._is_boundary_operator(current_op):
                # If this operator is a boundary operator, then insert RandomizeBlocks
                # right before this operator rather than the end of the DAG.
                # Boundary operators can have only 1 input operator.
                assert len(upstream_ops) == 1
                input_op = upstream_ops[0]
                for random_op in operators:
                    random_op._input_dependencies = [input_op]
                    input_op = random_op
                upstream_ops[0] = input_op
                operators = []

        # Add RandomizeBlocks operator as the last operator in the DAG if necessary.
        for random_op in operators:
            random_op._input_dependencies = [op]
            op = random_op

        return op
