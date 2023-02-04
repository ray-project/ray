from collections import deque

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomizeBlocks,
    Repartition,
)


class RandomizeBlockOrderRule(Rule):
    """Rule for handling RandomizeBlockOrder Logical Operator.

    1. Dedupes multiple RandomizeBlocks operators.
    2. Moves RandomizeBlocks operator to the end of the operator DAG.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        optimized_dag: LogicalOperator = self._apply(plan.dag)
        return LogicalPlan(dag=optimized_dag)

    def _apply(self, op: LogicalOperator) -> LogicalOperator:
        should_add_randomize_block_operator = False
        seed = op._seed if isinstance(op, RandomizeBlocks) else None

        # Post-order traversal.
        nodes = deque()
        for node in op:
            nodes.appendleft(node)

        while len(nodes) > 0:
            current_op = nodes.pop()
            upstream_ops = current_op.input_dependencies

            # Iterate through all upstream ops, and collapse all RandomizeBlocks
            # operators.
            for i in range(len(upstream_ops)):
                if isinstance(upstream_ops[i], RandomizeBlocks):
                    if seed is not None and upstream_ops[i]._seed != seed:
                        raise RuntimeError(
                            "Cannot create execution plan for the provided Dataset: "
                            "`randomize_block_order` has been called multiple times "
                            "with different seeds."
                        )
                    should_add_randomize_block_operator = True
                    seed = upstream_ops[i]._seed
                    # Remove RandomizeBlocks operator from the dag and wire in new input
                    # dependencies.
                    assert len(upstream_ops[i].input_dependencies) == 1
                    upstream_ops[i] = upstream_ops[i].input_dependencies[0]
            if isinstance(current_op, Repartition):
                # If this operator is a Repartition, insert RandomizeBlocks right
                # before this operator rather than the end.
                new_op = RandomizeBlocks(input_op=upstream_ops[0], seed=seed)
                upstream_ops[0] = new_op
                should_add_randomize_block_operator = False
                seed = None

        # Add RandomizeBlocks operator as the last operator in the DAG if necessary.
        if should_add_randomize_block_operator:
            op = RandomizeBlocks(input_op=op, seed=seed)

        return op
