import copy
from typing import List

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import StreamingRepartition
from ray.data._internal.logical.operators.n_ary_operator import Zip


class AddStreamingRepartitionWhenZip(Rule):
    """Insert StreamingRepartition before each Zip input so blocks align."""

    TARGET_NUM_ROWS_PER_BLOCK = 128

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        def transform(node: LogicalOperator) -> LogicalOperator:
            if not isinstance(node, Zip):
                return node

            new_inputs: List[LogicalOperator] = []
            changed = False
            for child in node.input_dependencies:
                if isinstance(child, StreamingRepartition):
                    # Replace existing StreamingRepartition with new one using our target
                    if (
                        child.target_num_rows_per_block
                        != self.TARGET_NUM_ROWS_PER_BLOCK
                    ):
                        # Get the input operator of the existing StreamingRepartition
                        input_op = child.input_dependencies[0]
                        new_repartition = StreamingRepartition(
                            input_op,
                            self.TARGET_NUM_ROWS_PER_BLOCK,
                        )
                        new_inputs.append(new_repartition)
                        changed = True
                    else:
                        new_inputs.append(child)
                    continue

                streaming_repartition = StreamingRepartition(
                    child,
                    self.TARGET_NUM_ROWS_PER_BLOCK,
                )
                new_inputs.append(streaming_repartition)
                changed = True

            if not changed:
                return node

            return self._clone_op_with_new_inputs(node, new_inputs)

        new_dag = plan.dag._apply_transform(transform)
        if new_dag is plan.dag:
            return plan
        return LogicalPlan(new_dag, plan.context)

    @staticmethod
    def _clone_op_with_new_inputs(
        op: LogicalOperator, new_inputs: List[LogicalOperator]
    ) -> LogicalOperator:
        """Clone an operator with new inputs.

        Args:
            op: The operator to clone
            new_inputs: List of new input operators (can be single element list)

        Returns:
            A shallow copy of the operator with updated input dependencies
        """
        new_op = copy.copy(op)
        new_op._input_dependencies = new_inputs
        # Clear and re-wire dependencies for the new operator.
        # The output dependencies will be wired by the parent transform's traversal.
        new_op._output_dependencies = []
        new_op._wire_output_deps(new_inputs)
        return new_op
