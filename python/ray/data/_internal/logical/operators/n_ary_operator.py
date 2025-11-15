from typing import Dict, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsProjectionPassThrough,
    PredicatePassThroughBehavior,
)


class NAry(LogicalOperator):
    """Base class for n-ary operators, which take multiple input operators."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
        num_outputs: Optional[int] = None,
    ):
        """
        Args:
            input_ops: The input operators.
        """
        super().__init__(self.__class__.__name__, list(input_ops), num_outputs)


class Zip(NAry, LogicalOperatorSupportsProjectionPassThrough):
    """Logical operator for zip."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self._input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs = max(total_num_outputs, num_outputs)
        return total_num_outputs

    def apply_projection_pass_through(
        self,
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        new_input_ops = []
        for input_op in self.input_dependencies:
            # Get the schema of this input to determine which columns it contributes
            schema = input_op.infer_schema()
            if schema is None:
                # cannot pass projections if schema is unknown.
                new_input_ops.append(input_op)
                continue

            # Calling .names works for both Pandas + Pyarrow schema
            input_schema_cols = set(input_op.infer_schema().names)

            # Only push down columns that exist in this input
            columns_to_pass_through = [
                old_col for old_col in column_rename_map if old_col in input_schema_cols
            ]

            if columns_to_pass_through:
                # There exists columns referenced in the input_op that can
                # be pass to, so we create an upstream project operator.
                upstream_project = self._create_upstream_project(
                    columns=columns_to_pass_through,
                    column_rename_map=column_rename_map,
                    input_op=input_op,
                )
                new_input_ops.append(upstream_project)
            else:
                # There are no columns that this input_op references, projection
                # cannot be done.
                new_input_ops.append(input_op)

        return Zip(*new_input_ops)


class Union(
    NAry,
    LogicalOperatorSupportsProjectionPassThrough,
    LogicalOperatorSupportsPredicatePassThrough,
):
    """Logical operator for union."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self._input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs += num_outputs
        return total_num_outputs

    def apply_projection_pass_through(
        self,
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        new_input_ops = []
        for input_op in self.input_dependencies:
            upstream_project = self._create_upstream_project(
                columns_to_rename=list(column_rename_map.keys()),
                column_rename_map=column_rename_map,
                input_op=input_op,
            )
            new_input_ops.append(upstream_project)

        return Union(*new_input_ops)

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Union allows pushing filter into each branch
        return PredicatePassThroughBehavior.PUSH_INTO_BRANCHES
