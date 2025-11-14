from typing import Dict, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
    SupportsPushThrough,
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


class Zip(NAry, SupportsPushThrough):
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

    def apply_projection(
        self,
        columns: List[str],
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        new_input_ops = []
        for input_op in self.input_dependencies:
            upstream_project = self._create_upstream_project(
                columns=columns,
                column_rename_map=column_rename_map,
                input_op=input_op,
            )
            new_input_ops.append(upstream_project)

        return Zip(*new_input_ops)


class Union(NAry, SupportsPushThrough, LogicalOperatorSupportsPredicatePassThrough):
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

    def apply_projection(
        self,
        columns: List[str],
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        new_input_ops = []
        for input_op in self.input_dependencies:
            upstream_project = self._create_upstream_project(
                columns=columns,
                column_rename_map=column_rename_map,
                input_op=input_op,
            )
            new_input_ops.append(upstream_project)

        return Union(*new_input_ops)

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Union allows pushing filter into each branch
        return PredicatePassThroughBehavior.PUSH_INTO_BRANCHES
