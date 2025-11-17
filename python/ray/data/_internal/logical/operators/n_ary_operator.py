from typing import List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsProjectionPassThrough,
    PredicatePassThroughBehavior,
    ProjectionPassThroughBehavior,
)
from ray.data._internal.logical.operators.map_operator import Project


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

    def get_referenced_keys(self) -> Optional[List[List[str]]]:
        """Return empty lists for each input (Zip has no keys)."""
        return [[] for _ in self.input_dependencies]

    def apply_projection_pass_through(
        self,
        renamed_keys: Optional[List[List[str]]],
        upstream_projects: List["Project"],
    ) -> LogicalOperator:
        """Recreate Zip with upstream projects.

        Args:
            renamed_keys: Not used for Zip (no keys to rename)
            upstream_projects: List of projects, one per input
        """
        return Zip(*upstream_projects)

    def projection_passthrough_behavior(self) -> ProjectionPassThroughBehavior:
        return ProjectionPassThroughBehavior.PASSTHROUGH_WITH_CONDITIONAL


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
        renamed_keys: Optional[List[List[str]]],
        upstream_projects: List["Project"],
    ) -> LogicalOperator:
        """Recreate Union with upstream projects for all branches."""
        return Union(*upstream_projects)

    def projection_passthrough_behavior(self) -> ProjectionPassThroughBehavior:
        return ProjectionPassThroughBehavior.PASSTHROUGH_INTO_BRANCHES

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Union allows pushing filter into each branch
        return PredicatePassThroughBehavior.PUSH_INTO_BRANCHES
