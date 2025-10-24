from typing import Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePushdown,
)
from ray.data.expressions import Expr


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


class Zip(NAry):
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


class Union(NAry, LogicalOperatorSupportsPredicatePushdown):
    """Logical operator for union."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)
        self._predicate_expr: Optional[Expr] = None

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self._input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs += num_outputs
        return total_num_outputs

    def supports_predicate_pushdown(self) -> bool:
        """Union supports predicate pushdown by applying predicates to all branches."""
        return True

    def get_current_predicate(self) -> Optional[Expr]:
        """Returns the current predicate expression applied to this Union."""
        return self._predicate_expr

    def apply_predicate(self, predicate_expr: Expr) -> "Union":
        """Apply a predicate by pushing it down to all input branches.

        This creates a new Union with the predicate applied to each input operator
        that supports predicate pushdown.
        """
        import copy

        from ray.data._internal.logical.operators.map_operator import Filter

        clone = copy.copy(self)

        # Combine with existing predicate using AND
        if clone._predicate_expr is not None:
            clone._predicate_expr = clone._predicate_expr & predicate_expr
        else:
            clone._predicate_expr = predicate_expr

        # Apply predicate to each branch
        new_inputs = []
        for branch in self._input_dependencies:
            # If the branch supports predicate pushdown, use it
            if (
                isinstance(branch, LogicalOperatorSupportsPredicatePushdown)
                and branch.supports_predicate_pushdown()
            ):
                new_inputs.append(branch.apply_predicate(predicate_expr))
            else:
                # Otherwise, wrap with a Filter operator
                new_inputs.append(Filter(branch, predicate_expr=predicate_expr))

        clone._input_dependencies = new_inputs
        return clone
