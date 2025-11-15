import copy
from typing import List

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsPredicatePushdown,
    LogicalPlan,
    PredicatePassThroughBehavior,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Filter
from ray.data._internal.planner.plan_expression.expression_visitors import (
    _ColumnSubstitutionVisitor,
)
from ray.data.expressions import Expr, col


class PredicatePushdown(Rule):
    """Pushes down predicates across the graph.

    This rule performs the following optimizations:
    1. Combines chained Filter operators with compatible expressions
    2. Pushes filter expressions through eligible operators using trait-based rules
    3. Pushes filters into data sources that support predicate pushdown

    Eligibility is determined by the LogicalOperatorSupportsPredicatePassThrough trait, which operators
    implement to declare their pushdown behavior:
    - PASSTHROUGH: Filter passes through unchanged (Sort, Repartition, Shuffle, Limit)
    - PASSTHROUGH_WITH_SUBSTITUTION: Filter passes through with column rebinding (Project)
    - PUSH_INTO_BRANCHES: Filter is pushed into each branch (Union)
    - CONDITIONAL: Filter may be pushed based on analysis (Join - analyzes which side
      the predicate references and pushes to that side if safe for the join type)
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply predicate pushdown optimization to the logical plan."""
        dag = plan.dag
        new_dag = dag._apply_transform(self._try_fuse_filters)
        new_dag = new_dag._apply_transform(self._try_push_down_predicate)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _is_valid_filter_operator(cls, op: LogicalOperator) -> bool:
        return isinstance(op, Filter) and op.is_expression_based()

    @classmethod
    def _try_fuse_filters(cls, op: LogicalOperator) -> LogicalOperator:
        """Fuse consecutive Filter operators with compatible expressions."""
        if not cls._is_valid_filter_operator(op):
            return op

        input_op = op.input_dependencies[0]
        if not cls._is_valid_filter_operator(input_op):
            return op

        # Combine predicates
        combined_predicate = op._predicate_expr & input_op._predicate_expr

        # Create new filter on the input of the lower filter
        return Filter(
            input_op.input_dependencies[0],
            predicate_expr=combined_predicate,
        )

    @classmethod
    def _substitute_predicate_columns(
        cls, predicate_expr: Expr, column_rename_map: dict[str, str]
    ) -> Expr:
        """Rebind column references in a predicate expression.

        When pushing a predicate through a projection with column renames,
        we need to rewrite column references from new names to old names.

        Args:
            predicate_expr: The predicate with new column names
            column_rename_map: Mapping from old_name -> new_name

        Returns:
            The predicate rewritten to use old column names
        """
        # Invert the mapping: new_name -> old_name (as col expression)
        # This is because the predicate uses new names and we need to map
        # them back to old names
        column_mapping = {
            new_col: col(old_col) for old_col, new_col in column_rename_map.items()
        }

        visitor = _ColumnSubstitutionVisitor(column_mapping)
        return visitor.visit(predicate_expr)

    @classmethod
    def _try_push_down_predicate(cls, op: LogicalOperator) -> LogicalOperator:
        """Push Filter down through the operator tree."""
        if not cls._is_valid_filter_operator(op):
            return op
        filter_op: Filter = op
        input_op = filter_op.input_dependencies[0]
        predicate_expr = filter_op._predicate_expr

        # Case 1: Check if operator supports predicate pushdown (e.g., Read)
        if (
            isinstance(input_op, LogicalOperatorSupportsPredicatePushdown)
            and input_op.supports_predicate_pushdown()
        ):
            # Check if the operator has column renames that need rebinding
            # This happens when projection pushdown has been applied
            rename_map = input_op.get_column_renames()
            if rename_map:
                # Substitute the predicate to use original column names
                # This is needed to ensure that the predicate expression can be pushed into the input operator.
                predicate_expr = cls._substitute_predicate_columns(
                    predicate_expr, rename_map
                )

            # Push the predicate down and return the result without the filter
            return input_op.apply_predicate(predicate_expr)

        # Case 2: Check if operator allows predicates to pass through
        if isinstance(input_op, LogicalOperatorSupportsPredicatePassThrough):
            behavior = input_op.predicate_passthrough_behavior()

            if behavior in (
                PredicatePassThroughBehavior.PASSTHROUGH,
                PredicatePassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION,
            ):
                # Both cases push through a single input with optional column rebinding
                assert len(input_op.input_dependencies) == 1, (
                    f"{behavior.value} operators must have exactly 1 input, "
                    f"got {len(input_op.input_dependencies)}"
                )

                # Apply column substitution if needed
                if (
                    behavior
                    == PredicatePassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION
                ):
                    rename_map = input_op.get_column_substitutions()
                    if rename_map:
                        predicate_expr = cls._substitute_predicate_columns(
                            predicate_expr, rename_map
                        )

                # Push filter through and recursively try to push further
                new_filter = Filter(
                    input_op.input_dependencies[0],
                    predicate_expr=predicate_expr,
                )
                pushed_filter = cls._try_push_down_predicate(new_filter)

                # Return input_op with the pushed filter as its input
                return cls._clone_op_with_new_inputs(input_op, [pushed_filter])

            elif behavior == PredicatePassThroughBehavior.PUSH_INTO_BRANCHES:
                # Push into each branch (e.g., Union)
                # Apply filter to each branch and recursively push down
                new_inputs = []
                for branch_op in input_op.input_dependencies:
                    branch_filter = Filter(branch_op, predicate_expr=predicate_expr)
                    pushed_branch = cls._try_push_down_predicate(branch_filter)
                    new_inputs.append(pushed_branch)

                # Return operator with filtered branches
                return cls._clone_op_with_new_inputs(input_op, new_inputs)

            elif behavior == PredicatePassThroughBehavior.CONDITIONAL:
                # Handle conditional pushdown (e.g., Join)
                return cls._push_filter_through_conditionally(filter_op, input_op)

        return filter_op

    @classmethod
    def _push_filter_through_conditionally(
        cls, filter_op: Filter, conditional_op: LogicalOperator
    ) -> LogicalOperator:
        """Handle conditional pushdown for operators like Join.

        For operators with multiple inputs, we can push predicates that reference
        only one side down to that side, when semantically safe.
        """
        # Check if operator supports conditional pushdown by having the required method
        if not hasattr(conditional_op, "which_side_to_push_predicate"):
            return filter_op

        push_side = conditional_op.which_side_to_push_predicate(
            filter_op._predicate_expr
        )

        if push_side is None:
            # Cannot push through
            return filter_op

        # Use the enum value directly as branch index
        branch_idx = push_side.value

        # Push to the appropriate branch
        new_inputs = list(conditional_op.input_dependencies)
        branch_filter = Filter(
            new_inputs[branch_idx],
            predicate_expr=filter_op._predicate_expr,
        )
        new_inputs[branch_idx] = cls._try_push_down_predicate(branch_filter)

        # Return operator with updated input
        return cls._clone_op_with_new_inputs(conditional_op, new_inputs)

    @classmethod
    def _clone_op_with_new_inputs(
        cls, op: LogicalOperator, new_inputs: List[LogicalOperator]
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
