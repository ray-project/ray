from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Filter
from ray.data._internal.logical.operators.n_ary_operator import Union
from ray.data._internal.planner.plan_expression.expression_visitors import (
    _ColumnSubstitutionVisitor,
)
from ray.data.expressions import Expr, col


class PredicatePushdown(Rule):
    """Pushes down predicates across the graph.

    This rule performs the following optimizations:
    1. Combines chained Filter operators with compatible expressions
    2. Pushes filter expressions down to operators that support predicate pushdown,
       rebinding column references when necessary (e.g., after projections with renames)
    3. Pushes filters through Union operators into each branch
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
    def _rebind_predicate_columns(
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

        input_op = op.input_dependencies[0]

        # Special case: Push filter through Union into each branch
        # TODO: Push filter through other operators like Projection, Zip, Join, Sort, Aggregate (after expression support lands)
        if isinstance(input_op, Union):
            return cls._push_filter_through_union(op, input_op)

        # Check if the input operator supports predicate pushdown
        if (
            isinstance(input_op, LogicalOperatorSupportsPredicatePushdown)
            and input_op.supports_predicate_pushdown()
        ):
            predicate_expr = op._predicate_expr

            # Check if the operator has column renames that need rebinding
            # This happens when projection pushdown has been applied
            rename_map = input_op.get_column_renames()
            if rename_map:
                # Rebind the predicate to use original column names
                # This is needed to ensure that the predicate expression can be pushed into the input operator.
                predicate_expr = cls._rebind_predicate_columns(
                    predicate_expr, rename_map
                )

            # Push the predicate down and return the result without the filter
            return input_op.apply_predicate(predicate_expr)

        return op

    @classmethod
    def _push_filter_through_union(cls, filter_op: Filter, union_op: Union) -> Union:
        """Push a Filter through a Union into each branch.

        Transforms:
            branch₁ ─┐
            branch₂ ─┤ Union ─> Filter(predicate)
            branch₃ ─┘

        Into:
            branch₁ ─> Filter(predicate) ─┐
            branch₂ ─> Filter(predicate) ─┤ Union
            branch₃ ─> Filter(predicate) ─┘
        """
        predicate_expr = filter_op._predicate_expr

        # Apply filter to each branch of the union
        new_inputs = []
        for input_op in union_op.input_dependencies:
            # Create a filter for this branch and recursively try to push it down
            branch_filter = Filter(input_op, predicate_expr=predicate_expr)
            # Recursively apply pushdown to each branch's filter
            pushed_branch = cls._try_push_down_predicate(branch_filter)
            new_inputs.append(pushed_branch)

        # Return a new Union with filtered branches
        return Union(*new_inputs)
