from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Filter


class PredicatePushdown(Rule):
    """Pushes down predicates across the graph.

    This rule performs the following optimizations:
    1. Combines chained Filter operators with compatible expressions
    2. Pushes filter expressions down to operators that support predicate pushdown
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply predicate pushdown optimization to the logical plan."""
        dag = plan.dag
        while True:
            new_dag = dag._apply_transform(self._try_fuse_filters)
            new_dag = new_dag._apply_transform(self._try_push_down_predicate)
            if new_dag is dag:
                break
            dag = new_dag
        return LogicalPlan(dag, plan.context)

    @classmethod
    def _try_fuse_filters(cls, op: LogicalOperator) -> LogicalOperator:
        """Fuse consecutive Filter operators with compatible expressions."""
        if not isinstance(op, Filter) or not op.is_expression_based():
            return op

        input_op = op.input_dependencies[0]
        if not isinstance(input_op, Filter) or not input_op.is_expression_based():
            return op

        # Combine predicates
        combined_predicate = op._predicate_expr & input_op._predicate_expr

        # Create new filter on the input of the lower filter
        return Filter(
            input_op.input_dependencies[0],
            predicate_expr=combined_predicate,
        )

    @classmethod
    def _try_push_down_predicate(cls, op: LogicalOperator) -> LogicalOperator:
        """Push Filter down to any operator that supports predicate pushdown."""
        if not isinstance(op, Filter) or not op.is_expression_based():
            return op

        input_op = op.input_dependencies[0]

        # Check if the input operator supports predicate pushdown
        if (
            isinstance(input_op, LogicalOperatorSupportsPredicatePushdown)
            and input_op.supports_predicate_pushdown()
        ):
            # Push the predicate down and return the result without the filter
            return input_op.apply_predicate(op._predicate_expr)

        return op
