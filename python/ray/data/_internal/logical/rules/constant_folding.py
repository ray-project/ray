"""Constant folding optimization rule for logical plans.

This rule applies constant folding to all operators in a logical plan
that contain expressions (Project, Filter, etc.).
"""

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators import Filter, Project
from ray.data._internal.planner.plan_expression.constant_folder import (
    fold_constant_expressions,
)

__all__ = ["ConstantFoldingRule"]


class ConstantFoldingRule(Rule):
    """Folds constant expressions in Project and Filter operators.

    This rule applies compile-time evaluation of constant subexpressions
    to reduce runtime computation overhead. It handles:

    1. Project operators: Folds constants in all column expressions
    2. Filter operators: Folds constants in predicate expressions

    The rule performs iterative optimization to handle complex nested
    expressions that require multiple passes.

    Example transformations:
        Project([lit(3) + lit(5)])           → Project([lit(8)])
        Filter(col("x") > (lit(10) - lit(5))) → Filter(col("x") > lit(5))
        Project([col("x") * 1])              → Project([col("x")])

    Dependencies:
        - Should run early in the logical optimization pipeline
        - Should run before ProjectionPushdown to maximize effectiveness
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply constant folding to all operators in the plan.

        Args:
            plan: The logical plan to optimize

        Returns:
            An optimized logical plan with constants folded
        """
        dag = plan.dag
        new_dag = dag._apply_transform(self._fold_operator_expressions)

        # Only create new plan if DAG actually changed
        if new_dag is dag:
            return plan

        return LogicalPlan(new_dag, plan.context)

    @classmethod
    def _fold_operator_expressions(cls, op: LogicalOperator) -> LogicalOperator:
        """Fold constants in a single operator's expressions.

        Args:
            op: The operator to optimize

        Returns:
            The operator with folded expressions, or the original if unchanged
        """
        # Handle Project operators
        if isinstance(op, Project):
            return cls._fold_project_operator(op)

        # Handle Filter operators
        if isinstance(op, Filter):
            return cls._fold_filter_operator(op)

        # Other operators are returned unchanged
        return op

    @classmethod
    def _fold_project_operator(cls, op: Project) -> Project:
        """Fold constants in a Project operator's expressions.

        Args:
            op: The Project operator to optimize

        Returns:
            A new Project with folded expressions, or original if unchanged
        """
        folded_exprs = [fold_constant_expressions(e) for e in op.exprs]

        # Only create new operator if expressions actually changed
        # Use structural equality to check
        expressions_changed = any(
            not folded.structurally_equals(original)
            for folded, original in zip(folded_exprs, op.exprs)
        )

        if not expressions_changed:
            return op

        return Project(
            op.input_dependency,
            exprs=folded_exprs,
            compute=op._compute,
            ray_remote_args=op._ray_remote_args,
        )

    @classmethod
    def _fold_filter_operator(cls, op: Filter) -> Filter:
        """Fold constants in a Filter operator's predicate expression.

        Args:
            op: The Filter operator to optimize

        Returns:
            A new Filter with folded predicate, or original if unchanged
        """
        # Only fold expression-based filters
        if not op.is_expression_based():
            return op

        folded_predicate = fold_constant_expressions(op._predicate_expr)

        # Only create new operator if predicate actually changed
        if folded_predicate.structurally_equals(op._predicate_expr):
            return op

        return Filter(
            op.input_dependency,
            predicate_expr=folded_predicate,
        )
