from typing import List, Optional, Set, Tuple

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.planner.plan_expression.expression_visitors import (
    _ColumnReferenceCollector,
    _ColumnRewriter,
)
from ray.data.expressions import (
    AliasExpr,
    ColumnExpr,
    Expr,
    StarExpr,
)


def _collect_referenced_columns(exprs: List[Expr]) -> Optional[Set[str]]:
    """
    Extract all column names referenced by the given expressions.

    Recursively traverses expression trees to find all ColumnExpr nodes
    and collects their names.

    Example: For expression "col1 + col2", returns {"col1", "col2"}
    """
    # If any expression is star(), we need all columns
    if any(isinstance(expr, StarExpr) for expr in exprs):
        # TODO (goutam): Instead of using None to refer to All columns, resolve the AST against the schema.
        # https://github.com/ray-project/ray/issues/57720
        return None

    collector = _ColumnReferenceCollector()
    for expr in exprs or []:
        collector.visit(expr)
    return collector.referenced_columns


def _try_wrap_expression_with_alias(expr: Expr, target_name: str) -> Expr:
    """
    Ensure an expression outputs with the specified name.

    If the expression already has the target name, returns it unchanged.
    Otherwise, wraps it with an alias to produce the target name.
    """
    if expr.name == target_name:
        return expr
    if isinstance(expr, AliasExpr):
        # Re-alias the unwrapped expression
        return expr.expr.alias(target_name)
    return expr.alias(target_name)


def _extract_simple_rename(expr: Expr) -> Optional[Tuple[str, str]]:
    """
    Check if an expression is a simple column rename.

    Returns (source_name, dest_name) if the expression is of form:
        col("source").alias("dest")
    where source != dest.

    Returns None for other expression types.
    """
    if isinstance(expr, AliasExpr) and isinstance(expr.expr, ColumnExpr):
        dest_name = expr.name
        source_name = expr.expr.name
        if source_name != dest_name:
            return source_name, dest_name
    return None


def _analyze_upstream_project(
    upstream_project: Project,
) -> Tuple[Set[str], dict[str, Expr], Set[str]]:
    """
    Analyze what the upstream project produces and identifies removed columns.

    Example: Upstream exprs [col("x").alias("y")] → removed_by_renames = {"x"} if "x" not in output
    """
    output_columns = {
        expr.name for expr in upstream_project.exprs if not isinstance(expr, StarExpr)
    }
    column_definitions = {
        expr.name: _try_wrap_expression_with_alias(expr, expr.name)
        for expr in upstream_project.exprs
        if not isinstance(expr, StarExpr)
    }

    # Identify columns removed by renames (source not in output)
    removed_by_renames: Set[str] = set()
    for expr in upstream_project.exprs:
        if isinstance(expr, StarExpr):
            continue
        rename_pair = _extract_simple_rename(expr)
        if rename_pair is not None:
            source_name, _ = rename_pair
            if source_name not in output_columns:
                removed_by_renames.add(source_name)

    return output_columns, column_definitions, removed_by_renames


def _validate_fusion(
    downstream_project: Project,
    upstream_has_all: bool,
    upstream_output_columns: Set[str],
    removed_by_renames: Set[str],
) -> bool:
    """
    Validate if fusion is possible without rewriting expressions.
    Returns True if all expressions can be fused, False otherwise.

    Example: Downstream refs "x" but upstream renamed "x" to "y" and dropped "x" → False
    """
    for expr in downstream_project.exprs:
        if isinstance(expr, StarExpr):
            continue

        referenced_columns = _collect_referenced_columns([expr]) or set()
        columns_from_original = referenced_columns - (
            referenced_columns & upstream_output_columns
        )

        # Validate accessibility
        if not upstream_has_all and columns_from_original:
            # Example: Upstream selects ["a", "b"], Downstream refs "c" → can't fuse
            return False  # Downstream needs columns not in upstream output
        if any(col in removed_by_renames for col in columns_from_original):
            # Example: Upstream renames "x" to "y" (dropping "x"), Downstream refs "x" → can't fuse
            return False  # Downstream needs a removed column

    return True


def _merge_projects_with_star(
    upstream_project: Project,
    downstream_project: Project,
    upstream_has_star: bool,
    upstream_column_definitions: dict[str, Expr],
) -> Project:
    """
    Merge projects when downstream has star(), preserving order and applying transformations.

    Example: Upstream outputs "a": col("x"), Downstream renames "a" to "b" → fused has "b": col("x").alias("b"), drops "a"
    """
    downstream_output_columns = {
        expr.name for expr in downstream_project.exprs if not isinstance(expr, StarExpr)
    }

    # Start with upstream's definitions and order
    column_definitions = {
        expr.name: _try_wrap_expression_with_alias(expr, expr.name)
        for expr in upstream_project.exprs
        if not isinstance(expr, StarExpr)
    }
    column_order = [
        expr.name for expr in upstream_project.exprs if not isinstance(expr, StarExpr)
    ]

    # Apply downstream transformations using original expressions
    for expr in downstream_project.exprs:
        if isinstance(expr, StarExpr):
            continue

        column_name = expr.name
        rename_pair = _extract_simple_rename(expr)

        if rename_pair is not None:
            # Handle rename: resolve from upstream and update column order
            # Example: Upstream has {"d": col("x") + 1}, Downstream renames "d" to "D"
            # → Result has "D": col("x") + 1, drops "d" from definitions and order
            source_name, dest_name = rename_pair
            resolved_expr = upstream_column_definitions.get(source_name, expr)
            column_definitions[dest_name] = _try_wrap_expression_with_alias(
                resolved_expr, dest_name
            )

            if (
                source_name not in downstream_output_columns
                and source_name in column_definitions
            ):
                del column_definitions[source_name]

            if (
                source_name in column_order
                and source_name not in downstream_output_columns
            ):
                idx = column_order.index(source_name)
                column_order[idx] = dest_name
            elif dest_name not in column_order:
                column_order.append(dest_name)
            continue

        # Handle non-rename: rewrite the expression
        rewritten_expr = _ColumnRewriter(upstream_column_definitions).visit(expr)
        column_definitions[column_name] = _try_wrap_expression_with_alias(
            rewritten_expr, column_name
        )
        if column_name not in column_order:
            column_order.append(column_name)

    # Build fused expressions
    fused_exprs = (
        [StarExpr()] + [column_definitions[name] for name in column_order]
        if upstream_has_star
        else [column_definitions[name] for name in column_order]
    )

    return Project(
        upstream_project.input_dependency,
        exprs=fused_exprs,
        ray_remote_args=downstream_project._ray_remote_args,
    )


def _try_fuse_consecutive_projects(
    upstream_project: Project, downstream_project: Project
) -> Optional[Project]:
    """
    Attempt to merge two consecutive Project operations into one.

    Example: Upstream: [star(), col("x").alias("y")], Downstream: [star(), col("y") + 1 .alias("z")] → Fused: [star(), col("x") + 1 .alias("z")]
    """
    upstream_has_star: bool = upstream_project.has_star_expr()
    downstream_has_star: bool = downstream_project.has_star_expr()

    # Analyze upstream
    (
        upstream_output_columns,
        upstream_column_definitions,
        removed_by_renames,
    ) = _analyze_upstream_project(upstream_project)

    # Validate fusion possibility
    if not _validate_fusion(
        downstream_project,
        upstream_has_star,
        upstream_output_columns,
        removed_by_renames,
    ):
        return None

    # If downstream is a selection (no star), rewrite and output only specified columns
    if not downstream_has_star:
        rewritten_exprs: List[Expr] = []
        for expr in downstream_project.exprs:
            rewritten = _ColumnRewriter(upstream_column_definitions).visit(expr)
            rewritten_exprs.append(
                _try_wrap_expression_with_alias(rewritten, expr.name)
            )

        return Project(
            upstream_project.input_dependency,
            exprs=rewritten_exprs,
            ray_remote_args=downstream_project._ray_remote_args,
        )

    # Downstream has star: merge both, preserving upstream's star if present
    return _merge_projects_with_star(
        upstream_project,
        downstream_project,
        upstream_has_star,
        upstream_column_definitions,
    )


class ProjectionPushdown(Rule):
    """
    Optimization rule that pushes projections (column selections) down the query plan.

    This rule performs two optimizations:
    1. Fuses consecutive Project operations to eliminate redundant projections
    2. Pushes projections into data sources (e.g., Read operations) to enable
       column pruning at the storage layer
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply projection pushdown optimization to the entire plan."""
        dag = plan.dag
        new_dag = dag._apply_transform(self._optimize_project)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _optimize_project(cls, op: LogicalOperator) -> LogicalOperator:
        """
        Optimize a single Project operator.

        Steps:
        1. Iteratively fuse with upstream Project operations
        2. Push the resulting projection into the data source if possible
        """
        if not isinstance(op, Project):
            return op

        # Step 1: Iteratively fuse with upstream Project operations
        current_project: Project = op
        while isinstance(current_project.input_dependency, Project):
            upstream_project: Project = current_project.input_dependency  # type: ignore[assignment]
            fused_project = _try_fuse_consecutive_projects(
                upstream_project, current_project
            )
            if fused_project is None:
                # Fusion not possible, stop iterating
                break
            current_project = fused_project

        # Step 2: Push projection into the data source if supported
        input_op = current_project.input_dependency
        if (
            not current_project.has_star_expr()  # Must be a selection, not additive
            and isinstance(input_op, LogicalOperatorSupportsProjectionPushdown)
            and input_op.supports_projection_pushdown()
        ):
            required_columns = _collect_referenced_columns(list(current_project.exprs))
            if required_columns is not None:  # None means star() was present
                optimized_source = input_op.apply_projection(list(required_columns))
                return optimized_source

        return current_project
