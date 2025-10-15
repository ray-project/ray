from typing import Any, List, Optional, Set, Tuple

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Project
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    StarColumnsExpr,
    UDFExpr,
    UnaryExpr,
    _ExprVisitor,
)


class _ColumnReferenceCollector(_ExprVisitor):
    """Visitor that collects all column references from expression trees.

    This visitor traverses expression trees and accumulates column names
    referenced in ColumnExpr nodes.
    """

    def __init__(self):
        """Initialize with an empty set of referenced columns."""
        self.referenced_columns: Set[str] = set()

    def visit(self, expr: Expr) -> Any:
        """Visit an expression node and dispatch to the appropriate method.

        Extends the base visitor to handle StarColumnsExpr which is not
        part of the base _ExprVisitor interface.

        Args:
            expr: The expression to visit.

        Returns:
            None (only collects columns as a side effect).
        """
        if isinstance(expr, StarColumnsExpr):
            # StarColumnsExpr doesn't reference specific columns
            return None
        return super().visit(expr)

    def visit_column(self, expr: ColumnExpr) -> Any:
        """Visit a column expression and collect its name.

        Args:
            expr: The column expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.referenced_columns.add(expr.name)

    def visit_literal(self, expr: LiteralExpr) -> Any:
        """Visit a literal expression (no columns to collect).

        Args:
            expr: The literal expression.

        Returns:
            None.
        """
        # Literals don't reference any columns
        pass

    def visit_binary(self, expr: BinaryExpr) -> Any:
        """Visit a binary expression and collect from both operands.

        Args:
            expr: The binary expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_unary(self, expr: UnaryExpr) -> Any:
        """Visit a unary expression and collect from its operand.

        Args:
            expr: The unary expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.operand)

    def visit_udf(self, expr: UDFExpr) -> Any:
        """Visit a UDF expression and collect from all arguments.

        Args:
            expr: The UDF expression.

        Returns:
            None (only collects columns as a side effect).
        """
        for arg in expr.args:
            self.visit(arg)
        for value in expr.kwargs.values():
            self.visit(value)

    def visit_alias(self, expr: AliasExpr) -> Any:
        """Visit an alias expression and collect from its inner expression.

        Args:
            expr: The alias expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.expr)

    def visit_download(self, expr: "Expr") -> Any:
        """Visit a download expression (no columns to collect).

        Args:
            expr: The download expression.

        Returns:
            None.
        """
        # DownloadExpr doesn't reference any columns in the projection pushdown context
        pass


def _collect_referenced_columns(exprs: List[Expr]) -> Optional[Set[str]]:
    """
    Extract all column names referenced by the given expressions.

    Recursively traverses expression trees to find all ColumnExpr nodes
    and collects their names.

    Example: For expression "col1 + col2", returns {"col1", "col2"}
    """
    # If any expression is star(), we need all columns
    if any(isinstance(expr, StarColumnsExpr) for expr in exprs):
        # TODO (goutam): Instead of using None to refer to All columns, resolve the AST against the schema.
        # https://github.com/ray-project/ray/issues/57720
        return None

    collector = _ColumnReferenceCollector()
    for expr in exprs or []:
        collector.visit(expr)
    return collector.referenced_columns


def _rewrite_column_references(
    expr: Expr, column_substitutions: dict[str, Expr]
) -> Expr:
    """
    Rewrite an expression by substituting column references.

    Recursively replaces ColumnExpr nodes according to the substitution map.
    Preserves the structure of the expression tree.

    Example: If column_substitutions = {"col1": col2_expr}, then
             "col1 + 10" becomes "col2 + 10"
    """
    if isinstance(expr, ColumnExpr):
        # Check if this column should be substituted
        substitution = column_substitutions.get(expr.name)
        if substitution is not None:
            # Unwrap aliases to get the actual expression
            return (
                substitution.expr
                if isinstance(substitution, AliasExpr)
                else substitution
            )
        return expr

    if isinstance(expr, AliasExpr):
        # Rewrite the inner expression but preserve the alias name
        return _rewrite_column_references(expr.expr, column_substitutions).alias(
            expr.name
        )

    if isinstance(expr, BinaryExpr):
        return type(expr)(
            expr.op,
            _rewrite_column_references(expr.left, column_substitutions),
            _rewrite_column_references(expr.right, column_substitutions),
        )

    if isinstance(expr, UnaryExpr):
        return type(expr)(
            expr.op, _rewrite_column_references(expr.operand, column_substitutions)
        )

    if isinstance(expr, UDFExpr):
        new_args = [
            _rewrite_column_references(arg, column_substitutions) for arg in expr.args
        ]
        new_kwargs = {
            key: _rewrite_column_references(value, column_substitutions)
            for key, value in expr.kwargs.items()
        }
        return type(expr)(
            fn=expr.fn, data_type=expr.data_type, args=new_args, kwargs=new_kwargs
        )

    # For other expression types (e.g., LiteralExpr), return as-is
    return expr


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


def _try_fuse_consecutive_projects(
    upstream_project: Project, downstream_project: Project
) -> Optional[Project]:
    """
    Attempt to merge two consecutive Project operations into one.

    Updated to handle StarColumnsExpr instead of preserve_existing flag.
    """
    from ray.data.expressions import StarColumnsExpr

    # Check if projects have star()
    upstream_has_all = upstream_project.has_all_columns_expr()
    downstream_has_all = downstream_project.has_all_columns_expr()

    # Step 1: Analyze what the upstream project produces
    upstream_output_columns = {
        expr.name
        for expr in upstream_project.exprs
        if not isinstance(expr, StarColumnsExpr)
    }
    upstream_column_definitions = {
        expr.name: _try_wrap_expression_with_alias(expr, expr.name)
        for expr in upstream_project.exprs
        if not isinstance(expr, StarColumnsExpr)
    }

    # Step 2: Identify columns removed by upstream renames
    # When "col1" is renamed to "col2" and "col1" is not in the output,
    # then "col1" is effectively removed and cannot be accessed downstream.
    columns_removed_by_renames: Set[str] = set()
    for expr in upstream_project.exprs:
        if isinstance(expr, StarColumnsExpr):
            continue
        rename_pair = _extract_simple_rename(expr)
        if rename_pair is not None:
            source_name, _ = rename_pair
            if source_name not in upstream_output_columns:
                columns_removed_by_renames.add(source_name)

    # Step 3: Validate and rewrite downstream expressions
    rewritten_downstream_exprs: List[Expr] = []
    for expr in downstream_project.exprs:
        if isinstance(expr, StarColumnsExpr):
            # star() passes through in fusion
            rewritten_downstream_exprs.append(expr)
            continue

        # Find which columns this expression references
        referenced_columns = _collect_referenced_columns([expr])

        # Separate columns: produced by upstream vs. pass-through from original input
        columns_from_original_input = referenced_columns - (
            referenced_columns & upstream_output_columns
        )

        # Validate that downstream can access the columns it needs
        if not upstream_has_all:
            # Upstream is a selection: only upstream outputs are visible
            if columns_from_original_input:
                # Fusion not possible: downstream needs columns not in upstream output
                return None
        else:
            # Upstream preserves existing: pass-through columns are allowed,
            # except those explicitly removed by renames
            if any(
                col in columns_removed_by_renames for col in columns_from_original_input
            ):
                # Fusion not possible: downstream needs a removed column
                return None

        # Rewrite the expression to use upstream's definitions
        rewritten_expr = _rewrite_column_references(expr, upstream_column_definitions)
        rewritten_downstream_exprs.append(
            _try_wrap_expression_with_alias(rewritten_expr, expr.name)
        )

    # Step 4: Build the fused project based on downstream's behavior
    if not downstream_has_all:
        # Downstream is a selection: output only what downstream specifies
        return Project(
            upstream_project.input_dependency,
            exprs=rewritten_downstream_exprs,
            ray_remote_args=downstream_project._ray_remote_args,
        )

    # Step 5: Downstream has star(): merge both projections
    downstream_output_columns = {
        expr.name
        for expr in downstream_project.exprs
        if not isinstance(expr, StarColumnsExpr)
    }

    # Start with upstream's column definitions and ordering
    column_definitions = {
        expr.name: _try_wrap_expression_with_alias(expr, expr.name)
        for expr in upstream_project.exprs
        if not isinstance(expr, StarColumnsExpr)
    }
    column_order = [
        expr.name
        for expr in upstream_project.exprs
        if not isinstance(expr, StarColumnsExpr)
    ]

    # Apply downstream's transformations
    #
    # Example scenario:
    #   Upstream outputs: {a: col("x"), b: col("y") + 1, c: col("z")}
    #   Downstream exprs: [col("a").alias("d"), col("b") + 2]
    #
    # After this loop:
    #   - "a" is renamed to "d" (source "a" removed if not in downstream output)
    #   - "b" is overwritten with a new definition: (col("y") + 1) + 2
    #   - "c" passes through unchanged from upstream
    for expr in downstream_project.exprs:
        if isinstance(expr, StarColumnsExpr):
            continue

        column_name = expr.name
        rename_pair = _extract_simple_rename(expr)

        if rename_pair is not None:
            # Handle rename: source -> dest
            # Example: col("a").alias("d") means rename "a" to "d"
            source_name, dest_name = rename_pair
            resolved_expr = upstream_column_definitions.get(source_name, expr)
            column_definitions[dest_name] = _try_wrap_expression_with_alias(
                resolved_expr, dest_name
            )

            # If source is not kept by downstream, remove it from the output
            # Example: After renaming "a" to "d", if "a" is not in downstream outputs,
            # we remove "a" from the final output (only "d" remains)
            if (
                source_name not in downstream_output_columns
                and source_name in column_definitions
            ):
                del column_definitions[source_name]

            # Update column ordering: replace source with dest or append dest
            # Example: If order was ["a", "b", "c"] and "a" renamed to "d",
            # order becomes ["d", "b", "c"] (maintaining position)
            if (
                source_name in column_order
                and source_name not in downstream_output_columns
            ):
                idx = column_order.index(source_name)
                column_order[idx] = dest_name
            elif dest_name not in column_order:
                column_order.append(dest_name)
            continue

        # Handle non-rename: add or overwrite column definition
        # Example: If downstream has col("b") + 2, and upstream had b: col("y") + 1,
        # we rewrite to: (col("y") + 1) + 2, collapsing both transformations
        rewritten_expr = _rewrite_column_references(expr, upstream_column_definitions)
        column_definitions[column_name] = _try_wrap_expression_with_alias(
            rewritten_expr, column_name
        )
        if column_name not in column_order:
            column_order.append(column_name)

    # Build final fused project
    # Only include star() if upstream also had it (preserving selection semantics)
    if upstream_has_all:
        # Upstream preserves existing: fused result should too
        fused_exprs = [StarColumnsExpr()] + [
            column_definitions[name] for name in column_order
        ]
    else:
        # Upstream is a selection: fused result should only have explicit columns
        fused_exprs = [column_definitions[name] for name in column_order]

    return Project(
        upstream_project.input_dependency,
        exprs=fused_exprs,
        ray_remote_args=downstream_project._ray_remote_args,
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
        # For example, when reading Parquet files, we can pass column names
        # to only read the required columns.
        input_op = current_project.input_dependency
        if (
            not current_project.has_all_columns_expr()  # Must be a selection, not additive
            and isinstance(input_op, LogicalOperatorSupportsProjectionPushdown)
            and input_op.supports_projection_pushdown()
        ):
            required_columns = _collect_referenced_columns(list(current_project.exprs))
            if required_columns is not None:  # None means star() was present
                optimized_source = input_op.apply_projection(sorted(required_columns))
                return optimized_source

        return current_project
