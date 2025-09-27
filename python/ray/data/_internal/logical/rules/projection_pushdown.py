import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Project, ProjectionMode
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    UDFExpr,
    UnaryExpr,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ProjectSpec:
    """Projection specification based on expressions."""

    exprs: Dict[str, Expr]
    mode: ProjectionMode


class ProjectionPushdown(Rule):
    """Optimization rule that pushes down projections across the graph.

    This rule handles all projection modes:
    - SELECT: Selects only specified columns
    - RENAME: Renames columns via aliases
    - HSTACK: Adds computed columns
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self._pushdown_project)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _pushdown_project(cls, op: LogicalOperator) -> LogicalOperator:
        if not isinstance(op, Project):
            return op

        # Try to push down simple projections into read operations
        if cls._supports_projection_pushdown(op):
            project_op: Project = op
            target_op: LogicalOperatorSupportsProjectionPushdown = op.input_dependency
            return cls._try_combine(target_op, project_op)

        # Try to fuse consecutive projection operations
        if isinstance(op.input_dependency, Project):
            outer_op: Project = op
            inner_op: Project = op.input_dependency
            return cls._fuse(inner_op, outer_op)

        return op

    @classmethod
    def _supports_projection_pushdown(cls, op: Project) -> bool:
        """Check if projection can be pushed down to read operation."""
        input_op = op.input_dependency

        return (
            isinstance(input_op, LogicalOperatorSupportsProjectionPushdown)
            and input_op.supports_projection_pushdown()
            and op._mode == ProjectionMode.SELECT
            and _is_simple_column_selection(op.exprs)
        )

    @staticmethod
    def _fuse(inner_op: Project, outer_op: Project) -> Project:
        """Fuse two Project operators into a single operator."""
        inner_spec = _ProjectSpec(exprs=inner_op.exprs, mode=inner_op._mode)
        outer_spec = _ProjectSpec(exprs=outer_op.exprs, mode=outer_op._mode)

        combined_spec = _combine_projection_specs(inner_spec, outer_spec)

        logger.debug(
            f"Fusing projections: {inner_op._mode} + {outer_op._mode} "
            f"-> {combined_spec.mode}, columns: {list(combined_spec.exprs.keys())}"
        )

        return Project(
            inner_op.input_dependency,
            exprs=combined_spec.exprs,
            mode=combined_spec.mode,
            ray_remote_args={
                **inner_op._ray_remote_args,
                **outer_op._ray_remote_args,
            },
        )

    @staticmethod
    def _try_combine(
        target_op: LogicalOperatorSupportsProjectionPushdown,
        project_op: Project,
    ) -> LogicalOperator:
        """Push simple column selection into read operation."""
        if not _is_simple_column_selection(project_op.exprs):
            return project_op

        if project_op._mode != ProjectionMode.SELECT:
            return project_op

        try:
            cols_to_select = _extract_column_names(project_op.exprs)
        except ValueError as e:
            logger.debug(f"Cannot extract columns for pushdown: {e}")
            return project_op

        logger.debug(f"Pushing projection down into read: columns = {cols_to_select}")
        return target_op.apply_projection(cols_to_select)


def _combine_projection_specs(
    inner_spec: _ProjectSpec, outer_spec: _ProjectSpec
) -> _ProjectSpec:
    """Combine two projection specifications."""

    # Case 1: Outer is SELECT - always filters to selected columns only
    if outer_spec.mode == ProjectionMode.SELECT:
        return _combine_with_select_outer(inner_spec, outer_spec)

    # Case 2: Inner is SELECT - only selected columns exist
    if inner_spec.mode == ProjectionMode.SELECT:
        return _combine_with_select_inner(inner_spec, outer_spec)

    # Case 3: RENAME + HSTACK
    if (
        inner_spec.mode == ProjectionMode.RENAME
        and outer_spec.mode == ProjectionMode.HSTACK
    ):
        return _combine_rename_hstack(inner_spec, outer_spec)

    # Case 4: RENAME + RENAME
    if (
        inner_spec.mode == ProjectionMode.RENAME
        and outer_spec.mode == ProjectionMode.RENAME
    ):
        return _combine_rename_rename(inner_spec, outer_spec)

    # Case 5: HSTACK + RENAME
    if (
        inner_spec.mode == ProjectionMode.HSTACK
        and outer_spec.mode == ProjectionMode.RENAME
    ):
        return _combine_hstack_rename(inner_spec, outer_spec)

    # Case 6: HSTACK + HSTACK
    if (
        inner_spec.mode == ProjectionMode.HSTACK
        and outer_spec.mode == ProjectionMode.HSTACK
    ):
        return _combine_hstack_hstack(inner_spec, outer_spec)

    raise ValueError(f"Unhandled combination: {inner_spec.mode} + {outer_spec.mode}")


def _combine_with_select_outer(
    inner_spec: _ProjectSpec, outer_spec: _ProjectSpec
) -> _ProjectSpec:
    """Handle case where outer operation is SELECT."""
    combined_exprs = {}

    for output_name, select_expr in outer_spec.exprs.items():
        if isinstance(select_expr, ColumnExpr):
            col_name = select_expr.name
            # Check if this column exists in inner output
            if col_name in inner_spec.exprs:
                combined_exprs[output_name] = inner_spec.exprs[col_name]
            else:
                # Column from original source
                combined_exprs[output_name] = select_expr
        else:
            # Complex expression
            combined_exprs[output_name] = _substitute_in_expression(
                select_expr, inner_spec.exprs
            )

    return _ProjectSpec(exprs=combined_exprs, mode=ProjectionMode.SELECT)


def _combine_with_select_inner(
    inner_spec: _ProjectSpec, outer_spec: _ProjectSpec
) -> _ProjectSpec:
    """Handle case where inner operation is SELECT."""
    if outer_spec.mode == ProjectionMode.RENAME:
        # Can only rename selected columns
        combined_exprs = {}

        # First copy non-renamed selected columns
        for col_name, expr in inner_spec.exprs.items():
            combined_exprs[col_name] = expr

        # Apply renames
        for output_name, rename_expr in outer_spec.exprs.items():
            source = _get_source_column(rename_expr)
            if source and source in inner_spec.exprs:
                combined_exprs[output_name] = inner_spec.exprs[source]
                if output_name != source and source in combined_exprs:
                    del combined_exprs[source]

        return _ProjectSpec(exprs=combined_exprs, mode=ProjectionMode.SELECT)

    elif outer_spec.mode == ProjectionMode.HSTACK:
        # Add new columns to selected
        combined_exprs = inner_spec.exprs.copy()
        for name, expr in outer_spec.exprs.items():
            combined_exprs[name] = _substitute_in_expression(expr, inner_spec.exprs)
        # Stay in SELECT mode - only these columns should exist
        return _ProjectSpec(exprs=combined_exprs, mode=ProjectionMode.SELECT)

    raise ValueError(f"Unexpected combination: SELECT + {outer_spec.mode}")


def _combine_rename_hstack(
    inner_spec: _ProjectSpec, outer_spec: _ProjectSpec
) -> _ProjectSpec:
    """Combine RENAME followed by HSTACK."""
    combined_exprs = {}

    # Include all renames from inner
    combined_exprs.update(inner_spec.exprs)

    # Build substitution map: new name -> original column
    rename_map = {}
    for name, expr in inner_spec.exprs.items():
        if isinstance(expr, AliasExpr) and isinstance(expr.expr, ColumnExpr):
            # col("old").alias("new") - "new" references "old"
            rename_map[name] = expr.expr
        elif isinstance(expr, ColumnExpr):
            rename_map[name] = expr

    # Add new columns from outer, substituting renamed column references
    for name, expr in outer_spec.exprs.items():
        if name not in combined_exprs:
            combined_exprs[name] = _substitute_in_expression(expr, rename_map)

    # Keep RENAME mode to ensure rename semantics are preserved
    return _ProjectSpec(exprs=combined_exprs, mode=ProjectionMode.RENAME)


def _combine_rename_rename(
    inner_spec: _ProjectSpec, outer_spec: _ProjectSpec
) -> _ProjectSpec:
    """Chain two RENAME operations."""
    combined_exprs = {}

    # Find which columns are renamed by outer
    outer_renames = _get_renamed_columns(outer_spec.exprs)

    # Keep inner renames that aren't overridden
    for name, expr in inner_spec.exprs.items():
        if name not in outer_renames:
            combined_exprs[name] = expr

    # Apply outer renames, checking for chains
    for output_name, rename_expr in outer_spec.exprs.items():
        source = _get_source_column(rename_expr)
        if source and source in inner_spec.exprs:
            # Chain the rename
            inner_expr = inner_spec.exprs[source]
            original_source = _get_original_source(inner_expr)
            combined_exprs[output_name] = ColumnExpr(original_source).alias(output_name)
        else:
            combined_exprs[output_name] = rename_expr

    return _ProjectSpec(exprs=combined_exprs, mode=ProjectionMode.RENAME)


def _combine_hstack_rename(
    inner_spec: _ProjectSpec, outer_spec: _ProjectSpec
) -> _ProjectSpec:
    """Combine HSTACK followed by RENAME."""
    combined_exprs = inner_spec.exprs.copy()

    # Apply renames
    for output_name, rename_expr in outer_spec.exprs.items():
        source = _get_source_column(rename_expr)
        if source:
            if source in combined_exprs:
                # Rename hstacked column
                combined_exprs[output_name] = combined_exprs[source]
                if output_name != source:
                    del combined_exprs[source]
            else:
                # Rename original column
                combined_exprs[output_name] = rename_expr

    return _ProjectSpec(exprs=combined_exprs, mode=ProjectionMode.HSTACK)


def _combine_hstack_hstack(
    inner_spec: _ProjectSpec, outer_spec: _ProjectSpec
) -> _ProjectSpec:
    """Combine two HSTACK operations."""
    combined_exprs = inner_spec.exprs.copy()

    for name, expr in outer_spec.exprs.items():
        combined_exprs[name] = _substitute_in_expression(expr, inner_spec.exprs)

    return _ProjectSpec(exprs=combined_exprs, mode=ProjectionMode.HSTACK)


# Helper functions


def _is_simple_column_selection(exprs: Dict[str, Expr]) -> bool:
    """Check if all expressions are simple column references."""
    return all(isinstance(expr, ColumnExpr) for expr in exprs.values())


def _extract_column_names(exprs: Dict[str, Expr]) -> List[str]:
    """Extract column names from simple column expressions."""
    columns = []
    for expr in exprs.values():
        if isinstance(expr, ColumnExpr):
            columns.append(expr.name)
        else:
            raise ValueError(
                f"Cannot extract column name from complex expression: {expr}"
            )
    return columns


def _get_source_column(expr: Expr) -> Optional[str]:
    """Get the source column name from a rename expression."""
    if isinstance(expr, AliasExpr) and isinstance(expr.expr, ColumnExpr):
        return expr.expr.name
    elif isinstance(expr, ColumnExpr):
        return expr.name
    return None


def _get_original_source(expr: Expr) -> str:
    """Get the original source column from a potentially chained rename."""
    if isinstance(expr, ColumnExpr):
        return expr.name
    elif isinstance(expr, AliasExpr) and isinstance(expr.expr, ColumnExpr):
        return expr.expr.name
    else:
        raise ValueError(f"Cannot determine original source from {expr}")


def _get_renamed_columns(exprs: Dict[str, Expr]) -> Set[str]:
    """Get the set of columns being renamed."""
    renamed = set()
    for expr in exprs.values():
        source = _get_source_column(expr)
        if source:
            renamed.add(source)
    return renamed


def _substitute_in_expression(expr: Expr, substitutions: Dict[str, Expr]) -> Expr:
    """Recursively substitute column references in an expression."""
    # Base case: simple column reference
    if isinstance(expr, ColumnExpr):
        return substitutions.get(expr.name, expr)

    # Handle alias expressions
    if isinstance(expr, AliasExpr):
        substituted_inner = _substitute_in_expression(expr.expr, substitutions)
        if substituted_inner is expr.expr:
            return expr
        return AliasExpr(
            expr=substituted_inner, _name=expr._name, data_type=expr.data_type
        )

    # Handle binary expressions
    if isinstance(expr, BinaryExpr):
        substituted_left = _substitute_in_expression(expr.left, substitutions)
        substituted_right = _substitute_in_expression(expr.right, substitutions)

        if substituted_left is expr.left and substituted_right is expr.right:
            return expr

        return BinaryExpr(op=expr.op, left=substituted_left, right=substituted_right)

    # Handle unary expressions
    if isinstance(expr, UnaryExpr):
        substituted_operand = _substitute_in_expression(expr.operand, substitutions)

        if substituted_operand is expr.operand:
            return expr

        return UnaryExpr(op=expr.op, operand=substituted_operand)

    # Handle other expression types
    if isinstance(expr, (UDFExpr, LiteralExpr)):
        return expr

    # For unknown expression types, return unchanged
    return expr
