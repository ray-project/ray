from typing import List, Optional, Set, Tuple

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
    UDFExpr,
    UnaryExpr,
)


def _collect_input_columns_from_exprs(exprs: List[Expr]) -> Set[str]:
    cols: Set[str] = set()

    def visit(e: Expr) -> None:
        if isinstance(e, ColumnExpr):
            cols.add(e.name)
        elif isinstance(e, AliasExpr):
            visit(e.expr)
        elif isinstance(e, BinaryExpr):
            visit(e.left)
            visit(e.right)
        elif isinstance(e, UnaryExpr):
            visit(e.operand)
        elif isinstance(e, UDFExpr):
            for a in e.args:
                visit(a)
            for v in e.kwargs.values():
                visit(v)
        elif isinstance(e, LiteralExpr):
            pass

    for ex in exprs or []:
        visit(ex)
    return cols


def _substitute_columns(expr: Expr, col_mapping: dict[str, Expr]) -> Expr:
    if isinstance(expr, ColumnExpr):
        mapped = col_mapping.get(expr.name)
        if mapped is not None:
            return mapped.expr if isinstance(mapped, AliasExpr) else mapped
        return expr
    if isinstance(expr, AliasExpr):
        return _substitute_columns(expr.expr, col_mapping).alias(expr.name)
    if isinstance(expr, BinaryExpr):
        return type(expr)(
            expr.op,
            _substitute_columns(expr.left, col_mapping),
            _substitute_columns(expr.right, col_mapping),
        )
    if isinstance(expr, UnaryExpr):
        return type(expr)(expr.op, _substitute_columns(expr.operand, col_mapping))
    if isinstance(expr, UDFExpr):
        new_args = [_substitute_columns(a, col_mapping) for a in expr.args]
        new_kwargs = {
            k: _substitute_columns(v, col_mapping) for k, v in expr.kwargs.items()
        }
        return type(expr)(
            fn=expr.fn, data_type=expr.data_type, args=new_args, kwargs=new_kwargs
        )
    return expr


def _get_aliased_expr(expr: Expr, name: str) -> Expr:
    if expr.name == name:
        return expr
    if isinstance(expr, AliasExpr):
        return expr.expr.alias(name)
    return expr.alias(name)


def _get_expr_renames(expr: Expr) -> Optional[Tuple[str, str]]:
    if isinstance(expr, AliasExpr) and isinstance(expr.expr, ColumnExpr):
        dest = expr.name
        src = expr.expr.name
        if src != dest:
            return src, dest
    return None


def _pairwise_fuse_projects(
    upstream: Project, downstream: Project
) -> Optional[Project]:
    """Try to fuse two consecutive Projects (upstream -> downstream). Return fused Project or None if invalid."""
    cols_produced_by_upstream = {e.name for e in upstream.exprs}
    produced_map = {e.name: _get_aliased_expr(e, e.name) for e in upstream.exprs}

    # Compute base names explicitly removed by 'rename' in up (when source isn't kept).
    columns_consumed_by_renames: Set[str] = set()
    for expr in upstream.exprs:
        renamed_expr = _get_expr_renames(expr)
        if renamed_expr is not None:
            src, _ = renamed_expr
            if src not in cols_produced_by_upstream:
                columns_consumed_by_renames.add(src)

    # Validate and substitute downstream expressions
    substituted_downstream_exprs: List[Expr] = []
    for expr in downstream.exprs:
        input_columns_from_expr = _collect_input_columns_from_exprs([expr])
        downstream_input_only_columns = input_columns_from_expr - (
            input_columns_from_expr & cols_produced_by_upstream
        )

        if not upstream.preserve_existing:
            # After a selection, only the produced names are visible.
            if downstream_input_only_columns:
                return None
        else:
            # Preserve-existing: base columns are allowed except ones explicitly removed by rename.
            if any(
                base_col in columns_consumed_by_renames
                for base_col in downstream_input_only_columns
            ):
                return None

        substituted = _substitute_columns(expr, produced_map)
        substituted_downstream_exprs.append(_get_aliased_expr(substituted, expr.name))

    # If downstream is a selection, it resets outputs.
    if not downstream.preserve_existing:
        return Project(
            upstream.input_dependency,
            exprs=substituted_downstream_exprs,
            preserve_existing=False,
            ray_remote_args=downstream._ray_remote_args,
        )

    # Preserve-existing downstream: merge into upstream outputs, honoring renames.
    current_output_names = {e.name for e in downstream.exprs}
    defs = {e.name: _get_aliased_expr(e, e.name) for e in upstream.exprs}
    order = [e.name for e in upstream.exprs]

    for expr in downstream.exprs:
        name = expr.name
        rename = _get_expr_renames(expr)

        if rename is not None:
            src, dest = rename
            resolved = produced_map.get(src, expr)
            defs[dest] = _get_aliased_expr(resolved, dest)

            # If source is not kept by downstream, remove/replace it.
            if src not in current_output_names and src in defs:
                del defs[src]
            if src in order and src not in current_output_names:
                idx = order.index(src)
                order[idx] = dest
            elif dest not in order:
                order.append(dest)
            continue

        # Non-rename: add/overwrite, keeping order stable.
        substituted = _substitute_columns(expr, produced_map)
        defs[name] = _get_aliased_expr(substituted, name)
        if name not in order:
            order.append(name)

    fused_exprs = [defs[n] for n in order]
    fused_preserve = upstream.preserve_existing and downstream.preserve_existing

    return Project(
        upstream.input_dependency,
        exprs=fused_exprs,
        preserve_existing=fused_preserve,
        ray_remote_args=downstream._ray_remote_args,
    )


class ProjectionPushdown(Rule):
    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self._pushdown_project)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _pushdown_project(cls, op: LogicalOperator) -> LogicalOperator:
        if not isinstance(op, Project):
            return op

        # Iteratively fuse pairwise with upstream Projects.
        current: Project = op
        while isinstance(current.input_dependency, Project):
            parent: Project = current.input_dependency  # type: ignore[assignment]
            fused = _pairwise_fuse_projects(parent, current)
            if fused is None:
                break
            current = fused

        # Optional projection pushdown if the final fused op is a selection.
        ancestor = current.input_dependency
        if (
            not current.preserve_existing
            and isinstance(ancestor, LogicalOperatorSupportsProjectionPushdown)
            and ancestor.supports_projection_pushdown()
        ):
            required = _collect_input_columns_from_exprs(list(current.exprs))
            if required:
                ancestor = ancestor.apply_projection(sorted(required))
                current = Project(
                    ancestor,
                    exprs=list(current.exprs),
                    preserve_existing=False,
                    ray_remote_args=current._ray_remote_args,
                )

        return current
