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
    """Collect all input column names referenced by the given expressions."""
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
            # literal: no columns
            pass

    for ex in exprs or []:
        visit(ex)
    return cols


def _substitute_column_refs(expr: Expr, col_mapping: dict[str, Expr]) -> Expr:
    """Recursively substitute ColumnExpr references according to col_mapping."""
    if isinstance(expr, ColumnExpr):
        mapped = col_mapping.get(expr.name)
        if mapped is not None:
            # Unwrap alias to avoid double-aliasing downstream
            return mapped.expr if isinstance(mapped, AliasExpr) else mapped
        return expr
    if isinstance(expr, AliasExpr):
        return _substitute_column_refs(expr.expr, col_mapping).alias(expr.name)
    if isinstance(expr, BinaryExpr):
        return type(expr)(
            expr.op,
            _substitute_column_refs(expr.left, col_mapping),
            _substitute_column_refs(expr.right, col_mapping),
        )
    if isinstance(expr, UnaryExpr):
        return type(expr)(expr.op, _substitute_column_refs(expr.operand, col_mapping))
    if isinstance(expr, UDFExpr):
        new_args = [_substitute_column_refs(a, col_mapping) for a in expr.args]
        new_kwargs = {
            k: _substitute_column_refs(v, col_mapping) for k, v in expr.kwargs.items()
        }
        return type(expr)(
            fn=expr.fn, data_type=expr.data_type, args=new_args, kwargs=new_kwargs
        )
    return expr


def _gather_project_chain(op: Project) -> Tuple[LogicalOperator, List[Project]]:
    """Return (first_non_project_ancestor, projects_chain) where projects_chain is from
    farthest upstream Project to the given op (inclusive order)."""
    chain: List[Project] = []
    cur = op
    while isinstance(cur, Project):
        chain.append(cur)
        parent = cur.input_dependency
        if not isinstance(parent, Project):
            return parent, list(reversed(chain))
        cur = parent
    return cur, [op]


def _ensure_named(expr: Expr, name: str) -> Expr:
    """Ensure expression is aliased with the given name, unwrapping existing aliases."""
    if expr.name == name:
        return expr
    if isinstance(expr, AliasExpr):
        return expr.expr.alias(name)
    return expr.alias(name)


def _is_simple_rename(expr: Expr) -> Optional[Tuple[str, str]]:
    """Detect expressions of the form col(src).alias(dest) where src != dest."""
    if isinstance(expr, AliasExpr) and isinstance(expr.expr, ColumnExpr):
        dest = expr.name
        src = expr.expr.name
        if src != dest:
            return src, dest
    return None


def _apply_selection_step(
    prev_defs: dict[str, Expr], project: Project
) -> Tuple[dict[str, Expr], List[str]]:
    """Selection step: reset outputs to exactly the given expressions and order."""
    new_defs: dict[str, Expr] = {}
    new_order: List[str] = []
    for expr in project.exprs:
        name = expr.name
        resolved = _substitute_column_refs(expr, prev_defs)
        new_defs[name] = _ensure_named(resolved, name)
        new_order.append(name)
    return new_defs, new_order


def _apply_preserve_step(
    prev_defs: dict[str, Expr], prev_order: List[str], project: Project
) -> Tuple[dict[str, Expr], List[str]]:
    """Preserve-existing step: apply renames and adds without losing ordering.

    - Renames replace the position of the source column if the source isn't also
      produced as an output by this same project.
    - New columns are appended to the end.
    """
    snapshot_defs = prev_defs.copy()
    snapshot_order = prev_order.copy()
    current_output_names = {e.name for e in project.exprs}

    defs = prev_defs.copy()
    order = prev_order.copy()

    for expr in project.exprs:
        name = expr.name
        rename = _is_simple_rename(expr)

        if rename is not None:
            src, dest = rename
            resolved = snapshot_defs.get(src, expr)
            defs[dest] = _ensure_named(resolved, dest)

            if src in snapshot_order and src not in current_output_names:
                idx = snapshot_order.index(src)
                order[idx] = dest
            elif dest not in order:
                order.append(dest)
            continue

        # Non-rename: substitute column references through snapshot_defs
        substituted = _substitute_column_refs(expr, snapshot_defs)
        defs[name] = _ensure_named(substituted, name)
        if name not in order:
            order.append(name)

    return defs, order


class ProjectionPushdown(Rule):
    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self._pushdown_project)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _pushdown_project(cls, op: LogicalOperator) -> LogicalOperator:
        if isinstance(op, Project):
            return cls._fuse(op)
        return op

    @staticmethod
    def _fuse(op: Project) -> LogicalOperator:
        # Fuse the entire Project chain into one Project with correct semantics.
        ancestor, chain = _gather_project_chain(op)

        # defs: name -> Expr producing that name
        # order: final output order (names)
        defs: dict[str, Expr] = {}
        order: List[str] = []
        is_selection_op = False

        for project in chain:
            if not project.preserve_existing:
                is_selection_op = True
                defs, order = _apply_selection_step(defs, project)
            else:
                defs, order = _apply_preserve_step(defs, order, project)

        # Build final expr list in order; decide preservation.
        final_exprs = [defs[n] for n in order]

        # Optional pushdown for selection-only final.
        if (
            is_selection_op
            and isinstance(ancestor, LogicalOperatorSupportsProjectionPushdown)
            and ancestor.supports_projection_pushdown()
        ):
            required = _collect_input_columns_from_exprs(final_exprs)
            if required:
                ancestor = ancestor.apply_projection(sorted(required))

        return Project(
            ancestor,
            exprs=final_exprs,
            preserve_existing=not is_selection_op,
            ray_remote_args=chain[-1]._ray_remote_args,
        )
