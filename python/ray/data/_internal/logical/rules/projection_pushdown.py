import logging
from typing import Dict, List, Set, Tuple

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Project
from ray.data.expressions import ColumnExpr, Expr

logger = logging.getLogger(__name__)


def _expr_output_name(expr: Expr) -> str:
    name = getattr(expr, "name", None)
    if name is None:
        raise ValueError(
            "Project expressions must be named; use .alias(name) or col(name)."
        )
    return name


def _collect_input_columns_from_exprs(exprs: List[Expr]) -> Set[str]:
    from ray.data.expressions import (
        AliasExpr,
        BinaryExpr,
        LiteralExpr,
        UDFExpr,
        UnaryExpr,
    )

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


def _substitute_column_refs(expr: Expr, col_mapping: Dict[str, Expr]) -> Expr:
    """Recursively substitute ColumnExpr references based on col_mapping."""
    from ray.data.expressions import (
        AliasExpr,
        BinaryExpr,
        LiteralExpr,
        UDFExpr,
        UnaryExpr,
    )

    if isinstance(expr, ColumnExpr):
        # If this column has a mapped expression, return it (without alias)
        if expr.name in col_mapping:
            mapped = col_mapping[expr.name]
            # Unwrap alias to get base expression
            if isinstance(mapped, AliasExpr):
                return mapped.expr
            return mapped
        return expr
    elif isinstance(expr, AliasExpr):
        # Recursively substitute in the inner expression, keep the alias name
        return _substitute_column_refs(expr.expr, col_mapping).alias(expr.name)
    elif isinstance(expr, BinaryExpr):
        return type(expr)(
            expr.op,
            _substitute_column_refs(expr.left, col_mapping),
            _substitute_column_refs(expr.right, col_mapping),
        )
    elif isinstance(expr, UnaryExpr):
        return type(expr)(
            expr.op,
            _substitute_column_refs(expr.operand, col_mapping),
        )
    elif isinstance(expr, UDFExpr):
        new_args = [_substitute_column_refs(a, col_mapping) for a in expr.args]
        new_kwargs = {
            k: _substitute_column_refs(v, col_mapping) for k, v in expr.kwargs.items()
        }
        return type(expr)(
            fn=expr.fn, data_type=expr.data_type, args=new_args, kwargs=new_kwargs
        )
    elif isinstance(expr, LiteralExpr):
        return expr
    else:
        # Unknown expression type, return as-is
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

    @classmethod
    def _supports_projection_pushdown(cls, op: Project) -> bool:
        # Only safe when we don't need to preserve unspecified columns.
        if op.preserve_existing:
            return False
        inp = op.input_dependency
        return (
            isinstance(inp, LogicalOperatorSupportsProjectionPushdown)
            and inp.supports_projection_pushdown()
        )

    @staticmethod
    def _fuse(op: Project) -> LogicalOperator:
        from ray.data.expressions import AliasExpr

        # Fuse the entire Project chain into one Project with correct semantics.
        ancestor, chain = _gather_project_chain(op)

        # defs: name -> Expr producing that name
        # order: final output order (names)
        defs: dict[str, Expr] = {}
        order: List[str] = []
        saw_selection = False

        def ensure_named(expr: Expr, name: str) -> Expr:
            """Ensure expression is aliased with the given name, unwrapping existing aliases."""
            if expr.name == name:
                return expr
            # Unwrap AliasExpr to avoid double-aliasing
            if isinstance(expr, AliasExpr):
                return expr.expr.alias(name)
            return expr.alias(name)

        for p in chain:
            if not p.preserve_existing:
                # Selection: reset to exactly requested outputs, resolved from previous defs.
                saw_selection = True
                new_defs: dict[str, Expr] = {}
                new_order: List[str] = []
                for e in p.exprs:
                    name = _expr_output_name(e)
                    # Substitute column references, then check if this column is already defined
                    resolved = _substitute_column_refs(e, defs)
                    new_defs[name] = ensure_named(resolved, name)
                    new_order.append(name)
                defs = new_defs
                order = new_order
                continue

            # Preserve-existing step: apply renames and adds without losing ordering.
            # Snapshot current defs to handle column swaps correctly
            snapshot_defs = defs.copy()
            snapshot_order = order.copy()

            # Track output names from this Project to avoid replacing them in order
            current_output_names = {_expr_output_name(e) for e in p.exprs}

            for e in p.exprs:
                name = _expr_output_name(e)
                # Detect simple rename: alias(ColumnExpr(src), name) with src != name
                inner = getattr(e, "expr", None)
                if isinstance(inner, ColumnExpr) and inner.name != name:
                    src = inner.name
                    # Resolve definition from snapshot, not current defs
                    resolved = snapshot_defs.get(src, e)
                    # Update mapping: name <- src
                    defs[name] = ensure_named(resolved, name)
                    # Update order: replace src with name ONLY if src is not also an output of this Project
                    if src in snapshot_order and src not in current_output_names:
                        idx = snapshot_order.index(src)
                        order[idx] = name
                    elif name not in order:
                        order.append(name)
                else:
                    # For non-rename expressions, substitute column references
                    e_subst = _substitute_column_refs(e, snapshot_defs)
                    defs[name] = ensure_named(e_subst, name)
                    if name not in order:
                        order.append(name)

        # Build final expr list in order; decide preservation.
        final_exprs = [defs[n] for n in order]
        final_preserve = not saw_selection

        # Optional pushdown for selection-only final.
        if (
            not final_preserve
            and isinstance(ancestor, LogicalOperatorSupportsProjectionPushdown)
            and ancestor.supports_projection_pushdown()
        ):
            required = _collect_input_columns_from_exprs(final_exprs)
            if required:
                ancestor = ancestor.apply_projection(sorted(required))

        return Project(
            ancestor,
            exprs=final_exprs,
            preserve_existing=final_preserve,
            ray_remote_args=chain[-1]._ray_remote_args,
        )
