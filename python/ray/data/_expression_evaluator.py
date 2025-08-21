from __future__ import annotations

import operator
from typing import Any, Callable, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from ray.data.expressions import (
    BinaryExpr,
    CaseExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    WhenExpr,
)

_PANDAS_EXPR_OPS_MAP = {
    Operation.ADD: operator.add,
    Operation.SUB: operator.sub,
    Operation.MUL: operator.mul,
    Operation.DIV: operator.truediv,
    Operation.GT: operator.gt,
    Operation.LT: operator.lt,
    Operation.GE: operator.ge,
    Operation.LE: operator.le,
    Operation.EQ: operator.eq,
    Operation.AND: operator.and_,
    Operation.OR: operator.or_,
}

_ARROW_EXPR_OPS_MAP = {
    Operation.ADD: pc.add,
    Operation.SUB: pc.subtract,
    Operation.MUL: pc.multiply,
    Operation.DIV: pc.divide,
    Operation.GT: pc.greater,
    Operation.LT: pc.less,
    Operation.GE: pc.greater_equal,
    Operation.LE: pc.less_equal,
    Operation.EQ: pc.equal,
    Operation.AND: pc.and_,
    Operation.OR: pc.or_,
}


def _eval_expr_recursive(expr: "Expr", batch, ops: Dict["Operation", Callable]) -> Any:
    """Generic recursive expression evaluator."""
    # TODO: Separate unresolved expressions (arbitrary AST with unresolved refs)
    # and resolved expressions (bound to a schema) for better error handling

    if isinstance(expr, ColumnExpr):
        return batch[expr.name]
    if isinstance(expr, LiteralExpr):
        return expr.value
    if isinstance(expr, BinaryExpr):
        return ops[expr.op](
            _eval_expr_recursive(expr.left, batch, ops),
            _eval_expr_recursive(expr.right, batch, ops),
        )
    if isinstance(expr, CaseExpr):
        # Evaluate case statement: test each condition in order
        for condition, value in expr.when_clauses:
            if _eval_expr_recursive(condition, batch, ops):
                return _eval_expr_recursive(value, batch, ops)
        # If no conditions match, return default value
        return _eval_expr_recursive(expr.default, batch, ops)
    if isinstance(expr, WhenExpr):
        # WhenExpr should not be evaluated directly - it should be converted to CaseExpr first
        raise TypeError(
            "WhenExpr cannot be evaluated directly. Use .otherwise() to complete the case statement."
        )

    raise TypeError(f"Unsupported expression node: {type(expr).__name__}")


def eval_expr(expr: "Expr", batch) -> Any:
    """Recursively evaluate *expr* against a batch of the appropriate type."""
    if isinstance(batch, pd.DataFrame):
        return _eval_expr_recursive(expr, batch, _PANDAS_EXPR_OPS_MAP)
    elif isinstance(batch, pa.Table):
        return _eval_expr_recursive(expr, batch, _ARROW_EXPR_OPS_MAP)
    else:
        raise TypeError(f"Unsupported batch type: {type(batch).__name__}")
