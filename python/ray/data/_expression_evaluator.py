from __future__ import annotations

import operator
from typing import Any, Callable, Dict

import numpy as np
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
        # Evaluate case statement using vectorized operations for batch processing
        # For pandas: use numpy.select for efficient vectorized evaluation
        # For Arrow: use pyarrow.compute.case_when for efficient vectorized evaluation

        # Evaluate all conditions and values first
        conditions = [
            _eval_expr_recursive(condition, batch, ops)
            for condition, _ in expr.when_clauses
        ]
        choices = [
            _eval_expr_recursive(value, batch, ops) for _, value in expr.when_clauses
        ]
        default = _eval_expr_recursive(expr.default, batch, ops)

        # Use appropriate vectorized operation based on batch type
        if isinstance(batch, pd.DataFrame):
            # For pandas, use numpy.select which handles Series efficiently
            return np.select(conditions, choices, default=default)
        elif isinstance(batch, pa.Table):
            # For Arrow, use pyarrow.compute.case_when
            # Convert to arrays if needed and use case_when
            return pc.case_when(conditions, choices, default=default)
        else:
            # Fallback for other types (should not happen in practice)
            raise TypeError(
                f"Unsupported batch type for CaseExpr: {type(batch).__name__}"
            )

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
