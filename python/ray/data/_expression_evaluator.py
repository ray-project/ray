from __future__ import annotations

import operator
from typing import Any, Callable, Dict, TYPE_CHECKING

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

# Use TYPE_CHECKING imports to avoid circular imports
if TYPE_CHECKING:
    from ray.data.expressions import BinaryExpr, ColumnExpr, Expr, LiteralExpr, Operation # noqa: F401

def _get_operation_maps():
    """Get operation maps, importing Operation enum at runtime to avoid circular imports."""
    from ray.data.expressions import Operation

    pandas_ops = {
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

    numpy_ops = {
        Operation.ADD: np.add,
        Operation.SUB: np.subtract,
        Operation.MUL: np.multiply,
        Operation.DIV: np.divide,
        Operation.GT: np.greater,
        Operation.LT: np.less,
        Operation.GE: np.greater_equal,
        Operation.LE: np.less_equal,
        Operation.EQ: np.equal,
        Operation.AND: np.logical_and,
        Operation.OR: np.logical_or,
    }

    arrow_ops = {
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

    return pandas_ops, numpy_ops, arrow_ops


def _eval_expr_recursive(expr: "Expr", batch, ops: Dict["Operation", Callable]) -> Any:
    """Generic recursive expression evaluator."""
    # Import classes at runtime to avoid circular imports
    from ray.data.expressions import BinaryExpr, ColumnExpr, LiteralExpr

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
    raise TypeError(f"Unsupported expression node: {type(expr).__name__}")


def eval_expr(expr: "Expr", batch) -> Any:
    """Recursively evaluate *expr* against a batch of the appropriate type."""
    pandas_ops, numpy_ops, arrow_ops = _get_operation_maps()

    if isinstance(batch, pd.DataFrame):
        return _eval_expr_recursive(expr, batch, pandas_ops)
    elif isinstance(batch, (np.ndarray, dict)):
        return _eval_expr_recursive(expr, batch, numpy_ops)
    elif isinstance(batch, pa.Table):
        return _eval_expr_recursive(expr, batch, arrow_ops)
    raise TypeError(f"Unsupported batch type: {type(batch).__name__}")
