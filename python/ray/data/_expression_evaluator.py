from __future__ import annotations

import operator
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from ray.data.block import DataBatch
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    CaseExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    UDFExpr,
    UnaryExpr,
    WhenExpr,
)

_PANDAS_EXPR_OPS_MAP = {
    Operation.ADD: operator.add,
    Operation.SUB: operator.sub,
    Operation.MUL: operator.mul,
    Operation.DIV: operator.truediv,
    Operation.FLOORDIV: operator.floordiv,
    Operation.GT: operator.gt,
    Operation.LT: operator.lt,
    Operation.GE: operator.ge,
    Operation.LE: operator.le,
    Operation.EQ: operator.eq,
    Operation.NE: operator.ne,
    Operation.AND: operator.and_,
    Operation.OR: operator.or_,
    Operation.NOT: operator.not_,
}

_ARROW_EXPR_OPS_MAP = {
    Operation.ADD: lambda left, right: (
        pc.binary_join_element_wise(left, "", right)
        if (
            pa.types.is_string(left.type)
            or pa.types.is_large_string(left.type)
            or pa.types.is_string(right.type)
            or pa.types.is_large_string(right.type)
        )
        else pc.add(left, right)
    ),
    Operation.SUB: pc.subtract,
    Operation.MUL: pc.multiply,
    Operation.DIV: pc.divide,
    Operation.FLOORDIV: lambda left, right: pc.floor(pc.divide(left, right)),
    Operation.GT: pc.greater,
    Operation.LT: pc.less,
    Operation.GE: pc.greater_equal,
    Operation.LE: pc.less_equal,
    Operation.EQ: pc.equal,
    Operation.NE: pc.not_equal,
    Operation.AND: pc.and_kleene,
    Operation.OR: pc.or_kleene,
    Operation.NOT: pc.invert,
    Operation.IS_NULL: pc.is_null,
    Operation.IS_NOT_NULL: lambda x: pc.invert(pc.is_null(x)),
}


def _eval_expr_recursive(
    expr: "Expr", batch: DataBatch, ops: Dict["Operation", Callable[..., Any]]
) -> Any:
    """Generic recursive expression evaluator."""
    # TODO: Separate unresolved expressions (arbitrary AST with unresolved refs)
    # and resolved expressions (bound to a schema) for better error handling

    if isinstance(expr, ColumnExpr):
        return batch[expr._name]
    if isinstance(expr, LiteralExpr):
        return expr.value
    if isinstance(expr, AliasExpr):
        # For alias expressions, evaluate the underlying expression
        return _eval_expr_recursive(expr.expr, batch, ops)
    if isinstance(expr, BinaryExpr):
        # Handle IN and NOT_IN operations specially
        if expr.op in (Operation.IN, Operation.NOT_IN):
            left_val = _eval_expr_recursive(expr.left, batch, ops)
            right_val = _eval_expr_recursive(expr.right, batch, ops)

            # For pandas, use isin()
            if isinstance(batch, pd.DataFrame):
                if isinstance(left_val, pd.Series):
                    result = left_val.isin(right_val)
                else:
                    # Scalar value
                    result = pd.Series([left_val in right_val] * len(batch))
                return ~result if expr.op == Operation.NOT_IN else result
            # For Arrow, use is_in()
            elif isinstance(batch, pa.Table):
                if not isinstance(right_val, (pa.Array, pa.ChunkedArray)):
                    right_val = pa.array(right_val)
                result = pc.is_in(left_val, right_val)
                return pc.invert(result) if expr.op == Operation.NOT_IN else result

        return ops[expr.op](
            _eval_expr_recursive(expr.left, batch, ops),
            _eval_expr_recursive(expr.right, batch, ops),
        )

    if isinstance(expr, UnaryExpr):
        operand = _eval_expr_recursive(expr.operand, batch, ops)

        # Handle IS_NULL and IS_NOT_NULL for pandas
        if expr.op == Operation.IS_NULL and isinstance(batch, pd.DataFrame):
            if isinstance(operand, pd.Series):
                return operand.isna()
            else:
                return pd.Series([pd.isna(operand)] * len(batch))
        elif expr.op == Operation.IS_NOT_NULL and isinstance(batch, pd.DataFrame):
            if isinstance(operand, pd.Series):
                return operand.notna()
            else:
                return pd.Series([pd.notna(operand)] * len(batch))

        return ops[expr.op](operand)

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

        # Handle edge case: no when clauses (just return default)
        if not conditions:
            return default

        # Use appropriate vectorized operation based on batch type
        if isinstance(batch, pd.DataFrame):
            # For pandas, use numpy.select which handles Series efficiently
            return np.select(conditions, choices, default=default)
        elif isinstance(batch, pa.Table):
            # For Arrow, use pyarrow.compute.case_when
            # PyArrow case_when expects:
            # - cond: a struct array of boolean conditions
            # - *cases: the case values (one for each condition, plus default)

            # Create a struct array from the conditions
            if len(conditions) == 1:
                # Single condition case
                cond_struct = pa.StructArray.from_arrays(
                    [conditions[0]], names=["cond0"]
                )
                return pc.case_when(cond_struct, choices[0], default)
            else:
                # Multiple conditions case
                cond_names = [f"cond{i}" for i in range(len(conditions))]
                cond_struct = pa.StructArray.from_arrays(conditions, names=cond_names)
                # Pass all choices plus default as separate arguments
                return pc.case_when(cond_struct, *choices, default)
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

    if isinstance(expr, UDFExpr):
        args = [_eval_expr_recursive(arg, batch, ops) for arg in expr.args]
        kwargs = {
            k: _eval_expr_recursive(v, batch, ops) for k, v in expr.kwargs.items()
        }
        result = expr.fn(*args, **kwargs)

        # Can't perform type validation for unions if python version is < 3.10
        if not isinstance(result, (pd.Series, np.ndarray, pa.Array, pa.ChunkedArray)):
            function_name = expr.fn.__name__
            raise TypeError(
                f"UDF '{function_name}' returned invalid type {type(result).__name__}. "
                f"Expected type (pandas.Series, numpy.ndarray, pyarrow.Array, or pyarrow.ChunkedArray)"
            )

        return result

    raise TypeError(f"Unsupported expression node: {type(expr).__name__}")


def eval_expr(expr: "Expr", batch: DataBatch) -> Any:
    """Recursively evaluate *expr* against a batch of the appropriate type."""
    if isinstance(batch, pd.DataFrame):
        return _eval_expr_recursive(expr, batch, _PANDAS_EXPR_OPS_MAP)
    elif isinstance(batch, pa.Table):
        return _eval_expr_recursive(expr, batch, _ARROW_EXPR_OPS_MAP)
    else:
        raise TypeError(f"Unsupported batch type: {type(batch).__name__}")
