from __future__ import annotations

import operator
from typing import Any, Callable, Dict, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from ray.data.block import DataBatch
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    UDFExpr,
    UnaryExpr,
)


def _pa_is_in(left: Any, right: Any) -> Any:
    if not isinstance(right, (pa.Array, pa.ChunkedArray)):
        right = pa.array(right.as_py() if isinstance(right, pa.Scalar) else right)
    return pc.is_in(left, right)


_PANDAS_EXPR_OPS_MAP: Dict[Operation, Callable[..., Any]] = {
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
    Operation.IS_NULL: pd.isna,
    Operation.IS_NOT_NULL: pd.notna,
    Operation.IN: lambda left, right: left.is_in(right),
    Operation.NOT_IN: lambda left, right: ~left.is_in(right),
}


def _is_pa_string_type(t: pa.DataType) -> bool:
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def _is_pa_string_like(x: Union[pa.Array, pa.ChunkedArray]) -> bool:
    t = x.type
    if pa.types.is_dictionary(t):
        t = t.value_type
    return _is_pa_string_type(t)


def _pa_decode_dict_string_array(x: Union[pa.Array, pa.ChunkedArray]) -> Any:
    """Convert Arrow dictionary-encoded string arrays to regular string arrays.

    Dictionary encoding stores strings as indices into a dictionary of unique values.
    This function converts them back to regular string arrays for string operations.

    Example:
        # Input: pa.array(['a', 'b']).dictionary_encode()
        #   -- dictionary: ["a", "b"]
        #   -- indices: [0, 1]
        # Output: regular string array ["a", "b"]
    Args:
        x: The input array to convert.
    Returns:
        The converted string array.
    """
    if pa.types.is_dictionary(x.type) and _is_pa_string_type(x.type.value_type):
        return pc.cast(x, pa.string())
    return x


def _to_pa_string_input(x: Any) -> Any:
    if isinstance(x, str):
        return pa.scalar(x)
    elif _is_pa_string_like(x) and isinstance(x, (pa.Array, pa.ChunkedArray)):
        x = _pa_decode_dict_string_array(x)
    else:
        raise
    return x


def _pa_add_or_concat(left: Any, right: Any) -> Any:
    # If either side is string-like, perform string concatenation.
    if (
        isinstance(left, str)
        or isinstance(right, str)
        or (isinstance(left, (pa.Array, pa.ChunkedArray)) and _is_pa_string_like(left))
        or (
            isinstance(right, (pa.Array, pa.ChunkedArray)) and _is_pa_string_like(right)
        )
    ):
        left_input = _to_pa_string_input(left)
        right_input = _to_pa_string_input(right)
        return pc.binary_join_element_wise(left_input, right_input, "")
    return pc.add(left, right)


_ARROW_EXPR_OPS_MAP: Dict[Operation, Callable[..., Any]] = {
    Operation.ADD: _pa_add_or_concat,
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
    Operation.IS_NOT_NULL: pc.is_valid,
    Operation.IN: _pa_is_in,
    Operation.NOT_IN: lambda left, right: pc.invert(_pa_is_in(left, right)),
}


def _eval_expr_recursive(
    expr: "Expr", batch: DataBatch, ops: Dict["Operation", Callable[..., Any]]
) -> Any:
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
    if isinstance(expr, UnaryExpr):
        # TODO: Use Visitor pattern here and store ops in shared state.
        return ops[expr.op](_eval_expr_recursive(expr.operand, batch, ops))

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

    if isinstance(expr, AliasExpr):
        # The renaming of the column is handled in the project op planner stage.
        return _eval_expr_recursive(expr.expr, batch, ops)

    raise TypeError(f"Unsupported expression node: {type(expr).__name__}")


def eval_expr(expr: "Expr", batch: DataBatch) -> Any:
    """Recursively evaluate *expr* against a batch of the appropriate type."""
    if isinstance(batch, pd.DataFrame):
        return _eval_expr_recursive(expr, batch, _PANDAS_EXPR_OPS_MAP)
    elif isinstance(batch, pa.Table):
        return _eval_expr_recursive(expr, batch, _ARROW_EXPR_OPS_MAP)
    else:
        raise TypeError(f"Unsupported batch type: {type(batch).__name__}")
