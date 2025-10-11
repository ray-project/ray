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


def _pa_add_or_concat(left, right):
    """Add numeric values or concatenate strings with proper type validation."""
    # Convert scalars to arrays if needed for type checking
    left_is_scalar = isinstance(left, (int, float, str, bool, type(None)))
    right_is_scalar = isinstance(right, (int, float, str, bool, type(None)))

    if left_is_scalar:
        left = pa.scalar(left)
    if right_is_scalar:
        right = pa.scalar(right)

    # Get the type, handling both scalars and arrays
    if hasattr(left, "type"):
        left_type = left.type
    elif isinstance(left, np.ndarray):
        left_type = pa.from_numpy_dtype(left.dtype)
    else:
        left_type = pa.from_numpy_dtype(type(left))

    if hasattr(right, "type"):
        right_type = right.type
    elif isinstance(right, np.ndarray):
        right_type = pa.from_numpy_dtype(right.dtype)
    else:
        right_type = pa.from_numpy_dtype(type(right))

    # Unwrap dictionary-encoded types
    if pa.types.is_dictionary(left_type):
        left_type = left_type.value_type
    if pa.types.is_dictionary(right_type):
        right_type = right_type.value_type

    # Check if either operand is a string type
    is_string_op = (
        pa.types.is_string(left_type)
        or pa.types.is_large_string(left_type)
        or pa.types.is_string(right_type)
        or pa.types.is_large_string(right_type)
    )

    if is_string_op:
        # Use binary_join_element_wise with correct argument order: (*arrays, separator)
        return pc.binary_join_element_wise(left, right, "")
    else:
        return pc.add(left, right)


_ARROW_EXPR_OPS_MAP = {
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
    Operation.IS_NOT_NULL: lambda x: pc.invert(pc.is_null(x)),
}


def _eval_binary_in_operation(
    expr: "BinaryExpr", batch: DataBatch, ops: Dict["Operation", Callable[..., Any]]
) -> Any:
    """Evaluate IN and NOT_IN binary operations.

    Args:
        expr: Binary expression with IN or NOT_IN operation
        batch: Data batch to evaluate against
        ops: Operations map (unused but kept for consistency)

    Returns:
        Boolean result of the membership test
    """
    left_val = _eval_expr_recursive(expr.left, batch, ops)
    right_val = _eval_expr_recursive(expr.right, batch, ops)

    # Handle pandas DataFrames
    if isinstance(batch, pd.DataFrame):
        if isinstance(left_val, pd.Series):
            result = left_val.isin(right_val)
        else:
            # Broadcast scalar value to Series
            result = pd.Series([left_val in right_val] * len(batch))
        return ~result if expr.op == Operation.NOT_IN else result

    # Handle PyArrow Tables
    elif isinstance(batch, pa.Table):
        # Convert right_val to PyArrow array if needed
        if not isinstance(right_val, (pa.Array, pa.ChunkedArray)):
            right_val = pa.array(right_val)
        result = pc.is_in(left_val, right_val)
        return pc.invert(result) if expr.op == Operation.NOT_IN else result


def _eval_unary_null_operation(operand: Any, op: "Operation", batch: DataBatch) -> Any:
    """Evaluate IS_NULL and IS_NOT_NULL unary operations for pandas.

    Args:
        operand: The operand value to check for null
        op: The operation (IS_NULL or IS_NOT_NULL)
        batch: Data batch (used to determine batch size for scalar values)

    Returns:
        Boolean result indicating null/not-null status
    """
    if op == Operation.IS_NULL:
        if isinstance(operand, pd.Series):
            return operand.isna()
        else:
            # Broadcast scalar result to Series
            return pd.Series([pd.isna(operand)] * len(batch))
    elif op == Operation.IS_NOT_NULL:
        if isinstance(operand, pd.Series):
            return operand.notna()
        else:
            # Broadcast scalar result to Series
            return pd.Series([pd.notna(operand)] * len(batch))


def _eval_case_expr_pandas(conditions: list, choices: list, default: Any) -> Any:
    """Evaluate case expression for pandas DataFrames using numpy.select.

    Args:
        conditions: List of boolean condition arrays
        choices: List of value arrays corresponding to conditions
        default: Default value when no conditions match

    Returns:
        Result array with case logic applied
    """
    return np.select(conditions, choices, default=default)


def _eval_case_expr_arrow(conditions: list, choices: list, default: Any) -> Any:
    """Evaluate case expression for PyArrow Tables using case_when.

    Args:
        conditions: List of boolean condition arrays
        choices: List of value arrays corresponding to conditions
        default: Default value when no conditions match

    Returns:
        Result array with case logic applied
    """
    # PyArrow case_when expects:
    # - cond: a struct array of boolean conditions
    # - *cases: the case values (one for each condition, plus default)

    if len(conditions) == 1:
        # Single condition case
        cond_struct = pa.StructArray.from_arrays([conditions[0]], names=["cond0"])
        return pc.case_when(cond_struct, choices[0], default)
    else:
        # Multiple conditions case
        cond_names = [f"cond{i}" for i in range(len(conditions))]
        cond_struct = pa.StructArray.from_arrays(conditions, names=cond_names)
        # Pass all choices plus default as separate arguments
        return pc.case_when(cond_struct, *choices, default)


def _eval_expr_recursive(
    expr: "Expr", batch: DataBatch, ops: Dict["Operation", Callable[..., Any]]
) -> Any:
    """Generic recursive expression evaluator.

    This function recursively evaluates an expression tree against a data batch,
    dispatching to specialized handlers for different expression types.

    Args:
        expr: Expression to evaluate
        batch: Data batch (pandas DataFrame or PyArrow Table)
        ops: Dictionary mapping operations to their implementation functions

    Returns:
        Evaluation result (Series, Array, or scalar)

    Raises:
        TypeError: For unsupported expression types or batch types
    """
    # TODO: Separate unresolved expressions (arbitrary AST with unresolved refs)
    # and resolved expressions (bound to a schema) for better error handling

    # Base cases: column and literal expressions
    if isinstance(expr, ColumnExpr):
        return batch[expr._name]

    if isinstance(expr, LiteralExpr):
        return expr.value

    # Alias expressions: evaluate the underlying expression
    if isinstance(expr, AliasExpr):
        return _eval_expr_recursive(expr.expr, batch, ops)

    # Binary expressions: arithmetic, comparison, and membership operations
    if isinstance(expr, BinaryExpr):
        # Handle IN and NOT_IN operations with specialized logic
        if expr.op in (Operation.IN, Operation.NOT_IN):
            return _eval_binary_in_operation(expr, batch, ops)

        # Standard binary operations
        left_result = _eval_expr_recursive(expr.left, batch, ops)
        right_result = _eval_expr_recursive(expr.right, batch, ops)
        return ops[expr.op](left_result, right_result)

    # Unary expressions: NOT, IS_NULL, IS_NOT_NULL
    if isinstance(expr, UnaryExpr):
        operand = _eval_expr_recursive(expr.operand, batch, ops)

        # Handle IS_NULL and IS_NOT_NULL for pandas with specialized logic
        if expr.op in (Operation.IS_NULL, Operation.IS_NOT_NULL) and isinstance(
            batch, pd.DataFrame
        ):
            return _eval_unary_null_operation(operand, expr.op, batch)

        # Standard unary operations
        return ops[expr.op](operand)

    # Case expressions: conditional branching
    if isinstance(expr, CaseExpr):
        # Evaluate all conditions and values upfront for vectorized processing
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

        # Dispatch to appropriate vectorized implementation
        if isinstance(batch, pd.DataFrame):
            return _eval_case_expr_pandas(conditions, choices, default)
        elif isinstance(batch, pa.Table):
            return _eval_case_expr_arrow(conditions, choices, default)
        else:
            raise TypeError(
                f"Unsupported batch type for CaseExpr: {type(batch).__name__}"
            )

    # When expressions: incomplete case statements
    if isinstance(expr, WhenExpr):
        raise TypeError(
            "WhenExpr cannot be evaluated directly. "
            "Use .otherwise() to complete the case statement."
        )

    # UDF expressions: user-defined functions
    if isinstance(expr, UDFExpr):
        # Evaluate all arguments recursively
        args = [_eval_expr_recursive(arg, batch, ops) for arg in expr.args]
        kwargs = {
            k: _eval_expr_recursive(v, batch, ops) for k, v in expr.kwargs.items()
        }

        # Execute the UDF
        result = expr.fn(*args, **kwargs)

        # Validate return type
        if not isinstance(result, (pd.Series, np.ndarray, pa.Array, pa.ChunkedArray)):
            function_name = expr.fn.__name__
            raise TypeError(
                f"UDF '{function_name}' returned invalid type "
                f"{type(result).__name__}. Expected pandas.Series, numpy.ndarray, "
                f"pyarrow.Array, or pyarrow.ChunkedArray"
            )

        return result

    # Unsupported expression type
    raise TypeError(f"Unsupported expression node: {type(expr).__name__}")


def eval_expr(expr: "Expr", batch: DataBatch) -> Any:
    """Recursively evaluate *expr* against a batch of the appropriate type."""
    if isinstance(batch, pd.DataFrame):
        return _eval_expr_recursive(expr, batch, _PANDAS_EXPR_OPS_MAP)
    elif isinstance(batch, pa.Table):
        return _eval_expr_recursive(expr, batch, _ARROW_EXPR_OPS_MAP)
    else:
        raise TypeError(f"Unsupported batch type: {type(batch).__name__}")
