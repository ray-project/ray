from __future__ import annotations

import ast
import logging
import operator
from typing import Any, Callable, Dict, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from ray.data.block import Block, BlockAccessor, BlockColumn
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

logger = logging.getLogger(__name__)


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


# NOTE: (srinathk) There are 3 distinct stages of handling passed in exprs:
# 1. Parsing it (as text)
# 2. Resolving unbound names (to schema)
# 3. Converting resolved expressions to PA ones
# Need to break up the abstraction provided by ExpressionEvaluator.


class ExpressionEvaluator:
    @staticmethod
    def get_filters(expression: str) -> ds.Expression:
        """Parse and evaluate the expression to generate a filter condition.

        Args:
            expression: A string representing the filter expression to parse.

        Returns:
            A PyArrow compute expression for filtering data.

        """
        try:
            tree = ast.parse(expression, mode="eval")
            return _ConvertToArrowExpressionVisitor().visit(tree.body)
        except SyntaxError as e:
            raise ValueError(f"Invalid syntax in the expression: {expression}") from e
        except Exception as e:
            logger.exception(f"Error processing expression: {e}")
            raise


class _ConvertToArrowExpressionVisitor(ast.NodeVisitor):
    def visit_Compare(self, node: ast.Compare) -> ds.Expression:
        """Handle comparison operations (e.g., a == b, a < b, a in b).

        Args:
            node: The AST node representing a comparison operation.

        Returns:
            An expression representing the comparison.
        """
        # Handle left operand
        # TODO Validate columns
        if isinstance(node.left, ast.Attribute):
            # Visit and handle attributes
            left_expr = self.visit(node.left)
        elif isinstance(node.left, ast.Name):
            # Treat as a simple field
            left_expr = self.visit(node.left)
        elif isinstance(node.left, ast.Constant):
            # Constant values are used directly
            left_expr = node.left.value
        else:
            raise ValueError(f"Unsupported left operand type: {type(node.left)}")

        comparators = [self.visit(comp) for comp in node.comparators]

        op = node.ops[0]
        if isinstance(op, ast.In):
            return left_expr.is_in(comparators[0])
        elif isinstance(op, ast.NotIn):
            return ~left_expr.is_in(comparators[0])
        elif isinstance(op, ast.Eq):
            return left_expr == comparators[0]
        elif isinstance(op, ast.NotEq):
            return left_expr != comparators[0]
        elif isinstance(op, ast.Lt):
            return left_expr < comparators[0]
        elif isinstance(op, ast.LtE):
            return left_expr <= comparators[0]
        elif isinstance(op, ast.Gt):
            return left_expr > comparators[0]
        elif isinstance(op, ast.GtE):
            return left_expr >= comparators[0]
        else:
            raise ValueError(f"Unsupported operator type: {op}")

    def visit_BoolOp(self, node: ast.BoolOp) -> ds.Expression:
        """Handle logical operations (e.g., a and b, a or b).

        Args:
            node: The AST node representing a boolean operation.

        Returns:
            An expression representing the logical operation.
        """
        conditions = [self.visit(value) for value in node.values]
        combined_expr = conditions[0]

        for condition in conditions[1:]:
            if isinstance(node.op, ast.And):
                # Combine conditions with logical AND
                combined_expr &= condition
            elif isinstance(node.op, ast.Or):
                # Combine conditions with logical OR
                combined_expr |= condition
            else:
                raise ValueError(
                    f"Unsupported logical operator: {type(node.op).__name__}"
                )

        return combined_expr

    def visit_Name(self, node: ast.Name) -> ds.Expression:
        """Handle variable (name) nodes and return them as pa.dataset.Expression.

        Even if the name contains periods, it's treated as a single string.

        Args:
            node: The AST node representing a variable.

        Returns:
            The variable wrapped as a pa.dataset.Expression.
        """
        # Directly use the field name as a string (even if it contains periods)
        field_name = node.id
        return pc.field(field_name)

    def visit_Attribute(self, node: ast.Attribute) -> object:
        """Handle attribute access (e.g., np.nan).

        Args:
            node: The AST node representing an attribute access.

        Returns:
            object: The attribute value.

        Raises:
            ValueError: If the attribute is unsupported.
        """
        # Recursively visit the left side (base object or previous attribute)
        if isinstance(node.value, ast.Attribute):
            # If the value is an attribute, recursively resolve it
            left_expr = self.visit(node.value)
            return pc.field(f"{left_expr}.{node.attr}")

        elif isinstance(node.value, ast.Name):
            # If the value is a name (e.g., "foo"), we can directly return the field
            left_name = node.value.id  # The base name, e.g., "foo"
            return pc.field(f"{left_name}.{node.attr}")

        raise ValueError(f"Unsupported attribute: {node.attr}")

    def visit_List(self, node: ast.List) -> ds.Expression:
        """Handle list literals.

        Args:
            node: The AST node representing a list.

        Returns:
            The list of elements wrapped as a pa.dataset.Expression.
        """
        elements = [self.visit(elt) for elt in node.elts]
        return pa.array(elements)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> ds.Expression:
        """Handle case where comparator is UnaryOP (e.g., a == -1).
        AST for this expression will be Compare(left=Name(id='a'), ops=[Eq()],
        comparators=[UnaryOp(op=USub(), operand=Constant(value=1))])

        Args:
            node: The constant value."""

        op = node.op
        if isinstance(op, ast.USub):
            return pc.scalar(-node.operand.value)
        else:
            raise ValueError(f"Unsupported unary operator: {op}")

    # TODO (srinathk) Note that visit_Constant does not return pa.dataset.Expression
    # because to support function in() which takes in a List, the elements in the List
    # needs to values instead of pa.dataset.Expression per pyarrow.dataset.Expression
    # specification. May be down the road, we can update it as Arrow relaxes this
    # constraint.
    def visit_Constant(self, node: ast.Constant) -> object:
        """Handle constant values (e.g., numbers, strings).

        Args:
            node: The AST node representing a constant value.

        Returns:
            object: The constant value itself (e.g., number, string, or boolean).
        """
        return node.value  # Return the constant value directly.

    def visit_Call(self, node: ast.Call) -> ds.Expression:
        """Handle function calls (e.g., is_nan(a), is_valid(b)).

        Args:
            node: The AST node representing a function call.

        Returns:
            The corresponding expression based on the function called.

        Raises:
            ValueError: If the function is unsupported or has incorrect arguments.
        """
        func_name = node.func.id
        function_map = {
            "is_nan": lambda arg: arg.is_nan(),
            "is_null": lambda arg, nan_is_null=False: arg.is_null(
                nan_is_null=nan_is_null
            ),
            "is_valid": lambda arg: arg.is_valid(),
            "is_in": lambda arg1, arg2: arg1.is_in(arg2),
        }

        if func_name in function_map:
            # Visit all arguments of the function call
            args = [self.visit(arg) for arg in node.args]
            # Handle the "is_null" function with one or two arguments
            if func_name == "is_null":
                if len(args) == 1:
                    return function_map[func_name](args[0])
                elif len(args) == 2:
                    return function_map[func_name](args[0], args[1])
                else:
                    raise ValueError("is_null function requires one or two arguments.")
            # Handle the "is_in" function with exactly two arguments
            elif func_name == "is_in" and len(args) != 2:
                raise ValueError("is_in function requires two arguments.")
            # Ensure the function has one argument (for functions like is_valid)
            elif func_name != "is_in" and len(args) != 1:
                raise ValueError(f"{func_name} function requires exactly one argument.")
            # Call the corresponding function with the arguments
            return function_map[func_name](*args)
        else:
            raise ValueError(f"Unsupported function: {func_name}")


class RayDataExpressionEvaluator:
    """Visitor-based expression evaluator that uses Block and BlockColumns

    This evaluator implements the visitor pattern to traverse expression trees
    and evaluate them against Block data structures. It maintains operation
    mappings in shared state and returns consistent BlockColumn types.
    """

    def __init__(self, block: Block):
        """Initialize the evaluator with a block and operation mappings.

        Args:
            block: The Block to evaluate expressions against.
        """
        self.block = block
        self.block_accessor = BlockAccessor.for_block(block)

        # Use BlockAccessor to determine operation mappings
        block_type = self.block_accessor.block_type()
        if block_type.value == "pandas":
            self.ops = _PANDAS_EXPR_OPS_MAP
        elif block_type.value == "arrow":
            self.ops = _ARROW_EXPR_OPS_MAP
        else:
            raise TypeError(f"Unsupported block type: {block_type}")

    def visit(self, expr: Expr) -> Union[BlockColumn, Block]:
        """Visit an expression node and return the evaluated result.

        Args:
            expr: The expression to evaluate.

        Returns:
            The evaluated result as a BlockColumn.
        """
        if isinstance(expr, ColumnExpr):
            return self.visit_column(expr)
        elif isinstance(expr, LiteralExpr):
            return self.visit_literal(expr)
        elif isinstance(expr, BinaryExpr):
            return self.visit_binary(expr)
        elif isinstance(expr, UnaryExpr):
            return self.visit_unary(expr)
        elif isinstance(expr, UDFExpr):
            return self.visit_udf(expr)
        elif isinstance(expr, AliasExpr):
            return self.visit_alias(expr)
        else:
            raise TypeError(f"Unsupported expression node: {type(expr).__name__}")

    def visit_column(self, expr: ColumnExpr) -> BlockColumn:
        """Visit a column expression and return the column data.

        Args:
            expr: The column expression.

        Returns:
            The column data as a BlockColumn.
        """
        return self.block[expr.name]

    def visit_literal(self, expr: LiteralExpr) -> Any:
        """Visit a literal expression and return the literal value.

        Args:
            expr: The literal expression.

        Returns:
            The literal value.
        """
        return expr.value

    def visit_binary(self, expr: BinaryExpr) -> BlockColumn:
        """Visit a binary expression and return the result of the operation.

        Args:
            expr: The binary expression.

        Returns:
            The result of the binary operation as a BlockColumn.
        """
        left_result = self.visit(expr.left)
        right_result = self.visit(expr.right)

        return self.ops[expr.op](left_result, right_result)

    def visit_unary(self, expr: UnaryExpr) -> BlockColumn:
        """Visit a unary expression and return the result of the operation.

        Args:
            expr: The unary expression.

        Returns:
            The result of the unary operation as a BlockColumn.
        """
        operand_result = self.visit(expr.operand)
        return self.ops[expr.op](operand_result)

    def visit_udf(self, expr: UDFExpr) -> BlockColumn:
        """Visit a UDF expression and return the result of the function call.

        Args:
            expr: The UDF expression.

        Returns:
            The result of the UDF call as a BlockColumn.
        """
        args = [self.visit(arg) for arg in expr.args]
        kwargs = {k: self.visit(v) for k, v in expr.kwargs.items()}
        result = expr.fn(*args, **kwargs)

        # Validate return type
        if not isinstance(result, (pd.Series, np.ndarray, pa.Array, pa.ChunkedArray)):
            function_name = expr.fn.__name__
            raise TypeError(
                f"UDF '{function_name}' returned invalid type {type(result).__name__}. "
                f"Expected type (pandas.Series, numpy.ndarray, pyarrow.Array, or pyarrow.ChunkedArray)"
            )

        return result

    def visit_alias(self, expr: AliasExpr) -> BlockColumn:
        """Visit an alias expression and return the renamed result.

        Args:
            expr: The alias expression.

        Returns:
            A Block with the data from the inner expression.
        """
        # Evaluate the inner expression
        return self.visit(expr.expr)


def eval_expr(expr: Expr, block: Block) -> BlockColumn:
    """Evaluate an expression against a block using the visitor pattern.

    Args:
        expr: The expression to evaluate.
        block: The Block to evaluate against.

    Returns:
        The evaluated result as a BlockColumn.
    """
    evaluator = RayDataExpressionEvaluator(block)
    return evaluator.visit(expr)
