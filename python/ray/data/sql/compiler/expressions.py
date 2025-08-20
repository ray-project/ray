"""
Expression compilation for Ray Data SQL API.

This module provides the ExpressionCompiler class for converting SQLGlot
expressions into Python callables that can evaluate expressions against row data.
"""

from typing import Any, Callable, Dict

from sqlglot import exp

from ray.data.sql.config import SQLConfig
from ray.data.sql.utils import (
    parse_literal_value,
    safe_divide,
    safe_get_column,
    setup_logger,
)


class ExpressionCompiler:
    """Compiles SQLGlot expressions into Python callables for row evaluation.

    The ExpressionCompiler is a critical component that bridges SQL expressions
    and Ray Dataset operations. It takes SQLGlot AST expressions and converts them
    into Python lambda functions that can be applied to dataset rows.

    This compiler handles a wide range of SQL expression types:
    - Column references (name, table.name)
    - Literal values (strings, numbers, booleans, null)
    - Arithmetic operations (+, -, *, /, %)
    - Comparison operations (=, <>, <, >, <=, >=)
    - Logical operations (AND, OR, NOT)
    - String functions (UPPER, LOWER, LENGTH, etc.)
    - Conditional expressions (CASE WHEN)
    - Null checking (IS NULL, IS NOT NULL)

    The compiled functions operate on row dictionaries and return the computed
    values, enabling efficient distributed expression evaluation in Ray.
    """

    def __init__(self, config: SQLConfig):
        """Initialize the expression compiler with configuration.

        Args:
            config: SQL configuration affecting compilation behavior (case sensitivity, etc.).
        """
        # Store configuration for expression compilation behavior
        self.config = config

        # Logger for debugging expression compilation issues
        self._logger = setup_logger("ExpressionCompiler")

    def compile(self, expr: exp.Expression) -> Callable[[Dict[str, Any]], Any]:
        """Compile a SQLGlot expression into a Python callable function.

        This is the main public interface for expression compilation. It takes
        any SQLGlot expression and returns a function that can evaluate that
        expression against row data.

        Args:
            expr: SQLGlot expression AST node to compile.

        Returns:
            Python callable that takes a row dictionary and returns the computed value.
        """
        return self._compile_expression(expr)

    def _compile_expression(
        self, expr: exp.Expression
    ) -> Callable[[Dict[str, Any]], Any]:
        """Main expression compilation method with comprehensive coverage.

        This method dispatches different expression types to specialized compilation
        methods. It handles the full spectrum of SQL expressions by pattern matching
        on SQLGlot AST node types and calling appropriate compilation handlers.

        Args:
            expr: SQLGlot expression to compile.

        Returns:
            Callable function that evaluates the expression on row data.
        """
        # Column reference (e.g., "name", "users.id")
        if isinstance(expr, exp.Column):
            return self._compile_column(expr)

        # Literal values (strings, numbers, booleans, null)
        if self._is_literal(expr):
            return self._compile_literal(expr)

        # Arithmetic operations (+, -, *, /, %)
        if self._is_arithmetic_operation(expr):
            return self._compile_arithmetic(expr)

        # Comparison operations (=, <>, <, >, <=, >=)
        if self._is_comparison_operation(expr):
            return self._compile_comparison(expr)

        # Logical operations (AND, OR)
        if self._is_logical_operation(expr):
            return self._compile_logical(expr)

        # String functions (UPPER, LOWER, LENGTH, etc.)
        if self._is_string_function(expr):
            return self._compile_string_function(expr)

        # CASE WHEN expressions (conditional logic)
        if isinstance(expr, exp.Case):
            return self._compile_case(expr)

        # IS NULL / IS NOT NULL checks
        if isinstance(expr, exp.Is):
            return self._compile_is_null(expr)

        # NOT expressions (logical negation)
        if isinstance(expr, exp.Not):
            # Compile the inner expression and negate its result
            inner = self._compile_expression(expr.this)
            return lambda row: not inner(row)

        # Parenthesized expressions (just compile the inner expression)
        if isinstance(expr, exp.Paren):
            return self._compile_expression(expr.this)

        # Fallback for unsupported expressions - log warning and return null function
        self._logger.warning(f"Unsupported expression type: {type(expr).__name__}")
        return lambda row: None

    def _compile_column(self, expr: exp.Column) -> Callable[[Dict[str, Any]], Any]:
        """Compile column reference expressions."""
        col_name = str(expr.name)
        # Handle qualified column names (table.column) by extracting just the column part
        if "." in col_name:
            col_name = col_name.split(".")[-1]
        return lambda row: safe_get_column(row, col_name, self.config.case_sensitive)

    def _is_literal(self, expr: exp.Expression) -> bool:
        """Check if expression is a literal value."""
        return isinstance(expr, (exp.Literal, exp.Boolean)) or (
            hasattr(exp, "Null") and isinstance(expr, exp.Null)
        )

    def _compile_literal(self, expr: exp.Expression) -> Callable[[Dict[str, Any]], Any]:
        """Compile literal values."""
        if isinstance(expr, exp.Literal):
            value = parse_literal_value(expr)
            return lambda row: value
        elif hasattr(exp, "Boolean") and isinstance(expr, exp.Boolean):
            value = str(expr.name).lower() == "true"
            return lambda row: value
        elif hasattr(exp, "Null") and isinstance(expr, exp.Null):
            return lambda row: None
        else:
            raise NotImplementedError(
                f"Unsupported literal type: {type(expr).__name__}"
            )

    def _is_arithmetic_operation(self, expr: exp.Expression) -> bool:
        """Check if expression is an arithmetic operation."""
        return isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod))

    def _compile_arithmetic(
        self, expr: exp.Expression
    ) -> Callable[[Dict[str, Any]], Any]:
        """Compile arithmetic operations."""
        left_func = self._compile_expression(expr.left)
        right_func = self._compile_expression(expr.right)

        if isinstance(expr, exp.Add):
            return lambda row: left_func(row) + right_func(row)
        elif isinstance(expr, exp.Sub):
            return lambda row: left_func(row) - right_func(row)
        elif isinstance(expr, exp.Mul):
            return lambda row: left_func(row) * right_func(row)
        elif isinstance(expr, exp.Div):
            return lambda row: safe_divide(left_func(row), right_func(row))
        elif isinstance(expr, exp.Mod):
            return lambda row: (
                left_func(row) % right_func(row) if right_func(row) != 0 else None
            )
        else:
            raise NotImplementedError(
                f"Unsupported arithmetic operation: {type(expr).__name__}"
            )

    def _is_comparison_operation(self, expr: exp.Expression) -> bool:
        """Check if expression is a comparison operation."""
        return isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE))

    def _compile_comparison(
        self, expr: exp.Expression
    ) -> Callable[[Dict[str, Any]], Any]:
        """Compile comparison operations."""
        left_func = self._compile_expression(expr.left)
        right_func = self._compile_expression(expr.right)

        if isinstance(expr, exp.EQ):
            return lambda row: left_func(row) == right_func(row)
        elif isinstance(expr, exp.NEQ):
            return lambda row: left_func(row) != right_func(row)
        elif isinstance(expr, exp.GT):
            return lambda row: left_func(row) > right_func(row)
        elif isinstance(expr, exp.GTE):
            return lambda row: left_func(row) >= right_func(row)
        elif isinstance(expr, exp.LT):
            return lambda row: left_func(row) < right_func(row)
        elif isinstance(expr, exp.LTE):
            return lambda row: left_func(row) <= right_func(row)
        else:
            raise NotImplementedError(
                f"Unsupported comparison operation: {type(expr).__name__}"
            )

    def _is_logical_operation(self, expr: exp.Expression) -> bool:
        """Check if expression is a logical operation."""
        return isinstance(expr, (exp.And, exp.Or))

    def _compile_logical(self, expr: exp.Expression) -> Callable[[Dict[str, Any]], Any]:
        """Compile logical operations."""
        if isinstance(expr, exp.And):
            left_func = self._compile_expression(expr.left)
            right_func = self._compile_expression(expr.right)
            return lambda row: left_func(row) and right_func(row)
        elif isinstance(expr, exp.Or):
            left_func = self._compile_expression(expr.left)
            right_func = self._compile_expression(expr.right)
            return lambda row: left_func(row) or right_func(row)
        else:
            raise NotImplementedError(
                f"Unsupported logical operation: {type(expr).__name__}"
            )

    def _is_string_function(self, expr: exp.Expression) -> bool:
        """Check if expression is a string function."""
        return isinstance(expr, (exp.Upper, exp.Lower))

    def _compile_string_function(
        self, expr: exp.Expression
    ) -> Callable[[Dict[str, Any]], Any]:
        """Compile string functions."""
        if isinstance(expr, exp.Upper):
            operand_func = self._compile_expression(expr.this)
            return lambda row: (
                str(operand_func(row)).upper()
                if operand_func(row) is not None
                else None
            )
        elif isinstance(expr, exp.Lower):
            operand_func = self._compile_expression(expr.this)
            return lambda row: (
                str(operand_func(row)).lower()
                if operand_func(row) is not None
                else None
            )
        else:
            raise NotImplementedError(
                f"Unsupported string function: {type(expr).__name__}"
            )

    def _compile_case(self, case_expr: exp.Case) -> Callable[[Dict[str, Any]], Any]:
        """Compile a CASE expression into a Python function."""
        conditions = []
        for when_clause in case_expr.args.get("ifs", []):
            condition_func = self._compile_expression(when_clause.this)
            value_func = self._compile_expression(when_clause.args["true"])
            conditions.append((condition_func, value_func))

        else_func = None
        if case_expr.args.get("default"):
            else_func = self._compile_expression(case_expr.args["default"])

        def evaluate_case(row: Dict[str, Any]) -> Any:
            for condition_func, value_func in conditions:
                if condition_func(row):
                    return value_func(row)
            return else_func(row) if else_func else None

        return evaluate_case

    def _compile_is_null(self, is_expr: exp.Is) -> Callable[[Dict[str, Any]], Any]:
        """Compile IS NULL / IS NOT NULL expressions."""
        operand_func = self._compile_expression(is_expr.this)
        is_not = bool(is_expr.args.get("not"))

        def evaluate_is_null(row: Dict[str, Any]) -> bool:
            value = operand_func(row)
            is_null = value is None
            return not is_null if is_not else is_null

        return evaluate_is_null

    # Backward compatibility method
    def _compile_lambda(self, expr: exp.Expression):
        """Legacy method for backward compatibility."""
        return self._compile_expression(expr)

    def _parse_literal(self, literal: exp.Literal) -> Any:
        """Legacy method for backward compatibility."""
        return parse_literal_value(literal)

    def _safe_divide(self, a: Any, b: Any) -> float:
        """Legacy method for backward compatibility."""
        return safe_divide(a, b)
