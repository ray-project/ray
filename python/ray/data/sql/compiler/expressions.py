"""Expression compilation for Ray Data SQL API.

This module compiles SQLGlot expressions into executable Python functions
that can be used with Ray Dataset operations.
"""

import operator
from typing import Any, Callable, Dict, Mapping

from sqlglot import exp

from ray.data.sql.exceptions import SchemaError, UnsupportedOperationError


class ExpressionCompiler:
    """Compiles SQLGlot expressions into executable Python functions.

    This class converts SQL expressions into Python callables that can be
    used with Ray Dataset operations like map(), filter(), and aggregate().
    """

    # Comparison operators mapping
    COMPARISON_OPS = {
        exp.EQ: operator.eq,
        exp.NEQ: operator.ne,
        exp.GT: operator.gt,
        exp.LT: operator.lt,
        exp.GTE: operator.ge,
        exp.LTE: operator.le,
        exp.Is: lambda a, b: a is b,
        exp.Null: lambda a, b: a is None,
    }

    # Arithmetic operators mapping
    ARITHMETIC_OPS = {
        exp.Add: operator.add,
        exp.Sub: operator.sub,
        exp.Mul: operator.mul,
        exp.Div: lambda a, b: float(a) / float(b) if b != 0 else None,
        exp.Mod: operator.mod,
    }

    # Logical operators mapping
    LOGICAL_OPS = {
        exp.And: lambda a, b: bool(a and b),
        exp.Or: lambda a, b: bool(a or b),
        exp.Not: operator.not_,
    }

    # String functions mapping
    STRING_FUNCTIONS = {
        exp.Upper: str.upper,
        exp.Lower: str.lower,
    }

    # Date/time functions mapping
    DATETIME_FUNCTIONS = {
        exp.Year: lambda d: d.year if hasattr(d, "year") else None,
        exp.Month: lambda d: d.month if hasattr(d, "month") else None,
        exp.Day: lambda d: d.day if hasattr(d, "day") else None,
    }

    @classmethod
    def compile(cls, expr: exp.Expression) -> Callable[[Mapping[str, Any]], Any]:
        """Compile a SQLGlot expression into a Python function.

        Args:
            expr: SQLGlot expression to compile.

        Returns:
            Callable function that takes a row dict and returns the computed value.

        Raises:
            UnsupportedOperationError: If the expression type is not supported.
        """
        expr_type = type(expr)

        # Column references
        if isinstance(expr, exp.Column):
            col_name = str(expr.name)
            return lambda row: row.get(col_name)

        # Table-qualified column references
        if isinstance(expr, exp.Column) and expr.table:
            col_name = str(expr.name)
            return lambda row: row.get(col_name)  # For now, ignore table qualification

        # Literal values
        if isinstance(expr, exp.Literal):
            value = expr.this
            return lambda _: value

        # Comparison operators
        if expr_type in cls.COMPARISON_OPS:
            return cls._compile_comparison(expr)

        # Arithmetic operators
        if expr_type in cls.ARITHMETIC_OPS:
            return cls._compile_arithmetic(expr)

        # Logical operators
        if expr_type in cls.LOGICAL_OPS:
            return cls._compile_logical(expr)

        # String functions
        if expr_type in cls.STRING_FUNCTIONS:
            return cls._compile_string_function(expr)

        # Date/time functions
        if expr_type in cls.DATETIME_FUNCTIONS:
            return cls._compile_datetime_function(expr)

        # Aggregate functions
        if expr_type in {exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max}:
            return cls._compile_aggregate(expr)

        # Special expressions
        if isinstance(expr, exp.Between):
            return cls._compile_between(expr)

        if isinstance(expr, exp.In):
            return cls._compile_in(expr)

        if isinstance(expr, exp.Like):
            return cls._compile_like(expr)

        if isinstance(expr, exp.Cast):
            return cls._compile_cast(expr)

        # Parenthesized expressions
        if isinstance(expr, exp.Paren):
            return cls.compile(expr.this)

        # Unsupported expression
        raise UnsupportedOperationError(
            f"Expression type {expr_type.__name__}",
            suggestion="Check the documentation for supported SQL constructs",
        )

    @classmethod
    def _compile_comparison(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], bool]:
        """Compile comparison expressions.

        Args:
            expr: Comparison expression.

        Returns:
            Function that performs the comparison.
        """
        left_func = cls.compile(expr.left)
        right_func = cls.compile(expr.right)
        op = cls.COMPARISON_OPS[type(expr)]

        def compare_func(row: Mapping[str, Any]) -> bool:
            left_val = left_func(row)
            right_val = right_func(row)

            # Handle NULL comparisons
            if left_val is None or right_val is None:
                return False

            try:
                return op(left_val, right_val)
            except TypeError:
                raise SchemaError(
                    f"Cannot compare {type(left_val).__name__} and {type(right_val).__name__}"
                )

        return compare_func

    @classmethod
    def _compile_arithmetic(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile arithmetic expressions.

        Args:
            expr: Arithmetic expression.

        Returns:
            Function that performs the arithmetic operation.
        """
        left_func = cls.compile(expr.left)
        right_func = cls.compile(expr.right)
        op = cls.ARITHMETIC_OPS[type(expr)]

        def arithmetic_func(row: Mapping[str, Any]) -> Any:
            left_val = left_func(row)
            right_val = right_func(row)

            # Handle NULL values
            if left_val is None or right_val is None:
                return None

            try:
                return op(left_val, right_val)
            except (TypeError, ValueError, ZeroDivisionError) as e:
                if isinstance(expr, exp.Div) and right_val == 0:
                    return None  # Division by zero returns NULL
                raise SchemaError(f"Arithmetic error: {str(e)}")

        return arithmetic_func

    @classmethod
    def _compile_logical(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], bool]:
        """Compile logical expressions.

        Args:
            expr: Logical expression.

        Returns:
            Function that performs the logical operation.
        """
        if isinstance(expr, exp.Not):
            func = cls.compile(expr.this)
            op = cls.LOGICAL_OPS[type(expr)]

            def not_func(row: Mapping[str, Any]) -> bool:
                val = func(row)
                return op(val)

            return not_func
        else:
            left_func = cls.compile(expr.left)
            right_func = cls.compile(expr.right)
            op = cls.LOGICAL_OPS[type(expr)]

            def logical_func(row: Mapping[str, Any]) -> bool:
                left_val = left_func(row)
                right_val = right_func(row)
                return op(left_val, right_val)

            return logical_func

    @classmethod
    def _compile_string_function(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile string function expressions.

        Args:
            expr: String function expression.

        Returns:
            Function that performs the string operation.
        """
        func_type = type(expr)
        func = cls.STRING_FUNCTIONS[func_type]

        if hasattr(expr, "this"):
            arg_func = cls.compile(expr.this)

            def string_func(row: Mapping[str, Any]) -> Any:
                arg = arg_func(row)
                if arg is None:
                    return None
                return func(arg)

            return string_func
        else:
            return lambda _: None

    @classmethod
    def _compile_datetime_function(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile date/time function expressions.

        Args:
            expr: Date/time function expression.

        Returns:
            Function that performs the date/time operation.
        """
        func_type = type(expr)
        func = cls.DATETIME_FUNCTIONS[func_type]

        if hasattr(expr, "this"):
            arg_func = cls.compile(expr.this)

            def datetime_func(row: Mapping[str, Any]) -> Any:
                arg = arg_func(row)
                if arg is None:
                    return None
                return func(arg)

            return datetime_func
        else:
            return lambda _: None

    @classmethod
    def _compile_aggregate(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile aggregate function expressions.

        Args:
            expr: Aggregate function expression.

        Returns:
            Function that returns the aggregate function name for later processing.
        """
        func_name = type(expr).__name__.lower()

        if hasattr(expr, "this"):
            arg_func = cls.compile(expr.this)

            def aggregate_func(row: Mapping[str, Any]) -> Dict[str, Any]:
                arg = arg_func(row)
                return {"function": func_name, "argument": arg, "type": "aggregate"}

            return aggregate_func
        else:
            return lambda _: {
                "function": func_name,
                "argument": None,
                "type": "aggregate",
            }

    @classmethod
    def _compile_between(cls, expr: exp.Between) -> Callable[[Mapping[str, Any]], bool]:
        """Compile BETWEEN expressions.

        Args:
            expr: BETWEEN expression.

        Returns:
            Function that checks if a value is between two bounds.
        """
        value_func = cls.compile(expr.this)
        low_func = cls.compile(expr.args.get("low"))
        high_func = cls.compile(expr.args.get("high"))

        def between_func(row: Mapping[str, Any]) -> bool:
            value = value_func(row)
            low = low_func(row)
            high = high_func(row)

            if any(v is None for v in [value, low, high]):
                return False

            try:
                return low <= value <= high
            except TypeError:
                raise SchemaError("Cannot compare values in BETWEEN clause")

        return between_func

    @classmethod
    def _compile_in(cls, expr: exp.In) -> Callable[[Mapping[str, Any]], bool]:
        """Compile IN expressions.

        Args:
            expr: IN expression.

        Returns:
            Function that checks if a value is in a list.
        """
        value_func = cls.compile(expr.this)

        # Extract the list of values from the IN clause
        if hasattr(expr, "expressions"):
            values = [cls.compile(e) for e in expr.expressions]
        else:
            values = []

        def in_func(row: Mapping[str, Any]) -> bool:
            value = value_func(row)
            if value is None:
                return False

            try:
                return any(val_func(row) == value for val_func in values)
            except TypeError:
                return False

        return in_func

    @classmethod
    def _compile_like(cls, expr: exp.Like) -> Callable[[Mapping[str, Any]], bool]:
        """Compile LIKE expressions.

        Args:
            expr: LIKE expression.

        Returns:
            Function that performs pattern matching.
        """
        value_func = cls.compile(expr.this)
        pattern_func = cls.compile(expr.args.get("pattern"))

        def like_func(row: Mapping[str, Any]) -> bool:
            value = value_func(row)
            pattern = pattern_func(row)

            if value is None or pattern is None:
                return False

            try:
                # Convert SQL LIKE pattern to regex
                import re

                regex_pattern = pattern.replace("%", ".*").replace("_", ".")
                return bool(re.match(regex_pattern, str(value)))
            except (TypeError, re.error):
                return False

        return like_func

    @classmethod
    def _compile_cast(cls, expr: exp.Cast) -> Callable[[Mapping[str, Any]], Any]:
        """Compile CAST expressions.

        Args:
            expr: CAST expression.

        Returns:
            Function that performs type casting.
        """
        value_func = cls.compile(expr.this)
        target_type = str(expr.args.get("to")).upper()

        def cast_func(row: Mapping[str, Any]) -> Any:
            value = value_func(row)
            if value is None:
                return None

            try:
                if target_type == "INTEGER" or target_type == "INT":
                    return int(value)
                elif target_type == "FLOAT" or target_type == "DOUBLE":
                    return float(value)
                elif target_type == "STRING" or target_type == "VARCHAR":
                    return str(value)
                elif target_type == "BOOLEAN" or target_type == "BOOL":
                    return bool(value)
                else:
                    # For unsupported types, return the original value
                    return value
            except (ValueError, TypeError):
                return None

        return cast_func
