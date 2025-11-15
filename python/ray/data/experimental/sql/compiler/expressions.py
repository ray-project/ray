"""Convert SQLGlot expressions to Ray Data expressions.

This module provides the ExpressionConverter class that translates SQLGlot
AST expressions into Ray Data's native expression API.

SQLGlot: SQL parser, transpiler, and optimizer
https://github.com/tobymao/sqlglot
"""

from typing import Any, Optional

from sqlglot import exp

from ray.data.experimental.sql.exceptions import UnsupportedOperationError
from ray.data.expressions import col, lit
from ray.data.expressions import Expr


class ExpressionConverter:
    """Converts SQLGlot expressions to Ray Data expressions."""

    def __init__(self, config: Optional[Any] = None) -> None:
        self.config = config

    def convert(self, sqlglot_expr: exp.Expression) -> Expr:
        """Convert a SQLGlot expression to a Ray Data expression."""
        if isinstance(sqlglot_expr, exp.Column):
            col_name = str(sqlglot_expr.name)
            if "." in col_name:
                col_name = col_name.split(".")[-1]
            return col(col_name)

        if isinstance(sqlglot_expr, exp.Literal):
            value = self._parse_literal(sqlglot_expr)
            return lit(value)

        if isinstance(sqlglot_expr, exp.EQ):
            return self.convert(sqlglot_expr.left) == self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.NEQ):
            return self.convert(sqlglot_expr.left) != self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.GT):
            return self.convert(sqlglot_expr.left) > self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.LT):
            return self.convert(sqlglot_expr.left) < self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.GTE):
            return self.convert(sqlglot_expr.left) >= self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.LTE):
            return self.convert(sqlglot_expr.left) <= self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.Add):
            return self.convert(sqlglot_expr.left) + self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.Sub):
            return self.convert(sqlglot_expr.left) - self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.Mul):
            return self.convert(sqlglot_expr.left) * self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.Div):
            right_expr = self.convert(sqlglot_expr.right)
            if isinstance(sqlglot_expr.right, exp.Literal):
                right_value = self._parse_literal(sqlglot_expr.right)
                if right_value == 0:
                    raise ValueError("Division by zero")
            return self.convert(sqlglot_expr.left) / right_expr

        if isinstance(sqlglot_expr, exp.Mod):
            raise UnsupportedOperationError(
                "MOD operator",
                suggestion="Use PyArrow compute expressions or arithmetic operations",
            )

        if isinstance(sqlglot_expr, exp.And):
            return self.convert(sqlglot_expr.left) & self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.Or):
            return self.convert(sqlglot_expr.left) | self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.Not):
            return ~self.convert(sqlglot_expr.this)

        if isinstance(sqlglot_expr, exp.Is):
            if isinstance(sqlglot_expr.right, exp.Null):
                return self.convert(sqlglot_expr.left).is_null()
            else:
                return self.convert(sqlglot_expr.left) == self.convert(sqlglot_expr.right)

        if isinstance(sqlglot_expr, exp.In):
            left = self.convert(sqlglot_expr.this)
            if hasattr(sqlglot_expr, "expressions"):
                values = [self._parse_literal(e) for e in sqlglot_expr.expressions if isinstance(e, exp.Literal)]
                if values:
                    return left.is_in(values)
            raise UnsupportedOperationError(
                "IN operator with subqueries",
                suggestion="Use JOIN operations instead",
            )

        if isinstance(sqlglot_expr, exp.Like):
            raise UnsupportedOperationError(
                "LIKE operator",
                suggestion="Use string functions or PyArrow compute expressions",
            )

        if isinstance(sqlglot_expr, exp.Between):
            value = self.convert(sqlglot_expr.this)
            low = self.convert(sqlglot_expr.args.get("low"))
            high = self.convert(sqlglot_expr.args.get("high"))
            return (value >= low) & (value <= high)

        if isinstance(sqlglot_expr, exp.Cast):
            raise UnsupportedOperationError(
                "CAST operator",
                suggestion="Use PyArrow compute expressions for type casting",
            )

        if isinstance(sqlglot_expr, exp.Coalesce):
            raise UnsupportedOperationError(
                "COALESCE function",
                suggestion="Use CASE expressions or PyArrow compute expressions",
            )

        if isinstance(sqlglot_expr, exp.Case):
            raise UnsupportedOperationError(
                "CASE expressions",
                suggestion="Use PyArrow compute expressions for CASE logic",
            )

        if isinstance(sqlglot_expr, exp.Paren):
            return self.convert(sqlglot_expr.this)

        if isinstance(sqlglot_expr, (exp.Upper, exp.Lower, exp.Length, exp.Substring, exp.Trim, exp.Concat)):
            raise UnsupportedOperationError(
                f"{type(sqlglot_expr).__name__} function",
                suggestion="Use PyArrow compute expressions or UDFs for string functions",
            )

        raise UnsupportedOperationError(
            f"Expression type {type(sqlglot_expr).__name__}",
            suggestion="Check the documentation for supported SQL constructs",
        )

    def _parse_literal(self, literal_expr: exp.Literal) -> Any:
        """Parse a SQLGlot literal into a Python value."""
        if literal_expr is None:
            return None
        value = literal_expr.this
        if value is None:
            return None

        if literal_expr.is_string:
            return str(value)

        elif literal_expr.is_number:
            try:
                value_str = str(value)
                if "." in value_str or "e" in value_str.lower():
                    return float(value_str)
                else:
                    return int(value_str)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid numeric literal: {value}") from e

        value_lower = str(value).lower()
        if value_lower in ("true", "false", "1", "0"):
            return value_lower in ("true", "1")

        if value_lower in ("null", "none"):
            return None

        return str(value)


# Backward compatibility alias
ExpressionCompiler = ExpressionConverter
