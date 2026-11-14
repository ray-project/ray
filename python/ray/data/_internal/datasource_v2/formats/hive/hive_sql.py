"""Translate a conservative subset of Ray predicates to HiveSQL."""

import re
from datetime import date
from decimal import Decimal
from typing import Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.common.pushdown_utils import combine_predicates
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    UnaryExpr,
)

_COMPARISONS = {
    Operation.EQ: "=",
    Operation.NE: "<>",
    Operation.LT: "<",
    Operation.LE: "<=",
    Operation.GT: ">",
    Operation.GE: ">=",
}
_REVERSE = {
    Operation.EQ: Operation.EQ,
    Operation.NE: Operation.NE,
    Operation.LT: Operation.GT,
    Operation.LE: Operation.GE,
    Operation.GT: Operation.LT,
    Operation.GE: Operation.LE,
}
_INTEGER_BITS = {"tinyint": 8, "smallint": 16, "int": 32, "integer": 32, "bigint": 64}
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)\Z")


def quote_identifier(name: str) -> str:
    """Quote a literal name in Hive's default quoted-identifier mode."""
    if not name or "\0" in name:
        raise ValueError("HiveServer2 column identifier is invalid")
    return "`" + name.replace("`", "``") + "`"


def _string_literal(value: str) -> Optional[str]:
    # Hive uses C-style escaping, not SQL's doubled-quote string convention.
    encoded: list[str] = []
    for char in value:
        code = ord(char)
        if 0xD800 <= code <= 0xDFFF:
            return None
        if char in ("\\", "'"):
            encoded.append("\\" + char)
        elif code < 32 or code == 127:
            encoded.append(f"\\u{code:04x}")
        else:
            encoded.append(char)
    return "'" + "".join(encoded) + "'"


def _literal(value, hive_type: str, op: Operation) -> Optional[str]:
    if hive_type in _INTEGER_BITS and type(value) is int:
        bits = _INTEGER_BITS[hive_type]
        if -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
            return f"CAST('{value}' AS {hive_type})"
    if hive_type == "boolean" and type(value) is bool and op == Operation.EQ:
        return "TRUE" if value else "FALSE"
    if hive_type == "date" and type(value) is date:
        return f"DATE '{value.isoformat()}'"
    if (
        (
            hive_type == "string"
            or hive_type == "varchar"
            or hive_type.startswith("varchar(")
        )
        and type(value) is str
        and op == Operation.EQ
    ):
        return _string_literal(value)
    decimal_type = _DECIMAL.fullmatch(hive_type)
    if decimal_type and isinstance(value, Decimal) and value.is_finite():
        precision, scale = map(int, decimal_type.groups())
        # Inspect digits without rounding through the process-wide Decimal context.
        sign, digits, exponent = value.as_tuple()
        assert isinstance(exponent, int)
        if not any(digits):
            return f"CAST('0' AS DECIMAL({precision},{scale}))"
        while digits[-1] == 0:
            digits, exponent = digits[:-1], exponent + 1
        if exponent >= -scale and len(digits) + exponent + scale <= precision:
            exact = Decimal((sign, digits, exponent))
            return f"CAST('{exact:f}' AS DECIMAL({precision},{scale}))"
    return None


def _translate(expr: Expr, schema: pa.Schema, native_types: dict) -> Optional[str]:
    if isinstance(expr, BinaryExpr) and expr.op in (Operation.AND, Operation.OR):
        left = _translate(expr.left, schema, native_types)
        right = _translate(expr.right, schema, native_types)
        if left is not None and right is not None:
            operator = "AND" if expr.op == Operation.AND else "OR"
            return f"({left} {operator} {right})"
        return None
    if isinstance(expr, UnaryExpr):
        if expr.op == Operation.NOT:
            operand = _translate(expr.operand, schema, native_types)
            return f"(NOT {operand})" if operand is not None else None
        if (
            expr.op in (Operation.IS_NULL, Operation.IS_NOT_NULL)
            and isinstance(expr.operand, ColumnExpr)
            and expr.operand.name in schema.names
            and expr.operand.name in native_types
        ):
            operator = "IS NULL" if expr.op == Operation.IS_NULL else "IS NOT NULL"
            return f"({quote_identifier(expr.operand.name)} {operator})"
        return None
    if not isinstance(expr, BinaryExpr) or expr.op not in _COMPARISONS:
        return None
    column, literal, op = expr.left, expr.right, expr.op
    if isinstance(column, LiteralExpr) and isinstance(literal, ColumnExpr):
        column, literal, op = literal, column, _REVERSE[op]
    if not isinstance(column, ColumnExpr) or not isinstance(literal, LiteralExpr):
        return None
    if column.name not in schema.names or column.name not in native_types:
        return None
    hive_type = native_types[column.name].lower().replace(" ", "")
    encoded = _literal(literal.value, hive_type, op)
    if encoded is None:
        return None
    return f"({quote_identifier(column.name)} {_COMPARISONS[op]} {encoded})"


def split_predicate(
    predicate: Expr, schema: pa.Schema, native_types: Tuple[Tuple[str, str], ...]
) -> Tuple[Optional[Expr], Optional[str], Optional[Expr]]:
    """Return accepted expression, SQL, and residual; split only AND chains."""
    if isinstance(predicate, BinaryExpr) and predicate.op == Operation.AND:
        left, left_sql, left_residual = split_predicate(
            predicate.left, schema, native_types
        )
        right, right_sql, right_residual = split_predicate(
            predicate.right, schema, native_types
        )
        sql = (
            f"({left_sql} AND {right_sql})"
            if left_sql is not None and right_sql is not None
            else left_sql
            if left_sql is not None
            else right_sql
        )
        return (
            combine_predicates(left, right),
            sql,
            combine_predicates(left_residual, right_residual),
        )
    sql = _translate(predicate, schema, dict(native_types))
    if sql is None:
        return None, None, predicate
    return predicate, sql, None
