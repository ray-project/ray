"""Hive predicate equivalence and literal encoding boundaries."""

import sys
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.hive.hive_sql import (
    quote_identifier,
    split_predicate,
)
from ray.data.expressions import col, lit

_TYPES = (
    ("id", "BIGINT"),
    ("tiny", "tinyint"),
    ("flag", "boolean"),
    ("amount", "decimal(12,2)"),
    ("day", "date"),
    ("name", "string"),
    ("code", "varchar(20)"),
    ("padded", "char(20)"),
    ("created", "timestamp"),
    ("score", "double"),
)
_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("tiny", pa.int8()),
        ("flag", pa.bool_()),
        ("amount", pa.decimal128(12, 2)),
        ("day", pa.date32()),
        ("name", pa.string()),
        ("code", pa.string()),
        ("padded", pa.string()),
        ("created", pa.timestamp("us")),
        ("score", pa.float64()),
    ]
)


@pytest.mark.parametrize(
    "expr,sql",
    [
        (col("id") >= 1, "(`id` >= CAST('1' AS bigint))"),
        (lit(1) < col("id"), "(`id` > CAST('1' AS bigint))"),
        (col("tiny") == -128, "(`tiny` = CAST('-128' AS tinyint))"),
        (col("flag") == lit(True), "(`flag` = TRUE)"),
        (
            col("amount") == Decimal("9999999999.99"),
            "(`amount` = CAST('9999999999.99' AS DECIMAL(12,2)))",
        ),
        (
            col("amount") < Decimal("-0.01"),
            "(`amount` < CAST('-0.01' AS DECIMAL(12,2)))",
        ),
        (
            col("amount") == Decimal("0E+999999999"),
            "(`amount` = CAST('0' AS DECIMAL(12,2)))",
        ),
        (col("day") == date(2024, 1, 2), "(`day` = DATE '2024-01-02')"),
        (col("code") == " x ", "(`code` = ' x ')"),
        (col("padded").is_null(), "(`padded` IS NULL)"),
        (col("created").is_not_null(), "(`created` IS NOT NULL)"),
    ],
)
def test_supported_predicates(expr, sql):
    accepted, actual, residual = split_predicate(expr, _SCHEMA, _TYPES)
    assert accepted is expr
    assert actual == sql
    assert residual is None


@pytest.mark.parametrize(
    "expr",
    [
        col("tiny") == 128,
        col("id") == 2**63,
        col("id") == lit(True),
        col("id") == 1.0,
        col("flag") != lit(False),
        col("amount") == Decimal("1.001"),
        col("amount") == Decimal("10000000000"),
        col("amount") == Decimal("NaN"),
        col("amount") == Decimal("Infinity"),
        col("amount") == Decimal("1E+999999999"),
        col("amount") == Decimal("1E-999999999"),
        col("day") == datetime(2024, 1, 2),
        col("day") == "2024-01-02",
        col("name") < "z",
        col("padded") == "x",
        col("created") == datetime(2024, 1, 2),
        col("score") > 1.0,
        col("id").is_in([1, None]),
        col("id").not_in([1]),
        col("id") == lit(None),
        col("id") != lit(None),
        (col("id") + 1) > 2,
        col("unknown") == 1,
        col("name") == "\ud800",
    ],
)
def test_unsupported_predicates_remain_residual(expr):
    accepted, sql, residual = split_predicate(expr, _SCHEMA, _TYPES)
    assert accepted is None and sql is None
    assert residual is expr


def test_and_splits_but_or_and_not_require_the_whole_subtree():
    supported = col("id") > 1
    residual = col("score") > 1.0
    accepted, sql, remainder = split_predicate(supported & residual, _SCHEMA, _TYPES)
    assert accepted is supported and remainder is residual
    assert sql == "(`id` > CAST('1' AS bigint))"
    for expr in (supported | residual, ~(supported & residual)):
        accepted, sql, remainder = split_predicate(expr, _SCHEMA, _TYPES)
        assert accepted is None and sql is None and remainder is expr
    expr = ~((col("id") > 1) | col("name").is_null())
    accepted, sql, remainder = split_predicate(expr, _SCHEMA, _TYPES)
    assert accepted is expr and remainder is None
    assert sql == "(NOT ((`id` > CAST('1' AS bigint)) OR (`name` IS NULL)))"


def test_hive_identifier_and_string_literal_encoding():
    name = "select 类别`"
    assert quote_identifier(name) == "`select 类别```"
    expr = col(name) == "a'\\\n\t\x00中文"
    _, sql, remainder = split_predicate(
        expr, pa.schema([(name, pa.string())]), ((name, "string"),)
    )
    assert remainder is None
    assert sql == "(`select 类别``` = 'a\\'\\\\\\u000a\\u0009\\u0000中文')"
    with pytest.raises(ValueError, match="identifier"):
        quote_identifier("bad\0name")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
