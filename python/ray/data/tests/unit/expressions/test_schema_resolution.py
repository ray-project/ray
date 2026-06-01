"""Tests for schema-aware expression resolution.

Covers ``Expr.get_type``, ``Expr.nullable``, ``Expr.to_field``, and the
``exprlist_to_fields`` helper. These are the building blocks Phase 1
operators (``Project``, ``Aggregate``, ``Join``, etc.) use to compute
``infer_schema()`` without executing the plan.
"""

import pyarrow as pa
import pytest

from ray.data.datatype import DataType
from ray.data.expressions import (
    ColumnExpr,
    DownloadExpr,
    MonotonicallyIncreasingIdExpr,
    RandomExpr,
    StarExpr,
    UDFExpr,
    UUIDExpr,
    _expand_star_exprs,
    col,
    exprlist_to_fields,
    lit,
    star,
    udf,
)


@pytest.fixture
def schema():
    return pa.schema(
        [
            pa.field("a", pa.int32(), nullable=True),
            pa.field("b", pa.float32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("flag", pa.bool_(), nullable=True),
        ]
    )


class TestColumnExpr:
    def test_to_field_returns_input_field_verbatim(self):
        expected = pa.field("x", pa.int64(), nullable=False, metadata={b"k": b"v"})
        in_schema = pa.schema([expected])
        assert col("x").to_field(in_schema) == expected

    def test_to_field_int_column(self, schema):
        assert col("a").to_field(schema) == pa.field("a", pa.int32(), nullable=True)

    def test_to_field_non_nullable_column(self, schema):
        assert col("b").to_field(schema) == pa.field("b", pa.float32(), nullable=False)

    def test_to_field_missing(self, schema):
        assert col("missing").to_field(schema) is None


class TestLiteralExpr:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (5, pa.field("v", pa.int64(), nullable=False)),
            (5.0, pa.field("v", pa.float64(), nullable=False)),
            ("hello", pa.field("v", pa.string(), nullable=False)),
            (True, pa.field("v", pa.bool_(), nullable=False)),
            (None, pa.field("v", pa.null(), nullable=True)),
        ],
    )
    def test_to_field(self, schema, value, expected):
        # ``LiteralExpr`` has no name, so ``to_field`` returns ``None``;
        # we wrap in an alias to get a named field.
        assert lit(value).alias("v").to_field(schema) == expected


class TestBinaryExpr:
    @pytest.mark.parametrize(
        "expr_factory,expected",
        [
            # int32 + int64 -> int64 (PyArrow promotion); a is nullable.
            (lambda: col("a") + lit(5), pa.field("out", pa.int64(), nullable=True)),
            # int32 + float32 -> float; a is nullable, so output is nullable.
            (lambda: col("a") + col("b"), pa.field("out", pa.float32(), nullable=True)),
            # float32 * float64 -> double; both operands are non-nullable
            # (b is non-null, literal is not None) -> output is non-nullable.
            (
                lambda: col("b") * lit(2.0),
                pa.field("out", pa.float64(), nullable=False),
            ),
            # comparisons -> bool
            (lambda: col("a") > lit(0), pa.field("out", pa.bool_(), nullable=True)),
            (lambda: col("a") == lit(1), pa.field("out", pa.bool_(), nullable=True)),
            # logical -> bool
            (
                lambda: col("flag") & col("flag"),
                pa.field("out", pa.bool_(), nullable=True),
            ),
            (
                lambda: col("flag") | col("flag"),
                pa.field("out", pa.bool_(), nullable=True),
            ),
            # in/not_in -> bool
            (
                lambda: col("a").is_in([1, 2, 3]),
                pa.field("out", pa.bool_(), nullable=True),
            ),
            (
                lambda: col("a").not_in([1, 2, 3]),
                pa.field("out", pa.bool_(), nullable=True),
            ),
            # string concat
            (
                lambda: col("name") + lit("!"),
                pa.field("out", pa.string(), nullable=True),
            ),
        ],
    )
    def test_to_field(self, schema, expr_factory, expected):
        assert expr_factory().alias("out").to_field(schema) == expected

    def test_unresolvable_returns_none(self, schema):
        assert (col("missing") + lit(1)).alias("x").to_field(schema) is None


class TestUnaryExpr:
    def test_is_null(self, schema):
        expected = pa.field("isnull", pa.bool_(), nullable=False)
        assert col("a").is_null().alias("isnull").to_field(schema) == expected

    def test_is_not_null(self, schema):
        expected = pa.field("isnotnull", pa.bool_(), nullable=False)
        assert col("a").is_not_null().alias("isnotnull").to_field(schema) == expected

    def test_not_bool(self, schema):
        expected = pa.field("neg", pa.bool_(), nullable=True)
        assert (~col("flag")).alias("neg").to_field(schema) == expected


class TestAliasExpr:
    def test_to_field_renames(self, schema):
        # Wraps a column field; alias swaps the name, preserves type/nullability.
        assert col("a").alias("renamed").to_field(schema) == pa.field(
            "renamed", pa.int32(), nullable=True
        )

    def test_to_field_around_binary(self, schema):
        assert (col("a") + col("b")).alias("sum").to_field(schema) == pa.field(
            "sum", pa.float32(), nullable=True
        )


class TestSelfContainedExprs:
    def test_udf_uses_return_dtype(self, schema):
        @udf(return_dtype=DataType.float64())  # pyrefly: ignore[missing-attribute]
        def double(x):
            return x

        assert double(col("a")).alias("d").to_field(  # pyrefly: ignore[not-callable]
            schema
        ) == pa.field("d", pa.float64(), nullable=True)

    def test_download_is_binary(self, schema):
        assert DownloadExpr("uri").alias("bytes").to_field(schema) == pa.field(
            "bytes", pa.binary(), nullable=True
        )

    def test_monotonically_increasing_id(self, schema):
        assert MonotonicallyIncreasingIdExpr().alias("id").to_field(schema) == pa.field(
            "id", pa.int64(), nullable=False
        )

    def test_random(self, schema):
        assert RandomExpr().alias("r").to_field(schema) == pa.field(
            "r", pa.float64(), nullable=False
        )

    def test_uuid(self, schema):
        assert UUIDExpr().alias("u").to_field(schema) == pa.field(
            "u", pa.string(), nullable=False
        )


class TestStarExpr:
    def test_to_field_returns_none(self, schema):
        # ``StarExpr`` represents many columns; ``exprlist_to_fields``
        # expands it inline rather than calling ``to_field`` on it.
        assert star().to_field(schema) is None


class TestExprlistToFields:
    def test_simple_columns(self, schema):
        expected = pa.schema(
            [
                pa.field("a", pa.int32(), nullable=True),
                pa.field("b", pa.float32(), nullable=False),
            ]
        )
        result = pa.schema(exprlist_to_fields([col("a"), col("b")], schema))
        assert result == expected

    def test_star_expansion(self, schema):
        # Star expands to all input fields verbatim.
        result = pa.schema(exprlist_to_fields([star()], schema))
        assert result == schema

    def test_star_with_rename(self, schema):
        result = pa.schema(
            exprlist_to_fields([star(), col("a")._rename("renamed_a")], schema)
        )
        # Renaming "a" -> "renamed_a" substitutes the renamed field at
        # "a"'s position (matching runtime ``eval_projection``).
        expected = pa.schema(
            [
                pa.field("renamed_a", pa.int32(), nullable=True),
                pa.field("b", pa.float32(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
                pa.field("flag", pa.bool_(), nullable=True),
            ]
        )
        assert result == expected

    def test_star_with_rename_missing_source_returns_none(self, schema):
        # Renaming an absent column must fail resolution (matching the
        # runtime, which raises "column not found"), not silently append.
        assert exprlist_to_fields([star(), col("missing")._rename("x")], schema) is None

    def test_star_with_with_column(self, schema):
        # with_column-style: [star(), expr.alias(name)] preserves all input
        # columns and appends the new computed column.
        result = pa.schema(
            exprlist_to_fields([star(), (col("a") + col("b")).alias("sum")], schema)
        )
        expected = pa.schema(
            list(schema) + [pa.field("sum", pa.float32(), nullable=True)]
        )
        assert result == expected

    def test_returns_none_on_unresolvable(self, schema):
        assert exprlist_to_fields([col("missing")], schema) is None

    def test_with_column_overrides_existing_column(self, schema):
        # ``with_column("a", new_expr)`` builds ``[star(), new_expr.alias("a")]``.
        # The override should replace the existing "a" in place (last-wins,
        # matching runtime ``eval_projection``'s upsert behavior), not
        # produce a duplicate.
        result = pa.schema(
            exprlist_to_fields([star(), (col("a") + lit(10)).alias("a")], schema)
        )
        expected = pa.schema(
            [
                pa.field("a", pa.int64(), nullable=True),  # new type from a + 10
                pa.field("b", pa.float32(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
                pa.field("flag", pa.bool_(), nullable=True),
            ]
        )
        assert result == expected

    def test_returns_none_on_udf_without_return_dtype(self, schema):
        # Construct a synthetic UDFExpr with object data_type to simulate
        # the "untyped UDF" case.
        e = UDFExpr(
            fn=lambda x: x,
            args=[col("a")],
            kwargs={},
            data_type=DataType(object),
        )
        assert exprlist_to_fields([e.alias("out")], schema) is None


class TestExpandStarExprs:
    """Tests for ``_expand_star_exprs`` (Phase 2a eager expansion)."""

    def test_passthrough_without_star(self, schema):
        # No StarExpr -> input list returned unchanged.
        exprs = [col("a"), (col("a") + col("b")).alias("sum")]
        assert _expand_star_exprs(exprs, schema) is exprs

    def test_passthrough_when_schema_is_none(self):
        exprs = [star(), col("a")]
        assert _expand_star_exprs(exprs, None) is exprs

    def test_simple_star(self, schema):
        # ``[star()]`` -> one ``col()`` per input column.
        result = _expand_star_exprs([star()], schema)
        assert [type(e) for e in result] == [ColumnExpr] * 4
        assert [e.name for e in result] == ["a", "b", "name", "flag"]

    def test_star_with_with_column(self, schema):
        # ``with_column``-style: ``[star(), expr.alias("new")]`` expands
        # to ``[col(a), col(b), col(name), col(flag), expr.alias("new")]``.
        new_expr = (col("a") + col("b")).alias("new")
        result = _expand_star_exprs([star(), new_expr], schema)
        assert len(result) == 5
        assert [e.name for e in result[:4]] == ["a", "b", "name", "flag"]
        assert result[-1] is new_expr

    def test_star_with_rename(self, schema):
        # ``rename_columns({"a": "renamed_a"})``: ``[star(), col(a)._rename("renamed_a")]``
        # -> ``[col(a)._rename("renamed_a"), col(b), col(name), col(flag)]``.
        # The rename substitutes for its source column *in place* (matching
        # runtime ``eval_projection`` / ``exprlist_to_fields``), so output
        # column order is preserved rather than moving the renamed column
        # to the end.
        rename = col("a")._rename("renamed_a")
        result = _expand_star_exprs([star(), rename], schema)
        assert len(result) == 4
        assert result[0] is rename
        assert [e.name for e in result[1:]] == ["b", "name", "flag"]

    def test_star_with_rename_source_missing(self, schema):
        # A rename whose source column isn't in the input schema stays in
        # its trailing position so it still errors ("column not found") at
        # runtime instead of being silently dropped.
        rename = col("missing")._rename("renamed")
        result = _expand_star_exprs([star(), rename], schema)
        assert len(result) == 5
        assert [e.name for e in result[:4]] == ["a", "b", "name", "flag"]
        assert result[-1] is rename

    def test_no_star_no_op(self, schema):
        # Verify ``StarExpr`` is gone from the result of ``with_column``
        # expansion.
        result = _expand_star_exprs([star(), (col("a") + lit(1)).alias("new")], schema)
        assert not any(isinstance(e, StarExpr) for e in result)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
