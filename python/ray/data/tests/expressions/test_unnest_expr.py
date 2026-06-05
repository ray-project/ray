"""
Tests for UnnestExpr / unnest() — evaluates eval_projection directly,
no with_columns() required (that's in the PR being worked on separately).

Run with:
    pytest python/ray/data/tests/expressions/test_unnest_expr.py -v
"""

import pyarrow as pa
import pyarrow.compute as pc
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_block():
    """Arrow table with columns a (int64) and b (int64)."""
    return pa.table({"a": pa.array([2, 4]), "b": pa.array([3, 5])})


def _get_eval_projection():
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_projection,
    )

    return eval_projection


def _make_struct_udf():
    """A UDF that returns a struct with sum_ab and product_ab fields."""
    from ray.data.datatype import DataType
    from ray.data.expressions import col, udf

    @udf(
        return_dtype=DataType.struct(
            [
                ("sum_ab", DataType.int64()),
                ("product_ab", DataType.int64()),
            ]
        )
    )
    def make_features(a: pa.Array, b: pa.Array) -> pa.StructArray:
        # pa.table columns arrive as ChunkedArrays; flatten/combine before use.
        if isinstance(a, pa.ChunkedArray):
            a = a.combine_chunks()
        if isinstance(b, pa.ChunkedArray):
            b = b.combine_chunks()
        return pa.StructArray.from_arrays(
            [pc.add(a, b), pc.multiply(a, b)],
            names=["sum_ab", "product_ab"],
        )

    return make_features(col("a"), col("b"))


# ---------------------------------------------------------------------------
# Test 1: Basic unnest — new struct fields appear as sibling columns
# ---------------------------------------------------------------------------


def test_eval_projection_unnest_basic():
    """UnnestExpr expands struct fields into sibling columns."""
    from ray.data.expressions import StarExpr, UnnestExpr

    eval_projection = _get_eval_projection()
    block = _make_block()

    result = eval_projection(
        [StarExpr(), UnnestExpr(inner=_make_struct_udf())],
        block,
    )

    # Unnested columns present with correct values
    assert result["sum_ab"].to_pylist() == [5, 9]
    assert result["product_ab"].to_pylist() == [6, 20]
    # Original columns preserved
    assert result["a"].to_pylist() == [2, 4]
    assert result["b"].to_pylist() == [3, 5]


# ---------------------------------------------------------------------------
# Test 2: UnnestExpr overwrites an existing column (no duplicates)
# ---------------------------------------------------------------------------


def test_eval_projection_unnest_overwrites_existing():
    """UnnestExpr field names that match existing cols replace them, no duplicates."""
    from ray.data.datatype import DataType
    from ray.data.expressions import StarExpr, UnnestExpr, col, udf

    @udf(return_dtype=DataType.struct([("a", DataType.int64())]))
    def replace_a(x: pa.Array) -> pa.StructArray:
        if isinstance(x, pa.ChunkedArray):
            x = x.combine_chunks()
        return pa.StructArray.from_arrays([pc.multiply(x, 10)], names=["a"])

    eval_projection = _get_eval_projection()
    block = pa.table({"a": pa.array([1, 2]), "b": pa.array([3, 4])})

    result = eval_projection(
        [StarExpr(), UnnestExpr(inner=replace_a(col("a")))],
        block,
    )

    # "a" overwritten, not duplicated
    assert result.schema.names.count("a") == 1
    assert result["a"].to_pylist() == [10, 20]
    assert result["b"].to_pylist() == [3, 4]  # unchanged


# ---------------------------------------------------------------------------
# Test 3: unnest() factory — wrong type raises TypeError
# ---------------------------------------------------------------------------


def test_unnest_factory_non_expr_raises():
    """unnest() raises TypeError when passed a non-Expr argument."""
    from ray.data.expressions import unnest

    with pytest.raises(TypeError, match="unnest\\(\\) expects an Expr"):
        unnest("not_an_expr")


# ---------------------------------------------------------------------------
# Test 4: UnnestExpr.alias() raises TypeError
# ---------------------------------------------------------------------------


def test_unnest_alias_raises():
    """Calling .alias() on UnnestExpr raises TypeError."""
    from ray.data.expressions import UnnestExpr

    expr = UnnestExpr(inner=_make_struct_udf())
    with pytest.raises(TypeError, match="UnnestExpr cannot be aliased"):
        expr.alias("bad_name")


# ---------------------------------------------------------------------------
# Test 5: UnnestExpr on non-struct expression raises TypeError at eval time
# ---------------------------------------------------------------------------


def test_unnest_non_struct_raises_at_eval():
    """UnnestExpr on a non-struct expression raises TypeError at eval time."""
    from ray.data.expressions import StarExpr, UnnestExpr, col

    eval_projection = _get_eval_projection()
    block = pa.table({"a": pa.array([1, 2])})

    # col("a") returns an int64 array, not a struct
    with pytest.raises(TypeError, match="unnest\\(\\) requires a struct"):
        eval_projection([StarExpr(), UnnestExpr(inner=col("a"))], block)


# ---------------------------------------------------------------------------
# Test 6: Regression — normal projection still works unchanged
# ---------------------------------------------------------------------------


def test_existing_eval_projection_unchanged():
    """Regression: normal expressions still work exactly as before."""
    from ray.data.expressions import StarExpr, col

    eval_projection = _get_eval_projection()
    block = pa.table({"id": pa.array([0, 1, 2])})

    result = eval_projection(
        [StarExpr(), (col("id") * 2).alias("id_2")],
        block,
    )

    assert result["id"].to_pylist() == [0, 1, 2]
    assert result["id_2"].to_pylist() == [0, 2, 4]


# ---------------------------------------------------------------------------
# Test 7: structurally_equals symmetry
# ---------------------------------------------------------------------------


def test_unnest_structurally_equals():
    """Two UnnestExpr wrapping structurally equal inners are structurally equal."""
    from ray.data.expressions import UnnestExpr, col

    from ray.data.datatype import DataType
    from ray.data.expressions import udf

    @udf(return_dtype=DataType.struct([("x", DataType.int64())]))
    def f(a: pa.Array) -> pa.StructArray:
        return pa.StructArray.from_arrays([a], names=["x"])

    inner = f(col("a"))
    expr1 = UnnestExpr(inner=inner)
    expr2 = UnnestExpr(inner=inner)

    assert expr1.structurally_equals(expr2)
    assert not expr1.structurally_equals(col("a"))


# ---------------------------------------------------------------------------
# Test 8: UnnestExpr.name returns the sentinel
# ---------------------------------------------------------------------------


def test_unnest_name_sentinel():
    """UnnestExpr.name returns __unnest__ sentinel, never None."""
    from ray.data.expressions import UnnestExpr, col

    expr = UnnestExpr(inner=col("a"))
    assert expr.name == "__unnest__"


# ---------------------------------------------------------------------------
# Test 9: repr / tree repr doesn't crash
# ---------------------------------------------------------------------------


def test_unnest_repr_does_not_crash():
    """repr(UnnestExpr(...)) should not raise."""
    from ray.data.expressions import UnnestExpr, col

    expr = UnnestExpr(inner=col("a"))
    r = repr(expr)
    assert "UNNEST" in r


# ---------------------------------------------------------------------------
# Test 10: UnnestExpr without StarExpr (standalone unnest-only projection)
# ---------------------------------------------------------------------------


def test_unnest_without_star():
    """UnnestExpr can be used without StarExpr (drops all existing columns)."""
    from ray.data.expressions import UnnestExpr

    eval_projection = _get_eval_projection()
    block = _make_block()

    result = eval_projection(
        [UnnestExpr(inner=_make_struct_udf())],
        block,
    )

    # Only the struct fields are present — original columns are dropped
    assert set(result.schema.names) == {"sum_ab", "product_ab"}
    assert result["sum_ab"].to_pylist() == [5, 9]
    assert result["product_ab"].to_pylist() == [6, 20]


# ---------------------------------------------------------------------------
# Test 11 (Gap 1): Pandas block — proves BlockAccessor path not broken
# ---------------------------------------------------------------------------


def test_eval_projection_unnest_pandas_block():
    """
    Prove that UnnestExpr works correctly with Pandas DataFrame blocks.

    Why this matters: we specifically preserved the BlockAccessor / fill_column
    path so that eval_projection stays block-type-agnostic. If we had replaced
    the function body with a pa.table(...) constructor (as the original plan
    proposed), this test would crash with a type error.

    Implementation note: PandasBlockAccessor.upsert_column() explicitly handles
    pa.Array / pa.ChunkedArray inputs by calling .to_pandas(), so the pa.Array
    child arrays produced by StructArray.flatten() flow correctly into a
    Pandas DataFrame output.

    UDF note: Pandas block columns arrive as pd.Series via `block[col_name]`.
    `pc.add` does NOT work on Pandas integer Series with list-backed storage
    (raises ArrowNotImplementedError). User UDFs against Pandas blocks must
    use numpy arithmetic (or explicit .to_numpy()) and then wrap results back
    into pa.Array before constructing a StructArray.
    """
    import pandas as pd

    from ray.data.datatype import DataType
    from ray.data.expressions import StarExpr, UnnestExpr, col, udf

    @udf(
        return_dtype=DataType.struct(
            [
                ("sum_ab", DataType.int64()),
                ("product_ab", DataType.int64()),
            ]
        )
    )
    def make_features_pandas(a, b) -> pa.StructArray:
        # When the block is a Pandas DataFrame, eval_expr returns pd.Series.
        # pc.add does NOT accept Pandas Series with list-backed storage;
        # use numpy arithmetic instead (always safe with integer Pandas Series).
        import numpy as np

        a_np = np.asarray(a)
        b_np = np.asarray(b)
        sum_arr = pa.array(a_np + b_np, type=pa.int64())
        prod_arr = pa.array(a_np * b_np, type=pa.int64())
        return pa.StructArray.from_arrays(
            [sum_arr, prod_arr],
            names=["sum_ab", "product_ab"],
        )

    eval_projection = _get_eval_projection()

    # Pass a PANDAS DataFrame, not a PyArrow table.
    block = pd.DataFrame({"a": [2, 4], "b": [3, 5]})

    result = eval_projection(
        [StarExpr(), UnnestExpr(inner=make_features_pandas(col("a"), col("b")))],
        block,
    )

    # Result is a Pandas DataFrame (block type is preserved).
    assert isinstance(result, pd.DataFrame), (
        f"Expected pd.DataFrame, got {type(result).__name__}. "
        "This means the BlockAccessor path was bypassed."
    )

    # Values are correct.
    assert result["sum_ab"].tolist() == [5, 9]
    assert result["product_ab"].tolist() == [6, 20]

    # Original columns preserved.
    assert result["a"].tolist() == [2, 4]
    assert result["b"].tolist() == [3, 5]

    # Column order: original columns first, then unnested columns.
    assert list(result.columns) == ["a", "b", "sum_ab", "product_ab"]


# ---------------------------------------------------------------------------
# Test 12 (Gap 2): Multiple UnnestExprs in a single projection
# ---------------------------------------------------------------------------


def test_eval_projection_multiple_unnests():
    """
    Multiple UnnestExprs in one projection list must not crash or drop columns.

    Two UnnestExprs with non-overlapping output field names: both sets of
    fields should appear in the result without duplicates.

    If two UnnestExprs produce the same field name, fill_column's upsert
    semantics mean the last one wins (no crash, no duplicate column).
    """
    from ray.data.datatype import DataType
    from ray.data.expressions import StarExpr, UnnestExpr, col, udf

    @udf(
        return_dtype=DataType.struct(
            [
                ("diff_ab", DataType.int64()),
            ]
        )
    )
    def make_diff(a, b) -> pa.StructArray:
        diff = pc.subtract(b, a)
        if isinstance(diff, pa.ChunkedArray):
            diff = diff.combine_chunks()
        return pa.StructArray.from_arrays([diff], names=["diff_ab"])

    eval_projection = _get_eval_projection()
    block = _make_block()

    result = eval_projection(
        [
            StarExpr(),
            UnnestExpr(inner=_make_struct_udf()),  # produces sum_ab, product_ab
            UnnestExpr(inner=make_diff(col("a"), col("b"))),  # produces diff_ab
        ],
        block,
    )

    # All three new columns present alongside originals.
    schema_names = list(result.schema.names)
    assert "sum_ab" in schema_names
    assert "product_ab" in schema_names
    assert "diff_ab" in schema_names
    assert "a" in schema_names
    assert "b" in schema_names

    # No duplicate column names.
    assert len(schema_names) == len(set(schema_names)), (
        f"Duplicate column names found: {schema_names}"
    )

    # Correct values.
    assert result["sum_ab"].to_pylist() == [5, 9]
    assert result["product_ab"].to_pylist() == [6, 20]
    assert result["diff_ab"].to_pylist() == [1, 1]  # b - a = [3-2, 5-4]


# ---------------------------------------------------------------------------
# Test 13 (Gap 3): ChunkedArray input — proves combine_chunks guard fires
# ---------------------------------------------------------------------------


def test_eval_projection_chunked_array():
    """
    UnnestExpr handles PyArrow ChunkedArray column inputs correctly.

    pa.table columns are always ChunkedArrays. When the UDF receives them
    and returns a StructArray (not a ChunkedArray), our combine_chunks guard
    in eval_projection is NOT triggered for the result — but the UDF itself
    must handle ChunkedArray inputs (which _make_struct_udf does).

    If we hadn't guarded the UDF inputs in _make_struct_udf, this test
    would crash with:
      TypeError: Expected Array, got <class 'pyarrow.lib.ChunkedArray'>

    This explicitly fractures the data into 2 chunks per column so the
    ChunkedArray path is always exercised, regardless of how pa.table
    internally stores single-array columns.
    """
    from ray.data.expressions import StarExpr, UnnestExpr

    eval_projection = _get_eval_projection()

    # Explicitly fracture each column into 2 chunks to force ChunkedArray path.
    chunked_a = pa.chunked_array([[1], [2]])
    chunked_b = pa.chunked_array([[3], [4]])
    block = pa.table({"a": chunked_a, "b": chunked_b})

    # _make_struct_udf() uses combine_chunks on its inputs — this is required
    # because pa.StructArray.from_arrays does not accept ChunkedArray.
    result = eval_projection(
        [StarExpr(), UnnestExpr(inner=_make_struct_udf())],
        block,
    )

    assert result["sum_ab"].to_pylist() == [4, 6]  # [1+3, 2+4]
    assert result["product_ab"].to_pylist() == [3, 8]  # [1*3, 2*4]
    assert result["a"].to_pylist() == [1, 2]
    assert result["b"].to_pylist() == [3, 4]


# ---------------------------------------------------------------------------
# Test 14 (Gap 4): Zero-row block — schema is preserved even with 0 rows
# ---------------------------------------------------------------------------


def test_eval_projection_zero_rows():
    """
    eval_projection must not crash when the block has 0 rows but a known schema.

    The early-return guard (`num_rows == 0 AND column_names == []`) does NOT
    fire here because the block has a schema. The UDF receives empty arrays,
    returns an empty StructArray, and flatten() produces empty child arrays.

    Without this test we would not catch a crash in the StructArray.flatten()
    path caused by empty struct types or divide-by-zero in row-count math.

    The output schema must contain sum_ab and product_ab even with 0 rows,
    because schema comes from the block structure, not from the data.
    """
    from ray.data.expressions import StarExpr, UnnestExpr

    eval_projection = _get_eval_projection()

    # 0 rows with explicit int64 schema.
    empty_block = pa.table(
        {
            "a": pa.array([], type=pa.int64()),
            "b": pa.array([], type=pa.int64()),
        }
    )

    result = eval_projection(
        [StarExpr(), UnnestExpr(inner=_make_struct_udf())],
        empty_block,
    )

    assert result.num_rows == 0
    assert "sum_ab" in result.schema.names
    assert "product_ab" in result.schema.names
    # Original columns also preserved with 0 rows.
    assert "a" in result.schema.names
    assert "b" in result.schema.names


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
