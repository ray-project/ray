from typing import Callable

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import ray
from ray.data.datatype import DataType
from ray.data.expressions import AliasExpr, UDFExpr, UnnestExpr, col, udf, unnest
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

_FEATURES_DTYPE = DataType.struct(
    [
        ("sum_ab", DataType.from_arrow(pa.int64())),
        ("product_ab", DataType.from_arrow(pa.int64())),
    ]
)


def _make_features_udf() -> Callable[..., UDFExpr]:
    """Build a struct-returning UDF computing sum_ab/product_ab from a, b.

    Everything the UDF references is defined inside this function so
    cloudpickle serializes it by value; a reference to a module-level helper
    would make workers try to import this test module.
    """

    @udf(return_dtype=_FEATURES_DTYPE)
    def make_features(a, b):
        def arr(x):
            return x.combine_chunks() if isinstance(x, pa.ChunkedArray) else x

        a, b = arr(a), arr(b)
        return pa.StructArray.from_arrays(
            # pyrefly: ignore[missing-attribute]  # pc functions are runtime-generated.
            [arr(pc.add(a, b)), arr(pc.multiply(a, b))],
            names=["sum_ab", "product_ab"],
        )

    # pyrefly: ignore[bad-return]  # @udf mis-annotates the decorated function
    # as UDFExpr; it is a callable producing UDFExpr (see udf_callable).
    return make_features


@pytest.fixture
def ab_dataset(ray_start_regular_shared):
    return ray.data.from_items([{"a": 2, "b": 10}, {"a": 3, "b": 20}])


def test_unnest_udf_struct(ab_dataset):
    """unnest of a struct-returning UDF adds one column per struct field,
    in field order."""
    make_features = _make_features_udf()
    result = ab_dataset.with_columns(
        unnest(make_features(col("a"), col("b")))
    ).take_all()
    assert result == [
        {"a": 2, "b": 10, "sum_ab": 12, "product_ab": 20},
        {"a": 3, "b": 20, "sum_ab": 23, "product_ab": 60},
    ]


def test_unnest_mixed_with_mapping(ab_dataset):
    """A mapping and an unnest combine in one projection, in argument order."""
    make_features = _make_features_udf()
    result = ab_dataset.with_columns(
        {"a2": col("a") * 2},
        unnest(make_features(col("a"), col("b"))),
    ).take_all()
    assert result == [
        {"a": 2, "b": 10, "a2": 4, "sum_ab": 12, "product_ab": 20},
        {"a": 3, "b": 20, "a2": 6, "sum_ab": 23, "product_ab": 60},
    ]


def test_unnest_existing_struct_column(ray_start_regular_shared):
    """unnest(col(...)) resolves the struct type from the dataset schema.
    The source struct column is preserved (with_columns adds columns)."""
    ds = ray.data.from_items(
        [
            {"name": "bob", "stats": {"h": 180, "w": 80}},
            {"name": "amy", "stats": {"h": 165, "w": 55}},
        ]
    )
    result = ds.with_columns(unnest(col("stats"))).take_all()
    assert result == [
        {"name": "bob", "stats": {"h": 180, "w": 80}, "h": 180, "w": 80},
        {"name": "amy", "stats": {"h": 165, "w": 55}, "h": 165, "w": 55},
    ]


def test_unnest_desugars_before_optimizer(ab_dataset):
    """UnnestExpr never survives Project construction: the logical plan
    contains only aliased single-column expressions."""
    make_features = _make_features_udf()
    ds = ab_dataset.with_columns(unnest(make_features(col("a"), col("b"))))
    project_op = ds._logical_plan.dag
    assert not any(isinstance(e, UnnestExpr) for e in project_op.exprs)
    unnested = [
        e
        for e in project_op.exprs
        if isinstance(e, AliasExpr) and e.name in ("sum_ab", "product_ab")
    ]
    assert [e.name for e in unnested] == ["sum_ab", "product_ab"]


def test_unnest_single_evaluation(ray_start_regular_shared, tmp_path):
    """The struct expression is evaluated once per block, not once per field.
    Counted via a marker file appended on each UDF invocation."""
    marker = tmp_path / "invocations.log"

    marker_path = str(marker)

    @udf(return_dtype=_FEATURES_DTYPE)
    def make_features(a, b):
        def arr(x):
            return x.combine_chunks() if isinstance(x, pa.ChunkedArray) else x

        with open(marker_path, "a") as f:
            f.write("x\n")
        a, b = arr(a), arr(b)
        return pa.StructArray.from_arrays(
            # pyrefly: ignore[missing-attribute]  # pc functions are runtime-generated.
            [arr(pc.add(a, b)), arr(pc.multiply(a, b))],
            names=["sum_ab", "product_ab"],
        )

    ds = ray.data.from_items([{"a": 2, "b": 10}, {"a": 3, "b": 20}]).repartition(1)
    # pyrefly: ignore[not-callable]  # @udf mis-annotates make_features as UDFExpr.
    ds.with_columns(unnest(make_features(col("a"), col("b")))).materialize()
    assert marker.read_text().count("x") == 1


def test_unnest_non_struct_raises(ab_dataset):
    with pytest.raises(TypeError, match="struct-typed expression"):
        ab_dataset.with_columns(unnest(col("a")))


def test_unnest_unknown_schema_raises(ab_dataset):
    """unnest(col(...)) downstream of an opaque map_batches cannot resolve
    the struct type at plan time and raises."""
    opaque = ab_dataset.map_batches(lambda b: b)
    with pytest.raises(ValueError, match="known when the plan is built"):
        opaque.with_columns(unnest(col("stats")))


def test_unnest_udf_works_after_opaque_input(ab_dataset):
    """A UDF-wrapped unnest resolves from return_dtype, so it works even when
    the input schema is opaque."""
    make_features = _make_features_udf()
    opaque = ab_dataset.map_batches(lambda b: b)
    result = opaque.with_columns(unnest(make_features(col("a"), col("b")))).take_all()
    assert sorted(result, key=lambda r: r["a"]) == [
        {"a": 2, "b": 10, "sum_ab": 12, "product_ab": 20},
        {"a": 3, "b": 20, "sum_ab": 23, "product_ab": 60},
    ]


def test_unnest_cannot_be_aliased():
    with pytest.raises(TypeError, match="cannot be aliased"):
        unnest(col("stats")).alias("x")


def test_unnest_cannot_nest():
    with pytest.raises(TypeError, match="cannot be nested"):
        unnest(unnest(col("stats")))


def test_unnest_rejects_non_expr():
    with pytest.raises(TypeError, match="expects an expression"):
        # pyrefly: ignore[bad-argument-type]  # the bad argument is the test.
        unnest("stats")


def test_with_columns_rejects_unknown_positional(ab_dataset):
    with pytest.raises(TypeError, match="mappings from column name"):
        ab_dataset.with_columns(col("a"))


def test_with_columns_empty_returns_self(ab_dataset):
    assert ab_dataset.with_columns() is ab_dataset
    assert ab_dataset.with_columns({}) is ab_dataset


def test_with_columns_mapping_unchanged(ab_dataset):
    """Pre-existing with_columns(mapping) behavior is untouched."""
    result = ab_dataset.with_columns({"a2": col("a") * 2}).take_all()
    assert result == [
        {"a": 2, "b": 10, "a2": 4},
        {"a": 3, "b": 20, "a2": 6},
    ]


def test_unnest_pushdown_prunes_unused_columns(ray_start_regular_shared, tmp_path):
    """Because unnest desugars to ordinary expressions, projection pushdown
    still prunes columns the projection doesn't reference."""
    import pyarrow.parquet as pq

    table = pa.table({"a": [2, 3], "b": [10, 20], "unused": ["x", "y"]})
    path = str(tmp_path / "t.parquet")
    pq.write_table(table, path)

    make_features = _make_features_udf()
    ds = (
        ray.data.read_parquet(path)
        .with_columns(unnest(make_features(col("a"), col("b"))))
        .select_columns(["sum_ab", "product_ab"])
    )
    # The optimized read should be narrowed to the columns the desugared
    # projection references (a, b) — "unused" must be pruned. Check the plan
    # BEFORE executing: execution optimizes the plan in place, and optimizing
    # an already-optimized plan a second time double-applies pushdown.
    from typing import Optional

    from ray.data._internal.logical.interfaces import Operator
    from ray.data._internal.logical.optimizers import LogicalOptimizer

    optimized = LogicalOptimizer().optimize(ds._logical_plan)
    op: Optional[Operator] = optimized.dag
    read_op = None
    while op is not None:
        if hasattr(op, "scanner"):
            read_op = op
            break
        op = op.input_dependencies[0] if op.input_dependencies else None
    assert read_op is not None, "no read operator with a scanner found"
    # pyrefly: ignore[missing-attribute]  # hasattr-guarded; scanner is on ReadFiles.
    pruned = read_op.scanner.pruned_column_names()
    assert pruned is not None and set(pruned) == {"a", "b"}

    assert ds.take_all() == [
        {"sum_ab": 12, "product_ab": 20},
        {"sum_ab": 23, "product_ab": 60},
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
