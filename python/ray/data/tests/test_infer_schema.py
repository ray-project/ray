"""Integration tests for ``LogicalOperator.infer_schema()`` (Phase 1).

Asserts that ``Dataset.schema()`` resolves the output schema **without**
falling back to a ``limit(1)`` execution for every non-UDF chain. UDF
chains (``map``, ``map_batches``, ``flat_map``) correctly return ``None``.

The headline guarantee is verified by calling ``ds.schema(fetch_if_missing=False)``
through the public API: this disables the ``limit(1)`` fallback in
``_base_schema``, so a non-``None`` return proves the static
``LogicalOperator.infer_schema()`` path resolved the schema without
materializing any blocks.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.aggregate import Count, Max, Mean, Min, Sum
from ray.data.expressions import col, lit
from ray.data.tests.conftest import *  # noqa: F401,F403


@pytest.fixture(scope="module")
def parquet_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("infer_schema")
    table = pa.table(
        {
            "a": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
            "b": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32()),
            "k": pa.array(["x", "x", "y", "y", "z"], type=pa.string()),
        }
    )
    pq.write_table(table, tmp / "data.parquet")
    return tmp


def _static_schema(ds: ray.data.Dataset) -> pa.Schema:
    """Return the dataset's schema via the static path only.

    ``fetch_if_missing=False`` disables ``_base_schema``'s ``limit(1)``
    fallback, so a non-``None`` return proves the chain's
    ``infer_schema()`` resolved without materializing any blocks. Returns
    ``None`` if the static path failed.
    """
    schema = ds.schema(fetch_if_missing=False)
    return None if schema is None else schema.base_schema


class TestSourceAndPassthroughs:
    def test_read_parquet(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = ray.data.read_parquet(str(parquet_path))
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
            ]
        )

    def test_filter_with_expr_passthrough(
        self, ray_start_regular_shared_2_cpus, parquet_path
    ):
        ds = ray.data.read_parquet(str(parquet_path)).filter(expr=col("a") > 0)
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
            ]
        )

    def test_filter_with_fn_passthrough(
        self, ray_start_regular_shared_2_cpus, parquet_path
    ):
        # Filter with a callable still preserves the input schema.
        ds = ray.data.read_parquet(str(parquet_path)).filter(lambda row: row["a"] > 0)
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
            ]
        )

    def test_limit_passthrough(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = ray.data.read_parquet(str(parquet_path)).limit(2)
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
            ]
        )

    def test_sort_passthrough(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = ray.data.read_parquet(str(parquet_path)).sort("a")
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
            ]
        )


class TestProject:
    def test_select_columns(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = ray.data.read_parquet(str(parquet_path)).select_columns(["a", "b"])
        assert _static_schema(ds) == pa.schema(
            [pa.field("a", pa.int32()), pa.field("b", pa.float32())]
        )

    def test_with_column(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = ray.data.read_parquet(str(parquet_path)).with_column(
            "s", col("a") + col("b")
        )
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
                pa.field("s", pa.float32()),
            ]
        )

    def test_rename_columns(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = ray.data.read_parquet(str(parquet_path)).rename_columns({"a": "x"})
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
                pa.field("x", pa.int32()),
            ]
        )

    def test_with_column_chain(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = (
            ray.data.read_parquet(str(parquet_path))
            .with_column("s", col("a") + col("b"))
            .with_column("d", col("s") * lit(2.0))
        )
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
                pa.field("s", pa.float32()),
                pa.field("d", pa.float64()),
            ]
        )


class TestAggregate:
    def test_groupby_multi_aggs(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = (
            ray.data.read_parquet(str(parquet_path))
            .groupby("k")
            .aggregate(Sum("a"), Mean("b"), Count("a"), Max("a"), Min("a"))
        )
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("k", pa.string()),
                pa.field("sum(a)", pa.int64()),
                pa.field("mean(b)", pa.float64()),
                pa.field("count(a)", pa.int64(), nullable=False),
                pa.field("max(a)", pa.int32()),
                pa.field("min(a)", pa.int32()),
            ]
        )

    def test_groupby_then_sort(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = (
            ray.data.read_parquet(str(parquet_path))
            .groupby("k")
            .aggregate(Sum("a"))
            .sort("k")
        )
        assert _static_schema(ds) == pa.schema(
            [pa.field("k", pa.string()), pa.field("sum(a)", pa.int64())]
        )


class TestNAry:
    def test_union(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds_a = ray.data.read_parquet(str(parquet_path))
        ds_b = ray.data.read_parquet(str(parquet_path))
        ds = ds_a.union(ds_b)
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.float32()),
                pa.field("k", pa.string()),
            ]
        )

    def test_zip_disjoint_columns(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds_a = ray.data.read_parquet(str(parquet_path)).select_columns(["a"])
        ds_b = ray.data.read_parquet(str(parquet_path)).select_columns(["b"])
        ds = ds_a.zip(ds_b)
        assert _static_schema(ds) == pa.schema(
            [pa.field("a", pa.int32()), pa.field("b", pa.float32())]
        )

    def test_zip_overlapping_columns_get_suffixed(
        self, ray_start_regular_shared_2_cpus, parquet_path
    ):
        # When both inputs have a column named "a", the second input's
        # column is renamed to "a_1" to match runtime ``_zip`` behavior.
        ds_a = ray.data.read_parquet(str(parquet_path)).select_columns(["a"])
        ds_b = ray.data.read_parquet(str(parquet_path)).select_columns(["a"])
        ds = ds_a.zip(ds_b)
        assert _static_schema(ds) == pa.schema(
            [pa.field("a", pa.int32()), pa.field("a_1", pa.int32())]
        )


class TestJoin:
    def test_inner_join(self, ray_start_regular_shared_2_cpus, tmp_path):
        left = pa.table(
            {
                "k": pa.array(["a", "b", "c"]),
                "lval": pa.array([1, 2, 3], type=pa.int32()),
            }
        )
        right = pa.table(
            {
                "k": pa.array(["a", "b", "c"]),
                "rval": pa.array([10.0, 20.0, 30.0], type=pa.float32()),
            }
        )
        l_path = tmp_path / "left.parquet"
        r_path = tmp_path / "right.parquet"
        pq.write_table(left, l_path)
        pq.write_table(right, r_path)
        ds = ray.data.read_parquet(str(l_path)).join(
            ray.data.read_parquet(str(r_path)),
            on=("k",),
            join_type="inner",
            num_partitions=2,
        )
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("k", pa.string()),
                pa.field("lval", pa.int32()),
                pa.field("rval", pa.float32()),
            ]
        )


class TestUDFFallback:
    def test_map_batches_returns_none(
        self, ray_start_regular_shared_2_cpus, parquet_path
    ):
        # The headline guarantee inverse: UDF maps return None from
        # infer_schema, so ds.schema(fetch_if_missing=False) returns None
        # (it would only return a value via the limit(1) fallback, which
        # we've disabled).
        ds = ray.data.read_parquet(str(parquet_path)).map_batches(lambda b: b)
        assert ds.schema(fetch_if_missing=False) is None

    def test_select_after_map_batches_also_returns_none(
        self, ray_start_regular_shared_2_cpus, parquet_path
    ):
        # The transitive break: anything downstream of a UDF can't infer
        # schema either.
        ds = (
            ray.data.read_parquet(str(parquet_path))
            .map_batches(lambda b: b)
            .select_columns(["a"])
        )
        assert ds.schema(fetch_if_missing=False) is None


class TestEndToEndStaticResolution:
    """The headline Phase 1 guarantee: a complex non-UDF chain resolves
    via ``Dataset.schema()`` without triggering the ``limit(1)`` fallback.

    The check is:
    - ``ds.schema(fetch_if_missing=False)`` returns the expected schema.
      Because ``fetch_if_missing=False`` disables the ``limit(1)``
      fallback in ``_base_schema``, a non-``None`` return proves that
      ``LogicalOperator.infer_schema()`` resolved the schema statically.
    """

    def test_complex_typed_chain(self, ray_start_regular_shared_2_cpus, parquet_path):
        ds = (
            ray.data.read_parquet(str(parquet_path))
            .filter(expr=col("a") > 0)
            .select_columns(["a", "b", "k"])
            .with_column("s", col("a") + col("b"))
            .groupby("k")
            .aggregate(Sum("a"), Mean("b"))
            .sort("k")
        )
        assert _static_schema(ds) == pa.schema(
            [
                pa.field("k", pa.string()),
                pa.field("sum(a)", pa.int64()),
                pa.field("mean(b)", pa.float64()),
            ]
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
