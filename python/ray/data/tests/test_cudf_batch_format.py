"""Tests for cuDF batch format support in Ray Data.

These tests require cuDF to be installed and run on GPU CI. Use pytest.importorskip
so the file is skipped when cudf is missing (e.g. local CPU runs).

Uses cudf.testing.assert_eq for comparisons (see cuDF developer guide:
https://docs.rapids.ai/api/cudf/latest/developer_guide/testing/).
"""

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa

cudf = pytest.importorskip("cudf")


def block_generator(num_rows: int, num_blocks: int):
    """Yield Arrow blocks for testing batch_blocks."""
    for i in range(num_blocks):
        yield pa.table({"foo": list(range(i * num_rows, (i + 1) * num_rows))})


def _make_dataset(data_source: str, shutdown_only, **kwargs):
    """Create dataset from data_source type."""
    if data_source == "range":
        return ray.data.range(
            kwargs.get("n", 100), override_num_blocks=kwargs.get("blocks", 2)
        )
    elif data_source == "range_tensor":
        return ray.data.range_tensor(
            kwargs.get("n", 100), override_num_blocks=kwargs.get("blocks", 2)
        )
    elif data_source == "from_pandas":
        df = kwargs.get(
            "df",
            pd.DataFrame({"foo": ["a", "b"], "bar": [0, 1]}),
        )
        return ray.data.from_pandas(df)
    else:
        raise ValueError(f"Unknown data_source: {data_source}")


@pytest.mark.parametrize(
    "data_source",
    ["range", "range_tensor", "from_pandas"],
    ids=["range", "range_tensor", "from_pandas"],
)
class TestCudfIterBatches:
    """Tests for iter_batches with batch_format='cudf'."""

    def test_iter_batches_returns_cudf(self, shutdown_only, data_source):
        ds = _make_dataset(data_source, shutdown_only)
        batch = next(iter(ds.iter_batches(batch_format="cudf")))
        assert isinstance(batch, cudf.DataFrame)
        assert len(batch) > 0

    def test_iter_batches_columns(self, shutdown_only, data_source):
        ds = _make_dataset(data_source, shutdown_only)
        batch = next(iter(ds.iter_batches(batch_format="cudf")))
        if data_source == "range":
            assert list(batch.columns) == ["id"]
        elif data_source == "range_tensor":
            assert "data" in batch.columns
        else:
            assert list(batch.columns) == ["foo", "bar"]
            cudf.testing.assert_eq(
                batch, cudf.DataFrame({"foo": ["a", "b"], "bar": [0, 1]})
            )


@pytest.mark.parametrize(
    "data_source",
    ["range", "range_tensor"],
    ids=["range", "range_tensor"],
)
class TestCudfTakeBatch:
    """Tests for take_batch with batch_format='cudf'."""

    def test_take_batch_returns_cudf(self, ray_start_regular_shared, data_source):
        ds = _make_dataset(data_source, None, n=10, blocks=2)
        batch = ds.take_batch(3, batch_format="cudf")
        assert isinstance(batch, cudf.DataFrame)

    def test_take_batch_data(self, ray_start_regular_shared, data_source):
        ds = _make_dataset(data_source, None, n=10, blocks=2)
        batch = ds.take_batch(3, batch_format="cudf")
        if data_source == "range":
            cudf.testing.assert_eq(batch["id"], cudf.Series([0, 1, 2], name="id"))
        else:
            # Tensor columns are stored as list-type in cudf; compare via Arrow
            assert batch["data"].to_arrow().to_pylist() == [[0], [1], [2]]


class TestCudfBatchBlocks:
    """Tests for batch_blocks with batch_format='cudf'."""

    def test_batch_blocks_cudf(self):
        blocks = block_generator(num_rows=3, num_blocks=2)
        batches = list(batch_blocks(blocks, batch_format="cudf"))

        assert len(batches) == 2
        assert isinstance(batches[0], cudf.DataFrame)
        assert isinstance(batches[1], cudf.DataFrame)
        cudf.testing.assert_eq(batches[0], cudf.DataFrame({"foo": [0, 1, 2]}))
        cudf.testing.assert_eq(batches[1], cudf.DataFrame({"foo": [3, 4, 5]}))


@pytest.mark.parametrize(
    "batch_format",
    ["cudf", "pandas", "pyarrow"],
    ids=["cudf", "pandas", "pyarrow"],
)
class TestCudfMapBatches:
    """Tests for map_batches with various batch formats (cuDF in/out)."""

    def test_map_batches_cudf_receive_and_return(
        self, ray_start_regular_shared, batch_format
    ):
        """UDF receives batches in requested format; test cudf round-trip."""
        ds = ray.data.range(10, override_num_blocks=2)

        def add_one(batch):
            if batch_format == "cudf":
                assert isinstance(batch, cudf.DataFrame)
                batch = batch.copy()
                batch["id"] = batch["id"] + 1
                return batch
            # For pandas/pyarrow input, convert to cudf, transform, return cudf
            cudf_batch = (
                cudf.from_pandas(batch)
                if batch_format == "pandas"
                else cudf.DataFrame.from_arrow(batch)
            )
            cudf_batch["id"] = cudf_batch["id"] + 1
            return cudf_batch

        result = ds.map_batches(
            add_one,
            batch_format=batch_format,
            batch_size=10,
            num_gpus=0.001,
        ).take()
        assert result == [{"id": i} for i in range(1, 11)]

    def test_map_batches_udf_returns_cudf(self, ray_start_regular_shared, batch_format):
        """UDF returns cudf.DataFrame regardless of input format (batch_to_block)."""
        if batch_format == "cudf":
            pytest.skip("Already testing cudf in/out above")
        ds = ray.data.range(5, override_num_blocks=1)

        def to_cudf_and_double(batch):
            cudf_batch = (
                cudf.from_pandas(batch)
                if batch_format == "pandas"
                else cudf.DataFrame.from_arrow(batch)
            )
            cudf_batch["id"] = cudf_batch["id"] * 2
            return cudf_batch

        result = ds.map_batches(
            to_cudf_and_double,
            batch_format=batch_format,
            batch_size=5,
            num_gpus=0.001,
        ).take()
        assert result == [{"id": 0}, {"id": 2}, {"id": 4}, {"id": 6}, {"id": 8}]


@pytest.mark.parametrize(
    "predicate_expr, test_data, expected_ids",
    [
        (col("id") > 5, None, list(range(6, 10))),
        (col("id") >= 3, None, list(range(3, 10))),
        (col("id") < 3, None, [0, 1, 2]),
        (col("id") <= 2, None, [0, 1, 2]),
        (col("id") == 4, None, [4]),
        (col("id") != 4, None, [0, 1, 2, 3, 5, 6, 7, 8, 9]),
        ((col("id") >= 2) & (col("id") < 6), None, [2, 3, 4, 5]),
        ((col("id") < 2) | (col("id") > 7), None, [0, 1, 8, 9]),
        (
            col("value").is_not_null(),
            [{"value": None}, {"value": 1}, {"value": 2}],
            [1, 2],
        ),
    ],
    ids=[
        "gt",
        "gte",
        "lt",
        "lte",
        "eq",
        "neq",
        "and",
        "or",
        "is_not_null",
    ],
)
class TestCudfFilterExpressions:
    """Tests for filter with expressions on cuDF blocks."""

    def test_filter_expr_after_map_batches_cudf(
        self, ray_start_regular_shared, predicate_expr, test_data, expected_ids
    ):
        """filter(expr=...) works on cuDF blocks from map_batches(batch_format='cudf')."""
        if test_data is not None:
            ds = ray.data.from_items(test_data)
            ds = ds.map_batches(
                lambda x: x,
                batch_format="cudf",
                batch_size=3,
                num_gpus=0.001,
            )
            result = ds.filter(expr=predicate_expr).take()
            result_ids = [r.get("value", r.get("id", r)) for r in result]
        else:
            ds = ray.data.range(10, override_num_blocks=2)
            ds = ds.map_batches(
                lambda x: x,
                batch_format="cudf",
                batch_size=10,
                num_gpus=0.001,
            )
            result = ds.filter(expr=predicate_expr).take()
            result_ids = [r["id"] for r in result]

        assert result_ids == expected_ids

    def test_map_batches_after_filter_expr(
        self, ray_start_regular_shared, predicate_expr, test_data, expected_ids
    ):
        """map_batches(batch_format='cudf') after filter(expr=...) works."""
        if test_data is not None:
            ds = ray.data.from_items(test_data)
            ds = ds.filter(expr=predicate_expr)
            ds = ds.map_batches(
                lambda x: x,
                batch_format="cudf",
                batch_size=3,
                num_gpus=0.001,
            )
            result = ds.take()
            result_ids = [r.get("value", r.get("id", r)) for r in result]
        else:
            ds = ray.data.range(10, override_num_blocks=2)
            ds = ds.filter(expr=predicate_expr)
            ds = ds.map_batches(
                lambda x: x,
                batch_format="cudf",
                batch_size=10,
                num_gpus=0.001,
            )
            result = ds.take()
            result_ids = [r["id"] for r in result]

        assert result_ids == expected_ids


class TestCudfAddColumn:
    """Tests for add_column with batch_format='cudf'."""

    def test_add_column_cudf(self, ray_start_regular_shared):
        """add_column with batch_format='cudf' adds column to cudf batches."""
        ds = ray.data.range(5).add_column(
            "doubled",
            lambda x: x["id"] * 2,
            batch_format="cudf",
            batch_size=5,
            num_gpus=0.001,
        )
        result = ds.take()
        assert result == [
            {"id": 0, "doubled": 0},
            {"id": 1, "doubled": 2},
            {"id": 2, "doubled": 4},
            {"id": 3, "doubled": 6},
            {"id": 4, "doubled": 8},
        ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
