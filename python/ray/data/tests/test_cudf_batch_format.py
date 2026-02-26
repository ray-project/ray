"""Tests for cuDF batch format support in Ray Data.

These tests require cuDF to be installed and run on GPU CI. Use pytest.importorskip
so the file is skipped when cudf is missing (e.g. local CPU runs).

Uses cudf.testing.assert_eq for comparisons (see cuDF developer guide:
https://docs.rapids.ai/api/cudf/latest/developer_guide/testing/).
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.cudf_block import (
    CudfBlockAccessor,
    CudfBlockColumnAccessor,
    CudfRow,
)
from ray.data._internal.util import is_null
from ray.data.block import BlockAccessor, BlockColumnAccessor
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


# ---------------------------------------------------------------------------
# Tests for cudf_block.py - CudfBlockColumnAccessor, CudfBlockAccessor,
# CudfRow, _zip
# ---------------------------------------------------------------------------


def _cudf_series_with_null(null_val):
    """cuDF Series [1, 2, null, 6] for testing column accessor."""
    return cudf.Series([1, 2, null_val, 6])


@pytest.mark.parametrize("null_val", [None, float("nan")])
class TestCudfBlockColumnAccessor:
    """Tests for CudfBlockColumnAccessor (cudf_block.py)."""

    @pytest.mark.parametrize("ignore_nulls, expected", [(True, 3), (False, 4)])
    def test_count(self, null_val, ignore_nulls, expected):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        assert isinstance(accessor, CudfBlockColumnAccessor)
        result = accessor.count(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected

    @pytest.mark.parametrize("ignore_nulls, expected", [(True, 9), (False, None)])
    def test_sum(self, null_val, ignore_nulls, expected):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.sum(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or (is_null(result) and is_null(expected))

    @pytest.mark.parametrize("ignore_nulls, expected", [(True, 1), (False, None)])
    def test_min(self, null_val, ignore_nulls, expected):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.min(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or (is_null(result) and is_null(expected))

    @pytest.mark.parametrize("ignore_nulls, expected", [(True, 6), (False, None)])
    def test_max(self, null_val, ignore_nulls, expected):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.max(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or (is_null(result) and is_null(expected))

    @pytest.mark.parametrize("ignore_nulls, expected", [(True, 3.0), (False, None)])
    def test_mean(self, null_val, ignore_nulls, expected):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.mean(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or (is_null(result) and is_null(expected))

    def test_sum_of_squared_diffs_from_mean(self, null_val):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.sum_of_squared_diffs_from_mean(
            ignore_nulls=True, mean=3.0, as_py=True
        )
        assert result == 14.0 or (is_null(result) and is_null(14.0))

    def test_quantile(self, null_val):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.quantile(q=0.5, ignore_nulls=True, as_py=True)
        assert result is not None  # median of [1,2,6] = 2

    def test_value_counts(self, null_val):
        arr = cudf.Series([1, 2, 2, 3])
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.value_counts()
        assert result is not None
        assert "values" in result and "counts" in result
        vals, counts = result["values"], result["counts"]
        assert set(vals) == {1, 2, 3}
        idx_2 = list(vals).index(2)
        assert counts[idx_2] == 2

    def test_unique(self, null_val):
        arr = cudf.Series([1, 2, 2, 3])
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.unique()
        assert len(result) == 3
        # cuDF unique() returns Index/Series; extract values via numpy
        vals = (
            result.values_host.tolist()
            if hasattr(result, "values_host")
            else list(result)
        )
        assert set(vals) == {1, 2, 3}

    def test_to_pylist(self, null_val):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.to_pylist()
        assert len(result) == 4
        assert result[0] == 1 and result[1] == 2 and result[3] == 6
        assert result[2] is None or (result[2] != result[2])  # NaN check

    def test_to_numpy(self, null_val):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.to_numpy()
        assert isinstance(result, np.ndarray)
        assert len(result) == 4

    def test_hash(self, null_val):
        arr = cudf.Series([1, 2, 3])
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.hash()
        assert len(result) == 3
        assert hasattr(result, "dtype")  # cudf.Series from hash_values()

    def test_dropna(self, null_val):
        arr = _cudf_series_with_null(null_val)
        accessor = BlockColumnAccessor.for_column(arr)
        result = accessor.dropna()
        assert len(result) == 3

    def test_is_composed_of_lists(self, null_val):
        arr = cudf.Series([1, 2, 3])
        accessor = BlockColumnAccessor.for_column(arr)
        assert accessor.is_composed_of_lists() is False

    def test_flatten_raises_not_implemented(self, null_val):
        arr = cudf.Series([1, 2, 3])
        accessor = BlockColumnAccessor.for_column(arr)
        with pytest.raises(NotImplementedError, match="flatten not implemented"):
            accessor.flatten()


class TestCudfBlockColumnAccessorAllNull:
    """Tests for CudfBlockColumnAccessor with all-null series."""

    def test_count_all_null(self):
        arr = cudf.Series([None, None, None], dtype="float64")
        accessor = BlockColumnAccessor.for_column(arr)
        assert accessor.count(ignore_nulls=True, as_py=True) == 0
        assert accessor.count(ignore_nulls=False, as_py=True) == 3

    @pytest.mark.parametrize("ignore_nulls", [True, False])
    def test_sum_min_max_mean_all_null(self, ignore_nulls):
        arr = cudf.Series([None, None, None], dtype="float64")
        accessor = BlockColumnAccessor.for_column(arr)
        assert accessor.sum(ignore_nulls=ignore_nulls) is None
        assert accessor.min(ignore_nulls=ignore_nulls) is None
        assert accessor.max(ignore_nulls=ignore_nulls) is None
        assert accessor.mean(ignore_nulls=ignore_nulls) is None


class TestCudfBlockAccessor:
    """Tests for CudfBlockAccessor (cudf_block.py)."""

    def _acc(self, df):
        acc = BlockAccessor.for_block(df)
        assert isinstance(acc, CudfBlockAccessor)
        return acc

    def test_to_cudf(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3, 4]})
        acc = self._acc(df)
        result = acc.to_cudf()
        cudf.testing.assert_eq(result, df)
        assert result is df  # zero-copy

    def test_to_arrow(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3, 4]})
        acc = self._acc(df)
        result = acc.to_arrow()
        assert isinstance(result, pa.Table)
        assert result.column_names == ["a", "b"]
        assert result.column("a").to_pylist() == [1, 2]

    def test_to_pandas(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3, 4]})
        acc = self._acc(df)
        result = acc.to_pandas()
        pd.testing.assert_frame_equal(result, pd.DataFrame({"a": [1, 2], "b": [3, 4]}))

    def test_fill_column_scalar(self):
        df = cudf.DataFrame({"a": [1, 2]})
        acc = self._acc(df)
        result = acc.fill_column("b", 10)
        cudf.testing.assert_eq(result, cudf.DataFrame({"a": [1, 2], "b": [10, 10]}))

    def test_fill_column_series(self):
        df = cudf.DataFrame({"a": [1, 2]})
        acc = self._acc(df)
        result = acc.fill_column("b", cudf.Series([10, 20]))
        cudf.testing.assert_eq(result, cudf.DataFrame({"a": [1, 2], "b": [10, 20]}))

    def test_slice(self):
        df = cudf.DataFrame({"a": [0, 1, 2, 3, 4]})
        acc = self._acc(df)
        result = acc.slice(1, 4, copy=False)
        cudf.testing.assert_eq(result, cudf.DataFrame({"a": [1, 2, 3]}))
        assert result.index.to_arrow().to_pylist() == [0, 1, 2], "index must be reset"

    def test_take(self):
        df = cudf.DataFrame({"a": [0, 1, 2, 3, 4]})
        acc = self._acc(df)
        result = acc.take([2, 0, 4])
        cudf.testing.assert_eq(result, cudf.DataFrame({"a": [2, 0, 4]}))
        assert result.index.to_arrow().to_pylist() == [0, 1, 2], "index must be reset"

    def test_drop(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        acc = self._acc(df)
        result = acc.drop(["b"])
        cudf.testing.assert_eq(result, cudf.DataFrame({"a": [1, 2], "c": [5, 6]}))

    def test_select(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        acc = self._acc(df)
        result = acc.select(["a", "c"])
        cudf.testing.assert_eq(result, cudf.DataFrame({"a": [1, 2], "c": [5, 6]}))

    def test_select_invalid_raises(self):
        df = cudf.DataFrame({"a": [1, 2]})
        acc = self._acc(df)
        with pytest.raises(ValueError, match="Columns must be a list"):
            acc.select([1, 2])  # type: ignore

    def test_rename_columns(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3, 4]})
        acc = self._acc(df)
        result = acc.rename_columns({"a": "x", "b": "y"})
        cudf.testing.assert_eq(result, cudf.DataFrame({"x": [1, 2], "y": [3, 4]}))

    def test_upsert_column(self):
        df = cudf.DataFrame({"a": [1, 2]})
        acc = self._acc(df)
        result = acc.upsert_column("b", cudf.Series([10, 20]))
        cudf.testing.assert_eq(result, cudf.DataFrame({"a": [1, 2], "b": [10, 20]}))

    def test_random_shuffle(self):
        df = cudf.DataFrame({"a": [1, 2, 3, 4, 5]})
        acc = self._acc(df)
        result = acc.random_shuffle(random_seed=42)
        assert len(result) == 5
        # cuDF does not support .to_list(); use .to_arrow().to_pylist()
        assert set(result["a"].to_arrow().to_pylist()) == {1, 2, 3, 4, 5}

    def test_schema(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        acc = self._acc(df)
        schema = acc.schema()
        assert schema.names == ["a", "b"]
        assert "int" in schema.types[0].lower() or "int64" in schema.types[0]
        assert "float" in schema.types[1].lower()

    def test_num_rows(self):
        df = cudf.DataFrame({"a": list(range(100))})
        acc = self._acc(df)
        assert acc.num_rows() == 100

    def test_size_bytes(self):
        df = cudf.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        acc = self._acc(df)
        size = acc.size_bytes()
        assert size > 0

    def test_column_names(self):
        df = cudf.DataFrame({"x": [1], "y": [2], "z": [3]})
        acc = self._acc(df)
        assert acc.column_names() == ["x", "y", "z"]

    def test_to_numpy_single_column(self):
        df = cudf.DataFrame({"a": [1, 2, 3]})
        acc = self._acc(df)
        result = acc.to_numpy("a")
        np.testing.assert_array_equal(result, np.array([1, 2, 3]))

    def test_to_numpy_multiple_columns(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [3, 4]})
        acc = self._acc(df)
        result = acc.to_numpy(["a", "b"])
        assert isinstance(result, dict)
        np.testing.assert_array_equal(result["a"], np.array([1, 2]))
        np.testing.assert_array_equal(result["b"], np.array([3, 4]))


class TestCudfRow:
    """Tests for CudfRow (cudf_block.py) via iter_rows."""

    def test_iter_rows_public_format(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [10, 20]})
        acc = BlockAccessor.for_block(df)
        rows = list(acc.iter_rows(public_row_format=True))
        assert rows == [{"a": 1, "b": 10}, {"a": 2, "b": 20}]

    def test_iter_rows_row_format(self):
        df = cudf.DataFrame({"a": [1, 2], "b": [10, 20]})
        acc = BlockAccessor.for_block(df)
        rows = list(acc.iter_rows(public_row_format=False))
        assert len(rows) == 2
        row0 = rows[0]
        assert isinstance(row0, CudfRow)
        assert row0["a"] == 1
        assert row0["b"] == 10
        assert row0.as_pydict() == {"a": 1, "b": 10}
        assert list(row0) == ["a", "b"]
        assert len(row0) == 2
        # Multi-key __getitem__
        assert row0[["a", "b"]] == (1, 10)


class TestCudfBlockZip:
    """Tests for CudfBlockAccessor._zip with Arrow and Pandas blocks."""

    def test_zip_with_arrow(self):
        cudf_df = cudf.DataFrame({"a": [1, 2, 3]})
        arrow_t = pa.table({"b": [10, 20, 30]})
        acc = CudfBlockAccessor(cudf_df)
        arrow_acc = BlockAccessor.for_block(arrow_t)
        result = acc._zip(arrow_acc)
        cudf.testing.assert_eq(
            result, cudf.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        )

    def test_zip_with_pandas(self):
        cudf_df = cudf.DataFrame({"a": [1, 2, 3]})
        pd_df = pd.DataFrame({"b": [10, 20, 30]})
        acc = CudfBlockAccessor(cudf_df)
        pd_acc = BlockAccessor.for_block(pd_df)
        result = acc._zip(pd_acc)
        cudf.testing.assert_eq(
            result, cudf.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
