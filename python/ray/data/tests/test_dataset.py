import itertools
import math
import os
import random
import signal
import time
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.air.util.tensor_extensions.arrow import ArrowVariableShapedTensorType
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.stats import _StatsActor
from ray.data._internal.arrow_block import ArrowRow
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.pandas_block import PandasRow
from ray.data.aggregate import AggregateFn, Count, Max, Mean, Min, Std, Sum
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.dataset import Dataset, _sliding_window
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.data.extensions.tensor_extension import (
    ArrowTensorArray,
    ArrowTensorType,
    ArrowVariableShapedTensorArray,
    TensorArray,
    TensorDtype,
)
from ray.data.row import TableRow
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


class SlowCSVDatasource(CSVDatasource):
    def _read_stream(self, f: "pa.NativeFile", path: str, **reader_args):
        for block in CSVDatasource._read_stream(self, f, path, **reader_args):
            time.sleep(3)
            yield block


# Tests that we don't block on exponential rampup when doing bulk reads.
# https://github.com/ray-project/ray/issues/20625
@pytest.mark.parametrize("block_split", [False, True])
def test_bulk_lazy_eval_split_mode(shutdown_only, block_split, tmp_path):
    ray.init(num_cpus=8)
    ctx = ray.data.context.DatasetContext.get_current()

    try:
        original = ctx.block_splitting_enabled

        ray.data.range(8, parallelism=8).write_csv(str(tmp_path))
        if not block_split:
            # Setting infinite block size effectively disables block splitting.
            ctx.target_max_block_size = float("inf")
        ds = ray.data.read_datasource(
            SlowCSVDatasource(), parallelism=8, paths=str(tmp_path)
        )

        start = time.time()
        ds.map(lambda x: x)
        delta = time.time() - start

        print("full read time", delta)
        # Should run in ~3 seconds. It takes >9 seconds if bulk read is broken.
        assert delta < 8, delta
    finally:
        ctx.block_splitting_enabled = original


@pytest.mark.parametrize("pipelined", [False, True])
def test_basic_actors(shutdown_only, pipelined):
    ray.init(num_cpus=2)
    n = 5
    ds = ray.data.range(n)
    ds = maybe_pipeline(ds, pipelined)
    assert sorted(ds.map(lambda x: x + 1, compute="actors").take()) == list(
        range(1, n + 1)
    )

    # Should still work even if num actors > num cpus.
    ds = ray.data.range(n)
    ds = maybe_pipeline(ds, pipelined)
    assert sorted(
        ds.map(lambda x: x + 1, compute=ray.data.ActorPoolStrategy(4, 4)).take()
    ) == list(range(1, n + 1))

    # Test setting custom max inflight tasks.
    ds = ray.data.range(10, parallelism=5)
    ds = maybe_pipeline(ds, pipelined)
    assert sorted(
        ds.map(
            lambda x: x + 1,
            compute=ray.data.ActorPoolStrategy(max_tasks_in_flight_per_actor=3),
        ).take()
    ) == list(range(1, 11))

    # Test invalid max tasks inflight arg.
    with pytest.raises(ValueError):
        ray.data.range(10).map(
            lambda x: x,
            compute=ray.data.ActorPoolStrategy(max_tasks_in_flight_per_actor=0),
        )

    # Test min no more than max check.
    with pytest.raises(ValueError):
        ray.data.range(10).map(lambda x: x, compute=ray.data.ActorPoolStrategy(8, 4))


@pytest.mark.parametrize("pipelined", [False, True])
def test_avoid_placement_group_capture(shutdown_only, pipelined):
    ray.init(num_cpus=2)

    @ray.remote
    def run():
        ds0 = ray.data.range(5)
        ds = maybe_pipeline(ds0, pipelined)
        assert sorted(ds.map(lambda x: x + 1).take()) == [1, 2, 3, 4, 5]
        ds = maybe_pipeline(ds0, pipelined)
        assert ds.count() == 5
        ds = maybe_pipeline(ds0, pipelined)
        assert sorted(ds.iter_rows()) == [0, 1, 2, 3, 4]

    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(
        run.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_capture_child_tasks=True
            )
        ).remote()
    )


def test_callable_classes(shutdown_only):
    ray.init(num_cpus=1)
    ds = ray.data.range(10, parallelism=10)

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return r

    # Need to specify compute explicitly.
    with pytest.raises(ValueError):
        ds.map(StatefulFn).take()

    # Need to specify actor compute strategy.
    with pytest.raises(ValueError):
        ds.map(StatefulFn, compute="tasks").take()

    # Need to specify compute explicitly.
    with pytest.raises(ValueError):
        ds.flat_map(StatefulFn).take()

    # Need to specify actor compute strategy.
    with pytest.raises(ValueError):
        ds.flat_map(StatefulFn, compute="tasks")

    # Need to specify compute explicitly.
    with pytest.raises(ValueError):
        ds.filter(StatefulFn).take()

    # Need to specify actor compute strategy.
    with pytest.raises(ValueError):
        ds.filter(StatefulFn, compute="tasks")

    # map
    actor_reuse = ds.map(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return [r]

    # flat map
    actor_reuse = ds.flat_map(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    # map batches
    actor_reuse = ds.map_batches(StatefulFn, batch_size=1, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return r > 0

    # filter
    actor_reuse = ds.filter(StatefulFn, compute="actors").take()
    assert len(actor_reuse) == 9, actor_reuse


def test_transform_failure(shutdown_only):
    ray.init(num_cpus=2)
    ds = ray.data.from_items([0, 10], parallelism=2)

    def mapper(x):
        time.sleep(x)
        raise ValueError("oops")
        return x

    with pytest.raises(ray.exceptions.RayTaskError):
        ds.map(mapper)


def test_dataset_lineage_serialization(shutdown_only):
    ray.init()
    ds = ray.data.range(10)
    ds = ds.map(lambda x: x + 1)
    ds = ds.map(lambda x: x + 1)
    ds = ds.random_shuffle()
    epoch = ds._get_epoch()
    uuid = ds._get_uuid()
    plan_uuid = ds._plan._dataset_uuid

    serialized_ds = ds.serialize_lineage()
    # Confirm that the original Dataset was properly copied before clearing/mutating.
    in_blocks = ds._plan._in_blocks
    # Should not raise.
    in_blocks._check_if_cleared()
    assert isinstance(in_blocks, LazyBlockList)
    assert in_blocks._block_partition_refs[0] is None

    ray.shutdown()
    ray.init()

    ds = Dataset.deserialize_lineage(serialized_ds)
    # Check Dataset state.
    assert ds._get_epoch() == epoch
    assert ds._get_uuid() == uuid
    assert ds._plan._dataset_uuid == plan_uuid
    # Check Dataset content.
    assert ds.count() == 10
    assert sorted(ds.take()) == list(range(2, 12))


def test_dataset_lineage_serialization_unsupported(shutdown_only):
    ray.init()
    # In-memory data sources not supported.
    ds = ray.data.from_items(list(range(10)))
    ds = ds.map(lambda x: x + 1)
    ds = ds.map(lambda x: x + 1)

    with pytest.raises(ValueError):
        ds.serialize_lineage()

    # In-memory data source unions not supported.
    ds = ray.data.from_items(list(range(10)))
    ds1 = ray.data.from_items(list(range(10, 20)))
    ds2 = ds.union(ds1)

    with pytest.raises(ValueError):
        ds2.serialize_lineage()

    # Post-lazy-read unions not supported.
    ds = ray.data.range(10).map(lambda x: x + 1)
    ds1 = ray.data.range(20).map(lambda x: 2 * x)
    ds2 = ds.union(ds1)

    with pytest.raises(ValueError):
        ds2.serialize_lineage()

    # Lazy read unions supported.
    ds = ray.data.range(10)
    ds1 = ray.data.range(20)
    ds2 = ds.union(ds1)

    serialized_ds = ds2.serialize_lineage()
    ds3 = Dataset.deserialize_lineage(serialized_ds)
    assert ds3.take(30) == list(range(10)) + list(range(20))

    # Zips not supported.
    ds = ray.data.from_items(list(range(10)))
    ds1 = ray.data.from_items(list(range(10, 20)))
    ds2 = ds.zip(ds1)

    with pytest.raises(ValueError):
        ds2.serialize_lineage()


@pytest.mark.parametrize("pipelined", [False, True])
def test_basic(ray_start_regular_shared, pipelined):
    ds0 = ray.data.range(5)
    ds = maybe_pipeline(ds0, pipelined)
    assert sorted(ds.map(lambda x: x + 1).take()) == [1, 2, 3, 4, 5]
    ds = maybe_pipeline(ds0, pipelined)
    assert ds.count() == 5
    ds = maybe_pipeline(ds0, pipelined)
    assert sorted(ds.iter_rows()) == [0, 1, 2, 3, 4]


def test_zip(ray_start_regular_shared):
    ds1 = ray.data.range(5, parallelism=5)
    ds2 = ray.data.range(5, parallelism=5).map(lambda x: x + 1)
    ds = ds1.zip(ds2)
    assert ds.schema() == tuple
    assert ds.take() == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]
    with pytest.raises(ValueError):
        ds.zip(ray.data.range(3)).fully_executed()


def test_zip_pandas(ray_start_regular_shared):
    ds1 = ray.data.from_pandas(pd.DataFrame({"col1": [1, 2], "col2": [4, 5]}))
    ds2 = ray.data.from_pandas(pd.DataFrame({"col3": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds2)
    assert ds.count() == 2
    assert "{col1: int64, col2: int64, col3: object, col4: object}" in str(ds)
    result = [r.as_pydict() for r in ds.take()]
    assert result[0] == {"col1": 1, "col2": 4, "col3": "a", "col4": "d"}

    ds3 = ray.data.from_pandas(pd.DataFrame({"col2": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds3)
    assert ds.count() == 2
    assert "{col1: int64, col2: int64, col2_1: object, col4: object}" in str(ds)
    result = [r.as_pydict() for r in ds.take()]
    assert result[0] == {"col1": 1, "col2": 4, "col2_1": "a", "col4": "d"}


def test_zip_arrow(ray_start_regular_shared):
    ds1 = ray.data.range_table(5).map(lambda r: {"id": r["value"]})
    ds2 = ray.data.range_table(5).map(
        lambda r: {"a": r["value"] + 1, "b": r["value"] + 2}
    )
    ds = ds1.zip(ds2)
    assert ds.count() == 5
    assert "{id: int64, a: int64, b: int64}" in str(ds)
    result = [r.as_pydict() for r in ds.take()]
    assert result[0] == {"id": 0, "a": 1, "b": 2}

    # Test duplicate column names.
    ds = ds1.zip(ds1).zip(ds1)
    assert ds.count() == 5
    assert "{id: int64, id_1: int64, id_2: int64}" in str(ds)
    result = [r.as_pydict() for r in ds.take()]
    assert result[0] == {"id": 0, "id_1": 0, "id_2": 0}


def test_batch_tensors(ray_start_regular_shared):
    import torch

    ds = ray.data.from_items([torch.tensor([0, 0]) for _ in range(40)], parallelism=40)
    res = "Dataset(num_blocks=40, num_rows=40, schema=<class 'torch.Tensor'>)"
    assert str(ds) == res, str(ds)
    with pytest.raises(pa.lib.ArrowInvalid):
        next(ds.iter_batches(batch_format="pyarrow"))
    df = next(ds.iter_batches(batch_format="pandas"))
    assert df.to_dict().keys() == {"value"}


def test_arrow_block_select():
    df = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13], "three": [14, 15, 16]})
    table = pa.Table.from_pandas(df)
    block_accessor = BlockAccessor.for_block(table)

    block = block_accessor.select(["two"])
    assert block.schema == pa.schema([("two", pa.int64())])
    assert block.to_pandas().equals(df[["two"]])

    block = block_accessor.select(["two", "one"])
    assert block.schema == pa.schema([("two", pa.int64()), ("one", pa.int64())])
    assert block.to_pandas().equals(df[["two", "one"]])

    with pytest.raises(ValueError):
        block = block_accessor.select([lambda x: x % 3, "two"])


def test_pandas_block_select():
    df = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13], "three": [14, 15, 16]})
    block_accessor = BlockAccessor.for_block(df)

    block = block_accessor.select(["two"])
    assert block.equals(df[["two"]])

    block = block_accessor.select(["two", "one"])
    assert block.equals(df[["two", "one"]])

    with pytest.raises(ValueError):
        block = block_accessor.select([lambda x: x % 3, "two"])


def test_simple_block_select():
    xs = list(range(100))
    block_accessor = BlockAccessor.for_block(xs)

    block = block_accessor.select([lambda x: x % 3])
    assert block == [x % 3 for x in xs]

    with pytest.raises(ValueError):
        block = block_accessor.select(["foo"])

    with pytest.raises(ValueError):
        block = block_accessor.select([])


def test_arrow_block_slice_copy():
    # Test that ArrowBlock slicing properly copies the underlying Arrow
    # table.
    def check_for_copy(table1, table2, a, b, is_copy):
        expected_slice = table1.slice(a, b - a)
        assert table2.equals(expected_slice)
        assert table2.schema == table1.schema
        assert table1.num_columns == table2.num_columns
        for col1, col2 in zip(table1.columns, table2.columns):
            assert col1.num_chunks == col2.num_chunks
            for chunk1, chunk2 in zip(col1.chunks, col2.chunks):
                bufs1 = chunk1.buffers()
                bufs2 = chunk2.buffers()
                expected_offset = 0 if is_copy else a
                assert chunk2.offset == expected_offset
                assert len(chunk2) == b - a
                if is_copy:
                    assert bufs2[1].address != bufs1[1].address
                else:
                    assert bufs2[1].address == bufs1[1].address

    n = 20
    df = pd.DataFrame(
        {"one": list(range(n)), "two": ["a"] * n, "three": [np.nan] + [1.5] * (n - 1)}
    )
    table = pa.Table.from_pandas(df)
    a, b = 5, 10
    block_accessor = BlockAccessor.for_block(table)

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    check_for_copy(table, table2, a, b, is_copy=True)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    check_for_copy(table, table2, a, b, is_copy=False)


def test_arrow_block_slice_copy_empty():
    # Test that ArrowBlock slicing properly copies the underlying Arrow
    # table when the table is empty.
    df = pd.DataFrame({"one": []})
    table = pa.Table.from_pandas(df)
    a, b = 0, 0
    expected_slice = table.slice(a, b - a)
    block_accessor = BlockAccessor.for_block(table)

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    assert table2.equals(expected_slice)
    assert table2.schema == table.schema
    assert table2.num_rows == 0

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    assert table2.equals(expected_slice)
    assert table2.schema == table.schema
    assert table2.num_rows == 0


def test_range_table(ray_start_regular_shared):
    ds = ray.data.range_table(10, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.count() == 10
    assert ds.take() == [{"value": i} for i in range(10)]

    ds = ray.data.range_table(10, parallelism=2)
    assert ds.num_blocks() == 2
    assert ds.count() == 10
    assert ds.take() == [{"value": i} for i in range(10)]


def test_tensors_basic(ray_start_regular_shared):
    # Create directly.
    tensor_shape = (3, 5)
    ds = ray.data.range_tensor(6, shape=tensor_shape, parallelism=6)
    assert str(ds) == (
        "Dataset(num_blocks=6, num_rows=6, "
        "schema={__value__: ArrowTensorType(shape=(3, 5), dtype=int64)})"
    )
    assert ds.size_bytes() == 5 * 3 * 6 * 8

    # Test row iterator yields tensors.
    for tensor in ds.iter_rows():
        assert isinstance(tensor, np.ndarray)
        assert tensor.shape == tensor_shape

    # Test batch iterator yields tensors.
    for tensor in ds.iter_batches(batch_size=2):
        assert isinstance(tensor, np.ndarray)
        assert tensor.shape == (2,) + tensor_shape

    # Native format.
    def np_mapper(arr):
        assert isinstance(arr, np.ndarray)
        return arr + 1

    res = ray.data.range_tensor(2, shape=(2, 2)).map(np_mapper).take()
    np.testing.assert_equal(res, [np.ones((2, 2)), 2 * np.ones((2, 2))])

    # Explicit NumPy format.
    res = (
        ray.data.range_tensor(2, shape=(2, 2))
        .map_batches(np_mapper, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(res, [np.ones((2, 2)), 2 * np.ones((2, 2))])

    # Pandas conversion.
    def pd_mapper(df):
        assert isinstance(df, pd.DataFrame)
        return df + 2

    res = ray.data.range_tensor(2).map_batches(pd_mapper, batch_format="pandas").take()
    np.testing.assert_equal(res, [np.array([2]), np.array([3])])

    # Arrow columns in NumPy format.
    def multi_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["a", "b", "c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return {"a": col_arrs["a"] + 1, "b": col_arrs["b"] + 1, "c": col_arrs["c"] + 1}

    # Multiple columns.
    t = pa.table(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": ArrowTensorArray.from_numpy(np.array([[1, 2], [3, 4], [5, 6]])),
        }
    )
    res = (
        ray.data.from_arrow(t)
        .map_batches(multi_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        [r.as_pydict() for r in res],
        [
            {"a": 2, "b": 5.0, "c": np.array([2, 3])},
            {"a": 3, "b": 6.0, "c": np.array([4, 5])},
            {"a": 4, "b": 7.0, "c": np.array([6, 7])},
        ],
    )

    def single_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return {"c": col_arrs["c"] + 1}

    # Single column (should still yield ndarray dict batches).
    t = t.select(["c"])
    res = (
        ray.data.from_arrow(t)
        .map_batches(single_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        [r.as_pydict() for r in res],
        [
            {"c": np.array([2, 3])},
            {"c": np.array([4, 5])},
            {"c": np.array([6, 7])},
        ],
    )

    # Pandas columns in NumPy format.
    def multi_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["a", "b", "c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return pd.DataFrame(
            {
                "a": col_arrs["a"] + 1,
                "b": col_arrs["b"] + 1,
                "c": TensorArray(col_arrs["c"] + 1),
            }
        )

    # Multiple columns.
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": TensorArray(np.array([[1, 2], [3, 4], [5, 6]])),
        }
    )
    res = (
        ray.data.from_pandas(df)
        .map_batches(multi_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        [r.as_pydict() for r in res],
        [
            {"a": 2, "b": 5.0, "c": np.array([2, 3])},
            {"a": 3, "b": 6.0, "c": np.array([4, 5])},
            {"a": 4, "b": 7.0, "c": np.array([6, 7])},
        ],
    )

    # Single column (should still yield ndarray dict batches).
    def single_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return pd.DataFrame({"c": TensorArray(col_arrs["c"] + 1)})

    df = df[["c"]]
    res = (
        ray.data.from_pandas(df)
        .map_batches(single_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        [r.as_pydict() for r in res],
        [
            {"c": np.array([2, 3])},
            {"c": np.array([4, 5])},
            {"c": np.array([6, 7])},
        ],
    )

    # Simple dataset in NumPy format.
    def mapper(arr):
        arr = np_mapper(arr)
        return arr.tolist()

    res = (
        ray.data.range(10, parallelism=2)
        .map_batches(mapper, batch_format="numpy")
        .take()
    )
    assert res == list(range(1, 11))


def test_tensors_shuffle(ray_start_regular_shared):
    # Test Arrow table representation.
    tensor_shape = (3, 5)
    ds = ray.data.range_tensor(6, shape=tensor_shape)
    shuffled_ds = ds.random_shuffle()
    shuffled = shuffled_ds.take()
    base = ds.take()
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        shuffled,
        base,
    )
    np.testing.assert_equal(
        sorted(shuffled, key=lambda arr: arr.min()),
        sorted(base, key=lambda arr: arr.min()),
    )

    # Test Pandas table representation.
    tensor_shape = (3, 5)
    ds = ray.data.range_tensor(6, shape=tensor_shape)
    ds = ds.map_batches(lambda df: df, batch_format="pandas")
    shuffled_ds = ds.random_shuffle()
    shuffled = shuffled_ds.take()
    base = ds.take()
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        shuffled,
        base,
    )
    np.testing.assert_equal(
        sorted(shuffled, key=lambda arr: arr.min()),
        sorted(base, key=lambda arr: arr.min()),
    )


def test_tensors_sort(ray_start_regular_shared):
    # Test Arrow table representation.
    t = pa.table({"a": TensorArray(np.arange(32).reshape((2, 4, 4))), "b": [1, 2]})
    ds = ray.data.from_arrow(t)
    sorted_ds = ds.sort(key="b", descending=True)
    sorted_arrs = [row["a"] for row in sorted_ds.take()]
    base = [row["a"] for row in ds.take()]
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        sorted_arrs,
        base,
    )
    np.testing.assert_equal(
        sorted_arrs,
        sorted(base, key=lambda arr: -arr.min()),
    )

    # Test Pandas table representation.
    df = pd.DataFrame({"a": TensorArray(np.arange(32).reshape((2, 4, 4))), "b": [1, 2]})
    ds = ray.data.from_pandas(df)
    sorted_ds = ds.sort(key="b", descending=True)
    sorted_arrs = [np.asarray(row["a"]) for row in sorted_ds.take()]
    base = [np.asarray(row["a"]) for row in ds.take()]
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        sorted_arrs,
        base,
    )
    np.testing.assert_equal(
        sorted_arrs,
        sorted(base, key=lambda arr: -arr.min()),
    )


def test_tensors_inferred_from_map(ray_start_regular_shared):
    # Test map.
    ds = ray.data.range(10, parallelism=10).map(lambda _: np.ones((4, 4)))
    ds.fully_executed()
    assert str(ds) == (
        "Dataset(num_blocks=10, num_rows=10, "
        "schema={__value__: ArrowTensorType(shape=(4, 4), dtype=double)})"
    )

    # Test map_batches.
    ds = ray.data.range(16, parallelism=4).map_batches(
        lambda _: np.ones((3, 4, 4)), batch_size=2
    )
    ds.fully_executed()
    assert str(ds) == (
        "Dataset(num_blocks=4, num_rows=24, "
        "schema={__value__: ArrowTensorType(shape=(4, 4), dtype=double)})"
    )

    # Test flat_map.
    ds = ray.data.range(10, parallelism=10).flat_map(
        lambda _: [np.ones((4, 4)), np.ones((4, 4))]
    )
    ds.fully_executed()
    assert str(ds) == (
        "Dataset(num_blocks=10, num_rows=20, "
        "schema={__value__: ArrowTensorType(shape=(4, 4), dtype=double)})"
    )

    # Test map_batches ndarray column.
    ds = ray.data.range(16, parallelism=4).map_batches(
        lambda _: pd.DataFrame({"a": [np.ones((4, 4))] * 3}), batch_size=2
    )
    ds.fully_executed()
    assert str(ds) == (
        "Dataset(num_blocks=4, num_rows=24, "
        "schema={a: TensorDtype(shape=(4, 4), dtype=float64)})"
    )

    ds = ray.data.range(16, parallelism=4).map_batches(
        lambda _: pd.DataFrame({"a": [np.ones((2, 2)), np.ones((3, 3))]}),
        batch_size=2,
    )
    ds.fully_executed()
    assert str(ds) == (
        "Dataset(num_blocks=4, num_rows=16, "
        "schema={a: TensorDtype(shape=(None, None), dtype=float64)})"
    )


def test_tensor_array_block_slice():
    # Test that ArrowBlock slicing works with tensor column extension type.
    def check_for_copy(table1, table2, a, b, is_copy):
        expected_slice = table1.slice(a, b - a)
        assert table2.equals(expected_slice)
        assert table2.schema == table1.schema
        assert table1.num_columns == table2.num_columns
        for col1, col2 in zip(table1.columns, table2.columns):
            assert col1.num_chunks == col2.num_chunks
            for chunk1, chunk2 in zip(col1.chunks, col2.chunks):
                bufs1 = chunk1.buffers()
                bufs2 = chunk2.buffers()
                expected_offset = 0 if is_copy else a
                assert chunk2.offset == expected_offset
                assert len(chunk2) == b - a
                if is_copy:
                    assert bufs2[1].address != bufs1[1].address
                else:
                    assert bufs2[1].address == bufs1[1].address

    n = 20
    one_arr = np.arange(4 * n).reshape(n, 2, 2)
    df = pd.DataFrame({"one": TensorArray(one_arr), "two": ["a"] * n})
    table = pa.Table.from_pandas(df)
    a, b = 5, 10
    block_accessor = BlockAccessor.for_block(table)

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(), one_arr[a:b, :, :])
    check_for_copy(table, table2, a, b, is_copy=True)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(), one_arr[a:b, :, :])
    check_for_copy(table, table2, a, b, is_copy=False)


@pytest.mark.parametrize(
    "test_data,a,b",
    [
        ([[False, True], [True, False], [True, True], [False, False]], 1, 3),
        ([[False, True], [True, False], [True, True], [False, False]], 0, 1),
        (
            [
                [False, True],
                [True, False],
                [True, True],
                [False, False],
                [True, False],
                [False, False],
                [False, True],
                [True, True],
                [False, False],
                [True, True],
                [False, True],
                [True, False],
            ],
            3,
            6,
        ),
        (
            [
                [False, True],
                [True, False],
                [True, True],
                [False, False],
                [True, False],
                [False, False],
                [False, True],
                [True, True],
                [False, False],
                [True, True],
                [False, True],
                [True, False],
            ],
            7,
            11,
        ),
        (
            [
                [False, True],
                [True, False],
                [True, True],
                [False, False],
                [True, False],
                [False, False],
                [False, True],
                [True, True],
                [False, False],
                [True, True],
                [False, True],
                [True, False],
            ],
            9,
            12,
        ),
        # Variable-shaped tensors.
        (
            [[False, True], [True, False, True], [False], [False, False, True, True]],
            1,
            3,
        ),
    ],
)
@pytest.mark.parametrize("init_with_pandas", [True, False])
def test_tensor_array_boolean_slice_pandas_roundtrip(init_with_pandas, test_data, a, b):
    is_variable_shaped = len({len(elem) for elem in test_data}) > 1
    n = len(test_data)
    test_arr = np.array(test_data)
    df = pd.DataFrame({"one": TensorArray(test_arr), "two": ["a"] * n})
    if init_with_pandas:
        table = pa.Table.from_pandas(df)
    else:
        if is_variable_shaped:
            col = ArrowVariableShapedTensorArray.from_numpy(test_arr)
        else:
            col = ArrowTensorArray.from_numpy(test_arr)
        table = pa.table({"one": col, "two": ["a"] * n})
    block_accessor = BlockAccessor.for_block(table)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    out = table2["one"].chunk(0).to_numpy()
    expected = test_arr[a:b]
    if is_variable_shaped:
        for o, e in zip(out, expected):
            np.testing.assert_array_equal(o, e)
    else:
        np.testing.assert_array_equal(out, expected)
    pd.testing.assert_frame_equal(
        table2.to_pandas().reset_index(drop=True), df[a:b].reset_index(drop=True)
    )

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    out = table2["one"].chunk(0).to_numpy()
    expected = test_arr[a:b]
    if is_variable_shaped:
        for o, e in zip(out, expected):
            np.testing.assert_array_equal(o, e)
    else:
        np.testing.assert_array_equal(out, expected)
    pd.testing.assert_frame_equal(
        table2.to_pandas().reset_index(drop=True), df[a:b].reset_index(drop=True)
    )


def test_tensors_in_tables_from_pandas(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": list(arr)})
    # Cast column to tensor extension dtype.
    df["two"] = df["two"].astype(TensorDtype(shape, np.int64))
    ds = ray.data.from_pandas([df])
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_from_pandas_variable_shaped(ray_start_regular_shared):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    outer_dim = len(arrs)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": arrs})
    # Cast column to tensor extension dtype.
    df["two"] = df["two"].astype(TensorDtype(None, np.int64))
    ds = ray.data.from_pandas(df)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(range(outer_dim), arrs))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_pandas_roundtrip(
    ray_start_regular_shared,
    enable_automatic_tensor_extension_cast,
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2)
    ds_df = ds.to_pandas()
    expected_df = df + 1
    if enable_automatic_tensor_extension_cast:
        expected_df.loc[:, "two"] = list(expected_df["two"].to_numpy())
    pd.testing.assert_frame_equal(ds_df, expected_df)


def test_tensors_in_tables_pandas_roundtrip_variable_shaped(
    ray_start_regular_shared,
    enable_automatic_tensor_extension_cast,
):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    outer_dim = len(arrs)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arrs)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2)
    ds_df = ds.to_pandas()
    expected_df = df + 1
    if enable_automatic_tensor_extension_cast:
        expected_df.loc[:, "two"] = list(expected_df["two"].to_numpy())
    pd.testing.assert_frame_equal(ds_df, expected_df)


def test_tensors_in_tables_parquet_roundtrip(ray_start_regular_shared, tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2)
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(1, outer_dim + 1)), arr + 1))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_roundtrip_variable_shaped(
    ray_start_regular_shared, tmp_path
):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    outer_dim = len(arrs)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arrs)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2)
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(1, outer_dim + 1)), [arr + 1 for arr in arrs]))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_with_schema(ray_start_regular_shared, tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    schema = pa.schema(
        [
            ("one", pa.int32()),
            ("two", ArrowTensorType(inner_shape, pa.from_numpy_dtype(arr.dtype))),
        ]
    )
    ds = ray.data.read_parquet(str(tmp_path), schema=schema)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_pickle_manual_serde(
    ray_start_regular_shared, tmp_path
):
    import pickle

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), "two": [pickle.dumps(a) for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))

    # Manually deserialize the tensor pickle bytes and cast to our tensor
    # extension type.
    def deser_mapper(batch: pd.DataFrame):
        batch["two"] = [pickle.loads(a) for a in batch["two"]]
        batch["two"] = batch["two"].astype(TensorDtype(shape, np.int64))
        return batch

    casted_ds = ds.map_batches(deser_mapper, batch_format="pandas")

    values = [[s["one"], s["two"]] for s in casted_ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)

    # Manually deserialize the pickle tensor bytes and directly cast it to a
    # TensorArray.
    def deser_mapper_direct(batch: pd.DataFrame):
        batch["two"] = TensorArray([pickle.loads(a) for a in batch["two"]])
        return batch

    casted_ds = ds.map_batches(deser_mapper_direct, batch_format="pandas")

    values = [[s["one"], s["two"]] for s in casted_ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), "two": [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))

    tensor_col_name = "two"

    # Manually deserialize the tensor bytes and cast to a TensorArray.
    def np_deser_mapper(batch: pa.Table):
        # NOTE(Clark): We use NumPy to consolidate these potentially
        # non-contiguous buffers, and to do buffer bookkeeping in general.
        np_col = np.array(
            [
                np.ndarray(inner_shape, buffer=buf.as_buffer(), dtype=arr.dtype)
                for buf in batch.column(tensor_col_name)
            ]
        )

        return batch.set_column(
            batch._ensure_integer_index(tensor_col_name),
            tensor_col_name,
            ArrowTensorArray.from_numpy(np_col),
        )

    ds = ds.map_batches(np_deser_mapper, batch_format="pyarrow")

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde_udf(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    tensor_col_name = "two"
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), tensor_col_name: [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))

    # Manually deserialize the tensor bytes and cast to a TensorArray.
    def np_deser_udf(block: pa.Table):
        # NOTE(Clark): We use NumPy to consolidate these potentially
        # non-contiguous buffers, and to do buffer bookkeeping in general.
        np_col = np.array(
            [
                np.ndarray(inner_shape, buffer=buf.as_buffer(), dtype=arr.dtype)
                for buf in block.column(tensor_col_name)
            ]
        )

        return block.set_column(
            block._ensure_integer_index(tensor_col_name),
            tensor_col_name,
            ArrowTensorArray.from_numpy(np_col),
        )

    ds = ray.data.read_parquet(str(tmp_path), _block_udf=np_deser_udf)

    assert isinstance(ds.schema().field_by_name(tensor_col_name).type, ArrowTensorType)

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde_col_schema(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    tensor_col_name = "two"
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), tensor_col_name: [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))

    def _block_udf(block: pa.Table):
        df = block.to_pandas()
        df[tensor_col_name] += 1
        return pa.Table.from_pandas(df)

    ds = ray.data.read_parquet(
        str(tmp_path),
        tensor_column_schema={tensor_col_name: (arr.dtype, inner_shape)},
        _block_udf=_block_udf,
    )

    assert isinstance(ds.schema().field_by_name(tensor_col_name).type, ArrowTensorType)

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr + 1))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


@pytest.mark.skip(
    reason=(
        "Waiting for Arrow to support registering custom ExtensionType "
        "casting kernels. See "
        "https://issues.apache.org/jira/browse/ARROW-5890#"
    )
)
def test_tensors_in_tables_parquet_bytes_with_schema(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), "two": [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    schema = pa.schema(
        [
            ("one", pa.int32()),
            ("two", ArrowTensorType(inner_shape, pa.from_numpy_dtype(arr.dtype))),
        ]
    )
    ds = ray.data.read_parquet(str(tmp_path), schema=schema)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_iter_batches(
    ray_start_regular_shared,
    enable_automatic_tensor_extension_cast,
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df1 = pd.DataFrame(
        {"one": TensorArray(arr), "two": TensorArray(arr + 1), "label": [1.0, 2.0, 3.0]}
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape)
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arr2),
            "two": TensorArray(arr2 + 1),
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2], ignore_index=True)
    if enable_automatic_tensor_extension_cast:
        df.loc[:, "one"] = list(df["one"].to_numpy())
        df.loc[:, "two"] = list(df["two"].to_numpy())
    ds = ray.data.from_pandas([df1, df2])
    batches = list(ds.iter_batches(batch_size=2))
    assert len(batches) == 3
    expected_batches = [df.iloc[:2], df.iloc[2:4], df.iloc[4:]]
    for batch, expected_batch in zip(batches, expected_batches):
        batch = batch.reset_index(drop=True)
        expected_batch = expected_batch.reset_index(drop=True)
        pd.testing.assert_frame_equal(batch, expected_batch)


@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_torch(ray_start_regular_shared, pipelined):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df1 = pd.DataFrame(
        {"one": TensorArray(arr), "two": TensorArray(arr + 1), "label": [1.0, 2.0, 3.0]}
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape)
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arr2),
            "two": TensorArray(arr2 + 1),
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(
        label_column="label", batch_size=2, unsqueeze_label_tensor=False
    )

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        features, labels = [], []
        for batch in iter(torchd):
            features.append(batch[0].numpy())
            labels.append(batch[1].numpy())
        features, labels = np.concatenate(features), np.concatenate(labels)
        values = np.stack([df["one"].to_numpy(), df["two"].to_numpy()], axis=1)
        np.testing.assert_array_equal(values, features)
        np.testing.assert_array_equal(df["label"].to_numpy(), labels)


@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_torch_mix(ray_start_regular_shared, pipelined):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df1 = pd.DataFrame(
        {
            "one": TensorArray(arr),
            "two": [1, 2, 3],
            "label": [1.0, 2.0, 3.0],
        }
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape)
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arr2),
            "two": [4, 5, 6],
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(
        label_column="label",
        feature_columns=[["one"], ["two"]],
        batch_size=2,
        unsqueeze_label_tensor=False,
        unsqueeze_feature_tensors=False,
    )

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        col1, col2, labels = [], [], []
        for batch in iter(torchd):
            col1.append(batch[0][0].numpy())
            col2.append(batch[0][1].numpy())
            labels.append(batch[1].numpy())
        col1, col2 = np.concatenate(col1), np.concatenate(col2)
        labels = np.concatenate(labels)
        np.testing.assert_array_equal(col1, np.sort(df["one"].to_numpy()))
        np.testing.assert_array_equal(col2, np.sort(df["two"].to_numpy()))
        np.testing.assert_array_equal(labels, np.sort(df["label"].to_numpy()))


@pytest.mark.skip(
    reason=(
        "Waiting for Torch to support unsqueezing and concatenating nested tensors."
    )
)
@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_torch_variable_shaped(
    ray_start_regular_shared, pipelined
):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs1 = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    df1 = pd.DataFrame(
        {
            "one": TensorArray(arrs1),
            "two": TensorArray([a + 1 for a in arrs1]),
            "label": [1.0, 2.0, 3.0],
        }
    )
    base = cumsum_sizes[-1]
    arrs2 = [
        np.arange(base + offset, base + offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arrs2),
            "two": TensorArray([a + 1 for a in arrs2]),
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(
        label_column="label", batch_size=2, unsqueeze_label_tensor=False
    )

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        features, labels = [], []
        for batch in iter(torchd):
            features.append(batch[0].numpy())
            labels.append(batch[1].numpy())
        features, labels = np.concatenate(features), np.concatenate(labels)
        values = np.stack([df["one"].to_numpy(), df["two"].to_numpy()], axis=1)
        np.testing.assert_array_equal(values, features)
        np.testing.assert_array_equal(df["label"].to_numpy(), labels)


def test_empty_shuffle(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=100)
    ds = ds.filter(lambda x: x)
    ds = ds.map_batches(lambda x: x)
    ds = ds.random_shuffle()  # Would prev. crash with AssertionError: pyarrow.Table.
    ds.show()


def test_empty_dataset(ray_start_regular_shared):
    ds = ray.data.range(0)
    assert ds.count() == 0
    assert ds.size_bytes() is None
    assert ds.schema() is None

    ds = ray.data.range(1)
    ds = ds.filter(lambda x: x > 1)
    ds.fully_executed()
    assert str(ds) == "Dataset(num_blocks=1, num_rows=0, schema=Unknown schema)"

    # Test map on empty dataset.
    ds = ray.data.from_items([])
    ds = ds.map(lambda x: x)
    ds.fully_executed()
    assert ds.count() == 0

    # Test filter on empty dataset.
    ds = ray.data.from_items([])
    ds = ds.filter(lambda: True)
    ds.fully_executed()
    assert ds.count() == 0


def test_schema(ray_start_regular_shared):
    ds = ray.data.range(10, parallelism=10)
    ds2 = ray.data.range_table(10, parallelism=10)
    ds3 = ds2.repartition(5)
    ds3.fully_executed()
    ds4 = ds3.map(lambda x: {"a": "hi", "b": 1.0}).limit(5).repartition(1)
    ds4.fully_executed()
    assert str(ds) == "Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)"
    assert str(ds2) == "Dataset(num_blocks=10, num_rows=10, schema={value: int64})"
    assert str(ds3) == "Dataset(num_blocks=5, num_rows=10, schema={value: int64})"
    assert (
        str(ds4) == "Dataset(num_blocks=1, num_rows=5, schema={a: string, b: double})"
    )


def test_schema_lazy(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=10)
    # We do not kick off the read task by default.
    assert ds._plan._in_blocks._num_computed() == 0
    schema = ds.schema()
    assert schema == int
    assert ds._plan._in_blocks._num_computed() == 1
    # Fetching the schema should not trigger execution of extra read tasks.
    assert ds._plan.execute()._num_computed() == 1


def test_count_lazy(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=10)
    # We do not kick off the read task by default.
    assert ds._plan._in_blocks._num_computed() == 0
    assert ds.count() == 100
    # Getting number of rows should not trigger execution of any read tasks
    # for ray.data.range(), as the number of rows is known beforehand.
    assert ds._plan._in_blocks._num_computed() == 0


def test_lazy_loading_exponential_rampup(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=20)
    assert ds._plan.execute()._num_computed() == 0
    assert ds.take(10) == list(range(10))
    assert ds._plan.execute()._num_computed() == 2
    assert ds.take(20) == list(range(20))
    assert ds._plan.execute()._num_computed() == 4
    assert ds.take(30) == list(range(30))
    assert ds._plan.execute()._num_computed() == 8
    assert ds.take(50) == list(range(50))
    assert ds._plan.execute()._num_computed() == 16
    assert ds.take(100) == list(range(100))
    assert ds._plan.execute()._num_computed() == 20


def test_dataset_repr(ray_start_regular_shared):
    ds = ray.data.range(10, parallelism=10)
    assert repr(ds) == "Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)"
    ds = ds.map_batches(lambda x: x)
    assert repr(ds) == (
        "MapBatches\n" "+- Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)"
    )
    ds = ds.filter(lambda x: x > 0)
    assert repr(ds) == (
        "Filter\n"
        "+- MapBatches\n"
        "   +- Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)"
    )
    ds = ds.random_shuffle()
    assert repr(ds) == (
        "RandomShuffle\n"
        "+- Filter\n"
        "   +- MapBatches\n"
        "      +- Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)"
    )
    ds.fully_executed()
    assert repr(ds) == "Dataset(num_blocks=10, num_rows=9, schema=<class 'int'>)"
    ds = ds.map_batches(lambda x: x)
    assert repr(ds) == (
        "MapBatches\n" "+- Dataset(num_blocks=10, num_rows=9, schema=<class 'int'>)"
    )
    ds1, ds2 = ds.split(2)
    assert (
        repr(ds1)
        == f"Dataset(num_blocks=5, num_rows={ds1.count()}, schema=<class 'int'>)"
    )
    assert (
        repr(ds2)
        == f"Dataset(num_blocks=5, num_rows={ds2.count()}, schema=<class 'int'>)"
    )
    ds3 = ds1.union(ds2)
    assert repr(ds3) == "Dataset(num_blocks=10, num_rows=9, schema=<class 'int'>)"
    ds = ds.zip(ds3)
    assert repr(ds) == (
        "Zip\n" "+- Dataset(num_blocks=10, num_rows=9, schema=<class 'int'>)"
    )


@pytest.mark.parametrize("lazy", [False, True])
def test_limit(ray_start_regular_shared, lazy):
    ds = ray.data.range(100, parallelism=20)
    if not lazy:
        ds = ds.fully_executed()
    for i in range(100):
        assert ds.limit(i).take(200) == list(range(i))


# NOTE: We test outside the power-of-2 range in order to ensure that we're not reading
# redundant files due to exponential ramp-up.
@pytest.mark.parametrize("limit,expected", [(10, 1), (20, 2), (30, 3), (60, 6)])
def test_limit_no_redundant_read(ray_start_regular_shared, limit, expected):
    # Test that dataset truncation eliminates redundant reads.
    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1

        def get(self):
            return self.count

        def reset(self):
            self.count = 0

    class CountingRangeDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism, n):
            def range_(i):
                ray.get(self.counter.increment.remote())
                return [list(range(parallelism * i, parallelism * i + n))]

            return [
                ReadTask(
                    lambda i=i: range_(i),
                    BlockMetadata(
                        num_rows=n,
                        size_bytes=None,
                        schema=None,
                        input_files=None,
                        exec_stats=None,
                    ),
                )
                for i in range(parallelism)
            ]

    source = CountingRangeDatasource()

    ds = ray.data.read_datasource(
        source,
        parallelism=10,
        n=10,
    )
    ds2 = ds.limit(limit)
    # Check content.
    assert ds2.take(limit) == list(range(limit))
    # Check number of read tasks launched.
    assert ray.get(source.counter.get.remote()) == expected


def test_limit_no_num_row_info(ray_start_regular_shared):
    # Test that datasources with no number-of-rows metadata available are still able to
    # be truncated, falling back to kicking off all read tasks.
    class DumbOnesDatasource(Datasource):
        def prepare_read(self, parallelism, n):
            return parallelism * [
                ReadTask(
                    lambda: [[1] * n],
                    BlockMetadata(
                        num_rows=None,
                        size_bytes=None,
                        schema=None,
                        input_files=None,
                        exec_stats=None,
                    ),
                )
            ]

    ds = ray.data.read_datasource(DumbOnesDatasource(), parallelism=10, n=10)
    for i in range(1, 100):
        assert ds.limit(i).take(100) == [1] * i


def test_convert_types(ray_start_regular_shared):
    plain_ds = ray.data.range(1)
    arrow_ds = plain_ds.map(lambda x: {"a": x})
    assert arrow_ds.take() == [{"a": 0}]
    assert "ArrowRow" in arrow_ds.map(lambda x: str(type(x))).take()[0]

    arrow_ds = ray.data.range_table(1)
    assert arrow_ds.map(lambda x: "plain_{}".format(x["value"])).take() == ["plain_0"]
    assert arrow_ds.map(lambda x: {"a": (x["value"],)}).take() == [{"a": [0]}]


def test_from_items(ray_start_regular_shared):
    ds = ray.data.from_items(["hello", "world"])
    assert ds.take() == ["hello", "world"]


def test_repartition_shuffle(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2.num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3.num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range(10000, parallelism=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


def test_repartition_noshuffle(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=False)
    assert ds2.num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [4, 4, 4, 4, 4]

    ds3 = ds2.repartition(20, shuffle=False)
    assert ds3.num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [1] * 20

    # Test num_partitions > num_rows
    ds4 = ds.repartition(40, shuffle=False)
    assert ds4.num_blocks() == 40
    blocks = ray.get(ds4.get_internal_block_refs())
    assert all(isinstance(block, list) for block in blocks), blocks
    assert ds4.sum() == 190
    assert ds4._block_num_rows() == [1] * 20 + [0] * 20

    ds5 = ray.data.range(22).repartition(4)
    assert ds5.num_blocks() == 4
    assert ds5._block_num_rows() == [5, 6, 5, 6]

    large = ray.data.range(10000, parallelism=10)
    large = large.repartition(20)
    assert large._block_num_rows() == [500] * 20


def test_repartition_shuffle_arrow(ray_start_regular_shared):
    ds = ray.data.range_table(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.count() == 20
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2.num_blocks() == 5
    assert ds2.count() == 20
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3.num_blocks() == 20
    assert ds3.count() == 20
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range_table(10000, parallelism=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


def test_take_all(ray_start_regular_shared):
    assert ray.data.range(5).take_all() == [0, 1, 2, 3, 4]

    with pytest.raises(ValueError):
        assert ray.data.range(5).take_all(4)


def test_convert_to_pyarrow(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(100)
    assert ds.to_dask().sum().compute()[0] == 4950
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds.write_parquet(path)
    assert ray.data.read_parquet(path).count() == 100


def test_pyarrow(ray_start_regular_shared):
    ds = ray.data.range_table(5)
    assert ds.map(lambda x: {"b": x["value"] + 2}).take() == [
        {"b": 2},
        {"b": 3},
        {"b": 4},
        {"b": 5},
        {"b": 6},
    ]
    assert ds.map(lambda x: {"b": x["value"] + 2}).filter(
        lambda x: x["b"] % 2 == 0
    ).take() == [{"b": 2}, {"b": 4}, {"b": 6}]
    assert ds.filter(lambda x: x["value"] == 0).flat_map(
        lambda x: [{"b": x["value"] + 2}, {"b": x["value"] + 20}]
    ).take() == [{"b": 2}, {"b": 20}]


def test_sliding_window():
    arr = list(range(10))

    # Test all windows over this iterable.
    window_sizes = list(range(1, len(arr) + 1))
    for window_size in window_sizes:
        windows = list(_sliding_window(arr, window_size))
        assert len(windows) == len(arr) - window_size + 1
        assert all(len(window) == window_size for window in windows)
        assert all(
            list(window) == arr[i : i + window_size] for i, window in enumerate(windows)
        )

    # Test window size larger than iterable length.
    windows = list(_sliding_window(arr, 15))
    assert len(windows) == 1
    assert list(windows[0]) == arr


def test_iter_rows(ray_start_regular_shared):
    # Test simple rows.
    n = 10
    ds = ray.data.range(n)
    for row, k in zip(ds.iter_rows(), range(n)):
        assert row == k

    # Test tabular rows.
    t1 = pa.Table.from_pydict({"one": [1, 2, 3], "two": [2, 3, 4]})
    t2 = pa.Table.from_pydict({"one": [4, 5, 6], "two": [5, 6, 7]})
    t3 = pa.Table.from_pydict({"one": [7, 8, 9], "two": [8, 9, 10]})
    t4 = pa.Table.from_pydict({"one": [10, 11, 12], "two": [11, 12, 13]})
    ts = [t1, t2, t3, t4]
    t = pa.concat_tables(ts)
    ds = ray.data.from_arrow(ts)

    def to_pylist(table):
        pydict = table.to_pydict()
        names = table.schema.names
        pylist = [
            {column: pydict[column][row] for column in names}
            for row in range(table.num_rows)
        ]
        return pylist

    # Default ArrowRows.
    for row, t_row in zip(ds.iter_rows(), to_pylist(t)):
        assert isinstance(row, TableRow)
        assert isinstance(row, ArrowRow)
        assert row == t_row

    # PandasRows after conversion.
    pandas_ds = ds.map_batches(lambda x: x, batch_format="pandas")
    df = t.to_pandas()
    for row, (index, df_row) in zip(pandas_ds.iter_rows(), df.iterrows()):
        assert isinstance(row, TableRow)
        assert isinstance(row, PandasRow)
        assert row == df_row.to_dict()

    # Prefetch.
    for row, t_row in zip(ds.iter_rows(prefetch_blocks=1), to_pylist(t)):
        assert isinstance(row, TableRow)
        assert isinstance(row, ArrowRow)
        assert row == t_row


def test_iter_batches_basic(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": [5, 6, 7]})
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": [8, 9, 10]})
    df4 = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13]})
    dfs = [df1, df2, df3, df4]
    ds = ray.data.from_pandas(dfs)

    # Default.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="pandas"), dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # pyarrow.Table format.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="pyarrow"), dfs):
        assert isinstance(batch, pa.Table)
        assert batch.equals(pa.Table.from_pandas(df))

    # NumPy format.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Numpy format (single column).
    ds2 = ds.select_columns(["one"])
    for batch, df in zip(ds2.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df[["one"]])

    # Test NumPy format on Arrow blocks.
    ds2 = ds.map_batches(lambda b: b, batch_size=None, batch_format="pyarrow")
    for batch, df in zip(ds2.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Test NumPy format on Arrow blocks (single column).
    ds3 = ds2.select_columns(["one"])
    for batch, df in zip(ds3.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df[["one"]])

    # Native format (deprecated).
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="native"), dfs):
        assert BlockAccessor.for_block(batch).to_pandas().equals(df)

    # Default format.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="default"), dfs):
        assert BlockAccessor.for_block(batch).to_pandas().equals(df)

    # Batch size.
    batch_size = 2
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size larger than block.
    batch_size = 4
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size larger than dataset.
    batch_size = 15
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == ds.count() for batch in batches)
    assert len(batches) == 1
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(batch_size=batch_size, drop_last=True, batch_format="pandas")
    )
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == (len(df1) + len(df2) + len(df3) + len(df4)) // batch_size
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)[:10]
    )

    # Batch size don't drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(batch_size=batch_size, drop_last=False, batch_format="pandas")
    )
    assert all(len(batch) == batch_size for batch in batches[:-1])
    assert len(batches[-1]) == (len(df1) + len(df2) + len(df3) + len(df4)) % batch_size
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Prefetch.
    batches = list(
        ds.iter_batches(prefetch_blocks=1, batch_size=None, batch_format="pandas")
    )
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    batch_size = 2
    batches = list(
        ds.iter_batches(prefetch_blocks=2, batch_size=batch_size, batch_format="pandas")
    )
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Prefetch more than number of blocks.
    batches = list(
        ds.iter_batches(
            prefetch_blocks=len(dfs), batch_size=None, batch_format="pandas"
        )
    )
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # Prefetch with ray.wait.
    context = DatasetContext.get_current()
    old_config = context.actor_prefetcher_enabled
    try:
        context.actor_prefetcher_enabled = False
        batches = list(
            ds.iter_batches(prefetch_blocks=1, batch_size=None, batch_format="pandas")
        )
        assert len(batches) == len(dfs)
        for batch, df in zip(batches, dfs):
            assert isinstance(batch, pd.DataFrame)
            assert batch.equals(df)
    finally:
        context.actor_prefetcher_enabled = old_config


def test_iter_batches_empty_block(ray_start_regular_shared):
    ds = ray.data.range(1).repartition(10)
    assert list(ds.iter_batches(batch_size=None)) == [[0]]
    assert list(ds.iter_batches(batch_size=1, local_shuffle_buffer_size=1)) == [[0]]


@pytest.mark.parametrize("pipelined", [False, True])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas", "simple"])
def test_iter_batches_local_shuffle(shutdown_only, pipelined, ds_format):
    # Input validation.
    # Batch size must be given for local shuffle.
    with pytest.raises(ValueError):
        list(
            ray.data.range(100).iter_batches(
                batch_size=None, local_shuffle_buffer_size=10
            )
        )

    def range(n, parallelism=200):
        if ds_format == "simple":
            ds = ray.data.range(n, parallelism=parallelism)
        elif ds_format == "arrow":
            ds = ray.data.range_table(n, parallelism=parallelism)
        elif ds_format == "pandas":
            ds = ray.data.range_table(n, parallelism=parallelism).map_batches(
                lambda df: df, batch_size=None, batch_format="pandas"
            )
        if pipelined:
            pipe = ds.repeat(2)
            return pipe
        else:
            return ds

    def to_row_dicts(batch):
        if isinstance(batch, pd.DataFrame):
            batch = batch.to_dict(orient="records")
        return batch

    def unbatch(batches):
        return [r for batch in batches for r in to_row_dicts(batch)]

    def sort(r):
        if ds_format == "simple":
            return sorted(r)
        return sorted(r, key=lambda v: v["value"])

    base = range(100).take_all()

    # Local shuffle.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Set seed.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
            local_shuffle_seed=0,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
            local_shuffle_seed=0,
        )
    )
    # Check randomness of shuffle.
    assert r1 == r2, (r1, r2)
    assert r1 != base
    # Check content.
    assert sort(r1) == sort(base)

    # Single block.
    r1 = unbatch(
        range(100, parallelism=1).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=1).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Single-row blocks.
    r1 = unbatch(
        range(100, parallelism=100).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=100).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Buffer larger than dataset.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=200,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=200,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Batch size larger than block.
    r1 = unbatch(
        range(100, parallelism=20).iter_batches(
            batch_size=12,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=20).iter_batches(
            batch_size=12,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Batch size larger than dataset.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=200,
            local_shuffle_buffer_size=400,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=200,
            local_shuffle_buffer_size=400,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Drop partial batches.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=7,
            local_shuffle_buffer_size=21,
            drop_last=True,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=7,
            local_shuffle_buffer_size=21,
            drop_last=True,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    # Check that partial batches were dropped.
    assert len(r1) % 7 == 0
    assert len(r2) % 7 == 0
    tmp_base = base
    if ds_format in ("arrow", "pandas"):
        r1 = [tuple(r.items()) for r in r1]
        r2 = [tuple(r.items()) for r in r2]
        tmp_base = [tuple(r.items()) for r in base]
    assert set(r1) <= set(tmp_base)
    assert set(r2) <= set(tmp_base)

    # Test empty dataset.
    ds = ray.data.from_items([])
    r1 = unbatch(ds.iter_batches(batch_size=2, local_shuffle_buffer_size=10))
    assert len(r1) == 0
    assert r1 == ds.take()


def test_iter_batches_grid(ray_start_regular_shared):
    # Tests slicing, batch combining, and partial batch dropping logic over
    # a grid of dataset, batching, and dropping configurations.
    # Grid: num_blocks x num_rows_block_1 x ... x num_rows_block_N x
    #       batch_size x drop_last
    seed = int(time.time())
    print(f"Seeding RNG for test_iter_batches_grid with: {seed}")
    random.seed(seed)
    max_num_blocks = 20
    max_num_rows_per_block = 20
    num_blocks_samples = 3
    block_sizes_samples = 3
    batch_size_samples = 3

    for num_blocks in np.random.randint(1, max_num_blocks + 1, size=num_blocks_samples):
        block_sizes_list = [
            np.random.randint(1, max_num_rows_per_block + 1, size=num_blocks)
            for _ in range(block_sizes_samples)
        ]
        for block_sizes in block_sizes_list:
            # Create the dataset with the given block sizes.
            dfs = []
            running_size = 0
            for block_size in block_sizes:
                dfs.append(
                    pd.DataFrame(
                        {"value": list(range(running_size, running_size + block_size))}
                    )
                )
                running_size += block_size
            num_rows = running_size
            ds = ray.data.from_pandas(dfs)
            for batch_size in np.random.randint(
                1, num_rows + 1, size=batch_size_samples
            ):
                for drop_last in (False, True):
                    batches = list(
                        ds.iter_batches(
                            batch_size=batch_size,
                            drop_last=drop_last,
                            batch_format="pandas",
                        )
                    )
                    if num_rows % batch_size == 0 or not drop_last:
                        # Number of batches should be equal to
                        # num_rows / batch_size,  rounded up.
                        assert len(batches) == math.ceil(num_rows / batch_size)
                        # Concatenated batches should equal the DataFrame
                        # representation of the entire dataset.
                        assert pd.concat(batches, ignore_index=True).equals(
                            ds.to_pandas()
                        )
                    else:
                        # Number of batches should be equal to
                        # num_rows / batch_size, rounded down.
                        assert len(batches) == num_rows // batch_size
                        # Concatenated batches should equal the DataFrame
                        # representation of the dataset with the partial batch
                        # remainder sliced off.
                        assert pd.concat(batches, ignore_index=True).equals(
                            ds.to_pandas()[: batch_size * (num_rows // batch_size)]
                        )
                    if num_rows % batch_size == 0 or drop_last:
                        assert all(len(batch) == batch_size for batch in batches)
                    else:
                        assert all(len(batch) == batch_size for batch in batches[:-1])
                        assert len(batches[-1]) == num_rows % batch_size


def test_lazy_loading_iter_batches_exponential_rampup(ray_start_regular_shared):
    ds = ray.data.range(32, parallelism=8)
    expected_num_blocks = [1, 2, 4, 4, 8, 8, 8, 8]
    for _, expected in zip(ds.iter_batches(batch_size=None), expected_num_blocks):
        assert ds._plan.execute()._num_computed() == expected


def test_add_column(ray_start_regular_shared):
    ds = ray.data.range(5).add_column("foo", lambda x: 1)
    assert ds.take(1) == [{"value": 0, "foo": 1}]

    ds = ray.data.range_table(5).add_column("foo", lambda x: x["value"] + 1)
    assert ds.take(1) == [{"value": 0, "foo": 1}]

    ds = ray.data.range_table(5).add_column("value", lambda x: x["value"] + 1)
    assert ds.take(2) == [{"value": 1}, {"value": 2}]

    with pytest.raises(ValueError):
        ds = ray.data.range(5).add_column("value", 0)


def test_drop_columns(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds1 = ray.data.from_pandas(df)
    ds1.write_parquet(str(tmp_path))
    ds2 = ray.data.read_parquet(str(tmp_path))

    for ds in [ds1, ds2]:
        assert ds.drop_columns(["col2"]).take(1) == [{"col1": 1, "col3": 3}]
        assert ds.drop_columns(["col1", "col3"]).take(1) == [{"col2": 2}]
        assert ds.drop_columns([]).take(1) == [{"col1": 1, "col2": 2, "col3": 3}]
        assert ds.drop_columns(["col1", "col2", "col3"]).take(1) == [{}]
        assert ds.drop_columns(["col1", "col1", "col2", "col1"]).take(1) == [
            {"col3": 3}
        ]
        # Test dropping non-existent column
        with pytest.raises(KeyError):
            ds.drop_columns(["dummy_col", "col1", "col2"]).fully_executed()


def test_select_columns(ray_start_regular_shared):
    # Test pandas and arrow
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds1 = ray.data.from_pandas(df)
    assert ds1.dataset_format() == "pandas"

    ds2 = ds1.map_batches(lambda pa: pa, batch_size=1, batch_format="pyarrow")
    assert ds2.dataset_format() == "arrow"

    for each_ds in [ds1, ds2]:
        assert each_ds.select_columns(cols=[]).take(1) == [{}]
        assert each_ds.select_columns(cols=["col1", "col2", "col3"]).take(1) == [
            {"col1": 1, "col2": 2, "col3": 3}
        ]
        assert each_ds.select_columns(cols=["col1", "col2"]).take(1) == [
            {"col1": 1, "col2": 2}
        ]
        assert each_ds.select_columns(cols=["col2", "col1"]).take(1) == [
            {"col1": 1, "col2": 2}
        ]
        # Test selecting columns with duplicates
        assert each_ds.select_columns(cols=["col1", "col2", "col2"]).schema().names == [
            "col1",
            "col2",
            "col2",
        ]
        # Test selecting a column that is not in the dataset schema
        with pytest.raises(KeyError):
            each_ds.select_columns(cols=["col1", "col2", "dummy_col"]).fully_executed()

    # Test simple
    ds3 = ray.data.range(10)
    assert ds3.dataset_format() == "simple"
    with pytest.raises(ValueError):
        ds3.select_columns(cols=[]).fully_executed()


def test_map_batches_basic(ray_start_regular_shared, tmp_path):
    # Test input validation
    ds = ray.data.range(5)
    with pytest.raises(ValueError):
        ds.map_batches(lambda x: x + 1, batch_format="pyarrow", batch_size=-1).take()

    # Set up.
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))

    # Test pandas
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda df: df + 1, batch_size=1, batch_format="pandas")
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test Pyarrow
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda pa: pa, batch_size=1, batch_format="pyarrow")
    assert ds2.dataset_format() == "arrow"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [1, 2, 3]
    values = [s["two"] for s in ds_list]
    assert values == [2, 3, 4]

    # Test batch
    size = 300
    ds = ray.data.range(size)
    ds2 = ds.map_batches(lambda df: df + 1, batch_size=17, batch_format="pandas")
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take_all()
    for i in range(size):
        # The pandas column is "value", and it originally has rows from 0~299.
        # After the map batch, it should have 1~300.
        row = ds_list[i]
        assert row["value"] == i + 1
    assert ds.count() == 300

    # Test the lambda returns different types than the batch_format
    # pandas => list block
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda df: [1], batch_size=1)
    assert ds2.dataset_format() == "simple"
    ds_list = ds2.take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # pyarrow => list block
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda df: [1], batch_size=1, batch_format="pyarrow")
    assert ds2.dataset_format() == "simple"
    ds_list = ds2.take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # Test the wrong return value raises an exception.
    ds = ray.data.read_parquet(str(tmp_path))
    with pytest.raises(ValueError):
        ds_list = ds.map_batches(
            lambda df: 1, batch_size=2, batch_format="pyarrow"
        ).take()


def test_map_batches_extra_args(ray_start_regular_shared, tmp_path):
    def put(x):
        # We only support automatic deref in the legacy backend.
        if DatasetContext.get_current().new_execution_backend:
            return x
        else:
            return ray.put(x)

    # Test input validation
    ds = ray.data.range(5)

    class Foo:
        def __call__(self, df):
            return df

    with pytest.raises(ValueError):
        # CallableClass not supported for task compute strategy, which is the default.
        ds.map_batches(Foo)

    with pytest.raises(ValueError):
        # CallableClass not supported for task compute strategy.
        ds.map_batches(Foo, compute="tasks")

    with pytest.raises(ValueError):
        # fn_constructor_args and fn_constructor_kwargs only supported for actor
        # compute strategy.
        ds.map_batches(
            lambda x: x,
            compute="tasks",
            fn_constructor_args=(1,),
            fn_constructor_kwargs={"a": 1},
        )

    with pytest.raises(ValueError):
        # fn_constructor_args and fn_constructor_kwargs only supported for callable
        # class UDFs.
        ds.map_batches(
            lambda x: x,
            compute="actors",
            fn_constructor_args=(1,),
            fn_constructor_kwargs={"a": 1},
        )

    # Set up.
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))

    # Test extra UDF args.
    # Test positional.
    def udf(batch, a):
        assert a == 1
        return batch + a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        udf,
        batch_size=1,
        batch_format="pandas",
        fn_args=(put(1),),
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test kwargs.
    def udf(batch, b=None):
        assert b == 2
        return b * batch

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        udf,
        batch_size=1,
        batch_format="pandas",
        fn_kwargs={"b": put(2)},
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 4, 6]
    values = [s["two"] for s in ds_list]
    assert values == [4, 6, 8]

    # Test both.
    def udf(batch, a, b=None):
        assert a == 1
        assert b == 2
        return b * batch + a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        udf,
        batch_size=1,
        batch_format="pandas",
        fn_args=(put(1),),
        fn_kwargs={"b": put(2)},
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [3, 5, 7]
    values = [s["two"] for s in ds_list]
    assert values == [5, 7, 9]

    # Test constructor UDF args.
    # Test positional.
    class CallableFn:
        def __init__(self, a):
            assert a == 1
            self.a = a

        def __call__(self, x):
            return x + self.a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        CallableFn,
        batch_size=1,
        batch_format="pandas",
        compute="actors",
        fn_constructor_args=(put(1),),
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test kwarg.
    class CallableFn:
        def __init__(self, b=None):
            assert b == 2
            self.b = b

        def __call__(self, x):
            return self.b * x

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        CallableFn,
        batch_size=1,
        batch_format="pandas",
        compute="actors",
        fn_constructor_kwargs={"b": put(2)},
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 4, 6]
    values = [s["two"] for s in ds_list]
    assert values == [4, 6, 8]

    # Test both.
    class CallableFn:
        def __init__(self, a, b=None):
            assert a == 1
            assert b == 2
            self.a = a
            self.b = b

        def __call__(self, x):
            return self.b * x + self.a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        CallableFn,
        batch_size=1,
        batch_format="pandas",
        compute="actors",
        fn_constructor_args=(put(1),),
        fn_constructor_kwargs={"b": put(2)},
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [3, 5, 7]
    values = [s["two"] for s in ds_list]
    assert values == [5, 7, 9]

    # Test callable chain.
    ds = ray.data.read_parquet(str(tmp_path))
    fn_constructor_args = (put(1),)
    fn_constructor_kwargs = {"b": put(2)}
    ds2 = (
        ds.lazy()
        .map_batches(
            CallableFn,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
        .map_batches(
            CallableFn,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [7, 11, 15]
    values = [s["two"] for s in ds_list]
    assert values == [11, 15, 19]

    # Test function + callable chain.
    ds = ray.data.read_parquet(str(tmp_path))
    fn_constructor_args = (put(1),)
    fn_constructor_kwargs = {"b": put(2)}
    ds2 = (
        ds.lazy()
        .map_batches(
            lambda df, a, b=None: b * df + a,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_args=(put(1),),
            fn_kwargs={"b": put(2)},
        )
        .map_batches(
            CallableFn,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
    )
    assert ds2.dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [7, 11, 15]
    values = [s["two"] for s in ds_list]
    assert values == [11, 15, 19]


def test_map_batches_actors_preserves_order(ray_start_regular_shared):
    # Test that actor compute model preserves block order.
    ds = ray.data.range(10, parallelism=5)
    assert ds.map_batches(lambda x: x, compute="actors").take() == list(range(10))


@pytest.mark.parametrize(
    "num_rows,num_blocks,batch_size",
    [
        (10, 5, 2),
        (10, 1, 10),
        (12, 3, 2),
    ],
)
def test_map_batches_batch_mutation(
    ray_start_regular_shared, num_rows, num_blocks, batch_size
):
    # Test that batch mutation works without encountering a read-only error (e.g. if the
    # batch is a zero-copy view on data in the object store).
    def mutate(df):
        df["value"] += 1
        return df

    ds = ray.data.range_table(num_rows, parallelism=num_blocks).repartition(num_blocks)
    # Convert to Pandas blocks.
    ds = ds.map_batches(lambda df: df, batch_format="pandas", batch_size=None)

    # Apply UDF that mutates the batches.
    ds = ds.map_batches(mutate, batch_size=batch_size)
    assert [row["value"] for row in ds.iter_rows()] == list(range(1, num_rows + 1))


@pytest.mark.parametrize(
    "num_rows,num_blocks,batch_size",
    [
        (10, 5, 2),
        (10, 1, 10),
        (12, 3, 2),
    ],
)
def test_map_batches_batch_zero_copy(
    ray_start_regular_shared, num_rows, num_blocks, batch_size
):
    # Test that batches are zero-copy read-only views when zero_copy_batch=True.
    def mutate(df):
        # Check that batch is read-only.
        assert not df.values.flags.writeable
        df["value"] += 1
        return df

    ds = ray.data.range_table(num_rows, parallelism=num_blocks).repartition(num_blocks)
    # Convert to Pandas blocks.
    ds = ds.map_batches(lambda df: df, batch_format="pandas", batch_size=None)
    ds.fully_executed()

    # Apply UDF that mutates the batches, which should fail since the batch is
    # read-only.
    with pytest.raises(ValueError, match="tried to mutate a zero-copy read-only batch"):
        ds = ds.map_batches(mutate, batch_size=batch_size, zero_copy_batch=True)
        ds.fully_executed()


BLOCK_BUNDLING_TEST_CASES = [
    (block_size, batch_size)
    for batch_size in range(1, 8)
    for block_size in range(1, 2 * batch_size + 1)
]


@pytest.mark.parametrize("block_size,batch_size", BLOCK_BUNDLING_TEST_CASES)
def test_map_batches_block_bundling_auto(
    ray_start_regular_shared, block_size, batch_size
):
    # Ensure that we test at least 2 batches worth of blocks.
    num_blocks = max(10, 2 * batch_size // block_size)
    ds = ray.data.range(num_blocks * block_size, parallelism=num_blocks)
    # Confirm that we have the expected number of initial blocks.
    assert ds.num_blocks() == num_blocks

    # Blocks should be bundled up to the batch size.
    ds1 = ds.map_batches(lambda x: x, batch_size=batch_size)
    ds1.fully_executed()
    assert ds1.num_blocks() == math.ceil(num_blocks / max(batch_size // block_size, 1))

    # Blocks should not be bundled up when batch_size is not specified.
    ds2 = ds.map_batches(lambda x: x)
    ds2.fully_executed()
    assert ds2.num_blocks() == num_blocks


@pytest.mark.parametrize(
    "block_sizes,batch_size,expected_num_blocks",
    [
        ([1, 2], 3, 1),
        ([2, 2, 1], 3, 2),
        ([1, 2, 3, 4], 4, 3),
        ([3, 1, 1, 3], 4, 2),
        ([2, 4, 1, 8], 4, 4),
        ([1, 1, 1, 1], 4, 1),
        ([1, 0, 3, 2], 4, 2),
        ([4, 4, 4, 4], 4, 4),
    ],
)
def test_map_batches_block_bundling_skewed_manual(
    ray_start_regular_shared, block_sizes, batch_size, expected_num_blocks
):
    num_blocks = len(block_sizes)
    ds = ray.data.from_pandas(
        [pd.DataFrame({"a": [1] * block_size}) for block_size in block_sizes]
    )
    # Confirm that we have the expected number of initial blocks.
    assert ds.num_blocks() == num_blocks
    ds = ds.map_batches(lambda x: x, batch_size=batch_size)

    # Blocks should be bundled up to the batch size.
    assert ds.num_blocks() == expected_num_blocks


BLOCK_BUNDLING_SKEWED_TEST_CASES = [
    (block_sizes, batch_size)
    for batch_size in range(1, 4)
    for num_blocks in range(1, batch_size + 1)
    for block_sizes in itertools.product(
        range(1, 2 * batch_size + 1), repeat=num_blocks
    )
]


@pytest.mark.parametrize("block_sizes,batch_size", BLOCK_BUNDLING_SKEWED_TEST_CASES)
def test_map_batches_block_bundling_skewed_auto(
    ray_start_regular_shared, block_sizes, batch_size
):
    num_blocks = len(block_sizes)
    ds = ray.data.from_pandas(
        [pd.DataFrame({"a": [1] * block_size}) for block_size in block_sizes]
    )
    # Confirm that we have the expected number of initial blocks.
    assert ds.num_blocks() == num_blocks
    ds = ds.map_batches(lambda x: x, batch_size=batch_size)
    curr = 0
    num_out_blocks = 0
    for block_size in block_sizes:
        if curr > 0 and curr + block_size > batch_size:
            num_out_blocks += 1
            curr = 0
        curr += block_size
    if curr > 0:
        num_out_blocks += 1

    # Blocks should be bundled up to the batch size.
    assert ds.num_blocks() == num_out_blocks


def test_map_with_mismatched_columns(ray_start_regular_shared):
    def bad_fn(row):
        if row > 5:
            return {"a": "hello1"}
        else:
            return {"b": "hello1"}

    def good_fn(row):
        if row > 5:
            return {"a": "hello1", "b": "hello2"}
        else:
            return {"b": "hello2", "a": "hello1"}

    ds = ray.data.range(10, parallelism=1)
    error_message = "Current row has different columns compared to previous rows."
    with pytest.raises(ValueError) as e:
        ds.map(bad_fn).fully_executed()
    assert error_message in str(e.value)
    ds_map = ds.map(good_fn)
    assert ds_map.take() == [{"a": "hello1", "b": "hello2"} for _ in range(10)]


def test_union(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)

    # Test lazy union.
    ds = ds.union(ds, ds, ds, ds)
    assert ds.num_blocks() == 50
    assert ds.count() == 100
    assert ds.sum() == 950

    ds = ds.union(ds)
    assert ds.count() == 200
    assert ds.sum() == (950 * 2)

    # Test materialized union.
    ds2 = ray.data.from_items([1, 2, 3, 4, 5])
    assert ds2.count() == 5
    ds2 = ds2.union(ds2)
    assert ds2.count() == 10
    ds2 = ds2.union(ds)
    assert ds2.count() == 210


def test_from_dask(ray_start_regular_shared):
    import dask.dataframe as dd

    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))})
    ddf = dd.from_pandas(df, npartitions=10)
    ds = ray.data.from_dask(ddf)
    dfds = ds.to_pandas()
    assert df.equals(dfds)


@pytest.mark.parametrize("ds_format", ["pandas", "arrow"])
def test_to_dask(ray_start_regular_shared, ds_format):
    from ray.util.dask import ray_dask_get

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    if ds_format == "arrow":
        ds = ds.map_batches(lambda df: df, batch_format="pyarrow", batch_size=None)
    ddf = ds.to_dask()
    meta = ddf._meta
    # Check metadata.
    assert isinstance(meta, pd.DataFrame)
    assert meta.empty
    assert list(meta.columns) == ["one", "two"]
    assert list(meta.dtypes) == [np.int64, object]
    # Explicit Dask-on-Ray
    assert df.equals(ddf.compute(scheduler=ray_dask_get))
    # Implicit Dask-on-Ray.
    assert df.equals(ddf.compute())

    # Explicit metadata.
    df1["two"] = df1["two"].astype(pd.StringDtype())
    df2["two"] = df2["two"].astype(pd.StringDtype())
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    if ds_format == "arrow":
        ds = ds.map_batches(lambda df: df, batch_format="pyarrow", batch_size=None)
    ddf = ds.to_dask(
        meta=pd.DataFrame(
            {"one": pd.Series(dtype=np.int16), "two": pd.Series(dtype=pd.StringDtype())}
        ),
    )
    meta = ddf._meta
    # Check metadata.
    assert isinstance(meta, pd.DataFrame)
    assert meta.empty
    assert list(meta.columns) == ["one", "two"]
    assert list(meta.dtypes) == [np.int16, pd.StringDtype()]
    # Explicit Dask-on-Ray
    assert df.equals(ddf.compute(scheduler=ray_dask_get))
    # Implicit Dask-on-Ray.
    assert df.equals(ddf.compute())


def test_to_dask_tensor_column_cast_pandas(ray_start_regular_shared):
    # Check that tensor column casting occurs when converting a Dataset to a Dask
    # DataFrame.
    data = np.arange(12).reshape((3, 2, 2))
    ctx = ray.data.context.DatasetContext.get_current()
    original = ctx.enable_tensor_extension_casting
    try:
        ctx.enable_tensor_extension_casting = True
        in_df = pd.DataFrame({"a": TensorArray(data)})
        ds = ray.data.from_pandas(in_df)
        dtypes = ds.schema().types
        assert len(dtypes) == 1
        assert isinstance(dtypes[0], TensorDtype)
        out_df = ds.to_dask().compute()
        assert out_df["a"].dtype.type is np.object_
        expected_df = pd.DataFrame({"a": list(data)})
        pd.testing.assert_frame_equal(out_df, expected_df)
    finally:
        ctx.enable_tensor_extension_casting = original


def test_to_dask_tensor_column_cast_arrow(ray_start_regular_shared):
    # Check that tensor column casting occurs when converting a Dataset to a Dask
    # DataFrame.
    data = np.arange(12).reshape((3, 2, 2))
    ctx = ray.data.context.DatasetContext.get_current()
    original = ctx.enable_tensor_extension_casting
    try:
        ctx.enable_tensor_extension_casting = True
        in_table = pa.table({"a": ArrowTensorArray.from_numpy(data)})
        ds = ray.data.from_arrow(in_table)
        dtype = ds.schema().field(0).type
        assert isinstance(dtype, ArrowTensorType)
        out_df = ds.to_dask().compute()
        assert out_df["a"].dtype.type is np.object_
        expected_df = pd.DataFrame({"a": list(data)})
        pd.testing.assert_frame_equal(out_df, expected_df)
    finally:
        ctx.enable_tensor_extension_casting = original


def test_from_modin(ray_start_regular_shared):
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf = mopd.DataFrame(df)
    ds = ray.data.from_modin(modf)
    dfds = ds.to_pandas()
    assert df.equals(dfds)


def test_to_modin(ray_start_regular_shared):
    # create two modin dataframes
    # one directly from a pandas dataframe, and
    # another from ray.dataset created from the original pandas dataframe
    #
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf1 = mopd.DataFrame(df)
    ds = ray.data.from_pandas([df])
    modf2 = ds.to_modin()
    assert modf1.equals(modf2)


@pytest.mark.parametrize("pipelined", [False, True])
def test_iter_tf_batches(ray_start_regular_shared, pipelined):
    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])
    ds = maybe_pipeline(ds, pipelined)

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_tf_batches(batch_size=3):
            iterations.append(
                np.stack((batch["one"], batch["two"], batch["label"]), axis=1)
            )
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


@pytest.mark.parametrize("pipelined", [False, True])
def test_iter_tf_batches_tensor_ds(ray_start_regular_shared, pipelined):
    arr1 = np.arange(12).reshape((3, 2, 2))
    arr2 = np.arange(12, 24).reshape((3, 2, 2))
    arr = np.concatenate((arr1, arr2))
    ds = ray.data.from_numpy([arr1, arr2])
    ds = maybe_pipeline(ds, pipelined)

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_tf_batches(batch_size=2):
            iterations.append(batch)
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(arr, combined_iterations)


@pytest.mark.parametrize("pipelined", [False, True])
def test_to_torch(ray_start_regular_shared, pipelined):
    import torch

    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(label_column="label", batch_size=3)

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        iterations = []
        for batch in iter(torchd):
            iterations.append(torch.cat((batch[0], batch[1]), dim=1).numpy())
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


@pytest.mark.parametrize("pipelined", [False, True])
def test_iter_torch_batches(ray_start_regular_shared, pipelined):
    import torch

    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])
    ds = maybe_pipeline(ds, pipelined)

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_torch_batches(batch_size=3):
            iterations.append(
                torch.stack(
                    (batch["one"], batch["two"], batch["label"]),
                    dim=1,
                ).numpy()
            )
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


@pytest.mark.parametrize("pipelined", [False, True])
def test_iter_torch_batches_tensor_ds(ray_start_regular_shared, pipelined):
    arr1 = np.arange(12).reshape((3, 2, 2))
    arr2 = np.arange(12, 24).reshape((3, 2, 2))
    arr = np.concatenate((arr1, arr2))
    ds = ray.data.from_numpy([arr1, arr2])
    ds = maybe_pipeline(ds, pipelined)

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_torch_batches(batch_size=2):
            iterations.append(batch.numpy())
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(arr, combined_iterations)


@pytest.mark.parametrize("input", ["single", "list", "dict"])
@pytest.mark.parametrize("force_dtype", [False, True])
@pytest.mark.parametrize("label_type", [None, "squeezed", "unsqueezed"])
def test_to_torch_feature_columns(
    ray_start_regular_shared, input, force_dtype, label_type
):
    import torch

    df1 = pd.DataFrame(
        {
            "one": [1, 2, 3],
            "two": [1.0, 2.0, 3.0],
            "three": [4.0, 5.0, 6.0],
            "label": [1.0, 2.0, 3.0],
        }
    )
    df2 = pd.DataFrame(
        {
            "one": [4, 5, 6],
            "two": [4.0, 5.0, 6.0],
            "three": [7.0, 8.0, 9.0],
            "label": [4.0, 5.0, 6.0],
        }
    )
    df3 = pd.DataFrame(
        {"one": [7, 8], "two": [7.0, 8.0], "three": [10.0, 11.0], "label": [7.0, 8.0]}
    )
    df = pd.concat([df1, df2, df3]).drop("three", axis=1)
    ds = ray.data.from_pandas([df1, df2, df3])

    feature_column_dtypes = None
    label_column_dtype = None
    if force_dtype:
        label_column_dtype = torch.long
    if input == "single":
        feature_columns = ["one", "two"]
        if force_dtype:
            feature_column_dtypes = torch.long
    elif input == "list":
        feature_columns = [["one"], ["two"]]
        if force_dtype:
            feature_column_dtypes = [torch.long, torch.long]
    elif input == "dict":
        feature_columns = {"X1": ["one"], "X2": ["two"]}
        if force_dtype:
            feature_column_dtypes = {"X1": torch.long, "X2": torch.long}

    label_column = None if label_type is None else "label"
    unsqueeze_label_tensor = label_type == "unsqueezed"

    torchd = ds.to_torch(
        label_column=label_column,
        feature_columns=feature_columns,
        feature_column_dtypes=feature_column_dtypes,
        label_column_dtype=label_column_dtype,
        unsqueeze_label_tensor=unsqueeze_label_tensor,
        batch_size=3,
    )
    iterations = []

    for batch in iter(torchd):
        features, label = batch

        if input == "single":
            assert isinstance(features, torch.Tensor)
            if force_dtype:
                assert features.dtype == torch.long
            data = features
        elif input == "list":
            assert isinstance(features, list)
            assert all(isinstance(item, torch.Tensor) for item in features)
            if force_dtype:
                assert all(item.dtype == torch.long for item in features)
            data = torch.cat(tuple(features), dim=1)
        elif input == "dict":
            assert isinstance(features, dict)
            assert all(isinstance(item, torch.Tensor) for item in features.values())
            if force_dtype:
                assert all(item.dtype == torch.long for item in features.values())
            data = torch.cat(tuple(features.values()), dim=1)

        if not label_type:
            assert label is None
        else:
            assert isinstance(label, torch.Tensor)
            if force_dtype:
                assert label.dtype == torch.long
            if unsqueeze_label_tensor:
                assert label.dim() == 2
            else:
                assert label.dim() == 1
                label = label.view(-1, 1)
            data = torch.cat((data, label), dim=1)
        iterations.append(data.numpy())

    combined_iterations = np.concatenate(iterations)
    if not label_type:
        df.drop("label", axis=1, inplace=True)
    np.testing.assert_array_equal(df.values, combined_iterations)


def test_block_builder_for_block(ray_start_regular_shared):
    # list
    builder = BlockBuilder.for_block(list())
    builder.add_block([1, 2])
    assert builder.build() == [1, 2]
    builder.add_block([3, 4])
    assert builder.build() == [1, 2, 3, 4]

    # pandas dataframe
    builder = BlockBuilder.for_block(pd.DataFrame())
    b1 = pd.DataFrame({"A": [1], "B": ["a"]})
    builder.add_block(b1)
    assert builder.build().equals(b1)
    b2 = pd.DataFrame({"A": [2, 3], "B": ["c", "d"]})
    builder.add_block(b2)
    expected = pd.DataFrame({"A": [1, 2, 3], "B": ["a", "c", "d"]})
    assert builder.build().equals(expected)

    # pyarrow table
    builder = BlockBuilder.for_block(pa.Table.from_arrays(list()))
    b1 = pa.Table.from_pydict({"A": [1], "B": ["a"]})
    builder.add_block(b1)
    builder.build().equals(b1)
    b2 = pa.Table.from_pydict({"A": [2, 3], "B": ["c", "d"]})
    builder.add_block(b2)
    expected = pa.Table.from_pydict({"A": [1, 2, 3], "B": ["a", "c", "d"]})
    builder.build().equals(expected)

    # wrong type
    with pytest.raises(TypeError):
        BlockBuilder.for_block(str())


def test_grouped_dataset_repr(ray_start_regular_shared):
    ds = ray.data.from_items([{"key": "spam"}, {"key": "ham"}, {"key": "spam"}])
    assert repr(ds.groupby("key")) == f"GroupedDataset(dataset={ds!r}, key='key')"


def test_groupby_arrow(ray_start_regular_shared, use_push_based_shuffle):
    # Test empty dataset.
    agg_ds = (
        ray.data.range_table(10)
        .filter(lambda r: r["value"] > 10)
        .groupby("value")
        .count()
    )
    assert agg_ds.count() == 0


def test_groupby_errors(ray_start_regular_shared):
    ds = ray.data.range(100)

    ds.groupby(None).count().show()  # OK
    ds.groupby(lambda x: x % 2).count().show()  # OK
    with pytest.raises(ValueError):
        ds.groupby("foo").count().show()

    ds = ray.data.range_table(100)
    ds.groupby(None).count().show()  # OK
    with pytest.raises(ValueError):
        ds.groupby(lambda x: x % 2).count().show()


def test_agg_errors(ray_start_regular_shared):
    ds = ray.data.range(100)
    from ray.data.aggregate import Max

    ds.aggregate(Max())  # OK
    ds.aggregate(Max(lambda x: x))  # OK
    with pytest.raises(ValueError):
        ds.aggregate(Max("foo"))

    ds = ray.data.range_table(100)
    ds.aggregate(Max("value"))  # OK
    with pytest.raises(ValueError):
        ds.aggregate(Max())
    with pytest.raises(ValueError):
        ds.aggregate(Max(lambda x: x))
    with pytest.raises(ValueError):
        ds.aggregate(Max("bad_field"))


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_agg_name_conflict(ray_start_regular_shared, num_parts):
    # Test aggregation name conflict.
    xs = list(range(100))
    grouped_ds = (
        ray.data.from_items([{"A": (x % 3), "B": x} for x in xs])
        .repartition(num_parts)
        .groupby("A")
    )
    agg_ds = grouped_ds.aggregate(
        AggregateFn(
            init=lambda k: [0, 0],
            accumulate_row=lambda a, r: [a[0] + r["B"], a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name="foo",
        ),
        AggregateFn(
            init=lambda k: [0, 0],
            accumulate_row=lambda a, r: [a[0] + r["B"], a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name="foo",
        ),
    )
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "foo": 49.5, "foo_2": 49.5},
        {"A": 1, "foo": 49.0, "foo_2": 49.0},
        {"A": 2, "foo": 50.0, "foo_2": 50.0},
    ]


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_count(
    ray_start_regular_shared, ds_format, num_parts, use_push_based_shuffle
):
    # Test built-in count aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    agg_ds = ds.groupby("A").count()
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "count()": 34},
        {"A": 1, "count()": 33},
        {"A": 2, "count()": 33},
    ]


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_sum(
    ray_start_regular_shared, ds_format, num_parts, use_push_based_shuffle
):
    # Test built-in sum aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").sum("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "sum(B)": 1683},
        {"A": 1, "sum(B)": 1617},
        {"A": 2, "sum(B)": 1650},
    ]

    # Test built-in sum aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.sum("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "sum(B)": 1683},
        {"A": 1, "sum(B)": 1617},
        {"A": 2, "sum(B)": 1650},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.sum("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "sum(B)": [None, 1617, 1650],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").sum("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "sum(B)": [None, None, None],
            }
        ),
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_sum(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global sum aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.sum("A") == 4950

    # Test empty dataset
    ds = ray.data.range_table(10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["value"] > 10).sum("value") is None

    # Test built-in global sum aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.sum("A") == 4950
    # Test ignore_nulls=False
    assert nan_ds.sum("A", ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.sum("A") is None
    assert nan_ds.sum("A", ignore_nulls=False) is None


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_min(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in min aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").min("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "min(B)": 0},
        {"A": 1, "min(B)": 1},
        {"A": 2, "min(B)": 2},
    ]

    # Test built-in min aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.min("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "min(B)": 0},
        {"A": 1, "min(B)": 1},
        {"A": 2, "min(B)": 2},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.min("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "min(B)": [None, 1, 2],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").min("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "min(B)": [None, None, None],
            }
        ),
        check_dtype=False,
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_min(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global min aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.min("A") == 0

    # Test empty dataset
    ds = ray.data.range_table(10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["value"] > 10).min("value") is None

    # Test built-in global min aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.min("A") == 0
    # Test ignore_nulls=False
    assert nan_ds.min("A", ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.min("A") is None
    assert nan_ds.min("A", ignore_nulls=False) is None


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_max(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in max aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").max("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "max(B)": 99},
        {"A": 1, "max(B)": 97},
        {"A": 2, "max(B)": 98},
    ]

    # Test built-in min aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.max("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "max(B)": 99},
        {"A": 1, "max(B)": 97},
        {"A": 2, "max(B)": 98},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.max("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "max(B)": [None, 97, 98],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").max("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "max(B)": [None, None, None],
            }
        ),
        check_dtype=False,
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_max(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global max aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.max("A") == 99

    # Test empty dataset
    ds = ray.data.range_table(10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["value"] > 10).max("value") is None

    # Test built-in global max aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.max("A") == 99
    # Test ignore_nulls=False
    assert nan_ds.max("A", ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.max("A") is None
    assert nan_ds.max("A", ignore_nulls=False) is None


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_mean(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").mean("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "mean(B)": 49.5},
        {"A": 1, "mean(B)": 49.0},
        {"A": 2, "mean(B)": 50.0},
    ]

    # Test built-in mean aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.mean("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "mean(B)": 49.5},
        {"A": 1, "mean(B)": 49.0},
        {"A": 2, "mean(B)": 50.0},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.mean("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "mean(B)": [None, 49.0, 50.0],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").mean("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "mean(B)": [None, None, None],
            }
        ),
        check_dtype=False,
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_mean(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global mean aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.mean("A") == 49.5

    # Test empty dataset
    ds = ray.data.range_table(10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["value"] > 10).mean("value") is None

    # Test built-in global mean aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.mean("A") == 49.5
    # Test ignore_nulls=False
    assert nan_ds.mean("A", ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.mean("A") is None
    assert nan_ds.mean("A", ignore_nulls=False) is None


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_std(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in std aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_std with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_arrow(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pyarrow")

    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    ds = ray.data.from_pandas(df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    agg_ds = ds.groupby("A").std("B")
    assert agg_ds.count() == 3
    result = agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std().to_numpy()
    np.testing.assert_array_almost_equal(result, expected)
    # ddof of 0
    ds = ray.data.from_pandas(df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    agg_ds = ds.groupby("A").std("B", ddof=0)
    assert agg_ds.count() == 3
    result = agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std(ddof=0).to_numpy()
    np.testing.assert_array_almost_equal(result, expected)

    # Test built-in std aggregation with nans
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs] + [0], "B": xs + [None]})
    ds = ray.data.from_pandas(nan_df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.std("B")
    assert nan_agg_ds.count() == 3
    result = nan_agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = nan_df.groupby("A")["B"].std().to_numpy()
    np.testing.assert_array_almost_equal(result, expected)
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.std("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    result = nan_agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = nan_df.groupby("A")["B"].std()
    expected[0] = None
    np.testing.assert_array_almost_equal(result, expected)
    # Test all nans
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs], "B": [None] * len(xs)})
    ds = ray.data.from_pandas(nan_df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    nan_agg_ds = ds.groupby("A").std("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    result = nan_agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = pd.Series([None] * 3)
    np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_std(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_std with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_arrow(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pyarrow")

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global max aggregation
    df = pd.DataFrame({"A": xs})
    ds = ray.data.from_pandas(df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert math.isclose(ds.std("A"), df["A"].std())
    assert math.isclose(ds.std("A", ddof=0), df["A"].std(ddof=0))

    # Test empty dataset
    ds = ray.data.from_pandas(pd.DataFrame({"A": []}))
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert ds.std("A") is None
    # Test edge cases
    ds = ray.data.from_pandas(pd.DataFrame({"A": [3]}))
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert ds.std("A") == 0

    # Test built-in global std aggregation with nans
    nan_df = pd.DataFrame({"A": xs + [None]})
    nan_ds = ray.data.from_pandas(nan_df).repartition(num_parts)
    if ds_format == "arrow":
        nan_ds = _to_arrow(nan_ds)
    assert math.isclose(nan_ds.std("A"), nan_df["A"].std())
    # Test ignore_nulls=False
    assert nan_ds.std("A", ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.std("A") is None
    assert nan_ds.std("A", ignore_nulls=False) is None


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_arrow_multicolumn(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation on multiple columns
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_multicolumn with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs, "C": [2 * x for x in xs]})
    agg_ds = (
        ray.data.from_pandas(df).repartition(num_parts).groupby("A").mean(["B", "C"])
    )
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "mean(B)": 49.5, "mean(C)": 99.0},
        {"A": 1, "mean(B)": 49.0, "mean(C)": 98.0},
        {"A": 2, "mean(B)": 50.0, "mean(C)": 100.0},
    ]

    # Test that unspecified agg column ==> agg on all columns except for
    # groupby keys.
    agg_ds = ray.data.from_pandas(df).repartition(num_parts).groupby("A").mean()
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "mean(B)": 49.5, "mean(C)": 99.0},
        {"A": 1, "mean(B)": 49.0, "mean(C)": 98.0},
        {"A": 2, "mean(B)": 50.0, "mean(C)": 100.0},
    ]

    # Test built-in global mean aggregation
    df = pd.DataFrame({"A": xs, "B": [2 * x for x in xs]})
    result_row = ray.data.from_pandas(df).repartition(num_parts).mean(["A", "B"])
    assert result_row["mean(A)"] == df["A"].mean()
    assert result_row["mean(B)"] == df["B"].mean()


def test_groupby_agg_bad_on(ray_start_regular_shared):
    # Test bad on for groupby aggregation
    xs = list(range(100))
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs, "C": [2 * x for x in xs]})
    # Wrong type.
    with pytest.raises(TypeError):
        ray.data.from_pandas(df).groupby("A").mean(5)
    with pytest.raises(TypeError):
        ray.data.from_pandas(df).groupby("A").mean([5])
    # Empty list.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).groupby("A").mean([])
    # Nonexistent column.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).groupby("A").mean("D")
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).groupby("A").mean(["B", "D"])
    # Columns for simple Dataset.
    with pytest.raises(ValueError):
        ray.data.from_items(xs).groupby(lambda x: x % 3 == 0).mean("A")

    # Test bad on for global aggregation
    # Wrong type.
    with pytest.raises(TypeError):
        ray.data.from_pandas(df).mean(5)
    with pytest.raises(TypeError):
        ray.data.from_pandas(df).mean([5])
    # Empty list.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).mean([])
    # Nonexistent column.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).mean("D")
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).mean(["B", "D"])
    # Columns for simple Dataset.
    with pytest.raises(ValueError):
        ray.data.from_items(xs).mean("A")


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_arrow_multi_agg(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_multi_agg with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = (
        ray.data.from_pandas(df)
        .repartition(num_parts)
        .groupby("A")
        .aggregate(
            Count(),
            Sum("B"),
            Min("B"),
            Max("B"),
            Mean("B"),
            Std("B"),
        )
    )
    assert agg_ds.count() == 3
    agg_df = agg_ds.to_pandas()
    expected_grouped = df.groupby("A")["B"]
    np.testing.assert_array_equal(agg_df["count()"].to_numpy(), [34, 33, 33])
    for agg in ["sum", "min", "max", "mean", "std"]:
        result = agg_df[f"{agg}(B)"].to_numpy()
        expected = getattr(expected_grouped, agg)().to_numpy()
        if agg == "std":
            np.testing.assert_array_almost_equal(result, expected)
        else:
            np.testing.assert_array_equal(result, expected)
    # Test built-in global std aggregation
    df = pd.DataFrame({"A": xs})

    result_row = (
        ray.data.from_pandas(df)
        .repartition(num_parts)
        .aggregate(
            Sum("A"),
            Min("A"),
            Max("A"),
            Mean("A"),
            Std("A"),
        )
    )
    for agg in ["sum", "min", "max", "mean", "std"]:
        result = result_row[f"{agg}(A)"]
        expected = getattr(df["A"], agg)()
        if agg == "std":
            assert math.isclose(result, expected)
        else:
            assert result == expected


def test_groupby_simple(ray_start_regular_shared):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple with: {seed}")
    random.seed(seed)
    parallelism = 3
    xs = [
        ("A", 2),
        ("A", 4),
        ("A", 9),
        ("B", 10),
        ("B", 20),
        ("C", 3),
        ("C", 5),
        ("C", 8),
        ("C", 12),
    ]
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)

    # Mean aggregation
    agg_ds = ds.groupby(lambda r: r[0]).aggregate(
        AggregateFn(
            init=lambda k: (0, 0),
            accumulate_row=lambda a, r: (a[0] + r[1], a[1] + 1),
            merge=lambda a1, a2: (a1[0] + a2[0], a1[1] + a2[1]),
            finalize=lambda a: a[0] / a[1],
        )
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [("A", 5), ("B", 15), ("C", 7)]

    # Test None row
    parallelism = 2
    xs = ["A", "A", "A", None, None, None, "B"]
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)
    # Count aggregation
    agg_ds = ds.groupby(lambda r: str(r)).aggregate(
        AggregateFn(
            init=lambda k: 0,
            accumulate_row=lambda a, r: a + 1,
            merge=lambda a1, a2: a1 + a2,
        )
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: str(r[0])).take(3) == [
        ("A", 3),
        ("B", 1),
        ("None", 3),
    ]

    # Test empty dataset.
    ds = ray.data.from_items([])
    agg_ds = ds.groupby(lambda r: r[0]).aggregate(
        AggregateFn(
            init=lambda k: 1 / 0,  # should never reach here
            accumulate_row=lambda a, r: 1 / 0,
            merge=lambda a1, a2: 1 / 0,
            finalize=lambda a: 1 / 0,
        )
    )
    assert agg_ds.count() == 0
    assert agg_ds.take() == ds.take()
    agg_ds = ray.data.range(10).filter(lambda r: r > 10).groupby(lambda r: r).count()
    assert agg_ds.count() == 0


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_count(ray_start_regular_shared, num_parts):
    # Test built-in count aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).count()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 34), (1, 33), (2, 33)]


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_sum(ray_start_regular_shared, num_parts):
    # Test built-in sum aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).sum()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 1683), (1, 1617), (2, 1650)]

    # Test built-in sum aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.sum()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, 1683),
        (1, 1617),
        (2, 1650),
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.sum(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, None),
        (1, 1617),
        (2, 1650),
    ]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .sum()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global sum aggregation
    assert ray.data.from_items(xs).repartition(num_parts).sum() == 4950
    assert ray.data.range(10).filter(lambda r: r > 10).sum() is None

    # Test built-in global sum aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.sum() == 4950
    # Test ignore_nulls=False
    assert nan_ds.sum(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.sum() is None


def test_map_batches_preserve_empty_blocks(ray_start_regular_shared):
    ds = ray.data.range(10, parallelism=10)
    ds = ds.map_batches(lambda x: [])
    ds = ds.map_batches(lambda x: x)
    assert ds.num_blocks() == 10, ds


def test_map_batches_combine_empty_blocks(ray_start_regular_shared):
    xs = [x % 3 for x in list(range(100))]

    # ds1 has 1 block which contains 100 rows.
    ds1 = ray.data.from_items(xs).repartition(1).sort().map_batches(lambda x: x)
    assert ds1._block_num_rows() == [100]

    # ds2 has 30 blocks, but only 3 of them are non-empty
    ds2 = (
        ray.data.from_items(xs)
        .repartition(30)
        .sort()
        .map_batches(lambda x: x, batch_size=1)
    )
    assert len(ds2._block_num_rows()) == 3
    count = sum(1 for x in ds2._block_num_rows() if x > 0)
    assert count == 3

    # The number of partitions should not affect the map_batches() result.
    assert ds1.take_all() == ds2.take_all()


def test_groupby_map_groups_for_empty_dataset(ray_start_regular_shared):
    ds = ray.data.from_items([])
    mapped = ds.groupby(lambda x: x % 3).map_groups(lambda x: [min(x) * min(x)])
    assert mapped.count() == 0
    assert mapped.take_all() == []


def test_groupby_map_groups_merging_empty_result(ray_start_regular_shared):
    ds = ray.data.from_items([1, 2, 3])
    # This needs to merge empty and non-empty results from different groups.
    mapped = ds.groupby(lambda x: x).map_groups(lambda x: [] if x == [1] else x)
    assert mapped.count() == 2
    assert mapped.take_all() == [2, 3]


def test_groupby_map_groups_merging_invalid_result(ray_start_regular_shared):
    ds = ray.data.from_items([1, 2, 3])
    grouped = ds.groupby(lambda x: x)

    # The UDF returns None, which is invalid.
    with pytest.raises(TypeError):
        grouped.map_groups(lambda x: None if x == [1] else x)


@pytest.mark.parametrize("num_parts", [1, 2, 30])
def test_groupby_map_groups_for_none_groupkey(ray_start_regular_shared, num_parts):
    ds = ray.data.from_items(list(range(100)))
    mapped = (
        ds.repartition(num_parts).groupby(None).map_groups(lambda x: [min(x) + max(x)])
    )
    assert mapped.count() == 1
    assert mapped.take_all() == [99]


@pytest.mark.parametrize("num_parts", [1, 2, 30])
def test_groupby_map_groups_returning_empty_result(ray_start_regular_shared, num_parts):
    xs = list(range(100))
    mapped = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .map_groups(lambda x: [])
    )
    assert mapped.count() == 0
    assert mapped.take_all() == []


def test_groupby_map_groups_perf(ray_start_regular_shared):
    data_list = [x % 100 for x in range(5000000)]
    ds = ray.data.from_pandas(pd.DataFrame({"A": data_list}))
    start = time.perf_counter()
    ds.groupby("A").map_groups(lambda df: df)
    end = time.perf_counter()
    # On a t3.2xlarge instance, it ran in about 5 seconds, so expecting it has to
    # finish within about 10x of that time, unless something went wrong.
    assert end - start < 60


@pytest.mark.parametrize("num_parts", [1, 2, 3, 30])
def test_groupby_map_groups_for_list(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    mapped = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .map_groups(lambda x: [min(x) * min(x)])
    )
    assert mapped.count() == 3
    assert mapped.take_all() == [0, 1, 4]


@pytest.mark.parametrize("num_parts", [1, 2, 3, 30])
def test_groupby_map_groups_for_pandas(ray_start_regular_shared, num_parts):
    df = pd.DataFrame({"A": "a a b".split(), "B": [1, 1, 3], "C": [4, 6, 5]})
    grouped = ray.data.from_pandas(df).repartition(num_parts).groupby("A")

    # Normalize the numeric columns (i.e. B and C) for each group.
    mapped = grouped.map_groups(
        lambda g: g.apply(
            lambda col: col / g[col.name].sum() if col.name in ["B", "C"] else col
        )
    )

    # The function (i.e. the normalization) performed on each group doesn't
    # aggregate rows, so we still have 3 rows.
    assert mapped.count() == 3
    expected = pd.DataFrame(
        {"A": ["a", "a", "b"], "B": [0.5, 0.5, 1.000000], "C": [0.4, 0.6, 1.0]}
    )
    assert mapped.to_pandas().equals(expected)


@pytest.mark.parametrize("num_parts", [1, 2, 3, 30])
def test_groupby_map_groups_for_arrow(ray_start_regular_shared, num_parts):
    at = pa.Table.from_pydict({"A": "a a b".split(), "B": [1, 1, 3], "C": [4, 6, 5]})
    grouped = ray.data.from_arrow(at).repartition(num_parts).groupby("A")

    # Normalize the numeric columns (i.e. B and C) for each group.
    def normalize(at: pa.Table):
        r = at.select("A")
        sb = pa.compute.sum(at.column("B")).cast(pa.float64())
        r = r.append_column("B", pa.compute.divide(at.column("B"), sb))
        sc = pa.compute.sum(at.column("C")).cast(pa.float64())
        r = r.append_column("C", pa.compute.divide(at.column("C"), sc))
        return r

    mapped = grouped.map_groups(normalize, batch_format="pyarrow")

    # The function (i.e. the normalization) performed on each group doesn't
    # aggregate rows, so we still have 3 rows.
    assert mapped.count() == 3
    expected = pa.Table.from_pydict(
        {"A": ["a", "a", "b"], "B": [0.5, 0.5, 1], "C": [0.4, 0.6, 1]}
    )
    result = pa.Table.from_pandas(mapped.to_pandas())
    assert result.equals(expected)


def test_groupby_map_groups_for_numpy(ray_start_regular_shared):
    ds = ray.data.from_items(
        [
            {"group": 1, "value": 1},
            {"group": 1, "value": 2},
            {"group": 2, "value": 3},
            {"group": 2, "value": 4},
        ]
    )

    def func(group):
        # Test output type is NumPy format.
        return {"group": group["group"] + 1, "value": group["value"] + 1}

    ds = ds.groupby("group").map_groups(func, batch_format="numpy")
    expected = pa.Table.from_pydict({"group": [2, 2, 3, 3], "value": [2, 3, 4, 5]})
    result = pa.Table.from_pandas(ds.to_pandas())
    assert result.equals(expected)


def test_groupby_map_groups_with_different_types(ray_start_regular_shared):
    ds = ray.data.from_items(
        [
            {"group": 1, "value": 1},
            {"group": 1, "value": 2},
            {"group": 2, "value": 3},
            {"group": 2, "value": 4},
        ]
    )

    def func(group):
        # Test output type is Python list, different from input type.
        return [group["value"][0]]

    ds = ds.groupby("group").map_groups(func)
    assert sorted(ds.take()) == [1, 3]


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_min(ray_start_regular_shared, num_parts):
    # Test built-in min aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).min()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 0), (1, 1), (2, 2)]

    # Test built-in min aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.min()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 0), (1, 1), (2, 2)]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.min(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, None), (1, 1), (2, 2)]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .min()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global min aggregation
    assert ray.data.from_items(xs).repartition(num_parts).min() == 0
    assert ray.data.range(10).filter(lambda r: r > 10).min() is None

    # Test built-in global min aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.min() == 0
    # Test ignore_nulls=False
    assert nan_ds.min(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.min() is None


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_max(ray_start_regular_shared, num_parts):
    # Test built-in max aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).max()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 99), (1, 97), (2, 98)]

    # Test built-in max aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.max()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 99), (1, 97), (2, 98)]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.max(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, None), (1, 97), (2, 98)]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .max()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global max aggregation
    assert ray.data.from_items(xs).repartition(num_parts).max() == 99
    assert ray.data.range(10).filter(lambda r: r > 10).max() is None

    # Test built-in global max aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.max() == 99
    # Test ignore_nulls=False
    assert nan_ds.max(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.max() is None


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_mean(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).mean()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 49.5), (1, 49.0), (2, 50.0)]

    # Test built-in mean aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.mean()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, 49.5),
        (1, 49.0),
        (2, 50.0),
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.mean(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, None),
        (1, 49.0),
        (2, 50.0),
    ]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .mean()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global mean aggregation
    assert ray.data.from_items(xs).repartition(num_parts).mean() == 49.5
    # Test empty dataset
    assert ray.data.range(10).filter(lambda r: r > 10).mean() is None

    # Test built-in global mean aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.mean() == 49.5
    # Test ignore_nulls=False
    assert nan_ds.mean(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.mean() is None


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_std(ray_start_regular_shared, num_parts):
    # Test built-in std aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_std with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).std()
    )
    assert agg_ds.count() == 3
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    expected = df.groupby("A")["B"].std()
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # ddof of 0
    agg_ds = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .std(ddof=0)
    )
    assert agg_ds.count() == 3
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    expected = df.groupby("A")["B"].std(ddof=0)
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)

    # Test built-in std aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.std()
    assert nan_agg_ds.count() == 3
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs] + [0], "B": xs + [None]})
    expected = nan_df.groupby("A")["B"].std()
    result = nan_agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.std(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    expected = nan_df.groupby("A")["B"].std()
    expected[0] = None
    result = nan_agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .std(ignore_nulls=False)
    )
    assert nan_agg_ds.count() == 1
    expected = pd.Series([None], name="B")
    expected.index.rename("A", inplace=True)
    result = nan_agg_ds.sort(key=lambda r: r[0]).take(1)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)

    # Test built-in global std aggregation
    assert math.isclose(
        ray.data.from_items(xs).repartition(num_parts).std(), pd.Series(xs).std()
    )
    # ddof of 0
    assert math.isclose(
        ray.data.from_items(xs).repartition(num_parts).std(ddof=0),
        pd.Series(xs).std(ddof=0),
    )

    # Test empty dataset
    assert ray.data.from_items([]).std() is None
    # Test edge cases
    assert ray.data.from_items([3]).std() == 0

    # Test built-in global std aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert math.isclose(nan_ds.std(), pd.Series(xs).std())
    # Test ignore_nulls=False
    assert nan_ds.std(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.std() is None


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_multilambda(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_multilambda with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items([[x, 2 * x] for x in xs])
        .repartition(num_parts)
        .groupby(lambda x: x[0] % 3)
        .mean([lambda x: x[0], lambda x: x[1]])
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, 49.5, 99.0),
        (1, 49.0, 98.0),
        (2, 50.0, 100.0),
    ]
    # Test built-in global mean aggregation
    assert ray.data.from_items([[x, 2 * x] for x in xs]).repartition(num_parts).mean(
        [lambda x: x[0], lambda x: x[1]]
    ) == (49.5, 99.0)
    assert ray.data.from_items([[x, 2 * x] for x in range(10)]).filter(
        lambda r: r[0] > 10
    ).mean([lambda x: x[0], lambda x: x[1]]) == (None, None)


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_multi_agg(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_multi_agg with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .aggregate(
            Count(),
            Sum(),
            Min(),
            Max(),
            Mean(),
            Std(),
        )
    )
    assert agg_ds.count() == 3
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, counts, sums, mins, maxs, means, stds = zip(*result)
    agg_df = pd.DataFrame(
        {
            "groups": list(groups),
            "count": list(counts),
            "sum": list(sums),
            "min": list(mins),
            "max": list(maxs),
            "mean": list(means),
            "std": list(stds),
        }
    )
    agg_df = agg_df.set_index("groups")
    df = pd.DataFrame({"groups": [x % 3 for x in xs], "B": xs})
    expected_grouped = df.groupby("groups")["B"]
    np.testing.assert_array_equal(agg_df["count"].to_numpy(), [34, 33, 33])
    for agg in ["sum", "min", "max", "mean", "std"]:
        result = agg_df[agg].to_numpy()
        expected = getattr(expected_grouped, agg)().to_numpy()
        if agg == "std":
            np.testing.assert_array_almost_equal(result, expected)
        else:
            np.testing.assert_array_equal(result, expected)
    # Test built-in global multi-aggregation
    result_row = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .aggregate(
            Sum(),
            Min(),
            Max(),
            Mean(),
            Std(),
        )
    )
    series = pd.Series(xs)
    for idx, agg in enumerate(["sum", "min", "max", "mean", "std"]):
        result = result_row[idx]
        expected = getattr(series, agg)()
        if agg == "std":
            assert math.isclose(result, expected)
        else:
            assert result == expected


def test_column_name_type_check(ray_start_regular_shared):
    df = pd.DataFrame({"1": np.random.rand(10), "a": np.random.rand(10)})
    ds = ray.data.from_pandas(df)
    expected_str = "Dataset(num_blocks=1, num_rows=10, schema={1: float64, a: float64})"
    assert str(ds) == expected_str, str(ds)
    df = pd.DataFrame({1: np.random.rand(10), "a": np.random.rand(10)})
    with pytest.raises(ValueError):
        ray.data.from_pandas(df)


def test_len(ray_start_regular_shared):
    ds = ray.data.range(1)
    with pytest.raises(AttributeError):
        len(ds)


def test_random_sample(ray_start_regular_shared):
    import math

    def ensure_sample_size_close(dataset, sample_percent=0.5):
        r1 = ds.random_sample(sample_percent)
        assert math.isclose(
            r1.count(), int(ds.count() * sample_percent), rel_tol=2, abs_tol=2
        )

    ds = ray.data.range(10, parallelism=2)
    ensure_sample_size_close(ds)

    ds = ray.data.range_table(10, parallelism=2)
    ensure_sample_size_close(ds)

    ds = ray.data.range_tensor(5, parallelism=2, shape=(2, 2))
    ensure_sample_size_close(ds)

    # imbalanced datasets
    ds1 = ray.data.range(1, parallelism=1)
    ds2 = ray.data.range(2, parallelism=1)
    ds3 = ray.data.range(3, parallelism=1)
    # noinspection PyTypeChecker
    ds = ds1.union(ds2).union(ds3)
    ensure_sample_size_close(ds)
    # Small datasets
    ds1 = ray.data.range(5, parallelism=5)
    ensure_sample_size_close(ds1)


def test_random_sample_checks(ray_start_regular_shared):
    with pytest.raises(ValueError):
        # Cannot sample -1
        ray.data.range(1).random_sample(-1)
    with pytest.raises(ValueError):
        # Cannot sample from empty dataset
        ray.data.range(0).random_sample(0.2)
    with pytest.raises(ValueError):
        # Cannot sample fraction > 1
        ray.data.range(1).random_sample(10)


def test_random_block_order_schema(ray_start_regular_shared):
    df = pd.DataFrame({"a": np.random.rand(10), "b": np.random.rand(10)})
    ds = ray.data.from_pandas(df).randomize_block_order()
    ds.schema().names == ["a", "b"]


def test_random_block_order(ray_start_regular_shared):

    # Test BlockList.randomize_block_order.
    ds = ray.data.range(12).repartition(4)
    ds = ds.randomize_block_order(seed=0)

    results = ds.take()
    expected = [6, 7, 8, 0, 1, 2, 3, 4, 5, 9, 10, 11]
    assert results == expected

    # Test LazyBlockList.randomize_block_order.
    context = DatasetContext.get_current()
    try:
        original_optimize_fuse_read_stages = context.optimize_fuse_read_stages
        context.optimize_fuse_read_stages = False

        lazy_blocklist_ds = ray.data.range(12, parallelism=4)
        lazy_blocklist_ds = lazy_blocklist_ds.randomize_block_order(seed=0)
        lazy_blocklist_results = lazy_blocklist_ds.take()
        lazy_blocklist_expected = [6, 7, 8, 0, 1, 2, 3, 4, 5, 9, 10, 11]
        assert lazy_blocklist_results == lazy_blocklist_expected
    finally:
        context.optimize_fuse_read_stages = original_optimize_fuse_read_stages


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


@pytest.mark.parametrize("pipelined", [False, True])
def test_random_shuffle(shutdown_only, pipelined, use_push_based_shuffle):
    def range(n, parallelism=200):
        ds = ray.data.range(n, parallelism=parallelism)
        if pipelined:
            pipe = ds.repeat(2)
            pipe.random_shuffle = pipe.random_shuffle_each_window
            return pipe
        else:
            return ds

    r1 = range(100).random_shuffle().take(999)
    r2 = range(100).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    r1 = range(100, parallelism=1).random_shuffle().take(999)
    r2 = range(100, parallelism=1).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    # TODO(swang): fix this
    if not use_push_based_shuffle:
        if not pipelined:
            assert range(100).random_shuffle(num_blocks=1).num_blocks() == 1
        r1 = range(100).random_shuffle(num_blocks=1).take(999)
        r2 = range(100).random_shuffle(num_blocks=1).take(999)
        assert r1 != r2, (r1, r2)

    r0 = range(100, parallelism=5).take(999)
    r1 = range(100, parallelism=5).random_shuffle(seed=0).take(999)
    r2 = range(100, parallelism=5).random_shuffle(seed=0).take(999)
    r3 = range(100, parallelism=5).random_shuffle(seed=12345).take(999)
    assert r1 == r2, (r1, r2)
    assert r1 != r0, (r1, r0)
    assert r1 != r3, (r1, r3)

    r0 = ray.data.range_table(100, parallelism=5).take(999)
    r1 = ray.data.range_table(100, parallelism=5).random_shuffle(seed=0).take(999)
    r2 = ray.data.range_table(100, parallelism=5).random_shuffle(seed=0).take(999)
    assert r1 == r2, (r1, r2)
    assert r1 != r0, (r1, r0)

    # Test move.
    ds = range(100, parallelism=2)
    r1 = ds.random_shuffle().take(999)
    if pipelined:
        with pytest.raises(RuntimeError):
            ds = ds.map(lambda x: x).take(999)
    else:
        ds = ds.map(lambda x: x).take(999)
    r2 = range(100).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    # Test empty dataset.
    ds = ray.data.from_items([])
    r1 = ds.random_shuffle()
    assert r1.count() == 0
    assert r1.take() == ds.take()


def test_random_shuffle_check_random(shutdown_only):
    # Rows from the same input should not be contiguous in the final output.
    num_files = 10
    num_rows = 100
    items = [i for i in range(num_files) for _ in range(num_rows)]
    ds = ray.data.from_items(items, parallelism=num_files)
    out = ds.random_shuffle().take(num_files * num_rows)
    for i in range(num_files):
        part = out[i * num_rows : (i + 1) * num_rows]
        seen = set()
        num_contiguous = 1
        prev = -1
        for x in part:
            if prev != x:
                prev = x
                num_contiguous = 1
            else:
                num_contiguous += 1
                assert num_contiguous < (
                    num_rows / num_files
                ), f"{part} contains too many contiguous rows from same input block"
            seen.add(x)
        assert (
            set(range(num_files)) == seen
        ), f"{part} does not contain elements from all input blocks"

    # Rows from the same input should appear in a different order in the
    # output.
    num_files = 10
    num_rows = 100
    items = [j for i in range(num_files) for j in range(num_rows)]
    ds = ray.data.from_items(items, parallelism=num_files)
    out = ds.random_shuffle().take(num_files * num_rows)
    for i in range(num_files):
        part = out[i * num_rows : (i + 1) * num_rows]
        num_increasing = 0
        prev = -1
        for x in part:
            if x >= prev:
                num_increasing += 1
            else:
                assert num_increasing < (
                    num_rows / num_files
                ), f"{part} contains non-shuffled rows from input blocks"
                num_increasing = 0
            prev = x


def test_unsupported_pyarrow_versions_check(shutdown_only, unsupported_pyarrow_version):
    # Test that unsupported pyarrow versions cause an error to be raised upon the
    # initial pyarrow use.
    ray.init(runtime_env={"pip": [f"pyarrow=={unsupported_pyarrow_version}"]})

    # Test Arrow-native creation APIs.
    # Test range_table.
    with pytest.raises(ImportError):
        ray.data.range_table(10).take_all()

    # Test from_arrow.
    with pytest.raises(ImportError):
        ray.data.from_arrow(pa.table({"a": [1, 2]}))

    # Test read_parquet.
    with pytest.raises(ImportError):
        ray.data.read_parquet("example://iris.parquet").take_all()

    # Test from_numpy (we use Arrow for representing the tensors).
    with pytest.raises(ImportError):
        ray.data.from_numpy(np.arange(12).reshape((3, 2, 2)))


def test_unsupported_pyarrow_versions_check_disabled(
    shutdown_only,
    unsupported_pyarrow_version,
    disable_pyarrow_version_check,
):
    # Test that unsupported pyarrow versions DO NOT cause an error to be raised upon the
    # initial pyarrow use when the version check is disabled.
    ray.init(
        runtime_env={
            "pip": [f"pyarrow=={unsupported_pyarrow_version}"],
            "env_vars": {"RAY_DISABLE_PYARROW_VERSION_CHECK": "1"},
        },
    )

    # Test Arrow-native creation APIs.
    # Test range_table.
    try:
        ray.data.range_table(10).take_all()
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")

    # Test from_arrow.
    try:
        ray.data.from_arrow(pa.table({"a": [1, 2]}))
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")

    # Test read_parquet.
    try:
        ray.data.read_parquet("example://iris.parquet").take_all()
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")

    # Test from_numpy (we use Arrow for representing the tensors).
    try:
        ray.data.from_numpy(np.arange(12).reshape((3, 2, 2)))
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")


def test_random_shuffle_with_custom_resource(ray_start_cluster):
    cluster = ray_start_cluster
    # Create two nodes which have different custom resources.
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    # Run dataset in "bar" nodes.
    ds = ray.data.read_parquet(
        "example://parquet_images_mini",
        parallelism=2,
        ray_remote_args={"resources": {"bar": 1}},
    )
    ds = ds.random_shuffle(resources={"bar": 1}).fully_executed()
    assert "1 nodes used" in ds.stats()
    assert "2 nodes used" not in ds.stats()


def test_read_write_local_node_ray_client(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=4)
    cluster.head_node._ray_params.ray_client_server_port = "10004"
    cluster.head_node.start_ray_client_server()
    address = "ray://localhost:10004"

    import tempfile

    data_path = tempfile.mkdtemp()
    df = pd.DataFrame({"one": list(range(0, 10)), "two": list(range(10, 20))})
    path = os.path.join(data_path, "test.parquet")
    df.to_parquet(path)

    # Read/write from Ray Client will result in error.
    ray.init(address)
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet("local://" + path).fully_executed()
    ds = ray.data.from_pandas(df)
    with pytest.raises(ValueError):
        ds.write_parquet("local://" + data_path).fully_executed()


def test_read_write_local_node(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar:2": 100}, num_cpus=10)
    cluster.add_node(resources={"bar:3": 100}, num_cpus=10)

    ray.init(cluster.address)

    import os
    import tempfile

    data_path = tempfile.mkdtemp()
    num_files = 5
    for idx in range(num_files):
        df = pd.DataFrame(
            {"one": list(range(idx, idx + 10)), "two": list(range(idx + 10, idx + 20))}
        )
        path = os.path.join(data_path, f"test{idx}.parquet")
        df.to_parquet(path)

    ctx = ray.data.context.DatasetContext.get_current()
    ctx.read_write_local_node = True

    def check_dataset_is_local(ds):
        blocks = ds.get_internal_block_refs()
        assert len(blocks) == num_files
        ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
        location_data = ray.experimental.get_object_locations(blocks)
        locations = []
        for block in blocks:
            locations.extend(location_data[block]["node_ids"])
        assert set(locations) == {ray.get_runtime_context().get_node_id()}

    local_path = "local://" + data_path
    # Plain read.
    ds = ray.data.read_parquet(local_path).fully_executed()
    check_dataset_is_local(ds)

    # SPREAD scheduling got overridden when read local scheme.
    ds = ray.data.read_parquet(
        local_path, ray_remote_args={"scheduling_strategy": "SPREAD"}
    ).fully_executed()
    check_dataset_is_local(ds)

    # With fusion.
    ds = ray.data.read_parquet(local_path).map(lambda x: x).fully_executed()
    check_dataset_is_local(ds)

    # Write back to local scheme.
    output = os.path.join(local_path, "test_read_write_local_node")
    ds.write_parquet(output)
    assert "1 nodes used" in ds.stats(), ds.stats()
    ray.data.read_parquet(output).take_all() == ds.take_all()

    # Mixing paths of local and non-local scheme is invalid.
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet(
            [local_path + "/test1.parquet", data_path + "/test2.parquet"]
        ).fully_executed()
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet(
            [local_path + "/test1.parquet", "example://iris.parquet"]
        ).fully_executed()
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet(
            ["example://iris.parquet", local_path + "/test1.parquet"]
        ).fully_executed()


def test_random_shuffle_spread(ray_start_cluster, use_push_based_shuffle):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar:2": 100}, num_cpus=10)
    cluster.add_node(resources={"bar:3": 100}, num_cpus=0)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    node1_id = ray.get(get_node_id.options(resources={"bar:1": 1}).remote())
    node2_id = ray.get(get_node_id.options(resources={"bar:2": 1}).remote())

    ds = ray.data.range(100, parallelism=2).random_shuffle()
    blocks = ds.get_internal_block_refs()
    ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
    location_data = ray.experimental.get_object_locations(blocks)
    locations = []
    for block in blocks:
        locations.extend(location_data[block]["node_ids"])
    assert "2 nodes used" in ds.stats()

    if not use_push_based_shuffle:
        # We don't check this for push-based shuffle since it will try to
        # colocate reduce tasks to improve locality.
        assert set(locations) == {node1_id, node2_id}


def test_parquet_read_spread(ray_start_cluster, tmp_path):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar:2": 100}, num_cpus=10)
    cluster.add_node(resources={"bar:3": 100}, num_cpus=0)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    node1_id = ray.get(get_node_id.options(resources={"bar:1": 1}).remote())
    node2_id = ray.get(get_node_id.options(resources={"bar:2": 1}).remote())

    data_path = str(tmp_path)
    df1 = pd.DataFrame({"one": list(range(100)), "two": list(range(100, 200))})
    path1 = os.path.join(data_path, "test1.parquet")
    df1.to_parquet(path1)
    df2 = pd.DataFrame({"one": list(range(300, 400)), "two": list(range(400, 500))})
    path2 = os.path.join(data_path, "test2.parquet")
    df2.to_parquet(path2)

    ds = ray.data.read_parquet(data_path)

    # Force reads.
    blocks = ds.get_internal_block_refs()
    assert len(blocks) == 2

    ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
    location_data = ray.experimental.get_object_locations(blocks)
    locations = []
    for block in blocks:
        locations.extend(location_data[block]["node_ids"])
    assert set(locations) == {node1_id, node2_id}


def test_stats_actor_cap_num_stats(ray_start_cluster):
    actor = _StatsActor.remote(3)
    metadatas = []
    task_idx = 0
    for uuid in range(3):
        metadatas.append(
            BlockMetadata(
                num_rows=uuid,
                size_bytes=None,
                schema=None,
                input_files=None,
                exec_stats=None,
            )
        )
        num_stats = uuid + 1
        actor.record_start.remote(uuid)
        assert ray.get(actor._get_stats_dict_size.remote()) == (
            num_stats,
            num_stats - 1,
            num_stats - 1,
        )
        actor.record_task.remote(uuid, task_idx, [metadatas[-1]])
        assert ray.get(actor._get_stats_dict_size.remote()) == (
            num_stats,
            num_stats,
            num_stats,
        )
    for uuid in range(3):
        assert ray.get(actor.get.remote(uuid))[0][task_idx] == [metadatas[uuid]]
    # Add the fourth stats to exceed the limit.
    actor.record_start.remote(3)
    # The first stats (with uuid=0) should have been purged.
    assert ray.get(actor.get.remote(0))[0] == {}
    # The start_time has 3 entries because we just added it above with record_start().
    assert ray.get(actor._get_stats_dict_size.remote()) == (3, 2, 2)


@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


class FlakyCSVDatasource(CSVDatasource):
    def __init__(self):
        self.counter = Counter.remote()

    def _read_stream(self, f: "pa.NativeFile", path: str, **reader_args):
        count = self.counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            for block in CSVDatasource._read_stream(self, f, path, **reader_args):
                yield block

    def _write_block(self, f: "pa.NativeFile", block: BlockAccessor, **writer_args):
        count = self.counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            CSVDatasource._write_block(self, f, block, **writer_args)


def test_dataset_retry_exceptions(ray_start_regular, local_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(local_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options={})
    ds1 = ray.data.read_datasource(FlakyCSVDatasource(), parallelism=1, paths=path1)
    ds1.write_datasource(FlakyCSVDatasource(), path=local_path, dataset_uuid="data")
    assert df1.equals(
        pd.read_csv(os.path.join(local_path, "data_000000.csv"), storage_options={})
    )

    counter = Counter.remote()

    def flaky_mapper(x):
        count = counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            return ray.get(count)

    assert sorted(ds1.map(flaky_mapper).take()) == [2, 3, 4]

    with pytest.raises(ValueError):
        ray.data.read_datasource(
            FlakyCSVDatasource(),
            parallelism=1,
            paths=path1,
            ray_remote_args={"retry_exceptions": False},
        ).take()


def test_split_is_not_disruptive(ray_start_regular):
    ds = ray.data.range(100, parallelism=10).map_batches(lambda x: x).lazy()

    def verify_integrity(splits):
        for dss in splits:
            for batch in dss.iter_batches():
                pass
        for batch in ds.iter_batches():
            pass

    # No block splitting invovled: split 10 even blocks into 2 groups.
    verify_integrity(ds.split(2, equal=True))
    # Block splitting invovled: split 10 even blocks into 3 groups.
    verify_integrity(ds.split(3, equal=True))

    # Same as above but having tranforms post converting to lazy.
    verify_integrity(ds.map_batches(lambda x: x).split(2, equal=True))
    verify_integrity(ds.map_batches(lambda x: x).split(3, equal=True))

    # Same as above but having in-place tranforms post converting to lazy.
    verify_integrity(ds.randomize_block_order().split(2, equal=True))
    verify_integrity(ds.randomize_block_order().split(3, equal=True))


def test_datasource(ray_start_regular):
    source = ray.data.datasource.RandomIntRowDatasource()
    assert len(ray.data.read_datasource(source, n=10, num_columns=2).take()) == 10
    source = ray.data.datasource.RangeDatasource()
    assert ray.data.read_datasource(source, n=10).take() == list(range(10))


def test_polars_lazy_import(shutdown_only):
    import sys

    ctx = ray.data.context.DatasetContext.get_current()

    try:
        original_use_polars = ctx.use_polars
        ctx.use_polars = True

        num_items = 100
        parallelism = 4
        ray.init(num_cpus=4)

        @ray.remote
        def f(should_import_polars):
            # Sleep to spread the tasks.
            time.sleep(1)
            polars_imported = "polars" in sys.modules.keys()
            return polars_imported == should_import_polars

        # We should not use polars for non-Arrow sort.
        _ = ray.data.range(num_items, parallelism=parallelism).sort()
        assert all(ray.get([f.remote(False) for _ in range(parallelism)]))

        a = range(100)
        dfs = []
        partition_size = num_items // parallelism
        for i in range(parallelism):
            dfs.append(
                pd.DataFrame({"a": a[i * partition_size : (i + 1) * partition_size]})
            )
        # At least one worker should have imported polars.
        _ = (
            ray.data.from_pandas(dfs)
            .map_batches(lambda t: t, batch_format="pyarrow", batch_size=None)
            .sort(key="a")
        )
        assert any(ray.get([f.remote(True) for _ in range(parallelism)]))

    finally:
        ctx.use_polars = original_use_polars


def test_actor_pool_strategy_apply_interrupt(shutdown_only):
    """Test that _apply kills the actor pool if an interrupt is raised."""
    ray.init(include_dashboard=False, num_cpus=1)

    cpus = ray.available_resources()["CPU"]
    ds = ray.data.range(5, parallelism=5)
    aps = ray.data.ActorPoolStrategy(max_size=5)
    blocks = ds._plan.execute()

    # Start some actors, the first one sends a SIGINT, emulating a KeyboardInterrupt
    def test_func(block):
        for i, _ in enumerate(BlockAccessor.for_block(block).iter_rows()):
            if i == 0:
                os.kill(os.getpid(), signal.SIGINT)
            else:
                time.sleep(1000)
                return block

    # No need to test ActorPoolStrategy in new execution backend.
    if not DatasetContext.get_current().new_execution_backend:
        with pytest.raises(ray.exceptions.RayTaskError):
            aps._apply(test_func, {}, blocks, False)

    # Check that all actors have been killed by counting the available CPUs
    wait_for_condition(lambda: (ray.available_resources().get("CPU", 0) == cpus))


def test_actor_pool_strategy_default_num_actors(shutdown_only):
    def f(x):
        import time

        time.sleep(1)
        return x

    num_cpus = 5
    ray.init(num_cpus=num_cpus)
    compute_strategy = ray.data.ActorPoolStrategy()
    ray.data.range(10, parallelism=10).map_batches(
        f, batch_size=1, compute=compute_strategy
    ).fully_executed()

    # The new execution backend is not using the ActorPoolStrategy under
    # the hood, so the expectation here applies only to the old backend.
    # TODO(https://github.com/ray-project/ray/issues/31723): we should check
    # the num of workers once we have autoscaling in new execution backend.
    if not DatasetContext.get_current().new_execution_backend:
        expected_max_num_workers = math.ceil(
            num_cpus * (1 / compute_strategy.ready_to_total_workers_ratio)
        )
        assert (
            compute_strategy.num_workers >= num_cpus
            and compute_strategy.num_workers <= expected_max_num_workers
        ), "Number of actors is out of the expected bound"


def test_actor_pool_strategy_bundles_to_max_actors(shutdown_only):
    """Tests that blocks are bundled up to the specified max number of actors."""

    def f(x):
        return x

    max_size = 2
    compute_strategy = ray.data.ActorPoolStrategy(max_size=max_size)
    ds = (
        ray.data.range(10, parallelism=10)
        .map_batches(f, batch_size=None, compute=compute_strategy)
        .fully_executed()
    )

    assert f"{max_size}/{max_size} blocks" in ds.stats()


def test_default_batch_format(shutdown_only):
    ds = ray.data.range(100)
    assert ds.default_batch_format() == list

    ds = ray.data.range_tensor(100)
    assert ds.default_batch_format() == np.ndarray

    df = pd.DataFrame({"foo": ["a", "b"], "bar": [0, 1]})
    ds = ray.data.from_pandas(df)
    assert ds.default_batch_format() == pd.DataFrame


def test_dataset_schema_after_read_stats(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(cluster.address)
    cluster.add_node(num_cpus=1, resources={"foo": 1})
    ds = ray.data.read_csv(
        "example://iris.csv", ray_remote_args={"resources": {"foo": 1}}
    )
    schema = ds.schema()
    ds.stats()
    assert schema == ds.schema()


def test_ragged_tensors(ray_start_regular_shared):
    """Test Arrow type promotion between ArrowTensorType and
    ArrowVariableShapedTensorType when a column contains ragged tensors."""
    import numpy as np

    ds = ray.data.from_items(
        [
            {"spam": np.zeros((32, 32, 5))},
            {"spam": np.zeros((64, 64, 5))},
        ]
    )
    new_type = ds.schema().types[0].scalar_type
    assert ds.schema().types == [
        ArrowVariableShapedTensorType(dtype=new_type, ndim=3),
    ]


class LoggerWarningCalled(Exception):
    """Custom exception used in test_warning_execute_with_no_cpu() and
    test_nowarning_execute_with_cpu(). Raised when the `logger.warning` method
    is called, so that we can kick out of `plan.execute()` by catching this Exception
    and check logging was done properly."""

    pass


def test_warning_execute_with_no_cpu(ray_start_cluster):
    """Tests ExecutionPlan.execute() to ensure a warning is logged
    when no CPU resources are available."""
    # Create one node with no CPUs to trigger the Dataset warning
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)

    logger = DatasetLogger("ray.data._internal.plan").get_logger()
    with patch.object(
        logger,
        "warning",
        side_effect=LoggerWarningCalled,
    ) as mock_logger:
        try:
            ds = ray.data.range(10)
            ds = ds.map_batches(lambda x: x)
            ds.take()
        except LoggerWarningCalled:
            logger_args, logger_kwargs = mock_logger.call_args
            assert "Warning: The Ray cluster currently does not have " in logger_args[0]


def test_nowarning_execute_with_cpu(ray_start_cluster_init):
    """Tests ExecutionPlan.execute() to ensure no warning is logged
    when there are available CPU resources."""
    # Create one node with CPUs to avoid triggering the Dataset warning
    ray.init(ray_start_cluster_init.address)

    logger = DatasetLogger("ray.data._internal.plan").get_logger()
    with patch.object(
        logger,
        "warning",
        side_effect=LoggerWarningCalled,
    ) as mock_logger:
        ds = ray.data.range(10)
        ds = ds.map_batches(lambda x: x)
        ds.take()
        mock_logger.assert_not_called()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
