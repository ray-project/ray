import math
import os
import random
import signal
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.data._internal.arrow_block import ArrowRow
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.pandas_block import PandasRow
from ray.data.aggregate import AggregateFn, Count, Max, Mean, Min, Std, Sum
from ray.data.block import BlockAccessor
from ray.data.context import DatasetContext
from ray.data.dataset import Dataset, _sliding_window
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.data.extensions.tensor_extension import (
    ArrowTensorArray,
    ArrowTensorType,
    TensorArray,
    TensorDtype,
)
from ray.data.row import TableRow
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


def maybe_lazy(ds, enabled):
    if enabled:
        return ds.experimental_lazy()
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
        ctx.block_splitting_enabled = block_split
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
    assert (
        sorted(
            ds.map(
                lambda x: x + 1,
                compute=ray.data.ActorPoolStrategy(max_tasks_in_flight_per_actor=3),
            ).take()
        )
        == list(range(1, 11))
    )

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
            placement_group=pg, placement_group_capture_child_tasks=True
        ).remote()
    )


def test_callable_classes(shutdown_only):
    ray.init(num_cpus=1)
    ds = ray.data.range(10)

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
    actor_reuse = ds.map_batches(StatefulFn, compute="actors").take()
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


@pytest.mark.parametrize("lazy", [False, True])
def test_dataset_lineage_serialization(shutdown_only, lazy):
    ray.init()
    ds = ray.data.range(10)
    ds = maybe_lazy(ds, lazy)
    ds = ds.map(lambda x: x + 1)
    ds = ds.map(lambda x: x + 1)
    ds = ds.random_shuffle()
    epoch = ds._get_epoch()
    uuid = ds._get_uuid()
    plan_uuid = ds._plan._dataset_uuid
    lazy = ds._lazy

    serialized_ds = ds.serialize_lineage()
    # Confirm that the original Dataset was properly copied before clearing/mutating.
    in_blocks = ds._plan._in_blocks
    # Should not raise.
    in_blocks._check_if_cleared()
    assert isinstance(in_blocks, LazyBlockList)
    if lazy:
        assert in_blocks._block_partition_refs[0] is not None
    else:
        assert ds._plan._snapshot_blocks is not None

    ray.shutdown()
    ray.init()

    ds = Dataset.deserialize_lineage(serialized_ds)
    # Check Dataset state.
    assert ds._get_epoch() == epoch
    assert ds._get_uuid() == uuid
    assert ds._plan._dataset_uuid == plan_uuid
    assert ds._lazy == lazy
    # Check Dataset content.
    assert ds.count() == 10
    assert sorted(ds.take()) == list(range(2, 12))


@pytest.mark.parametrize("lazy", [False, True])
def test_dataset_lineage_serialization_unsupported(shutdown_only, lazy):
    ray.init()
    # In-memory data sources not supported.
    ds = ray.data.from_items(list(range(10)))
    ds = maybe_lazy(ds, lazy)
    ds = ds.map(lambda x: x + 1)
    ds = ds.map(lambda x: x + 1)

    with pytest.raises(ValueError):
        ds.serialize_lineage()

    # In-memory data source unions not supported.
    ds = ray.data.from_items(list(range(10)))
    ds = maybe_lazy(ds, lazy)
    ds1 = ray.data.from_items(list(range(10, 20)))
    ds2 = ds.union(ds1)

    with pytest.raises(ValueError):
        ds2.serialize_lineage()

    # Post-lazy-read unions not supported.
    ds = ray.data.range(10).map(lambda x: x + 1)
    ds = maybe_lazy(ds, lazy)
    ds1 = ray.data.range(20).map(lambda x: 2 * x)
    ds2 = ds.union(ds1)

    with pytest.raises(ValueError):
        ds2.serialize_lineage()

    # Lazy read unions supported.
    ds = ray.data.range(10)
    ds = maybe_lazy(ds, lazy)
    ds1 = ray.data.range(20)
    ds2 = ds.union(ds1)

    serialized_ds = ds2.serialize_lineage()
    ds3 = Dataset.deserialize_lineage(serialized_ds)
    assert ds3.take(30) == list(range(10)) + list(range(20))

    # Zips not supported.
    ds = ray.data.from_items(list(range(10)))
    ds = maybe_lazy(ds, lazy)
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
    ds1 = ray.data.range(5)
    ds2 = ray.data.range(5).map(lambda x: x + 1)
    ds = ds1.zip(ds2)
    assert ds.schema() == tuple
    assert ds.take() == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]
    with pytest.raises(ValueError):
        ds.zip(ray.data.range(3))


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
    assert "{id: int64, a: int64, b: int64}" in str(ds)
    assert ds.count() == 5
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

    ds = ray.data.from_items([torch.tensor([0, 0]) for _ in range(40)])
    res = "Dataset(num_blocks=40, num_rows=40, schema=<class 'torch.Tensor'>)"
    assert str(ds) == res, str(ds)
    with pytest.raises(pa.lib.ArrowInvalid):
        next(ds.iter_batches(batch_format="pyarrow"))
    df = next(ds.iter_batches(batch_format="pandas"))
    assert df.to_dict().keys() == {"value"}


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
    ds = ray.data.range_table(10)
    assert ds.num_blocks() == 10
    assert ds.count() == 10
    assert ds.take() == [{"value": i} for i in range(10)]

    ds = ray.data.range_table(10, parallelism=2)
    assert ds.num_blocks() == 2
    assert ds.count() == 10
    assert ds.take() == [{"value": i} for i in range(10)]


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
    ],
)
@pytest.mark.parametrize("init_with_pandas", [True, False])
def test_tensor_array_boolean_slice_pandas_roundtrip(init_with_pandas, test_data, a, b):
    n = len(test_data)
    test_arr = np.array(test_data)
    df = pd.DataFrame({"one": TensorArray(test_arr), "two": ["a"] * n})
    if init_with_pandas:
        table = pa.Table.from_pandas(df)
    else:
        pa_dtype = pa.bool_()
        flat = [w for v in test_data for w in v]
        data_array = pa.array(flat, pa_dtype)
        inner_len = len(test_data[0])
        offsets = list(range(0, len(flat) + 1, inner_len))
        offset_buffer = pa.py_buffer(np.int32(offsets))
        storage = pa.Array.from_buffers(
            pa.list_(pa_dtype),
            len(test_data),
            [None, offset_buffer],
            children=[data_array],
        )
        t_arr = pa.ExtensionArray.from_storage(
            ArrowTensorType((inner_len,), pa.bool_()), storage
        )
        table = pa.table({"one": t_arr, "two": ["a"] * n})
    block_accessor = BlockAccessor.for_block(table)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(), test_arr[a:b, :])
    pd.testing.assert_frame_equal(
        table2.to_pandas().reset_index(drop=True), df[a:b].reset_index(drop=True)
    )

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(), test_arr[a:b, :])
    pd.testing.assert_frame_equal(
        table2.to_pandas().reset_index(drop=True), df[a:b].reset_index(drop=True)
    )


def test_tensors_basic(ray_start_regular_shared):
    # Create directly.
    tensor_shape = (3, 5)
    ds = ray.data.range_tensor(6, shape=tensor_shape)
    assert str(ds) == (
        "Dataset(num_blocks=6, num_rows=6, "
        "schema={__value__: <ArrowTensorType: shape=(3, 5), dtype=int64>})"
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
    def mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["a", "b", "c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return {"a": col_arrs["a"] + 1, "b": col_arrs["b"] + 1, "c": col_arrs["c"] + 1}

    t = pa.table(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": ArrowTensorArray.from_numpy(np.array([[1, 2], [3, 4], [5, 6]])),
        }
    )
    res = (
        ray.data.from_arrow(t)
        .map_batches(mapper, batch_size=2, batch_format="numpy")
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

    # Pandas columns in NumPy format.
    def mapper(col_arrs):
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

    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": TensorArray(np.array([[1, 2], [3, 4], [5, 6]])),
        }
    )
    res = (
        ray.data.from_pandas(df)
        .map_batches(mapper, batch_size=2, batch_format="numpy")
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
    ds = ray.data.range(10).map(lambda _: np.ones((4, 4)))
    assert str(ds) == (
        "Dataset(num_blocks=10, num_rows=10, "
        "schema={__value__: <ArrowTensorType: shape=(4, 4), dtype=double>})"
    )

    # Test map_batches.
    ds = ray.data.range(16, parallelism=4).map_batches(
        lambda _: np.ones((3, 4, 4)), batch_size=2
    )
    assert str(ds) == (
        "Dataset(num_blocks=4, num_rows=24, "
        "schema={__value__: <ArrowTensorType: shape=(4, 4), dtype=double>})"
    )

    # Test flat_map.
    ds = ray.data.range(10).flat_map(lambda _: [np.ones((4, 4)), np.ones((4, 4))])
    assert str(ds) == (
        "Dataset(num_blocks=10, num_rows=20, "
        "schema={__value__: <ArrowTensorType: shape=(4, 4), dtype=double>})"
    )


def test_tensors_in_tables_from_pandas(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": list(arr)})
    # Cast column to tensor extension dtype.
    df["two"] = df["two"].astype(TensorDtype())
    ds = ray.data.from_pandas([df])
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_pandas_roundtrip(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas([df])
    ds_df = ds.to_pandas()
    assert ds_df.equals(df)


def test_tensors_in_tables_parquet_roundtrip(ray_start_regular_shared, tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
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
        batch["two"] = batch["two"].astype(TensorDtype())
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


@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_tf(ray_start_regular_shared, pipelined):
    import tensorflow as tf

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape).astype(np.float)
    df1 = pd.DataFrame(
        {
            "one": TensorArray(arr),
            "two": TensorArray(arr + 1),
            "label": [1, 2, 3],
        }
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape).astype(np.float)
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arr2),
            "two": TensorArray(arr2 + 1),
            "label": [4, 5, 6],
        }
    )
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    tfd = ds.to_tf(
        label_column="label",
        output_signature=(
            tf.TensorSpec(shape=(None, 2, 2, 2, 2), dtype=tf.float32),
            tf.TensorSpec(shape=(None,), dtype=tf.float32),
        ),
        batch_size=2,
    )
    features, labels = [], []
    for batch in tfd.as_numpy_iterator():
        features.append(batch[0])
        labels.append(batch[1])
    features, labels = np.concatenate(features), np.concatenate(labels)
    values = np.stack([df["one"].to_numpy(), df["two"].to_numpy()], axis=1)
    np.testing.assert_array_equal(values, features)
    np.testing.assert_array_equal(df["label"].to_numpy(), labels)


@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_tf_mix(ray_start_regular_shared, pipelined):
    import tensorflow as tf

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape).astype(np.float)
    df1 = pd.DataFrame(
        {
            "one": TensorArray(arr),
            "two": [1, 2, 3],
            "label": [1.0, 2.0, 3.0],
        }
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape).astype(np.float)
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
    tfd = ds.to_tf(
        label_column="label",
        feature_columns=[["one"], ["two"]],
        output_signature=(
            (
                tf.TensorSpec(shape=(None, 2, 2, 2), dtype=tf.float32),
                tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
            ),
            tf.TensorSpec(shape=(None,), dtype=tf.float32),
        ),
        batch_size=2,
    )
    col1, col2, labels = [], [], []
    for batch in tfd.as_numpy_iterator():
        col1.append(batch[0][0])
        col2.append(batch[0][1])
        labels.append(batch[1])
    col1 = np.concatenate(col1)
    col2 = np.squeeze(np.concatenate(col2), axis=1)
    labels = np.concatenate(labels)
    np.testing.assert_array_equal(col1, np.sort(df["one"].to_numpy()))
    np.testing.assert_array_equal(col2, np.sort(df["two"].to_numpy()))
    np.testing.assert_array_equal(labels, np.sort(df["label"].to_numpy()))


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
    assert str(ds) == "Dataset(num_blocks=1, num_rows=0, schema=Unknown schema)"

    # Test map on empty dataset.
    ds = ray.data.from_items([])
    ds = ds.map(lambda x: x)
    assert ds.count() == 0

    # Test filter on empty dataset.
    ds = ray.data.from_items([])
    ds = ds.filter(lambda: True)
    assert ds.count() == 0


def test_schema(ray_start_regular_shared):
    ds = ray.data.range(10)
    ds2 = ray.data.range_table(10)
    ds3 = ds2.repartition(5)
    ds4 = ds3.map(lambda x: {"a": "hi", "b": 1.0}).limit(5).repartition(1)
    assert str(ds) == "Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)"
    assert str(ds2) == "Dataset(num_blocks=10, num_rows=10, schema={value: int64})"
    assert str(ds3) == "Dataset(num_blocks=5, num_rows=10, schema={value: int64})"
    assert (
        str(ds4) == "Dataset(num_blocks=1, num_rows=5, schema={a: string, b: double})"
    )


def test_schema_lazy(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=10)
    # We kick off the read task for the first block by default.
    assert ds._plan._in_blocks._num_computed() == 1
    schema = ds.schema()
    assert schema == int
    # Fetching the schema should not trigger execution of extra read tasks.
    assert ds._plan.execute()._num_computed() == 1


def test_lazy_loading_exponential_rampup(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=20)
    assert ds._plan.execute()._num_computed() == 1
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


def test_limit(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=20)
    for i in range(100):
        assert ds.limit(i).take(200) == list(range(i))


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
    assert ds4._block_num_rows() == ([1] * 20) + ([0] * 20)

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
    for batch, df in zip(ds.iter_batches(batch_format="pandas"), dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # pyarrow.Table format.
    for batch, df in zip(ds.iter_batches(batch_format="pyarrow"), dfs):
        assert isinstance(batch, pa.Table)
        assert batch.equals(pa.Table.from_pandas(df))

    # NumPy format.
    for batch, df in zip(ds.iter_batches(batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Test NumPy format on Arrow blocks.
    ds2 = ds.map_batches(lambda b: b, batch_size=None, batch_format="pyarrow")
    for batch, df in zip(ds2.iter_batches(batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Native format.
    for batch, df in zip(ds.iter_batches(batch_format="native"), dfs):
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
    batches = list(ds.iter_batches(prefetch_blocks=1, batch_format="pandas"))
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
    batches = list(ds.iter_batches(prefetch_blocks=len(dfs), batch_format="pandas"))
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # Prefetch with ray.wait.
    context = DatasetContext.get_current()
    context.actor_prefetcher_enabled = False
    batches = list(ds.iter_batches(prefetch_blocks=1, batch_format="pandas"))
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)


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
    for _, expected in zip(ds.iter_batches(), expected_num_blocks):
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
    assert ds2._dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test Pyarrow
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda pa: pa, batch_size=1, batch_format="pyarrow")
    assert ds2._dataset_format() == "arrow"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [1, 2, 3]
    values = [s["two"] for s in ds_list]
    assert values == [2, 3, 4]

    # Test batch
    size = 300
    ds = ray.data.range(size)
    ds2 = ds.map_batches(lambda df: df + 1, batch_size=17, batch_format="pandas")
    assert ds2._dataset_format() == "pandas"
    ds_list = ds2.take(limit=size)
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
    assert ds2._dataset_format() == "simple"
    ds_list = ds2.take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # pyarrow => list block
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda df: [1], batch_size=1, batch_format="pyarrow")
    assert ds2._dataset_format() == "simple"
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
        # fn_constructor_args and fn_constructor_kwargs only supported for actor compute
        # strategy.
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
        fn_args=(ray.put(1),),
    )
    assert ds2._dataset_format() == "pandas"
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
        fn_kwargs={"b": ray.put(2)},
    )
    assert ds2._dataset_format() == "pandas"
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
        fn_args=(ray.put(1),),
        fn_kwargs={"b": ray.put(2)},
    )
    assert ds2._dataset_format() == "pandas"
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
        fn_constructor_args=(ray.put(1),),
    )
    assert ds2._dataset_format() == "pandas"
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
        fn_constructor_kwargs={"b": ray.put(2)},
    )
    assert ds2._dataset_format() == "pandas"
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
        fn_constructor_args=(ray.put(1),),
        fn_constructor_kwargs={"b": ray.put(2)},
    )
    assert ds2._dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [3, 5, 7]
    values = [s["two"] for s in ds_list]
    assert values == [5, 7, 9]

    # Test callable chain.
    ds = ray.data.read_parquet(str(tmp_path))
    fn_constructor_args = (ray.put(1),)
    fn_constructor_kwargs = {"b": ray.put(2)}
    ds2 = (
        ds.experimental_lazy()
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
    assert ds2._dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [7, 11, 15]
    values = [s["two"] for s in ds_list]
    assert values == [11, 15, 19]

    # Test function + callable chain.
    ds = ray.data.read_parquet(str(tmp_path))
    fn_constructor_args = (ray.put(1),)
    fn_constructor_kwargs = {"b": ray.put(2)}
    ds2 = (
        ds.experimental_lazy()
        .map_batches(
            lambda df, a, b=None: b * df + a,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_args=(ray.put(1),),
            fn_kwargs={"b": ray.put(2)},
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
    assert ds2._dataset_format() == "pandas"
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [7, 11, 15]
    values = [s["two"] for s in ds_list]
    assert values == [11, 15, 19]


def test_map_batches_actors_preserves_order(ray_start_regular_shared):
    # Test that actor compute model preserves block order.
    ds = ray.data.range(10, parallelism=5)
    assert ds.map_batches(lambda x: x, compute="actors").take() == list(range(10))


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


def test_to_dask(ray_start_regular_shared):
    from ray.util.dask import ray_dask_get

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ddf = ds.to_dask()
    # Explicit Dask-on-Ray
    assert df.equals(ddf.compute(scheduler=ray_dask_get))
    # Implicit Dask-on-Ray.
    assert df.equals(ddf.compute())


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
def test_to_tf(ray_start_regular_shared, pipelined):
    import tensorflow as tf

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
    tfd = ds.to_tf(
        label_column="label",
        output_signature=(
            tf.TensorSpec(shape=(None, 2), dtype=tf.float32),
            tf.TensorSpec(shape=(None), dtype=tf.float32),
        ),
    )
    iterations = []
    for batch in tfd.as_numpy_iterator():
        iterations.append(np.concatenate((batch[0], batch[1].reshape(-1, 1)), axis=1))
    combined_iterations = np.concatenate(iterations)
    assert np.array_equal(df.values, combined_iterations)


def test_to_tf_feature_columns_list(ray_start_regular_shared):
    import tensorflow as tf

    df = pd.DataFrame({"X1": [1, 2, 3], "X2": [4, 5, 6], "X3": [7, 8, 9]})
    ds = ray.data.from_pandas([df])

    feature_columns = ["X1", "X3"]
    batch_size = 2
    dataset = ds.to_tf(
        feature_columns=feature_columns,
        output_signature=tf.TensorSpec(shape=(None, len(feature_columns))),
        batch_size=batch_size,
    )

    batches = list(dataset.as_numpy_iterator())
    assert len(batches) == math.ceil(len(df) / batch_size)
    assert np.array_equal(batches[0], np.array([[1, 7], [2, 8]]))
    assert np.array_equal(batches[1], np.array([[3, 9]]))


def test_to_tf_feature_columns_list_with_label(ray_start_regular_shared):
    import tensorflow as tf

    df = pd.DataFrame({"X1": [1, 2, 3], "X2": [4, 5, 6], "Y": [7, 8, 9]})
    ds = ray.data.from_pandas([df])

    feature_columns = ["X1", "X2"]
    output_signature = [
        tf.TensorSpec(shape=(None, len(feature_columns))),
        tf.TensorSpec(shape=(None)),
    ]
    batch_size = 2
    dataset = ds.to_tf(
        feature_columns=feature_columns,
        label_column="Y",
        output_signature=output_signature,
        batch_size=batch_size,
    )

    batches = list(dataset.as_numpy_iterator())
    assert len(batches) == math.ceil(len(df) / batch_size)
    # Each batch should be a two-tuple corresponding to (features, labels).
    assert all(len(batch) == 2 for batch in batches)
    assert np.array_equal(batches[0][0], np.array([[1, 4], [2, 5]]))
    assert np.array_equal(batches[0][1], np.array([7, 8]))
    assert np.array_equal(batches[1][0], np.array([[3, 6]]))
    assert np.array_equal(batches[1][1], np.array([9]))


def test_to_tf_feature_columns_nested_list(ray_start_regular_shared):
    import tensorflow as tf

    df = pd.DataFrame({"X1": [1, 2, 3], "X2": [4, 5, 6], "X3": [7, 8, 9]})
    ds = ray.data.from_pandas([df])

    feature_columns = [["X1", "X2"], ["X3"]]
    output_signature = [
        tf.TensorSpec(shape=(None, len(feature_columns[0]))),
        tf.TensorSpec(shape=(None, len(feature_columns[1]))),
    ]
    batch_size = 2
    dataset = ds.to_tf(
        feature_columns=feature_columns,
        output_signature=output_signature,
        batch_size=batch_size,
    )

    batches = list(dataset.as_numpy_iterator())
    assert len(batches) == math.ceil(len(df) / batch_size)
    assert all(len(batch) == len(feature_columns) for batch in batches)
    assert np.array_equal(batches[0][0], np.array([[1, 4], [2, 5]]))
    assert np.array_equal(batches[0][1], np.array([[7], [8]]))
    assert np.array_equal(batches[1][0], np.array([[3, 6]]))
    assert np.array_equal(batches[1][1], np.array([[9]]))


def test_to_tf_feature_columns_dict(ray_start_regular_shared):
    import tensorflow as tf

    df = pd.DataFrame({"X1": [1, 2, 3], "X2": [4, 5, 6], "X3": [7, 8, 9]})
    ds = ray.data.from_pandas([df])

    feature_columns = {"A": ["X1", "X2"], "B": ["X3"]}
    output_signature = {
        "A": tf.TensorSpec(shape=(None, len(feature_columns["A"]))),
        "B": tf.TensorSpec(shape=(None, len(feature_columns["B"]))),
    }
    batch_size = 2
    dataset = ds.to_tf(
        feature_columns=feature_columns, output_signature=output_signature, batch_size=2
    )

    batches = list(dataset.as_numpy_iterator())
    assert len(batches) == math.ceil(len(df) / batch_size)
    assert all(batch.keys() == feature_columns.keys() for batch in batches)
    assert np.array_equal(batches[0]["A"], np.array([[1, 4], [2, 5]]))
    assert np.array_equal(batches[0]["B"], np.array([[7], [8]]))
    assert np.array_equal(batches[1]["A"], np.array([[3, 6]]))
    assert np.array_equal(batches[1]["B"], np.array([[9]]))


def test_to_tf_feature_columns_dict_with_label(ray_start_regular_shared):
    import tensorflow as tf

    df = pd.DataFrame({"X1": [1, 2, 3], "X2": [4, 5, 6], "Y": [7, 8, 9]})
    ds = ray.data.from_pandas([df])

    feature_columns = {"A": ["X1", "X2"]}
    output_signature = (
        {
            "A": tf.TensorSpec(shape=(None, len(feature_columns["A"]))),
        },
        tf.TensorSpec(shape=(None)),
    )
    batch_size = 2
    dataset = ds.to_tf(
        feature_columns=feature_columns,
        label_column="Y",
        output_signature=output_signature,
        batch_size=2,
    )

    batches = list(dataset.as_numpy_iterator())
    assert len(batches) == math.ceil(len(df) / batch_size)
    # Each batch should be a two-tuple corresponding to (features, labels).
    assert all(len(batch) == 2 for batch in batches)
    assert all(features.keys() == feature_columns.keys() for features, _ in batches)

    features0, labels0 = batches[0]
    assert np.array_equal(features0["A"], np.array([[1, 4], [2, 5]]))
    assert np.array_equal(labels0, np.array([7, 8]))

    features1, labels1 = batches[1]
    assert np.array_equal(features1["A"], np.array([[3, 6]]))
    assert np.array_equal(labels1, np.array([9]))


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
        assert np.array_equal(np.sort(df.values), np.sort(combined_iterations))


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
    assert np.array_equal(df.values, combined_iterations)


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


def test_groupby_arrow(ray_start_regular_shared):
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
def test_groupby_tabular_count(ray_start_regular_shared, ds_format, num_parts):
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
def test_groupby_tabular_sum(ray_start_regular_shared, ds_format, num_parts):
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
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "sum(B)": None},
        {"A": 1, "sum(B)": 1617},
        {"A": 2, "sum(B)": 1650},
    ]
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    nan_agg_ds = ds.groupby("A").sum("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "sum(B)": None},
        {"A": 1, "sum(B)": None},
        {"A": 2, "sum(B)": None},
    ]


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
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "min(B)": None},
        {"A": 1, "min(B)": 1},
        {"A": 2, "min(B)": 2},
    ]
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").min("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "min(B)": None},
        {"A": 1, "min(B)": None},
        {"A": 2, "min(B)": None},
    ]


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
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "max(B)": None},
        {"A": 1, "max(B)": 97},
        {"A": 2, "max(B)": 98},
    ]
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").max("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "max(B)": None},
        {"A": 1, "max(B)": None},
        {"A": 2, "max(B)": None},
    ]


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

    # Test built-in min aggregation with nans
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
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "mean(B)": None},
        {"A": 1, "mean(B)": 49.0},
        {"A": 2, "mean(B)": 50.0},
    ]
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").mean("B")
    assert nan_agg_ds.count() == 3
    assert [row.as_pydict() for row in nan_agg_ds.sort("A").iter_rows()] == [
        {"A": 0, "mean(B)": None},
        {"A": 1, "mean(B)": None},
        {"A": 2, "mean(B)": None},
    ]


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


def test_map_batches_combine_empty_blocks(ray_start_regular_shared):
    xs = [x % 3 for x in list(range(100))]

    # ds1 has 1 block which contains 100 rows.
    ds1 = ray.data.from_items(xs).repartition(1).sort().map_batches(lambda x: x)
    assert ds1._block_num_rows() == [100]

    # ds2 has 30 blocks, but only 3 of them are non-empty
    ds2 = ray.data.from_items(xs).repartition(30).sort().map_batches(lambda x: x)
    assert len(ds2._block_num_rows()) == 30
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

    # The UDF returns a type that's different than the input type, which is invalid.
    with pytest.raises(TypeError):
        grouped.map_groups(lambda x: pd.DataFrame([1]) if x == [1] else x)


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


@pytest.mark.parametrize("pipelined", [False, True])
@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_random_shuffle(shutdown_only, pipelined, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()

    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = use_push_based_shuffle

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
    finally:
        ctx.use_push_based_shuffle = original


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


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_random_shuffle_spread(ray_start_cluster, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()
    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = use_push_based_shuffle

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
            return ray.get_runtime_context().node_id.hex()

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

    finally:
        ctx.use_push_based_shuffle = original


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
        return ray.get_runtime_context().node_id.hex()

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
        _ = ray.data.from_pandas(dfs).sort(key="a")
        assert any(ray.get([f.remote(True) for _ in range(parallelism)]))

    finally:
        ctx.use_polars = original_use_polars


def test_actorpoolstrategy_apply_interrupt():
    """Test that _apply kills the actor pool if an interrupt is raised."""
    ray.init(include_dashboard=False, num_cpus=1)

    cpus = ray.available_resources()["CPU"]
    ds = ray.data.range(5)
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

    with pytest.raises(ray.exceptions.RayTaskError):
        aps._apply(test_func, {}, blocks, False)

    # Check that all actors have been killed by counting the available CPUs
    wait_for_condition(lambda: (ray.available_resources().get("CPU", 0) == cpus))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
