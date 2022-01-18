import math
import os
import random
import requests
import shutil
import time

from unittest.mock import patch
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.local import LocalFileSystem
from pytest_lazyfixture import lazy_fixture

import ray

from ray.tests.conftest import *  # noqa
from ray.data.dataset import Dataset, _sliding_window
from ray.data.datasource import DummyOutputDatasource
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.data.block import BlockAccessor
from ray.data.impl.block_list import BlockList
from ray.data.impl.stats import DatasetStats
from ray.data.aggregate import AggregateFn, Count, Sum, Min, Max, Mean, Std
from ray.data.datasource.file_based_datasource import _unwrap_protocol
from ray.data.datasource.parquet_datasource import (
    PARALLELIZE_META_FETCH_THRESHOLD)
from ray.data.extensions.tensor_extension import (
    TensorArray, TensorDtype, ArrowTensorType, ArrowTensorArray)
import ray.data.tests.util as util
from ray.data.tests.conftest import *  # noqa


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


# Tests that we don't block on exponential rampup when doing bulk reads.
# https://github.com/ray-project/ray/issues/20625
@pytest.mark.parametrize("block_split", [False, True])
def test_bulk_lazy_eval_split_mode(shutdown_only, block_split, tmp_path):
    ray.init(num_cpus=8)
    ctx = ray.data.context.DatasetContext.get_current()

    class SlowCSVDatasource(CSVDatasource):
        def _read_stream(self, f: "pa.NativeFile", path: str, **reader_args):
            for block in CSVDatasource._read_stream(self, f, path,
                                                    **reader_args):
                time.sleep(3)
                yield block

    try:
        original = ctx.block_splitting_enabled

        ray.data.range(8, parallelism=8).write_csv(str(tmp_path))
        ctx.block_splitting_enabled = True
        ds = ray.data.read_datasource(
            SlowCSVDatasource(), parallelism=8, paths=str(tmp_path))

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
    ds = ray.data.range(5)
    ds = maybe_pipeline(ds, pipelined)
    assert sorted(ds.map(lambda x: x + 1,
                         compute="actors").take()) == [1, 2, 3, 4, 5]


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
            placement_group=pg,
            placement_group_capture_child_tasks=True).remote())


@pytest.mark.parametrize("pipelined", [False, True])
def test_equal_split(shutdown_only, pipelined):
    ray.init(num_cpus=2)

    def range2x(n):
        if pipelined:
            return ray.data.range(n).repeat(2)
        else:
            return ray.data.range(2 * n)

    def counts(shards):
        @ray.remote(num_cpus=0)
        def count(s):
            return s.count()

        return ray.get([count.remote(s) for s in shards])

    r1 = counts(range2x(10).split(3, equal=True))
    assert all(c == 6 for c in r1), r1

    r2 = counts(range2x(10).split(3, equal=False))
    assert all(c >= 6 for c in r2), r2
    assert not all(c == 6 for c in r2), r2


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

    # map
    task_reuse = ds.map(StatefulFn, compute="tasks").take()
    assert sorted(task_reuse) == list(range(10)), task_reuse
    actor_reuse = ds.map(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10, 20)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return [r]

    # flat map
    task_reuse = ds.flat_map(StatefulFn, compute="tasks").take()
    assert sorted(task_reuse) == list(range(10)), task_reuse
    actor_reuse = ds.flat_map(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10, 20)), actor_reuse

    # map batches
    task_reuse = ds.map_batches(StatefulFn, compute="tasks").take()
    assert sorted(task_reuse) == list(range(10)), task_reuse
    actor_reuse = ds.map_batches(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10, 20)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return r > 0

    # filter
    task_reuse = ds.filter(StatefulFn, compute="tasks").take()
    assert len(task_reuse) == 9, task_reuse
    actor_reuse = ds.filter(StatefulFn, compute="actors").take()
    assert len(actor_reuse) == 10, actor_reuse


def test_transform_failure(shutdown_only):
    ray.init(num_cpus=2)
    ds = ray.data.from_items([0, 10], parallelism=2)

    def mapper(x):
        time.sleep(x)
        raise ValueError("oops")
        return x

    with pytest.raises(ray.exceptions.RayTaskError):
        ds.map(mapper)


@pytest.mark.parametrize(
    "block_sizes,num_splits",
    [
        (  # Test baseline.
            [3, 6, 3], 3),
        (  # Already balanced.
            [3, 3, 3], 3),
        (  # Row truncation.
            [3, 6, 4], 3),
        (  # Row truncation, smaller number of blocks.
            [3, 6, 2, 3], 3),
        (  # Row truncation, larger number of blocks.
            [5, 6, 2, 5], 5),
        (  # All smaller but one.
            [1, 1, 1, 1, 6], 5),
        (  # All larger but one.
            [4, 4, 4, 4, 1], 5),
        (  # Single block.
            [2], 2),
        (  # Single split.
            [2, 5], 1),
    ])
def test_equal_split_balanced(ray_start_regular_shared, block_sizes,
                              num_splits):
    _test_equal_split_balanced(block_sizes, num_splits)


def _test_equal_split_balanced(block_sizes, num_splits):
    blocks = []
    metadata = []
    total_rows = 0
    for block_size in block_sizes:
        block = list(range(total_rows, total_rows + block_size))
        blocks.append(ray.put(block))
        metadata.append(
            BlockAccessor.for_block(block).get_metadata(None, None))
        total_rows += block_size
    block_list = BlockList(blocks, metadata)
    ds = Dataset(block_list, 0, DatasetStats(stages={}, parent=None))

    splits = ds.split(num_splits, equal=True)
    split_counts = [split.count() for split in splits]
    assert len(split_counts) == num_splits
    expected_block_size = total_rows // num_splits
    # Check that all splits are the expected size.
    assert all([count == expected_block_size for count in split_counts])
    expected_total_rows = sum(split_counts)
    # Check that the expected number of rows were dropped.
    assert total_rows - expected_total_rows == total_rows % num_splits
    # Check that all rows are unique (content check).
    split_rows = [row for split in splits for row in split.take(total_rows)]
    assert len(set(split_rows)) == len(split_rows)


def test_equal_split_balanced_grid(ray_start_regular_shared):

    # Tests balanced equal splitting over a grid of configurations.
    # Grid: num_blocks x num_splits x num_rows_block_1 x ... x num_rows_block_n
    seed = int(time.time())
    print(f"Seeding RNG for test_equal_split_balanced_grid with: {seed}")
    random.seed(seed)
    max_num_splits = 20
    num_splits_samples = 5
    max_num_blocks = 50
    max_num_rows_per_block = 100
    num_blocks_samples = 5
    block_sizes_samples = 5
    for num_splits in np.random.randint(
            2, max_num_splits + 1, size=num_splits_samples):
        for num_blocks in np.random.randint(
                1, max_num_blocks + 1, size=num_blocks_samples):
            block_sizes_list = [
                np.random.randint(
                    1, max_num_rows_per_block + 1, size=num_blocks)
                for _ in range(block_sizes_samples)
            ]
            for block_sizes in block_sizes_list:
                if sum(block_sizes) < num_splits:
                    min_ = math.ceil(num_splits / num_blocks)
                    block_sizes = np.random.randint(
                        min_, max_num_rows_per_block + 1, size=num_blocks)
                _test_equal_split_balanced(block_sizes, num_splits)


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


def test_zip_arrow(ray_start_regular_shared):
    ds1 = ray.data.range_arrow(5).map(lambda r: {"id": r["value"]})
    ds2 = ray.data.range_arrow(5).map(
        lambda r: {"a": r["value"] + 1, "b": r["value"] + 2})
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
    res = ("Dataset(num_blocks=40, num_rows=40, "
           "schema=<class 'torch.Tensor'>)")
    assert str(ds) == res, str(ds)
    with pytest.raises(pa.lib.ArrowInvalid):
        next(ds.iter_batches(batch_format="pyarrow"))
    df = next(ds.iter_batches(batch_format="pandas"))
    assert df.to_dict().keys() == {0, 1}


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
    df = pd.DataFrame({
        "one": list(range(n)),
        "two": ["a"] * n,
        "three": [np.nan] + [1.5] * (n - 1)
    })
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


def test_tensors(ray_start_regular_shared):
    # Create directly.
    ds = ray.data.range_tensor(5, shape=(3, 5))
    assert str(ds) == (
        "Dataset(num_blocks=5, num_rows=5, "
        "schema={value: <ArrowTensorType: shape=(3, 5), dtype=int64>})")

    # Pandas conversion.
    res = ray.data.range_tensor(10).map_batches(
        lambda t: t + 2, batch_format="pandas").take(2)
    assert str(res) == \
        "[{'value': array([2])}, {'value': array([3])}]"


def test_tensor_array_ops(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    df = pd.DataFrame({"one": [1, 2, 3], "two": TensorArray(arr)})

    def apply_arithmetic_ops(arr):
        return 2 * (arr + 1) / 3

    def apply_comparison_ops(arr):
        return arr % 2 == 0

    def apply_logical_ops(arr):
        return arr & (3 * arr) | (5 * arr)

    # Op tests, using NumPy as the groundtruth.
    np.testing.assert_equal(
        apply_arithmetic_ops(arr), apply_arithmetic_ops(df["two"]))

    np.testing.assert_equal(
        apply_comparison_ops(arr), apply_comparison_ops(df["two"]))

    np.testing.assert_equal(
        apply_logical_ops(arr), apply_logical_ops(df["two"]))


def test_tensor_array_reductions(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})

    # Reduction tests, using NumPy as the groundtruth.
    for name, reducer in TensorArray.SUPPORTED_REDUCERS.items():
        np_kwargs = {}
        if name in ("std", "var"):
            # Pandas uses a ddof default of 1 while NumPy uses 0.
            # Give NumPy a ddof kwarg of 1 in order to ensure equivalent
            # standard deviation calculations.
            np_kwargs["ddof"] = 1
        np.testing.assert_equal(df["two"].agg(name),
                                reducer(arr, axis=0, **np_kwargs))


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
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(),
                                  one_arr[a:b, :, :])
    check_for_copy(table, table2, a, b, is_copy=True)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(),
                                  one_arr[a:b, :, :])
    check_for_copy(table, table2, a, b, is_copy=False)


def test_arrow_tensor_array_getitem(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    t_arr = ArrowTensorArray.from_numpy(arr)

    for idx in range(outer_dim):
        np.testing.assert_array_equal(t_arr[idx], arr[idx])

    # Test __iter__.
    for t_subarr, subarr in zip(t_arr, arr):
        np.testing.assert_array_equal(t_subarr, subarr)

    # Test to_pylist.
    np.testing.assert_array_equal(t_arr.to_pylist(), list(arr))

    # Test slicing and indexing.
    t_arr2 = t_arr[1:]

    np.testing.assert_array_equal(t_arr2.to_numpy(), arr[1:])

    for idx in range(1, outer_dim):
        np.testing.assert_array_equal(t_arr2[idx - 1], arr[idx])


@pytest.mark.parametrize("test_arr,dtype", [
    ([[1, 2], [3, 4], [5, 6], [7, 8]], None),
    ([[1, 2], [3, 4], [5, 6], [7, 8]], np.int32),
    ([[1, 2], [3, 4], [5, 6], [7, 8]], np.int16),
    ([[1, 2], [3, 4], [5, 6], [7, 8]], np.longlong),
    ([[1.5, 2.5], [3.3, 4.2], [5.2, 6.9], [7.6, 8.1]], None),
    ([[1.5, 2.5], [3.3, 4.2], [5.2, 6.9], [7.6, 8.1]], np.float32),
    ([[1.5, 2.5], [3.3, 4.2], [5.2, 6.9], [7.6, 8.1]], np.float16),
    ([[False, True], [True, False], [True, True], [False, False]], None),
])
def test_arrow_tensor_array_slice(test_arr, dtype):
    # Test that ArrowTensorArray slicing works as expected.
    arr = np.array(test_arr, dtype=dtype)
    ata = ArrowTensorArray.from_numpy(arr)
    np.testing.assert_array_equal(ata.to_numpy(), arr)
    slice1 = ata.slice(0, 2)
    np.testing.assert_array_equal(slice1.to_numpy(), arr[0:2])
    np.testing.assert_array_equal(slice1[1], arr[1])
    slice2 = ata.slice(2, 2)
    np.testing.assert_array_equal(slice2.to_numpy(), arr[2:4])
    np.testing.assert_array_equal(slice2[1], arr[3])


def test_tensors_in_tables_from_pandas(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
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
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas([df])
    ds_df = ds.to_pandas()
    assert ds_df.equals(df)


def test_tensors_in_tables_parquet_roundtrip(ray_start_regular_shared,
                                             tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
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


def test_tensors_in_tables_parquet_with_schema(ray_start_regular_shared,
                                               tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    schema = pa.schema([
        ("one", pa.int32()),
        ("two", ArrowTensorType(inner_shape, pa.from_numpy_dtype(arr.dtype))),
    ])
    ds = ray.data.read_parquet(str(tmp_path), schema=schema)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_pickle_manual_serde(
        ray_start_regular_shared, tmp_path):
    import pickle

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({
        "one": list(range(outer_dim)),
        "two": [pickle.dumps(a) for a in arr]
    })
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


def test_tensors_in_tables_parquet_bytes_manual_serde(ray_start_regular_shared,
                                                      tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({
        "one": list(range(outer_dim)),
        "two": [a.tobytes() for a in arr]
    })
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))

    tensor_col_name = "two"

    # Manually deserialize the tensor bytes and cast to a TensorArray.
    def np_deser_mapper(batch: pa.Table):
        # NOTE(Clark): We use NumPy to consolidate these potentially
        # non-contiguous buffers, and to do buffer bookkeeping in general.
        np_col = np.array([
            np.ndarray(inner_shape, buffer=buf.as_buffer(), dtype=arr.dtype)
            for buf in batch.column(tensor_col_name)
        ])

        return batch.set_column(
            batch._ensure_integer_index(tensor_col_name), tensor_col_name,
            ArrowTensorArray.from_numpy(np_col))

    ds = ds.map_batches(np_deser_mapper, batch_format="pyarrow")

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde_udf(
        ray_start_regular_shared, tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    tensor_col_name = "two"
    df = pd.DataFrame({
        "one": list(range(outer_dim)),
        tensor_col_name: [a.tobytes() for a in arr]
    })
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))

    # Manually deserialize the tensor bytes and cast to a TensorArray.
    def np_deser_udf(block: pa.Table):
        # NOTE(Clark): We use NumPy to consolidate these potentially
        # non-contiguous buffers, and to do buffer bookkeeping in general.
        np_col = np.array([
            np.ndarray(inner_shape, buffer=buf.as_buffer(), dtype=arr.dtype)
            for buf in block.column(tensor_col_name)
        ])

        return block.set_column(
            block._ensure_integer_index(tensor_col_name), tensor_col_name,
            ArrowTensorArray.from_numpy(np_col))

    ds = ray.data.read_parquet(str(tmp_path), _block_udf=np_deser_udf)

    assert isinstance(ds.schema().field_by_name(tensor_col_name).type,
                      ArrowTensorType)

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde_col_schema(
        ray_start_regular_shared, tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    tensor_col_name = "two"
    df = pd.DataFrame({
        "one": list(range(outer_dim)),
        tensor_col_name: [a.tobytes() for a in arr]
    })
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))

    def _block_udf(block: pa.Table):
        df = block.to_pandas()
        df[tensor_col_name] += 1
        return pa.Table.from_pandas(df)

    ds = ray.data.read_parquet(
        str(tmp_path),
        _block_udf=_block_udf,
        _tensor_column_schema={tensor_col_name: (arr.dtype, inner_shape)})

    assert isinstance(ds.schema().field_by_name(tensor_col_name).type,
                      ArrowTensorType)

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr + 1))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


@pytest.mark.skip(
    reason=("Waiting for Arrow to support registering custom ExtensionType "
            "casting kernels. See "
            "https://issues.apache.org/jira/browse/ARROW-5890#"))
def test_tensors_in_tables_parquet_bytes_with_schema(ray_start_regular_shared,
                                                     tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({
        "one": list(range(outer_dim)),
        "two": [a.tobytes() for a in arr]
    })
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    schema = pa.schema([
        ("one", pa.int32()),
        ("two", ArrowTensorType(inner_shape, pa.from_numpy_dtype(arr.dtype))),
    ])
    ds = ray.data.read_parquet(str(tmp_path), schema=schema)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


@pytest.mark.skip(
    reason=("Waiting for pytorch to support tensor creation from objects that "
            "implement the __array__ interface. See "
            "https://github.com/pytorch/pytorch/issues/51156"))
@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_torch(ray_start_regular_shared, pipelined):
    import torch

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": TensorArray(arr),
        "label": [1.0, 2.0, 3.0]
    })
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape)
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": TensorArray(arr2),
        "label": [4.0, 5.0, 6.0]
    })
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(label_column="label", batch_size=2)

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in iter(torchd):
            iterations.append(torch.cat((*batch[0], batch[1]), axis=1).numpy())
        combined_iterations = np.concatenate(iterations)
        assert np.array_equal(np.sort(df.values), np.sort(combined_iterations))


@pytest.mark.skip(
    reason=(
        "Waiting for Pandas DataFrame.values for extension arrays fix to be "
        "released. See https://github.com/pandas-dev/pandas/pull/43160"))
@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_tf(ray_start_regular_shared, pipelined):
    import tensorflow as tf

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim, ) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape).astype(np.float)
    # TODO(Clark): Ensure that heterogeneous columns is properly supported
    # (tf.RaggedTensorSpec)
    df1 = pd.DataFrame({
        "one": TensorArray(arr),
        "two": TensorArray(arr),
        "label": TensorArray(arr),
    })
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape).astype(np.float)
    df2 = pd.DataFrame({
        "one": TensorArray(arr2),
        "two": TensorArray(arr2),
        "label": TensorArray(arr2),
    })
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    tfd = ds.to_tf(
        label_column="label",
        output_signature=(tf.TensorSpec(
            shape=(None, 2, 2, 2, 2), dtype=tf.float32),
                          tf.TensorSpec(
                              shape=(None, 1, 2, 2, 2), dtype=tf.float32)))
    iterations = []
    for batch in tfd.as_numpy_iterator():
        iterations.append(np.concatenate((batch[0], batch[1]), axis=1))
    combined_iterations = np.concatenate(iterations)
    arr = np.array(
        [[np.asarray(v) for v in values] for values in df.to_numpy()])
    np.testing.assert_array_equal(arr, combined_iterations)


@pytest.mark.parametrize(
    "fs,data_path", [(None, lazy_fixture("local_path")),
                     (lazy_fixture("local_fs"), lazy_fixture("local_path")),
                     (lazy_fixture("s3_fs"), lazy_fixture("s3_path"))])
def test_numpy_roundtrip(ray_start_regular_shared, fs, data_path):
    ds = ray.data.range_tensor(10, parallelism=2)
    ds.write_numpy(data_path, filesystem=fs)
    ds = ray.data.read_numpy(data_path, filesystem=fs)
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=None, "
        "schema={value: <ArrowTensorType: shape=(1,), dtype=int64>})")
    assert str(ds.take(2)) == \
        "[{'value': array([0])}, {'value': array([1])}]"


def test_numpy_read(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    np.save(
        os.path.join(path, "test.npy"), np.expand_dims(np.arange(0, 10), 1))
    ds = ray.data.read_numpy(path)
    assert str(ds) == (
        "Dataset(num_blocks=1, num_rows=None, "
        "schema={value: <ArrowTensorType: shape=(1,), dtype=int64>})")
    assert str(ds.take(2)) == \
        "[{'value': array([0])}, {'value': array([1])}]"


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_numpy_write(ray_start_regular_shared, fs, data_path, endpoint_url):
    ds = ray.data.range_tensor(10, parallelism=2)
    ds._set_uuid("data")
    ds.write_numpy(data_path, filesystem=fs)
    file_path1 = os.path.join(data_path, "data_000000.npy")
    file_path2 = os.path.join(data_path, "data_000001.npy")
    if endpoint_url is None:
        arr1 = np.load(file_path1)
        arr2 = np.load(file_path2)
    else:
        from s3fs.core import S3FileSystem
        s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_url})
        arr1 = np.load(s3.open(file_path1))
        arr2 = np.load(s3.open(file_path2))
    assert ds.count() == 10
    assert len(arr1) == 5
    assert len(arr2) == 5
    assert arr1.sum() == 10
    assert arr2.sum() == 35
    assert str(ds.take(1)) == "[{'value': array([0])}]"


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_numpy_write_block_path_provider(ray_start_regular_shared, fs,
                                         data_path, endpoint_url,
                                         test_block_write_path_provider):
    ds = ray.data.range_tensor(10, parallelism=2)
    ds._set_uuid("data")
    ds.write_numpy(
        data_path,
        filesystem=fs,
        block_path_provider=test_block_write_path_provider)
    file_path1 = os.path.join(data_path, "000000_05_data.test.npy")
    file_path2 = os.path.join(data_path, "000001_05_data.test.npy")
    if endpoint_url is None:
        arr1 = np.load(file_path1)
        arr2 = np.load(file_path2)
    else:
        from s3fs.core import S3FileSystem
        s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_url})
        arr1 = np.load(s3.open(file_path1))
        arr2 = np.load(s3.open(file_path2))
    assert ds.count() == 10
    assert len(arr1) == 5
    assert len(arr2) == 5
    assert arr1.sum() == 10
    assert arr2.sum() == 35
    assert str(ds.take(1)) == "[{'value': array([0])}]"


def test_read_text(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    with open(os.path.join(path, "file1.txt"), "w") as f:
        f.write("hello\n")
        f.write("world")
    with open(os.path.join(path, "file2.txt"), "w") as f:
        f.write("goodbye")
    ds = ray.data.read_text(path)
    assert sorted(ds.take()) == ["goodbye", "hello", "world"]


@pytest.mark.parametrize("pipelined", [False, True])
def test_write_datasource(ray_start_regular_shared, pipelined):
    output = DummyOutputDatasource()
    ds0 = ray.data.range(10, parallelism=2)
    ds = maybe_pipeline(ds0, pipelined)
    ds.write_datasource(output)
    if pipelined:
        assert output.num_ok == 2
    else:
        assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10

    ray.get(output.data_sink.set_enabled.remote(False))
    ds = maybe_pipeline(ds0, pipelined)
    with pytest.raises(ValueError):
        ds.write_datasource(output)
    if pipelined:
        assert output.num_ok == 2
    else:
        assert output.num_ok == 1
    assert output.num_failed == 1
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10


def test_empty_dataset(ray_start_regular_shared):
    ds = ray.data.range(0)
    assert ds.count() == 0
    assert ds.size_bytes() is None
    assert ds.schema() is None

    ds = ray.data.range(1)
    ds = ds.filter(lambda x: x > 1)
    assert str(ds) == \
        "Dataset(num_blocks=1, num_rows=0, schema=Unknown schema)"

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
    ds2 = ray.data.range_arrow(10)
    ds3 = ds2.repartition(5)
    ds4 = ds3.map(lambda x: {"a": "hi", "b": 1.0}).limit(5).repartition(1)
    assert str(ds) == \
        "Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)"
    assert str(ds2) == \
        "Dataset(num_blocks=10, num_rows=10, schema={value: int64})"
    assert str(ds3) == \
        "Dataset(num_blocks=5, num_rows=10, schema={value: int64})"
    assert str(ds4) == \
        "Dataset(num_blocks=1, num_rows=5, schema={a: string, b: double})"


def test_lazy_loading_exponential_rampup(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=20)
    assert ds._blocks._num_computed() == 1
    assert ds.take(10) == list(range(10))
    assert ds._blocks._num_computed() == 2
    assert ds.take(20) == list(range(20))
    assert ds._blocks._num_computed() == 4
    assert ds.take(30) == list(range(30))
    assert ds._blocks._num_computed() == 8
    assert ds.take(50) == list(range(50))
    assert ds._blocks._num_computed() == 16
    assert ds.take(100) == list(range(100))
    assert ds._blocks._num_computed() == 20


def test_limit(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=20)
    for i in range(100):
        assert ds.limit(i).take(200) == list(range(i))


def test_convert_types(ray_start_regular_shared):
    plain_ds = ray.data.range(1)
    arrow_ds = plain_ds.map(lambda x: {"a": x})
    assert arrow_ds.take() == [{"a": 0}]
    assert "ArrowRow" in arrow_ds.map(lambda x: str(type(x))).take()[0]

    arrow_ds = ray.data.range_arrow(1)
    assert arrow_ds.map(lambda x: "plain_{}".format(x["value"])).take() \
        == ["plain_0"]
    assert arrow_ds.map(lambda x: {"a": (x["value"],)}).take() == \
        [{"a": (0,)}]


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
    ds = ray.data.range_arrow(20, parallelism=10)
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

    large = ray.data.range_arrow(10000, parallelism=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


def test_from_pandas(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows

    # test from single pandas dataframe
    ds = ray.data.from_pandas(df1)
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows


def test_from_pandas_refs(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas_refs([ray.put(df1), ray.put(df2)])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows

    # test from single pandas dataframe ref
    ds = ray.data.from_pandas_refs(ray.put(df1))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows


def test_from_numpy(ray_start_regular_shared):
    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)
    ds = ray.data.from_numpy([ray.put(arr1), ray.put(arr2)])
    values = np.array(ds.take(8))
    np.testing.assert_array_equal(
        values, np.expand_dims(np.concatenate((arr1, arr2)), axis=1))


def test_from_arrow(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow(
        [pa.Table.from_pandas(df1),
         pa.Table.from_pandas(df2)])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows

    # test from single pyarrow table
    ds = ray.data.from_arrow(pa.Table.from_pandas(df1))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows


def test_from_arrow_refs(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow_refs([
        ray.put(pa.Table.from_pandas(df1)),
        ray.put(pa.Table.from_pandas(df2))
    ])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows

    # test from single pyarrow table ref
    ds = ray.data.from_arrow_refs(ray.put(pa.Table.from_pandas(df1)))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows


def test_to_pandas(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range_arrow(n)
    dfds = ds.to_pandas()
    assert df.equals(dfds)

    # Test limit.
    with pytest.raises(ValueError):
        dfds = ds.to_pandas(limit=3)

    # Test limit greater than number of rows.
    dfds = ds.to_pandas(limit=6)
    assert df.equals(dfds)


def test_take_all(ray_start_regular_shared):
    assert ray.data.range(5).take_all() == [0, 1, 2, 3, 4]

    with pytest.raises(ValueError):
        assert ray.data.range(5).take_all(4)


def test_to_pandas_refs(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range_arrow(n)
    dfds = pd.concat(ray.get(ds.to_pandas_refs()), ignore_index=True)
    assert df.equals(dfds)


def test_to_numpy_refs(ray_start_regular_shared):
    # Simple Dataset
    ds = ray.data.range(10)
    arr = np.concatenate(ray.get(ds.to_numpy_refs()))
    np.testing.assert_equal(arr, np.arange(0, 10))

    # Tensor Dataset
    ds = ray.data.range_tensor(10, parallelism=2)
    arr = np.concatenate(ray.get(ds.to_numpy_refs(column="value")))
    np.testing.assert_equal(arr, np.expand_dims(np.arange(0, 10), 1))

    # Table Dataset
    ds = ray.data.range_arrow(10)
    arr = np.concatenate(ray.get(ds.to_numpy_refs(column="value")))
    np.testing.assert_equal(arr, np.arange(0, 10))

    # Table Dataset requires column
    with pytest.raises(ValueError):
        ray.get(ds.to_numpy_refs())


def test_to_arrow_refs(ray_start_regular_shared):
    n = 5

    # Zero-copy.
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range_arrow(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow_refs())],
        ignore_index=True)
    assert df.equals(dfds)

    # Conversion.
    df = pd.DataFrame({0: list(range(n))})
    ds = ray.data.range(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow_refs())],
        ignore_index=True)
    assert df.equals(dfds)


def test_get_internal_block_refs(ray_start_regular_shared):
    blocks = ray.data.range(10).get_internal_block_refs()
    assert len(blocks) == 10
    out = []
    for b in ray.get(blocks):
        out.extend(list(BlockAccessor.for_block(b).iter_rows()))
    out = sorted(out)
    assert out == list(range(10)), out


def test_pandas_roundtrip(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    dfds = ds.to_pandas()
    assert pd.concat([df1, df2], ignore_index=True).equals(dfds)


def test_fsspec_filesystem(ray_start_regular_shared, tmp_path):
    """Same as `test_parquet_write` but using a custom, fsspec filesystem.

    TODO (Alex): We should write a similar test with a mock PyArrow fs, but
    unfortunately pa.fs._MockFileSystem isn't serializable, so this may require
    some effort.
    """
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    path1 = os.path.join(str(tmp_path), "test1.parquet")
    pq.write_table(table, path1)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(str(tmp_path), "test2.parquet")
    pq.write_table(table, path2)

    fs = LocalFileSystem()

    ds = ray.data.read_parquet([path1, path2], filesystem=fs)

    # Test metadata-only parquet ops.
    assert ds._blocks._num_computed() == 1
    assert ds.count() == 6

    out_path = os.path.join(tmp_path, "out")
    os.mkdir(out_path)

    ds._set_uuid("data")
    ds.write_parquet(out_path)

    ds_df1 = pd.read_parquet(os.path.join(out_path, "data_000000.parquet"))
    ds_df2 = pd.read_parquet(os.path.join(out_path, "data_000001.parquet"))
    ds_df = pd.concat([ds_df1, ds_df2])
    df = pd.concat([df1, df2])
    assert ds_df.equals(df)


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (lazy_fixture("s3_fs_with_space"), lazy_fixture("s3_path_with_space")
         )  # Path contains space.
    ])
def test_parquet_read(ray_start_regular_shared, fs, data_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    setup_data_path = _unwrap_protocol(data_path)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    ds = ray.data.read_parquet(data_path, filesystem=fs)

    # Test metadata-only parquet ops.
    assert ds._blocks._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert str(ds) == \
        "Dataset(num_blocks=2, num_rows=6, " \
        "schema={one: int64, two: string})", ds
    assert repr(ds) == \
        "Dataset(num_blocks=2, num_rows=6, " \
        "schema={one: int64, two: string})", ds
    assert ds._blocks._num_computed() == 1

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._blocks._num_computed() == 2
    assert sorted(values) == [[1, "a"], [2, "b"], [3, "c"], [4, "e"], [5, "f"],
                              [6, "g"]]

    # Test column selection.
    ds = ray.data.read_parquet(data_path, columns=["one"], filesystem=fs)
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]
    assert ds.schema().names == ["one"]


@pytest.mark.parametrize(
    "fs,data_path", [(None, lazy_fixture("local_path")),
                     (lazy_fixture("local_fs"), lazy_fixture("local_path")),
                     (lazy_fixture("s3_fs"), lazy_fixture("s3_path"))])
def test_parquet_read_partitioned(ray_start_regular_shared, fs, data_path):
    df = pd.DataFrame({
        "one": [1, 1, 1, 3, 3, 3],
        "two": ["a", "b", "c", "e", "f", "g"]
    })
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=_unwrap_protocol(data_path),
        partition_cols=["one"],
        filesystem=fs,
        use_legacy_dataset=False)

    ds = ray.data.read_parquet(data_path, filesystem=fs)

    # Test metadata-only parquet ops.
    assert ds._blocks._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert str(ds) == \
        "Dataset(num_blocks=2, num_rows=6, " \
        "schema={two: string, " \
        "one: dictionary<values=int32, indices=int32, ordered=0>})", ds
    assert repr(ds) == \
        "Dataset(num_blocks=2, num_rows=6, " \
        "schema={two: string, " \
        "one: dictionary<values=int32, indices=int32, ordered=0>})", ds
    assert ds._blocks._num_computed() == 1

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._blocks._num_computed() == 2
    assert sorted(values) == [[1, "a"], [1, "b"], [1, "c"], [3, "e"], [3, "f"],
                              [3, "g"]]

    # Test column selection.
    ds = ray.data.read_parquet(data_path, columns=["one"], filesystem=fs)
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 1, 1, 3, 3, 3]


def test_parquet_read_partitioned_with_filter(ray_start_regular_shared,
                                              tmp_path):
    df = pd.DataFrame({
        "one": [1, 1, 1, 3, 3, 3],
        "two": ["a", "a", "b", "b", "c", "c"]
    })
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=str(tmp_path),
        partition_cols=["one"],
        use_legacy_dataset=False)

    # 2 partitions, 1 empty partition, 1 block/read task

    ds = ray.data.read_parquet(
        str(tmp_path), parallelism=1, filter=(pa.dataset.field("two") == "a"))

    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._blocks._num_computed() == 1
    assert sorted(values) == [[1, "a"], [1, "a"]]

    # 2 partitions, 1 empty partition, 2 block/read tasks, 1 empty block

    ds = ray.data.read_parquet(
        str(tmp_path), parallelism=2, filter=(pa.dataset.field("two") == "a"))

    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._blocks._num_computed() == 2
    assert sorted(values) == [[1, "a"], [1, "a"]]


def test_parquet_read_with_udf(ray_start_regular_shared, tmp_path):
    one_data = list(range(6))
    df = pd.DataFrame({
        "one": one_data,
        "two": 2 * ["a"] + 2 * ["b"] + 2 * ["c"]
    })
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=str(tmp_path),
        partition_cols=["two"],
        use_legacy_dataset=False)

    def _block_udf(block: pa.Table):
        df = block.to_pandas()
        df["one"] += 1
        return pa.Table.from_pandas(df)

    # 1 block/read task

    ds = ray.data.read_parquet(
        str(tmp_path), parallelism=1, _block_udf=_block_udf)

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    assert ds._blocks._num_computed() == 1
    np.testing.assert_array_equal(sorted(ones), np.array(one_data) + 1)

    # 2 blocks/read tasks

    ds = ray.data.read_parquet(
        str(tmp_path), parallelism=2, _block_udf=_block_udf)

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    assert ds._blocks._num_computed() == 2
    np.testing.assert_array_equal(sorted(ones), np.array(one_data) + 1)

    # 2 blocks/read tasks, 1 empty block

    ds = ray.data.read_parquet(
        str(tmp_path),
        parallelism=2,
        filter=(pa.dataset.field("two") == "a"),
        _block_udf=_block_udf)

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    assert ds._blocks._num_computed() == 2
    np.testing.assert_array_equal(sorted(ones), np.array(one_data[:2]) + 1)


@pytest.mark.parametrize(
    "fs,data_path",
    [(None, lazy_fixture("local_path")),
     (lazy_fixture("local_fs"), lazy_fixture("local_path")),
     (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
     (lazy_fixture("s3_fs_with_space"), lazy_fixture("s3_path_with_space"))])
def test_parquet_read_parallel_meta_fetch(ray_start_regular_shared, fs,
                                          data_path):
    setup_data_path = _unwrap_protocol(data_path)
    num_dfs = PARALLELIZE_META_FETCH_THRESHOLD + 1
    for idx in range(num_dfs):
        df = pd.DataFrame({"one": list(range(3 * idx, 3 * (idx + 1)))})
        table = pa.Table.from_pandas(df)
        path = os.path.join(setup_data_path, f"test_{idx}.parquet")
        pq.write_table(table, path, filesystem=fs)

    parallelism = 8
    ds = ray.data.read_parquet(
        data_path, filesystem=fs, parallelism=parallelism)

    # Test metadata-only parquet ops.
    assert ds._blocks._num_computed() == 1
    assert ds.count() == num_dfs * 3
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == num_dfs, input_files
    assert ds._blocks._num_computed() == 1

    # Forces a data read.
    values = [s["one"] for s in ds.take(limit=3 * num_dfs)]
    assert ds._blocks._num_computed() == parallelism
    assert sorted(values) == list(range(3 * num_dfs))


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_parquet_write(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    path = os.path.join(data_path, "test_parquet_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    ds._set_uuid("data")
    ds.write_parquet(path, filesystem=fs)
    path1 = os.path.join(path, "data_000000.parquet")
    path2 = os.path.join(path, "data_000001.parquet")
    dfds = pd.concat([
        pd.read_parquet(path1, storage_options=storage_options),
        pd.read_parquet(path2, storage_options=storage_options)
    ])
    assert df.equals(dfds)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_parquet_write_create_dir(ray_start_regular_shared, fs, data_path,
                                  endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    path = os.path.join(data_path, "test_parquet_dir")
    ds._set_uuid("data")
    ds.write_parquet(path, filesystem=fs)

    # Ensure that directory was created.
    if fs is None:
        assert os.path.isdir(path)
    else:
        assert fs.get_file_info(
            _unwrap_protocol(path)).type == pa.fs.FileType.Directory

    # Check that data was properly written to the directory.
    path1 = os.path.join(path, "data_000000.parquet")
    path2 = os.path.join(path, "data_000001.parquet")
    dfds = pd.concat([
        pd.read_parquet(path1, storage_options=storage_options),
        pd.read_parquet(path2, storage_options=storage_options)
    ])
    assert df.equals(dfds)

    # Ensure that directories that already exist are left alone and that the
    # attempted creation still succeeds.
    path3 = os.path.join(path, "data_0000002.parquet")
    path4 = os.path.join(path, "data_0000003.parquet")
    if fs is None:
        os.rename(path1, path3)
        os.rename(path2, path4)
    else:
        fs.move(_unwrap_protocol(path1), _unwrap_protocol(path3))
        fs.move(_unwrap_protocol(path2), _unwrap_protocol(path4))
    ds.write_parquet(path, filesystem=fs)

    # Check that the original Parquet files were left untouched and that the
    # new ones were added.
    dfds = pd.concat([
        pd.read_parquet(path1, storage_options=storage_options),
        pd.read_parquet(path2, storage_options=storage_options),
        pd.read_parquet(path3, storage_options=storage_options),
        pd.read_parquet(path4, storage_options=storage_options)
    ])
    assert pd.concat([df, df]).equals(dfds)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


def test_parquet_write_with_udf(ray_start_regular_shared, tmp_path):
    data_path = str(tmp_path)
    one_data = list(range(6))
    df1 = pd.DataFrame({"one": one_data[:3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": one_data[3:], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])

    def _block_udf(block: pa.Table):
        df = block.to_pandas()
        df["one"] += 1
        return pa.Table.from_pandas(df)

    # 2 write tasks
    ds._set_uuid("data")
    ds.write_parquet(data_path, _block_udf=_block_udf)
    path1 = os.path.join(data_path, "data_000000.parquet")
    path2 = os.path.join(data_path, "data_000001.parquet")
    dfds = pd.concat([pd.read_parquet(path1), pd.read_parquet(path2)])
    expected_df = df
    expected_df["one"] += 1
    assert expected_df.equals(dfds)


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_parquet_write_block_path_provider(ray_start_regular_shared, fs,
                                           data_path, endpoint_url,
                                           test_block_write_path_provider):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    path = os.path.join(data_path, "test_parquet_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    ds._set_uuid("data")

    ds.write_parquet(
        path,
        filesystem=fs,
        block_path_provider=test_block_write_path_provider)
    path1 = os.path.join(path, "000000_03_data.test.parquet")
    path2 = os.path.join(path, "000001_03_data.test.parquet")
    dfds = pd.concat([
        pd.read_parquet(path1, storage_options=storage_options),
        pd.read_parquet(path2, storage_options=storage_options)
    ])
    assert df.equals(dfds)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


@pytest.mark.parametrize(
    "fs,data_path", [(None, lazy_fixture("local_path")),
                     (lazy_fixture("local_fs"), lazy_fixture("local_path")),
                     (lazy_fixture("s3_fs"), lazy_fixture("s3_path"))])
def test_parquet_roundtrip(ray_start_regular_shared, fs, data_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    path = os.path.join(data_path, "test_parquet_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    ds.write_parquet(path, filesystem=fs)
    ds2 = ray.data.read_parquet(path, parallelism=2, filesystem=fs)
    ds2df = ds2.to_pandas()
    assert pd.concat([df1, df2], ignore_index=True).equals(ds2df)
    # Test metadata ops.
    for block, meta in ds2._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


def test_convert_to_pyarrow(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(100)
    assert ds.to_dask().sum().compute()[0] == 4950
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds.write_parquet(path)
    assert ray.data.read_parquet(path).count() == 100


def test_pyarrow(ray_start_regular_shared):
    ds = ray.data.range_arrow(5)
    assert ds.map(lambda x: {"b": x["value"] + 2}).take() == \
        [{"b": 2}, {"b": 3}, {"b": 4}, {"b": 5}, {"b": 6}]
    assert ds.map(lambda x: {"b": x["value"] + 2}) \
        .filter(lambda x: x["b"] % 2 == 0).take() == \
        [{"b": 2}, {"b": 4}, {"b": 6}]
    assert ds.filter(lambda x: x["value"] == 0) \
        .flat_map(lambda x: [{"b": x["value"] + 2}, {"b": x["value"] + 20}]) \
        .take() == [{"b": 2}, {"b": 20}]


def test_read_binary_files(ray_start_regular_shared):
    with util.gen_bin_files(10) as (_, paths):
        ds = ray.data.read_binary_files(paths, parallelism=10)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item
        # Test metadata ops.
        assert ds.count() == 10
        assert "bytes" in str(ds.schema()), ds
        assert "bytes" in str(ds), ds


def test_read_binary_files_with_fs(ray_start_regular_shared):
    with util.gen_bin_files(10) as (tempdir, paths):
        # All the paths are absolute, so we want the root file system.
        fs, _ = pa.fs.FileSystem.from_uri("/")
        ds = ray.data.read_binary_files(paths, filesystem=fs, parallelism=10)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item


def test_read_binary_files_with_paths(ray_start_regular_shared):
    with util.gen_bin_files(10) as (_, paths):
        ds = ray.data.read_binary_files(
            paths, include_paths=True, parallelism=10)
        for i, (path, item) in enumerate(ds.iter_rows()):
            assert path == paths[i]
            expected = open(paths[i], "rb").read()
            assert expected == item


# TODO(Clark): Hitting S3 in CI is currently broken due to some AWS
# credentials issue, unskip this test once that's fixed or once ported to moto.
@pytest.mark.skip(reason="Shouldn't hit S3 in CI")
def test_read_binary_files_s3(ray_start_regular_shared):
    ds = ray.data.read_binary_files(["s3://anyscale-data/small-files/0.dat"])
    item = ds.take(1).pop()
    expected = requests.get(
        "https://anyscale-data.s3.us-west-2.amazonaws.com/small-files/0.dat"
    ).content
    assert item == expected


def test_sliding_window():
    arr = list(range(10))

    # Test all windows over this iterable.
    window_sizes = list(range(1, len(arr) + 1))
    for window_size in window_sizes:
        windows = list(_sliding_window(arr, window_size))
        assert len(windows) == len(arr) - window_size + 1
        assert all(len(window) == window_size for window in windows)
        assert all(
            list(window) == arr[i:i + window_size]
            for i, window in enumerate(windows))

    # Test window size larger than iterable length.
    windows = list(_sliding_window(arr, 15))
    assert len(windows) == 1
    assert list(windows[0]) == arr


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

    # blocks format.
    for batch, df in zip(ds.iter_batches(batch_format="native"), dfs):
        assert batch.to_pandas().equals(df)

    # Batch size.
    batch_size = 2
    batches = list(
        ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert (len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size))
    assert pd.concat(
        batches, ignore_index=True).equals(pd.concat(dfs, ignore_index=True))

    # Batch size larger than block.
    batch_size = 4
    batches = list(
        ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert (len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size))
    assert pd.concat(
        batches, ignore_index=True).equals(pd.concat(dfs, ignore_index=True))

    # Batch size drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(
            batch_size=batch_size, drop_last=True, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert (len(batches) == (len(df1) + len(df2) + len(df3) + len(df4)) //
            batch_size)
    assert pd.concat(
        batches, ignore_index=True).equals(
            pd.concat(dfs, ignore_index=True)[:10])

    # Batch size don't drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(
            batch_size=batch_size, drop_last=False, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches[:-1])
    assert (len(batches[-1]) == (len(df1) + len(df2) + len(df3) + len(df4)) %
            batch_size)
    assert (len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size))
    assert pd.concat(
        batches, ignore_index=True).equals(pd.concat(dfs, ignore_index=True))

    # Prefetch.
    batches = list(ds.iter_batches(prefetch_blocks=1, batch_format="pandas"))
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    batch_size = 2
    batches = list(
        ds.iter_batches(
            prefetch_blocks=2, batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert (len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size))
    assert pd.concat(
        batches, ignore_index=True).equals(pd.concat(dfs, ignore_index=True))

    # Prefetch more than number of blocks.
    batches = list(
        ds.iter_batches(prefetch_blocks=len(dfs), batch_format="pandas"))
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

    for num_blocks in np.random.randint(
            1, max_num_blocks + 1, size=num_blocks_samples):
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
                    pd.DataFrame({
                        "value": list(
                            range(running_size, running_size + block_size))
                    }))
                running_size += block_size
            num_rows = running_size
            ds = ray.data.from_pandas(dfs)
            for batch_size in np.random.randint(
                    1, num_rows + 1, size=batch_size_samples):
                for drop_last in (False, True):
                    batches = list(
                        ds.iter_batches(
                            batch_size=batch_size,
                            drop_last=drop_last,
                            batch_format="pandas"))
                    if num_rows % batch_size == 0 or not drop_last:
                        # Number of batches should be equal to
                        # num_rows / batch_size,  rounded up.
                        assert len(batches) == math.ceil(num_rows / batch_size)
                        # Concatenated batches should equal the DataFrame
                        # representation of the entire dataset.
                        assert pd.concat(
                            batches, ignore_index=True).equals(ds.to_pandas())
                    else:
                        # Number of batches should be equal to
                        # num_rows / batch_size, rounded down.
                        assert len(batches) == num_rows // batch_size
                        # Concatenated batches should equal the DataFrame
                        # representation of the dataset with the partial batch
                        # remainder sliced off.
                        assert pd.concat(
                            batches, ignore_index=True).equals(
                                ds.to_pandas()[:batch_size *
                                               (num_rows // batch_size)])
                    if num_rows % batch_size == 0 or drop_last:
                        assert all(
                            len(batch) == batch_size for batch in batches)
                    else:
                        assert all(
                            len(batch) == batch_size for batch in batches[:-1])
                        assert len(batches[-1]) == num_rows % batch_size


def test_lazy_loading_iter_batches_exponential_rampup(
        ray_start_regular_shared):
    ds = ray.data.range(32, parallelism=8)
    expected_num_blocks = [1, 2, 4, 4, 8, 8, 8, 8]
    for _, expected in zip(ds.iter_batches(), expected_num_blocks):
        assert ds._blocks._num_computed() == expected


def test_map_batch(ray_start_regular_shared, tmp_path):
    # Test input validation
    ds = ray.data.range(5)
    with pytest.raises(ValueError):
        ds.map_batches(
            lambda x: x + 1, batch_format="pyarrow", batch_size=-1).take()

    # Test pandas
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))
    ds = ray.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(
        lambda df: df + 1, batch_size=1, batch_format="pandas").take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test Pyarrow
    ds = ray.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(
        lambda pa: pa, batch_size=1, batch_format="pyarrow").take()
    values = [s["one"] for s in ds_list]
    assert values == [1, 2, 3]
    values = [s["two"] for s in ds_list]
    assert values == [2, 3, 4]

    # Test batch
    size = 300
    ds = ray.data.range(size)
    ds_list = ds.map_batches(
        lambda df: df + 1, batch_size=17,
        batch_format="pandas").take(limit=size)
    for i in range(size):
        # The pandas column is "0", and it originally has rows from 0~299.
        # After the map batch, it should have 1~300.
        row = ds_list[i]
        assert row["0"] == i + 1
    assert ds.count() == 300

    # Test the lambda returns different types than the batch_format
    # pandas => list block
    ds = ray.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(lambda df: [1], batch_size=1).take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # pyarrow => list block
    ds = ray.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(
        lambda df: [1], batch_size=1, batch_format="pyarrow").take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # Test the wrong return value raises an exception.
    ds = ray.data.read_parquet(str(tmp_path))
    with pytest.raises(ValueError):
        ds_list = ds.map_batches(
            lambda df: 1, batch_size=2, batch_format="pyarrow").take()


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


def test_split_at_indices(ray_start_regular_shared):
    ds = ray.data.range(10, parallelism=3)

    with pytest.raises(ValueError):
        ds.split_at_indices([])

    with pytest.raises(ValueError):
        ds.split_at_indices([-1])

    with pytest.raises(ValueError):
        ds.split_at_indices([3, 1])

    splits = ds.split_at_indices([5])
    r = [s.take() for s in splits]
    assert r == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_at_indices([2, 5])
    r = [s.take() for s in splits]
    assert r == [[0, 1], [2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_at_indices([2, 5, 5, 100])
    r = [s.take() for s in splits]
    assert r == [[0, 1], [2, 3, 4], [], [5, 6, 7, 8, 9], []]

    splits = ds.split_at_indices([100])
    r = [s.take() for s in splits]
    assert r == [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], []]

    splits = ds.split_at_indices([0])
    r = [s.take() for s in splits]
    assert r == [[], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]


def test_split(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    datasets = ds.split(5)
    assert [2] * 5 == [
        dataset._blocks.initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(3)
    assert [4, 3, 3] == [
        dataset._blocks.initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(1)
    assert [10] == [
        dataset._blocks.initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(10)
    assert [1] * 10 == [
        dataset._blocks.initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(11)
    assert [1] * 10 + [0] == [
        dataset._blocks.initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])


def test_split_hints(ray_start_regular_shared):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

    def assert_split_assignment(block_node_ids, actor_node_ids,
                                expected_split_result):
        """Helper function to setup split hints test.

        Args:
            block_node_ids: a list of blocks with their locations. For
                example ["node1", "node2"] represents two blocks with
                "node1", "node2" as their location respectively.
            actor_node_ids: a list of actors with their locations. For
                example ["node1", "node2"] represents two actors with
                "node1", "node2" as their location respectively.
            expected_split_result: a list of allocation result, each entry
                in the list stores the block_index in the split dataset.
                For example, [[0, 1], [2]] represents the split result has
                two datasets, datasets[0] contains block 0 and 1; and
                datasets[1] contains block 2.
        """
        num_blocks = len(block_node_ids)
        ds = ray.data.range(num_blocks, parallelism=num_blocks)
        blocks = ds._blocks.get_blocks()
        assert len(block_node_ids) == len(blocks)
        actors = [Actor.remote() for i in range(len(actor_node_ids))]
        with patch("ray.experimental.get_object_locations") as location_mock:
            with patch("ray.state.actors") as state_mock:
                block_locations = {}
                for i, node_id in enumerate(block_node_ids):
                    if node_id:
                        block_locations[blocks[i]] = {"node_ids": [node_id]}
                location_mock.return_value = block_locations

                actor_state = {}
                for i, node_id in enumerate(actor_node_ids):
                    actor_state[actors[i]._actor_id.hex()] = {
                        "Address": {
                            "NodeID": node_id
                        }
                    }

                state_mock.return_value = actor_state

                datasets = ds.split(len(actors), locality_hints=actors)
                assert len(datasets) == len(actors)
                for i in range(len(actors)):
                    assert {blocks[j]
                            for j in expected_split_result[i]} == set(
                                datasets[i]._blocks.get_blocks())

    assert_split_assignment(["node2", "node1", "node1"], ["node1", "node2"],
                            [[1, 2], [0]])
    assert_split_assignment(["node1", "node1", "node1"], ["node1", "node2"],
                            [[2, 1], [0]])
    assert_split_assignment(["node2", "node2", None], ["node1", "node2"],
                            [[0, 2], [1]])
    assert_split_assignment(["node2", "node2", None], [None, None],
                            [[2, 1], [0]])
    assert_split_assignment(["n1", "n2", "n3", "n1", "n2"], ["n1", "n2"],
                            [[0, 2, 3], [1, 4]])

    assert_split_assignment(["n1", "n2"], ["n1", "n2", "n3"], [[0], [1], []])

    # perfect split:
    #
    # split 300 blocks
    #   with node_ids interleaving between "n0", "n1", "n2"
    #
    # to 3 actors
    #   with has node_id "n1", "n2", "n0"
    #
    # expect that block 1, 4, 7... are assigned to actor with node_id n1
    #             block 2, 5, 8... are assigned to actor with node_id n2
    #             block 0, 3, 6... are assigned to actor with node_id n0
    assert_split_assignment(
        ["n0", "n1", "n2"] * 100, ["n1", "n2", "n0"],
        [range(1, 300, 3),
         range(2, 300, 3),
         range(0, 300, 3)])

    # even split regardless of locality:
    #
    # split 301 blocks
    #   with block 0 to block 50 on "n0",
    #        block 51 to block 300 on "n1"
    #
    # to 3 actors
    #   with node_ids "n1", "n2", "n0"
    #
    # expect that block 200 to block 300 are assigned to actor with node_id n1
    #             block 100 to block 199 are assigned to actor with node_id n2
    #             block 0 to block 99 are assigned to actor with node_id n0
    assert_split_assignment(["n0"] * 50 + ["n1"] * 251, ["n1", "n2", "n0"], [
        range(200, 301),
        range(100, 200),
        list(range(0, 50)) + list(range(50, 100))
    ])


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
    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))}, )
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
    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))}, )
    modf1 = mopd.DataFrame(df)
    ds = ray.data.from_pandas([df])
    modf2 = ds.to_modin()
    assert modf1.equals(modf2)


@pytest.mark.parametrize("pipelined", [False, True])
def test_to_tf(ray_start_regular_shared, pipelined):
    import tensorflow as tf
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "label": [4.0, 5.0, 6.0]
    })
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])
    ds = maybe_pipeline(ds, pipelined)
    tfd = ds.to_tf(
        label_column="label",
        output_signature=(tf.TensorSpec(shape=(None, 2), dtype=tf.float32),
                          tf.TensorSpec(shape=(None), dtype=tf.float32)))
    iterations = []
    for batch in tfd.as_numpy_iterator():
        iterations.append(
            np.concatenate((batch[0], batch[1].reshape(-1, 1)), axis=1))
    combined_iterations = np.concatenate(iterations)
    assert np.array_equal(df.values, combined_iterations)


def test_to_tf_feature_columns(ray_start_regular_shared):
    import tensorflow as tf
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "label": [4.0, 5.0, 6.0]
    })
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3]).drop("two", axis=1)
    ds = ray.data.from_pandas([df1, df2, df3])
    tfd = ds.to_tf(
        label_column="label",
        feature_columns=["one"],
        output_signature=(tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
                          tf.TensorSpec(shape=(None), dtype=tf.float32)))
    iterations = []
    for batch in tfd.as_numpy_iterator():
        iterations.append(
            np.concatenate((batch[0], batch[1].reshape(-1, 1)), axis=1))
    combined_iterations = np.concatenate(iterations)
    assert np.array_equal(df.values, combined_iterations)


@pytest.mark.parametrize("pipelined", [False, True])
def test_to_torch(ray_start_regular_shared, pipelined):
    import torch
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "label": [4.0, 5.0, 6.0]
    })
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
def test_to_torch_feature_columns(ray_start_regular_shared, input, force_dtype,
                                  label_type):
    import torch
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "three": [4.0, 5.0, 6.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "three": [7.0, 8.0, 9.0],
        "label": [4.0, 5.0, 6.0]
    })
    df3 = pd.DataFrame({
        "one": [7, 8],
        "two": [7.0, 8.0],
        "three": [10.0, 11.0],
        "label": [7.0, 8.0]
    })
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
        batch_size=3)
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
            assert all(
                isinstance(item, torch.Tensor) for item in features.values())
            if force_dtype:
                assert all(
                    item.dtype == torch.long for item in features.values())
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


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_json_read(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(
        path1, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(path1, filesystem=fs)
    dsdf = ds.to_pandas()
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert "{one: int64, two: string}" in str(ds), ds

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "test2.json")
    df2.to_json(
        path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([path1, path2], parallelism=2, filesystem=fs)
    dsdf = ds.to_pandas()
    df = pd.concat([df1, df2], ignore_index=True)
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(data_path, "test3.json")
    df3.to_json(
        path3, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(
        [path1, path2, path3], parallelism=2, filesystem=fs)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)

    # Directory, two files.
    path = os.path.join(data_path, "test_json_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.json")
    df1.to_json(
        path1, orient="records", lines=True, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.json")
    df2.to_json(
        path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(path, filesystem=fs)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))

    # Two directories, three files.
    path1 = os.path.join(data_path, "test_json_dir1")
    path2 = os.path.join(data_path, "test_json_dir2")
    if fs is None:
        os.mkdir(path1)
        os.mkdir(path2)
    else:
        fs.create_dir(_unwrap_protocol(path1))
        fs.create_dir(_unwrap_protocol(path2))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    file_path1 = os.path.join(path1, "data0.json")
    df1.to_json(
        file_path1,
        orient="records",
        lines=True,
        storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    file_path2 = os.path.join(path2, "data1.json")
    df2.to_json(
        file_path2,
        orient="records",
        lines=True,
        storage_options=storage_options)
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    file_path3 = os.path.join(path2, "data2.json")
    df3.to_json(
        file_path3,
        orient="records",
        lines=True,
        storage_options=storage_options)
    ds = ray.data.read_json([path1, path2], filesystem=fs)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(path1)
        shutil.rmtree(path2)
    else:
        fs.delete_dir(_unwrap_protocol(path1))
        fs.delete_dir(_unwrap_protocol(path2))

    # Directory and file, two files.
    dir_path = os.path.join(data_path, "test_json_dir")
    if fs is None:
        os.mkdir(dir_path)
    else:
        fs.create_dir(_unwrap_protocol(dir_path))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.json")
    df1.to_json(
        path1, orient="records", lines=True, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "data1.json")
    df2.to_json(
        path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([dir_path, path2], filesystem=fs)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(dir_path)
    else:
        fs.delete_dir(_unwrap_protocol(dir_path))


def test_zipped_json_read(ray_start_regular_shared, tmp_path):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.json.gz")
    df1.to_json(path1, compression="gzip", orient="records", lines=True)
    ds = ray.data.read_json(path1)
    assert df1.equals(ds.to_pandas())
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [path1]

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.json.gz")
    df2.to_json(path2, compression="gzip", orient="records", lines=True)
    ds = ray.data.read_json([path1, path2], parallelism=2)
    dsdf = ds.to_pandas()
    assert pd.concat([df1, df2], ignore_index=True).equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes()

    # Directory and file, two files.
    dir_path = os.path.join(tmp_path, "test_json_dir")
    os.mkdir(dir_path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.json.gz")
    df1.to_json(path1, compression="gzip", orient="records", lines=True)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "data1.json.gz")
    df2.to_json(path2, compression="gzip", orient="records", lines=True)
    ds = ray.data.read_json([dir_path, path2])
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    shutil.rmtree(dir_path)


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_json_write(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    file_path = os.path.join(data_path, "data_000000.json")
    assert df1.equals(
        pd.read_json(
            file_path,
            orient="records",
            lines=True,
            storage_options=storage_options))

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    file_path2 = os.path.join(data_path, "data_000001.json")
    df = pd.concat([df1, df2])
    ds_df = pd.concat([
        pd.read_json(
            file_path,
            orient="records",
            lines=True,
            storage_options=storage_options),
        pd.read_json(
            file_path2,
            orient="records",
            lines=True,
            storage_options=storage_options)
    ])
    assert df.equals(ds_df)


@pytest.mark.parametrize(
    "fs,data_path", [(None, lazy_fixture("local_path")),
                     (lazy_fixture("local_fs"), lazy_fixture("local_path")),
                     (lazy_fixture("s3_fs"), lazy_fixture("s3_path"))])
def test_json_roundtrip(ray_start_regular_shared, fs, data_path):
    # Single block.
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    file_path = os.path.join(data_path, "data_000000.json")
    ds2 = ray.data.read_json([file_path], filesystem=fs)
    ds2df = ds2.to_pandas()
    assert ds2df.equals(df)
    # Test metadata ops.
    for block, meta in ds2._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    if fs is None:
        os.remove(file_path)
    else:
        fs.delete_file(_unwrap_protocol(file_path))

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df, df2])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    ds2 = ray.data.read_json(data_path, parallelism=2, filesystem=fs)
    ds2df = ds2.to_pandas()
    assert pd.concat([df, df2], ignore_index=True).equals(ds2df)
    # Test metadata ops.
    for block, meta in ds2._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_json_write_block_path_provider(ray_start_regular_shared, fs,
                                        data_path, endpoint_url,
                                        test_block_write_path_provider):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_json(
        data_path,
        filesystem=fs,
        block_path_provider=test_block_write_path_provider)
    file_path = os.path.join(data_path, "000000_03_data.test.json")
    assert df1.equals(
        pd.read_json(
            file_path,
            orient="records",
            lines=True,
            storage_options=storage_options))

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_json(
        data_path,
        filesystem=fs,
        block_path_provider=test_block_write_path_provider)
    file_path2 = os.path.join(data_path, "000001_03_data.test.json")
    df = pd.concat([df1, df2])
    ds_df = pd.concat([
        pd.read_json(
            file_path,
            orient="records",
            lines=True,
            storage_options=storage_options),
        pd.read_json(
            file_path2,
            orient="records",
            lines=True,
            storage_options=storage_options)
    ])
    assert df.equals(ds_df)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [(None, lazy_fixture("local_path"), None),
     (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
     (lazy_fixture("s3_fs"), lazy_fixture("s3_path"),
      lazy_fixture("s3_server")),
     (lazy_fixture("s3_fs_with_space"), lazy_fixture("s3_path_with_space"),
      lazy_fixture("s3_server"))])
def test_csv_read(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(path1, filesystem=fs)
    dsdf = ds.to_pandas()
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert "{one: int64, two: string}" in str(ds), ds

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "test2.csv")
    df2.to_csv(path2, index=False, storage_options=storage_options)
    ds = ray.data.read_csv([path1, path2], parallelism=2, filesystem=fs)
    dsdf = ds.to_pandas()
    df = pd.concat([df1, df2], ignore_index=True)
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(data_path, "test3.csv")
    df3.to_csv(path3, index=False, storage_options=storage_options)
    ds = ray.data.read_csv([path1, path2, path3], parallelism=2, filesystem=fs)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)

    # Directory, two files.
    path = os.path.join(data_path, "test_csv_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.csv")
    df1.to_csv(path1, index=False, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.csv")
    df2.to_csv(path2, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(path, filesystem=fs)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))

    # Two directories, three files.
    path1 = os.path.join(data_path, "test_csv_dir1")
    path2 = os.path.join(data_path, "test_csv_dir2")
    if fs is None:
        os.mkdir(path1)
        os.mkdir(path2)
    else:
        fs.create_dir(_unwrap_protocol(path1))
        fs.create_dir(_unwrap_protocol(path2))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    file_path1 = os.path.join(path1, "data0.csv")
    df1.to_csv(file_path1, index=False, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    file_path2 = os.path.join(path2, "data1.csv")
    df2.to_csv(file_path2, index=False, storage_options=storage_options)
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    file_path3 = os.path.join(path2, "data2.csv")
    df3.to_csv(file_path3, index=False, storage_options=storage_options)
    ds = ray.data.read_csv([path1, path2], filesystem=fs)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(path1)
        shutil.rmtree(path2)
    else:
        fs.delete_dir(_unwrap_protocol(path1))
        fs.delete_dir(_unwrap_protocol(path2))

    # Directory and file, two files.
    dir_path = os.path.join(data_path, "test_csv_dir")
    if fs is None:
        os.mkdir(dir_path)
    else:
        fs.create_dir(_unwrap_protocol(dir_path))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.csv")
    df1.to_csv(path1, index=False, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "data1.csv")
    df2.to_csv(path2, index=False, storage_options=storage_options)
    ds = ray.data.read_csv([dir_path, path2], filesystem=fs)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(dir_path)
    else:
        fs.delete_dir(_unwrap_protocol(dir_path))


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_csv_write(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_csv(data_path, filesystem=fs)
    file_path = os.path.join(data_path, "data_000000.csv")
    assert df1.equals(pd.read_csv(file_path, storage_options=storage_options))

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_csv(data_path, filesystem=fs)
    file_path2 = os.path.join(data_path, "data_000001.csv")
    df = pd.concat([df1, df2])
    ds_df = pd.concat([
        pd.read_csv(file_path, storage_options=storage_options),
        pd.read_csv(file_path2, storage_options=storage_options)
    ])
    assert df.equals(ds_df)


@pytest.mark.parametrize(
    "fs,data_path", [(None, lazy_fixture("local_path")),
                     (lazy_fixture("local_fs"), lazy_fixture("local_path")),
                     (lazy_fixture("s3_fs"), lazy_fixture("s3_path"))])
def test_csv_roundtrip(ray_start_regular_shared, fs, data_path):
    # Single block.
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df])
    ds._set_uuid("data")
    ds.write_csv(data_path, filesystem=fs)
    file_path = os.path.join(data_path, "data_000000.csv")
    ds2 = ray.data.read_csv([file_path], filesystem=fs)
    ds2df = ds2.to_pandas()
    assert ds2df.equals(df)
    # Test metadata ops.
    for block, meta in ds2._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df, df2])
    ds._set_uuid("data")
    ds.write_csv(data_path, filesystem=fs)
    ds2 = ray.data.read_csv(data_path, parallelism=2, filesystem=fs)
    ds2df = ds2.to_pandas()
    assert pd.concat([df, df2], ignore_index=True).equals(ds2df)
    # Test metadata ops.
    for block, meta in ds2._blocks.get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


@pytest.mark.parametrize("fs,data_path,endpoint_url", [
    (None, lazy_fixture("local_path"), None),
    (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
    (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server"))
])
def test_csv_write_block_path_provider(ray_start_regular_shared, fs, data_path,
                                       endpoint_url,
                                       test_block_write_path_provider):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_csv(
        data_path,
        filesystem=fs,
        block_path_provider=test_block_write_path_provider)
    file_path = os.path.join(data_path, "000000_03_data.test.csv")
    assert df1.equals(pd.read_csv(file_path, storage_options=storage_options))

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_csv(
        data_path,
        filesystem=fs,
        block_path_provider=test_block_write_path_provider)
    file_path2 = os.path.join(data_path, "000001_03_data.test.csv")
    df = pd.concat([df1, df2])
    ds_df = pd.concat([
        pd.read_csv(file_path, storage_options=storage_options),
        pd.read_csv(file_path2, storage_options=storage_options)
    ])
    assert df.equals(ds_df)


def test_groupby_arrow(ray_start_regular_shared):
    # Test empty dataset.
    agg_ds = ray.data.range_arrow(10).filter(
        lambda r: r["value"] > 10).groupby("value").count()
    assert agg_ds.count() == 0


def test_groupby_errors(ray_start_regular_shared):
    ds = ray.data.range(100)

    ds.groupby(None).count().show()  # OK
    ds.groupby(lambda x: x % 2).count().show()  # OK
    with pytest.raises(TypeError):
        ds.groupby("foo").count().show()

    ds = ray.data.range_arrow(100)  # OK
    ds.groupby(None).count().show()  # OK
    with pytest.raises(NotImplementedError):
        ds.groupby(lambda x: x % 2).count().show()


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_agg_name_conflict(ray_start_regular_shared, num_parts):
    # Test aggregation name conflict.
    xs = list(range(100))
    grouped_ds = ray.data.from_items([{
        "A": (x % 3),
        "B": x
    } for x in xs]).repartition(num_parts).groupby("A")
    agg_ds = grouped_ds.aggregate(
        AggregateFn(
            init=lambda k: [0, 0],
            accumulate=lambda a, r: [a[0] + r["B"], a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name="foo"),
        AggregateFn(
            init=lambda k: [0, 0],
            accumulate=lambda a, r: [a[0] + r["B"], a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name="foo"))
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "foo": 49.5, "foo_2": 49.5},
         {"A": 1, "foo": 49.0, "foo_2": 49.0},
         {"A": 2, "foo": 50.0, "foo_2": 50.0}]


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_count(ray_start_regular_shared, num_parts):
    # Test built-in count aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items([{
        "A": (x % 3),
        "B": x
    } for x in xs]).repartition(num_parts).groupby("A").count()
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "count()": 34}, {"A": 1, "count()": 33},
         {"A": 2, "count()": 33}]


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_sum(ray_start_regular_shared, num_parts):
    # Test built-in sum aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items([{
        "A": (x % 3),
        "B": x
    } for x in xs]).repartition(num_parts).groupby("A").sum("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "sum(B)": 1683}, {"A": 1, "sum(B)": 1617},
         {"A": 2, "sum(B)": 1650}]
    # Test built-in global sum aggregation
    assert ray.data.from_items([{
        "A": x
    } for x in xs]).repartition(num_parts).sum("A") == 4950
    assert ray.data.range_arrow(10).filter(lambda r: r["value"] > 10).sum(
        "value") == 0


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_min(ray_start_regular_shared, num_parts):
    # Test built-in min aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items([{
        "A": (x % 3),
        "B": x
    } for x in xs]).repartition(num_parts).groupby("A").min("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "min(B)": 0}, {"A": 1, "min(B)": 1},
         {"A": 2, "min(B)": 2}]
    # Test built-in global min aggregation
    assert ray.data.from_items([{
        "A": x
    } for x in xs]).repartition(num_parts).min("A") == 0
    with pytest.raises(ValueError):
        ray.data.range_arrow(10).filter(lambda r: r["value"] > 10).min("value")


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_max(ray_start_regular_shared, num_parts):
    # Test built-in max aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items([{
        "A": (x % 3),
        "B": x
    } for x in xs]).repartition(num_parts).groupby("A").max("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "max(B)": 99}, {"A": 1, "max(B)": 97},
         {"A": 2, "max(B)": 98}]
    # Test built-in global max aggregation
    assert ray.data.from_items([{
        "A": x
    } for x in xs]).repartition(num_parts).max("A") == 99
    with pytest.raises(ValueError):
        ray.data.range_arrow(10).filter(lambda r: r["value"] > 10).max("value")


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_mean(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items([{
        "A": (x % 3),
        "B": x
    } for x in xs]).repartition(num_parts).groupby("A").mean("B")
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "mean(B)": 49.5}, {"A": 1, "mean(B)": 49.0},
         {"A": 2, "mean(B)": 50.0}]
    # Test built-in global mean aggregation
    assert ray.data.from_items([{
        "A": x
    } for x in xs]).repartition(num_parts).mean("A") == 49.5
    with pytest.raises(ValueError):
        ray.data.range_arrow(10).filter(lambda r: r["value"] > 10).mean(
            "value")


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_std(ray_start_regular_shared, num_parts):
    # Test built-in std aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_std with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = ray.data.from_pandas(df).repartition(num_parts).groupby("A").std(
        "B")
    assert agg_ds.count() == 3
    result = agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std().to_numpy()
    np.testing.assert_array_almost_equal(result, expected)
    # ddof of 0
    agg_ds = ray.data.from_pandas(df).repartition(num_parts).groupby("A").std(
        "B", ddof=0)
    assert agg_ds.count() == 3
    result = agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std(ddof=0).to_numpy()
    np.testing.assert_array_almost_equal(result, expected)
    # Test built-in global std aggregation
    df = pd.DataFrame({"A": xs})
    assert math.isclose(
        ray.data.from_pandas(df).repartition(num_parts).std("A"),
        df["A"].std())
    # ddof of 0
    assert math.isclose(
        ray.data.from_pandas(df).repartition(num_parts).std("A", ddof=0),
        df["A"].std(ddof=0))
    with pytest.raises(ValueError):
        ray.data.from_pandas(pd.DataFrame({"A": []})).std("A")
    # Test edge cases
    assert ray.data.from_pandas(pd.DataFrame({"A": [3]})).std("A") == 0


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_multicolumn(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation on multiple columns
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_multicolumn with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({
        "A": [x % 3 for x in xs],
        "B": xs,
        "C": [2 * x for x in xs]
    })
    agg_ds = ray.data.from_pandas(df).repartition(num_parts).groupby("A").mean(
        ["B", "C"])
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "mean(B)": 49.5, "mean(C)": 99.0},
         {"A": 1, "mean(B)": 49.0, "mean(C)": 98.0},
         {"A": 2, "mean(B)": 50.0, "mean(C)": 100.0}]
    # Test that unspecified agg column ==> agg on all columns except for
    # groupby keys.
    agg_ds = ray.data.from_pandas(df).repartition(num_parts).groupby(
        "A").mean()
    assert agg_ds.count() == 3
    assert [row.as_pydict() for row in agg_ds.sort("A").iter_rows()] == \
        [{"A": 0, "mean(B)": 49.5, "mean(C)": 99.0},
         {"A": 1, "mean(B)": 49.0, "mean(C)": 98.0},
         {"A": 2, "mean(B)": 50.0, "mean(C)": 100.0}]
    # Test built-in global mean aggregation
    df = pd.DataFrame({"A": xs, "B": [2 * x for x in xs]})
    result_row = ray.data.from_pandas(df).repartition(num_parts).mean(
        ["A", "B"])
    assert result_row["mean(A)"] == df["A"].mean()
    assert result_row["mean(B)"] == df["B"].mean()


def test_groupby_agg_bad_on(ray_start_regular_shared):
    # Test bad on for groupby aggregation
    xs = list(range(100))
    df = pd.DataFrame({
        "A": [x % 3 for x in xs],
        "B": xs,
        "C": [2 * x for x in xs]
    })
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


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_arrow_multi_agg(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_multi_agg with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = ray.data.from_pandas(df).repartition(num_parts).groupby(
        "A").aggregate(
            Count(),
            Sum("B"),
            Min("B"),
            Max("B"),
            Mean("B"),
            Std("B"),
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
    result_row = ray.data.from_pandas(df).repartition(num_parts).aggregate(
        Sum("A"),
        Min("A"),
        Max("A"),
        Mean("A"),
        Std("A"),
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
    xs = [("A", 2), ("A", 4), ("A", 9), ("B", 10), ("B", 20), ("C", 3),
          ("C", 5), ("C", 8), ("C", 12)]
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)
    # Mean aggregation
    agg_ds = ds.groupby(lambda r: r[0]).aggregate(
        AggregateFn(
            init=lambda k: (0, 0),
            accumulate=lambda a, r: (a[0] + r[1], a[1] + 1),
            merge=lambda a1, a2: (a1[0] + a2[0], a1[1] + a2[1]),
            finalize=lambda a: a[0] / a[1]))
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [("A", 5), ("B", 15),
                                                       ("C", 7)]

    # Test None row
    parallelism = 2
    xs = ["A", "A", "A", None, None, None, "B"]
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)
    # Count aggregation
    agg_ds = ds.groupby(lambda r: str(r)).aggregate(
        AggregateFn(
            init=lambda k: 0,
            accumulate=lambda a, r: a + 1,
            merge=lambda a1, a2: a1 + a2))
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: str(r[0])).take(3) == [("A", 3), ("B", 1),
                                                            ("None", 3)]

    # Test empty dataset.
    ds = ray.data.from_items([])
    agg_ds = ds.groupby(lambda r: r[0]).aggregate(
        AggregateFn(
            init=lambda k: 1 / 0,  # should never reach here
            accumulate=lambda a, r: 1 / 0,
            merge=lambda a1, a2: 1 / 0,
            finalize=lambda a: 1 / 0))
    assert agg_ds.count() == 0
    assert agg_ds == ds
    agg_ds = ray.data.range(10).filter(lambda r: r > 10).groupby(
        lambda r: r).count()
    assert agg_ds.count() == 0


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_count(ray_start_regular_shared, num_parts):
    # Test built-in count aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).count()
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 34), (1, 33), (2,
                                                                          33)]


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_sum(ray_start_regular_shared, num_parts):
    # Test built-in sum aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).sum()
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 1683), (1, 1617),
                                                       (2, 1650)]
    # Test built-in global sum aggregation
    assert ray.data.from_items(xs).repartition(num_parts).sum() == 4950
    assert ray.data.range(10).filter(lambda r: r > 10).sum() == 0


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_min(ray_start_regular_shared, num_parts):
    # Test built-in min aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).min()
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 0), (1, 1), (2, 2)]
    # Test built-in global min aggregation
    assert ray.data.from_items(xs).repartition(num_parts).min() == 0
    with pytest.raises(ValueError):
        ray.data.range(10).filter(lambda r: r > 10).min()


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_max(ray_start_regular_shared, num_parts):
    # Test built-in max aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).max()
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 99), (1, 97), (2,
                                                                          98)]
    # Test built-in global max aggregation
    assert ray.data.from_items(xs).repartition(num_parts).max() == 99
    with pytest.raises(ValueError):
        ray.data.range(10).filter(lambda r: r > 10).max()


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_mean(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).mean()
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 49.5), (1, 49.0),
                                                       (2, 50.0)]
    # Test built-in global mean aggregation
    assert ray.data.from_items(xs).repartition(num_parts).mean() == 49.5
    with pytest.raises(ValueError):
        ray.data.range(10).filter(lambda r: r > 10).mean()


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_std(ray_start_regular_shared, num_parts):
    # Test built-in std aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_std with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).std()
    assert agg_ds.count() == 3
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    expected = df.groupby("A")["B"].std()
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # ddof of 0
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).std(ddof=0)
    assert agg_ds.count() == 3
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    expected = df.groupby("A")["B"].std(ddof=0)
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # Test built-in global std aggregation
    assert math.isclose(
        ray.data.from_items(xs).repartition(num_parts).std(),
        pd.Series(xs).std())
    # ddof of 0
    assert math.isclose(
        ray.data.from_items(xs).repartition(num_parts).std(ddof=0),
        pd.Series(xs).std(ddof=0))
    with pytest.raises(ValueError):
        ray.data.from_items([]).std()
    # Test edge cases
    assert ray.data.from_items([3]).std() == 0


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_multilambda(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_multilambda with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = ray.data.from_items([[x, 2*x] for x in xs]) \
        .repartition(num_parts) \
        .groupby(lambda x: x[0] % 3) \
        .mean([lambda x: x[0], lambda x: x[1]])
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 49.5,
                                                        99.0), (1, 49.0, 98.0),
                                                       (2, 50.0, 100.0)]
    # Test built-in global mean aggregation
    assert ray.data.from_items(
        [[x, 2 * x] for x in xs]).repartition(num_parts).mean(
            [lambda x: x[0], lambda x: x[1]]) == (49.5, 99.0)
    with pytest.raises(ValueError):
        ray.data.from_items([[x, 2*x] for x in range(10)]) \
            .filter(lambda r: r[0] > 10) \
            .mean([lambda x: x[0], lambda x: x[1]])


@pytest.mark.parametrize("num_parts", [1, 15, 100])
def test_groupby_simple_multi_agg(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_multi_agg with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = ray.data.from_items(xs).repartition(num_parts).groupby(
        lambda x: x % 3).aggregate(
            Count(),
            Sum(),
            Min(),
            Max(),
            Mean(),
            Std(),
        )
    assert agg_ds.count() == 3
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, counts, sums, mins, maxs, means, stds = zip(*result)
    agg_df = pd.DataFrame({
        "groups": list(groups),
        "count": list(counts),
        "sum": list(sums),
        "min": list(mins),
        "max": list(maxs),
        "mean": list(means),
        "std": list(stds),
    })
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
    result_row = ray.data.from_items(xs).repartition(num_parts).aggregate(
        Sum(),
        Min(),
        Max(),
        Mean(),
        Std(),
    )
    series = pd.Series(xs)
    for idx, agg in enumerate(["sum", "min", "max", "mean", "std"]):
        result = result_row[idx]
        expected = getattr(series, agg)()
        if agg == "std":
            assert math.isclose(result, expected)
        else:
            assert result == expected


def test_sort_simple(ray_start_regular_shared):
    num_items = 100
    parallelism = 4
    xs = list(range(num_items))
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)
    assert ds.sort().take(num_items) == list(range(num_items))
    # Make sure we have rows in each block.
    assert len(
        [n for n in ds.sort()._block_num_rows() if n > 0]) == parallelism
    assert ds.sort(descending=True).take(num_items) == list(
        reversed(range(num_items)))
    assert ds.sort(key=lambda x: -x).take(num_items) == list(
        reversed(range(num_items)))

    # Test empty dataset.
    ds = ray.data.from_items([])
    s1 = ds.sort()
    assert s1.count() == 0
    assert s1 == ds
    ds = ray.data.range(10).filter(lambda r: r > 10).sort()
    assert ds.count() == 0


@pytest.mark.parametrize("pipelined", [False, True])
def test_random_shuffle(shutdown_only, pipelined):
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

    r0 = ray.data.range_arrow(100, parallelism=5).take(999)
    r1 = ray.data.range_arrow(
        100, parallelism=5).random_shuffle(seed=0).take(999)
    r2 = ray.data.range_arrow(
        100, parallelism=5).random_shuffle(seed=0).take(999)
    assert r1 == r2, (r1, r2)
    assert r1 != r0, (r1, r0)

    # Test move.
    ds = range(100, parallelism=2)
    r1 = ds.random_shuffle(_move=True).take(999)
    if pipelined:
        with pytest.raises(RuntimeError):
            ds = ds.map(lambda x: x).take(999)
    else:
        # Source dataset should be unusable if not pipelining.
        with pytest.raises(ValueError):
            ds = ds.map(lambda x: x).take(999)
    r2 = range(100).random_shuffle(_move=True).take(999)
    assert r1 != r2, (r1, r2)

    # Test empty dataset.
    ds = ray.data.from_items([])
    r1 = ds.random_shuffle()
    assert r1.count() == 0
    assert r1 == ds


def test_random_shuffle_spread(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        _system_config={"max_direct_call_object_size": 0})
    cluster.add_node(resources={"bar:1": 100})
    cluster.add_node(resources={"bar:2": 100})
    cluster.add_node(resources={"bar:3": 100}, num_cpus=0)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().node_id.hex()

    node1_id = ray.get(get_node_id.options(resources={"bar:1": 1}).remote())
    node2_id = ray.get(get_node_id.options(resources={"bar:2": 1}).remote())

    ds = ray.data.range(
        100, parallelism=2).random_shuffle(_spread_resource_prefix="bar:")
    blocks = ds.get_internal_block_refs()
    ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
    location_data = ray.experimental.get_object_locations(blocks)
    locations = []
    for block in blocks:
        locations.extend(location_data[block]["node_ids"])
    assert set(locations) == {node1_id, node2_id}


def test_parquet_read_spread(ray_start_cluster, tmp_path):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        _system_config={"max_direct_call_object_size": 0})
    cluster.add_node(resources={"bar:1": 100})
    cluster.add_node(resources={"bar:2": 100})
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
    df2 = pd.DataFrame({
        "one": list(range(300, 400)),
        "two": list(range(400, 500))
    })
    path2 = os.path.join(data_path, "test2.parquet")
    df2.to_parquet(path2)

    ds = ray.data.read_parquet(data_path, _spread_resource_prefix="bar:")

    # Force reads.
    blocks = ds.get_internal_block_refs()
    assert len(blocks) == 2

    ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
    location_data = ray.experimental.get_object_locations(blocks)
    locations = []
    for block in blocks:
        locations.extend(location_data[block]["node_ids"])
    assert set(locations) == {node1_id, node2_id}


@pytest.mark.parametrize("num_items,parallelism", [(100, 1), (1000, 4)])
def test_sort_arrow(ray_start_regular, num_items, parallelism):
    a = list(reversed(range(num_items)))
    b = [f"{x:03}" for x in range(num_items)]
    shard = int(np.ceil(num_items / parallelism))
    offset = 0
    dfs = []
    while offset < num_items:
        dfs.append(
            pd.DataFrame({
                "a": a[offset:offset + shard],
                "b": b[offset:offset + shard]
            }))
        offset += shard
    if offset < num_items:
        dfs.append(pd.DataFrame({"a": a[offset:], "b": b[offset:]}))
    ds = ray.data.from_pandas(dfs)

    def assert_sorted(sorted_ds, expected_rows):
        assert [tuple(row.values())
                for row in sorted_ds.iter_rows()] == list(expected_rows)

    assert_sorted(ds.sort(key="a"), zip(reversed(a), reversed(b)))
    # Make sure we have rows in each block.
    assert len([n for n in ds.sort(key="a")._block_num_rows()
                if n > 0]) == parallelism
    assert_sorted(ds.sort(key="b"), zip(a, b))
    assert_sorted(ds.sort(key="a", descending=True), zip(a, b))
    assert_sorted(
        ds.sort(key=[("b", "descending")]), zip(reversed(a), reversed(b)))


def test_sort_arrow_with_empty_blocks(ray_start_regular):
    assert BlockAccessor.for_block(pa.Table.from_pydict({})).sample(
        10, "A").num_rows == 0

    partitions = BlockAccessor.for_block(pa.Table.from_pydict(
        {})).sort_and_partition(
            [1, 5, 10], "A", descending=False)
    assert len(partitions) == 4
    for partition in partitions:
        assert partition.num_rows == 0

    assert BlockAccessor.for_block(pa.Table.from_pydict(
        {})).merge_sorted_blocks([pa.Table.from_pydict({})], "A",
                                 False)[0].num_rows == 0

    ds = ray.data.from_items(
        [{
            "A": (x % 3),
            "B": x
        } for x in range(3)], parallelism=3)
    ds = ds.filter(lambda r: r["A"] == 0)
    assert [row.as_pydict() for row in ds.sort("A").iter_rows()] == \
        [{"A": 0, "B": 0}]

    # Test empty dataset.
    ds = ray.data.range_arrow(10).filter(lambda r: r["value"] > 10)
    assert len(
        ray.data.impl.sort.sample_boundaries(ds._blocks.get_blocks(), "value",
                                             3)) == 2
    assert ds.sort("value").count() == 0


def test_dataset_retry_exceptions(ray_start_regular, local_path):
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
                for block in CSVDatasource._read_stream(
                        self, f, path, **reader_args):
                    yield block

        def _write_block(self, f: "pa.NativeFile", block: BlockAccessor,
                         **writer_args):
            count = self.counter.increment.remote()
            if ray.get(count) == 1:
                raise ValueError("oops")
            else:
                CSVDatasource._write_block(self, f, block, **writer_args)

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(local_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options={})
    ds1 = ray.data.read_datasource(
        FlakyCSVDatasource(), parallelism=1, paths=path1)
    ds1.write_datasource(
        FlakyCSVDatasource(), path=local_path, dataset_uuid="data")
    assert df1.equals(
        pd.read_csv(
            os.path.join(local_path, "data_000000.csv"), storage_options={}))

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
            ray_remote_args={
                "retry_exceptions": False
            }).take()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
