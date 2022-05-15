import os
import shutil
from typing import Union, List, Dict, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import snappy
from fsspec.implementations.local import LocalFileSystem
from pytest_lazyfixture import lazy_fixture
from io import BytesIO
from functools import partial

import ray

from ray.tests.conftest import *  # noqa
from ray.types import ObjectRef
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource import (
    Datasource,
    DummyOutputDatasource,
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
    PathPartitionFilter,
    PathPartitionEncoder,
    PartitionStyle,
    SimpleTensorFlowDatasource,
    SimpleTorchDatasource,
    WriteResult,
)
from ray.data.impl.arrow_block import ArrowRow
from ray.data.datasource.file_based_datasource import _unwrap_protocol
from ray.data.datasource.parquet_datasource import PARALLELIZE_META_FETCH_THRESHOLD
from ray.data.tests.conftest import *  # noqa


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


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


def df_to_csv(dataframe, path, **kwargs):
    dataframe.to_csv(path, **kwargs)


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas(ray_start_regular_shared, enable_pandas_block):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
        ds = ray.data.from_pandas([df1, df2])
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows

        # test from single pandas dataframe
        ds = ray.data.from_pandas(df1)
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_refs(ray_start_regular_shared, enable_pandas_block):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
        ds = ray.data.from_pandas_refs([ray.put(df1), ray.put(df2)])
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows

        # test from single pandas dataframe ref
        ds = ray.data.from_pandas_refs(ray.put(df1))
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


@pytest.mark.parametrize("from_ref", [False, True])
def test_from_numpy(ray_start_regular_shared, from_ref):
    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)
    arrs = [arr1, arr2]
    if from_ref:
        ds = ray.data.from_numpy_refs([ray.put(arr) for arr in arrs])
    else:
        ds = ray.data.from_numpy(arrs)
    values = np.stack([x["value"] for x in ds.take(8)])
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))

    # Test from single NumPy ndarray.
    if from_ref:
        ds = ray.data.from_numpy_refs(ray.put(arr1))
    else:
        ds = ray.data.from_numpy(arr1)
    values = np.stack([x["value"] for x in ds.take(4)])
    np.testing.assert_array_equal(values, arr1)


def test_from_arrow(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow([pa.Table.from_pandas(df1), pa.Table.from_pandas(df2)])
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
    ds = ray.data.from_arrow_refs(
        [ray.put(pa.Table.from_pandas(df1)), ray.put(pa.Table.from_pandas(df2))]
    )
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


def test_to_arrow_refs(ray_start_regular_shared):
    n = 5

    # Zero-copy.
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range_arrow(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow_refs())], ignore_index=True
    )
    assert df.equals(dfds)

    # Conversion.
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow_refs())], ignore_index=True
    )
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
    assert ds._plan.execute()._num_computed() == 1
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


def test_read_example_data(ray_start_regular_shared, tmp_path):
    ds = ray.data.read_csv("example://iris.csv")
    assert ds.count() == 150
    assert ds.take(1) == [
        {
            "sepal.length": 5.1,
            "sepal.width": 3.5,
            "petal.length": 1.4,
            "petal.width": 0.2,
            "variety": "Setosa",
        }
    ]


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_space"),
            lazy_fixture("s3_path_with_space"),
        ),  # Path contains space.
    ],
)
def test_parquet_read_basic(ray_start_regular_shared, fs, data_path):
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
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert (
        str(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert (
        repr(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert ds._plan.execute()._num_computed() == 1

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 2
    assert sorted(values) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]

    # Test column selection.
    ds = ray.data.read_parquet(data_path, columns=["one"], filesystem=fs)
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]
    assert ds.schema().names == ["one"]


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
)
def test_parquet_read_meta_provider(ray_start_regular_shared, fs, data_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    setup_data_path = _unwrap_protocol(data_path)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    class TestMetadataProvider(DefaultParquetMetadataProvider):
        def prefetch_file_metadata(self, pieces):
            return None

    ds = ray.data.read_parquet(
        data_path,
        filesystem=fs,
        meta_provider=TestMetadataProvider(),
    )

    # Expect precomputed row counts and block sizes to be missing.
    assert ds._meta_count() is None
    assert ds._plan._snapshot_blocks.size_bytes() == -1

    # Expect to lazily compute all metadata correctly.
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert (
        str(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert (
        repr(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert ds._plan.execute()._num_computed() == 2

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 2
    assert sorted(values) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_space"),
            lazy_fixture("s3_path_with_space"),
        ),  # Path contains space.
    ],
)
def test_parquet_read_bulk(ray_start_regular_shared, fs, data_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    setup_data_path = _unwrap_protocol(data_path)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    # Expect directory path expansion to fail.
    with pytest.raises(OSError):
        ray.data.read_parquet_bulk(data_path, filesystem=fs)

    # Expect individual file paths to be processed successfully.
    paths = [path1, path2]
    ds = ray.data.read_parquet_bulk(paths, filesystem=fs)

    # Expect precomputed row counts to be missing.
    assert ds._meta_count() is None

    # Expect to lazily compute all metadata correctly.
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert (
        str(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert (
        repr(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert ds._plan.execute()._num_computed() == 2

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 2
    assert sorted(values) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_space"),
            lazy_fixture("s3_path_with_space"),
        ),  # Path contains space.
    ],
)
def test_parquet_read_bulk_meta_provider(ray_start_regular_shared, fs, data_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    setup_data_path = _unwrap_protocol(data_path)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    # Expect directory path expansion to succeed with the default metadata provider.
    ds = ray.data.read_parquet_bulk(
        data_path,
        filesystem=fs,
        meta_provider=DefaultFileMetadataProvider(),
    )

    # Expect precomputed row counts to be missing.
    assert ds._meta_count() is None

    # Expect to lazily compute all metadata correctly.
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert (
        str(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert (
        repr(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={one: int64, two: string})"
    ), ds
    assert ds._plan.execute()._num_computed() == 2

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 2
    assert sorted(values) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
)
def test_parquet_read_partitioned(ray_start_regular_shared, fs, data_path):
    df = pd.DataFrame(
        {"one": [1, 1, 1, 3, 3, 3], "two": ["a", "b", "c", "e", "f", "g"]}
    )
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=_unwrap_protocol(data_path),
        partition_cols=["one"],
        filesystem=fs,
        use_legacy_dataset=False,
    )

    ds = ray.data.read_parquet(data_path, filesystem=fs)

    # Test metadata-only parquet ops.
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert (
        str(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={two: string, "
        "one: dictionary<values=int32, indices=int32, ordered=0>})"
    ), ds
    assert (
        repr(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={two: string, "
        "one: dictionary<values=int32, indices=int32, ordered=0>})"
    ), ds
    assert ds._plan.execute()._num_computed() == 1

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 2
    assert sorted(values) == [
        [1, "a"],
        [1, "b"],
        [1, "c"],
        [3, "e"],
        [3, "f"],
        [3, "g"],
    ]

    # Test column selection.
    ds = ray.data.read_parquet(data_path, columns=["one"], filesystem=fs)
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 1, 1, 3, 3, 3]


def test_parquet_read_partitioned_with_filter(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame(
        {"one": [1, 1, 1, 3, 3, 3], "two": ["a", "a", "b", "b", "c", "c"]}
    )
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table, root_path=str(tmp_path), partition_cols=["one"], use_legacy_dataset=False
    )

    # 2 partitions, 1 empty partition, 1 block/read task

    ds = ray.data.read_parquet(
        str(tmp_path), parallelism=1, filter=(pa.dataset.field("two") == "a")
    )

    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 1
    assert sorted(values) == [[1, "a"], [1, "a"]]

    # 2 partitions, 1 empty partition, 2 block/read tasks, 1 empty block

    ds = ray.data.read_parquet(
        str(tmp_path), parallelism=2, filter=(pa.dataset.field("two") == "a")
    )

    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 2
    assert sorted(values) == [[1, "a"], [1, "a"]]


def test_parquet_read_partitioned_explicit(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame(
        {"one": [1, 1, 1, 3, 3, 3], "two": ["a", "b", "c", "e", "f", "g"]}
    )
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=str(tmp_path),
        partition_cols=["one"],
        use_legacy_dataset=False,
    )

    schema = pa.schema([("one", pa.int32()), ("two", pa.string())])
    partitioning = pa.dataset.partitioning(schema, flavor="hive")

    ds = ray.data.read_parquet(
        str(tmp_path), dataset_kwargs=dict(partitioning=partitioning)
    )

    # Test metadata-only parquet ops.
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert (
        str(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={two: string, one: int32})"
    ), ds
    assert (
        repr(ds) == "Dataset(num_blocks=2, num_rows=6, "
        "schema={two: string, one: int32})"
    ), ds
    assert ds._plan.execute()._num_computed() == 1

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert ds._plan.execute()._num_computed() == 2
    assert sorted(values) == [
        [1, "a"],
        [1, "b"],
        [1, "c"],
        [3, "e"],
        [3, "f"],
        [3, "g"],
    ]


def test_parquet_read_with_udf(ray_start_regular_shared, tmp_path):
    one_data = list(range(6))
    df = pd.DataFrame({"one": one_data, "two": 2 * ["a"] + 2 * ["b"] + 2 * ["c"]})
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table, root_path=str(tmp_path), partition_cols=["two"], use_legacy_dataset=False
    )

    def _block_udf(block: pa.Table):
        df = block.to_pandas()
        df["one"] += 1
        return pa.Table.from_pandas(df)

    # 1 block/read task

    ds = ray.data.read_parquet(str(tmp_path), parallelism=1, _block_udf=_block_udf)

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    assert ds._plan.execute()._num_computed() == 1
    np.testing.assert_array_equal(sorted(ones), np.array(one_data) + 1)

    # 2 blocks/read tasks

    ds = ray.data.read_parquet(str(tmp_path), parallelism=2, _block_udf=_block_udf)

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    assert ds._plan.execute()._num_computed() == 2
    np.testing.assert_array_equal(sorted(ones), np.array(one_data) + 1)

    # 2 blocks/read tasks, 1 empty block

    ds = ray.data.read_parquet(
        str(tmp_path),
        parallelism=2,
        filter=(pa.dataset.field("two") == "a"),
        _block_udf=_block_udf,
    )

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    assert ds._plan.execute()._num_computed() == 2
    np.testing.assert_array_equal(sorted(ones), np.array(one_data[:2]) + 1)


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (lazy_fixture("s3_fs_with_space"), lazy_fixture("s3_path_with_space")),
    ],
)
def test_parquet_read_parallel_meta_fetch(ray_start_regular_shared, fs, data_path):
    setup_data_path = _unwrap_protocol(data_path)
    num_dfs = PARALLELIZE_META_FETCH_THRESHOLD + 1
    for idx in range(num_dfs):
        df = pd.DataFrame({"one": list(range(3 * idx, 3 * (idx + 1)))})
        table = pa.Table.from_pandas(df)
        path = os.path.join(setup_data_path, f"test_{idx}.parquet")
        pq.write_table(table, path, filesystem=fs)

    parallelism = 8
    ds = ray.data.read_parquet(data_path, filesystem=fs, parallelism=parallelism)

    # Test metadata-only parquet ops.
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == num_dfs * 3
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == num_dfs, input_files
    assert ds._plan.execute()._num_computed() == 1

    # Forces a data read.
    values = [s["one"] for s in ds.take(limit=3 * num_dfs)]
    assert ds._plan.execute()._num_computed() == parallelism
    assert sorted(values) == list(range(3 * num_dfs))


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
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
    dfds = pd.concat(
        [
            pd.read_parquet(path1, storage_options=storage_options),
            pd.read_parquet(path2, storage_options=storage_options),
        ]
    )
    assert df.equals(dfds)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_parquet_write_create_dir(
    ray_start_regular_shared, fs, data_path, endpoint_url
):
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
        assert fs.get_file_info(_unwrap_protocol(path)).type == pa.fs.FileType.Directory

    # Check that data was properly written to the directory.
    path1 = os.path.join(path, "data_000000.parquet")
    path2 = os.path.join(path, "data_000001.parquet")
    dfds = pd.concat(
        [
            pd.read_parquet(path1, storage_options=storage_options),
            pd.read_parquet(path2, storage_options=storage_options),
        ]
    )
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
    dfds = pd.concat(
        [
            pd.read_parquet(path1, storage_options=storage_options),
            pd.read_parquet(path2, storage_options=storage_options),
            pd.read_parquet(path3, storage_options=storage_options),
            pd.read_parquet(path4, storage_options=storage_options),
        ]
    )
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

    def _block_udf(block):
        df = BlockAccessor.for_block(block).to_pandas().copy()
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


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_parquet_write_block_path_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    test_block_write_path_provider,
):
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
        path, filesystem=fs, block_path_provider=test_block_write_path_provider
    )
    path1 = os.path.join(path, "000000_03_data.test.parquet")
    path2 = os.path.join(path, "000001_03_data.test.parquet")
    dfds = pd.concat(
        [
            pd.read_parquet(path1, storage_options=storage_options),
            pd.read_parquet(path2, storage_options=storage_options),
        ]
    )
    assert df.equals(dfds)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
)
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
    for block, meta in ds2._plan.execute().get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
)
def test_numpy_roundtrip(ray_start_regular_shared, fs, data_path):
    ds = ray.data.range_tensor(10, parallelism=2)
    ds.write_numpy(data_path, filesystem=fs)
    ds = ray.data.read_numpy(data_path, filesystem=fs)
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=None, "
        "schema={value: <ArrowTensorType: shape=(1,), dtype=int64>})"
    )
    assert str(ds.take(2)) == "[{'value': array([0])}, {'value': array([1])}]"


def test_numpy_read(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    np.save(os.path.join(path, "test.npy"), np.expand_dims(np.arange(0, 10), 1))
    ds = ray.data.read_numpy(path)
    assert str(ds) == (
        "Dataset(num_blocks=1, num_rows=10, "
        "schema={value: <ArrowTensorType: shape=(1,), dtype=int64>})"
    )
    assert str(ds.take(2)) == "[{'value': array([0])}, {'value': array([1])}]"


def test_numpy_read_meta_provider(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    path = os.path.join(path, "test.npy")
    np.save(path, np.expand_dims(np.arange(0, 10), 1))
    ds = ray.data.read_numpy(path, meta_provider=FastFileMetadataProvider())
    assert str(ds) == (
        "Dataset(num_blocks=1, num_rows=10, "
        "schema={value: <ArrowTensorType: shape=(1,), dtype=int64>})"
    )
    assert str(ds.take(2)) == "[{'value': array([0])}, {'value': array([1])}]"

    with pytest.raises(NotImplementedError):
        ray.data.read_binary_files(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_numpy_read_partitioned_with_filter(
    ray_start_regular_shared,
    tmp_path,
    write_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_np(dataframe, path, **kwargs):
        np.save(path, dataframe.to_numpy(dtype=np.dtype(np.int8)), **kwargs)

    df = pd.DataFrame({"one": [1, 1, 1, 3, 3, 3], "two": [0, 1, 2, 3, 4, 5]})
    partition_keys = ["one"]
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(tmp_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
        )
        write_partitioned_df(
            df,
            partition_keys,
            partition_path_encoder,
            df_to_np,
        )
        df_to_np(df, os.path.join(base_dir, "test.npy"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filter_fn=skip_unpartitioned,
        )
        ds = ray.data.read_numpy(base_dir, partition_filter=partition_path_filter)

        vals = [[1, 0], [1, 1], [1, 2], [3, 3], [3, 4], [3, 5]]
        val_str = "".join([f"{{'value': array({v}, dtype=int8)}}, " for v in vals])[:-2]
        assert_base_partitioned_ds(
            ds,
            schema="{value: <ArrowTensorType: shape=(2,), dtype=int8>}",
            sorted_values=f"[[{val_str}]]",
            ds_take_transform_fn=lambda taken: [taken],
            sorted_values_transform_fn=lambda sorted_values: str(sorted_values),
        )
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
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


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_numpy_write_block_path_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    test_block_write_path_provider,
):
    ds = ray.data.range_tensor(10, parallelism=2)
    ds._set_uuid("data")
    ds.write_numpy(
        data_path, filesystem=fs, block_path_provider=test_block_write_path_provider
    )
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
    with open(os.path.join(path, "file3.txt"), "w") as f:
        f.write("ray\n")
    ds = ray.data.read_text(path)
    assert sorted(ds.take()) == ["goodbye", "hello", "ray", "world"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 5


def test_read_text_meta_provider(
    ray_start_regular_shared,
    tmp_path,
):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    path = os.path.join(path, "file1.txt")
    with open(path, "w") as f:
        f.write("hello\n")
        f.write("world\n")
        f.write("goodbye\n")
        f.write("ray\n")
    ds = ray.data.read_text(path, meta_provider=FastFileMetadataProvider())
    assert sorted(ds.take()) == ["goodbye", "hello", "ray", "world"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 5

    with pytest.raises(NotImplementedError):
        ray.data.read_text(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_read_text_partitioned_with_filter(
    ray_start_regular_shared,
    tmp_path,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_text(dataframe, path, **kwargs):
        dataframe.to_string(path, index=False, header=False, **kwargs)

    partition_keys = ["one"]
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(tmp_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            df_to_text,
        )
        df_to_text(pd.DataFrame({"1": [1]}), os.path.join(base_dir, "test.txt"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filter_fn=skip_unpartitioned,
        )
        ds = ray.data.read_text(base_dir, partition_filter=partition_path_filter)
        assert_base_partitioned_ds(
            ds,
            schema="<class 'str'>",
            num_computed=None,
            sorted_values=["1 a", "1 b", "1 c", "3 e", "3 f", "3 g"],
            ds_take_transform_fn=lambda t: t,
        )
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


def test_read_binary_snappy(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_binary_snappy")
    os.mkdir(path)
    with open(os.path.join(path, "file"), "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(
        path,
        arrow_open_stream_args=dict(compression="snappy"),
    )
    assert sorted(ds.take()) == [byte_str]


def test_read_binary_snappy_inferred(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_binary_snappy_inferred")
    os.mkdir(path)
    with open(os.path.join(path, "file.snappy"), "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(path)
    assert sorted(ds.take()) == [byte_str]


def test_read_binary_meta_provider(
    ray_start_regular_shared,
    tmp_path,
):
    path = os.path.join(tmp_path, "test_binary_snappy")
    os.mkdir(path)
    path = os.path.join(path, "file")
    with open(path, "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(
        path,
        arrow_open_stream_args=dict(compression="snappy"),
        meta_provider=FastFileMetadataProvider(),
    )
    assert sorted(ds.take()) == [byte_str]

    with pytest.raises(NotImplementedError):
        ray.data.read_binary_files(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_read_binary_snappy_partitioned_with_filter(
    ray_start_regular_shared,
    tmp_path,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_binary(dataframe, path, **kwargs):
        with open(path, "wb") as f:
            df_string = dataframe.to_string(index=False, header=False, **kwargs)
            byte_str = df_string.encode()
            bytes = BytesIO(byte_str)
            snappy.stream_compress(bytes, f)

    partition_keys = ["one"]
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(tmp_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            df_to_binary,
        )
        df_to_binary(pd.DataFrame({"1": [1]}), os.path.join(base_dir, "test.snappy"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filter_fn=skip_unpartitioned,
        )
        ds = ray.data.read_binary_files(
            base_dir,
            partition_filter=partition_path_filter,
            arrow_open_stream_args=dict(compression="snappy"),
        )
        assert_base_partitioned_ds(
            ds,
            count=2,
            num_rows=2,
            schema="<class 'bytes'>",
            num_computed=None,
            sorted_values=[b"1 a\n1 b\n1 c", b"3 e\n3 f\n3 g"],
            ds_take_transform_fn=lambda t: t,
        )
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


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


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
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
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([path1, path2], parallelism=2, filesystem=fs)
    dsdf = ds.to_pandas()
    df = pd.concat([df1, df2], ignore_index=True)
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._plan.execute().get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(data_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([path1, path2, path3], parallelism=2, filesystem=fs)
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
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
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
        file_path1, orient="records", lines=True, storage_options=storage_options
    )
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    file_path2 = os.path.join(path2, "data1.json")
    df2.to_json(
        file_path2, orient="records", lines=True, storage_options=storage_options
    )
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    file_path3 = os.path.join(path2, "data2.json")
    df3.to_json(
        file_path3, orient="records", lines=True, storage_options=storage_options
    )
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
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "data1.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
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
    for block, meta in ds._plan.execute().get_blocks_with_metadata():
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


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_meta_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(
        path1,
        filesystem=fs,
        meta_provider=FastFileMetadataProvider(),
    )

    # Expect to lazily compute all metadata correctly.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert "{one: int64, two: string}" in str(ds), ds

    with pytest.raises(NotImplementedError):
        ray.data.read_json(
            path1,
            filesystem=fs,
            meta_provider=BaseFileMetadataProvider(),
        )


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_partitioned_with_filter(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_json(dataframe, path, **kwargs):
        dataframe.to_json(path, **kwargs)

    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )
    file_writer_fn = partial(
        df_to_json,
        orient="records",
        lines=True,
        storage_options=storage_options,
    )
    partition_keys = ["one"]
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(data_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filesystem=fs,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            file_writer_fn,
        )
        file_writer_fn(pd.DataFrame({"1": [1]}), os.path.join(base_dir, "test.json"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filter_fn=skip_unpartitioned,
            filesystem=fs,
        )
        ds = ray.data.read_json(
            base_dir,
            partition_filter=partition_path_filter,
            filesystem=fs,
        )
        assert_base_partitioned_ds(ds)
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
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
            file_path, orient="records", lines=True, storage_options=storage_options
        )
    )

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    file_path2 = os.path.join(data_path, "data_000001.json")
    df = pd.concat([df1, df2])
    ds_df = pd.concat(
        [
            pd.read_json(
                file_path, orient="records", lines=True, storage_options=storage_options
            ),
            pd.read_json(
                file_path2,
                orient="records",
                lines=True,
                storage_options=storage_options,
            ),
        ]
    )
    assert df.equals(ds_df)


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
)
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
    for block, meta in ds2._plan.execute().get_blocks_with_metadata():
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
    for block, meta in ds2._plan.execute().get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_write_block_path_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    test_block_write_path_provider,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_json(
        data_path, filesystem=fs, block_path_provider=test_block_write_path_provider
    )
    file_path = os.path.join(data_path, "000000_03_data.test.json")
    assert df1.equals(
        pd.read_json(
            file_path, orient="records", lines=True, storage_options=storage_options
        )
    )

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_json(
        data_path, filesystem=fs, block_path_provider=test_block_write_path_provider
    )
    file_path2 = os.path.join(data_path, "000001_03_data.test.json")
    df = pd.concat([df1, df2])
    ds_df = pd.concat(
        [
            pd.read_json(
                file_path, orient="records", lines=True, storage_options=storage_options
            ),
            pd.read_json(
                file_path2,
                orient="records",
                lines=True,
                storage_options=storage_options,
            ),
        ]
    )
    assert df.equals(ds_df)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
        (
            lazy_fixture("s3_fs_with_space"),
            lazy_fixture("s3_path_with_space"),
            lazy_fixture("s3_server"),
        ),
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
            lazy_fixture("s3_server"),
        ),
    ],
)
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
    for block, meta in ds._plan.execute().get_blocks_with_metadata():
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


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_read_meta_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(
        path1,
        filesystem=fs,
        meta_provider=FastFileMetadataProvider(),
    )

    dsdf = ds.to_pandas()
    assert df1.equals(dsdf)

    # Expect to lazily compute all metadata correctly.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert "{one: int64, two: string}" in str(ds), ds

    with pytest.raises(NotImplementedError):
        ray.data.read_csv(
            path1,
            filesystem=fs,
            meta_provider=BaseFileMetadataProvider(),
        )


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_read_partitioned_hive_implicit(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )
    partition_keys = ["one"]
    partition_path_encoder = PathPartitionEncoder.of(
        base_dir=data_path,
        field_names=partition_keys,
        filesystem=fs,
    )
    write_base_partitioned_df(
        partition_keys,
        partition_path_encoder,
        partial(df_to_csv, storage_options=storage_options, index=False),
    )
    ds = ray.data.read_csv(
        data_path,
        partition_filter=PathPartitionFilter.of(None, filesystem=fs),
        filesystem=fs,
    )
    assert_base_partitioned_ds(ds)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_read_partitioned_styles_explicit(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )
    partition_keys = ["one"]
    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(data_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filesystem=fs,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            partial(df_to_csv, storage_options=storage_options, index=False),
        )
        partition_path_filter = PathPartitionFilter.of(
            None,
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filesystem=fs,
        )
        ds = ray.data.read_csv(
            base_dir,
            partition_filter=partition_path_filter,
            filesystem=fs,
        )
        assert_base_partitioned_ds(ds)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_read_partitioned_with_filter(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )
    partition_keys = ["one"]
    file_writer_fn = partial(df_to_csv, storage_options=storage_options, index=False)
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(data_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filesystem=fs,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            file_writer_fn,
        )
        file_writer_fn(pd.DataFrame({"1": [1]}), os.path.join(base_dir, "test.csv"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filesystem=fs,
            filter_fn=skip_unpartitioned,
        )
        ds = ray.data.read_csv(
            base_dir,
            partition_filter=partition_path_filter,
            filesystem=fs,
        )
        assert_base_partitioned_ds(ds)
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_read_partitioned_with_filter_multikey(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )
    partition_keys = ["one", "two"]
    file_writer_fn = partial(df_to_csv, storage_options=storage_options, index=False)
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def keep_expected_partitions(kv_dict):
        keep = bool(kv_dict) and (
            (kv_dict["one"] == "1" and kv_dict["two"] in {"a", "b", "c"})
            or (kv_dict["one"] == "3" and kv_dict["two"] in {"e", "f", "g"})
        )
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for i, style in enumerate([PartitionStyle.HIVE, PartitionStyle.DIRECTORY]):
        base_dir = os.path.join(data_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filesystem=fs,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            file_writer_fn,
        )
        df = pd.DataFrame({"1": [1]})
        file_writer_fn(df, os.path.join(data_path, f"test{i}.csv"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filesystem=fs,
            filter_fn=keep_expected_partitions,
        )
        ds = ray.data.read_csv(
            data_path,
            partition_filter=partition_path_filter,
            filesystem=fs,
        )
        assert_base_partitioned_ds(ds, num_input_files=6, num_computed=6)
        assert ray.get(kept_file_counter.get.remote()) == 6
        if i == 0:
            # expect to skip 1 unpartitioned files in the parent of the base directory
            assert ray.get(skipped_file_counter.get.remote()) == 1
        else:
            # expect to skip 2 unpartitioned files in the parent of the base directory
            # plus 6 unpartitioned files in the base directory's sibling directories
            assert ray.get(skipped_file_counter.get.remote()) == 8
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
            lazy_fixture("s3_server"),
        ),
    ],
)
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
    ds_df = pd.concat(
        [
            pd.read_csv(file_path, storage_options=storage_options),
            pd.read_csv(file_path2, storage_options=storage_options),
        ]
    )
    assert df.equals(ds_df)


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
)
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
    for block, meta in ds2._plan.execute().get_blocks_with_metadata():
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
    for block, meta in ds2._plan.execute().get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_write_block_path_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    test_block_write_path_provider,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_csv(
        data_path, filesystem=fs, block_path_provider=test_block_write_path_provider
    )
    file_path = os.path.join(data_path, "000000_03_data.test.csv")
    assert df1.equals(pd.read_csv(file_path, storage_options=storage_options))

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_csv(
        data_path, filesystem=fs, block_path_provider=test_block_write_path_provider
    )
    file_path2 = os.path.join(data_path, "000001_03_data.test.csv")
    df = pd.concat([df1, df2])
    ds_df = pd.concat(
        [
            pd.read_csv(file_path, storage_options=storage_options),
            pd.read_csv(file_path2, storage_options=storage_options),
        ]
    )
    assert df.equals(ds_df)


def test_tensorflow_datasource(ray_start_regular_shared):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]

    def dataset_factory():
        return tfds.load("mnist", split=["train"], as_supervised=True)[0]

    ray_dataset = ray.data.read_datasource(
        SimpleTensorFlowDatasource(), parallelism=1, dataset_factory=dataset_factory
    ).fully_executed()

    assert ray_dataset.num_blocks() == 1

    actual_data = ray_dataset.take_all()
    expected_data = list(tf_dataset)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)


def test_torch_datasource(ray_start_regular_shared, local_path):
    import torchvision

    # Download datasets to separate folders to prevent interference.
    torch_dataset_root = os.path.join(local_path, "torch")
    ray_dataset_root = os.path.join(local_path, "ray")

    torch_dataset = torchvision.datasets.MNIST(torch_dataset_root, download=True)
    expected_data = list(torch_dataset)

    def dataset_factory():
        return torchvision.datasets.MNIST(ray_dataset_root, download=True)

    ray_dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), parallelism=1, dataset_factory=dataset_factory
    )
    actual_data = list(next(ray_dataset.iter_batches(batch_size=None)))

    assert actual_data == expected_data


def test_torch_datasource_value_error(ray_start_regular_shared, local_path):
    import torchvision

    dataset = torchvision.datasets.MNIST(local_path, download=True)

    with pytest.raises(ValueError):
        # `dataset_factory` should be a function, not a Torch dataset.
        ray.data.read_datasource(
            SimpleTorchDatasource(),
            parallelism=1,
            dataset_factory=dataset,
        )


# NOTE: The last test using the shared ray_start_regular_shared cluster must use the
# shutdown_only fixture so the shared cluster is shut down, otherwise the below
# test_write_datasource_ray_remote_args test, which uses a cluster_utils cluster, will
# fail with a double-init.
def test_csv_read_no_header(shutdown_only, tmp_path):
    from pyarrow import csv

    file_path = os.path.join(tmp_path, "test.csv")
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df.to_csv(file_path, index=False, header=False)
    ds = ray.data.read_csv(
        file_path,
        read_options=csv.ReadOptions(column_names=["one", "two"]),
    )
    out_df = ds.to_pandas()
    assert df.equals(out_df)


def test_csv_read_with_column_type_specified(shutdown_only, tmp_path):
    from pyarrow import csv

    file_path = os.path.join(tmp_path, "test.csv")
    df = pd.DataFrame({"one": [1, 2, 3e1], "two": ["a", "b", "c"]})
    df.to_csv(file_path, index=False)

    # Incorrect to parse scientific notation in int64 as PyArrow represents
    # it as double.
    with pytest.raises(pa.lib.ArrowInvalid):
        ray.data.read_csv(
            file_path,
            convert_options=csv.ConvertOptions(
                column_types={"one": "int64", "two": "string"}
            ),
        )

    # Parsing scientific notation in double shoud work.
    ds = ray.data.read_csv(
        file_path,
        convert_options=csv.ConvertOptions(
            column_types={"one": "float64", "two": "string"}
        ),
    )
    expected_df = pd.DataFrame({"one": [1.0, 2.0, 30.0], "two": ["a", "b", "c"]})
    assert ds.to_pandas().equals(expected_df)


class NodeLoggerOutputDatasource(Datasource[Union[ArrowRow, int]]):
    """A writable datasource that logs node IDs of write tasks, for testing."""

    def __init__(self):
        @ray.remote
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.enabled = True
                self.node_ids = set()

            def write(self, node_id: str, block: Block) -> str:
                block = BlockAccessor.for_block(block)
                if not self.enabled:
                    raise ValueError("disabled")
                self.rows_written += block.num_rows()
                self.node_ids.add(node_id)
                return "ok"

            def get_rows_written(self):
                return self.rows_written

            def get_node_ids(self):
                return self.node_ids

            def set_enabled(self, enabled):
                self.enabled = enabled

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0

    def do_write(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        ray_remote_args: Dict[str, Any],
        **write_args,
    ) -> List[ObjectRef[WriteResult]]:
        data_sink = self.data_sink

        @ray.remote
        def write(b):
            node_id = ray.get_runtime_context().node_id.hex()
            return ray.get(data_sink.write.remote(node_id, b))

        tasks = []
        for b in blocks:
            tasks.append(write.options(**ray_remote_args).remote(b))
        return tasks

    def on_write_complete(self, write_results: List[WriteResult]) -> None:
        assert all(w == "ok" for w in write_results), write_results
        self.num_ok += 1

    def on_write_failed(
        self, write_results: List[ObjectRef[WriteResult]], error: Exception
    ) -> None:
        self.num_failed += 1


def test_write_datasource_ray_remote_args(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().node_id.hex()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    output = NodeLoggerOutputDatasource()
    ds = ray.data.range(100, parallelism=10)
    # Pin write tasks to
    ds.write_datasource(output, ray_remote_args={"resources": {"bar": 1}})
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 100

    node_ids = ray.get(output.data_sink.get_node_ids.remote())
    assert node_ids == {bar_node_id}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
