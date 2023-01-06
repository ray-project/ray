import os
import shutil

import pytest
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource import (
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
)
from ray.data.datasource.parquet_datasource import (
    PARALLELIZE_META_FETCH_THRESHOLD,
    _ParquetDatasourceReader,
)
from ray.data.datasource.file_based_datasource import _unwrap_protocol
from ray.data.datasource.parquet_datasource import (
    _SerializedPiece,
    _deserialize_pieces_with_retry,
)

from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

from pytest_lazyfixture import lazy_fixture


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_parquet_deserialize_pieces_with_retry(
    ray_start_regular_shared, fs, data_path, monkeypatch
):

    setup_data_path = _unwrap_protocol(data_path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    dataset_kwargs = {}
    pq_ds = pq.ParquetDataset(
        data_path, **dataset_kwargs, filesystem=fs, use_legacy_dataset=False
    )
    serialized_pieces = [_SerializedPiece(p) for p in pq_ds.pieces]

    # test 1st attempt succeed
    pieces = _deserialize_pieces_with_retry(serialized_pieces)
    assert "test1.parquet" in pieces[0].path
    assert "test2.parquet" in pieces[1].path

    # test the 3rd attempt succeed with a mock function constructed
    # to throw in the first two attempts
    class MockDeserializer:
        def __init__(self, planned_exp_or_return):
            self.planned_exp_or_return = planned_exp_or_return
            self.cur_index = 0

        def __call__(self, *args: Any, **kwds: Any) -> Any:
            exp_or_ret = self.planned_exp_or_return[self.cur_index]
            self.cur_index += 1
            if type(exp_or_ret) == Exception:
                raise exp_or_ret
            else:
                return exp_or_ret

    mock_deserializer = MockDeserializer(
        [
            Exception("1st mock failed attempt"),
            Exception("2nd mock failed attempt"),
            pieces,
        ]
    )
    monkeypatch.setattr(
        ray.data.datasource.parquet_datasource, "_deserialize_pieces", mock_deserializer
    )
    retried_pieces = _deserialize_pieces_with_retry(serialized_pieces)
    assert "test1.parquet" in retried_pieces[0].path
    assert "test2.parquet" in retried_pieces[1].path


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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
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
    assert (
        ds._plan._snapshot_blocks is None
        or ds._plan._snapshot_blocks.size_bytes() == -1
    )

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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
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

    # Expect directory path expansion to fail due to default format-based path
    # filtering: The filter will not match any of the files.
    with pytest.raises(ValueError):
        ray.data.read_parquet_bulk(data_path, filesystem=fs)

    # Expect directory path expansion to fail with OS error if default format-based path
    # filtering is turned off.
    with pytest.raises(OSError):
        ray.data.read_parquet_bulk(data_path, filesystem=fs, partition_filter=None)

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

    # Add a file with a non-matching file extension. This file should be ignored.
    txt_path = os.path.join(data_path, "foo.txt")
    txt_df = pd.DataFrame({"foobar": [4, 5, 6]})
    txt_table = pa.Table.from_pandas(txt_df)
    pq.write_table(txt_table, _unwrap_protocol(txt_path), filesystem=fs)

    ds = ray.data.read_parquet_bulk(paths + [txt_path], filesystem=fs)
    assert ds.num_blocks() == 2

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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
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


def test_parquet_reader_estimate_data_size(shutdown_only, tmp_path):
    ctx = ray.data.context.DatasetContext.get_current()
    old_decoding_size_estimation = ctx.decoding_size_estimation
    ctx.decoding_size_estimation = True
    try:
        tensor_output_path = os.path.join(tmp_path, "tensor")
        ray.data.range_tensor(1000, shape=(1000,)).write_parquet(tensor_output_path)
        ds = ray.data.read_parquet(tensor_output_path)
        assert ds.num_blocks() > 1
        data_size = ds.size_bytes()
        assert (
            data_size >= 6_000_000 and data_size <= 10_000_000
        ), "estimated data size is out of expected bound"
        data_size = ds.fully_executed().size_bytes()
        assert (
            data_size >= 7_000_000 and data_size <= 10_000_000
        ), "actual data size is out of expected bound"

        reader = _ParquetDatasourceReader(tensor_output_path)
        assert (
            reader._encoding_ratio >= 300 and reader._encoding_ratio <= 600
        ), "encoding ratio is out of expected bound"
        data_size = reader.estimate_inmemory_data_size()
        assert (
            data_size >= 6_000_000 and data_size <= 10_000_000
        ), "estimated data size is either out of expected bound"
        assert (
            data_size
            == _ParquetDatasourceReader(
                tensor_output_path
            ).estimate_inmemory_data_size()
        ), "estimated data size is not deterministic in multiple calls."

        text_output_path = os.path.join(tmp_path, "text")
        ray.data.range(1000).map(lambda _: "a" * 1000).write_parquet(text_output_path)
        ds = ray.data.read_parquet(text_output_path)
        assert ds.num_blocks() > 1
        data_size = ds.size_bytes()
        assert (
            data_size >= 1_000_000 and data_size <= 2_000_000
        ), "estimated data size is out of expected bound"
        data_size = ds.fully_executed().size_bytes()
        assert (
            data_size >= 1_000_000 and data_size <= 2_000_000
        ), "actual data size is out of expected bound"

        reader = _ParquetDatasourceReader(text_output_path)
        assert (
            reader._encoding_ratio >= 150 and reader._encoding_ratio <= 300
        ), "encoding ratio is out of expected bound"
        data_size = reader.estimate_inmemory_data_size()
        assert (
            data_size >= 1_000_000 and data_size <= 2_000_000
        ), "estimated data size is out of expected bound"
        assert (
            data_size
            == _ParquetDatasourceReader(text_output_path).estimate_inmemory_data_size()
        ), "estimated data size is not deterministic in multiple calls."
    finally:
        ctx.decoding_size_estimation = old_decoding_size_estimation


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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
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


def test_parquet_read_empty_file(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.parquet")
    table = pa.table({})
    pq.write_table(table, path)
    ds = ray.data.read_parquet(path)
    pd.testing.assert_frame_equal(ds.to_pandas(), table.to_pandas())


def test_parquet_reader_batch_size(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.parquet")
    ray.data.range_tensor(1000, shape=(1000,)).write_parquet(path)
    ds = ray.data.read_parquet(path, batch_size=10)
    assert ds.count() == 1000


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
