import os
import shutil
import time
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_lazyfixture import lazy_fixture

import ray
from ray.data._internal.datasource.parquet_bulk_datasource import ParquetBulkDatasource
from ray.data._internal.datasource.parquet_datasource import (
    NUM_CPUS_FOR_META_FETCH_TASK,
    ParquetDatasource,
    SerializedFragment,
    _deserialize_fragments_with_retry,
)
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource import DefaultFileMetadataProvider, ParquetMetadataProvider
from ray.data.datasource.parquet_meta_provider import PARALLELIZE_META_FETCH_THRESHOLD
from ray.data.datasource.partitioning import Partitioning, PathPartitionFilter
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.test_util import ConcurrencyCounter  # noqa
from ray.tests.conftest import *  # noqa


def test_write_parquet_supports_gzip(ray_start_regular_shared, tmp_path):
    ray.data.range(1).write_parquet(tmp_path, compression="gzip")

    # Test that all written files are gzip compressed.
    for filename in os.listdir(tmp_path):
        file_metadata = pq.ParquetFile(tmp_path / filename).metadata
        compression = file_metadata.row_group(0).column(0).compression
        assert compression == "GZIP", compression

    # Test that you can read the written files.
    assert pq.read_table(tmp_path).to_pydict() == {"id": [0]}


def test_include_paths(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test.txt")
    table = pa.Table.from_pydict({"animals": ["cat", "dog"]})
    pq.write_table(table, path)

    ds = ray.data.read_parquet(path, include_paths=True)

    paths = [row["path"] for row in ds.take_all()]
    assert paths == [path, path]


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_parquet_deserialize_fragments_with_retry(
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
    serialized_fragments = [SerializedFragment(p) for p in pq_ds.fragments]

    # test 1st attempt succeed
    fragments = _deserialize_fragments_with_retry(serialized_fragments)
    assert "test1.parquet" in fragments[0].path
    assert "test2.parquet" in fragments[1].path

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
            fragments,
        ]
    )
    monkeypatch.setattr(
        ray.data._internal.datasource.parquet_datasource,
        "_deserialize_fragments",
        mock_deserializer,
    )
    retried_fragments = _deserialize_fragments_with_retry(serialized_fragments)
    assert "test1.parquet" in retried_fragments[0].path
    assert "test2.parquet" in retried_fragments[1].path


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
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    # Schema information is available from Parquet metadata, so
    # we do not need to compute the first block.
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert str(ds) == "Dataset(num_rows=6, schema={one: int64, two: string})", ds
    assert repr(ds) == "Dataset(num_rows=6, schema={one: int64, two: string})", ds

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take_all()]
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

    # Test concurrency.
    ds = ray.data.read_parquet(data_path, filesystem=fs, concurrency=1)
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]


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

    class TestMetadataProvider(ParquetMetadataProvider):
        def prefetch_file_metadata(self, fragments, **ray_remote_args):
            assert ray_remote_args["num_cpus"] == NUM_CPUS_FOR_META_FETCH_TASK
            assert (
                ray_remote_args["scheduling_strategy"]
                == DataContext.get_current().scheduling_strategy
            )
            return None

    ds = ray.data.read_parquet(
        data_path,
        filesystem=fs,
        meta_provider=TestMetadataProvider(),
    )

    # Expect precomputed row counts and block sizes to be missing.
    assert ds._meta_count() is None

    # Expect to lazily compute all metadata correctly.
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert str(ds) == "Dataset(num_rows=6, schema={one: int64, two: string})", ds
    assert repr(ds) == "Dataset(num_rows=6, schema={one: int64, two: string})", ds

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
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
def test_parquet_read_random_shuffle(
    ray_start_regular_shared, restore_data_context, fs, data_path
):
    # NOTE: set preserve_order to True to allow consistent output behavior.
    context = ray.data.DataContext.get_current()
    context.execution_options.preserve_order = True

    input_list = list(range(10))
    df1 = pd.DataFrame({"one": input_list[: len(input_list) // 2]})
    table = pa.Table.from_pandas(df1)
    setup_data_path = _unwrap_protocol(data_path)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": input_list[len(input_list) // 2 :]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    ds = ray.data.read_parquet(data_path, filesystem=fs, shuffle="files")

    # Execute 10 times to get a set of output results.
    output_results_list = []
    for _ in range(10):
        result = [row["one"] for row in ds.take_all()]
        output_results_list.append(result)
    all_rows_matched = [
        input_list == output_list for output_list in output_results_list
    ]

    # Check when shuffle is enabled, output order has at least one different
    # case.
    assert not all(all_rows_matched)


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
        ds = ray.data.read_parquet_bulk(data_path, filesystem=fs, file_extensions=None)
        ds.schema()

    paths = [path1, path2]
    ds = ray.data.read_parquet_bulk(paths, filesystem=fs)

    # Expect to lazily compute all metadata correctly.
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert not ds._plan.has_started_execution

    # Schema isn't available, so we do a partial read.
    assert ds.schema() is not None
    assert str(ds) == "Dataset(num_rows=?, schema={one: int64, two: string})", ds
    assert repr(ds) == "Dataset(num_rows=?, schema={one: int64, two: string})", ds
    assert ds._plan.has_started_execution
    assert not ds._plan.has_computed_output()

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
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
    assert ds._plan.initial_num_blocks() == 2
    assert not ds._plan.has_started_execution

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
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

    # Expect to lazily compute all metadata correctly.
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert not ds._plan.has_started_execution

    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    assert str(ds) == "Dataset(num_rows=6, schema={one: int64, two: string})", ds
    assert repr(ds) == "Dataset(num_rows=6, schema={one: int64, two: string})", ds
    assert ds._plan.has_started_execution

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
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
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    # Schema information and input files are available from Parquet metadata,
    # so we do not need to compute the first block.
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert str(ds) == "Dataset(num_rows=6, schema={two: string, one: string})", ds
    assert repr(ds) == "Dataset(num_rows=6, schema={two: string, one: string})", ds

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert sorted(values) == [
        ["1", "a"],
        ["1", "b"],
        ["1", "c"],
        ["3", "e"],
        ["3", "f"],
        ["3", "g"],
    ]

    # Test column selection.
    ds = ray.data.read_parquet(data_path, columns=["one"], filesystem=fs)
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == ["1", "1", "1", "3", "3", "3"]


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
        str(tmp_path), override_num_blocks=1, filter=(pa.dataset.field("two") == "a")
    )

    values = [[s["one"], s["two"]] for s in ds.take()]
    assert sorted(values) == [["1", "a"], ["1", "a"]]
    assert ds.count() == 2

    # 2 partitions, 1 empty partition, 2 block/read tasks, 1 empty block

    ds = ray.data.read_parquet(
        str(tmp_path), override_num_blocks=2, filter=(pa.dataset.field("two") == "a")
    )

    values = [[s["one"], s["two"]] for s in ds.take()]
    assert sorted(values) == [["1", "a"], ["1", "a"]]
    assert ds.count() == 2


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
def test_parquet_read_partitioned_with_columns(ray_start_regular_shared, fs, data_path):
    data = {
        "x": [0, 0, 1, 1, 2, 2],
        "y": ["a", "b", "a", "b", "a", "b"],
        "z": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
    }
    table = pa.Table.from_pydict(data)

    pq.write_to_dataset(
        table,
        root_path=_unwrap_protocol(data_path),
        filesystem=fs,
        use_legacy_dataset=False,
        partition_cols=["x", "y"],
    )

    ds = ray.data.read_parquet(
        _unwrap_protocol(data_path),
        columns=["y", "z"],
        filesystem=fs,
    )
    assert set(ds.columns()) == {"y", "z"}
    values = [[s["y"], s["z"]] for s in ds.take()]
    assert sorted(values) == [
        ["a", 0.1],
        ["a", 0.3],
        ["a", 0.5],
        ["b", 0.2],
        ["b", 0.4],
        ["b", 0.6],
    ]


# Skip this test if pyarrow is below version 7. As the old
# pyarrow does not support single path with partitioning,
# this issue cannot be resolved by Ray data itself.
@pytest.mark.skipif(
    tuple(pa.__version__.split(".")) < ("7",),
    reason="Old pyarrow behavior cannot be fixed.",
)
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
def test_parquet_read_partitioned_with_partition_filter(
    ray_start_regular_shared, fs, data_path
):
    # This test is to make sure when only one file remains
    # after partition filtering, Ray data can still parse the
    # partitions correctly.
    data = {
        "x": [0, 0, 1, 1, 2, 2],
        "y": ["a", "b", "a", "b", "a", "b"],
        "z": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
    }
    table = pa.Table.from_pydict(data)

    pq.write_to_dataset(
        table,
        root_path=_unwrap_protocol(data_path),
        filesystem=fs,
        use_legacy_dataset=False,
        partition_cols=["x", "y"],
    )

    ds = ray.data.read_parquet(
        _unwrap_protocol(data_path),
        filesystem=fs,
        columns=["x", "y", "z"],
        partition_filter=ray.data.datasource.partitioning.PathPartitionFilter.of(
            filter_fn=lambda x: (x["x"] == "0") and (x["y"] == "a"), style="hive"
        ),
    )

    assert ds.columns() == ["x", "y", "z"]
    values = [[s["x"], s["y"], s["z"]] for s in ds.take()]
    assert sorted(values) == [[0, "a", 0.1]]


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

    partitioning = Partitioning("hive", field_types={"one": int})
    ds = ray.data.read_parquet(str(tmp_path), partitioning=partitioning)

    # Test metadata-only parquet ops.
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    # Schema information and input files are available from Parquet metadata,
    # so we do not need to compute the first block.
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert str(ds) == "Dataset(num_rows=6, schema={two: string, one: int64})", ds
    assert repr(ds) == "Dataset(num_rows=6, schema={two: string, one: int64})", ds

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
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

    ds = ray.data.read_parquet(
        str(tmp_path), override_num_blocks=1, _block_udf=_block_udf
    )

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    np.testing.assert_array_equal(sorted(ones), np.array(one_data) + 1)

    # 2 blocks/read tasks

    ds = ray.data.read_parquet(
        str(tmp_path), override_num_blocks=2, _block_udf=_block_udf
    )

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
    np.testing.assert_array_equal(sorted(ones), np.array(one_data) + 1)

    # 2 blocks/read tasks, 1 empty block

    ds = ray.data.read_parquet(
        str(tmp_path),
        override_num_blocks=2,
        partition_filter=PathPartitionFilter.of(
            lambda partitions: partitions["two"] == "a"
        ),
        _block_udf=_block_udf,
    )

    ones, twos = zip(*[[s["one"], s["two"]] for s in ds.take()])
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
    ds = ray.data.read_parquet(
        data_path, filesystem=fs, override_num_blocks=parallelism
    )

    # Test metadata-only parquet ops.
    assert ds.count() == num_dfs * 3
    assert ds.size_bytes() > 0
    # Schema information and input files are available from Parquet metadata,
    # so we do not need to compute the first block.
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == num_dfs, input_files

    # Forces a data read.
    values = [s["one"] for s in ds.take(limit=3 * num_dfs)]
    assert sorted(values) == list(range(3 * num_dfs))


def test_parquet_reader_estimate_data_size(shutdown_only, tmp_path):
    ctx = ray.data.context.DataContext.get_current()
    old_decoding_size_estimation = ctx.decoding_size_estimation
    ctx.decoding_size_estimation = True
    try:
        tensor_output_path = os.path.join(tmp_path, "tensor")
        ray.data.range_tensor(1000, shape=(1000,)).write_parquet(tensor_output_path)
        ds = ray.data.read_parquet(
            tensor_output_path, meta_provider=ParquetMetadataProvider()
        )
        assert ds._plan.initial_num_blocks() > 1
        data_size = ds.size_bytes()
        assert (
            data_size >= 6_000_000 and data_size <= 10_000_000
        ), "estimated data size is out of expected bound"
        data_size = ds.materialize().size_bytes()
        assert (
            data_size >= 7_000_000 and data_size <= 10_000_000
        ), "actual data size is out of expected bound"

        datasource = ParquetDatasource(
            tensor_output_path, meta_provider=ParquetMetadataProvider()
        )
        assert (
            datasource._encoding_ratio >= 300 and datasource._encoding_ratio <= 600
        ), "encoding ratio is out of expected bound"
        data_size = datasource.estimate_inmemory_data_size()
        assert (
            data_size >= 6_000_000 and data_size <= 10_000_000
        ), "estimated data size is either out of expected bound"
        assert (
            data_size
            == ParquetDatasource(
                tensor_output_path, meta_provider=ParquetMetadataProvider()
            ).estimate_inmemory_data_size()
        ), "estimated data size is not deterministic in multiple calls."

        text_output_path = os.path.join(tmp_path, "text")
        ray.data.range(1000).map(lambda _: {"text": "a" * 1000}).write_parquet(
            text_output_path
        )
        ds = ray.data.read_parquet(
            text_output_path, meta_provider=ParquetMetadataProvider()
        )
        assert ds._plan.initial_num_blocks() > 1
        data_size = ds.size_bytes()
        assert (
            data_size >= 1_000_000 and data_size <= 2_000_000
        ), "estimated data size is out of expected bound"
        data_size = ds.materialize().size_bytes()
        assert (
            data_size >= 1_000_000 and data_size <= 2_000_000
        ), "actual data size is out of expected bound"

        datasource = ParquetDatasource(
            text_output_path, meta_provider=ParquetMetadataProvider()
        )
        assert (
            datasource._encoding_ratio >= 150 and datasource._encoding_ratio <= 300
        ), "encoding ratio is out of expected bound"
        data_size = datasource.estimate_inmemory_data_size()
        assert (
            data_size >= 1_000_000 and data_size <= 2_000_000
        ), "estimated data size is out of expected bound"
        assert (
            data_size
            == ParquetDatasource(
                text_output_path, meta_provider=ParquetMetadataProvider()
            ).estimate_inmemory_data_size()
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
    ds = ray.data.from_blocks([df1, df2])
    path = os.path.join(data_path, "test_parquet_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    ds._set_uuid("data")
    ds.write_parquet(path, filesystem=fs)
    path1 = os.path.join(path, "data_000000_000000.parquet")
    path2 = os.path.join(path, "data_000001_000000.parquet")
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


def test_parquet_file_extensions(ray_start_regular_shared, tmp_path):
    table = pa.table({"food": ["spam", "ham", "eggs"]})
    pq.write_table(table, tmp_path / "table.parquet")
    # `spam` should be filtered out.
    with open(tmp_path / "spam", "w"):
        pass

    ds = ray.data.read_parquet(tmp_path, file_extensions=["parquet"])

    assert ds.count() == 3


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
    ds = ray.data.from_blocks([df1, df2])
    path = os.path.join(data_path, "test_parquet_dir")
    # Set the uuid to a known value so that we can easily get the parquet file names.
    data_key = "data"
    ds._set_uuid(data_key)
    ds.write_parquet(path, filesystem=fs)

    # Ensure that directory was created.
    if fs is None:
        assert os.path.isdir(path)
    else:
        assert fs.get_file_info(_unwrap_protocol(path)).type == pa.fs.FileType.Directory

    # Check that data was properly written to the directory.
    path1 = os.path.join(path, f"{data_key}_000000_000000.parquet")
    path2 = os.path.join(path, f"{data_key}_000001_000000.parquet")
    dfds = pd.concat(
        [
            pd.read_parquet(path1, storage_options=storage_options),
            pd.read_parquet(path2, storage_options=storage_options),
        ]
    )
    assert df.equals(dfds)

    # Ensure that directories that already exist are left alone and that the
    # attempted creation still succeeds.
    path3 = os.path.join(path, f"{data_key}_0000002_000000.parquet")
    path4 = os.path.join(path, f"{data_key}_0000003_000000.parquet")
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

    # Test that writing empty blocks does not create empty parquet files,
    # nor does it create empty directories when no files are created.
    ds_all_empty = ds.filter(lambda x: x["one"] > 10).materialize()
    assert ds_all_empty._plan.initial_num_blocks() == 2
    assert ds_all_empty.count() == 0

    all_empty_key = "all_empty"
    all_empty_path = os.path.join(data_path, f"test_parquet_dir_{all_empty_key}")
    ds_all_empty.write_parquet(all_empty_path, filesystem=fs)

    ds_contains_some_empty = ds.union(ds_all_empty)
    # 2 blocks from original ds with 6 rows total, 2 empty blocks from ds_all_empty.
    assert ds_contains_some_empty._plan.initial_num_blocks() == 4
    assert ds_contains_some_empty.count() == 6

    some_empty_key = "some_empty"
    # Set the uuid to a known value so that we can easily get the parquet file names.
    ds_contains_some_empty._set_uuid(some_empty_key)
    some_empty_path = os.path.join(path, f"test_parquet_dir_{some_empty_key}")
    ds_contains_some_empty.write_parquet(some_empty_path, filesystem=fs)

    # Ensure that directory was created for only the non-empty dataset.
    if fs is None:
        assert not os.path.isdir(all_empty_path)
        assert os.path.isdir(some_empty_path)
        # Only files for the non-empty blocks should be created.
        file_list = os.listdir(some_empty_path)
        file_list.sort()
        assert file_list == [
            f"{some_empty_key}_00000{i}_000000.parquet" for i in range(2)
        ]
    else:
        assert (
            fs.get_file_info(_unwrap_protocol(all_empty_path)).type
            == pa.fs.FileType.NotFound
        )
        assert (
            fs.get_file_info(_unwrap_protocol(some_empty_path)).type
            == pa.fs.FileType.Directory
        )

    # Check that data was properly written to the directory.
    dfds = pd.concat(
        [
            pd.read_parquet(
                os.path.join(
                    some_empty_path,
                    f"{some_empty_key}_00000{i}_000000.parquet",
                ),
                storage_options=storage_options,
            )
            for i in range(2)
        ]
    )
    assert df.equals(dfds)


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
    path = os.path.join(data_path, "test_parquet_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds.write_parquet(path, filesystem=fs)

    ds2 = ray.data.read_parquet(path, filesystem=fs)

    read_data = set(ds2.to_pandas().itertuples(index=False))
    written_data = set(pd.concat([df1, df2]).itertuples(index=False))
    assert read_data == written_data

    # Test metadata ops.
    for block, meta in ds2._plan.execute().blocks:
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


def test_parquet_datasource_names(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"spam": [1, 2, 3]})
    path = os.path.join(tmp_path, "data.parquet")
    df.to_parquet(path)

    assert ParquetBulkDatasource(path).get_name() == "ParquetBulk"
    assert ParquetDatasource(path).get_name() == "Parquet"


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_parquet_concurrency(ray_start_regular_shared, fs, data_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    setup_data_path = _unwrap_protocol(data_path)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    concurrency_counter = ConcurrencyCounter.remote()

    def map_batches(batch):
        ray.get(concurrency_counter.inc.remote())
        time.sleep(0.5)
        ray.get(concurrency_counter.decr.remote())
        return batch

    concurrency = 1
    ds = ray.data.read_parquet(
        data_path,
        filesystem=fs,
        concurrency=concurrency,
        override_num_blocks=2,
    )
    ds = ds.map_batches(
        map_batches,
        batch_size=None,
        concurrency=concurrency,
    )
    assert ds.count() == 6
    actual_max_concurrency = ray.get(concurrency_counter.get_max_concurrency.remote())
    assert actual_max_concurrency <= concurrency


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


def test_parquet_read_spread(ray_start_cluster, tmp_path, restore_data_context):
    ray.shutdown()
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        object_store_memory=2 * 1024 * 1024 * 1024,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(
        resources={"bar:2": 100},
        num_cpus=10,
        object_store_memory=2 * 1024 * 1024 * 1024,
    )
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

    # Minimize the block size to prevent Ray Data from reading multiple fragments in a
    # single task.
    ray.data.DataContext.get_current().target_max_block_size = 1
    ds = ray.data.read_parquet(data_path)

    # Force reads.
    bundles = ds.iter_internal_ref_bundles()
    block_refs = _ref_bundles_iterator_to_block_refs_list(bundles)
    ray.wait(block_refs, num_returns=len(block_refs), fetch_local=False)
    location_data = ray.experimental.get_object_locations(block_refs)
    locations = []
    for block in block_refs:
        locations.extend(location_data[block]["node_ids"])
    assert set(locations) == {node1_id, node2_id}, set(locations)


def test_parquet_bulk_columns(ray_start_regular_shared):
    ds = ray.data.read_parquet_bulk("example://iris.parquet", columns=["variety"])

    assert ds.columns() == ["variety"]


@pytest.mark.parametrize("num_rows_per_file", [5, 10, 50])
def test_write_num_rows_per_file(tmp_path, ray_start_regular_shared, num_rows_per_file):
    import pyarrow.parquet as pq

    ray.data.range(100, override_num_blocks=20).write_parquet(
        tmp_path, num_rows_per_file=num_rows_per_file
    )

    for filename in os.listdir(tmp_path):
        table = pq.read_table(os.path.join(tmp_path, filename))
        assert len(table) == num_rows_per_file


@pytest.mark.parametrize("shuffle", [True, False, "file"])
def test_invalid_shuffle_arg_raises_error(ray_start_regular_shared, shuffle):

    with pytest.raises(ValueError):
        ray.data.read_parquet("example://iris.parquet", shuffle=shuffle)


@pytest.mark.parametrize("shuffle", [None, "files"])
def test_valid_shuffle_arg_does_not_raise_error(ray_start_regular_shared, shuffle):
    ray.data.read_parquet("example://iris.parquet", shuffle=shuffle)


def test_partitioning_in_dataset_kwargs_raises_error(ray_start_regular_shared):
    with pytest.raises(ValueError):
        ray.data.read_parquet(
            "example://iris.parquet", dataset_kwargs=dict(partitioning="hive")
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
