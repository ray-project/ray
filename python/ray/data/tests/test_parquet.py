import os
import shutil
import time
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.air.util.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
    get_arrow_extension_fixed_shape_tensor_types,
)
from ray.data import FileShuffleConfig, Schema
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
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor, BlockMetadata
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


def test_write_parquet_partition_cols(ray_start_regular_shared, tmp_path):
    num_partitions = 10
    rows_per_partition = 10
    num_rows = num_partitions * rows_per_partition

    df = pd.DataFrame(
        {
            "a": list(range(num_partitions)) * rows_per_partition,
            "b": list(range(num_partitions)) * rows_per_partition,
            "c": list(range(num_rows)),
            "d": list(range(num_rows)),
            # Make sure algorithm does not fail for tensor types.
            "e": list(np.random.random((num_rows, 128))),
        }
    )

    ds = ray.data.from_pandas(df)
    ds.write_parquet(tmp_path, partition_cols=["a", "b"])

    # Test that files are written in partition style
    for i in range(num_partitions):
        partition = os.path.join(tmp_path, f"a={i}", f"b={i}")
        ds_partition = ray.data.read_parquet(partition)
        dsf_partition = ds_partition.to_pandas()
        c_expected = [k * i for k in range(rows_per_partition)].sort()
        d_expected = [k * i for k in range(rows_per_partition)].sort()
        assert c_expected == dsf_partition["c"].tolist().sort()
        assert d_expected == dsf_partition["d"].tolist().sort()
        assert dsf_partition["e"].shape == (rows_per_partition,)

    # Test that partition are read back properly into original dataset schema
    ds1 = ray.data.read_parquet(tmp_path)
    assert set(ds.schema().names) == set(ds1.schema().names)
    assert ds.count() == ds1.count()

    df = df.sort_values(by=["a", "b", "c", "d"])
    df1 = ds1.to_pandas().sort_values(by=["a", "b", "c", "d"])
    for (index1, row1), (index2, row2) in zip(df.iterrows(), df1.iterrows()):
        row1_dict = row1.to_dict()
        row2_dict = row2.to_dict()
        assert row1_dict["c"] == row2_dict["c"]
        assert row1_dict["d"] == row2_dict["d"]


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
        data_path,
        **dataset_kwargs,
        filesystem=fs,
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
            if isinstance(exp_or_ret, Exception):
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
    assert ds.schema() == Schema(pa.schema({"one": pa.int64(), "two": pa.string()}))
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)

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
    df1 = pd.DataFrame({"one": range(30_000), "two": ["a", "b", "c"] * 10_000})
    table = pa.Table.from_pandas(df1)
    setup_data_path = _unwrap_protocol(data_path)
    path1 = os.path.join(setup_data_path, "test1.parquet")
    pq.write_table(table, path1, filesystem=fs)
    df2 = pd.DataFrame({"one": range(30_000, 60_000), "two": ["e", "f", "g"] * 10000})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(setup_data_path, "test2.parquet")
    pq.write_table(table, path2, filesystem=fs)

    expected_num_rows = len(df1) + len(df2)
    expected_byte_size = 787500

    #
    # Case 1: Test metadata fetching happy path (obtaining, caching and propagating
    #         metadata)
    #

    class AssertingMetadataProvider(ParquetMetadataProvider):
        def prefetch_file_metadata(self, fragments, **ray_remote_args):
            assert ray_remote_args["num_cpus"] == NUM_CPUS_FOR_META_FETCH_TASK
            assert (
                ray_remote_args["scheduling_strategy"]
                == DataContext.get_current().scheduling_strategy
            )
            return super().prefetch_file_metadata(fragments, **ray_remote_args)

    ds = ray.data.read_parquet(
        data_path,
        filesystem=fs,
        meta_provider=AssertingMetadataProvider(),
    )

    # Expect precomputed row counts and block sizes to be missing.
    assert ds._meta_count() == expected_num_rows

    read_op = ds._plan._logical_plan.dag

    # Assert Read op metadata propagation
    assert read_op.infer_metadata() == BlockMetadata(
        num_rows=expected_num_rows,
        size_bytes=expected_byte_size,
        exec_stats=None,
        input_files=[path1, path2],
    )

    expected_schema = pa.schema({"one": pa.int64(), "two": pa.string()})

    assert read_op.infer_schema().equals(expected_schema)

    # Expected
    #   - Fetched Parquet metadata to be reused
    #   - *No* dataset execution performed
    assert ds.count() == expected_num_rows
    assert ds.size_bytes() == expected_byte_size
    assert ds.schema() == Schema(expected_schema)
    assert set(ds.input_files()) == {path1, path2}

    assert not ds._plan.has_computed_output()

    expected_values = list(
        zip(range(60_000), ["a", "b", "c"] * 10_000 + ["e", "f", "g"] * 10_000)
    )

    values = [(s["one"], s["two"]) for s in ds.take(60000)]

    exec_stats = ds._plan._snapshot_stats
    read_stats = exec_stats.parents[0]

    # Assert that ref-bundles
    #   - Passed to ReadParquet hold metadata matching actual bundle
    #   - Produced by ReadParquet reflects actual amount of bytes read
    assert read_stats.base_name == "ReadParquet"
    # NOTE: Size of the task should be ~5kb, but could vary from platform to platform
    #       alas for different Python versions. However, it is substantially smaller
    #       than the dataset itself (~750kb)
    assert read_stats.extra_metrics["average_bytes_inputs_per_task"] < 10_000

    # TODO stats are broken for iteration-based executions due to the fact
    #      that returned stats object is obtained before iteration completes,
    #      hence not capturing the final state of the pipeline
    # assert (
    #     read_stats.extra_metrics["bytes_task_outputs_generated"] == expected_byte_size
    # )

    assert sorted(values) == expected_values

    #
    # Case 2: Test metadata fetching *failing* (falling back to actually
    #         executing the dataset)
    #

    class FailingMetadataProvider(ParquetMetadataProvider):
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
        meta_provider=FailingMetadataProvider(),
    )

    # Expected
    #   - Fetched Parquet metadata is not used (returns null), hence
    #   - Dataset execution has to be performed
    assert ds.count() == expected_num_rows
    assert ds.size_bytes() == expected_byte_size
    assert ds.schema() == Schema(expected_schema)
    assert set(ds.input_files()) == {path1, path2}

    assert ds._plan.has_computed_output()


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
    assert ds.schema() == Schema(pa.schema({"one": pa.int64(), "two": pa.string()}))

    # Schema isn't available, so we do a partial read.
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
    assert ds.schema() == Schema(pa.schema({"one": pa.int64(), "two": pa.string()}))
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
    )

    ds = ray.data.read_parquet(data_path, filesystem=fs)

    # Test metadata-only parquet ops.
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    # Schema information and input files are available from Parquet metadata,
    # so we do not need to compute the first block.
    assert ds.schema() == Schema(pa.schema({"two": pa.string(), "one": pa.string()}))
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files

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
        table,
        root_path=str(tmp_path),
        partition_cols=["one"],
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

    # Where we insert partition columns is an implementation detail, so we don't check
    # the order of the columns.
    assert sorted(zip(ds.schema().names, ds.schema().types)) == [
        ("x", pa.string()),
        ("y", pa.string()),
        ("z", pa.float64()),
    ]

    values = [[s["x"], s["y"], s["z"]] for s in ds.take()]

    assert sorted(values) == [["0", "a", 0.1]]


def test_parquet_read_partitioned_explicit(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame(
        {"one": [1, 1, 1, 3, 3, 3], "two": ["a", "b", "c", "e", "f", "g"]}
    )
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=str(tmp_path),
        partition_cols=["one"],
    )

    partitioning = Partitioning("hive", field_types={"one": int})
    ds = ray.data.read_parquet(str(tmp_path), partitioning=partitioning)

    # Test metadata-only parquet ops.
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    # Schema information and input files are available from Parquet metadata,
    # so we do not need to compute the first block.
    assert ds.schema() == Schema(pa.schema({"two": pa.string(), "one": pa.int64()}))
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files

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
        table,
        root_path=str(tmp_path),
        partition_cols=["two"],
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


def test_parquet_write(ray_start_regular_shared, tmp_path):
    input_df = pd.DataFrame({"id": [0]})
    ds = ray.data.from_blocks([input_df])

    ds.write_parquet(tmp_path)

    output_df = pd.concat(
        [
            pd.read_parquet(os.path.join(tmp_path, filename))
            for filename in os.listdir(tmp_path)
        ]
    )
    assert rows_same(input_df, output_df)


def test_parquet_write_ignore_save_mode(ray_start_regular_shared, local_path):
    data_path = local_path
    path = os.path.join(data_path, "test_parquet_dir")
    os.mkdir(path)
    in_memory_table = pa.Table.from_pydict({"one": [1]})
    ds = ray.data.from_arrow(in_memory_table)
    ds.write_parquet(path, filesystem=None, mode="ignore")

    # directory was created, should ignore
    with os.scandir(path) as file_paths:
        count_of_files = sum(1 for path in file_paths)
        assert count_of_files == 0

    # now remove dir
    shutil.rmtree(path)

    # should write
    ds.write_parquet(path, filesystem=None, mode="ignore")
    on_disk_table = pq.read_table(path)

    assert in_memory_table.equals(on_disk_table)


def test_parquet_write_error_save_mode(ray_start_regular_shared, local_path):
    data_path = local_path
    path = os.path.join(data_path, "test_parquet_dir")
    os.mkdir(path)
    in_memory_table = pa.Table.from_pydict({"one": [1]})
    ds = ray.data.from_arrow(in_memory_table)

    with pytest.raises(ValueError):
        ds.write_parquet(path, filesystem=None, mode="error")

    # now remove dir
    shutil.rmtree(path)

    # should write
    ds.write_parquet(path, filesystem=None, mode="error")
    on_disk_table = pq.read_table(path)

    assert in_memory_table.equals(on_disk_table)


def test_parquet_write_append_save_mode(ray_start_regular_shared, local_path):
    data_path = local_path
    path = os.path.join(data_path, "test_parquet_dir")
    in_memory_table = pa.Table.from_pydict({"one": [1]})
    ds = ray.data.from_arrow(in_memory_table)
    ds.write_parquet(path, filesystem=None, mode="append")

    # one file should be added
    with os.scandir(path) as file_paths:
        count_of_files = sum(1 for path in file_paths)
        assert count_of_files == 1

    appended_in_memory_table = pa.Table.from_pydict({"two": [2]})
    ds = ray.data.from_arrow(appended_in_memory_table)
    ds.write_parquet(path, filesystem=None, mode="append")

    # another file should be added
    with os.scandir(path) as file_paths:
        count_of_files = sum(1 for path in file_paths)
        assert count_of_files == 2


@pytest.mark.parametrize(
    "filename_template,should_raise_error",
    [
        # Case 1: No UUID, no extension - should raise error in append mode
        ("myfile", True),
        # Case 2: No UUID, has extension - should raise error in append mode
        ("myfile.parquet", True),
        # Case 3: No UUID, different extension - should raise error in append mode
        ("myfile.txt", True),
        # Case 4: Already has UUID - should not raise error
        ("myfile_{write_uuid}", False),
        # Case 5: Already has UUID with extension - should not raise error
        ("myfile_{write_uuid}.parquet", False),
        # Case 6: Templated filename without UUID - should raise error in append mode
        ("myfile-{i}", True),
        # Case 7: Templated filename with extension but no UUID - should raise error in append mode
        ("myfile-{i}.parquet", True),
        # Case 8: Templated filename with UUID already present - should not raise error
        ("myfile_{write_uuid}-{i}.parquet", False),
    ],
    ids=[
        "no_uuid_no_ext",
        "no_uuid_with_parquet_ext",
        "no_uuid_with_other_ext",
        "has_uuid_no_ext",
        "has_uuid_with_ext",
        "templated_no_uuid_no_ext",
        "templated_no_uuid_with_ext",
        "templated_has_uuid",
    ],
)
def test_parquet_write_uuid_handling_with_custom_filename_provider(
    ray_start_regular_shared, tmp_path, filename_template, should_raise_error
):
    """Test that write_parquet correctly handles UUID validation in filenames when using custom filename providers in append mode."""
    import re

    from ray.data.datasource.filename_provider import FilenameProvider

    class CustomFilenameProvider(FilenameProvider):
        def __init__(self, filename_template, should_include_uuid):
            self.filename_template = filename_template
            self.should_include_uuid = should_include_uuid

        def get_filename_for_block(self, block, write_uuid, task_index, block_index):
            if self.should_include_uuid:
                # Replace {write_uuid} placeholder with actual write_uuid
                return self.filename_template.format(write_uuid=write_uuid, i="{i}")
            else:
                # Don't include UUID - this simulates the problematic case
                return self.filename_template

    # Create a simple dataset
    ds = ray.data.range(10).repartition(1)

    # Create custom filename provider
    custom_provider = CustomFilenameProvider(filename_template, not should_raise_error)

    if should_raise_error:
        # Should raise ValueError when UUID is missing in append mode
        # Updated regex to match the actual error message
        with pytest.raises(
            ValueError,
            match=r"Write UUID.*missing from filename template.*This could result in files being overwritten.*Modify your FileNameProvider implementation",
        ):
            ds.write_parquet(tmp_path, filename_provider=custom_provider, mode="append")
    else:
        # Should succeed when UUID is present
        ds.write_parquet(tmp_path, filename_provider=custom_provider, mode="append")

        # Check that files were created
        written_files = os.listdir(tmp_path)
        assert len(written_files) == 1

        written_file = written_files[0]

        # Verify UUID is present in filename (should be the actual write_uuid)
        uuid_pattern = r"[a-f0-9]{32}"  # 32 hex characters (UUID without dashes)
        assert re.search(
            uuid_pattern, written_file
        ), f"File '{written_file}' should contain UUID"

        # Verify the content is correct by reading back
        ds_read = ray.data.read_parquet(tmp_path)
        assert ds_read.count() == 10
        assert sorted([row["id"] for row in ds_read.take_all()]) == list(range(10))


def test_parquet_write_overwrite_save_mode(ray_start_regular_shared, local_path):
    data_path = local_path
    path = os.path.join(data_path, "test_parquet_dir")
    in_memory_table = pa.Table.from_pydict({"one": [1]})
    ds = ray.data.from_arrow(in_memory_table)
    ds.write_parquet(path, filesystem=None, mode="overwrite")

    # one file should be added
    with os.scandir(path) as file_paths:
        count_of_files = sum(1 for path in file_paths)
        assert count_of_files == 1

    overwritten_in_memory_table = pa.Table.from_pydict({"two": [2]})
    ds = ray.data.from_arrow(overwritten_in_memory_table)
    ds.write_parquet(path, filesystem=None, mode="overwrite")

    # another file should NOT be added
    with os.scandir(path) as file_paths:
        count_of_files = sum(1 for path in file_paths)
        assert count_of_files == 1

    on_disk_table = pq.read_table(path)
    assert on_disk_table.equals(overwritten_in_memory_table)


def test_parquet_file_extensions(ray_start_regular_shared, tmp_path):
    table = pa.table({"food": ["spam", "ham", "eggs"]})
    pq.write_table(table, tmp_path / "table.parquet")
    # `spam` should be filtered out.
    with open(tmp_path / "spam", "w"):
        pass

    ds = ray.data.read_parquet(tmp_path, file_extensions=["parquet"])

    assert ds.count() == 3


def test_parquet_write_creates_dir_if_not_exists(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(1)
    path = os.path.join(tmp_path, "does_not_exist")

    ds.write_parquet(path)

    assert os.path.isdir(path)
    expected_df = pd.DataFrame({"id": [0]})
    actual_df = pd.concat(
        [pd.read_parquet(os.path.join(path, filename)) for filename in os.listdir(path)]
    )
    assert rows_same(actual_df, expected_df)


def test_parquet_write_does_not_create_dir_for_empty_dataset(
    ray_start_regular_shared, tmp_path
):
    ds = ray.data.from_blocks([pd.DataFrame({})])
    path = os.path.join(tmp_path, "does_not_exist")

    ds.write_parquet(path)

    assert not os.path.isdir(path)


def test_parquet_write_does_not_write_empty_blocks(ray_start_regular_shared, tmp_path):
    ds = ray.data.from_blocks([pd.DataFrame({}), pd.DataFrame({"id": [0]})])
    path = os.path.join(tmp_path, "does_not_exist")

    ds.write_parquet(path)

    assert len(os.listdir(path)) == 1
    expected_df = pd.DataFrame({"id": [0]})
    actual_df = pd.read_parquet(os.path.join(path, os.listdir(path)[0]))
    assert rows_same(actual_df, expected_df)


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

    assert ds.take_all() == []


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


def test_tensors_in_tables_parquet(
    ray_start_regular_shared, tmp_path, restore_data_context
):
    """This test verifies both V1 and V2 Tensor Type extensions of
    Arrow Array types
    """

    num_rows = 10_000
    num_groups = 10

    inner_shape = (2, 2, 2)
    shape = (num_rows,) + inner_shape
    num_tensor_elem = np.prod(np.array(shape))

    arr = np.arange(num_tensor_elem).reshape(shape)

    id_col_name = "_id"
    group_col_name = "group"
    tensor_col_name = "tensor"

    id_vals = list(range(num_rows))
    group_vals = [i % num_groups for i in id_vals]

    df = pd.DataFrame(
        {
            id_col_name: id_vals,
            group_col_name: group_vals,
            tensor_col_name: [a.tobytes() for a in arr],
        }
    )

    #
    # Test #1: Verify writing tensors as ArrowTensorType (v1)
    #

    tensor_v1_path = f"{tmp_path}/tensor_v1"

    ds = ray.data.from_pandas([df])
    ds.write_parquet(tensor_v1_path)

    ds = ray.data.read_parquet(
        tensor_v1_path,
        tensor_column_schema={tensor_col_name: (arr.dtype, inner_shape)},
        override_num_blocks=10,
    )

    assert isinstance(
        ds.schema().base_schema.field_by_name(tensor_col_name).type,
        get_arrow_extension_fixed_shape_tensor_types(),
    )

    expected_tuples = list(zip(id_vals, group_vals, arr))

    def _assert_equal(rows, expected):
        values = [[s[id_col_name], s[group_col_name], s[tensor_col_name]] for s in rows]

        assert len(values) == len(expected)

        for v, e in zip(sorted(values, key=lambda v: v[0]), expected):
            np.testing.assert_equal(v, e)

    _assert_equal(ds.take_all(), expected_tuples)

    #
    # Test #2: Verify writing tensors as ArrowTensorTypeV2
    #

    DataContext.get_current().use_arrow_tensor_v2 = True

    tensor_v2_path = f"{tmp_path}/tensor_v2"

    ds = ray.data.from_pandas([df])
    ds.write_parquet(tensor_v2_path)

    ds = ray.data.read_parquet(
        tensor_v2_path,
        tensor_column_schema={tensor_col_name: (arr.dtype, inner_shape)},
        override_num_blocks=10,
    )

    assert isinstance(
        ds.schema().base_schema.field_by_name(tensor_col_name).type, ArrowTensorTypeV2
    )

    _assert_equal(ds.take_all(), expected_tuples)


def test_multiple_files_with_ragged_arrays(ray_start_regular_shared, tmp_path):
    # Test reading multiple parquet files, each of which has different-shaped
    # ndarrays in the same column.
    # See https://github.com/ray-project/ray/issues/47960 for more context.
    num_rows = 3
    ds = ray.data.range(num_rows)

    def map(row):
        id = row["id"] + 1
        row["data"] = np.zeros((id * 100, id * 100), dtype=np.int8)
        return row

    # Write 3 parquet files with different-shaped ndarray values in the
    # "data" column.
    ds.map(map).repartition(num_rows).write_parquet(tmp_path)

    # Read these 3 files, check that the result is correct.
    ds2 = ray.data.read_parquet(tmp_path, override_num_blocks=1)
    res = ds2.take_all()
    res = sorted(res, key=lambda row: row["id"])
    assert len(res) == num_rows
    for index, item in enumerate(res):
        assert item["id"] == index
        assert item["data"].shape == (100 * (index + 1), 100 * (index + 1))


def test_count_with_filter(ray_start_regular_shared):
    ds = ray.data.read_parquet(
        "example://iris.parquet", filter=(pds.field("sepal.length") < pds.scalar(0))
    )
    assert ds.count() == 0
    assert isinstance(ds.count(), int)


def test_write_with_schema(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(1)
    schema = pa.schema({"id": pa.float32()})

    ds.write_parquet(tmp_path, schema=schema)

    assert pq.read_table(tmp_path).schema == schema


@pytest.mark.parametrize(
    "row_data",
    [
        [{"a": 1, "b": None}, {"a": 1, "b": 2}],
        [{"a": None, "b": 3}, {"a": 1, "b": 2}],
        [{"a": None, "b": 1}, {"a": 1, "b": None}],
    ],
    ids=["row1_b_null", "row1_a_null", "row_each_null"],
)
def test_write_auto_infer_nullable_fields(
    tmp_path, ray_start_regular_shared, row_data, restore_data_context
):
    """
    Test that when writing multiple blocks, we can automatically infer nullable
    fields.
    """
    ctx = DataContext.get_current()
    # So that we force multiple blocks on mapping.
    ctx.target_max_block_size = 1
    ds = ray.data.range(len(row_data)).map(lambda row: row_data[row["id"]])
    # So we force writing to a single file.
    ds.write_parquet(tmp_path, min_rows_per_file=2)


def test_seed_file_shuffle(restore_data_context, tmp_path):
    def write_parquet_file(path, file_index):
        """Write a dummy Parquet file with test data."""
        # Create a dummy dataset with unique data for each file
        data = {
            "col1": range(10 * file_index, 10 * (file_index + 1)),
            "col2": ["foo", "bar"] * 5,
        }
        table = pa.Table.from_pydict(data)
        pq.write_table(table, path)

    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True

    # Create temporary Parquet files for testing in the current directory
    paths = [os.path.join(tmp_path, f"test_file_{i}.parquet") for i in range(5)]
    for i, path in enumerate(paths):
        # Write dummy Parquet files
        write_parquet_file(path, i)

    # Read with deterministic shuffling
    shuffle_config = FileShuffleConfig(seed=42)
    ds1 = ray.data.read_parquet(paths, shuffle=shuffle_config)
    ds2 = ray.data.read_parquet(paths, shuffle=shuffle_config)

    # Verify deterministic behavior
    assert ds1.take_all() == ds2.take_all()


def test_read_file_with_partition_values(ray_start_regular_shared, tmp_path):
    # Typically, partition values are excluded from the Parquet file and are instead
    # encoded in the directory structure. However, in some cases, partition values
    # are also included in the Parquet file. This test verifies that case.
    table = pa.Table.from_pydict({"data": [0], "year": [2024]})
    os.makedirs(tmp_path / "year=2024")
    pq.write_table(table, tmp_path / "year=2024" / "data.parquet")

    ds = ray.data.read_parquet(tmp_path)

    assert ds.take_all() == [{"data": 0, "year": 2024}]


def test_read_null_data_in_first_file(tmp_path, ray_start_regular_shared):
    # The `read_parquet` implementation might infer the schema from the first file.
    # This test ensures that implementation handles the case where the first file has no
    # data and the inferred type is `null`.
    pq.write_table(pa.Table.from_pydict({"data": [None, None, None]}), tmp_path / "1")
    pq.write_table(pa.Table.from_pydict({"data": ["spam", "ham"]}), tmp_path / "2")

    ds = ray.data.read_parquet(tmp_path)

    rows = sorted(ds.take_all(), key=lambda row: row["data"] or "")
    assert rows == [
        {"data": None},
        {"data": None},
        {"data": None},
        {"data": "ham"},
        {"data": "spam"},
    ]


def test_read_invalid_file_extensions_emits_warning(tmp_path, ray_start_regular_shared):
    table = pa.Table.from_pydict({})
    pq.write_table(table, tmp_path / "no_extension")

    with pytest.warns(FutureWarning, match="file_extensions"):
        ray.data.read_parquet(tmp_path)


def test_parquet_row_group_size_001(ray_start_regular_shared, tmp_path):
    """Verify row_group_size is respected."""

    (
        ray.data.range(10000)
        .repartition(1)
        .write_parquet(
            tmp_path / "test_row_group_5k.parquet",
            row_group_size=5000,
        )
    )

    # Since version 15, use_legacy_dataset is deprecated.
    if parse_version(pa.__version__) >= parse_version("15.0.0"):
        ds = pq.ParquetDataset(tmp_path / "test_row_group_5k.parquet")
    else:
        ds = pq.ParquetDataset(
            tmp_path / "test_row_group_5k.parquet",
            use_legacy_dataset=False,  # required for .fragments attribute
        )
    assert ds.fragments[0].num_row_groups == 2


def test_parquet_row_group_size_002(ray_start_regular_shared, tmp_path):
    """Verify arrow_parquet_args_fn is working with row_group_size."""
    (
        ray.data.range(10000)
        .repartition(1)
        .write_parquet(
            tmp_path / "test_row_group_1k.parquet",
            arrow_parquet_args_fn=lambda: {
                "row_group_size": 1000,  # overrides row_group_size
            },
            row_group_size=5000,
        )
    )

    # Since version 15, use_legacy_dataset is deprecated.
    if parse_version(pa.__version__) >= parse_version("15.0.0"):
        ds = pq.ParquetDataset(tmp_path / "test_row_group_1k.parquet")
    else:
        ds = pq.ParquetDataset(
            tmp_path / "test_row_group_1k.parquet",
            use_legacy_dataset=False,
        )
    assert ds.fragments[0].num_row_groups == 10


@pytest.mark.parametrize("min_rows_per_file", [5, 10])
def test_write_partition_cols_with_min_rows_per_file(
    tmp_path, ray_start_regular_shared, min_rows_per_file
):
    """Test write_parquet with both partition_cols and min_rows_per_file."""

    # Create dataset with 2 partitions, each having 20 rows
    df = pd.DataFrame(
        {
            "partition_col": [0] * 20 + [1] * 20,  # 2 partitions with 20 rows each
            "data": list(range(40)),
        }
    )

    ds = ray.data.from_pandas(df)
    ds.write_parquet(
        tmp_path, partition_cols=["partition_col"], min_rows_per_file=min_rows_per_file
    )

    # Check partition directories exist
    partition_0_dir = tmp_path / "partition_col=0"
    partition_1_dir = tmp_path / "partition_col=1"
    assert partition_0_dir.exists()
    assert partition_1_dir.exists()

    # With the new implementation that tries to minimize file count,
    # each partition (20 rows) should be written as a single file
    # since 20 >= min_rows_per_file for both test cases (5 and 10)
    for partition_dir in [partition_0_dir, partition_1_dir]:
        parquet_files = list(partition_dir.glob("*.parquet"))

        # Verify total rows across all files in partition
        total_rows = 0
        file_sizes = []
        for file_path in parquet_files:
            table = pq.read_table(file_path)
            file_size = len(table)
            file_sizes.append(file_size)
            total_rows += file_size

        assert total_rows == 20  # Each partition should have 20 rows total

        # Add explicit assertion about individual file sizes for clarity
        print(
            f"Partition {partition_dir.name} file sizes with min_rows_per_file={min_rows_per_file}: {file_sizes}"
        )

        # With the new optimization logic, we expect fewer files with larger sizes
        # Each file should have at least min_rows_per_file rows
        for file_size in file_sizes:
            assert (
                file_size >= min_rows_per_file
            ), f"File size {file_size} is less than min_rows_per_file {min_rows_per_file}"

    # Verify we can read back the data correctly
    ds_read = ray.data.read_parquet(tmp_path)
    assert ds_read.count() == 40
    assert set(ds_read.schema().names) == {"partition_col", "data"}

    # ------------------------------------------------------------------
    # Verify that the data written and read back are identical
    # ------------------------------------------------------------------
    expected_df = df.sort_values("data").reset_index(drop=True)
    actual_df = ds_read.to_pandas().sort_values("data").reset_index(drop=True)

    # Parquet partition values are read back as strings; cast both sides.
    actual_df["partition_col"] = actual_df["partition_col"].astype(str)
    expected_df["partition_col"] = expected_df["partition_col"].astype(str)

    # Align column order and compare.
    actual_df = actual_df[expected_df.columns]
    pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)


@pytest.mark.parametrize("max_rows_per_file", [5, 10, 25])
def test_write_max_rows_per_file(tmp_path, ray_start_regular_shared, max_rows_per_file):
    ray.data.range(100, override_num_blocks=1).write_parquet(
        tmp_path, max_rows_per_file=max_rows_per_file
    )

    total_rows = 0
    file_sizes = []
    for filename in os.listdir(tmp_path):
        table = pq.read_table(os.path.join(tmp_path, filename))
        file_size = len(table)
        file_sizes.append(file_size)
        assert file_size <= max_rows_per_file
        total_rows += file_size

    # Verify all rows were written
    assert total_rows == 100

    # Add explicit assertion about individual file sizes for clarity
    print(f"File sizes with max_rows_per_file={max_rows_per_file}: {file_sizes}")
    for size in file_sizes:
        assert (
            size <= max_rows_per_file
        ), f"File size {size} exceeds max_rows_per_file {max_rows_per_file}"

    # ------------------------------------------------------------------
    # Verify the parquet round-trip: written data == read-back data
    # ------------------------------------------------------------------
    ds_reloaded = ray.data.read_parquet(tmp_path)
    assert ds_reloaded.count() == 100

    expected_df = (
        pd.DataFrame({"id": list(range(100))}).sort_values("id").reset_index(drop=True)
    )
    actual_df = ds_reloaded.to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "min_rows_per_file,max_rows_per_file", [(5, 10), (10, 20), (15, 30)]
)
def test_write_min_max_rows_per_file(
    tmp_path, ray_start_regular_shared, min_rows_per_file, max_rows_per_file
):
    ray.data.range(100, override_num_blocks=1).write_parquet(
        tmp_path,
        min_rows_per_file=min_rows_per_file,
        max_rows_per_file=max_rows_per_file,
    )

    total_rows = 0
    file_sizes = []
    for filename in os.listdir(tmp_path):
        table = pq.read_table(os.path.join(tmp_path, filename))
        file_size = len(table)
        file_sizes.append(file_size)
        total_rows += file_size

    # Verify all rows were written
    assert total_rows == 100

    # Add explicit assertion about individual file sizes for clarity
    print(
        f"File sizes with min={min_rows_per_file}, max={max_rows_per_file}: {file_sizes}"
    )
    for size in file_sizes:
        if size < min_rows_per_file:
            print(
                f"File size {size} is less than min_rows_per_file {min_rows_per_file}"
            )
        assert (
            size <= max_rows_per_file
        ), f"File size {size} not less than {max_rows_per_file}"

    # ------------------------------------------------------------------
    # Verify the parquet round-trip: written data == read-back data
    # ------------------------------------------------------------------
    ds_reloaded = ray.data.read_parquet(tmp_path)
    assert ds_reloaded.count() == 100

    expected_df = (
        pd.DataFrame({"id": list(range(100))}).sort_values("id").reset_index(drop=True)
    )
    actual_df = ds_reloaded.to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)


def test_write_max_rows_per_file_validation(tmp_path, ray_start_regular_shared):
    """Test validation of max_rows_per_file parameter."""

    # Test negative value
    with pytest.raises(
        ValueError, match="max_rows_per_file must be a positive integer"
    ):
        ray.data.range(100).write_parquet(tmp_path, max_rows_per_file=-1)

    # Test zero value
    with pytest.raises(
        ValueError, match="max_rows_per_file must be a positive integer"
    ):
        ray.data.range(100).write_parquet(tmp_path, max_rows_per_file=0)


def test_write_min_max_rows_per_file_validation(tmp_path, ray_start_regular_shared):
    """Test validation when both min and max are specified."""

    # Test min > max
    with pytest.raises(
        ValueError,
        match="min_rows_per_file .* cannot be greater than max_rows_per_file",
    ):
        ray.data.range(100).write_parquet(
            tmp_path, min_rows_per_file=20, max_rows_per_file=10
        )


@pytest.mark.parametrize("max_rows_per_file", [5, 10])
def test_write_partition_cols_with_max_rows_per_file(
    tmp_path, ray_start_regular_shared, max_rows_per_file
):
    """Test max_rows_per_file with partition columns."""
    import pyarrow.parquet as pq

    # Create data with partition column
    def create_row(row):
        i = row["id"]
        return {"id": i, "partition": i % 3, "value": f"value_{i}"}

    ds = ray.data.range(30).map(create_row)
    ds.write_parquet(
        tmp_path, partition_cols=["partition"], max_rows_per_file=max_rows_per_file
    )

    # Check each partition directory
    total_rows = 0
    all_file_sizes = []
    for partition_dir in os.listdir(tmp_path):
        partition_path = os.path.join(tmp_path, partition_dir)
        if os.path.isdir(partition_path):
            partition_file_sizes = []
            for filename in os.listdir(partition_path):
                if filename.endswith(".parquet"):
                    table = pq.read_table(os.path.join(partition_path, filename))
                    file_size = len(table)
                    partition_file_sizes.append(file_size)
                    assert file_size <= max_rows_per_file
                    total_rows += file_size
            all_file_sizes.extend(partition_file_sizes)
            print(
                f"Partition {partition_dir} file sizes with max_rows_per_file={max_rows_per_file}: {partition_file_sizes}"
            )

    # Verify all rows were written
    assert total_rows == 30

    # Add explicit assertion about individual file sizes for clarity
    for size in all_file_sizes:
        assert (
            size <= max_rows_per_file
        ), f"File size {size} exceeds max_rows_per_file {max_rows_per_file}"

    # ------------------------------------------------------------------
    # Verify the parquet round-trip: data read back must equal original
    # ------------------------------------------------------------------
    ds_reloaded = ray.data.read_parquet(tmp_path)
    assert ds_reloaded.count() == 30

    expected_rows = [
        {"id": i, "partition": i % 3, "value": f"value_{i}"} for i in range(30)
    ]
    expected_df = pd.DataFrame(expected_rows).sort_values("id").reset_index(drop=True)
    actual_df = ds_reloaded.to_pandas().sort_values("id").reset_index(drop=True)

    # Align column order for a strict equality check.
    actual_df = actual_df[expected_df.columns]
    # Parquet partition values are read back as strings; make both sides `str`
    # so the value-level comparison succeeds (dtype may still differ).
    actual_df["partition"] = actual_df["partition"].astype(str)
    expected_df["partition"] = expected_df["partition"].astype(str)

    pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)


@dataclass
class RowGroupLimitCase:
    row_group_size: Optional[int]
    min_rows_per_file: Optional[int]
    max_rows_per_file: Optional[int]
    expected_min: Optional[int]
    expected_max: Optional[int]
    expected_max_file: Optional[int]


ROW_GROUP_LIMIT_CASES = [
    RowGroupLimitCase(None, None, None, None, None, None),
    RowGroupLimitCase(1000, None, None, 1000, 1000, None),
    RowGroupLimitCase(None, 500, None, 500, None, None),
    RowGroupLimitCase(None, None, 2000, None, 2000, 2000),
    RowGroupLimitCase(1000, 500, 2000, 1000, 1000, 2000),
    RowGroupLimitCase(3000, 500, 2000, 2000, 2000, 2000),
]


@pytest.mark.parametrize(
    "case",
    ROW_GROUP_LIMIT_CASES,
    ids=[f"case_{i}" for i in range(len(ROW_GROUP_LIMIT_CASES))],
)
def test_choose_row_group_limits_parameterized(case):
    """Validate the helper across representative inputs."""
    from ray.data._internal.datasource.parquet_datasink import choose_row_group_limits

    result = choose_row_group_limits(
        case.row_group_size, case.min_rows_per_file, case.max_rows_per_file
    )
    assert result == (
        case.expected_min,
        case.expected_max,
        case.expected_max_file,
    ), f"Unexpected result for {case}"

    # Invariants when both bounds are known.
    min_rows, max_rows, _ = result
    if min_rows is not None and max_rows is not None:
        assert min_rows <= max_rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
