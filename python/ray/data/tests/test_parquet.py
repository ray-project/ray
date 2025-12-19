import os
import pathlib
import shutil
import time
from dataclasses import dataclass
from typing import Optional, Union

import fsspec
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version
from pyarrow.fs import FSSpecHandler, PyFileSystem
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.air.util.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
    get_arrow_extension_fixed_shape_tensor_types,
)
from ray.data import FileShuffleConfig, Schema
from ray.data._internal.datasource.parquet_datasource import (
    ParquetDatasource,
)
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
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


def test_write_parquet_partition_cols(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
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


def test_include_paths(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    path = os.path.join(tmp_path, "test.parquet")
    table = pa.Table.from_pydict({"animals": ["cat", "dog"]})
    pq.write_table(table, path)

    ds = ray.data.read_parquet(path, include_paths=True)

    paths = [row["path"] for row in ds.take_all()]
    assert paths == [path, path]


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
def test_parquet_read_basic(
    ray_start_regular_shared, fs, data_path, target_max_block_size_infinite_or_default
):
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
    ray_start_regular_shared,
    restore_data_context,
    fs,
    data_path,
    target_max_block_size_infinite_or_default,
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
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
    ],
)
def test_parquet_read_partitioned(
    ray_start_regular_shared, fs, data_path, target_max_block_size_infinite_or_default
):
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


def test_parquet_read_partitioned_with_filter(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
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
def test_parquet_read_partitioned_with_columns(
    ray_start_regular_shared, fs, data_path, target_max_block_size_infinite_or_default
):
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
    ray_start_regular_shared, fs, data_path, target_max_block_size_infinite_or_default
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


def test_parquet_read_partitioned_explicit(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
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


def test_projection_pushdown_non_partitioned(ray_start_regular_shared, temp_dir):
    path = "example://iris.parquet"

    # Test projection from read_parquet
    ds = ray.data.read_parquet(path, columns=["variety"])

    schema = ds.schema()

    assert ["variety"] == schema.base_schema.names
    assert ds.count() == 150

    # Test projection pushed down into read op
    ds = ray.data.read_parquet(path).select_columns("variety")

    assert ds._plan.explain().strip() == (
        "-------- Logical Plan --------\n"
        "Project[Project]\n"
        "+- Read[ReadParquet]\n"
        "\n-------- Logical Plan (Optimized) --------\n"
        "Read[ReadParquet]\n"
        "\n-------- Physical Plan --------\n"
        "TaskPoolMapOperator[ReadParquet]\n"
        "+- InputDataBuffer[Input]\n"
        "\n-------- Physical Plan (Optimized) --------\n"
        "TaskPoolMapOperator[ReadParquet]\n"
        "+- InputDataBuffer[Input]"
    )

    # Assert schema being appropriately projected
    schema = ds.schema()
    assert ["variety"] == schema.base_schema.names

    assert ds.count() == 150

    # Assert empty projection is reading no data
    ds = ray.data.read_parquet(path).select_columns([])

    summary = ds.materialize()._plan.stats().to_summary()

    assert "ReadParquet" in summary.base_name
    assert summary.extra_metrics["bytes_task_outputs_generated"] == 0


def test_projection_pushdown_partitioned(ray_start_regular_shared, temp_dir):
    ds = ray.data.read_parquet("example://iris.parquet").materialize()

    partitioned_ds_path = f"{temp_dir}/partitioned_iris"
    # Write out partitioned dataset
    ds.write_parquet(partitioned_ds_path, partition_cols=["variety"])

    partitioned_ds = ray.data.read_parquet(
        partitioned_ds_path, columns=["variety"]
    ).materialize()

    print(partitioned_ds.schema())

    assert [
        "sepal.length",
        "sepal.width",
        "petal.length",
        "petal.width",
        "variety",
    ] == ds.take_batch(batch_format="pyarrow").column_names

    assert ["variety"] == partitioned_ds.take_batch(batch_format="pyarrow").column_names

    assert ds.count() == partitioned_ds.count()


def test_projection_pushdown_on_count(ray_start_regular_shared, temp_dir):
    path = "example://iris.parquet"

    # Test reading full dataset
    # ds = ray.data.read_parquet(path).materialize()

    # Test projection from read_parquet
    num_rows = ray.data.read_parquet(path).count()

    assert num_rows == 150


def test_parquet_read_with_udf(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
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


def test_parquet_reader_estimate_data_size(shutdown_only, tmp_path):
    ctx = ray.data.context.DataContext.get_current()
    old_decoding_size_estimation = ctx.decoding_size_estimation
    ctx.decoding_size_estimation = True
    try:
        tensor_output_path = os.path.join(tmp_path, "tensor")
        # NOTE: It's crucial to override # of blocks to get stable # of files
        #       produced and make sure data size estimates are stable
        ray.data.range_tensor(
            1000, shape=(1000,), override_num_blocks=10
        ).write_parquet(tensor_output_path)
        ds = ray.data.read_parquet(tensor_output_path)
        assert ds._plan.initial_num_blocks() > 1
        data_size = ds.size_bytes()
        assert (
            data_size >= 6_000_000 and data_size <= 10_000_000
        ), "estimated data size is out of expected bound"
        data_size = ds.materialize().size_bytes()
        assert (
            data_size >= 7_000_000 and data_size <= 10_000_000
        ), "actual data size is out of expected bound"

        datasource = ParquetDatasource(tensor_output_path)
        assert (
            datasource._encoding_ratio >= 300 and datasource._encoding_ratio <= 600
        ), "encoding ratio is out of expected bound"
        data_size = datasource.estimate_inmemory_data_size()
        assert (
            data_size >= 6_000_000 and data_size <= 10_000_000
        ), "estimated data size is either out of expected bound"
        assert (
            data_size
            == ParquetDatasource(tensor_output_path).estimate_inmemory_data_size()
        ), "estimated data size is not deterministic in multiple calls."

        text_output_path = os.path.join(tmp_path, "text")
        ray.data.range(1000).map(lambda _: {"text": "a" * 1000}).write_parquet(
            text_output_path
        )
        ds = ray.data.read_parquet(text_output_path)
        assert ds._plan.initial_num_blocks() > 1
        data_size = ds.size_bytes()
        assert (
            data_size >= 700_000 and data_size <= 2_200_000
        ), "estimated data size is out of expected bound"
        data_size = ds.materialize().size_bytes()
        assert (
            data_size >= 1_000_000 and data_size <= 2_000_000
        ), "actual data size is out of expected bound"

        datasource = ParquetDatasource(text_output_path)
        assert (
            datasource._encoding_ratio >= 6 and datasource._encoding_ratio <= 300
        ), "encoding ratio is out of expected bound"
        data_size = datasource.estimate_inmemory_data_size()
        assert (
            data_size >= 700_000 and data_size <= 2_200_000
        ), "estimated data size is out of expected bound"
        assert (
            data_size
            == ParquetDatasource(text_output_path).estimate_inmemory_data_size()
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
    ray_start_regular_shared,
    tmp_path,
    filename_template,
    should_raise_error,
    target_max_block_size_infinite_or_default,
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


def test_parquet_file_extensions(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
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
def test_parquet_roundtrip(
    ray_start_regular_shared, fs, data_path, target_max_block_size_infinite_or_default
):
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


def test_parquet_read_empty_file(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    path = os.path.join(tmp_path, "data.parquet")
    table = pa.table({})
    pq.write_table(table, path)

    ds = ray.data.read_parquet(path)

    assert ds.take_all() == []


def test_parquet_reader_batch_size(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    path = os.path.join(tmp_path, "data.parquet")
    ray.data.range_tensor(1000, shape=(1000,)).write_parquet(path)
    ds = ray.data.read_parquet(path, batch_size=10)
    assert ds.count() == 1000


def test_parquet_datasource_names(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"spam": [1, 2, 3]})
    path = os.path.join(tmp_path, "data.parquet")
    df.to_parquet(path)

    assert ParquetDatasource(path).get_name() == "Parquet"


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_parquet_concurrency(
    ray_start_regular_shared, fs, data_path, target_max_block_size_infinite_or_default
):
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


@pytest.mark.parametrize("shuffle", [True, False, "file"])
def test_invalid_shuffle_arg_raises_error(
    ray_start_regular_shared, shuffle, target_max_block_size_infinite_or_default
):

    with pytest.raises(ValueError):
        ray.data.read_parquet("example://iris.parquet", shuffle=shuffle)


@pytest.mark.parametrize("shuffle", [None, "files"])
def test_valid_shuffle_arg_does_not_raise_error(
    ray_start_regular_shared, shuffle, target_max_block_size_infinite_or_default
):
    ray.data.read_parquet("example://iris.parquet", shuffle=shuffle)


def test_partitioning_in_dataset_kwargs_raises_error(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    with pytest.raises(ValueError):
        ray.data.read_parquet(
            "example://iris.parquet", dataset_kwargs=dict(partitioning="hive")
        )


def test_tensors_in_tables_parquet(
    ray_start_regular_shared,
    tmp_path,
    restore_data_context,
    target_max_block_size_infinite_or_default,
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


def test_multiple_files_with_ragged_arrays(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
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


def test_count_with_filter(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
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


def test_seed_file_shuffle(
    restore_data_context, tmp_path, target_max_block_size_infinite_or_default
):
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
    shuffle_config = FileShuffleConfig(seed=42, reseed_after_execution=False)
    ds1 = ray.data.read_parquet(paths, shuffle=shuffle_config)
    ds2 = ray.data.read_parquet(paths, shuffle=shuffle_config)

    # Verify deterministic behavior
    assert ds1.take_all() == ds2.take_all()


def test_seed_file_shuffle_with_execution_update(
    restore_data_context, tmp_path, target_max_block_size_infinite_or_default
):
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
    paths = [os.path.join(tmp_path, f"test_file_{i}.parquet") for i in range(15)]
    for i, path in enumerate(paths):
        # Write dummy Parquet files
        write_parquet_file(path, i)

    shuffle_config = FileShuffleConfig(seed=42)
    ds1 = ray.data.read_parquet(paths, shuffle=shuffle_config)
    ds2 = ray.data.read_parquet(paths, shuffle=shuffle_config)

    ds1_epoch_results = []
    ds2_epoch_results = []
    for i in range(5):
        ds1_epoch = ds1.to_pandas()
        ds2_epoch = ds2.to_pandas()
        ds1_epoch_results.append(ds1_epoch)
        ds2_epoch_results.append(ds2_epoch)
        # For the same epoch, ds1 and ds2 should produce identical results
        pd.testing.assert_frame_equal(ds1_epoch, ds2_epoch)

    # Convert results to hashable format for comparison
    def make_hashable(df):
        """Convert a DataFrame to a hashable string representation."""
        return df.to_csv()

    ds1_hashable_results = {make_hashable(result) for result in ds1_epoch_results}
    ds2_hashable_results = {make_hashable(result) for result in ds2_epoch_results}

    assert (
        len(ds1_hashable_results) == 5
    ), "ds1 should produce different results across epochs"
    assert (
        len(ds2_hashable_results) == 5
    ), "ds2 should produce different results across epochs"


def test_seed_file_shuffle_with_execution_no_effect(
    restore_data_context, tmp_path, target_max_block_size_infinite_or_default
):
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

    shuffle_config = FileShuffleConfig(seed=42, reseed_after_execution=False)
    ds1 = ray.data.read_parquet(paths, shuffle=shuffle_config)
    ds2 = ray.data.read_parquet(paths, shuffle=shuffle_config)

    ds1_execution_results = []
    ds2_execution_results = []
    for i in range(5):
        ds1_execution = ds1.to_pandas()
        ds2_execution = ds2.to_pandas()
        ds1_execution_results.append(ds1_execution)
        ds2_execution_results.append(ds2_execution)
        # For the same execution, ds1 and ds2 should produce identical results
        pd.testing.assert_frame_equal(ds1_execution, ds2_execution)

    # Convert results to hashable format for comparison
    def make_hashable(df):
        """Convert a DataFrame to a hashable string representation."""
        return df.to_csv()

    ds1_hashable_results = {make_hashable(result) for result in ds1_execution_results}
    ds2_hashable_results = {make_hashable(result) for result in ds2_execution_results}

    assert (
        len(ds1_hashable_results) == 1
    ), "ds1 should produce the same results across executions"
    assert (
        len(ds2_hashable_results) == 1
    ), "ds2 should produce the same results across executions"


def test_read_file_with_partition_values(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    # Typically, partition values are excluded from the Parquet file and are instead
    # encoded in the directory structure. However, in some cases, partition values
    # are also included in the Parquet file. This test verifies that case.
    table = pa.Table.from_pydict({"data": [0], "year": [2024]})
    os.makedirs(tmp_path / "year=2024")
    pq.write_table(table, tmp_path / "year=2024" / "data.parquet")

    ds = ray.data.read_parquet(tmp_path)

    assert ds.take_all() == [{"data": 0, "year": 2024}]


def test_read_null_data_in_first_file(
    tmp_path, ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    # The `read_parquet` implementation might infer the schema from the first file.
    # This test ensures that implementation handles the case where the first file has no
    # data and the inferred type is `null`.
    pq.write_table(
        pa.Table.from_pydict({"data": [None, None, None]}), tmp_path / "1.parquet"
    )
    pq.write_table(
        pa.Table.from_pydict({"data": ["spam", "ham"]}), tmp_path / "2.parquet"
    )

    ds = ray.data.read_parquet(tmp_path)

    rows = sorted(ds.take_all(), key=lambda row: row["data"] or "")
    assert rows == [
        {"data": None},
        {"data": None},
        {"data": None},
        {"data": "ham"},
        {"data": "spam"},
    ]


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


@pytest.mark.parametrize("override_num_blocks", [1, 2, 3])
def test_max_block_size_none_respects_override_num_blocks(
    ray_start_regular_shared,
    tmp_path,
    override_num_blocks,
    target_max_block_size_infinite,
):
    """
    When `DataContext.target_max_block_size` is explicitly set to ``None``,
    TODO override_num_blocks should always be respected even when target_max_block_size isn't set to None.
    read_parquet must still honour ``override_num_blocks``.
    The read should yield the specified number of input blocks and – after a pivot –
    one output row per block (since all rows have the same ID).
    """
    import os

    import pandas as pd

    # Build a >10 k-row Parquet file.
    num_rows = 10_005
    df = pd.DataFrame(
        {
            "ID": ["A"] * num_rows,
            "values": range(num_rows),
            "dttm": pd.date_range("2024-01-01", periods=num_rows, freq="h").astype(str),
        }
    )
    file_path = os.path.join(tmp_path, "maxblock_none.parquet")
    df.to_parquet(file_path)

    # Read with the specified number of blocks enforced.
    ds = ray.data.read_parquet(file_path, override_num_blocks=override_num_blocks)

    def _pivot_data(batch: pd.DataFrame) -> pd.DataFrame:  # noqa: WPS430
        return batch.pivot(index="ID", columns="dttm", values="values")

    out_ds = ds.map_batches(
        _pivot_data,
        batch_size=None,
        batch_format="pandas",
    )
    out_df = out_ds.to_pandas()

    # Create expected result using pandas pivot on original data
    expected_df = df.pivot(index="ID", columns="dttm", values="values")

    # Verify the schemas match (same columns)
    assert set(out_df.columns) == set(expected_df.columns)

    # Verify we have the expected number of rows (one per block)
    assert len(out_df) == override_num_blocks

    # Verify that all original values are present by comparing with expected result
    # Only sum non-null values to avoid counting NaN as -1
    expected_sum = expected_df.sum(skipna=True).sum()
    actual_sum = out_df.sum(skipna=True).sum()
    assert actual_sum == expected_sum

    # Verify that the combined result contains the same data as the expected result
    # by checking that each column's non-null values match
    for col in expected_df.columns:
        expected_values = expected_df[col].dropna()
        actual_values = out_df[col].dropna()
        assert len(expected_values) == len(actual_values)
        assert set(expected_values) == set(actual_values)


@pytest.mark.parametrize("min_rows_per_file", [5, 10])
def test_write_partition_cols_with_min_rows_per_file(
    tmp_path,
    ray_start_regular_shared,
    min_rows_per_file,
    target_max_block_size_infinite_or_default,
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
def test_write_max_rows_per_file(
    tmp_path,
    ray_start_regular_shared,
    max_rows_per_file,
    target_max_block_size_infinite_or_default,
):
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
    tmp_path,
    ray_start_regular_shared,
    min_rows_per_file,
    max_rows_per_file,
    target_max_block_size_infinite_or_default,
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
    tmp_path,
    ray_start_regular_shared,
    max_rows_per_file,
    target_max_block_size_infinite_or_default,
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
    RowGroupLimitCase(
        row_group_size=None,
        min_rows_per_file=None,
        max_rows_per_file=None,
        expected_min=None,
        expected_max=None,
        expected_max_file=None,
    ),
    RowGroupLimitCase(
        row_group_size=1000,
        min_rows_per_file=None,
        max_rows_per_file=None,
        expected_min=1000,
        expected_max=1000,
        expected_max_file=None,
    ),
    RowGroupLimitCase(
        row_group_size=None,
        min_rows_per_file=500,
        max_rows_per_file=None,
        expected_min=500,
        expected_max=None,
        expected_max_file=None,
    ),
    RowGroupLimitCase(
        row_group_size=None,
        min_rows_per_file=None,
        max_rows_per_file=2000,
        expected_min=None,
        expected_max=2000,
        expected_max_file=2000,
    ),
    RowGroupLimitCase(
        row_group_size=1000,
        min_rows_per_file=500,
        max_rows_per_file=2000,
        expected_min=1000,
        expected_max=1000,
        expected_max_file=2000,
    ),
    RowGroupLimitCase(
        row_group_size=3000,
        min_rows_per_file=500,
        max_rows_per_file=2000,
        expected_min=2000,
        expected_max=2000,
        expected_max_file=2000,
    ),
    RowGroupLimitCase(
        row_group_size=None,
        min_rows_per_file=2000000,  # Greater than 1024 * 1024 (1048576)
        max_rows_per_file=None,
        expected_min=2000000,
        expected_max=2000000,
        expected_max_file=2000000,
    ),
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


def test_write_parquet_large_min_rows_per_file_exceeds_arrow_default(
    tmp_path, ray_start_regular_shared
):
    from ray.data._internal.datasource.parquet_datasink import (
        ARROW_DEFAULT_MAX_ROWS_PER_GROUP,
    )

    """Test that min_rows_per_file > ARROW_DEFAULT_MAX_ROWS_PER_GROUP triggers max_rows_per_group setting."""
    # ARROW_DEFAULT_MAX_ROWS_PER_GROUP = 1024 * 1024 = 1048576
    # We'll use a min_rows_per_file that exceeds this threshold
    min_rows_per_file = (
        2 * ARROW_DEFAULT_MAX_ROWS_PER_GROUP
    )  # 2097152, which is > 1048576

    # Create a dataset with the required number of rows
    ds = ray.data.range(min_rows_per_file, override_num_blocks=1)

    # Write with min_rows_per_file > ARROW_DEFAULT_MAX_ROWS_PER_GROUP
    # This should trigger the condition where max_rows_per_group and max_rows_per_file
    # are set to min_rows_per_group (which comes from min_rows_per_file)
    ds.write_parquet(tmp_path, min_rows_per_file=min_rows_per_file)

    # Verify that the parquet files were written correctly
    written_files = [f for f in os.listdir(tmp_path) if f.endswith(".parquet")]
    assert len(written_files) == 1

    # Read back the data to verify correctness
    ds_read = ray.data.read_parquet(tmp_path)
    assert ds_read.count() == min_rows_per_file


def test_read_parquet_with_zero_row_groups(shutdown_only, tmp_path):
    """Test reading a parquet file with 0 row groups."""
    # Create an empty parquet file (0 row groups)
    empty_path = os.path.join(tmp_path, "empty.parquet")
    schema = pa.schema({"id": pa.int64()})
    with pq.ParquetWriter(empty_path, schema):
        pass

    parquet_file = pq.ParquetFile(empty_path)
    assert parquet_file.num_row_groups == 0

    # Test reading the empty parquet file
    dataset = ray.data.read_parquet(empty_path)
    assert dataset.count() == 0


@pytest.mark.parametrize(
    "partition_info",
    [
        {"partition_cols": None, "output_dir": "test_output"},
        {
            "partition_cols": ["id_mod"],
            "output_dir": "test_output_partitioned",
        },
    ],
    ids=["no_partitioning", "with_partitioning"],
)
def test_parquet_write_parallel_overwrite(
    ray_start_regular_shared, tmp_path, partition_info
):
    """Test parallel Parquet write with overwrite mode."""

    partition_cols = partition_info["partition_cols"]
    output_dir = partition_info["output_dir"]

    # Create dataset with 1000 rows
    df_data = {"id": range(1000), "value": [f"value_{i}" for i in range(1000)]}
    if partition_cols:
        df_data["id_mod"] = [i % 10 for i in range(1000)]  # 10 partitions
    df = pd.DataFrame(df_data)
    ds = ray.data.from_pandas(df)

    # Repartition to ensure multiple write tasks
    ds = ds.repartition(10)

    # Write with overwrite mode
    path = os.path.join(tmp_path, output_dir)
    ds.write_parquet(path, mode="overwrite", partition_cols=partition_cols)

    # Read back and verify
    result = ray.data.read_parquet(path)
    assert result.count() == 1000


def test_read_parquet_with_none_partitioning_and_columns(tmp_path):
    # Test for https://github.com/ray-project/ray/issues/55279.
    table = pa.table({"column": [42]})
    path = os.path.join(tmp_path, "file.parquet")
    pq.write_table(table, path)

    ds = ray.data.read_parquet(path, partitioning=None, columns=["column"])

    assert ds.take_all() == [{"column": 42}]


def _create_test_data(num_rows: int) -> dict:
    return {
        "int_col": list(range(num_rows)),
        "float_col": [float(i) for i in range(num_rows)],
        "str_col": [f"str_{i}" for i in range(num_rows)],
    }


@pytest.mark.parametrize(
    "batch_size,filter_expr,expected_rows,description",
    [
        # No batch size cases
        (None, "int_col > 500", 499, "No batch size, int > 500"),
        (None, "int_col < 200", 200, "No batch size, int < 200"),
        (
            None,
            "float_col == 42.0",
            1,
            "No batch size, float == 42.0",
        ),
        (
            None,
            "str_col == 'str_42'",
            1,
            "No batch size, str == str_42",
        ),
        # Batch size cases
        (100, "int_col > 500", 499, "Fixed batch size, int > 500"),
        (200, "int_col < 200", 200, "Fixed batch size, int < 200"),
        (
            300,
            "float_col == 42.0",
            1,
            "Fixed batch size, float == 42.0",
        ),
        (
            400,
            "str_col == 'str_42'",
            1,
            "Fixed batch size, str == str_42",
        ),
    ],
)
def test_read_parquet_with_filter_selectivity(
    ray_start_regular_shared,
    tmp_path,
    batch_size,
    filter_expr,
    expected_rows,
    description,
):
    """Test reading parquet files with filter expressions and different batch sizes."""
    num_rows = 1000
    data = _create_test_data(num_rows)
    table = pa.Table.from_pydict(data)

    file_path = os.path.join(tmp_path, "test.parquet")
    pq.write_table(table, file_path, row_group_size=200)

    if batch_size is not None:
        ray.data.DataContext.get_current().target_max_block_size = batch_size
    ds = ray.data.read_parquet(file_path).filter(expr=filter_expr)

    assert ds.count() == expected_rows, (
        f"{description}: Filter '{filter_expr}' returned {ds.count()} rows, "
        f"expected {expected_rows}"
    )

    # Verify schema has expected columns and types
    assert ds.schema().base_schema == table.schema


@pytest.mark.parametrize("batch_size", [None, 100, 200, 10_000])
@pytest.mark.parametrize(
    "columns",
    [
        # Empty projection
        [],
        ["int_col"],
        ["int_col", "float_col", "str_col"],
    ],
)
def test_read_parquet_with_columns_selectivity(
    ray_start_regular_shared,
    tmp_path,
    batch_size,
    columns,
):
    """Test reading parquet files with different column selections and batch sizes."""
    num_rows = 1000
    data = _create_test_data(num_rows)
    table = pa.Table.from_pydict(data)

    file_path = os.path.join(tmp_path, "test.parquet")
    pq.write_table(table, file_path, row_group_size=200)

    if batch_size is not None:
        ray.data.DataContext.get_current().target_max_block_size = batch_size
    ds = ray.data.read_parquet(file_path, columns=columns)

    assert ds.count() == num_rows, (
        f"Column selection {columns} with batch_size={batch_size} "
        f"returned {ds.count()} rows, expected {num_rows}"
    )

    assert set(ds.schema().names) == set(columns), (
        f"Column selection {columns} with batch_size={batch_size} "
        f"returned columns {ds.schema().names}"
    )


def test_get_parquet_dataset_fs_serialization_fallback(
    ray_start_regular_shared, tmp_path: pathlib.Path
):
    """Test that the fallback mechanism for serializing the filesystem works."""
    # 1) Local parquet file
    local_file = tmp_path / "test.parquet"
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})), local_file
    )

    # 2) Problematic fsspec FS wrapped as a *PyArrow* FS so ParquetDataset accepts it
    class BadFSSpec(fsspec.AbstractFileSystem):
        protocol = "file"

        def info(self, path, **kwargs):
            # Wrong shape → fsspec/pyarrow will later blow up with TypeError
            return ["not", "a", "dict"]

        def ls(self, path, **kwargs):
            return [{"name": str(path), "type": "file", "size": os.path.getsize(path)}]

        def open(self, path, mode="rb", **kwargs):
            return open(path, mode)

    problematic_fs = PyFileSystem(FSSpecHandler(BadFSSpec()))

    # 3) Direct ParquetDataset in worker → should raise (TypeError/ArrowException)
    @ray.remote
    def direct_parquet_usage(paths, fs, kwargs):
        import pyarrow.parquet as pq

        return pq.ParquetDataset(paths, filesystem=fs, **(kwargs or {}))

    with pytest.raises(Exception) as exc_info:
        ray.get(direct_parquet_usage.remote([str(local_file)], problematic_fs, {}))

    msg = str(exc_info.value).lower()
    assert any(
        k in msg
        for k in ["typeerror", "filesystem", "cannot wrap", "pickle", "serialize"]
    )

    # 4) Helper should succeed (fallback re-resolves to LocalFileSystem inside worker)
    @ray.remote
    def call_helper(paths, fs, kwargs):
        from ray.data._internal.datasource.parquet_datasource import get_parquet_dataset

        return get_parquet_dataset(paths, fs, kwargs)

    ds = ray.get(call_helper.remote([str(local_file)], problematic_fs, {}))
    assert ds is not None


@pytest.fixture
def hive_partitioned_dataset(tmp_path):
    """Create a Hive-partitioned Parquet dataset for testing."""
    # Create test data with multiple partitions
    num_partitions = 3
    rows_per_partition = 10
    data = []
    for partition_val in range(num_partitions):
        for i in range(rows_per_partition):
            data.append(
                {
                    "id": partition_val * rows_per_partition + i,
                    "value": f"val_{partition_val}_{i}",
                    "score": partition_val * 10 + i,
                    "country": f"country_{partition_val % 2}",
                    "year": 2020 + partition_val,
                }
            )

    # Create base DataFrame
    base_df = pd.DataFrame(data)

    # Write as Hive-partitioned Parquet
    partitioned_path = os.path.join(tmp_path, "partitioned_data")
    table = pa.Table.from_pandas(base_df)
    pq.write_to_dataset(
        table,
        root_path=partitioned_path,
        partition_cols=["country", "year"],
        existing_data_behavior="overwrite_or_ignore",
    )

    return partitioned_path, base_df


@pytest.mark.parametrize(
    "operations",
    [
        # Single operations
        ("select",),
        ("select_partition_and_data",),
        ("select_data_only",),
        ("rename_partition",),
        ("rename_data",),
        ("rename_partition_and_data",),
        ("filter_partition",),
        ("filter_data",),
        ("filter_partition_and_data",),
        ("with_column",),
        # Two-operation combinations
        ("select", "rename_partition"),
        ("select", "rename_data"),
        ("select", "rename_partition_and_data"),
        ("select", "filter_partition"),
        ("select", "filter_data"),
        # Test narrowing projection: select all columns, then narrow to exclude some partition columns
        ("select", "select_partition_and_data"),
        ("select", "select_data_only"),
        ("rename_partition", "filter_partition"),
        ("rename_partition", "filter_data"),
        ("rename_data", "filter_partition"),
        ("rename_data", "filter_data"),
        ("rename_partition_and_data", "filter_partition_and_data"),
        ("with_column", "rename_partition"),
        ("with_column", "rename_data"),
        ("with_column", "filter_data"),
        # Three-operation combinations
        ("select", "rename_partition", "filter_partition"),
        ("select", "rename_data", "filter_data"),
        ("select", "rename_partition_and_data", "filter_partition_and_data"),
        ("rename_partition", "filter_partition", "with_column"),
        ("rename_data", "filter_data", "with_column"),
        # Four-operation combinations
        (
            "select",
            "rename_partition_and_data",
            "filter_partition_and_data",
            "with_column",
        ),
    ],
    ids=lambda ops: "_".join(ops) if isinstance(ops, tuple) else ops,
)
@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="Hive partitioned parquet operations require pyarrow >= 14.0.0",
)
def test_hive_partitioned_parquet_operations(
    ray_start_regular_shared,
    hive_partitioned_dataset,
    operations,
):
    """Test various operations on Hive-partitioned Parquet datasets.

    This test verifies that select_columns, rename_columns, filter, and with_column
    work correctly with Hive-partitioned datasets, including combinations of operations.
    All operations are tested without materializing to ensure projection pushdown works.
    """
    from ray.data.expressions import col

    partitioned_path, base_df = hive_partitioned_dataset

    # Define operations with their implementations for both pandas and Ray
    class ColumnTracker:
        """Helper to track column names as they get renamed."""

        def __init__(self, columns: list[str]) -> None:
            """Initialize tracker with column names.

            Args:
                columns: List of column names to track (identity mapping initially).
            """
            self.names: dict[str, str] = {col: col for col in columns}

        def __getitem__(self, key: str) -> str:
            return self.names[key]

        def rename(self, rename_map: dict[str, str]) -> None:
            """Update column names based on rename map."""
            self.names.update(rename_map)

    def _apply_rename(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
        base_rename_map: dict[str, str],
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply rename operation to pandas DataFrame or Ray Dataset."""
        rename_map = {cols[k]: v for k, v in base_rename_map.items()}
        cols.rename(rename_map)
        return (
            data.rename_columns(rename_map)
            if is_ray_ds
            else data.rename(columns=rename_map)
        )

    def _apply_select(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply select operation."""
        selected_cols = [
            cols["id"],
            cols["value"],
            cols["score"],
            cols["country"],
            cols["year"],
        ]
        return data.select_columns(selected_cols) if is_ray_ds else data[selected_cols]

    def _apply_select_partition_and_data(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply select partition and data operation (selects only country and id)."""
        selected_cols = [
            cols["country"],
            cols["id"],
        ]
        return data.select_columns(selected_cols) if is_ray_ds else data[selected_cols]

    def _apply_select_data_only(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply select data only operation (selects only data columns, no partition columns)."""
        selected_cols = [
            cols["id"],
            cols["value"],
            cols["score"],
        ]
        return data.select_columns(selected_cols) if is_ray_ds else data[selected_cols]

    def _apply_rename_partition(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply rename partition operation."""
        base_rename_map = {"country": "country_renamed", "year": "year_renamed"}
        return _apply_rename(data, cols, is_ray_ds, base_rename_map)

    def _apply_rename_data(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply rename data operation."""
        base_rename_map = {"id": "id_renamed", "value": "value_renamed"}
        return _apply_rename(data, cols, is_ray_ds, base_rename_map)

    def _apply_rename_partition_and_data(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply rename partition and data operation."""
        base_rename_map = {
            "country": "country_renamed",
            "year": "year_renamed",
            "id": "id_renamed",
            "value": "value_renamed",
        }
        return _apply_rename(data, cols, is_ray_ds, base_rename_map)

    def _apply_filter_partition(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply filter partition operation."""
        if is_ray_ds:
            return data.filter(expr=(col(cols["country"]) == "country_0"))
        else:
            return data[data[cols["country"]] == "country_0"]

    def _apply_filter_data(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply filter data operation."""
        if is_ray_ds:
            return data.filter(expr=(col(cols["score"]) >= 10))
        else:
            return data[data[cols["score"]] >= 10]

    def _apply_filter_partition_and_data(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply filter partition and data operation."""
        if is_ray_ds:
            return data.filter(
                expr=(col(cols["country"]) == "country_0") & (col(cols["score"]) >= 10)
            )
        else:
            return data[
                (data[cols["country"]] == "country_0") & (data[cols["score"]] >= 10)
            ]

    def _apply_with_column(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        cols: ColumnTracker,
        is_ray_ds: bool,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply with_column operation."""
        if is_ray_ds:
            return data.with_column("new_col", col(cols["score"]) * 2)
        else:
            data = data.copy()
            data["new_col"] = data[cols["score"]] * 2
            return data

    # Dispatch dictionary mapping operation names to their handlers
    op_handlers = {
        "select": _apply_select,
        "select_partition_and_data": _apply_select_partition_and_data,
        "select_data_only": _apply_select_data_only,
        "rename_partition": _apply_rename_partition,
        "rename_data": _apply_rename_data,
        "rename_partition_and_data": _apply_rename_partition_and_data,
        "filter_partition": _apply_filter_partition,
        "filter_data": _apply_filter_data,
        "filter_partition_and_data": _apply_filter_partition_and_data,
        "with_column": _apply_with_column,
    }

    def apply_operation(
        data: Union[pd.DataFrame, "ray.data.Dataset"],
        op: str,
        cols: ColumnTracker,
        is_ray_ds: bool = False,
    ) -> Union[pd.DataFrame, "ray.data.Dataset"]:
        """Apply a single operation to pandas DataFrame or Ray Dataset."""
        handler = op_handlers[op]
        return handler(data, cols, is_ray_ds)

    # Apply operations to pandas DataFrame for expected results
    expected_df = base_df.copy()
    expected_cols = ColumnTracker(list(base_df.columns))
    for op in operations:
        expected_df = apply_operation(expected_df, op, expected_cols, is_ray_ds=False)

    # Apply operations to Ray Dataset
    ds = ray.data.read_parquet(partitioned_path)
    ds_cols = ColumnTracker(list(base_df.columns))
    for op in operations:
        ds = apply_operation(ds, op, ds_cols, is_ray_ds=True)

    # Convert to pandas and normalize for comparison
    actual_df = ds.to_pandas()

    # Normalize column types (partition columns are strings in Parquet)
    for col_name in ["country", "country_renamed", "year", "year_renamed"]:
        if col_name in expected_df.columns:
            expected_df[col_name] = expected_df[col_name].astype(str)
        if col_name in actual_df.columns:
            actual_df[col_name] = actual_df[col_name].astype(str)

    # Sort both DataFrames for consistent comparison
    sort_cols = sorted(set(expected_df.columns) & set(actual_df.columns))
    expected_df = expected_df.sort_values(by=sort_cols).reset_index(drop=True)
    actual_df = actual_df.sort_values(by=sort_cols).reset_index(drop=True)

    # Ensure column order matches
    actual_df = actual_df[expected_df.columns]

    # Verify results match
    assert rows_same(actual_df, expected_df), (
        f"Operations {operations} produced different results.\n"
        f"Expected columns: {list(expected_df.columns)}\n"
        f"Actual columns: {list(actual_df.columns)}\n"
        f"Expected shape: {expected_df.shape}\n"
        f"Actual shape: {actual_df.shape}\n"
        f"Expected head:\n{expected_df.head()}\n"
        f"Actual head:\n{actual_df.head()}"
    )


@pytest.mark.parametrize("choice", ["default", "hive", "filename", "ray_default"])
@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="Hive partitioned parquet operations require pyarrow >= 14.0.0",
)
def test_write_parquet_partitioning(choice, tmp_path):

    # Ray's default is "hive", while pyarrow's default is "directory" (when None).
    kwargs = {
        "default": (
            {"partitioning_flavor": None},
            pds.partitioning(field_names=["grp"], flavor=None),
        ),
        "hive": ({"partitioning_flavor": "hive"}, pds.partitioning(flavor="hive")),
        "filename": (
            {"partitioning_flavor": "filename"},
            pds.partitioning(
                pa.schema([pa.field("grp", pa.int64())]), flavor="filename"
            ),
        ),
        "ray_default": (
            {},
            "hive",
        ),
    }

    parquet_kwargs, partitioning = kwargs[choice]

    ds = ray.data.range(1000).add_column("grp", lambda x: x["id"] % 10)

    ds.write_parquet(
        tmp_path,
        partition_cols=["grp"],
        **parquet_kwargs,
        mode="overwrite",
    )

    pq_ds = pq.ParquetDataset(
        tmp_path,
        partitioning=partitioning,
    )
    df = pq_ds.read_pandas().to_pandas()

    assert len(df) == 1000
    assert df["grp"].nunique() == 10
    assert set(df.columns.tolist()) == {"id", "grp"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
