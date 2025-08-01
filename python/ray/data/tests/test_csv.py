import itertools
import os
import shutil
from functools import partial

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging.version import Version
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.data import Schema
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
    PartitionStyle,
    PathPartitionFilter,
)
from ray.data.datasource.file_based_datasource import (
    FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD,
)
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.test_partitioning import PathPartitionEncoder
from ray.tests.conftest import *  # noqa


def df_to_csv(dataframe, path, **kwargs):
    dataframe.to_csv(path, **kwargs)


def test_csv_read_partitioning(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "country=us", "file.csv")
    os.mkdir(os.path.dirname(path))
    df = pd.DataFrame({"numbers": [1, 2, 3], "letters": ["a", "b", "c"]})
    df.to_csv(path, index=False)

    ds = ray.data.read_csv(path)

    assert ds.take() == [
        {"numbers": 1, "letters": "a", "country": "us"},
        {"numbers": 2, "letters": "b", "country": "us"},
        {"numbers": 3, "letters": "c", "country": "us"},
    ]


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
def test_csv_read(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    target_max_block_size_infinite_or_default,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(path1, filesystem=fs, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))

    # Two files, override_num_blocks=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "test2.csv")
    df2.to_csv(path2, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(
        [path1, path2], override_num_blocks=2, filesystem=fs, partitioning=None
    )
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    df = pd.concat([df1, df2], ignore_index=True)
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._plan.execute().blocks:
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, override_num_blocks=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(data_path, "test3.csv")
    df3.to_csv(path3, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(
        [path1, path2, path3],
        override_num_blocks=2,
        filesystem=fs,
        partitioning=None,
    )
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
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
    ds = ray.data.read_csv(path, filesystem=fs, partitioning=None)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(df, dsdf)
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
    ds = ray.data.read_csv([path1, path2], filesystem=fs, partitioning=None)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
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
    ds = ray.data.read_csv([dir_path, path2], filesystem=fs, partitioning=None)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(dir_path)
    else:
        fs.delete_dir(_unwrap_protocol(dir_path))

    # Directory, two files and non-csv file (test extension-based path filtering).
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

    # Add a file with a non-matching file extension. This file should be ignored.
    df_txt = pd.DataFrame({"foobar": [1, 2, 3]})
    df_txt.to_json(
        os.path.join(path, "foo.txt"),
        storage_options=storage_options,
    )

    ds = ray.data.read_csv(
        path,
        filesystem=fs,
        file_extensions=["csv"],
        partitioning=None,
    )
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df.equals(dsdf)
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
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))

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
def test_csv_read_many_files_basic(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    paths = []
    dfs = []
    num_dfs = 4 * FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD
    for i in range(num_dfs):
        df = pd.DataFrame({"one": list(range(i * 3, (i + 1) * 3))})
        dfs.append(df)
        path = os.path.join(data_path, f"test_{i}.csv")
        paths.append(path)
        df.to_csv(path, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(paths, filesystem=fs)

    dsdf = ds.to_pandas()
    df = pd.concat(dfs).reset_index(drop=True)
    pd.testing.assert_frame_equal(df, dsdf)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_read_many_files_partitioned(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    write_partitioned_df,
    assert_base_partitioned_ds,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    partition_keys = ["one"]
    partition_path_encoder = PathPartitionEncoder.of(
        base_dir=data_path,
        field_names=partition_keys,
        filesystem=fs,
    )
    paths = []
    dfs = []
    num_dfs = FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD
    num_rows = 6 * num_dfs
    num_files = 2 * num_dfs
    for i in range(num_dfs):
        df = pd.DataFrame(
            {"one": [1, 1, 1, 3, 3, 3], "two": list(range(6 * i, 6 * (i + 1)))}
        )
        df_paths = write_partitioned_df(
            df,
            partition_keys,
            partition_path_encoder,
            partial(df_to_csv, storage_options=storage_options, index=False),
            file_name_suffix=i,
        )
        dfs.append(df)
        paths.extend(df_paths)

    ds = ray.data.read_csv(
        paths,
        filesystem=fs,
        partitioning=partition_path_encoder.scheme,
        override_num_blocks=num_files,
    )

    assert_base_partitioned_ds(
        ds,
        count=num_rows,
        num_input_files=num_files,
        schema=Schema(pa.schema([("one", pa.int64()), ("two", pa.int64())])),
        sorted_values=sorted(
            itertools.chain.from_iterable(
                list(
                    map(list, zip([1, 1, 1, 3, 3, 3], list(range(6 * i, 6 * (i + 1)))))
                )
                for i in range(num_dfs)
            )
        ),
    )


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_csv_read_many_files_diff_dirs(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    dir1 = os.path.join(data_path, "dir1")
    dir2 = os.path.join(data_path, "dir2")
    if fs is None:
        os.mkdir(dir1)
        os.mkdir(dir2)
    else:
        fs.create_dir(_unwrap_protocol(dir1))
        fs.create_dir(_unwrap_protocol(dir2))

    paths = []
    dfs = []
    num_dfs = 2 * FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD
    for i, dir_path in enumerate([dir1, dir2]):
        for j in range(num_dfs * i, num_dfs * (i + 1)):
            df = pd.DataFrame({"one": list(range(3 * j, 3 * (j + 1)))})
            dfs.append(df)
            path = os.path.join(dir_path, f"test_{j}.csv")
            paths.append(path)
            df.to_csv(path, index=False, storage_options=storage_options)
    ds = ray.data.read_csv([dir1, dir2], filesystem=fs)

    dsdf = ds.to_pandas().sort_values(by=["one"]).reset_index(drop=True)
    df = pd.concat(dfs).reset_index(drop=True)
    pd.testing.assert_frame_equal(df, dsdf)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
            lazy_fixture("s3_server"),
        ),
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
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
            lazy_fixture("s3_server"),
        ),
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
@pytest.mark.parametrize("style", [PartitionStyle.HIVE, PartitionStyle.DIRECTORY])
def test_csv_read_partitioned_with_filter(
    style,
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

    def skip_unpartitioned(kv_dict):
        return bool(kv_dict)

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


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
@pytest.mark.parametrize("style", [PartitionStyle.HIVE, PartitionStyle.DIRECTORY])
def test_csv_read_partitioned_with_filter_multikey(
    style,
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

    def keep_expected_partitions(kv_dict):
        keep = bool(kv_dict) and (
            (kv_dict["one"] == "1" and kv_dict["two"] in {"a", "b", "c"})
            or (kv_dict["one"] == "3" and kv_dict["two"] in {"e", "f", "g"})
        )
        return keep

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
    file_writer_fn(df, os.path.join(data_path, "test0.csv"))
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
        override_num_blocks=6,
    )
    assert_base_partitioned_ds(ds, num_input_files=6)


def test_csv_write(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    input_df = pd.DataFrame({"id": [0]})
    ds = ray.data.from_blocks([input_df])

    ds.write_csv(tmp_path)

    output_df = pd.concat(
        [
            pd.read_csv(os.path.join(tmp_path, filename))
            for filename in os.listdir(tmp_path)
        ]
    )
    assert rows_same(input_df, output_df)


@pytest.mark.parametrize("override_num_blocks", [None, 2])
def test_csv_roundtrip(
    ray_start_regular_shared,
    tmp_path,
    override_num_blocks,
    target_max_block_size_infinite_or_default,
):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    ds = ray.data.from_pandas([df], override_num_blocks=override_num_blocks)
    ds.write_csv(tmp_path)

    ds2 = ray.data.read_csv(tmp_path)
    ds2df = ds2.to_pandas()
    assert rows_same(ds2df, df)
    for block, meta in ds2._plan.execute().blocks:
        assert BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


def test_csv_read_filter_non_csv_file(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    # CSV file with .csv extension.
    path1 = os.path.join(tmp_path, "test2.csv")
    df.to_csv(path1, index=False)

    # CSV file without .csv extension.
    path2 = os.path.join(tmp_path, "test3")
    df.to_csv(path2, index=False)

    # Directory of CSV files.
    ds = ray.data.read_csv(tmp_path)
    actual_data = sorted(ds.to_pandas().itertuples(index=False))
    expected_data = sorted(pd.concat([df, df]).itertuples(index=False))
    assert actual_data == expected_data, (actual_data, expected_data)

    # Non-CSV file in Parquet format.
    table = pa.Table.from_pandas(df)
    path3 = os.path.join(tmp_path, "test1.parquet")
    pq.write_table(table, path3)

    # Single non-CSV file.
    error_message = "Failed to read CSV file"
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(path3).materialize()

    # Single CSV file without extension.
    ds = ray.data.read_csv(path2)
    assert ds.to_pandas().equals(df)

    # Directory of CSV and non-CSV files.
    error_message = "Failed to read CSV file"
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(tmp_path).materialize()

    # Directory of CSV and non-CSV files with filter.
    ds = ray.data.read_csv(tmp_path, file_extensions=["csv"])
    assert ds.to_pandas().equals(df)


def test_csv_read_no_header(ray_start_regular_shared, tmp_path):
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


def test_csv_read_with_column_type_specified(ray_start_regular_shared, tmp_path):
    from pyarrow import csv

    file_path = os.path.join(tmp_path, "test.csv")
    df = pd.DataFrame({"one": [1, 2, 3e1], "two": ["a", "b", "c"]})
    df.to_csv(file_path, index=False)

    # Incorrect to parse scientific notation in int64 as PyArrow represents
    # it as double.
    with pytest.raises(ValueError):
        ray.data.read_csv(
            file_path,
            convert_options=csv.ConvertOptions(
                column_types={"one": "int64", "two": "string"}
            ),
        ).schema()

    # Parsing scientific notation in double should work.
    ds = ray.data.read_csv(
        file_path,
        convert_options=csv.ConvertOptions(
            column_types={"one": "float64", "two": "string"}
        ),
    )
    expected_df = pd.DataFrame({"one": [1.0, 2.0, 30.0], "two": ["a", "b", "c"]})
    assert ds.to_pandas().equals(expected_df)


@pytest.mark.skipif(
    Version(pa.__version__) < Version("7.0.0"),
    reason="invalid_row_handler was added in pyarrow 7.0.0",
)
def test_csv_invalid_file_handler(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    from pyarrow import csv

    invalid_txt = "f1,f2\n2,3\nx\n4,5"
    invalid_file = os.path.join(tmp_path, "invalid.csv")
    with open(invalid_file, "wt") as f:
        f.write(invalid_txt)

    ray.data.read_csv(
        invalid_file,
        parse_options=csv.ParseOptions(
            delimiter=",", invalid_row_handler=lambda i: "skip"
        ),
    )


@pytest.mark.parametrize("min_rows_per_file", [5, 10, 50])
def test_write_min_rows_per_file(
    tmp_path,
    ray_start_regular_shared,
    min_rows_per_file,
    target_max_block_size_infinite_or_default,
):
    ray.data.range(100, override_num_blocks=20).write_csv(
        tmp_path, min_rows_per_file=min_rows_per_file
    )

    for filename in os.listdir(tmp_path):
        with open(os.path.join(tmp_path, filename), "r") as file:
            # Subtract 1 from the number of lines to account for the header.
            num_rows_written = len(file.read().splitlines()) - 1
            assert num_rows_written == min_rows_per_file


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
