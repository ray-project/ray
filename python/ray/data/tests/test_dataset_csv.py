import os
import shutil
from functools import partial
from distutils.version import LooseVersion

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pytest_lazyfixture import lazy_fixture

import ray
from ray.data.tests.util import Counter
from ray.data.block import BlockAccessor
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
    PartitionStyle,
    PathPartitionEncoder,
    PathPartitionFilter,
)
from ray.data.datasource.file_based_datasource import (
    FileExtensionFilter,
    _unwrap_protocol,
)


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
def test_csv_read(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options=storage_options)
    ds = ray.data.read_csv(path1, filesystem=fs, partitioning=None)
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
    ds = ray.data.read_csv(
        [path1, path2], parallelism=2, filesystem=fs, partitioning=None
    )
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
    ds = ray.data.read_csv(
        [path1, path2, path3], parallelism=2, filesystem=fs, partitioning=None
    )
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
    ds = ray.data.read_csv(path, filesystem=fs, partitioning=None)
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
    ds = ray.data.read_csv([path1, path2], filesystem=fs, partitioning=None)
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
    ds = ray.data.read_csv([dir_path, path2], filesystem=fs, partitioning=None)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
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
        partition_filter=FileExtensionFilter("csv"),
        partitioning=None,
    )
    assert ds.num_blocks() == 2
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
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
            parallelism=100,
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
    # NOTE: This is shutdown_only because this is the last use of the shared local
    # cluster; following this, we start a new cluster, so we need to shut this one down.
    # If the ordering of these tests change, then this needs to change.
    shutdown_only,
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
    with pytest.raises(ValueError):
        ray.data.read_csv(
            file_path,
            convert_options=csv.ConvertOptions(
                column_types={"one": "int64", "two": "string"}
            ),
        )

    # Parsing scientific notation in double should work.
    ds = ray.data.read_csv(
        file_path,
        convert_options=csv.ConvertOptions(
            column_types={"one": "float64", "two": "string"}
        ),
    )
    expected_df = pd.DataFrame({"one": [1.0, 2.0, 30.0], "two": ["a", "b", "c"]})
    assert ds.to_pandas().equals(expected_df)


def test_csv_read_filter_non_csv_file(shutdown_only, tmp_path):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    # CSV file with .csv extension.
    path1 = os.path.join(tmp_path, "test2.csv")
    df.to_csv(path1, index=False)

    # CSV file without .csv extension.
    path2 = os.path.join(tmp_path, "test3")
    df.to_csv(path2, index=False)

    # Directory of CSV files.
    ds = ray.data.read_csv(tmp_path)
    assert ds.to_pandas().equals(pd.concat([df, df], ignore_index=True))

    # Non-CSV file in Parquet format.
    table = pa.Table.from_pandas(df)
    path3 = os.path.join(tmp_path, "test1.parquet")
    pq.write_table(table, path3)

    # Single non-CSV file.
    error_message = "Failed to read CSV file"
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(path3)

    # Single non-CSV file with filter.
    error_message = "No input files found to read"
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(path3, partition_filter=FileExtensionFilter("csv"))

    # Single CSV file without extension.
    ds = ray.data.read_csv(path2)
    assert ds.to_pandas().equals(df)

    # Single CSV file without extension with filter.
    error_message = "No input files found to read"
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(path2, partition_filter=FileExtensionFilter("csv"))

    # Directory of CSV and non-CSV files.
    error_message = "Failed to read CSV file"
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(tmp_path)

    # Directory of CSV and non-CSV files with filter.
    ds = ray.data.read_csv(tmp_path, partition_filter=FileExtensionFilter("csv"))
    assert ds.to_pandas().equals(df)


@pytest.mark.skipif(
    LooseVersion(pa.__version__) < LooseVersion("7.0.0"),
    reason="invalid_row_handler was added in pyarrow 7.0.0",
)
def test_csv_invalid_file_handler(shutdown_only, tmp_path):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
