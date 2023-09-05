import logging
import os
import posixpath
import urllib.parse
from functools import partial
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem
from pytest_lazyfixture import lazy_fixture

from ray.data.datasource import (
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
    FileMetadataProvider,
    ParquetMetadataProvider,
)
from ray.data.datasource.file_based_datasource import (
    FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD,
    _resolve_paths_and_filesystem,
    _unwrap_protocol,
)
from ray.data.datasource.file_meta_provider import (
    _get_file_infos_common_path_prefix,
    _get_file_infos_parallel,
    _get_file_infos_serial,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_partitioning import PathPartitionEncoder
from ray.tests.conftest import *  # noqa


def df_to_csv(dataframe, path, **kwargs):
    dataframe.to_csv(path, **kwargs)


def _get_parquet_file_meta_size_bytes(file_metas):
    return sum(
        sum(m.row_group(i).total_byte_size for i in range(m.num_row_groups))
        for m in file_metas
    )


def _get_file_sizes_bytes(paths, fs):
    from pyarrow.fs import FileType

    file_sizes = []
    for path in paths:
        file_info = fs.get_file_info(path)
        if file_info.type == FileType.File:
            file_sizes.append(file_info.size)
        else:
            raise FileNotFoundError(path)
    return file_sizes


def test_file_metadata_providers_not_implemented():
    meta_provider = FileMetadataProvider()
    with pytest.raises(NotImplementedError):
        meta_provider(["/foo/bar.csv"], None)
    meta_provider = BaseFileMetadataProvider()
    with pytest.raises(NotImplementedError):
        meta_provider(["/foo/bar.csv"], None, rows_per_file=None, file_sizes=[None])
    with pytest.raises(NotImplementedError):
        meta_provider.expand_paths(["/foo/bar.csv"], None)
    meta_provider = ParquetMetadataProvider()
    with pytest.raises(NotImplementedError):
        meta_provider(["/foo/bar.csv"], None, num_pieces=0, prefetched_metadata=None)
    assert meta_provider.prefetch_file_metadata(["test"]) is None


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
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
        ),
    ],
)
def test_default_parquet_metadata_provider(fs, data_path):
    path_module = os.path if urllib.parse.urlparse(data_path).scheme else posixpath
    paths = [
        path_module.join(data_path, "test1.parquet"),
        path_module.join(data_path, "test2.parquet"),
    ]
    paths, fs = _resolve_paths_and_filesystem(paths, fs)

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    pq.write_table(table, paths[0], filesystem=fs)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    pq.write_table(table, paths[1], filesystem=fs)

    meta_provider = DefaultParquetMetadataProvider()
    pq_ds = pq.ParquetDataset(paths, filesystem=fs, use_legacy_dataset=False)
    file_metas = meta_provider.prefetch_file_metadata(pq_ds.pieces)

    meta = meta_provider(
        [p.path for p in pq_ds.pieces],
        pq_ds.schema,
        num_pieces=len(pq_ds.pieces),
        prefetched_metadata=file_metas,
    )
    expected_meta_size_bytes = _get_parquet_file_meta_size_bytes(file_metas)
    assert meta.size_bytes == expected_meta_size_bytes
    assert meta.num_rows == 6
    assert len(paths) == 2
    assert all(path in meta.input_files for path in paths)
    assert meta.schema.equals(pq_ds.schema)


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
        ),  # Path contains space.
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
            lazy_fixture("s3_server"),
        ),
    ],
)
def test_default_file_metadata_provider(
    propagate_logs, caplog, fs, data_path, endpoint_url
):
    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )

    path_module = os.path if urllib.parse.urlparse(data_path).scheme else posixpath
    path1 = path_module.join(data_path, "test1.csv")
    path2 = path_module.join(data_path, "test2.csv")
    paths = [path1, path2]
    paths, fs = _resolve_paths_and_filesystem(paths, fs)

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df1.to_csv(path1, index=False, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df2.to_csv(path2, index=False, storage_options=storage_options)

    meta_provider = DefaultFileMetadataProvider()
    with caplog.at_level(logging.WARNING), patch(
        "ray.data.datasource.file_meta_provider._get_file_infos_serial",
        wraps=_get_file_infos_serial,
    ) as mock_get:
        file_paths, file_sizes = map(list, zip(*meta_provider.expand_paths(paths, fs)))
    mock_get.assert_called_once_with(paths, fs, False)
    # No warning should be logged.
    assert len(caplog.text) == 0
    assert file_paths == paths
    expected_file_sizes = _get_file_sizes_bytes(paths, fs)
    assert file_sizes == expected_file_sizes

    meta = meta_provider(
        paths,
        None,
        rows_per_file=3,
        file_sizes=file_sizes,
    )
    assert meta.size_bytes == sum(expected_file_sizes)
    assert meta.num_rows == 6
    assert len(paths) == 2
    assert all(path in meta.input_files for path in paths)
    assert meta.schema is None


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_default_metadata_provider_ignore_missing(fs, data_path, endpoint_url):
    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )

    path1 = os.path.join(data_path, "test1.csv")
    path2 = os.path.join(data_path, "test2.csv")
    paths = [path1, path2]
    paths_with_missing = paths + [os.path.join(data_path, "missing.csv")]
    paths_with_missing, fs = _resolve_paths_and_filesystem(paths_with_missing, fs)

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df1.to_csv(path1, index=False, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df2.to_csv(path2, index=False, storage_options=storage_options)

    meta_provider = DefaultFileMetadataProvider()
    file_paths, _ = map(
        list,
        zip(
            *meta_provider.expand_paths(
                paths_with_missing, fs, ignore_missing_paths=True
            )
        ),
    )

    assert len(file_paths) == 2


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_default_file_metadata_provider_many_files_basic(
    propagate_logs,
    caplog,
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
        if i % 4 == 0:
            # Append same path multiple times to test duplicated paths.
            paths.extend([path, path, path])
        else:
            paths.append(path)
        df.to_csv(path, index=False, storage_options=storage_options)
    paths, fs = _resolve_paths_and_filesystem(paths, fs)

    meta_provider = DefaultFileMetadataProvider()
    if isinstance(fs, LocalFileSystem):
        patcher = patch(
            "ray.data.datasource.file_meta_provider._get_file_infos_serial",
            wraps=_get_file_infos_serial,
        )
    else:
        patcher = patch(
            "ray.data.datasource.file_meta_provider._get_file_infos_common_path_prefix",
            wraps=_get_file_infos_common_path_prefix,
        )
    with caplog.at_level(logging.WARNING), patcher as mock_get:
        file_paths, file_sizes = map(list, zip(*meta_provider.expand_paths(paths, fs)))
    if isinstance(fs, LocalFileSystem):
        mock_get.assert_called_once_with(paths, fs, False)
    else:
        mock_get.assert_called_once_with(paths, _unwrap_protocol(data_path), fs, False)
    # No warning should be logged.
    assert len(caplog.text) == 0
    assert file_paths == paths
    expected_file_sizes = _get_file_sizes_bytes(paths, fs)
    assert file_sizes == expected_file_sizes


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_default_file_metadata_provider_many_files_partitioned(
    propagate_logs,
    caplog,
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
    paths, fs = _resolve_paths_and_filesystem(paths, fs)
    partitioning = partition_path_encoder.scheme

    meta_provider = DefaultFileMetadataProvider()
    if isinstance(fs, LocalFileSystem):
        patcher = patch(
            "ray.data.datasource.file_meta_provider._get_file_infos_serial",
            wraps=_get_file_infos_serial,
        )
    else:
        patcher = patch(
            "ray.data.datasource.file_meta_provider._get_file_infos_common_path_prefix",
            wraps=_get_file_infos_common_path_prefix,
        )
    with caplog.at_level(logging.WARNING), patcher as mock_get:
        file_paths, file_sizes = map(
            list, zip(*meta_provider.expand_paths(paths, fs, partitioning))
        )
    if isinstance(fs, LocalFileSystem):
        mock_get.assert_called_once_with(paths, fs, False)
    else:
        mock_get.assert_called_once_with(
            paths, _unwrap_protocol(partitioning.base_dir), fs, False
        )
    assert len(caplog.text) == 0
    assert file_paths == paths
    expected_file_sizes = _get_file_sizes_bytes(paths, fs)
    assert file_sizes == expected_file_sizes


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_default_file_metadata_provider_many_files_diff_dirs(
    ray_start_regular,
    propagate_logs,
    caplog,
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
    paths, fs = _resolve_paths_and_filesystem(paths, fs)

    meta_provider = DefaultFileMetadataProvider()
    if isinstance(fs, LocalFileSystem):
        patcher = patch(
            "ray.data.datasource.file_meta_provider._get_file_infos_serial",
            wraps=_get_file_infos_serial,
        )
    else:
        patcher = patch(
            "ray.data.datasource.file_meta_provider._get_file_infos_parallel",
            wraps=_get_file_infos_parallel,
        )
    with caplog.at_level(logging.WARNING), patcher as mock_get:
        file_paths, file_sizes = map(list, zip(*meta_provider.expand_paths(paths, fs)))

    mock_get.assert_called_once_with(paths, fs, False)
    if isinstance(fs, LocalFileSystem):
        # No warning should be logged.
        assert len(caplog.text) == 0
    else:
        # Many files with different directories on cloud storage should log warning.
        assert "common parent directory" in caplog.text
    assert file_paths == paths
    expected_file_sizes = _get_file_sizes_bytes(paths, fs)
    assert file_sizes == expected_file_sizes


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
        ),  # Path contains space.
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
            lazy_fixture("s3_server"),
        ),
    ],
)
def test_fast_file_metadata_provider(
    propagate_logs, caplog, fs, data_path, endpoint_url
):
    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )

    path_module = os.path if urllib.parse.urlparse(data_path).scheme else posixpath
    path1 = path_module.join(data_path, "test1.csv")
    path2 = path_module.join(data_path, "test2.csv")
    paths = [path1, path2]
    paths, fs = _resolve_paths_and_filesystem(paths, fs)

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df1.to_csv(path1, index=False, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df2.to_csv(path2, index=False, storage_options=storage_options)

    meta_provider = FastFileMetadataProvider()
    with caplog.at_level(logging.WARNING):
        file_paths, file_sizes = map(list, zip(*meta_provider.expand_paths(paths, fs)))
    assert "meta_provider=DefaultFileMetadataProvider()" in caplog.text
    assert file_paths == paths
    assert len(file_sizes) == len(file_paths)

    meta = meta_provider(
        paths,
        None,
        rows_per_file=3,
        file_sizes=file_sizes,
    )
    assert meta.size_bytes is None
    assert meta.num_rows == 6
    assert len(paths) == 2
    assert all(path in meta.input_files for path in paths)
    assert meta.schema is None


def test_fast_file_metadata_provider_ignore_missing():
    meta_provider = FastFileMetadataProvider()
    with pytest.raises(ValueError):
        paths = meta_provider.expand_paths([], None, ignore_missing_paths=True)
        for _ in paths:
            pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
