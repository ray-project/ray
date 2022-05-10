import pytest
import posixpath
import urllib.parse
import os
import logging

import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from pytest_lazyfixture import lazy_fixture
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem

from ray.tests.conftest import *  # noqa
from ray.data.datasource import (
    FileMetadataProvider,
    BaseFileMetadataProvider,
    ParquetMetadataProvider,
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
)

from ray.data.tests.conftest import *  # noqa


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
        meta_provider(["/foo/bar.csv"], None, pieces=[], prefetched_metadata=None)
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
        pieces=pq_ds.pieces,
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
def test_default_file_metadata_provider(caplog, fs, data_path, endpoint_url):
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
    with caplog.at_level(logging.WARNING):
        file_paths, file_sizes = meta_provider.expand_paths(paths, fs)
    assert "meta_provider=FastFileMetadataProvider()" in caplog.text
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
def test_fast_file_metadata_provider(caplog, fs, data_path, endpoint_url):
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
        file_paths, file_sizes = meta_provider.expand_paths(paths, fs)
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
