from unittest import mock

import pyarrow
import pytest
from fsspec.implementations.http import HTTPFileSystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from ray.data.datasource.path_util import (
    _has_file_extension,
    _is_local_windows_path,
    _resolve_paths_and_filesystem,
)


@pytest.mark.parametrize(
    "path, extensions, has_extension",
    [
        ("foo.csv", ["csv"], True),
        ("foo.csv", ["json", "csv"], True),
        ("foo.csv", ["json", "jsonl"], False),
        ("foo.csv", [".csv"], True),
        ("foo.parquet.crc", ["parquet"], False),
        ("foo.parquet.crc", ["crc"], True),
        ("foo.csv", None, True),
    ],
)
def test_has_file_extension(path, extensions, has_extension):
    assert _has_file_extension(path, extensions) == has_extension


@pytest.mark.parametrize(
    "filesystem", [None, PyFileSystem(FSSpecHandler(HTTPFileSystem()))]
)
def test_resolve_http_paths(filesystem):
    resolved_paths, resolve_filesystem = _resolve_paths_and_filesystem(
        "https://google.com", filesystem
    )
    # `_resolve_paths_and_filesystem` shouldn't remove the protocol/scheme from the
    # path for HTTP paths.
    assert resolved_paths == ["https://google.com"]
    assert isinstance(resolve_filesystem, pyarrow.fs.PyFileSystem)
    assert isinstance(resolve_filesystem.handler, pyarrow.fs.FSSpecHandler)
    assert isinstance(resolve_filesystem.handler.fs, HTTPFileSystem)


@pytest.mark.parametrize(
    "path",
    [
        "c:/some/where",
        "c:\\some\\where",
        "c:\\some\\where/mixed",
    ],
)
def test_windows_path(path):
    with mock.patch("sys.platform", "win32"):
        assert _is_local_windows_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "some/file",
        "some/file;semicolon",
        "some/file?questionmark",
        "some/file#hash",
        "some/file;all?of the#above",
    ],
)
def test_weird_local_paths(path):
    resolved_paths, _ = _resolve_paths_and_filesystem(path)
    assert resolved_paths[0] == path


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
