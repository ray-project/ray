from unittest import mock

import pyarrow
import pyarrow.fs
import pytest
from fsspec.implementations.http import HTTPFileSystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from ray.data._internal.util import RetryingPyFileSystem
from ray.data.datasource.path_util import (
    _has_file_extension,
    _is_filesystem_compatible_with_scheme,
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
        ("s3://bucket/foo.parquet?versionId=abc123", ["parquet"], True),
        ("bucket/foo.parquet?versionId=abc123", ["parquet"], True),
        ("s3://bucket/foo.parquet?versionId=abc123", ["csv"], False),
        ("s3://bucket/data#v2/file.parquet", ["parquet"], True),
        ("C:\\data\\test.parquet", ["parquet"], True),
        ("C:\\data\\parquet", ["parquet"], False),
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


class TestIsFilesystemCompatibleWithScheme:
    """Tests for _is_filesystem_compatible_with_scheme with real filesystem implementations."""

    def test_native_local_filesystem(self):
        """Native PyArrow LocalFileSystem should be compatible with empty scheme."""
        fs = pyarrow.fs.LocalFileSystem()
        assert _is_filesystem_compatible_with_scheme(fs, "") is True
        assert _is_filesystem_compatible_with_scheme(fs, "s3") is False

    def test_native_s3_filesystem(self):
        """Native PyArrow S3FileSystem should be compatible with s3 scheme."""
        fs = pyarrow.fs.S3FileSystem(anonymous=True, region="us-east-1")
        assert _is_filesystem_compatible_with_scheme(fs, "s3") is True
        assert _is_filesystem_compatible_with_scheme(fs, "") is False

    def test_fsspec_s3_filesystem_with_s3_scheme(self):
        """fsspec S3FileSystem wrapped in PyFileSystem should be compatible with s3 scheme."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "s3") is True

    def test_fsspec_s3_filesystem_with_bare_paths(self):
        """fsspec S3FileSystem wrapped in PyFileSystem should be compatible with bare paths."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "") is True

    def test_fsspec_s3_filesystem_with_s3a_scheme(self):
        """Real s3fs has protocol=('s3', 's3a'), so it should match s3a scheme."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "s3a") is True

    def test_fsspec_gcs_filesystem_with_gs_scheme(self):
        """fsspec GCS filesystem should be compatible with gs scheme."""
        gcsfs = pytest.importorskip("gcsfs")
        wrapped_fs = PyFileSystem(FSSpecHandler(gcsfs.GCSFileSystem(token="anon")))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gs") is True

    def test_fsspec_gcs_filesystem_with_gcs_scheme(self):
        """Real gcsfs has protocol=('gcs', 'gs'), so it should match gcs scheme."""
        gcsfs = pytest.importorskip("gcsfs")
        wrapped_fs = PyFileSystem(FSSpecHandler(gcsfs.GCSFileSystem(token="anon")))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gcs") is True

    def test_fsspec_http_filesystem(self):
        """fsspec HTTPFileSystem wrapped in PyFileSystem should be compatible with http."""
        wrapped_fs = PyFileSystem(FSSpecHandler(HTTPFileSystem()))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "http") is True
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "https") is True

    def test_fsspec_s3_not_compatible_with_gs(self):
        """fsspec S3FileSystem should NOT be compatible with gs scheme."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gs") is False

    def test_fsspec_s3_not_compatible_with_http(self):
        """fsspec S3FileSystem should NOT be compatible with http/https schemes."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "http") is False
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "https") is False

    def test_retrying_wrapper_around_native_s3(self):
        """RetryingPyFileSystem wrapping a native S3FileSystem should be compatible with s3."""
        s3_fs = pyarrow.fs.S3FileSystem(anonymous=True)
        retrying_fs = RetryingPyFileSystem.wrap(s3_fs, retryable_errors=["AWS Error"])
        assert _is_filesystem_compatible_with_scheme(retrying_fs, "s3") is True
        assert _is_filesystem_compatible_with_scheme(retrying_fs, "gs") is False

    def test_unknown_scheme_trusts_filesystem(self):
        """Unknown schemes should always return True (trust user's filesystem)."""
        fs = pyarrow.fs.LocalFileSystem()
        assert _is_filesystem_compatible_with_scheme(fs, "custom") is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
