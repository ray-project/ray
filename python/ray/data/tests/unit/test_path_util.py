from unittest import mock

import pyarrow
import pytest
from fsspec.implementations.http import HTTPFileSystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

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
    """Tests for _is_filesystem_compatible_with_scheme with fsspec-wrapped filesystems."""

    def _make_mock_fsspec_fs(self, protocol):
        """Create a PyFileSystem wrapping a mock fsspec filesystem with the given protocol."""
        mock_fsspec_fs = mock.MagicMock()
        mock_fsspec_fs.protocol = protocol
        return PyFileSystem(FSSpecHandler(mock_fsspec_fs))

    def test_native_s3_filesystem(self):
        """Native PyArrow S3FileSystem should be compatible with s3 scheme."""
        mock_fs = mock.MagicMock()
        mock_fs.type_name = "s3"
        assert _is_filesystem_compatible_with_scheme(mock_fs, "s3") is True
        assert _is_filesystem_compatible_with_scheme(mock_fs, "") is False

    def test_native_local_filesystem(self):
        """Native PyArrow LocalFileSystem should be compatible with empty scheme."""
        mock_fs = mock.MagicMock()
        mock_fs.type_name = "local"
        assert _is_filesystem_compatible_with_scheme(mock_fs, "") is True
        assert _is_filesystem_compatible_with_scheme(mock_fs, "s3") is False

    def test_fsspec_s3_filesystem_with_s3_scheme(self):
        """fsspec S3FileSystem wrapped in PyFileSystem should be compatible with s3 scheme."""
        wrapped_fs = self._make_mock_fsspec_fs("s3")
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "s3") is True

    def test_fsspec_s3_filesystem_with_bare_paths(self):
        """fsspec S3FileSystem wrapped in PyFileSystem should be compatible with bare paths."""
        wrapped_fs = self._make_mock_fsspec_fs("s3")
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "") is True

    def test_fsspec_s3_filesystem_with_s3a_scheme(self):
        """fsspec S3FileSystem with tuple protocol should match s3a scheme."""
        wrapped_fs = self._make_mock_fsspec_fs(("s3", "s3a"))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "s3a") is True

    def test_fsspec_gcs_filesystem_with_gs_scheme(self):
        """fsspec GCS filesystem should be compatible with gs scheme."""
        wrapped_fs = self._make_mock_fsspec_fs("gs")
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gs") is True

    def test_fsspec_gcs_filesystem_with_gcs_scheme(self):
        """fsspec GCS filesystem with tuple protocol should match gcs scheme."""
        wrapped_fs = self._make_mock_fsspec_fs(("gs", "gcs"))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gcs") is True

    def test_fsspec_http_filesystem(self):
        """fsspec HTTPFileSystem wrapped in PyFileSystem should be compatible with http."""
        wrapped_fs = PyFileSystem(FSSpecHandler(HTTPFileSystem()))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "http") is True
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "https") is True

    def test_fsspec_s3_not_compatible_with_gs(self):
        """fsspec S3FileSystem should NOT be compatible with gs scheme."""
        wrapped_fs = self._make_mock_fsspec_fs("s3")
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gs") is False

    def test_unknown_scheme_trusts_filesystem(self):
        """Unknown schemes should always return True (trust user's filesystem)."""
        wrapped_fs = self._make_mock_fsspec_fs("s3")
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "custom") is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
