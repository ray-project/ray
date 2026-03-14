"""Unit and integration tests for plan_download_op internals.

Tests are organized into three groups:
  - TestSplitUri: pure-Python unit tests for _split_uri (no Ray, no obstore).
  - TestDownloadHelpers: unit tests for credential extraction and URL scheme
    detection (no Ray, no obstore; obstore_parse_scheme is mocked where needed).
  - TestObstoreDownloadPath: integration tests for the obstore async download
    path. All tests in this class are skipped when obstore is not installed.
"""
import asyncio
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

from ray.data._internal.planner.plan_download_op import (
    _download_uris_with_obstore,
    _download_uris_with_pyarrow,
    _extract_credentials_from_filesystem,
    _is_obstore_supported_url,
    download_bytes_threaded,
)
from ray.data._internal.util import RetryingPyFileSystem
from ray.data.context import DataContext
from ray.data.datasource.path_util import _split_uri


class TestSplitUri:
    """Tests for _split_uri, which splits a URI into (store_url, path)."""

    def test_s3_uri(self):
        store_url, path = _split_uri("s3://my-bucket/prefix/key.jpg")
        assert store_url == "s3://my-bucket"
        assert path == "prefix/key.jpg"

    def test_https_uri(self):
        store_url, path = _split_uri("https://host.com/a/b/c.png")
        assert store_url == "https://host.com"
        assert path == "a/b/c.png"

    def test_gs_uri(self):
        store_url, path = _split_uri("gs://bucket/path/to/file")
        assert store_url == "gs://bucket"
        assert path == "path/to/file"

    def test_file_uri(self):
        # file:// URIs have an empty netloc; the path begins at the third slash.
        store_url, path = _split_uri("file:///tmp/test.txt")
        assert store_url == "file://"
        assert path == "tmp/test.txt"

    def test_nested_path(self):
        store_url, path = _split_uri("s3://bucket/a/b/c/d/e.parquet")
        assert store_url == "s3://bucket"
        assert path == "a/b/c/d/e.parquet"

    def test_no_path(self):
        store_url, path = _split_uri("s3://bucket/")
        assert store_url == "s3://bucket"
        assert path == ""


class TestDownloadHelpers:
    """Unit tests for credential extraction and URL-scheme detection."""

    # ------------------------------------------------------------------
    # _extract_credentials_from_filesystem
    # ------------------------------------------------------------------

    def test_extract_credentials_none(self):
        assert _extract_credentials_from_filesystem(None) == {}

    def test_extract_credentials_local_fs(self):
        # LocalFileSystem is not S3/GCS/Azure, so credentials should be empty.
        assert _extract_credentials_from_filesystem(pafs.LocalFileSystem()) == {}

    def test_extract_credentials_unwraps_retrying_fs(self):
        # RetryingPyFileSystem wraps another filesystem; credentials should
        # still be resolved from the inner filesystem.
        inner = pafs.LocalFileSystem()
        retrying = RetryingPyFileSystem.wrap(inner, retryable_errors=[])
        assert _extract_credentials_from_filesystem(retrying) == {}

    def test_extract_credentials_s3_region_real(self):
        # PyArrow's S3FileSystem C extension only exposes `region` as a Python
        # attribute; credential fields are not accessible. This test documents
        # that boundary with a real filesystem object.
        try:
            fs = pafs.S3FileSystem(region="us-west-2")
        except Exception:
            pytest.skip("Cannot instantiate S3FileSystem in this environment")
        result = _extract_credentials_from_filesystem(fs)
        assert result.get("region") == "us-west-2"
        assert "access_key_id" not in result
        assert "skip_signature" not in result

    def test_extract_credentials_s3_anonymous_mock(self):
        # Test extraction logic for anonymous access using a mock that simulates
        # a filesystem which exposes credential attributes. We patch
        # pyarrow.fs.S3FileSystem so that isinstance() recognises the mock.
        mock_fs = MagicMock()
        mock_fs.anonymous = True
        mock_fs.access_key = None
        mock_fs.secret_key = None
        mock_fs.session_token = None
        mock_fs.endpoint_override = None
        mock_fs.region = None
        with patch("pyarrow.fs.S3FileSystem", type(mock_fs)):
            result = _extract_credentials_from_filesystem(mock_fs)
        assert result.get("skip_signature") is True
        assert "access_key_id" not in result

    def test_extract_credentials_s3_with_keys_mock(self):
        # Test extraction logic for explicit credentials using a mock.
        mock_fs = MagicMock()
        mock_fs.anonymous = False
        mock_fs.access_key = "AKID"
        mock_fs.secret_key = "SECRET"
        mock_fs.session_token = "TOKEN"
        mock_fs.endpoint_override = "https://custom-endpoint.com"
        mock_fs.region = "us-west-2"
        with patch("pyarrow.fs.S3FileSystem", type(mock_fs)):
            result = _extract_credentials_from_filesystem(mock_fs)
        assert result["access_key_id"] == "AKID"
        assert result["secret_access_key"] == "SECRET"
        assert result["session_token"] == "TOKEN"
        assert result["endpoint"] == "https://custom-endpoint.com"
        assert result["region"] == "us-west-2"

    # ------------------------------------------------------------------
    # _is_obstore_supported_url
    # ------------------------------------------------------------------

    def test_is_supported_url_recognized_scheme(self):
        # Patch obstore_parse_scheme to simulate obstore being available and
        # recognizing the scheme.
        with patch(
            "ray.data._internal.planner.plan_download_op.obstore_parse_scheme"
        ) as mock_parse:
            mock_parse.return_value = "s3"
            assert _is_obstore_supported_url("s3://bucket/key") is True

    def test_is_supported_url_unrecognized_scheme(self):
        # parse_scheme raises when the scheme is not supported.
        with patch(
            "ray.data._internal.planner.plan_download_op.obstore_parse_scheme"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("unsupported scheme")
            assert _is_obstore_supported_url("custom://bucket/key") is False

    def test_is_supported_url_parse_scheme_none(self):
        # When obstore is not installed, obstore_parse_scheme is set to None.
        # Calling None(...) raises TypeError, which is caught and returns False.
        with patch(
            "ray.data._internal.planner.plan_download_op.obstore_parse_scheme",
            new=None,
        ):
            assert _is_obstore_supported_url("s3://bucket/key") is False


class TestObstoreDownloadPath:
    """Integration tests for the obstore async download path.

    All tests are skipped when obstore is not installed. Local ``file://``
    URIs are used so no cloud credentials are required.
    """

    @pytest.fixture(autouse=True)
    def require_obstore(self):
        pytest.importorskip("obstore")

    def test_downloads_files(self, tmp_path):
        content1, content2 = b"hello obstore", b"world obstore"
        (tmp_path / "f1.bin").write_bytes(content1)
        (tmp_path / "f2.bin").write_bytes(content2)

        uris = [f"file://{tmp_path}/f1.bin", f"file://{tmp_path}/f2.bin"]
        results = asyncio.run(_download_uris_with_obstore(uris, "uri"))
        assert results == [content1, content2]

    def test_preserves_ordering(self, tmp_path):
        # Verify asyncio.gather returns results in the same order as the
        # input URIs even when downloads complete out of order.
        contents = [f"file-{i}".encode() for i in range(10)]
        for i, data in enumerate(contents):
            (tmp_path / f"f{i}.bin").write_bytes(data)

        uris = [f"file://{tmp_path}/f{i}.bin" for i in range(10)]
        results = asyncio.run(_download_uris_with_obstore(uris, "uri"))
        assert results == contents

    def test_missing_file_returns_none(self, tmp_path):
        existing = tmp_path / "exists.bin"
        existing.write_bytes(b"real data")

        uris = [
            f"file://{tmp_path}/exists.bin",
            f"file://{tmp_path}/does_not_exist.bin",
        ]
        results = asyncio.run(_download_uris_with_obstore(uris, "uri"))
        assert results[0] == b"real data"
        assert results[1] is None

    def test_store_cache_reused_for_same_host(self, tmp_path):
        # Files under the same prefix should share one ObjectStore instance.
        for i in range(3):
            (tmp_path / f"f{i}.bin").write_bytes(f"data{i}".encode())

        uris = [f"file://{tmp_path}/f{i}.bin" for i in range(3)]

        # Spy on from_url to count how many store instances are created.
        with patch(
            "obstore.store.from_url",
            wraps=__import__("obstore.store", fromlist=["from_url"]).from_url,
        ) as spy:
            asyncio.run(_download_uris_with_obstore(uris, "uri"))
            # All three URIs share the same store_url ("file://"), so
            # from_url should be called exactly once.
            assert spy.call_count == 1

    def test_download_bytes_threaded_uses_obstore(self, tmp_path):
        # Verify that download_bytes_threaded dispatches to the obstore path
        # when use_obstore=True and the URI scheme is supported.
        content = b"dispatched via obstore"
        (tmp_path / "test.bin").write_bytes(content)

        table = pa.Table.from_arrays(
            [pa.array([f"file://{tmp_path}/test.bin"])],
            names=["uri"],
        )
        ctx = DataContext.get_current()

        with patch(
            "ray.data._internal.planner.plan_download_op._download_uris_with_obstore",
            wraps=_download_uris_with_obstore,
        ) as obstore_spy, patch(
            "ray.data._internal.planner.plan_download_op._download_uris_with_pyarrow",
            wraps=_download_uris_with_pyarrow,
        ) as pyarrow_spy:
            results = list(
                download_bytes_threaded(
                    table, ["uri"], ["bytes"], ctx, use_obstore=True
                )
            )

        assert obstore_spy.called
        assert not pyarrow_spy.called
        assert results[0].column("bytes")[0].as_py() == content

    def test_download_bytes_threaded_falls_back_to_pyarrow(self, tmp_path):
        # Verify that download_bytes_threaded falls back to PyArrow when
        # use_obstore=False, even if obstore is available.
        content = b"dispatched via pyarrow"
        (tmp_path / "test.bin").write_bytes(content)

        table = pa.Table.from_arrays(
            [pa.array([str(tmp_path / "test.bin")])],
            names=["uri"],
        )
        ctx = DataContext.get_current()

        with patch(
            "ray.data._internal.planner.plan_download_op._download_uris_with_obstore",
            wraps=_download_uris_with_obstore,
        ) as obstore_spy, patch(
            "ray.data._internal.planner.plan_download_op._download_uris_with_pyarrow",
            wraps=_download_uris_with_pyarrow,
        ) as pyarrow_spy:
            results = list(
                download_bytes_threaded(
                    table, ["uri"], ["bytes"], ctx, use_obstore=False
                )
            )

        assert not obstore_spy.called
        assert pyarrow_spy.called
        assert results[0].column("bytes")[0].as_py() == content


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
