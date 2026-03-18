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


# TestSplitUri
@pytest.mark.parametrize(
    "uri, expected_store_url, expected_path",
    [
        ("s3://my-bucket/prefix/key.jpg", "s3://my-bucket", "prefix/key.jpg"),
        ("gs://bucket/path/to/file", "gs://bucket", "path/to/file"),
        ("https://host.com/a/b/c.png", "https://host.com", "a/b/c.png"),
        # file:// URIs have an empty netloc; path starts at the third slash.
        ("file:///tmp/test.txt", "file://", "tmp/test.txt"),
        # Nested paths.
        ("s3://bucket/a/b/c/d/e.parquet", "s3://bucket", "a/b/c/d/e.parquet"),
        # Trailing slash: empty path.
        ("s3://bucket/", "s3://bucket", ""),
        # Query string must be preserved for pre-signed / parameterized URLs.
        (
            "https://s3.amazonaws.com/bucket/key?X-Amz-Signature=abc&X-Amz-Expires=3600",
            "https://s3.amazonaws.com",
            "bucket/key?X-Amz-Signature=abc&X-Amz-Expires=3600",
        ),
        # No query string: path only.
        ("https://host.com/a/b/c.png", "https://host.com", "a/b/c.png"),
    ],
)
def test_split_uri(uri, expected_store_url, expected_path):
    store_url, path = _split_uri(uri)
    assert store_url == expected_store_url
    assert path == expected_path


# TestDownloadHelpers
class TestDownloadHelpers:
    """Unit tests for credential extraction and URL-scheme detection."""

    # _extract_credentials_from_filesystem filesystem-agnostic cases
    def test_extract_credentials_none(self):
        assert _extract_credentials_from_filesystem(None) == {}

    def test_extract_credentials_unrecognized_fs(self):
        # LocalFileSystem is not S3/GCS/Azure, so no credentials extracted.
        assert _extract_credentials_from_filesystem(pafs.LocalFileSystem()) == {}

    def test_extract_credentials_unwraps_retrying_fs(self):
        # RetryingPyFileSystem must be unwrapped before the isinstance checks,
        # otherwise an S3/GCS/Azure branch would never be reached.
        inner = pafs.LocalFileSystem()
        retrying = RetryingPyFileSystem.wrap(inner, retryable_errors=[])
        assert _extract_credentials_from_filesystem(retrying) == {}

    def test_extract_credentials_unwraps_retrying_fs_over_s3(self):
        # Even when an S3FileSystem is wrapped, credentials must still be
        # extracted from the inner filesystem after unwrapping.
        mock_fs = MagicMock()
        mock_fs.region = "eu-west-1"
        mock_fs.access_key = None
        mock_fs.secret_key = None
        mock_fs.session_token = None
        mock_fs.endpoint_override = None
        mock_fs.anonymous = False

        mock_retrying = MagicMock(spec=RetryingPyFileSystem)
        mock_retrying.unwrap.return_value = mock_fs

        with patch("pyarrow.fs.S3FileSystem", type(mock_fs)):
            result = _extract_credentials_from_filesystem(mock_retrying)

        assert result.get("region") == "eu-west-1"

    # _extract_credentials_from_filesystem with S3
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
        # We patch pyarrow.fs.S3FileSystem so isinstance() recognises the mock.
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

    # _extract_credentials_from_filesystem with GCS
    @pytest.mark.parametrize(
        "anonymous, expected",
        [
            (True, {"skip_signature": True}),
            (False, {}),
            (None, {}),
        ],
    )
    def test_extract_credentials_gcs_anonymous_mock(self, anonymous, expected):
        # GCS anonymous access maps to obstore's skip_signature.
        # Other GCS credentials (service account, application default) are
        # not accessible via PyArrow attributes; obstore resolves them from
        # the environment automatically.
        mock_fs = MagicMock()
        mock_fs.anonymous = anonymous
        with patch("pyarrow.fs.GcsFileSystem", type(mock_fs)):
            result = _extract_credentials_from_filesystem(mock_fs)
        assert result == expected

    # _extract_credentials_from_filesystem with Azure
    @pytest.mark.parametrize(
        "account_name, account_key, expected",
        [
            (
                "myaccount",
                "mykey",
                {"account_name": "myaccount", "account_key": "mykey"},
            ),
            # account_key is optional (e.g. when using managed identity).
            ("myaccount", None, {"account_name": "myaccount"}),
            (None, None, {}),
        ],
    )
    def test_extract_credentials_azure_mock(self, account_name, account_key, expected):
        mock_fs = MagicMock()
        mock_fs.account_name = account_name
        mock_fs.account_key = account_key
        with patch("pyarrow.fs.AzureFileSystem", type(mock_fs)):
            result = _extract_credentials_from_filesystem(mock_fs)
        assert result == expected

    # _is_obstore_supported_url
    @pytest.mark.parametrize(
        "uri, raises, expected",
        [
            # Recognized scheme: parse_scheme succeeds.
            ("s3://bucket/key", None, True),
            ("https://host.com/file", None, True),
            # Unrecognized scheme: parse_scheme raises.
            ("custom://bucket/key", ValueError("unsupported"), False),
            # obstore not installed: parse_scheme is None, so TypeError.
            ("s3://bucket/key", "none", False),
        ],
    )
    def test_is_obstore_supported_url(self, uri, raises, expected):
        if raises == "none":
            ctx = patch(
                "ray.data._internal.planner.plan_download_op.obstore_parse_scheme",
                new=None,
            )
        else:
            mock = (
                MagicMock(side_effect=raises)
                if raises
                else MagicMock(return_value="s3")
            )
            ctx = patch(
                "ray.data._internal.planner.plan_download_op.obstore_parse_scheme",
                new=mock,
            )
        with ctx:
            assert _is_obstore_supported_url(uri) is expected


# TestObstoreDownloadPath
class TestObstoreDownloadPath:
    """Integration tests for the obstore async download path.

    All tests are skipped when obstore is not installed. Local ``file://``
    URIs are used so no cloud credentials are required.
    """

    @pytest.fixture(autouse=True)
    def require_obstore(self):
        pytest.importorskip("obstore")

    def test_empty_uris_returns_empty_list(self):
        # asyncio.gather on an empty task list returns [] — verify this holds
        # end-to-end so the obstore path is consistent with the PyArrow path's
        # len(uris) == 0 early-exit.
        results = asyncio.run(_download_uris_with_obstore([], "uri"))
        assert results == []

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

    @pytest.mark.parametrize(
        "use_obstore, expect_obstore_called",
        [
            (True, True),
            (False, False),
        ],
    )
    def test_download_bytes_threaded_dispatch(
        self, tmp_path, use_obstore, expect_obstore_called
    ):
        # Verify that download_bytes_threaded dispatches to the correct backend.
        content = b"dispatch test content"
        if use_obstore:
            uri = f"file://{tmp_path}/test.bin"
        else:
            # PyArrow path uses bare filesystem paths, not file:// URIs.
            uri = str(tmp_path / "test.bin")
        (tmp_path / "test.bin").write_bytes(content)

        table = pa.Table.from_arrays([pa.array([uri])], names=["uri"])
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
                    table, ["uri"], ["bytes"], ctx, use_obstore=use_obstore
                )
            )

        assert obstore_spy.called == expect_obstore_called
        assert pyarrow_spy.called == (not expect_obstore_called)
        assert results[0].column("bytes")[0].as_py() == content


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
