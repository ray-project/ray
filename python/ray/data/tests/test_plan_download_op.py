import asyncio
import os
from typing import Optional
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

from ray.data._internal.planner.plan_download_op import (
    _FILE_SIZE_COLUMN_PREFIX,
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
        # '#' in path must not be parsed as a fragment delimiter.
        ("s3://bucket/my#file.jpg", "s3://bucket", "my#file.jpg"),
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


class TestObstoreRangeSplitDownload:
    """Tests for the range-split download path (get_range_async).

    Uses local ``file://`` URIs.  The range-split logic is activated by
    patching the module-level ``RAY_DATA_OBSTORE_RANGE_THRESHOLD``.
    """

    @pytest.fixture(autouse=True)
    def require_obstore(self):
        pytest.importorskip("obstore")

    def test_range_split_downloads_large_file(self, tmp_path):
        # Create a file larger than the chunk size to trigger range splitting.
        chunk_size = 1024
        content = os.urandom(chunk_size * 3 + 500)
        (tmp_path / "big.bin").write_bytes(content)

        uri = f"file://{tmp_path}/big.bin"
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            chunk_size * 2,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE",
            chunk_size,
        ):
            results = asyncio.run(
                _download_uris_with_obstore([uri], "uri", file_sizes=[len(content)])
            )
        assert results == [content]

    def test_range_split_with_unknown_size_falls_back_to_head(self, tmp_path):
        # file_sizes=None → should HEAD to discover size, then range-split.
        chunk_size = 512
        content = os.urandom(chunk_size * 4)
        (tmp_path / "big2.bin").write_bytes(content)

        uri = f"file://{tmp_path}/big2.bin"
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            chunk_size,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE",
            chunk_size,
        ):
            results = asyncio.run(
                _download_uris_with_obstore([uri], "uri", file_sizes=None)
            )
        assert results == [content]

    def test_range_split_small_file_uses_simple_get(self, tmp_path):
        # Files below threshold should still use get_async, not range requests.
        content = b"small file"
        (tmp_path / "small.bin").write_bytes(content)

        uri = f"file://{tmp_path}/small.bin"
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            1024 * 1024,
        ):
            results = asyncio.run(
                _download_uris_with_obstore([uri], "uri", file_sizes=[len(content)])
            )
        assert results == [content]

    def test_range_split_mixed_sizes(self, tmp_path):
        # Mix of large and small files in a single batch.
        chunk_size = 256
        large_content = os.urandom(chunk_size * 5)
        small_content = b"tiny"
        (tmp_path / "large.bin").write_bytes(large_content)
        (tmp_path / "small.bin").write_bytes(small_content)

        uris = [
            f"file://{tmp_path}/large.bin",
            f"file://{tmp_path}/small.bin",
        ]
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            chunk_size * 2,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE",
            chunk_size,
        ):
            results = asyncio.run(
                _download_uris_with_obstore(
                    uris,
                    "uri",
                    file_sizes=[len(large_content), len(small_content)],
                )
            )
        assert results == [large_content, small_content]

    def test_threshold_zero_disables_range_split(self, tmp_path):
        # Default threshold=0 should use the simple path even for large files.
        content = os.urandom(10000)
        (tmp_path / "f.bin").write_bytes(content)

        uri = f"file://{tmp_path}/f.bin"
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            0,
        ):
            results = asyncio.run(
                _download_uris_with_obstore([uri], "uri", file_sizes=[len(content)])
            )
        assert results == [content]

    def test_download_bytes_threaded_reads_and_drops_size_column(self, tmp_path):
        # Verify that download_bytes_threaded reads the hidden size column
        # from PartitionActor, passes it through, and drops it from output.
        content = b"test content for size column"
        (tmp_path / "test.bin").write_bytes(content)
        uri = f"file://{tmp_path}/test.bin"

        size_col = f"{_FILE_SIZE_COLUMN_PREFIX}uri"
        table = pa.Table.from_arrays(
            [pa.array([uri]), pa.array([len(content)], type=pa.int64())],
            names=["uri", size_col],
        )
        ctx = DataContext.get_current()

        results = list(
            download_bytes_threaded(table, ["uri"], ["bytes"], ctx, use_obstore=True)
        )

        out = results[0]
        assert out.column("bytes")[0].as_py() == content
        assert size_col not in out.column_names

    def test_http_auto_detect_enables_allow_http(self):
        # Verify that http:// URIs actually download successfully when
        # allow_http is auto-enabled, and that a warning is logged.
        from http.server import BaseHTTPRequestHandler, HTTPServer
        from threading import Thread

        content = b"http download works"

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            def log_message(self, format: str, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), _Handler)
        port = server.server_address[1]
        t = Thread(target=server.serve_forever, daemon=True)
        t.start()

        http_uri = f"http://127.0.0.1:{port}/test.bin"
        try:
            with patch(
                "ray.data._internal.planner.plan_download_op.logger"
            ) as mock_logger:
                results = asyncio.run(_download_uris_with_obstore([http_uri], "uri"))

            assert results == [content], (
                "http:// download should succeed when allow_http is " "auto-enabled"
            )
            mock_logger.warning.assert_any_call(
                "Downloading over unencrypted HTTP. " "Consider using https:// instead."
            )
        finally:
            server.shutdown()

    def test_no_allow_http_for_non_http_uris(self, tmp_path):
        # Verify that allow_http is NOT set when no http:// URIs are present.
        import obstore.store as obs_store

        content = b"no http"
        (tmp_path / "f.bin").write_bytes(content)
        uri = f"file://{tmp_path}/f.bin"

        with patch.object(obs_store, "from_url", wraps=obs_store.from_url) as spy:
            results = asyncio.run(_download_uris_with_obstore([uri], "uri"))

        for call in spy.call_args_list:
            client_opts = call.kwargs.get("client_options", {})
            assert "allow_http" not in client_opts
        assert results == [content]

    def test_head_failure_falls_back_to_simple_get(self, tmp_path):
        # When HEAD fails (e.g., server error), the size stays 0 and the
        # file should be downloaded via simple GET instead of crashing.
        chunk_size = 256
        content = os.urandom(chunk_size * 10)
        (tmp_path / "f.bin").write_bytes(content)

        uri = f"file://{tmp_path}/f.bin"
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            chunk_size,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE",
            chunk_size,
        ):
            import obstore as obs

            async def _failing_head(*args, **kwargs):
                raise OSError("HEAD not supported")

            original_get_range = obs.get_range_async
            range_calls = []

            async def _tracking_range(*args, **kwargs):
                range_calls.append(args)
                return await original_get_range(*args, **kwargs)

            with patch.object(
                obs, "head_async", side_effect=_failing_head
            ), patch.object(obs, "get_range_async", side_effect=_tracking_range):
                results = asyncio.run(
                    _download_uris_with_obstore([uri], "uri", file_sizes=[0])
                )

        assert results == [content]
        # Explicitly verify ranged path was NOT taken.
        assert len(range_calls) == 0

    def test_partial_unknown_sizes_head_only_for_unknowns(self, tmp_path):
        # In a batch where some sizes are known and others are 0,
        # HEAD should only be issued for the unknowns.
        chunk_size = 256
        large_content = os.urandom(chunk_size * 5)
        small_content = b"small"
        unknown_content = os.urandom(chunk_size * 8)

        (tmp_path / "large.bin").write_bytes(large_content)
        (tmp_path / "small.bin").write_bytes(small_content)
        (tmp_path / "unknown.bin").write_bytes(unknown_content)

        uris = [
            f"file://{tmp_path}/large.bin",
            f"file://{tmp_path}/small.bin",
            f"file://{tmp_path}/unknown.bin",
        ]
        file_sizes: list[Optional[int]] = [len(large_content), len(small_content), 0]

        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            chunk_size * 2,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE",
            chunk_size,
        ):
            import obstore as obs

            original_head = obs.head_async
            head_calls = []

            async def _tracking_head(*args, **kwargs):
                head_calls.append(args)
                return await original_head(*args, **kwargs)

            with patch.object(obs, "head_async", side_effect=_tracking_head):
                results = asyncio.run(
                    _download_uris_with_obstore(uris, "uri", file_sizes=file_sizes)
                )

        # Only the unknown (index 2) should trigger a HEAD request.
        assert len(head_calls) == 1
        assert results == [large_content, small_content, unknown_content]

    def test_file_size_at_exact_threshold_uses_simple_get(self, tmp_path):
        # size == threshold should use simple GET (check is size > threshold).
        threshold = 1024
        content = os.urandom(threshold)
        (tmp_path / "exact.bin").write_bytes(content)

        uri = f"file://{tmp_path}/exact.bin"
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            threshold,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE",
            256,
        ):
            import obstore as obs

            range_calls = []
            original_get_range = obs.get_range_async

            async def _tracking_range(*args, **kwargs):
                range_calls.append(args)
                return await original_get_range(*args, **kwargs)

            with patch.object(obs, "get_range_async", side_effect=_tracking_range):
                results = asyncio.run(
                    _download_uris_with_obstore([uri], "uri", file_sizes=[threshold])
                )

        assert len(range_calls) == 0
        assert results == [content]

    def test_ranged_download_failure_returns_none(self, tmp_path):
        # If a range request fails mid-transfer, _download_one_ranged should
        # catch the exception and return None rather than crashing.
        chunk_size = 256
        content = os.urandom(chunk_size * 4)
        (tmp_path / "fail.bin").write_bytes(content)

        uri = f"file://{tmp_path}/fail.bin"
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            chunk_size,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE",
            chunk_size,
        ):
            import obstore as obs

            call_count = 0

            async def _failing_range(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 2:
                    raise OSError("simulated network failure")
                return await obs.__wrapped_get_range(*args, **kwargs)

            obs.__wrapped_get_range = obs.get_range_async
            try:
                with patch.object(obs, "get_range_async", side_effect=_failing_range):
                    results = asyncio.run(
                        _download_uris_with_obstore(
                            [uri], "uri", file_sizes=[len(content)]
                        )
                    )
            finally:
                del obs.__wrapped_get_range

        # Ranged failed, but simple GET fallback should succeed.
        assert results == [content]

    def test_invalid_max_concurrency_disables_range_split(self, tmp_path):
        # max_conc <= 0 with range splitting enabled is a misconfiguration.
        # Verify it warns and falls back to simple GET (no crash).
        chunk_size = 256
        content = os.urandom(chunk_size * 4)
        (tmp_path / "f.bin").write_bytes(content)

        uri = f"file://{tmp_path}/f.bin"
        # Simulate misconfiguration: range splitting on, but concurrency = 0.
        with patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_RANGE_THRESHOLD",
            chunk_size,
        ), patch(
            "ray.data._internal.planner.plan_download_op.RAY_DATA_OBSTORE_MAX_CONCURRENCY",
            0,
        ), patch(
            "ray.data._internal.planner.plan_download_op.logger"
        ) as mock_logger:
            import obstore as obs

            # Spy on get_range_async to verify it is never called.
            range_calls = []
            original_get_range = obs.get_range_async

            async def _tracking_range(*args, **kwargs):
                range_calls.append(args)
                return await original_get_range(*args, **kwargs)

            with patch.object(obs, "get_range_async", side_effect=_tracking_range):
                results = asyncio.run(
                    _download_uris_with_obstore([uri], "uri", file_sizes=[len(content)])
                )

        # Download should succeed via simple GET fallback.
        assert results == [content]
        # Ranged path must not have been entered.
        assert len(range_calls) == 0, "Range splitting should be disabled"
        # Warning about the invalid configuration should be logged.
        mock_logger.warning.assert_any_call(
            "RAY_DATA_OBSTORE_RANGE_THRESHOLD is set but "
            "RAY_DATA_OBSTORE_MAX_CONCURRENCY=%d is invalid. "
            "Range downloads require a positive concurrency limit to avoid "
            "socket exhaustion. Disabling range splitting.",
            0,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
