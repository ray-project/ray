import sys

import pyarrow as pa
import pytest

import ray
from ray.data.expressions import download
from ray.tests.conftest import _ray_start

# obstore itself does not retry under this budget, so every transient error
# surfaces to Ray Data's own retry machinery immediately.
_TINY_RETRY_BUDGET = {
    "RAY_DATA_OBSTORE_RETRY_MAX_RETRIES": "0",
    "RAY_DATA_OBSTORE_RETRY_INIT_BACKOFF_MS": "1",
    "RAY_DATA_OBSTORE_RETRY_MAX_BACKOFF_MS": "2",
    "RAY_DATA_OBSTORE_RETRY_TIMEOUT_S": "5",
    # Keep the block-level re-fetch (default 2 attempts) but don't wait.
    "RAY_DATA_DOWNLOAD_RETRY_BACKOFF_MS": "0",
}


@pytest.fixture(scope="module")
def retry_cluster(maybe_setup_external_redis_shared):
    with _ray_start(
        num_cpus=4,
        include_dashboard=False,
        runtime_env={"env_vars": _TINY_RETRY_BUDGET},
    ):
        yield


def _start_flaky_http_server(get_statuses, content=b"image bytes"):
    """Loopback HTTP server for ``download()`` tests.

    ``HEAD`` always succeeds with the content length, so the partition actor
    learns the size. Each ``GET`` answers the next status in ``get_statuses``;
    the last one repeats. Returns ``(server, port, served)`` where ``served``
    records every GET status sent.
    """
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from threading import Thread

    served = []

    class _Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()

        def do_GET(self):
            status = get_statuses[min(len(served), len(get_statuses) - 1)]
            served.append(status)
            body = content if status == 200 else b""
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1], served


@pytest.mark.usefixtures("retry_cluster")
class TestDownloadExpressionErrors:
    """Test error conditions and edge cases for download expressions."""

    def test_download_expression_invalid_uri_column(self):
        """Test download expression with non-existent URI column."""
        table = pa.Table.from_arrays(
            [
                pa.array(["local://test.txt"]),
            ],
            names=["existing_column"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("bytes", download("non_existent_column"))

        # Should raise error when trying to execute
        with pytest.raises(ValueError):
            ds_with_downloads.take_all()

    def test_download_expression_with_null_uris(self):
        """Test download expression handling of null/empty URIs."""
        table = pa.Table.from_arrays(
            [
                pa.array(["local://test.txt", None, ""]),
            ],
            names=["uri"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("bytes", download("uri"))

        # Should handle nulls gracefully (exact behavior may vary)
        # This test mainly ensures no crash occurs
        try:
            results = ds_with_downloads.take_all()
            # If it succeeds, verify structure is reasonable
            assert len(results) == 3
            for result in results:
                assert "bytes" in result
        except Exception as e:
            # If it fails, should be a reasonable error (not a crash)
            assert isinstance(e, (ValueError, KeyError, RuntimeError))

    def test_download_expression_with_malformed_uris(self, tmp_path):
        """Test download expression with malformed URIs.

        This tests that various malformed URIs are caught and return None
        instead of crashing.

        All of the URIs should be malformed in order to test the ZeroDivisionError
        described in https://github.com/ray-project/ray/issues/58462.
        """
        malformed_uris = [
            f"local://{tmp_path}/nonexistent.txt",  # File doesn't exist
            "local:///this/path/does/not/exist/file.txt",  # Invalid path
            "",  # Empty URI
            "foobar",  # Random string
            # TODO(xyuzh): Currently, using the below URIs raises an exception
            # in _resolve_paths_and_filesystem. We need to fix that issue and
            # add the tests in.
            # "file:///\x00/null/byte",  # Null byte
            # "http://host/path\n\r",  # Line breaks
            # "foo://bar",  # Invalid scheme
            # "://no-scheme",  # Missing scheme
            # "http://host/path?query=<script>",  # Injection attempts
        ]

        ds = ray.data.from_items([{"uri": uri} for uri in malformed_uris])
        ds_with_downloads = ds.with_column("bytes", download("uri"))
        results = ds_with_downloads.take_all()

        # All malformed URIs should return None
        assert len(results) == len(malformed_uris)
        for result in results:
            assert result["bytes"] is None

    def test_download_expression_mixed_valid_and_invalid_uris(self, tmp_path):
        """Test download expression when some but not all of the URIs are invalid."""
        # Create one valid file
        valid_file = tmp_path / "valid.txt"
        valid_file.write_bytes(b"valid content")

        # Create URIs: one valid and one non-existent file.
        ds = ray.data.from_items(
            [
                {"uri": str(valid_file), "id": 0},
                {"uri": str(tmp_path / "nonexistent.txt"), "id": 1},
            ]
        )
        ds_with_downloads = ds.with_column("bytes", download("uri"))

        # Should not crash - failed downloads return None
        results = sorted(ds_with_downloads.take_all(), key=lambda row: row["id"])
        assert len(results) == 2

        # First URI should succeed
        assert results[0]["bytes"] == b"valid content"

        # Second URI should fail gracefully (return None)
        assert results[1]["bytes"] is None


@pytest.mark.usefixtures("retry_cluster")
class TestDownloadTransientFailures:
    """A download that fails after its retries fails the task.

    Before, the row silently became ``None`` bytes and blew up downstream
    (``PIL.UnidentifiedImageError`` in ``read_from_uris_fixed_size``).
    """

    @pytest.fixture
    def tiny_retry_cluster(self):
        ctx = ray.data.DataContext.get_current()
        saved = (ctx.max_errored_blocks, ctx.retried_map_errors, ctx.max_map_retries)
        yield ctx
        ctx.max_errored_blocks, ctx.retried_map_errors, ctx.max_map_retries = saved

    @pytest.fixture
    def flaky_server(self):
        servers = []

        def start(get_statuses):
            server, port, served = _start_flaky_http_server(get_statuses)
            servers.append(server)
            return port, served

        yield start
        for server in servers:
            server.shutdown()

    def test_aborts_by_default(self, tiny_retry_cluster, flaky_server):
        pytest.importorskip("obstore")
        port, _ = flaky_server([503])
        ds = ray.data.from_items([{"uri": f"http://127.0.0.1:{port}/a.bin"}])
        ds = ds.with_column("bytes", download("uri"))
        with pytest.raises(Exception, match="Failed to download"):
            ds.take_all()

    def test_tolerated_with_max_errored_blocks(
        self, tiny_retry_cluster, flaky_server, tmp_path
    ):
        pytest.importorskip("obstore")
        (tmp_path / "ok.bin").write_bytes(b"fine")
        port, _ = flaky_server([503])
        tiny_retry_cluster.max_errored_blocks = -1
        ds = ray.data.from_items(
            [
                {"uri": f"file://{tmp_path}/ok.bin"},
                {"uri": f"http://127.0.0.1:{port}/a.bin"},
            ],
            override_num_blocks=2,
        )
        ds = ds.with_column("bytes", download("uri"))
        # The block with the failing URI is dropped; the other one survives.
        assert [row["bytes"] for row in ds.take_all()] == [b"fine"]

    def test_retried_with_map_retries(self, tiny_retry_cluster, flaky_server):
        pytest.importorskip("obstore")
        port, served = flaky_server([503, 503, 200])
        tiny_retry_cluster.retried_map_errors = ["DownloadError"]
        tiny_retry_cluster.max_map_retries = 2
        ds = ray.data.from_items([{"uri": f"http://127.0.0.1:{port}/a.bin"}])
        ds = ds.with_column("bytes", download("uri"))
        assert [row["bytes"] for row in ds.take_all()] == [b"image bytes"]
        assert served == [503, 503, 200]

    def test_transient_failure_recovers_without_settings(
        self, tiny_retry_cluster, flaky_server
    ):
        # One transient failure is re-fetched inside the task by default; no
        # DataContext retry settings are needed.
        pytest.importorskip("obstore")
        port, served = flaky_server([503, 200])
        ds = ray.data.from_items([{"uri": f"http://127.0.0.1:{port}/a.bin"}])
        ds = ds.with_column("bytes", download("uri"))
        assert [row["bytes"] for row in ds.take_all()] == [b"image bytes"]
        assert served == [503, 200]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
