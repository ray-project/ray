import os
import sys
from tempfile import NamedTemporaryFile

import pytest

from ray.dashboard.modules.job.common import JobSubmitRequest
from ray.dashboard.modules.job.utils import (
    fast_tail_last_n_lines,
    file_tail_iterator,
    parse_and_validate_request,
    redact_url_password,
    strip_keys_with_value_none,
)


# Polyfill anext() function for Python 3.9 compatibility
# May raise StopAsyncIteration.
async def anext_polyfill(iterator):
    return await iterator.__anext__()


# Use the built-in anext() for Python 3.10+, otherwise use our polyfilled function
if sys.version_info < (3, 10):
    anext = anext_polyfill


@pytest.fixture
def tmp():
    with NamedTemporaryFile() as f:
        yield f.name


def test_strip_keys_with_value_none():
    d = {"a": 1, "b": None, "c": 3}
    assert strip_keys_with_value_none(d) == {"a": 1, "c": 3}
    d = {"a": 1, "b": 2, "c": 3}
    assert strip_keys_with_value_none(d) == d
    d = {"a": 1, "b": None, "c": None}
    assert strip_keys_with_value_none(d) == {"a": 1}


def test_redact_url_password():
    url = "http://user:password@host:port"
    assert redact_url_password(url) == "http://user:<redacted>@host:port"
    url = "http://user:password@host:port?query=1"
    assert redact_url_password(url) == "http://user:<redacted>@host:port?query=1"
    url = "http://user:password@host:port?query=1&password=2"
    assert (
        redact_url_password(url)
        == "http://user:<redacted>@host:port?query=1&password=2"
    )
    url = "https://user:password@127.0.0.1:8080"
    assert redact_url_password(url) == "https://user:<redacted>@127.0.0.1:8080"
    url = "https://user:password@host:port?query=1"
    assert redact_url_password(url) == "https://user:<redacted>@host:port?query=1"
    url = "https://user:password@host:port?query=1&password=2"
    assert (
        redact_url_password(url)
        == "https://user:<redacted>@host:port?query=1&password=2"
    )


# Mock for aiohttp.web.Request, which should not be constructed directly.
class MockRequest:
    def __init__(self, **kwargs):
        self._json = kwargs

    async def json(self):
        return self._json


@pytest.mark.asyncio
async def test_mock_request():
    request = MockRequest(a=1, b=2)
    assert await request.json() == {"a": 1, "b": 2}
    request = MockRequest(a=1, b=None)
    assert await request.json() == {"a": 1, "b": None}


# async test
@pytest.mark.asyncio
class TestParseAndValidateRequest:
    async def test_basic(self):
        request = MockRequest(entrypoint="echo hi")
        expected = JobSubmitRequest(entrypoint="echo hi")
        assert await parse_and_validate_request(request, JobSubmitRequest) == expected

    async def test_forward_compatibility(self):
        request = MockRequest(entrypoint="echo hi", new_client_field=None)
        expected = JobSubmitRequest(entrypoint="echo hi")
        assert await parse_and_validate_request(request, JobSubmitRequest) == expected


class TestIterLine:
    @pytest.mark.asyncio
    async def test_invalid_type(self):
        with pytest.raises(TypeError, match="path must be a string"):
            await anext(file_tail_iterator(1))

    @pytest.mark.asyncio
    async def test_file_not_created(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None
        f = open(tmp, "w")
        f.write("hi\n")
        f.flush()
        assert await anext(it) is not None

    @pytest.mark.asyncio
    async def test_wait_for_newline(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")
        f.write("no_newline_yet")
        assert await anext(it) is None
        f.write("\n")
        f.flush()
        assert await anext(it) == ["no_newline_yet\n"]

    @pytest.mark.asyncio
    async def test_multiple_lines(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        num_lines = 10
        for i in range(num_lines):
            s = f"{i}\n"
            f.write(s)
            f.flush()
            assert await anext(it) == [s]

        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        # Write lines in batches of 10, check that we get them back in batches.
        for _ in range(100):
            num_lines = 10
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert await anext(it) == [f"{i}\n" for i in range(10)]

        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_max_line_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        # Write lines in batches of 50, check that we get them back in batches of 10.
        for _ in range(100):
            num_lines = 50
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert await anext(it) == [f"{i}\n" for i in range(10)]
            assert await anext(it) == [f"{i}\n" for i in range(10, 20)]
            assert await anext(it) == [f"{i}\n" for i in range(20, 30)]
            assert await anext(it) == [f"{i}\n" for i in range(30, 40)]
            assert await anext(it) == [f"{i}\n" for i in range(40, 50)]

        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_max_char_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        # Write a single line that is 60k characters
        f.write(f"{'1234567890' * 6000}\n")
        # Write a 4 lines that are 10k characters each
        for _ in range(4):
            f.write(f"{'1234567890' * 500}\n")
        f.flush()

        # First line will come in a batch of its own
        assert await anext(it) == [f"{'1234567890' * 6000}\n"]
        # Other 4 lines will be batched together
        assert (
            await anext(it)
            == [
                f"{'1234567890' * 500}\n",
            ]
            * 4
        )
        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_delete_file(self):
        with NamedTemporaryFile() as tmp:
            it = file_tail_iterator(tmp.name)
            f = open(tmp.name, "w")

            assert await anext(it) is None

            f.write("hi\n")
            f.flush()

            assert await anext(it) == ["hi\n"]

        # Calls should continue returning None after file deleted.
        assert await anext(it) is None


class TestFastTailLastNLines:
    def test_nonexistent_path(self, tmp):
        missing = tmp + ".missing"
        assert not os.path.exists(missing)
        with pytest.raises(FileNotFoundError):
            fast_tail_last_n_lines(missing, num_lines=10, max_chars=1000)

    def test_basic_last_n(self, tmp):
        # Write 100 lines, check that we get the last 10 lines.
        with open(tmp, "w") as f:
            for i in range(100):
                f.write(f"line-{i}\n")
        out = fast_tail_last_n_lines(tmp, num_lines=10, max_chars=1000)
        expected = "".join([f"line-{i}\n" for i in range(90, 100)])
        assert out == expected

    def test_truncate_max_chars(self, tmp):
        # Construct a log file with two lines, each over max_chars,
        # check that we truncate to max_chars.
        with open(tmp, "w") as f:
            f.write("x" * 5000 + "\n")
            f.write("y" * 5000 + "\n")
        out = fast_tail_last_n_lines(tmp, num_lines=2, max_chars=3000)
        assert len(out) == 3000
        # Check that we truncate to max_chars, and include the last line.
        assert out.endswith("\n")

    def test_partial_last_line(self, tmp):
        # Write a log file with a partial last line, check that we include it.
        with open(tmp, "w") as f:
            f.write("a\n")
            f.write("b\n")
            f.write("partial_last_line")  # No newline at end
        out = fast_tail_last_n_lines(tmp, num_lines=3, max_chars=1000)
        assert out == "a\nb\npartial_last_line"

    def test_small_block_size(self, tmp):
        # Write 30 lines, check that we can read a small block size and get the last N lines.
        with open(tmp, "w") as f:
            for i in range(30):
                f.write(f"{i}\n")
        out = fast_tail_last_n_lines(tmp, num_lines=5, max_chars=1000, block_size=16)
        expected = "".join([f"{i}\n" for i in range(25, 30)])
        assert out == expected

    def test_mixed_long_lines(self, tmp):
        # Write a log file with a mix of short and long lines, check that we get the last N lines.
        with open(tmp, "w") as f:
            f.write("short-1\n")
            f.write("short-2\n")
            f.write("long-" + ("Z" * 10000) + "\n")
            f.write("short-3\n")
            f.write("short-4\n")
        out = fast_tail_last_n_lines(tmp, num_lines=3, max_chars=20000)
        # Check that we get the last 3 lines, including the long line.
        assert out.splitlines()[-1] == "short-4"
        assert out.splitlines()[-2] == "short-3"
        assert out.splitlines()[-3].startswith("long-Z")

    def test_sparse_large_file_tail_max_chars(self, tmp):
        """Simulate ~8 GiB sparse file tail and verify max_chars=20000 truncation."""
        size_8g = 8 * 1024 * 1024 * 1024
        # Build tail of two extremely long lines
        tail = "\n" + ("Q" * 25000 + "\n") + ("R" * 25000 + "\n")
        tail_bytes = tail.encode("utf-8")

        print("Start writing sparse file tail...")
        # Create a sparse file: seek to near EOF then write only the tail.
        with open(tmp, "wb") as f:
            f.seek(size_8g - len(tail_bytes))
            f.write(tail_bytes)
            f.flush()

        print("Finish writing sparse file tail.")
        out = fast_tail_last_n_lines(tmp, num_lines=2, max_chars=20000)
        print("Finish reading sparse file tail.")
        assert len(out) == 20000
        assert out.endswith("\n")
        assert "R" * 100 in out  # sampling check for last line content


@pytest.mark.asyncio
async def test_get_driver_jobs_retries_deadline_once():
    """Regression test for https://github.com/ray-project/ray/issues/55998."""
    from unittest.mock import AsyncMock, MagicMock

    from ray._raylet import (
        GRPC_STATUS_CODE_DEADLINE_EXCEEDED,
        GRPC_STATUS_CODE_UNAVAILABLE,
    )
    from ray.dashboard.modules.job.utils import (
        DEFAULT_GET_ALL_JOB_INFO_RETRY_TIMEOUT_S,
        get_driver_jobs,
    )
    from ray.exceptions import RpcError

    deadline_err = RpcError(
        "Deadline Exceeded", rpc_code=GRPC_STATUS_CODE_DEADLINE_EXCEEDED
    )

    # 1. Single transient DEADLINE_EXCEEDED with timeout=None retries once and succeeds.
    gcs_client = MagicMock()
    gcs_client.async_get_all_job_info = AsyncMock(side_effect=[deadline_err, {}])
    jobs, submission_drivers = await get_driver_jobs(
        gcs_client, job_or_submission_id="raysubmit_123"
    )
    assert jobs == {}
    assert submission_drivers == {}
    assert gcs_client.async_get_all_job_info.await_count == 2
    first_kw = gcs_client.async_get_all_job_info.await_args_list[0].kwargs
    second_kw = gcs_client.async_get_all_job_info.await_args_list[1].kwargs
    assert first_kw["job_or_submission_id"] == "raysubmit_123"
    assert first_kw["timeout"] is None
    assert second_kw["job_or_submission_id"] == "raysubmit_123"
    assert second_kw["timeout"] == DEFAULT_GET_ALL_JOB_INFO_RETRY_TIMEOUT_S

    # 2. Consecutive DEADLINE_EXCEEDED propagates after the single bounded retry.
    gcs_client.async_get_all_job_info = AsyncMock(
        side_effect=[deadline_err, deadline_err]
    )
    with pytest.raises(RpcError, match="Deadline Exceeded"):
        await get_driver_jobs(gcs_client, job_or_submission_id="raysubmit_123")
    assert gcs_client.async_get_all_job_info.await_count == 2

    # 3. Non-deadline RpcError is not retried.
    unavailable_err = RpcError(
        "Unavailable", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )
    gcs_client.async_get_all_job_info = AsyncMock(side_effect=unavailable_err)
    with pytest.raises(RpcError, match="Unavailable"):
        await get_driver_jobs(gcs_client, job_or_submission_id="raysubmit_123")
    assert gcs_client.async_get_all_job_info.await_count == 1

    # 4. Explicit caller timeout is respected without retrying.
    gcs_client.async_get_all_job_info = AsyncMock(side_effect=[deadline_err, {}])
    with pytest.raises(RpcError, match="Deadline Exceeded"):
        await get_driver_jobs(
            gcs_client, job_or_submission_id="raysubmit_123", timeout=5
        )
    assert gcs_client.async_get_all_job_info.await_count == 1


@pytest.mark.asyncio
async def test_tail_job_logs_survives_transient_get_all_job_info_timeout():
    """Verify WebSocket log tailing does not close early on a single GetAllJobInfo timeout."""
    from unittest.mock import MagicMock

    import aiohttp
    from aiohttp import web

    from ray._raylet import GRPC_STATUS_CODE_DEADLINE_EXCEEDED
    from ray.dashboard.modules.job.common import (
        JobInfo,
        JobInfoStorageClient,
        JobStatus,
    )
    from ray.dashboard.modules.job.job_head import JobHead
    from ray.dashboard.modules.job.sdk import JobSubmissionClient
    from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
    from ray.exceptions import RpcError

    job_id = "raysubmit_tail_timeout_55998"
    call_count = 0
    gcs_client = MagicMock()

    async def fake_get_all_job_info(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RpcError(
                "Deadline Exceeded",
                rpc_code=GRPC_STATUS_CODE_DEADLINE_EXCEEDED,
            )
        return {}

    gcs_client.async_get_all_job_info = fake_get_all_job_info

    info_calls = 0
    job_info_client = MagicMock(spec=JobInfoStorageClient)

    async def fake_get_info(jid, timeout=30):
        nonlocal info_calls
        info_calls += 1
        if info_calls == 1:
            return JobInfo(status=JobStatus.PENDING, entrypoint="echo hi")
        return JobInfo(
            status=JobStatus.RUNNING,
            entrypoint="echo hi",
            driver_agent_http_address="http://127.0.0.1:9999",
            driver_node_id="node-1",
        )

    job_info_client.get_info = fake_get_info

    job_head = JobHead.__new__(JobHead)
    job_head._gcs_client = gcs_client
    job_head._job_info_client = job_info_client
    job_head.WAIT_FOR_SUPERVISOR_ACTOR_INTERVAL_S = 0.01

    fake_agent = MagicMock()

    async def fake_tail(sid):
        yield "hello from job\n"

    fake_agent.tail_job_logs = fake_tail
    job_head.get_job_driver_agent_client = lambda job: fake_agent

    backend_app = web.Application()
    backend_app.router.add_get(
        "/api/jobs/{job_or_submission_id}/logs/tail", job_head.tail_job_logs
    )
    backend_runner = web.AppRunner(backend_app)
    await backend_runner.setup()
    backend_site = web.TCPSite(backend_runner, "127.0.0.1", 0)
    await backend_site.start()
    backend_port = backend_site._server.sockets[0].getsockname()[1]

    proxy_handle = SubprocessModuleHandle.__new__(SubprocessModuleHandle)
    proxy_session = aiohttp.ClientSession()

    async def proxy_handler(request):
        orig_ws_connect = proxy_session.ws_connect

        def patched_ws_connect(url, **kw):
            url = url.replace("http://localhost", f"http://127.0.0.1:{backend_port}")
            return orig_ws_connect(url, **kw)

        proxy_handle.http_client_session = MagicMock(ws_connect=patched_ws_connect)
        return await proxy_handle.proxy_websocket(request)

    front_app = web.Application()
    front_app.router.add_get(
        "/api/jobs/{job_or_submission_id}/logs/tail", proxy_handler
    )
    front_runner = web.AppRunner(front_app)
    await front_runner.setup()
    front_site = web.TCPSite(front_runner, "127.0.0.1", 0)
    await front_site.start()
    front_port = front_site._server.sockets[0].getsockname()[1]

    try:
        client = JobSubmissionClient.__new__(JobSubmissionClient)
        client._address = f"http://127.0.0.1:{front_port}"
        client._cookies = None
        client._headers = {}
        client._ssl_context = None
        client._server_ray_version = "3.0.0.dev0"

        received = []
        async for chunk in client.tail_job_logs(job_id):
            received.append(chunk)

        assert received == ["hello from job\n"]
        assert call_count == 3
    finally:
        await proxy_session.close()
        await front_runner.cleanup()
        await backend_runner.cleanup()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
