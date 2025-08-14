import asyncio
import sys
from unittest.mock import AsyncMock, call, patch

import aiohttp
import aiohttp.web_exceptions
import pytest

from ray.llm._internal.batch.stages.http_request_stage import HttpRequestUDF


@pytest.fixture
def mock_response():
    mock = AsyncMock()
    mock.json = AsyncMock(return_value={"response": "test"})
    return mock


@pytest.fixture
def mock_session(mock_response):
    session = AsyncMock()
    session.post.return_value.__aenter__.return_value = mock_response
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    return session_cm


@pytest.mark.asyncio
async def test_http_request_udf_basic(mock_session):
    udf = HttpRequestUDF(
        data_column="__data",
        expected_input_keys=["payload"],
        url="http://test.com/api",
        additional_header={"Authorization": "Bearer 1234567890"},
        qps=None,
        session_factory=lambda: mock_session,  # noqa: E731
    )

    batch = {"__data": [{"payload": {"text": "hello", "metadata": "test"}}]}

    async for result in udf(batch):
        assert result["__data"][0]["http_response"]["response"] == "test"

    mock_session.__aenter__.return_value.post.assert_called_once_with(
        "http://test.com/api",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer 1234567890",
        },
        json={"text": "hello", "metadata": "test"},
    )


@pytest.mark.asyncio
async def test_http_request_udf_with_qps(mock_session):
    udf = HttpRequestUDF(
        data_column="__data",
        expected_input_keys=["payload"],
        url="http://test.com/api",
        qps=2,
        session_factory=lambda: mock_session,  # noqa: E731
    )

    batch = {
        "__data": [{"payload": {"text": "hello1"}}, {"payload": {"text": "hello2"}}]
    }

    with patch("time.time") as mock_time, patch("asyncio.sleep") as mock_sleep:
        # Mock time to test QPS limiting. Req2 cannot be sent until 0.5s,
        # so the asyncio.sleep should be called once.
        # [start_time, req1_time, req2_time]
        mock_time.side_effect = [0, 0.1, 0.2]

        results = []
        async for result in udf(batch):
            results.extend(result["__data"])

        assert len(results) == 2
        assert mock_sleep.called  # Should have called sleep for QPS limiting


@pytest.mark.asyncio
async def test_http_request_udf_with_retry(mock_response):
    batch = {
        "__data": [{"payload": {"text": "hello1"}}, {"payload": {"text": "hello2"}}]
    }
    # Create a fake session
    # create another response
    retry_resp = AsyncMock(status=429)
    retry_resp.json = AsyncMock(return_value={"detail": "Too Many Requests"})

    session = AsyncMock()
    session.post.return_value.__aenter__.side_effect = [
        mock_response,  # First request: success
        asyncio.TimeoutError(),  # Second request, initial attempt: timeout
        aiohttp.ClientConnectionError(),  # Second request, first retry: connection error
        retry_resp,  # Second request, second retry: HTTP 429 error
        mock_response,  # Final retry: success
    ]
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    fake_session_factory = lambda: session_cm  # noqa: E731

    udf = HttpRequestUDF(
        data_column="__data",
        expected_input_keys=["payload"],
        url="http://test.com/api",
        max_retries=3,
        base_retry_wait_time_in_s=1,
        session_factory=fake_session_factory,
    )

    with patch("asyncio.sleep") as mock_sleep:
        results = []
        async for result in udf(batch):
            results.extend(result["__data"])

        assert len(results) == 2
        mock_sleep.assert_called()
        mock_sleep.assert_has_calls(
            [
                call(udf.base_retry_wait_time_in_s),
                call(udf.base_retry_wait_time_in_s * 2),
                call(udf.base_retry_wait_time_in_s * 2 * 2),
            ]
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
