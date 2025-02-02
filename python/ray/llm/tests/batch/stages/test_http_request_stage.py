import pytest
from unittest.mock import AsyncMock, patch
from ray.llm._internal.batch.stages.http_request_stage import HttpRequestUDF


@pytest.fixture
def mock_response():
    async def mock_json():
        return {"response": "test"}

    mock = AsyncMock()
    mock.json = mock_json
    return mock


@pytest.fixture
def mock_session():
    async def mock_post(*args, **kwargs):
        return mock_response()

    mock = AsyncMock()
    mock.post = AsyncMock(side_effect=mock_post)
    return mock


@pytest.mark.asyncio
async def test_http_request_udf_basic():
    udf = HttpRequestUDF(
        data_column="__data",
        url="http://test.com/api",
        additional_header={"Authorization": "Bearer 1234567890"},
        qps=None,
    )

    batch = [{"text": "hello", "metadata": "test"}]

    with patch("aiohttp.ClientSession") as mock_session_cls:
        session = AsyncMock()
        session.post.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"response": "test"}
        )
        mock_session_cls.return_value.__aenter__.return_value = session

        async for result in udf.udf(batch):
            assert result == {"response": "test"}

        session.post.assert_called_once_with(
            "http://test.com/api",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer 1234567890",
            },
            json={"text": "hello", "metadata": "test"},
        )


@pytest.mark.asyncio
async def test_http_request_udf_with_qps():
    udf = HttpRequestUDF(
        data_column="__data",
        url="http://test.com/api",
        qps=2,
    )

    batch = [{"text": "hello1"}, {"text": "hello2"}]

    with patch("aiohttp.ClientSession") as mock_session_cls, patch(
        "time.time"
    ) as mock_time, patch("asyncio.sleep") as mock_sleep:

        session = AsyncMock()
        session.post.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"response": "test"}
        )
        mock_session_cls.return_value.__aenter__.return_value = session

        # Mock time to test QPS limiting. Req2 cannot be sent until 0.5s,
        # so the asyncio.sleep should be called once.
        # [start_time, req1_time, req2_time]
        mock_time.side_effect = [0, 0.1, 0.2]

        results = []
        async for result in udf.udf(batch):
            results.append(result)

        assert len(results) == 2
        assert mock_sleep.called  # Should have called sleep for QPS limiting
