import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ray.llm._internal.batch.stages.serve_deployment_stage import (
    ServeDeploymentStage,
    ServeDeploymentStageUDF,
)
from ray.llm._internal.serve.configs.openai_api_models import (
    CompletionRequest,
    ChatCompletionRequest,
)


@pytest.fixture
def mock_serve_deployment_handle():
    """Mock the serve deployment handle and its methods."""
    with patch("ray.serve.get_deployment_handle") as mock_get_handle:
        mock_handle = MagicMock()
        mock_handle.options.return_value = mock_handle

        # Mock the chat and completions methods
        mock_handle.chat = MagicMock()
        mock_handle.completions = MagicMock()

        mock_get_handle.return_value = mock_handle
        yield mock_handle


@pytest.mark.asyncio
async def test_serve_deployment_udf_completions(mock_serve_deployment_handle):
    mock_response = MagicMock()
    mock_response.model_dump.return_value = {"test": "response"}

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            yield mock_response

        return mock_async_iterator()

    mock_serve_deployment_handle.completions.remote.side_effect = mock_remote_call

    # Create UDF instance with completions method
    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["prompt"],
        deployment_name="test_deployment",
        app_name="test_app",
        method="completions",
    )

    assert udf._method == "completions"
    assert udf._request_type == CompletionRequest

    # Test batch processing
    batch = {
        "__data": [
            {"prompt": "Hello", "temperature": 0.7},
            {"prompt": "World", "temperature": 0.8},
            {"prompt": "Test", "temperature": 0.9},
        ]
    }

    responses = []
    async for response in udf(batch):
        responses.append(response)

    # The UDF returns a single response containing all the data
    assert len(responses) == 1
    assert "__data" in responses[0]
    assert len(responses[0]["__data"]) == 3

    # Check that each item in the response has the expected fields
    for i, item in enumerate(responses[0]["__data"]):
        assert "batch_uuid" in item
        assert "time_taken_llm" in item
        assert item["request_id"] == str(i)
        assert "test" in item  # From the mock response

    assert mock_serve_deployment_handle.completions.remote.call_count == 3


@pytest.mark.asyncio
async def test_serve_deployment_udf_chat(mock_serve_deployment_handle):
    mock_response = MagicMock()
    mock_response.model_dump.return_value = {"test": "response"}

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            yield mock_response

        return mock_async_iterator()

    mock_serve_deployment_handle.chat.remote.side_effect = mock_remote_call

    # Create UDF instance with chat method
    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        deployment_name="test_deployment",
        app_name="test_app",
        method="chat",
    )

    assert udf._method == "chat"
    assert udf._request_type == ChatCompletionRequest
    assert udf.request_id == 0

    # Test batch processing
    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant"},
                    {"role": "user", "content": "Hello 1"},
                ]
            },
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant"},
                    {"role": "user", "content": "Hello 2"},
                ]
            },
        ]
    }

    responses = []
    async for response in udf(batch):
        responses.append(response)

    # The UDF returns a single response containing all the data
    assert len(responses) == 1
    assert "__data" in responses[0]
    assert len(responses[0]["__data"]) == 2

    # Check that each item in the response has the expected fields
    for i, item in enumerate(responses[0]["__data"]):
        assert "batch_uuid" in item
        assert "time_taken_llm" in item
        assert item["request_id"] == str(i)
        assert "test" in item  # From the mock response

    assert mock_serve_deployment_handle.chat.remote.call_count == 2


@pytest.mark.asyncio
async def test_serve_deployment_invalid_method(mock_serve_deployment_handle):
    with pytest.raises(ValueError, match="Unsupported method: invalid_method"):
        ServeDeploymentStageUDF(
            data_column="__data",
            expected_input_keys=["prompt"],
            deployment_name="test_deployment",
            app_name="test_app",
            method="invalid_method",
        )

    mock_serve_deployment_handle.invalid_method = None
    with pytest.raises(
        ValueError,
        match="Method invalid_method is not supported by the serve deployment.",
    ):
        ServeDeploymentStageUDF(
            data_column="__data",
            expected_input_keys=["prompt"],
            deployment_name="test_deployment",
            app_name="test_app",
            method="invalid_method",
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
