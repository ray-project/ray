import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.batch.stages.serve_deployment_stage import (
    ServeDeploymentStageUDF,
)
from ray.serve.llm.openai_api_models import ChatCompletionRequest, CompletionRequest


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
@pytest.mark.parametrize(
    "method,test_data",
    [
        (
            "completions",
            [
                {
                    "method": "completions",
                    "dtype": "CompletionRequest",
                    "request_kwargs": {"prompt": "Hello", "temperature": 0.7},
                },
            ],
        ),
        (
            "chat",
            [
                {
                    "method": "chat",
                    "dtype": "ChatCompletionRequest",
                    "request_kwargs": {
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a helpful assistant",
                            },
                            {"role": "user", "content": "Hello 1"},
                        ]
                    },
                },
            ],
        ),
    ],
)
async def test_serve_deployment_udf_methods(
    mock_serve_deployment_handle, method, test_data
):
    """Test both completions and chat methods."""
    # Create a mock response that will be returned directly
    mock_response = {"test": "response"}

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            yield mock_response

        return mock_async_iterator()

    getattr(mock_serve_deployment_handle, method).remote.side_effect = mock_remote_call

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
            "ChatCompletionRequest": ChatCompletionRequest,
        },
    )

    batch = {"__data": test_data}

    responses = []
    async for response in udf(batch):
        responses.append(response)

    assert len(responses) == 1
    assert "__data" in responses[0]
    assert len(responses[0]["__data"]) == len(test_data)

    for i, item in enumerate(responses[0]["__data"]):
        assert "batch_uuid" in item
        assert "time_taken" in item
        assert item["request_id"] == str(i)
        assert "test" in item  # From the mock response

    assert getattr(mock_serve_deployment_handle, method).remote.call_count == len(
        test_data
    )


@pytest.mark.asyncio
async def test_serve_deployment_invalid_method(mock_serve_deployment_handle):
    """Test that invalid method raises error at runtime."""
    # Set up the mock to simulate a method that doesn't exist
    mock_serve_deployment_handle.invalid_method = None

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
    )

    batch = {
        "__data": [
            {
                "method": "invalid_method",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "Hello", "temperature": 0.7},
            }
        ]
    }

    with pytest.raises(
        ValueError, match="Method invalid_method not found in the serve deployment."
    ):
        async for _ in udf(batch):
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dtype_mapping", [None, {"ChatCompletionRequest": ChatCompletionRequest}]
)
async def test_serve_deployment_missing_dtype(
    mock_serve_deployment_handle, dtype_mapping
):
    """Test that missing dtype raises error at runtime."""

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping=dtype_mapping,
    )

    batch = {
        "__data": [
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "Hello", "temperature": 0.7},
            }
        ]
    }

    with pytest.raises(
        ValueError,
        match="CompletionRequest must be provided in ServeDeploymentProcessorConfig's dtype_mapping.",
    ):
        async for _ in udf(batch):
            pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
