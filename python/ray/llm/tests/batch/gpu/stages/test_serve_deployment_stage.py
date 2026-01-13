import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.exceptions import RayActorError
from ray.llm._internal.batch.stages.serve_deployment_stage import (
    ServeDeploymentStageUDF,
)
from ray.serve._private.common import DeploymentID
from ray.serve.exceptions import BackPressureError, DeploymentUnavailableError
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


# ============================================================================
# Error handling tests for should_continue_on_error
# ============================================================================


@pytest.mark.asyncio
async def test_serve_udf_default_raises_on_error(mock_serve_deployment_handle):
    """Default behavior (should_continue_on_error=False) raises on inference error."""

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            raise ValueError("prompt too long")
            yield  # Make it a generator

        return mock_async_iterator()

    mock_serve_deployment_handle.completions.remote.side_effect = mock_remote_call

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={"CompletionRequest": CompletionRequest},
        should_continue_on_error=False,
    )

    batch = {
        "__data": [
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "test", "temperature": 0.7},
            }
        ]
    }

    with pytest.raises(ValueError, match="prompt too long"):
        async for _ in udf(batch):
            pass


@pytest.mark.asyncio
async def test_serve_udf_continue_on_error_yields_error_row(
    mock_serve_deployment_handle,
):
    """With should_continue_on_error=True, errors yield rows with __inference_error__."""

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            raise ValueError("prompt too long")
            yield  # Make it a generator

        return mock_async_iterator()

    mock_serve_deployment_handle.completions.remote.side_effect = mock_remote_call

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={"CompletionRequest": CompletionRequest},
        should_continue_on_error=True,
    )

    batch = {
        "__data": [
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "test prompt", "temperature": 0.7},
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 1
    assert "__inference_error__" in results[0]
    assert "ValueError" in results[0]["__inference_error__"]
    assert "prompt too long" in results[0]["__inference_error__"]
    # Error rows include request_kwargs snippet for debuggability
    assert "request_kwargs" in results[0]


@pytest.mark.asyncio
async def test_serve_udf_mixed_success_and_error(mock_serve_deployment_handle):
    """Mixed batch: some rows succeed, some fail."""
    call_count = 0

    def mock_remote_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        current_call = call_count

        async def mock_async_iterator():
            # Second call fails
            if current_call == 2:
                raise ValueError("prompt too long")
            yield {"generated_text": f"Response {current_call}"}

        return mock_async_iterator()

    mock_serve_deployment_handle.completions.remote.side_effect = mock_remote_call

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={"CompletionRequest": CompletionRequest},
        should_continue_on_error=True,
    )

    batch = {
        "__data": [
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "first", "temperature": 0.7},
            },
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "second", "temperature": 0.7},
            },
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "third", "temperature": 0.7},
            },
        ]
    }

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 3

    errors = [r for r in results if r.get("__inference_error__") is not None]
    successes = [r for r in results if r.get("__inference_error__") is None]

    assert len(errors) == 1
    assert len(successes) == 2
    assert "ValueError" in errors[0]["__inference_error__"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fatal_error",
    [
        RayActorError(error_msg="Actor died"),
        BackPressureError(num_queued_requests=100, max_queued_requests=50),
        DeploymentUnavailableError(
            deployment_id=DeploymentID(name="test", app_name="test_app")
        ),
    ],
)
async def test_serve_udf_fatal_errors_always_propagate(
    mock_serve_deployment_handle, fatal_error
):
    """Fatal errors (RayActorError, BackPressureError, etc.) always propagate."""

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            raise fatal_error
            yield  # Make it a generator

        return mock_async_iterator()

    mock_serve_deployment_handle.completions.remote.side_effect = mock_remote_call

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={"CompletionRequest": CompletionRequest},
        should_continue_on_error=True,  # Even with this True, fatal errors propagate
    )

    batch = {
        "__data": [
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "test", "temperature": 0.7},
            }
        ]
    }

    with pytest.raises(type(fatal_error)):
        async for _ in udf(batch):
            pass


@pytest.mark.asyncio
async def test_serve_udf_unknown_errors_propagate(mock_serve_deployment_handle):
    """Unknown errors propagate even with should_continue_on_error=True."""

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            raise RuntimeError("unexpected system error")
            yield

        return mock_async_iterator()

    mock_serve_deployment_handle.completions.remote.side_effect = mock_remote_call

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={"CompletionRequest": CompletionRequest},
        should_continue_on_error=True,
    )

    batch = {
        "__data": [
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "test", "temperature": 0.7},
            }
        ]
    }

    with pytest.raises(RuntimeError, match="unexpected system error"):
        async for _ in udf(batch):
            pass


@pytest.mark.asyncio
async def test_serve_udf_success_with_continue_on_error_includes_none_error(
    mock_serve_deployment_handle,
):
    """Successful rows with should_continue_on_error=True have __inference_error__=None."""
    mock_response = {"generated_text": "Hello!"}

    def mock_remote_call(*args, **kwargs):
        async def mock_async_iterator():
            yield mock_response

        return mock_async_iterator()

    mock_serve_deployment_handle.completions.remote.side_effect = mock_remote_call

    udf = ServeDeploymentStageUDF(
        data_column="__data",
        expected_input_keys=["method", "request_kwargs"],
        deployment_name="test_deployment",
        app_name="test_app",
        dtype_mapping={"CompletionRequest": CompletionRequest},
        should_continue_on_error=True,
    )

    batch = {
        "__data": [
            {
                "method": "completions",
                "dtype": "CompletionRequest",
                "request_kwargs": {"prompt": "test", "temperature": 0.7},
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 1
    assert results[0]["__inference_error__"] is None
    assert results[0]["generated_text"] == "Hello!"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
