import sys
from unittest.mock import AsyncMock

import pytest

from ray.llm._internal.serve.configs.constants import MODEL_RESPONSE_BATCH_TIMEOUT_MS
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    ErrorResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    FinishReason,
    LLMConfig,
    LLMRawResponse,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_server import (
    ResponsePostprocessor,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine


async def stream_generator():
    yield LLMRawResponse(
        generated_text="Hello",
        num_generated_tokens=1,
        num_generated_tokens_batch=1,
        num_input_tokens=5,
        finish_reason=None,
    )
    yield LLMRawResponse(
        generated_text=" world",
        num_generated_tokens=1,
        num_generated_tokens_batch=1,
        num_input_tokens=5,
        finish_reason=FinishReason.STOP,
    )


class TestResponsePostprocessor:
    @pytest.mark.asyncio
    async def test_process_chat_streaming(self):
        """Test processing streaming chat responses."""
        postprocessor = ResponsePostprocessor()
        model = "test_model"

        # Process the generator as a streaming chat response
        response_gen = postprocessor.process_chat(
            model, stream_generator(), stream=True
        )

        # Collect all responses
        responses = [resp async for resp in response_gen]

        # Verify we got the expected responses
        assert len(responses) >= 3  # Role message + content chunks + final message
        assert (
            responses[0].choices[0].delta.role == "assistant"
        )  # First message has role
        assert (
            responses[1].choices[0].delta.content == "Hello"
        )  # Second has first chunk
        assert (
            responses[-1].choices[0].finish_reason == "stop"
        )  # Last has finish reason

    @pytest.mark.asyncio
    async def test_process_chat_non_streaming(self):
        """Test processing non-streaming chat responses."""
        postprocessor = ResponsePostprocessor()
        model = "test_model"

        # Process the generator as a non-streaming chat response
        response_gen = postprocessor.process_chat(
            model, stream_generator(), stream=False
        )

        # Collect the single response
        responses = [resp async for resp in response_gen]
        assert len(responses) == 1

        # Verify the content of the response
        response = responses[0]
        assert response.choices[0].message.role == "assistant"
        assert response.choices[0].message.content == "Hello world"
        assert response.choices[0].finish_reason == "stop"
        assert response.usage.prompt_tokens == 5
        assert response.usage.completion_tokens == 2
        assert response.usage.total_tokens == 7

    @pytest.mark.asyncio
    async def test_process_completions_streaming(self):
        """Test processing streaming completion responses."""
        postprocessor = ResponsePostprocessor()
        model = "test_model"

        # Process the generator as a streaming completion response
        response_gen = postprocessor.process_completions(
            model, stream_generator(), stream=True
        )

        # Collect all responses
        responses = [resp async for resp in response_gen]

        # Verify we got the expected responses
        assert len(responses) == 2
        assert responses[0].choices[0].text == "Hello"
        assert responses[0].choices[0].finish_reason is None
        assert responses[1].choices[0].text == " world"
        assert responses[1].choices[0].finish_reason == "stop"

    @pytest.mark.asyncio
    async def test_process_completions_non_streaming(self):
        """Test processing non-streaming completion responses."""
        postprocessor = ResponsePostprocessor()
        model = "test_model"

        # Process the generator as a non-streaming completion response
        response_gen = postprocessor.process_completions(
            model, stream_generator(), stream=False
        )

        # Collect the single response
        responses = [resp async for resp in response_gen]
        assert len(responses) == 1

        # Verify the content of the response
        response = responses[0]
        assert response.choices[0].text == "Hello world"
        assert response.choices[0].finish_reason == "stop"
        assert response.usage.prompt_tokens == 5
        assert response.usage.completion_tokens == 2
        assert response.usage.total_tokens == 7

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in response streams."""
        postprocessor = ResponsePostprocessor()
        model = "test_model"

        # Create a generator that raises an exception

        error_response = ErrorResponse(
            message="Test error",
            code=500,
            internal_message="Test error",
            type="Test error",
            original_exception=Exception("Test error"),
        )

        async def gen():
            yield LLMRawResponse(
                error=error_response,
            )
            yield LLMRawResponse(
                generated_text="Hello",
                num_generated_tokens=1,
                num_generated_tokens_batch=1,
                num_input_tokens=5,
                finish_reason=None,
            )

        # Process the generator as a non-streaming chat response
        response_gen = postprocessor.process_chat(model, gen(), stream=False)

        # Collect the responses, should contain the error
        responses = [resp async for resp in response_gen]
        assert len(responses) == 1
        assert responses[0] == error_response


class TestLLMServer:
    @pytest.mark.asyncio
    async def test_get_batch_interval_ms(self, create_server):
        """Test that the batch interval is set correctly in the config."""

        # Test with a no stream_batching_interval_ms.
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )
        server = await create_server(llm_config, engine_cls=MockVLLMEngine)

        assert server._get_batch_interval_ms() == MODEL_RESPONSE_BATCH_TIMEOUT_MS

        # Test with a non-zero stream_batching_interval_ms.
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs={
                "stream_batching_interval_ms": 13,
            },
        )
        server = await create_server(llm_config, engine_cls=MockVLLMEngine)
        assert server._get_batch_interval_ms() == 13

        # Test with zero stream_batching_interval_ms.
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs={
                "stream_batching_interval_ms": 0,
            },
        )
        server = await create_server(llm_config, engine_cls=MockVLLMEngine)
        assert server._get_batch_interval_ms() == 0

    @pytest.mark.asyncio
    async def test_chat_streaming(self, create_server):
        """Test chat completion in streaming mode."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test_model",
            ),
            experimental_configs={
                # Maximum batching
                "stream_batching_interval_ms": 10000,
            },
        )

        server = await create_server(llm_config, engine_cls=MockVLLMEngine)

        # Create a chat completion request
        request = ChatCompletionRequest(
            model="test_model",
            messages=[dict(role="user", content="Hello")],
            stream=True,
            max_tokens=5,
        )

        # Get the response stream
        response_stream = await server.chat(request)

        # Collect responses from the stream
        responses = []
        async for response in response_stream:
            responses.append(response)

        # Each response should be an iterator over ChatCompletionStreamResponse
        # Check that we got responses
        assert len(responses) > 0

        text = ""
        role = None
        for response in responses:
            assert isinstance(response, list)
            for chunk in response:
                if chunk.choices[0].delta.role is not None and role is None:
                    role = chunk.choices[0].delta.role

                text += chunk.choices[0].delta.content

        assert role == "assistant"
        # What mock vllm engine returns
        assert text == "test_0 test_1 test_2 test_3 test_4 "

    @pytest.mark.asyncio
    async def test_chat_non_streaming(self, create_server):
        """Test non-streaming chat completion."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test_model",
            ),
        )

        server = await create_server(llm_config, engine_cls=MockVLLMEngine)

        # Create a chat completion request
        request = ChatCompletionRequest(
            model="test_model",
            messages=[dict(role="user", content="Hello")],
            stream=False,
            max_tokens=5,
        )

        # Get the response
        response_stream = await server.chat(request)

        # Collect responses (should be just one)
        responses = []
        async for response in response_stream:
            responses.append(response)

        # Check that we got one response
        assert len(responses) == 1
        assert responses[0].choices[0].message.role == "assistant"
        assert (
            responses[0].choices[0].message.content
            == "test_0 test_1 test_2 test_3 test_4 "
        )
        assert responses[0].choices[0].finish_reason == "stop"

    @pytest.mark.asyncio
    async def test_completions_streaming(self, create_server):
        """Test streaming text completion."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test_model",
            ),
            experimental_configs={
                # Maximum batching
                "stream_batching_interval_ms": 10000,
            },
        )

        server = await create_server(llm_config, engine_cls=MockVLLMEngine)

        # Create a completion request
        request = CompletionRequest(
            model="test_model",
            prompt="Hello",
            stream=True,
            max_tokens=5,
        )

        # Get the response stream
        response_stream = await server.completions(request)

        # Collect responses from the stream
        responses = []
        async for response in response_stream:
            responses.append(response)

        # Check that we got responses
        assert len(responses) > 0

        text = ""
        for response in responses:
            assert isinstance(response, list)
            for chunk in response:
                text += chunk.choices[0].text

        assert text == "test_0 test_1 test_2 test_3 test_4 "

    @pytest.mark.asyncio
    async def test_completions_non_streaming(self, create_server):
        """Test non-streaming text completion."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test_model",
            ),
        )

        server = await create_server(llm_config, engine_cls=MockVLLMEngine)

        # Create a completion request
        request = CompletionRequest(
            model="test_model",
            prompt="Hello",
            stream=False,
            max_tokens=5,
        )

        # Get the response
        response_stream = await server.completions(request)

        # Collect responses (should be just one)
        responses = []
        async for response in response_stream:
            responses.append(response)

        # Check that we got one response
        assert len(responses) == 1
        assert responses[0].choices[0].text == "test_0 test_1 test_2 test_3 test_4 "
        assert responses[0].choices[0].finish_reason == "stop"

    @pytest.mark.asyncio
    async def test_check_health(self, create_server):
        """Test health check functionality."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test_model",
            ),
        )

        # Create a server with a mocked engine
        server = await create_server(llm_config, engine_cls=MockVLLMEngine)

        # Mock the engine's check_health method
        server.engine.check_health = AsyncMock(return_value=None)

        # Perform the health check, no exceptions should be raised
        await server.check_health()
        server.engine.check_health.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self, create_server):
        """Test error handling in the server."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test_model",
            ),
        )

        server = await create_server(llm_config, engine_cls=MockVLLMEngine)

        # Mock the _predict method to raise an exception
        server._predict = AsyncMock(side_effect=Exception("Test error"))

        # Create a chat completion request
        request = ChatCompletionRequest(
            model="test_model",
            messages=[dict(role="user", content="Hello")],
            stream=False,
        )

        # Get the response
        response_stream = await server.chat(request)

        # Collect responses (should contain an error)
        responses = []
        async for response in response_stream:
            responses.append(response)

        # Check that we got an error response
        assert len(responses) > 0
        assert isinstance(responses[0], ErrorResponse)

        # Internal server error
        assert responses[0].code == 500


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
