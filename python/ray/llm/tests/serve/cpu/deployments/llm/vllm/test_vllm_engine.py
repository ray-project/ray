import asyncio
import json
import sys
from types import SimpleNamespace
from typing import List
from unittest.mock import Mock, patch

import pytest

from ray.llm._internal.serve.configs.openai_api_models import ChatCompletionRequest
from ray.llm._internal.serve.configs.server_models import (
    FinishReason,
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import (
    VLLMEngine,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMGenerationRequest,
    VLLMSamplingParams,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockPDDisaggVLLMEngine
from ray.serve.llm.openai_api_models import CompletionRequest


class FakeVLLMEngine:
    def __init__(self, mock: Mock, output=None):
        self._engine_client = mock

        self._output = output or []
        self.num_generated = 0

    async def generate(self, *args, **kwargs):
        # Record the call
        self._engine_client.generate(*args, **kwargs)

        for x in self._output:
            await asyncio.sleep(0.01)
            self.num_generated += 1
            yield x

    async def abort(self, request_id: str):
        # Record the call
        self._engine_client.abort(request_id)

    def _abort(self, request_id: str, **kwargs):
        # Record the call
        self._engine_client.abort(request_id)


def get_fake_responses(*tokens: List[str]):
    total = ""
    output = []

    for token in tokens:
        total += token
        # For some reason vLLM appears to return the full text on each iteration
        # We should fix this in vllm
        output.append(
            SimpleNamespace(
                outputs=[
                    SimpleNamespace(
                        text=total,
                        finish_reason="stop",  # for some reason, vllm returns a finish reason on all tokens. We should fix this too.
                        token_ids=[0],
                        logprobs=[],
                    )
                ],
                prompt_token_ids=[0],
                metrics=SimpleNamespace(time_in_queue=0.01),
            )
        )

    return output


def get_fake_engine_and_request(llm_config: LLMConfig, expected_out: List[str]):
    vllm_engine = VLLMEngine(llm_config)
    # We normally set the model config when calling VLLMEngine.start()
    vllm_engine.model_config = Mock()
    vllm_engine.model_config.max_model_len = 1

    engine_mock = Mock()
    vllm_engine._engine_client = FakeVLLMEngine(
        engine_mock, get_fake_responses(*expected_out)
    )

    req = VLLMGenerationRequest(
        prompt="prompt",
        request_id="req_id",
        sampling_params=VLLMSamplingParams(),
        disk_multiplex_config=None,
        stream=True,
    )
    return vllm_engine, req, engine_mock


class TestVLLMEngine:
    """Test the VLLMEngine."""

    # Here we
    # 1. want to skip GPU placement in cpu test cases (https://github.com/ray-project/ray/blob/945b9d5dd55c9215d0aeb94a66cfda3b71c2fd43/python/ray/llm/_internal/serve/deployments/llm/vllm/vllm_engine.py#L330)
    # 2. cannot set it to None, otherwise it defaults to use_gpu=True (https://github.com/ray-project/ray/blob/c7e07328c9efbd0d67bf2da4fa098d6492478ef4/python/ray/llm/_internal/serve/deployments/llm/vllm/vllm_models.py#L159)
    # 3. cannot use "CPU" or anything random, which violates the check (https://github.com/ray-project/ray/blob/945b9d5dd55c9215d0aeb94a66cfda3b71c2fd43/python/ray/llm/_internal/serve/configs/server_models.py#L325)
    # so we select a non-NVIDIA type here: Intel-GAUDI.
    # Use str() such that it's not a literal string which violates pydantic check.
    accelerator_type = str("Intel-GAUDI")

    @pytest.mark.asyncio
    async def test_generate(self, llm_config):
        expected_out = ["hi ", "i ", "am ", "vllm."]
        vllm_engine, req, engine_mock = get_fake_engine_and_request(
            llm_config, expected_out
        )

        cur_idx = 0
        async for x in vllm_engine.generate(req):
            if cur_idx < len(expected_out):
                assert x.generated_text == expected_out[cur_idx]
                cur_idx += 1
                assert x.generation_time == pytest.approx(
                    0.01, abs=0.01
                ), "We are sleeping for this long before returning tokens in the fake"
                assert (
                    x.num_input_tokens == 1
                ), "We are setting the num input tokens to len 1 in the fake output"
            else:
                assert x.finish_reason == FinishReason.STOP

        await asyncio.sleep(0.02)  # wait for asyncio task scheduling

        # Abort should be called
        engine_mock.abort.assert_called_once_with("req_id")

    @pytest.mark.asyncio
    async def test_vllm_engine_error_in_caller(self, llm_config):
        expected_out = ["hi ", "i ", "am ", "vllm."]
        vllm_engine, req, engine_mock = get_fake_engine_and_request(
            llm_config, expected_out
        )

        with pytest.raises(RuntimeError):
            async for _x in vllm_engine.generate(req):
                raise RuntimeError()

        await asyncio.sleep(0.02)  # wait for asyncio task scheduling
        # Abort should be called
        engine_mock.abort.assert_called_once_with("req_id")

    @pytest.mark.asyncio
    async def test_vllm_engine_caller_cancellation(self, llm_config):
        expected_out = ["hi ", "i ", "am ", "vllm.", "and more"] * 10  # many tokens
        vllm_engine, req, engine_mock = get_fake_engine_and_request(
            llm_config, expected_out
        )

        async def run():
            async for x in vllm_engine.generate(req):
                print(x)

        task = asyncio.create_task(run())
        await asyncio.sleep(0.02)  # wait for some tokens to be returned

        # Cancel the task
        task.cancel()

        await asyncio.sleep(0.02)  # wait for asyncio task scheduling
        # Abort should be called
        engine_mock.abort.assert_called_once_with("req_id")
        assert (
            vllm_engine._engine_client.num_generated <= 4
        ), "We should have generated not more than 4 tokens"

    @pytest.mark.parametrize("enable_json_mode", [True, False])
    def test_parse_sampling_params_json_mode(
        self, llm_config: LLMConfig, enable_json_mode: bool
    ):
        # Make a deep copy to avoid modifying the session-scoped fixture
        llm_config = llm_config.model_copy(deep=True)
        vllm_engine = VLLMEngine(llm_config)

        # Mock model_config to avoid None errors
        vllm_engine.model_config = Mock()
        vllm_engine.model_config.max_model_len = 1000

        # Create sampling params with response format
        sampling_params = VLLMSamplingParams(
            response_format={
                "type": "json_object",
                "schema": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                },
            }
        )

        # Parse the sampling params
        parsed_params = vllm_engine._parse_sampling_params(sampling_params)

        # For both cases we should now have guided decoding since we are using oss vllm.
        # When json_mode is disabled, guided_decoding should be used instead
        assert hasattr(parsed_params, "guided_decoding")
        # Parse the JSON string from guided_decoding into a dict
        guided_json = json.loads(parsed_params.guided_decoding.json)
        assert guided_json == sampling_params.response_format.json_schema
        assert getattr(parsed_params, "response_format", None) is None

    @pytest.mark.asyncio
    async def test_chat_streaming(
        self,
        # create_server is a fixture defined in llm.tests.serve.conftest.py
        create_server,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
        vllm_cpu_platform,
    ):
        """Test chat completion in streaming mode."""
        with patch("vllm.platforms.current_platform", vllm_cpu_platform()):  # type: ignore
            llm_config = LLMConfig(
                accelerator_type=self.accelerator_type,
                model_loading_config=ModelLoadingConfig(
                    model_id=model_pixtral_12b,
                ),
                experimental_configs={
                    # Maximum batching
                    "stream_batching_interval_ms": 10000,
                },
            )

            server = await create_server(llm_config, engine_cls=MockPDDisaggVLLMEngine)

        # Create a chat completion request
        request = ChatCompletionRequest(
            model=model_pixtral_12b,
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
        assert (
            text
            == "mock_pd_client_response_0 mock_pd_client_response_1 mock_pd_client_response_2 mock_pd_client_response_3 mock_pd_client_response_4 "
        )

    @pytest.mark.asyncio
    async def test_chat_non_streaming(
        self,
        # create_server is a fixture defined in llm.tests.serve.conftest.py
        create_server,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
        vllm_cpu_platform,
    ):
        """Test non-streaming chat completion."""
        with patch("vllm.platforms.current_platform", vllm_cpu_platform()):  # type: ignore
            llm_config = LLMConfig(
                accelerator_type=self.accelerator_type,
                model_loading_config=ModelLoadingConfig(
                    model_id=model_pixtral_12b,
                ),
            )

            server = await create_server(llm_config, engine_cls=MockPDDisaggVLLMEngine)

        # Create a chat completion request
        request = ChatCompletionRequest(
            model=model_pixtral_12b,
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
            == "mock_pd_client_response_0 mock_pd_client_response_1 mock_pd_client_response_2 mock_pd_client_response_3 mock_pd_client_response_4 "
        )
        assert responses[0].choices[0].finish_reason == "stop"

    @pytest.mark.parametrize("prompt", ["Hello", [1, 2, 3]])
    @pytest.mark.asyncio
    async def test_completions_streaming(
        self,
        # create_server is a fixture defined in llm.tests.serve.conftest.py
        create_server,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
        vllm_cpu_platform,
        prompt,
    ):
        """Test streaming text completion."""
        with patch("vllm.platforms.current_platform", vllm_cpu_platform()):  # type: ignore
            llm_config = LLMConfig(
                accelerator_type=self.accelerator_type,
                model_loading_config=ModelLoadingConfig(
                    model_id=model_pixtral_12b,
                ),
                experimental_configs={
                    # Maximum batching
                    "stream_batching_interval_ms": 10000,
                },
            )

            server = await create_server(llm_config, engine_cls=MockPDDisaggVLLMEngine)

        # Create a completion request
        request = CompletionRequest(
            model=model_pixtral_12b,
            prompt=prompt,
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

        assert (
            text
            == "mock_pd_client_response_0 mock_pd_client_response_1 mock_pd_client_response_2 mock_pd_client_response_3 mock_pd_client_response_4 "
        )

    @pytest.mark.parametrize("prompt", ["Hello", [1, 2, 3]])
    @pytest.mark.asyncio
    async def test_completions_non_streaming(
        self,
        # create_server is a fixture defined in llm.tests.serve.conftest.py
        create_server,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
        vllm_cpu_platform,
        prompt,
    ):
        """Test non-streaming text completion."""
        with patch("vllm.platforms.current_platform", vllm_cpu_platform()):  # type: ignore
            llm_config = LLMConfig(
                accelerator_type=self.accelerator_type,
                model_loading_config=ModelLoadingConfig(
                    model_id=model_pixtral_12b,
                ),
            )

            server = await create_server(llm_config, engine_cls=MockPDDisaggVLLMEngine)

        # Create a completion request
        request = CompletionRequest(
            model=model_pixtral_12b,
            prompt=prompt,
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
        assert (
            responses[0].choices[0].text
            == "mock_pd_client_response_0 mock_pd_client_response_1 mock_pd_client_response_2 mock_pd_client_response_3 mock_pd_client_response_4 "
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
