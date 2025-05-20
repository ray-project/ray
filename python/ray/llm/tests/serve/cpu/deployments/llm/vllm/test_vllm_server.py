import pytest
import sys
import unittest
from vllm.config import KVTransferConfig
from vllm.envs import set_vllm_use_v1, VLLM_USE_V1

from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.serve.llm.openai_api_models import ChatCompletionRequest
from ray.llm._internal.serve.configs.prompt_formats import Prompt
from ray.llm._internal.serve.configs.server_models import LLMRawResponse
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockPDDisaggVLLMEngine
from ray.llm.tests.serve.cpu.deployments.utils.test_utils import create_server


class TestPDDisaggLLMServer:
    """Test PD-disaggregated LLM server.

    A real P/D disaggregation use case will spawn multiple LLM servers,
    so this test suite just does smoke test and verifies certain expected
    parameters exist in responses.
    """

    @pytest.mark.asyncio
    async def test_chat_non_streaming(
        self,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
    ):
        """This is smoke testing that normal chat completion works."""
        if not VLLM_USE_V1:
            # Cannot call set_vllm_use_v1() twice.
            set_vllm_use_v1(True)
        llm_config = LLMConfig(
            # Put a non-NVIDIA type here to avoid GPU placement.
            accelerator_type="Intel-GAUDI",
            model_loading_config=ModelLoadingConfig(
                model_id=model_pixtral_12b,
            ),
            engine_kwargs={
                "kv_transfer_config": KVTransferConfig(
                    kv_connector="NixlConnector",
                    kv_role="kv_both",
                ),
            },
        )

        server = await create_server(llm_config, engine_cls=MockPDDisaggVLLMEngine)

        # Create a chat completion request
        request = ChatCompletionRequest(
            model="test_model",
            messages=[dict(role="user", content="Hello")],
            stream=False,
            max_tokens=1,
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
        assert responses[0].choices[0].message.content == "test_0 "

    @pytest.mark.asyncio
    async def test_predict_non_streaming(
        self,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
    ):
        """Test non-streaming predict."""
        if not VLLM_USE_V1:
            # Cannot call set_vllm_use_v1() twice.
            set_vllm_use_v1(True)
        llm_config = LLMConfig(
            # Put a non-NVIDIA type here to avoid GPU placement.
            accelerator_type="Intel-GAUDI",
            model_loading_config=ModelLoadingConfig(
                model_id=model_pixtral_12b,
            ),
            engine_kwargs={
                "kv_transfer_config": KVTransferConfig(
                    kv_connector="NixlConnector",
                    kv_role="kv_both",
                ),
            },
        )

        server = await create_server(llm_config, engine_cls=MockPDDisaggVLLMEngine)

        # Create a predict request
        request = Prompt(
            prompt="test prompt",
            parameters=dict(
                max_tokens=1,
                stream=False,
                kv_transfer_params=dict(field_that_does_not_matter="1"),
            ),
        )

        # Get the response
        responses: list[LLMRawResponse] = []
        async for response in server._predict(
            request_id="test_request_id", prompt=request, stream=False
        ):
            responses.append(response)

        # Collect responses (should be just one)
        assert len(responses) == 1
        assert responses[0].generated_text == "test_0 "
        assert responses[0].metadata is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
