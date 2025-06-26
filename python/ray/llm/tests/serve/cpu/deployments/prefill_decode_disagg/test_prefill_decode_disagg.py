import sys
from unittest.mock import patch

import pytest
from vllm.config import KVTransferConfig
from vllm.platforms.interface import UnspecifiedPlatform

from ray.llm._internal.serve.configs.prompt_formats import Prompt
from ray.llm._internal.serve.configs.server_models import LLMRawResponse
from ray.llm._internal.serve.deployments.prefill_decode_disagg.prefill_decode_disagg import (
    build_app,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockPDDisaggVLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.serve.llm.openai_api_models import ChatCompletionRequest


class TestServingArgsParsing:
    def test_parse_dict(self):
        prefill_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen-0.5b",
                model_source="Qwen/Qwen2.5-0.5B-Instruct",
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=2,
                    max_replicas=2,
                )
            ),
            engine_kwargs=dict(
                tensor_parallel_size=1,
            ),
        )

        decode_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen-0.5b",
                model_source="Qwen/Qwen2.5-0.5B-Instruct",
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=1,
                    max_replicas=1,
                )
            ),
            engine_kwargs=dict(
                tensor_parallel_size=1,
            ),
        )

        pd_config = {"prefill_config": prefill_config, "decode_config": decode_config}

        app = build_app(pd_config)
        assert app is not None


class FakePlatform(UnspecifiedPlatform):
    """
    vllm UnspecifiedPlatform has some interfaces that's left unimplemented, which
    could trigger exception in following tests. So we implement needed interfaces
    and patch.
    """

    def is_async_output_supported(self, enforce_eager: bool) -> bool:
        return True


class TestPDDisaggLLMServer:
    """Test PD-disaggregated LLM server.

    A real P/D disaggregation use case will spawn multiple LLM servers,
    so this test suite just does smoke test and verifies certain expected
    parameters exist in responses.
    """

    @pytest.mark.asyncio
    @patch("vllm.platforms.current_platform", FakePlatform())
    async def test_chat_non_streaming(
        self,
        create_server,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
    ):
        """This is smoke testing that normal chat completion works."""
        llm_config = LLMConfig(
            # Here we
            # 1. want to skip GPU placement in cpu test cases (https://github.com/ray-project/ray/blob/945b9d5dd55c9215d0aeb94a66cfda3b71c2fd43/python/ray/llm/_internal/serve/deployments/llm/vllm/vllm_engine.py#L330)
            # 2. cannot set it to None, otherwise it defaults to use_gpu=True (https://github.com/ray-project/ray/blob/c7e07328c9efbd0d67bf2da4fa098d6492478ef4/python/ray/llm/_internal/serve/deployments/llm/vllm/vllm_models.py#L159)
            # 3. cannot use "CPU" or anything random, which violates the check (https://github.com/ray-project/ray/blob/945b9d5dd55c9215d0aeb94a66cfda3b71c2fd43/python/ray/llm/_internal/serve/configs/server_models.py#L325)
            # so we select a non-NVIDIA type here: Intel-GAUDI.
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
            max_tokens=5,
        )

        # Get the response
        response_stream = await server.chat(request)

        # Collect responses (should be just one)
        responses = [r async for r in response_stream]

        # Check that we got one response
        assert len(responses) == 1
        assert responses[0].choices[0].message.role == "assistant"
        assert (
            responses[0].choices[0].message.content
            == "mock_pd_client_response_0 mock_pd_client_response_1 mock_pd_client_response_2 mock_pd_client_response_3 mock_pd_client_response_4 "
        )

    @pytest.mark.asyncio
    @patch("vllm.platforms.current_platform", FakePlatform())
    async def test_predict_non_streaming(
        self,
        create_server,
        # model_pixtral_12b is a fixture that only contains config files without weights
        model_pixtral_12b,
    ):
        """Test non-streaming predict."""
        llm_config = LLMConfig(
            # Here we
            # 1. want to skip GPU placement in cpu test cases (https://github.com/ray-project/ray/blob/945b9d5dd55c9215d0aeb94a66cfda3b71c2fd43/python/ray/llm/_internal/serve/deployments/llm/vllm/vllm_engine.py#L330)
            # 2. cannot set it to None, otherwise it defaults to use_gpu=True (https://github.com/ray-project/ray/blob/c7e07328c9efbd0d67bf2da4fa098d6492478ef4/python/ray/llm/_internal/serve/deployments/llm/vllm/vllm_models.py#L159)
            # 3. cannot use "CPU" or anything random, which violates the check (https://github.com/ray-project/ray/blob/945b9d5dd55c9215d0aeb94a66cfda3b71c2fd43/python/ray/llm/_internal/serve/configs/server_models.py#L325)
            # so we select a non-NVIDIA type here: Intel-GAUDI.
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
        assert responses[0].generated_text == "mock_pd_client_response_0 "
        assert responses[0].metadata is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
