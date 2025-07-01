import sys

import pytest

from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import (
    VLLMEngine,
    _get_vllm_engine_config,
)


class TestVLLMEngine:
    """Test the VLLMEngine."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "engine_kwargs, expected_prompt_limit",
        [
            ({"enable_chunked_prefill": True}, 1024000),
            (
                {
                    "enable_chunked_prefill": True,
                    "max_model_len": 999,
                },
                999,
            ),
            (
                {
                    "enable_chunked_prefill": True,
                    "max_num_batched_tokens": 888,
                },
                1024000,
            ),
            (
                {
                    "enable_chunked_prefill": True,
                    "max_model_len": 999,
                    "max_num_batched_tokens": 888,
                    "enforce_eager": True,
                },
                999,
            ),
            ({"enable_chunked_prefill": False}, 1024000),
            (
                {
                    "enable_chunked_prefill": False,
                    "max_model_len": 999,
                },
                999,
            ),
        ],
    )
    async def test_get_prompt_limit(
        # llm_config is a fixture defined in serve.tests.conftest.py
        self,
        llm_config: LLMConfig,
        engine_kwargs: dict,
        expected_prompt_limit: int,
    ):
        llm_config = llm_config.model_copy(deep=True)
        vllm_engine = VLLMEngine(llm_config)

        # Test with default engine kwargs
        llm_config.engine_kwargs = engine_kwargs
        _, vllm_config = _get_vllm_engine_config(llm_config)
        vllm_engine.vllm_config = vllm_config
        assert vllm_engine._get_prompt_limit() == expected_prompt_limit


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
