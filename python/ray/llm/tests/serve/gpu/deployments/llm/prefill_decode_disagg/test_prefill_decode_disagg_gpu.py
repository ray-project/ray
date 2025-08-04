import sys

import pytest
from vllm.config import KVTransferConfig

from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import (
    VLLMEngine,
)


class TestPDDisaggVLLMEngine:
    """Test vLLM engine under PD disagg."""

    @pytest.mark.asyncio
    async def test_pd_disagg_vllm_engine(
        self,
        # llm_config is a fixture defined in serve.tests.conftest.py
        llm_config: LLMConfig,
    ):
        """Test vLLM engine under PD disagg."""
        llm_config = llm_config.model_copy(deep=True)
        llm_config.engine_kwargs.update(
            {
                "kv_transfer_config": KVTransferConfig(
                    kv_connector="NixlConnector",
                    kv_role="kv_both",
                ),
            }
        )
        vllm_engine = VLLMEngine(llm_config)
        assert vllm_engine is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
