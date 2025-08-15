import sys
from unittest.mock import MagicMock

import pytest

from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import (
    VLLMEngine,
)


class TestPDDisaggVLLMEngine:
    """Test vLLM engine under PD disagg."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kv_connector", ["NixlConnector", "LMCacheConnectorV1"])
    async def test_pd_disagg_vllm_engine(
        self,
        # llm_config is a fixture defined in serve.tests.conftest.py
        llm_config: LLMConfig,
        kv_connector: str,
        monkeypatch,
    ):
        """Test vLLM engine under PD disagg."""
        if kv_connector == "LMCacheConnectorV1":
            lmcache_mock = MagicMock()
            monkeypatch.setitem(sys.modules, "lmcache", lmcache_mock)
        llm_config = llm_config.model_copy(deep=True)
        llm_config.engine_kwargs.update(
            {
                "kv_transfer_config": dict(
                    kv_connector=kv_connector,
                    kv_role="kv_both",
                ),
            }
        )
        vllm_engine = VLLMEngine(llm_config)
        assert vllm_engine is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
