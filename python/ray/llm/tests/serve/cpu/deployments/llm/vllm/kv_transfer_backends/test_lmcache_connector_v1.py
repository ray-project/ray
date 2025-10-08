import sys
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.lmcache_connector_v1 import (
    LMCacheConnectorV1Backend,
)
from ray.serve.llm import LLMConfig


class TestLMCacheConnectorV1Backend:
    @pytest.fixture(autouse=True)
    def mock_lmcache_check(self):
        """Mock the lmcache installation check for all tests."""
        with patch(
            "ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.lmcache_connector_v1._check_lmcache_installed"
        ):
            yield

    @pytest.fixture
    def lmcache_backend_basic(self):
        """Fixture for basic LMCacheConnectorV1Backend."""
        return LMCacheConnectorV1Backend(
            llm_config=LLMConfig(
                model_loading_config=dict(
                    model_id="Qwen/Qwen3-0.6B",
                ),
                engine_kwargs=dict(
                    kv_transfer_config=dict(
                        kv_connector="LMCacheConnectorV1",
                        kv_role="kv_both",
                    )
                ),
            ),
        )

    @pytest.fixture
    def lmcache_backend_with_extra(self):
        """Fixture for LMCacheConnectorV1Backend with extra config."""
        return LMCacheConnectorV1Backend(
            llm_config=LLMConfig(
                model_loading_config=dict(
                    model_id="Qwen/Qwen3-0.6B",
                ),
                engine_kwargs=dict(
                    kv_transfer_config=dict(
                        kv_connector="LMCacheConnectorV1",
                        kv_role="kv_both",
                        kv_connector_extra_config={},
                    )
                ),
            ),
        )

    @pytest.fixture
    def lmcache_backend_with_port(self):
        """Fixture for LMCacheConnectorV1Backend with port config."""
        return LMCacheConnectorV1Backend(
            llm_config=LLMConfig(
                model_loading_config=dict(
                    model_id="Qwen/Qwen3-0.6B",
                ),
                engine_kwargs=dict(
                    kv_transfer_config=dict(
                        kv_connector="LMCacheConnectorV1",
                        kv_role="kv_both",
                        kv_connector_extra_config={
                            "lmcache_rpc_port": LMCacheConnectorV1Backend.DEFAULT_LMCACHE_RPC_PORT_NAME,
                        },
                    )
                ),
            ),
        )

    def test_setup_basic_config(self, lmcache_backend_basic):
        """Test setup with basic configuration (no kv_connector_extra_config)."""
        lmcache_backend_basic.setup()

        # Configuration should remain unchanged
        assert (
            "kv_connector_extra_config" not in lmcache_backend_basic.kv_transfer_config
        )

    def test_setup_with_extra_config_no_port(self, lmcache_backend_with_extra):
        """Test setup with extra config but no lmcache_rpc_port."""
        lmcache_backend_with_extra.setup()

        # Should add lmcache_rpc_port with default DEFAULT_LMCACHE_RPC_PORT_NAME + random string
        assert (
            "lmcache_rpc_port"
            in lmcache_backend_with_extra.kv_transfer_config[
                "kv_connector_extra_config"
            ]
        )
        port_value = lmcache_backend_with_extra.kv_transfer_config[
            "kv_connector_extra_config"
        ]["lmcache_rpc_port"]
        assert port_value.startswith(
            LMCacheConnectorV1Backend.DEFAULT_LMCACHE_RPC_PORT_NAME
        )
        assert len(port_value) > len(
            LMCacheConnectorV1Backend.DEFAULT_LMCACHE_RPC_PORT_NAME
        )  # Should have random string appended

    def test_setup_with_existing_port(self, lmcache_backend_with_port):
        """Test setup with existing lmcache_rpc_port configuration."""
        original_port = lmcache_backend_with_port.kv_transfer_config[
            "kv_connector_extra_config"
        ]["lmcache_rpc_port"]

        lmcache_backend_with_port.setup()

        # Should modify the existing port by appending random string
        new_port = lmcache_backend_with_port.kv_transfer_config[
            "kv_connector_extra_config"
        ]["lmcache_rpc_port"]
        assert new_port.startswith(original_port)
        assert len(new_port) > len(original_port)  # Should have random string appended


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
