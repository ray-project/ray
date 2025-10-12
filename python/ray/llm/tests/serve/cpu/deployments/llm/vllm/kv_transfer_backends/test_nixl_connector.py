import os
import sys
import uuid
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.nixl_connector import (
    NixlConnectorBackend,
)
from ray.serve.llm import LLMConfig


@pytest.fixture
def engine_id():
    """Fixture for the engine ID."""
    return str(uuid.uuid4())


class TestNixlConnectorBackend:
    @pytest.fixture
    def nixl_backend(self, engine_id: str):
        """Fixture for the NixlConnectorBackend."""
        return NixlConnectorBackend(
            llm_config=LLMConfig(
                model_loading_config=dict(
                    model_id="Qwen/Qwen3-0.6B",
                ),
                engine_kwargs=dict(
                    kv_transfer_config=dict(
                        kv_connector="NixlConnector",
                        kv_role="kv_both",
                        engine_id=engine_id,
                    )
                ),
            ),
        )

    @pytest.mark.parametrize(
        "env_vars",
        [
            {},
            {"VLLM_NIXL_SIDE_CHANNEL_PORT": "8080"},
            {"VLLM_NIXL_SIDE_CHANNEL_HOST": "127.0.0.1"},
            {
                "VLLM_NIXL_SIDE_CHANNEL_PORT": "8080",
                "VLLM_NIXL_SIDE_CHANNEL_HOST": "127.0.0.1",
            },
        ],
    )
    def test_setup_environment_variables(self, nixl_backend, env_vars, engine_id: str):
        """Test that setup configures environment variables and overrides engine_id correctly."""
        with patch.dict("os.environ", env_vars, clear=True):
            nixl_backend.setup()
            assert "VLLM_NIXL_SIDE_CHANNEL_PORT" in os.environ
            assert "VLLM_NIXL_SIDE_CHANNEL_HOST" in os.environ
            assert engine_id in nixl_backend.kv_transfer_config["engine_id"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
