import os
import sys
import uuid
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.engines.vllm.kv_transfer.nixl import (
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

    def test_default_side_channel_port_uses_configured_base(self):
        """When VLLM_NIXL_SIDE_CHANNEL_PORT is not set, the port is the
        configured base plus _compute_port_offset(). Outside a Serve
        replica context (e.g. unit tests) the offset is 0."""
        backend = NixlConnectorBackend(
            llm_config=LLMConfig(
                model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
                engine_kwargs=dict(
                    kv_transfer_config=dict(
                        kv_connector="NixlConnector",
                        kv_role="kv_both",
                    )
                ),
                experimental_configs={"NIXL_SIDE_CHANNEL_PORT_BASE": 30000},
            ),
        )
        with patch.dict("os.environ", {}, clear=True):
            backend.setup()
            assert os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] == "30000"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
