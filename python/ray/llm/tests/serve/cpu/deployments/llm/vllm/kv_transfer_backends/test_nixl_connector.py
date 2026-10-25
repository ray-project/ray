import os
import sys
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.engines.vllm.kv_transfer.nixl import (
    NixlConnectorBackend,
)
from ray.serve.exceptions import RayServeException
from ray.serve.llm import LLMConfig
from ray.serve.schema import ReplicaRank


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
        with patch.dict("os.environ", env_vars, clear=True), patch(
            "ray.serve.get_replica_context",
            return_value=self._replica_context(0),
        ):
            nixl_backend.setup()
            assert "VLLM_NIXL_SIDE_CHANNEL_PORT" in os.environ
            assert "VLLM_NIXL_SIDE_CHANNEL_HOST" in os.environ
            assert engine_id in nixl_backend.kv_transfer_config["engine_id"]

    @staticmethod
    def _backend_with_port_base(base: int = 30000) -> NixlConnectorBackend:
        return NixlConnectorBackend(
            llm_config=LLMConfig(
                model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
                engine_kwargs=dict(
                    kv_transfer_config=dict(
                        kv_connector="NixlConnector",
                        kv_role="kv_both",
                    )
                ),
                experimental_configs={"NIXL_SIDE_CHANNEL_PORT_BASE": base},
            ),
        )

    @staticmethod
    def _replica_context(global_rank: int) -> SimpleNamespace:
        """Fake serve.get_replica_context() result with the given global rank."""
        return SimpleNamespace(
            rank=ReplicaRank(rank=global_rank, node_rank=0, local_rank=global_rank)
        )

    def test_default_side_channel_port_uses_configured_base(self):
        """Rank 0 -> zero offset -> port equals the configured base."""
        backend = self._backend_with_port_base(30000)
        with (
            patch.dict("os.environ", {}, clear=True),
            patch(
                "ray.serve.get_replica_context",
                return_value=self._replica_context(0),
            ),
        ):
            backend.setup()
            assert os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] == "30000"

    def test_side_channel_port_offset_uses_replica_rank(self):
        """Nonzero rank shifts the port by rank * num_devices (disjoint port blocks)."""
        backend = self._backend_with_port_base(30000)
        num_devices = backend.llm_config.get_engine_config().num_devices
        with (
            patch.dict("os.environ", {}, clear=True),
            patch(
                "ray.serve.get_replica_context",
                return_value=self._replica_context(2),
            ),
        ):
            backend.setup()
            assert os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] == str(
                30000 + 2 * num_devices
            )

    def test_side_channel_port_requires_replica_context(self):
        """Outside a replica, get_replica_context() raises -> setup fails loudly
        instead of silently using a colliding 0 offset."""
        backend = self._backend_with_port_base(30000)
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(RayServeException):
                backend.setup()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
