"""Tests for the NIXL connector backend setup method."""

import os
import uuid
from unittest.mock import patch

import pytest
from vllm.config import KVTransferConfig

from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.nixl_connector import (
    NixlConnectorBackend,
)


class TestNixlConnectorBackend:

    ENGINE_ID = str(uuid.uuid4())

    @pytest.fixture
    def nixl_backend(self):
        return NixlConnectorBackend(
            KVTransferConfig(
                kv_connector="NixlConnector",
                kv_role="kv_both",
                engine_id=self.ENGINE_ID,
            )
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
    def test_setup_environment_variables(self, nixl_backend, env_vars):
        """Test that setup configures environment variables and overrides engine_id correctly."""
        with patch.dict("os.environ", env_vars, clear=True):
            nixl_backend.setup()
            assert "VLLM_NIXL_SIDE_CHANNEL_PORT" in os.environ
            assert "VLLM_NIXL_SIDE_CHANNEL_HOST" in os.environ
            assert nixl_backend.kv_transfer_config.engine_id is not self.ENGINE_ID
