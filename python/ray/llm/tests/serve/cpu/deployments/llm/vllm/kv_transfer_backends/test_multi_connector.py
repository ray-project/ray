import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.factory import (
    KVConnectorBackendFactory,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.multi_connector import (
    MultiConnectorBackend,
)
from ray.serve.llm import LLMConfig


class TestMultiConnectorBackend:
    """Test suite for MultiConnectorBackend."""

    @pytest.fixture
    def basic_llm_config(self):
        """Fixture for basic LLM config with MultiConnector."""
        return LLMConfig(
            model_loading_config=dict(model_id="test-model"),
            engine_kwargs=dict(
                kv_transfer_config=dict(
                    kv_connector="MultiConnector",
                    kv_connector_extra_config=dict(
                        connectors=[
                            {"kv_connector": "LMCacheConnectorV1"},
                            {"kv_connector": "NixlConnector"},
                        ]
                    ),
                )
            ),
        )

    @pytest.fixture
    def multi_backend(self, basic_llm_config):
        """Fixture for MultiConnectorBackend."""
        return MultiConnectorBackend(basic_llm_config)

    def test_multi_connector_initialization(self, multi_backend):
        """Test that MultiConnectorBackend can be initialized."""
        assert isinstance(multi_backend, MultiConnectorBackend)
        assert isinstance(multi_backend, BaseConnectorBackend)

    def test_setup_calls_all_connectors(self, multi_backend):
        """Test that setup calls setup on all configured connectors."""
        mock_backend1 = MagicMock(spec=BaseConnectorBackend)
        mock_backend2 = MagicMock(spec=BaseConnectorBackend)

        with patch.object(
            KVConnectorBackendFactory,
            "create_backend",
            side_effect=[mock_backend1, mock_backend2],
        ) as mock_create:
            multi_backend.setup()

            assert mock_create.call_count == 2
            mock_backend1.setup.assert_called_once()
            mock_backend2.setup.assert_called_once()

    def test_setup_raises_error_when_connector_missing_kv_connector(self):
        """Test that setup raises ValueError when a connector is missing kv_connector."""
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test-model"),
            engine_kwargs=dict(
                kv_transfer_config=dict(
                    kv_connector="MultiConnector",
                    kv_connector_extra_config=dict(
                        connectors=[
                            {"some_other_key": "value"},
                        ]
                    ),
                )
            ),
        )
        backend = MultiConnectorBackend(llm_config)

        with pytest.raises(ValueError, match="kv_connector is not set"):
            backend.setup()

    def test_setup_with_nested_multi_connector_raises_error(self):
        """Test that nesting MultiConnector raises a ValueError."""
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test-model"),
            engine_kwargs=dict(
                kv_transfer_config=dict(
                    kv_connector="MultiConnector",
                    kv_connector_extra_config=dict(
                        connectors=[
                            {"kv_connector": "MultiConnector"},
                        ]
                    ),
                )
            ),
        )
        backend = MultiConnectorBackend(llm_config)
        with pytest.raises(ValueError, match="Nesting MultiConnector"):
            backend.setup()

    def test_setup_passes_isolated_config_to_sub_connectors(self):
        """Test that sub-connectors inherit parent config and receive their specific settings."""
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test-model"),
            engine_kwargs=dict(
                kv_transfer_config=dict(
                    kv_connector="MultiConnector",
                    engine_id="test-engine-123",
                    kv_role="kv_both",
                    kv_connector_extra_config=dict(
                        connectors=[
                            {
                                "kv_connector": "LMCacheConnectorV1",
                                "custom_param": "value1",
                            },
                            {"kv_connector": "NixlConnector", "custom_param": "value2"},
                        ]
                    ),
                )
            ),
        )

        captured_configs = []

        def capture_config(name, config):
            captured_configs.append((name, config.engine_kwargs["kv_transfer_config"]))
            return MagicMock(spec=BaseConnectorBackend)

        with patch.object(
            KVConnectorBackendFactory, "create_backend", side_effect=capture_config
        ):
            MultiConnectorBackend(llm_config).setup()

        assert len(captured_configs) == 2

        # Verify each connector gets: inherited parent fields + its own specific config
        expected_configs = [
            (
                "LMCacheConnectorV1",
                {"kv_connector": "LMCacheConnectorV1", "custom_param": "value1"},
            ),
            (
                "NixlConnector",
                {"kv_connector": "NixlConnector", "custom_param": "value2"},
            ),
        ]

        for (actual_name, actual_config), (expected_name, expected_specific) in zip(
            captured_configs, expected_configs
        ):
            assert actual_name == expected_name
            # Check inherited parent fields
            assert actual_config["engine_id"] == "test-engine-123"
            assert actual_config["kv_role"] == "kv_both"
            # Check connector-specific fields
            for key, value in expected_specific.items():
                assert actual_config[key] == value
            # Verify kv_connector_extra_config is not passed to sub-connectors
            assert "kv_connector_extra_config" not in actual_config


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
