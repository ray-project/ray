import sys

import pytest

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.factory import (
    KVConnectorBackendFactory,
)
from ray.serve.llm import LLMConfig


class TestKVConnectorBackendFactory:
    """Test suite for KVConnectorBackendFactory."""

    @pytest.fixture(autouse=True)
    def reset_factory(self):
        """Reset the factory registry before each test."""
        original_registry = KVConnectorBackendFactory._registry.copy()
        yield
        KVConnectorBackendFactory._registry = original_registry

    def test_get_backend_class_success(self):
        """Test successful retrieval of a registered backend class."""
        backend_class = KVConnectorBackendFactory.get_backend_class(
            "LMCacheConnectorV1"
        )
        assert backend_class is not None
        assert hasattr(backend_class, "setup")

    def test_get_backend_class_not_registered_returns_base(self):
        """Test that getting a non-registered backend returns BaseConnectorBackend."""
        backend_class = KVConnectorBackendFactory.get_backend_class(
            "UnregisteredConnector"
        )
        assert backend_class == BaseConnectorBackend
        assert issubclass(backend_class, BaseConnectorBackend)

    def test_create_backend_success(self):
        """Test successful creation of a backend instance."""
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test-model"),
            engine_kwargs=dict(
                kv_transfer_config=dict(
                    kv_connector="LMCacheConnectorV1",
                    kv_role="kv_both",
                )
            ),
        )
        backend = KVConnectorBackendFactory.create_backend(
            "LMCacheConnectorV1", llm_config
        )
        assert isinstance(backend, BaseConnectorBackend)
        assert backend.llm_config == llm_config

    @pytest.mark.parametrize(
        "connector_name",
        ["LMCacheConnectorV1", "NixlConnector", "MultiConnector"],
    )
    def test_all_registered_backends_can_be_loaded(self, connector_name):
        """Test that all pre-registered backends can be loaded."""
        backend_class = KVConnectorBackendFactory.get_backend_class(connector_name)
        assert backend_class is not None
        assert issubclass(backend_class, BaseConnectorBackend)

    def test_get_backend_class_import_error_handling(self):
        """Test that ImportError during backend loading is handled with clear message."""
        # Register a backend with a non-existent module
        KVConnectorBackendFactory.register_backend(
            "BadBackend",
            "non.existent.module",
            "NonExistentClass",
        )

        with pytest.raises(
            ImportError, match="Failed to load connector backend 'BadBackend'"
        ):
            KVConnectorBackendFactory.get_backend_class("BadBackend")

    def test_unregistered_connector_with_llm_config_setup(self):
        """Test that unregistered connectors work with LLMConfig.setup_engine_backend()."""
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test-model"),
            engine_kwargs=dict(
                kv_transfer_config=dict(
                    kv_connector="SharedStorageConnector",
                    kv_role="kv_both",
                )
            ),
        )
        # Should not raise an error
        llm_config.setup_engine_backend()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
