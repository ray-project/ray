import sys
from contextlib import contextmanager
from typing import Any

import pytest

from ray import serve
from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.factory import (
    KVConnectorBackendFactory,
)
from ray.serve.llm import LLMConfig


@contextmanager
def registered_backend(name: str, backend_class_or_path: Any):
    KVConnectorBackendFactory.register_backend(name, backend_class_or_path)
    try:
        yield
    finally:
        if KVConnectorBackendFactory.is_registered(name):
            KVConnectorBackendFactory.unregister_backend(name)


@pytest.fixture
def test_deployment_handle():
    """Fixture that creates a Serve deployment for testing cross-process registry access."""

    # This ensures proper serialization when sent to child processes
    class TestCrossProcessConnector(BaseConnectorBackend):
        def setup(self):
            pass

    # Register the backend in the driver process and ensure cleanup
    with registered_backend("TestCrossProcessConnector", TestCrossProcessConnector):
        # Create a Serve deployment that will run in a different process than the
        # driver process
        @serve.deployment
        class TestDeployment:
            def __init__(self):
                # This runs in a child process - should be able to access the registered backend
                self.connector_class = KVConnectorBackendFactory.get_backend_class(
                    "TestCrossProcessConnector"
                )

            def __call__(self):
                """Return the connector class to verify it's correct."""
                return self.connector_class

        # Deploy and yield the handle and connector class
        app = TestDeployment.bind()
        handle = serve.run(app)
        try:
            yield handle, TestCrossProcessConnector
        finally:
            try:
                serve.shutdown()
            except RuntimeError:
                # Handle case where event loop is already closed
                pass


class TestKVConnectorBackendFactory:
    """Test suite for KVConnectorBackendFactory."""

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
        # Register a backend with a non-existent module path
        with registered_backend("BadBackend", "non.existent.module:NonExistentClass"):
            with pytest.raises(
                ImportError, match="Failed to load connector backend 'BadBackend'"
            ):
                KVConnectorBackendFactory.get_backend_class("BadBackend")

    def test_register_backend_with_class_directly(self):
        """Test registering a backend class directly."""

        class CustomBackend(BaseConnectorBackend):
            def setup(self):
                pass

        with registered_backend("CustomBackend", CustomBackend):
            assert KVConnectorBackendFactory.is_registered("CustomBackend")
            retrieved = KVConnectorBackendFactory.get_backend_class("CustomBackend")
            assert retrieved == CustomBackend

    def test_register_backend_with_module_path(self):
        """Test registering a backend via module path string."""
        # Register using module:class format
        with registered_backend(
            "LMCacheViaPath",
            "ray.llm._internal.serve.engines.vllm.kv_transfer.lmcache:LMCacheConnectorV1Backend",
        ):
            assert KVConnectorBackendFactory.is_registered("LMCacheViaPath")
            backend_class = KVConnectorBackendFactory.get_backend_class(
                "LMCacheViaPath"
            )
            assert backend_class is not None
            assert issubclass(backend_class, BaseConnectorBackend)

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

    @pytest.mark.asyncio
    async def test_cross_process_registry_access(self, test_deployment_handle):
        """Test that registrations made in driver are accessible in Ray Serve child processes."""
        handle, TestCrossProcessConnector = test_deployment_handle

        # Verify it's registered in driver
        assert KVConnectorBackendFactory.is_registered("TestCrossProcessConnector")

        result = await handle.remote()

        # Verify it's the correct class
        assert result == TestCrossProcessConnector
        assert issubclass(result, BaseConnectorBackend)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
