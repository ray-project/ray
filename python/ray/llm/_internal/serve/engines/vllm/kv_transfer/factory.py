"""Factory for lazy-loading KV connector backends.

This module provides a factory pattern for registering and instantiating
KV connector backends without eagerly importing all implementations.
This avoids circular import issues and improves startup performance.
"""

from typing import TYPE_CHECKING, Type, Union

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.utils.registry import get_registry

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


logger = get_logger(__name__)

# Get the registry instance for KV connector backends
_kv_backend_registry = get_registry("kv_connector_backend")


class KVConnectorBackendFactory:
    """Factory for creating KV connector backend instances with lazy loading."""

    @classmethod
    def register_backend(
        cls,
        name: str,
        backend_class_or_path: Union[Type["BaseConnectorBackend"], str],
    ) -> None:
        """Register a connector backend.

        This enables the backend to be accessed on every Ray process in the cluster.

        Args:
            name: The name of the connector (e.g., "LMCacheConnectorV1")
            backend_class_or_path: Either:
                - The backend class object directly (preferred), or
                - A string in the format "module_path:class_name" for lazy loading

        Examples:
            # Register with class directly (recommended):
            KVConnectorBackendFactory.register_backend("MyConnector", MyConnectorClass)

            # Register with module path string (for lazy loading):
            KVConnectorBackendFactory.register_backend("MyConnector", "my.module:MyClass")
        """
        _kv_backend_registry.register(name, backend_class_or_path)

    @classmethod
    def get_backend_class(cls, name: str) -> Type["BaseConnectorBackend"]:
        """Get the connector backend class by name.

        For registered connectors, returns the registered backend class.
        For unregistered connectors, returns BaseConnectorBackend which has
        a no-op setup() method, allowing connectors that don't require
        Ray Serve orchestration to work without registration.

        Args:
            name: The name of the connector backend

        Returns:
            The connector backend class

        Raises:
            ImportError: If a registered backend fails to load
        """
        try:
            return _kv_backend_registry.get(name)
        except ValueError:
            logger.warning(
                f"Unsupported connector backend: {name}. "
                f"Using default: {BaseConnectorBackend.__name__}."
            )
            return BaseConnectorBackend
        except Exception as e:
            raise ImportError(
                f"Failed to load connector backend '{name}': {type(e).__name__}: {e}"
            ) from e

    @classmethod
    def create_backend(
        cls, name: str, llm_config: "LLMConfig"
    ) -> "BaseConnectorBackend":
        """Create a connector backend instance.

        Args:
            name: The name of the connector backend
            llm_config: The LLM configuration

        Returns:
            An instance of the connector backend
        """
        return cls.get_backend_class(name)(llm_config)

    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check if a connector backend is registered."""
        return _kv_backend_registry.contains(name)

    @classmethod
    def unregister_backend(cls, name: str) -> None:
        """Unregister a connector backend.

        Removes the backend from the registry across all Ray processes.

        Args:
            name: The name of the connector backend to unregister
        """
        _kv_backend_registry.unregister(name)


BUILTIN_BACKENDS = {
    "LMCacheConnectorV1": "ray.llm._internal.serve.engines.vllm.kv_transfer.lmcache:LMCacheConnectorV1Backend",
    "NixlConnector": "ray.llm._internal.serve.engines.vllm.kv_transfer.nixl:NixlConnectorBackend",
    "MultiConnector": "ray.llm._internal.serve.engines.vllm.kv_transfer.multi_connector:MultiConnectorBackend",
}


def _initialize_registry() -> None:
    """Initialize the registry with built-in backends.

    This function is called when the module is imported to ensure
    built-in backends are registered.
    """
    for name, backend_path in BUILTIN_BACKENDS.items():
        if not KVConnectorBackendFactory.is_registered(name):
            KVConnectorBackendFactory.register_backend(name, backend_path)


# Initialize registry when module is imported
_initialize_registry()
