"""Factory for lazy-loading KV connector backends.

This module provides a factory pattern for registering and instantiating
KV connector backends without eagerly importing all implementations.
This avoids circular import issues and improves startup performance.
"""

import importlib
from typing import TYPE_CHECKING, Callable, Type

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.observability.logging import get_logger

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


logger = get_logger(__name__)


class KVConnectorBackendFactory:
    """Factory for creating KV connector backend instances with lazy loading."""

    _registry: dict[str, Callable[[], Type["BaseConnectorBackend"]]] = {}

    @classmethod
    def register_backend(cls, name: str, module_path: str, class_name: str) -> None:
        """Register a connector backend with lazy-loading module and class name.

        Args:
            name: The name of the connector (e.g., "LMCacheConnectorV1")
            module_path: The module path to import (e.g., "...lmcache")
            class_name: The class name to load from the module
        """
        if name in cls._registry:
            raise ValueError(f"Connector backend '{name}' is already registered.")

        def loader() -> Type["BaseConnectorBackend"]:
            module = importlib.import_module(module_path)
            return getattr(module, class_name)

        cls._registry[name] = loader

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
        if name not in cls._registry:
            logger.warning(
                f"Unsupported connector backend: {name}. "
                f"Registered backends: {list(cls._registry.keys())}."
                f"Using default backend: {BaseConnectorBackend.__name__}."
            )
            return BaseConnectorBackend
        try:
            return cls._registry[name]()
        except (ImportError, AttributeError) as e:
            raise ImportError(f"Failed to load connector backend '{name}': {e}") from e

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
        backend_class = cls.get_backend_class(name)
        return backend_class(llm_config)

    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check if a connector backend is registered.

        Args:
            name: The name of the connector backend

        Returns:
            True if the backend is registered, False otherwise
        """
        return name in cls._registry

    @classmethod
    def list_registered_backends(cls) -> list[str]:
        """List all registered connector backend names.

        Returns:
            A list of registered backend names
        """
        return list(cls._registry.keys())


# Register connector backends
KVConnectorBackendFactory.register_backend(
    "LMCacheConnectorV1",
    "ray.llm._internal.serve.engines.vllm.kv_transfer.lmcache",
    "LMCacheConnectorV1Backend",
)

KVConnectorBackendFactory.register_backend(
    "NixlConnector",
    "ray.llm._internal.serve.engines.vllm.kv_transfer.nixl",
    "NixlConnectorBackend",
)

KVConnectorBackendFactory.register_backend(
    "MultiConnector",
    "ray.llm._internal.serve.engines.vllm.kv_transfer.multi_connector",
    "MultiConnectorBackend",
)
