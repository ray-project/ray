"""Generic registry for LLM serving components using Ray's internal KV store.

This module provides a reusable registry mechanism that enables components to be
registered in the driver process and accessed across all Ray processes in the cluster,
including Ray Serve child processes.

Similar to RLlib/Tune's registry but with a fixed global prefix for cross-job access.
"""

import importlib
from typing import Any, Callable

import ray._private.worker as worker
import ray.cloudpickle as pickle
from ray.experimental.internal_kv import (
    _internal_kv_del,
    _internal_kv_exists,
    _internal_kv_get,
    _internal_kv_initialized,
    _internal_kv_put,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


# Fixed prefix for cross-job accessibility (Serve deployments run in different jobs)
_SERVE_REGISTRY_PREFIX = "serve_global"


def _make_key(category: str, name: str) -> bytes:
    """Generate a binary key for the KV store.

    Args:
        category: The component category (e.g., "kv_connector_backend")
        name: The component name

    Returns:
        The key to use for storing the value
    """
    return (
        b"LLMServeRegistry:"
        + _SERVE_REGISTRY_PREFIX.encode("ascii")
        + b":"
        + category.encode("ascii")
        + b"/"
        + name.encode("ascii")
    )


def _create_loader(value: Any) -> Callable[[], Any]:
    """Create a loader callable for a value.

    Handles both direct objects/classes and string paths for lazy loading.

    Args:
        value: Either:
            - A class, object, or callable (returns lambda: value)
            - A string in format "module_path:class_name" (creates import loader)

    Returns:
        A callable that returns the value when called

    Raises:
        ValueError: If value is a string but doesn't have the correct format
    """
    if isinstance(value, str):
        if ":" not in value:
            raise ValueError(
                f"Invalid format for string value: '{value}'. "
                f"Expected format: 'module_path:class_name' or a class/object."
            )
        module_path, class_name = value.rsplit(":", 1)
        # Create a loader callable that imports on demand
        def loader():
            module = importlib.import_module(module_path)
            return getattr(module, class_name)

        return loader
    else:
        # For direct objects/classes, create a simple loader
        return lambda: value


class ComponentRegistry:
    """Generic registry for LLM serving components using Ray's internal KV store.

    This registry enables components to be registered in the driver process and
    accessed across all Ray processes in the cluster, including Ray Serve child processes.

    Similar to RLlib/Tune's registry but with a fixed global prefix for cross-job access.

    **Usage Pattern:**
        This registry is designed for a "register once, read many" pattern:
        - Components are typically registered in the driver process before deployment
        - Ray Serve replicas read from the KV store during initialization
        - Once a component is resolved and cached in a process, subsequent `get()` calls return the cached value without checking the KV store for updates

    Example:
        # Create a registry for a component category
        registry = ComponentRegistry("my_component")

        # Register a component
        registry.register("my_component", MyComponentClass)

        # Get a registered component
        component = registry.get("my_component")

        # Check if registered
        if registry.contains("my_component"):
            ...
    """

    def __init__(self, category: str):
        """Initialize a registry for a specific component category.

        Args:
            category: The category name (e.g., "kv_connector_backend")
        """
        self.category = category
        self._loader_cache: dict[str, Callable[[], Any]] = {}
        self._resolved_cache: dict[str, Any] = {}
        self._pending: dict[str, bytes] = {}

    def register(self, name: str, value: Any) -> None:
        """Register a component.

        Args:
            name: The name to register under
            value: The component to register. Can be:
                - A class, object, or callable (serialized directly)
                - A string in format "module_path:class_name" (lazy-loaded via import)

        Raises:
            ValueError: If the component is already registered. Use unregister() first if you need to change the registration.

        Examples:
            # Register a class directly
            registry.register("MyClass", MyClass)

            # Register via module path (lazy loading)
            registry.register("MyClass", "my.module:MyClass")
        """
        # Prevent double registration to avoid cache inconsistencies
        if self.contains(name):
            raise ValueError(
                f"{self.category} '{name}' is already registered. "
                f"Use unregister() first if you need to change the registration."
            )

        # Create a loader callable (handles both direct values and string paths)
        loader = _create_loader(value)

        # Serialize the loader callable
        serialized = pickle.dumps(loader)

        # Store loader in cache
        self._loader_cache[name] = loader

        # Store in KV store if Ray is initialized, otherwise queue for later
        if _internal_kv_initialized():
            try:
                key = _make_key(self.category, name)
                _internal_kv_put(key, serialized, overwrite=True)
                logger.debug(f"Registered {self.category} '{name}' in KV store")
            except Exception as e:
                logger.warning(
                    f"Failed to register {self.category} '{name}' in KV store: {e}",
                    exc_info=True,
                )
                self._pending[name] = serialized
        else:
            self._pending[name] = serialized

    def get(self, name: str) -> Any:
        """Get a registered component.

        Args:
            name: The name of the component

        Returns:
            The registered component. If registered with a string path,
            returns the imported class/object. If registered directly,
            returns the original value.

        Raises:
            ValueError: If the component is not registered
        """
        # Check resolved cache first.
        if name in self._resolved_cache:
            return self._resolved_cache[name]

        loader = self._loader_cache.get(name)
        # If not in local loader cache, try fetching from KV store.
        if loader is None and _internal_kv_initialized():
            try:
                key = _make_key(self.category, name)
                serialized = _internal_kv_get(key)
                if serialized is not None:
                    loader = pickle.loads(serialized)
                    # Cache the loader for future gets.
                    self._loader_cache[name] = loader
                    logger.debug(f"Loaded {self.category} '{name}' from KV store")
            except Exception as e:
                logger.warning(
                    f"Failed to load {self.category} '{name}' from KV store: {e}",
                    exc_info=True,
                )

        if loader is not None:
            value = loader()
            self._resolved_cache[name] = value
            return value

        # Not found
        raise ValueError(
            f"{self.category} '{name}' not found. "
            f"Registered: {list(self._loader_cache.keys())}"
        )

    def contains(self, name: str) -> bool:
        """Check if a component is registered.

        Args:
            name: The name to check

        Returns:
            True if registered, False otherwise
        """
        if name in self._loader_cache:
            return True

        if _internal_kv_initialized():
            try:
                key = _make_key(self.category, name)
                return _internal_kv_exists(key)
            except Exception as e:
                logger.warning(
                    f"Failed to check if {self.category} '{name}' exists in KV store: {e}",
                    exc_info=True,
                )
                return False

        return False

    def unregister(self, name: str) -> None:
        """Unregister a component.

        Removes the component from local cache, pending registrations, and KV store.

        Args:
            name: The name of the component to unregister
        """
        # Remove from local caches
        if name in self._loader_cache:
            del self._loader_cache[name]
        if name in self._resolved_cache:
            del self._resolved_cache[name]

        # Remove from pending if present
        if name in self._pending:
            del self._pending[name]

        # Remove from KV store if Ray is initialized
        if _internal_kv_initialized():
            try:
                key = _make_key(self.category, name)
                _internal_kv_del(key)
                logger.debug(f"Unregistered {self.category} '{name}' from KV store")
            except Exception as e:
                logger.warning(
                    f"Failed to unregister {self.category} '{name}' from KV store: {e}",
                    exc_info=True,
                )

    def flush_pending(self) -> None:
        """Flush pending registrations to KV store.

        This is called automatically when Ray initializes via _post_init_hooks.
        """
        if not _internal_kv_initialized() or not self._pending:
            return

        for name, serialized in self._pending.items():
            try:
                key = _make_key(self.category, name)
                _internal_kv_put(key, serialized, overwrite=True)
                logger.debug(
                    f"Flushed pending registration for {self.category} '{name}'"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to flush {self.category} '{name}': {e}", exc_info=True
                )

        self._pending.clear()


# Global registry instances for different component categories
_registries: dict[str, ComponentRegistry] = {}


def get_registry(category: str) -> ComponentRegistry:
    """Get or create a registry for a component category.

    Args:
        category: The component category name

    Returns:
        The ComponentRegistry instance for this category
    """
    if category not in _registries:
        _registries[category] = ComponentRegistry(category)
    return _registries[category]


def _flush_all_registries():
    """Flush all pending registrations to KV store.

    This is registered as a Ray post-init hook to ensure registrations
    made before Ray initialization are available across processes.
    """
    for registry in _registries.values():
        registry.flush_pending()


if _flush_all_registries not in worker._post_init_hooks:
    worker._post_init_hooks.append(_flush_all_registries)
