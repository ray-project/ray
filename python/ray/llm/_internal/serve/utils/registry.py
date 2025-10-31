"""Generic registry for LLM serving components using Ray's internal KV store.

This module provides a reusable registry mechanism that enables components to be
registered in the driver process and accessed across all Ray processes in the cluster,
including Ray Serve child processes.

Similar to RLlib/Tune's registry but with a fixed global prefix for cross-job access.
"""

import importlib
from typing import Any, Callable

from ray.llm._internal.serve.observability.logging import get_logger

import ray.cloudpickle as pickle
from ray.experimental.internal_kv import (
    _internal_kv_get,
    _internal_kv_initialized,
    _internal_kv_put,
)
import ray._private.worker as worker

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
        self._local_cache: dict[str, Any] = {}
        self._pending: dict[str, bytes] = {}
    
    def register(self, name: str, value: Any) -> None:
        """Register a component.
        
        Args:
            name: The name to register under
            value: The component to register. Can be:
                - A class, object, or callable (serialized directly)
                - A string in format "module_path:class_name" (lazy-loaded via import)
        
        Examples:
            # Register a class directly
            registry.register("MyClass", MyClass)
            
            # Register via module path (lazy loading)
            registry.register("MyClass", "my.module:MyClass")
        """
        # Create a loader callable (handles both direct values and string paths)
        loader = _create_loader(value)
        
        # Serialize the loader callable
        try:
            serialized = pickle.dumps_debug(loader)
        except AttributeError:
            serialized = pickle.dumps(loader)
        
        # Store in local cache for immediate use
        # Store the loader so we can call it later if needed
        self._local_cache[name] = loader
        
        # Store in KV store if Ray is initialized, otherwise queue for later
        if _internal_kv_initialized():
            try:
                key = _make_key(self.category, name)
                _internal_kv_put(key, serialized, overwrite=True)
                logger.debug(f"Registered {self.category} '{name}' in KV store")
            except Exception as e:
                logger.warning(f"Failed to register {self.category} '{name}' in KV store: {e}")
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
        # Check local cache first
        if name in self._local_cache:
            cached = self._local_cache[name]
            # If it's a loader callable, call it to get the actual value
            if callable(cached) and not isinstance(cached, type):
                return cached()
            return cached
        
        # Try to fetch from KV store
        if _internal_kv_initialized():
            try:
                key = _make_key(self.category, name)
                serialized = _internal_kv_get(key)
                if serialized is not None:
                    loader = pickle.loads(serialized)
                    # Call the loader to get the actual value
                    value = loader()
                    self._local_cache[name] = value
                    logger.debug(f"Loaded {self.category} '{name}' from KV store")
                    return value
            except Exception as e:
                logger.warning(f"Failed to load {self.category} '{name}' from KV store: {e}")
        
        # Not found
        raise ValueError(
            f"{self.category} '{name}' not found. "
            f"Registered: {list(self._local_cache.keys())}"
        )
    
    def contains(self, name: str) -> bool:
        """Check if a component is registered.
        
        Args:
            name: The name to check
            
        Returns:
            True if registered, False otherwise
        """
        if name in self._local_cache:
            return True
        
        if _internal_kv_initialized():
            try:
                key = _make_key(self.category, name)
                return _internal_kv_get(key) is not None
            except Exception:
                return False
        
        return False
    
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
                logger.debug(f"Flushed pending registration for {self.category} '{name}'")
            except Exception as e:
                logger.warning(f"Failed to flush {self.category} '{name}': {e}")
        
        self._pending.clear()
    
    def list_registered(self) -> list[str]:
        """List all registered component names.
        
        Returns:
            List of registered names (from local cache)
        """
        return list(self._local_cache.keys())


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

