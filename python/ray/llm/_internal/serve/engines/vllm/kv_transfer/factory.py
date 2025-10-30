"""Factory for lazy-loading KV connector backends.

This module provides a factory pattern for registering and instantiating
KV connector backends without eagerly importing all implementations.
This avoids circular import issues and improves startup performance.
"""

import importlib
import inspect
from typing import TYPE_CHECKING, Callable, Type

from ray.experimental.internal_kv import (
    _internal_kv_del,
    _internal_kv_get,
    _internal_kv_initialized,
    _internal_kv_put,
)

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.observability.logging import get_logger

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


logger = get_logger(__name__)

# Category for KV store registrations
KV_CONNECTOR_BACKEND_CATEGORY = "kv_connector_backend"

# Use a global registry instance like Tune does
# This ensures consistent prefix usage across all processes
class _KVBackendRegistry:
    def __init__(self):
        self._prefix: str | None = None
    
    @property
    def prefix(self):
        if self._prefix is None:
            # Always use a fixed global prefix for Serve deployments
            # This ensures registrations are accessible across job boundaries
            # Serve deployments run in different job contexts than the driver
            self._prefix = "serve_global"
        return self._prefix
    
    def make_key(self, name: str) -> bytes:
        """Generate a binary key for the KV store.
        
        Args:
            name: The backend name
            
        Returns:
            The key to use for storing the value
        """
        return (
            b"KVConnectorBackendRegistry:"
            + self.prefix.encode("ascii")
            + b":"
            + KV_CONNECTOR_BACKEND_CATEGORY.encode("ascii")
            + b"/"
            + name.encode("ascii")
        )


# Global registry instance
_kv_backend_registry = _KVBackendRegistry()


def _make_key(name: str) -> bytes:
    """Generate a binary key for the KV store.
    
    Uses the global registry instance to ensure consistent prefix usage.
    
    Args:
        name: The backend name
        
    Returns:
        The key to use for storing the value
    """
    return _kv_backend_registry.make_key(name)


def _get_registration_metadata() -> dict[str, tuple[str, str]]:
    """Get registration metadata from local cache.
    
    Returns:
        Dictionary mapping backend names to (module_path, class_name) tuples
    """
    return _get_registration_metadata._cache.copy()


# Initialize cache as function attribute (similar to how RLlib does it)
_get_registration_metadata._cache: dict[str, tuple[str, str]] = {}

# Track pending registrations that need to be flushed to KV store
# Store the loader callable itself, not just metadata
_pending_registrations: dict[str, Callable[[], Type["BaseConnectorBackend"]]] = {}


def _flush_registrations_to_kv_store():
    """Flush pending registrations to Ray's internal KV store.
    
    This is called when Ray is initialized to ensure all registrations
    made before Ray initialization are available across processes.
    """
    if not _internal_kv_initialized():
        return
    
    import ray.cloudpickle as pickle
    
    # Flush all pending registrations (these are loader callables)
    flushed_count = 0
    for name, loader in _pending_registrations.items():
        try:
            key = _kv_backend_registry.make_key(name)
            try:
                serialized_loader = pickle.dumps_debug(loader)
            except AttributeError:
                serialized_loader = pickle.dumps(loader)
            _internal_kv_put(key, serialized_loader, overwrite=True)
            logger.info(f"Flushed registration for backend '{name}' to KV store with key: {key}")
            flushed_count += 1
        except Exception as e:
            logger.warning(f"Failed to flush registration for backend '{name}' to KV store: {e}")
    
    # Also flush any registrations in local registry that weren't in pending
    # This handles cases where registration happened but wasn't tracked as pending
    for name, loader in KVConnectorBackendFactory._registry.items():
        if name not in _pending_registrations:
            try:
                key = _kv_backend_registry.make_key(name)
                # Check if already in KV store
                if _internal_kv_get(key) is None:
                    try:
                        serialized_loader = pickle.dumps_debug(loader)
                    except AttributeError:
                        serialized_loader = pickle.dumps(loader)
                    _internal_kv_put(key, serialized_loader, overwrite=True)
                    logger.info(f"Flushed registry registration for backend '{name}' to KV store")
                    flushed_count += 1
            except Exception as e:
                logger.debug(f"Failed to flush registry registration for backend '{name}': {e}")
    
    if flushed_count > 0:
        logger.info(f"Flushed {flushed_count} backend registrations to KV store")
    
    _pending_registrations.clear()


# Register flush hook to be called when Ray is initialized (similar to Tune)
try:
    import ray._private.worker as worker
    if _flush_registrations_to_kv_store not in worker._post_init_hooks:
        worker._post_init_hooks.append(_flush_registrations_to_kv_store)
except Exception:
    # Ray might not be imported yet, that's okay
    pass

# Also ensure flush happens when Ray Serve is used
# This is critical because Serve deployments run in separate processes
def _ensure_registrations_flushed():
    """Ensure all pending registrations are flushed to KV store.
    
    This should be called before deployments are created to ensure
    registrations are available in child processes.
    """
    if _internal_kv_initialized() and _pending_registrations:
        _flush_registrations_to_kv_store()
    elif _pending_registrations:
        # Try to flush if Ray becomes available
        try:
            import ray
            if ray.is_initialized() and _internal_kv_initialized():
                _flush_registrations_to_kv_store()
        except Exception:
            pass


class KVConnectorBackendFactory:
    """Factory for creating KV connector backend instances with lazy loading."""

    _registry: dict[str, Callable[[], Type["BaseConnectorBackend"]]] = {}

    @classmethod
    def register_backend(
        cls, 
        name: str, 
        backend_class_or_path: Type["BaseConnectorBackend"] | str,
    ) -> None:
        """Register a connector backend.

        This enables the backend to be accessed on every Ray process in the cluster,
        similar to how RLlib/Tune registers environments.

        Args:
            name: The name of the connector (e.g., "LMCacheConnectorV1")
            backend_class_or_path: Either:
                - The backend class object directly (preferred), or
                - A string in the format "module_path:class_name" for lazy loading
                       
        Examples:
            # Register with class directly (recommended):
            KVConnectorBackendFactory.register_backend("MyConnector", MyConnectorClass)
            
            # Register with module path string (for lazy loading):
            KVConnectorBackendFactory.register_backend(
                "MyConnector", 
                "my.module.path:MyConnectorClass"
            )
        """
        import ray.cloudpickle as pickle
        
        # Determine how to load the backend
        if isinstance(backend_class_or_path, type):
            # Direct class registration - capture the class directly
            # This gets serialized with cloudpickle, similar to how RLlib/Tune works
            backend_class = backend_class_or_path
            def loader() -> Type["BaseConnectorBackend"]:
                return backend_class
            # Store metadata for compatibility
            module_path = backend_class.__module__
            class_name = backend_class.__name__
        elif isinstance(backend_class_or_path, str):
            # Parse module_path:class_name format
            if ":" in backend_class_or_path:
                module_path, class_name = backend_class_or_path.rsplit(":", 1)
            else:
                # Backward compatibility: if no colon, assume it's just module_path
                # and we need class_name - but this shouldn't happen in new code
                raise ValueError(
                    f"Invalid format for backend_class_or_path: '{backend_class_or_path}'. "
                    f"Expected format: 'module_path:class_name' or a class object."
                )
            # Lazy loading via module path
            def loader() -> Type["BaseConnectorBackend"]:
                module = importlib.import_module(module_path)
                return getattr(module, class_name)
        else:
            raise TypeError(
                f"backend_class_or_path must be a class or a string in format "
                f"'module_path:class_name', got {type(backend_class_or_path)}"
            )
        
        # Store metadata for local cache (for compatibility)
        metadata = (module_path, class_name)
        _get_registration_metadata._cache[name] = metadata
        
        # Store the loader callable in Ray's internal KV store
        # This is the same mechanism used by RLlib/Tune - serialize the actual callable
        if _internal_kv_initialized():
            try:
                key = _kv_backend_registry.make_key(name)
                try:
                    serialized_loader = pickle.dumps_debug(loader)
                except AttributeError:
                    serialized_loader = pickle.dumps(loader)
                _internal_kv_put(key, serialized_loader, overwrite=True)
                logger.info(f"Registered backend '{name}' in Ray KV store with key: {key}")
            except Exception as e:
                logger.warning(
                    f"Failed to register backend '{name}' in Ray KV store: {e}. "
                    f"Registration will be flushed when Ray is initialized."
                )
                # Store as pending to flush later
                _pending_registrations[name] = loader
        else:
            # Ray not initialized yet, store as pending to flush later
            _pending_registrations[name] = loader
            logger.info(f"Stored backend '{name}' registration as pending (Ray not initialized)")
        
        # Register in local registry for immediate use
        if name not in cls._registry:
            cls._registry[name] = loader
        else:
            # Check if it's the same registration by comparing metadata
            existing_metadata = _get_registration_metadata._cache.get(name)
            if existing_metadata != metadata:
                raise ValueError(
                    f"Connector backend '{name}' is already registered with different "
                    f"metadata. Existing: {existing_metadata}, New: {metadata}"
                )

    @classmethod
    def _ensure_registry_populated(cls) -> None:
        """Ensure the registry is populated, rebuilding from metadata if needed.
        
        This is needed because Ray Serve runs deployments in separate processes,
        and class variables are not shared across processes. When the module is
        imported in a child process, the registry may be empty even though
        registration metadata exists in Ray's internal KV store.
        """
        import ray.cloudpickle as pickle
        
        # First, try to load from Ray's internal KV store (cross-process)
        if _internal_kv_initialized():
            # We need to check for registered backends. Since we don't maintain
            # a list of names, we'll rely on the local cache and try known names.
            # For now, we'll load from local cache and let individual lookups
            # fetch from KV store if needed.
            pass
        
        # Rebuild registry from local cache
        metadata = _get_registration_metadata()
        for name, (module_path, class_name) in metadata.items():
            if name not in cls._registry:
                def loader(module_path=module_path, class_name=class_name) -> Type["BaseConnectorBackend"]:
                    module = importlib.import_module(module_path)
                    return getattr(module, class_name)
                cls._registry[name] = loader
        
        # Also try to fetch from KV store for any backends we're looking for
        # but don't have in local cache (this happens in child processes)
        if _internal_kv_initialized():
            # Check common backend names or maintain a registry list
            # For now, we'll fetch on-demand when get_backend_class is called
            pass

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
        import ray.cloudpickle as pickle
        
        # Try to flush pending registrations if Ray is now available
        if _pending_registrations and _internal_kv_initialized():
            _flush_registrations_to_kv_store()
        
        # Ensure registry is populated (important for Ray Serve child processes)
        cls._ensure_registry_populated()
        
        # If not in local registry, try to fetch from Ray's internal KV store
        if name not in cls._registry and _internal_kv_initialized():
            try:
                key = _kv_backend_registry.make_key(name)
                logger.info(f"Looking up backend '{name}' in KV store with key: {key}")
                serialized_loader = _internal_kv_get(key)
                if serialized_loader is not None:
                    logger.info(f"Found backend '{name}' in KV store, deserializing loader...")
                    # Deserialize the loader callable (like RLlib/Tune does)
                    loader = pickle.loads(serialized_loader)
                    # Add to local registry
                    cls._registry[name] = loader
                    logger.info(f"Successfully registered backend '{name}' from KV store")
                else:
                    logger.warning(f"Backend '{name}' not found in KV store (key: {key})")
            except Exception as e:
                logger.warning(f"Failed to load backend '{name}' from KV store: {e}", exc_info=True)
        
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


def _initialize_registry() -> None:
    """Initialize the registry with built-in backends.
    
    This function is called when the module is imported to ensure
    built-in backends are registered. It also rebuilds the registry
    from metadata if needed (important for Ray Serve child processes).
    """
    # Register built-in connector backends using module_path:class_name format
    builtin_backends = [
        ("LMCacheConnectorV1", "ray.llm._internal.serve.engines.vllm.kv_transfer.lmcache:LMCacheConnectorV1Backend"),
        ("NixlConnector", "ray.llm._internal.serve.engines.vllm.kv_transfer.nixl:NixlConnectorBackend"),
        ("MultiConnector", "ray.llm._internal.serve.engines.vllm.kv_transfer.multi_connector:MultiConnectorBackend"),
    ]
    
    for name, backend_path in builtin_backends:
        # Only register if not already registered (avoid duplicate registration errors)
        if name not in _get_registration_metadata._cache:
            KVConnectorBackendFactory.register_backend(name, backend_path)
        # If metadata exists but registry is empty, rebuild registry
        elif name not in KVConnectorBackendFactory._registry:
            KVConnectorBackendFactory._ensure_registry_populated()


# Initialize registry when module is imported
_initialize_registry()
