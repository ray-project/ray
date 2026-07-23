from typing import Dict, Type

from ray.sandbox.backend.base import BaseSandboxBackend
from ray.sandbox.exceptions import SandboxError


class SandboxBackendFactory:
    """Factory for instantiating and managing registered sandbox backends."""

    _backends: Dict[str, Type[BaseSandboxBackend]] = {}

    @classmethod
    def register_backend(cls, name: str, backend_cls: Type[BaseSandboxBackend]) -> None:
        """Register a backend implementation class under a given name.

        Args:
            name: String name of the backend (e.g. "kubernetes").
            backend_cls: Subclass of BaseSandboxBackend.
        """
        cls._backends[name.lower()] = backend_cls

    @classmethod
    def get_backend(cls, name: str) -> BaseSandboxBackend:
        """Instantiate and return a backend by name.

        Args:
            name: String name of the backend.

        Returns:
            An instance of BaseSandboxBackend.

        Raises:
            SandboxError: If the requested backend is not registered.
        """
        key = name.lower()
        if key not in cls._backends:
            # Lazy import kubernetes backend if requested
            if key == "kubernetes":
                from ray.sandbox.backend.kubernetes import KubernetesSandboxBackend

                cls.register_backend("kubernetes", KubernetesSandboxBackend)
            else:
                raise SandboxError(
                    f"Unsupported sandbox backend: '{name}'. "
                    f"Available backends: {list(cls._backends.keys())}"
                )
        return cls._backends[key]()
