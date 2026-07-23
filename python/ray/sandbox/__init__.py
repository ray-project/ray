import asyncio

from ray.sandbox.backend.base import ExecResult, SandboxStatus
from ray.sandbox.backend.factory import SandboxBackendFactory
from ray.sandbox.config import KubernetesSandboxConfig, SandboxConfig
from ray.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)
from ray.sandbox.pool import SandboxPool
from ray.sandbox.sandbox import Sandbox


def create(backend: str = "kubernetes", **kwargs) -> Sandbox:
    """Create a new Sandbox environment.

    Args:
        backend: Name of the sandbox backend (default: "kubernetes").
        **kwargs: Fields corresponding to SandboxConfig or backend-specific configs
                  (e.g., image, cpu, memory, env, namespace, ttl_seconds).

    Returns:
        A Sandbox instance.

    Example:
        >>> import ray.sandbox
        >>> sb = ray.sandbox.create(image="python:3.10-slim", memory="1Gi")
        >>> res = sb.exec("python3 -c 'print(\"Hello world\")'")
        >>> print(res.stdout)
        >>> sb.delete()
    """
    if backend.lower() == "kubernetes":
        config = KubernetesSandboxConfig(backend=backend, **kwargs)
    else:
        config = SandboxConfig(backend=backend, **kwargs)

    backend_impl = SandboxBackendFactory.get_backend(backend)
    sandbox_id = backend_impl.create_sandbox(config)
    return Sandbox(sandbox_id=sandbox_id, backend=backend_impl, config=config)


async def create_async(backend: str = "kubernetes", **kwargs) -> Sandbox:
    """Asynchronously create a new Sandbox environment."""
    return await asyncio.to_thread(create, backend=backend, **kwargs)


__all__ = [
    "create",
    "create_async",
    "Sandbox",
    "SandboxPool",
    "SandboxConfig",
    "KubernetesSandboxConfig",
    "ExecResult",
    "SandboxStatus",
    "SandboxError",
    "SandboxCreationError",
    "SandboxTimeoutError",
    "SandboxExecError",
    "SandboxNotFoundError",
]
