import asyncio
from typing import Optional

from ray.experimental.sandbox.backend.base import ExecResult, SandboxStatus
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import (
    GVisorSandboxConfig,
    SandboxConfig,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.pool import SandboxPool
from ray.experimental.sandbox.sandbox import Sandbox


def create(config: Optional[SandboxConfig] = None, **kwargs) -> Sandbox:
    """Create a new Sandbox environment using gVisor.

    Args:
        config: Optional SandboxConfig instance.
        **kwargs: Fields corresponding to SandboxConfig parameters.

    Returns:
        A Sandbox instance.
    """
    if config is None:
        config = SandboxConfig(**kwargs)
    elif kwargs:
        for k, v in kwargs.items():
            if hasattr(config, k):
                setattr(config, k, v)

    backend_impl = GVisorSandboxBackend()
    sandbox_id = backend_impl.create_sandbox(config)
    return Sandbox(sandbox_id=sandbox_id, backend=backend_impl, config=config)


async def create_async(config: Optional[SandboxConfig] = None, **kwargs) -> Sandbox:
    """Asynchronously create a new Sandbox environment using gVisor."""
    return await asyncio.to_thread(create, config=config, **kwargs)


__all__ = [
    "create",
    "create_async",
    "Sandbox",
    "SandboxPool",
    "SandboxConfig",
    "GVisorSandboxConfig",
    "ExecResult",
    "SandboxStatus",
    "SandboxError",
    "SandboxCreationError",
    "SandboxTimeoutError",
    "SandboxExecError",
    "SandboxNotFoundError",
]
