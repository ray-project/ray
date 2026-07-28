import asyncio
from typing import Optional

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
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
from ray.experimental.sandbox.sandbox import Sandbox


def create(
    config: Optional[SandboxConfig] = None,
    backend: Optional[BaseSandboxBackend] = None,
    **kwargs,
) -> Sandbox:
    """Create a new Sandbox environment using gVisor.

    Args:
        config: Optional SandboxConfig instance.
        backend: Optional custom BaseSandboxBackend instance. If None, defaults to GVisorSandboxBackend.
        **kwargs: Fields corresponding to SandboxConfig parameters or backend overrides.

    Returns:
        A Sandbox instance.
    """
    runsc_path_override = kwargs.pop("runsc_path_override", None)
    if config is None:
        config = SandboxConfig(**kwargs)
    elif kwargs:
        for k, v in kwargs.items():
            if hasattr(config, k):
                setattr(config, k, v)

    if backend is None:
        backend = GVisorSandboxBackend(runsc_path_override=runsc_path_override)

    sandbox_id = backend.create_sandbox(config)
    return Sandbox(sandbox_id=sandbox_id, backend=backend, config=config)


async def create_async(
    config: Optional[SandboxConfig] = None,
    backend: Optional[BaseSandboxBackend] = None,
    **kwargs,
) -> Sandbox:
    """Asynchronously create a new Sandbox environment using gVisor."""
    return await asyncio.to_thread(create, config=config, backend=backend, **kwargs)


__all__ = [
    "create",
    "create_async",
    "Sandbox",
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
