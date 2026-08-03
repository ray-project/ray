import asyncio
from typing import Optional

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    ExecutionResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import (
    GVisorSandboxConfig,
    SandboxConfig,
    parse_memory_bytes,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.runtime import SandboxRuntime
from ray.experimental.sandbox.sandbox import Sandbox, SandboxHandle


def create(
    config: Optional[SandboxConfig] = None,
    **kwargs,
) -> SandboxHandle:
    """Create a sandbox environment.

    Args:
        config: Optional SandboxConfig instance.
        **kwargs: Fields corresponding to SandboxConfig parameters or backend overrides.

    Returns:
        A SandboxHandle instance.
    """
    runsc_path_override = kwargs.pop("runsc_path_override", None)
    if config is None:
        config = SandboxConfig(**kwargs)
    elif kwargs:
        for k, v in kwargs.items():
            if hasattr(config, k):
                setattr(config, k, v)

    if runsc_path_override:
        config.runsc_path = runsc_path_override

    actor_opts = {}
    if config.cpu is not None and config.cpu > 0:
        actor_opts["num_cpus"] = config.cpu
    if config.memory is not None:
        parsed_mem = parse_memory_bytes(config.memory)
        if parsed_mem is not None and parsed_mem > 0:
            actor_opts["memory"] = parsed_mem
    if config.resources:
        actor_opts["resources"] = config.resources

    actor_handle = Sandbox.options(**actor_opts).remote(config=config)
    return SandboxHandle(actor_handle=actor_handle)


async def create_async(
    config: Optional[SandboxConfig] = None,
    **kwargs,
) -> SandboxHandle:
    """Create a sandbox environment asynchronously."""
    return await asyncio.to_thread(create, config=config, **kwargs)


__all__ = [
    "create",
    "create_async",
    "SandboxHandle",
    "Sandbox",
    "SandboxRuntime",
    "BaseSandboxBackend",
    "GVisorSandboxBackend",
    "SandboxConfig",
    "GVisorSandboxConfig",
    "ExecResult",
    "ExecutionResult",
    "SandboxStatus",
    "SandboxError",
    "SandboxCreationError",
    "SandboxTimeoutError",
    "SandboxExecError",
    "SandboxNotFoundError",
]
