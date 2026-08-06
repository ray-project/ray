import asyncio
from typing import Dict, Optional, Union

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    ExecutionResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import parse_memory_bytes
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
    image: Optional[str] = None,
    cpu: float = 0.0,
    memory: Union[str, int, float] = 0,
    env: Optional[Dict[str, str]] = None,
    work_dir: str = "/workspace",
    ttl_seconds: Optional[int] = 3600,
    labels: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 30.0,
    rootless: bool = True,
    network: str = "none",
    resources: Optional[Dict[str, float]] = None,
    readonly: bool = True,
    **kwargs,
) -> SandboxHandle:
    """Create a sandbox environment.

    Args:
        image: Container image for the sandbox environment.
        cpu: Number of CPU cores allocated to the sandbox.
        memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
        env: Environment variables to inject into the sandbox.
        work_dir: Default working directory inside the sandbox.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        labels: Optional key-value metadata labels for tracking.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode.
        network: Network mode for runsc.
        resources: Custom logical resource requirements.
        readonly: If True, mount container image rootfs in read-only mode (default: True).
            Only applicable when an image is specified.
        **kwargs: Additional options.

    Returns:
        A SandboxHandle instance.
    """
    actor_opts = {}
    if cpu is not None and cpu > 0:
        actor_opts["num_cpus"] = cpu
    if memory is not None:
        parsed_mem = parse_memory_bytes(memory)
        if parsed_mem is not None and parsed_mem > 0:
            actor_opts["memory"] = parsed_mem
    if resources:
        actor_opts["resources"] = resources

    actor_handle = Sandbox.options(**actor_opts).remote(
        image=image,
        cpu=cpu,
        memory=memory,
        env=env,
        work_dir=work_dir,
        ttl_seconds=ttl_seconds,
        labels=labels,
        timeout_seconds=timeout_seconds,
        rootless=rootless,
        network=network,
        resources=resources,
        readonly=readonly,
        **kwargs,
    )
    return SandboxHandle(actor_handle=actor_handle)


async def create_async(
    image: Optional[str] = None,
    cpu: float = 0.0,
    memory: Union[str, int, float] = 0,
    env: Optional[Dict[str, str]] = None,
    work_dir: str = "/workspace",
    ttl_seconds: Optional[int] = 3600,
    labels: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 30.0,
    rootless: bool = True,
    network: str = "none",
    resources: Optional[Dict[str, float]] = None,
    readonly: bool = True,
    **kwargs,
) -> SandboxHandle:
    """Create a sandbox environment asynchronously."""
    return await asyncio.to_thread(
        create,
        image=image,
        cpu=cpu,
        memory=memory,
        env=env,
        work_dir=work_dir,
        ttl_seconds=ttl_seconds,
        labels=labels,
        timeout_seconds=timeout_seconds,
        rootless=rootless,
        network=network,
        resources=resources,
        readonly=readonly,
        **kwargs,
    )


__all__ = [
    "create",
    "create_async",
    "SandboxHandle",
    "Sandbox",
    "SandboxRuntime",
    "BaseSandboxBackend",
    "GVisorSandboxBackend",
    "ExecResult",
    "ExecutionResult",
    "SandboxStatus",
    "SandboxError",
    "SandboxCreationError",
    "SandboxTimeoutError",
    "SandboxExecError",
    "SandboxNotFoundError",
]
