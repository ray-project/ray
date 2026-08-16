from typing import Dict, Optional, Union

from ray.actor import ActorHandle
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
from ray.experimental.sandbox.image_manager import (
    BaseImageManager,
    ImageManager,
    get_default_oci_spec,
)
from ray.experimental.sandbox.runtime import SandboxRuntime
from ray.experimental.sandbox.sandbox import Sandbox
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def create(
    image: str,
    cpu: Optional[float] = None,
    memory: Optional[Union[str, int, float]] = None,
    env: Optional[Dict[str, str]] = None,
    workdir: Optional[str] = None,
    ttl_seconds: Optional[int] = 3600,
    timeout_seconds: float = 30.0,
    rootless: bool = True,
    network: str = "none",
    resources: Optional[Dict[str, float]] = None,
    readonly: bool = True,
    **kwargs,
) -> ActorHandle:
    """Create a sandbox environment.

    Args:
        image: Container image for the sandbox environment.
        cpu: Number of CPU cores allocated to the sandbox.
        memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
        env: Environment variables to inject into the sandbox.
        workdir: Default working directory inside the sandbox. Note that the
            working directory is the only writable path in the sandbox. If not provided,
            the container's WORKDIR is used.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode.
        network: Network mode for runsc.
        resources: Custom logical resource requirements.
        readonly: If True, mount container image rootfs in read-only mode (default: True).
        **kwargs: Additional options.

    Returns:
        A Sandbox actor handle.
    """
    actor_opts = {}
    # Ray actors default to num_cpu=1 if not set
    if cpu is not None and cpu >= 0:
        actor_opts["num_cpus"] = cpu
    if memory is not None:
        parsed_mem = parse_memory_bytes(memory)
        if parsed_mem is not None and parsed_mem > 0:
            actor_opts["memory"] = parsed_mem
    if resources:
        actor_opts["resources"] = resources

    return Sandbox.options(**actor_opts).remote(
        image=image,
        cpu=cpu,
        memory=memory,
        env=env,
        workdir=workdir,
        ttl_seconds=ttl_seconds,
        timeout_seconds=timeout_seconds,
        rootless=rootless,
        network=network,
        readonly=readonly,
        **kwargs,
    )


__all__ = [
    "create",
    "Sandbox",
    "SandboxRuntime",
    "BaseImageManager",
    "ImageManager",
    "get_default_oci_spec",
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
