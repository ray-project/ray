from typing import Dict, List, Optional, Union

from ray.actor import ActorHandle
from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    ExecutionResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import (
    DOCKER_DEFAULT_CAPABILITIES,
    parse_memory_bytes,
)
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
    mount_workdir: bool = True,
    ttl_seconds: Optional[int] = None,
    timeout_seconds: float = 30.0,
    rootless: bool = True,
    network: str = "none",
    capabilities: Optional[List[str]] = None,
    resources: Optional[Dict[str, float]] = None,
    readonly: bool = True,
    **kwargs,
) -> ActorHandle:
    """Create a remote sandbox environment managed by a Ray actor.

    Spawns a :class:`~ray.experimental.sandbox.Sandbox` actor on the Ray cluster to manage
    the sandbox lifecycle and returns an :class:`~ray.actor.ActorHandle`. For low-level local
    sandbox management on the current node (e.g., inside custom worker actors), use
    :class:`~ray.experimental.sandbox.runtime.SandboxRuntime` instead.

    Args:
        image: Container image for the sandbox environment.
        cpu: Number of CPU cores allocated to the sandbox.
        memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
        env: Environment variables to inject into the sandbox.
        workdir: Default working directory inside the sandbox. By default, the
            working directory is the only writable path in the sandbox (unless
            ``readonly=False`` is set). If not provided, the container's WORKDIR is used.
        mount_workdir: If True (default), bind-mount a host-backed scratch
            directory at ``workdir``, shadowing any image content there. Set
            False to leave the image's filesystem (e.g. its own WORKDIR
            content) untouched; combine with ``readonly=False`` to write.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds,
            measured wall-clock from creation (not idle time): a sandbox that
            is mid-command when the TTL fires is still deleted. None (default)
            disables it; values <= 0 also mean no TTL.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode.
        network: Network mode for runsc ("none", "host", "sandbox"). With
            "host", the host's /etc/resolv.conf is bind-mounted read-only so
            DNS resolution works out of the box.
        capabilities: Optional additional Linux capabilities granted to the
            container process, unioned on top of the runtime defaults. Use
            :data:`~ray.experimental.sandbox.config.DOCKER_DEFAULT_CAPABILITIES`
            to match how Docker runs images.
        resources: Custom logical resource requirements.
        readonly: If True (default), mount container image rootfs in read-only mode
            such that only ``workdir`` is writable. If False, the entire root filesystem
            is writable. Writes are isolated within a per-sandbox copy-on-write overlay
            filesystem, ensuring multiple sandboxes running the same container image do
            not interfere with each other or modify the base image.
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
        mount_workdir=mount_workdir,
        ttl_seconds=ttl_seconds,
        timeout_seconds=timeout_seconds,
        rootless=rootless,
        network=network,
        capabilities=capabilities,
        readonly=readonly,
        **kwargs,
    )


__all__ = [
    "create",
    "DOCKER_DEFAULT_CAPABILITIES",
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
