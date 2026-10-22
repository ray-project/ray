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
    ALLOW_ALL_CIDRS,
    DEFAULT_PUBLIC_ALLOWLIST,
    DEFAULT_PUBLIC_DNS,
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
    ttl_seconds: Optional[int] = None,
    timeout_seconds: float = 30.0,
    rootless: bool = True,
    network: str = "none",
    dns: Optional[List[str]] = None,
    cidr_allowlist: Optional[List[str]] = None,
    capabilities: Optional[List[str]] = None,
    resources: Optional[Dict[str, float]] = None,
    readonly: bool = True,
    disk: Union[str, int, float] = 0,
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
        workdir: Working directory for commands; None uses the image's
            WORKDIR. On a readonly rootfs an explicit workdir is also the
            sandbox's only writable path; see
            :class:`~ray.experimental.sandbox.config.SandboxConfig`.
        ttl_seconds: Optional time-to-live in seconds, wall-clock from
            creation (not idle time). None (default) or <= 0 disables it.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode.
        network: Network mode ("none", "public", "host", "sandbox"); see
            :class:`~ray.experimental.sandbox.config.SandboxConfig`. "public"
            is the recommended internet-access mode.
        dns: Optional nameserver IPs for the generated /etc/resolv.conf
            (public resolvers by default for "public").
        cidr_allowlist: Optional egress allowlist for network="public": only
            the listed destinations are reachable (plus DNS to the ``dns``
            resolvers). None means the public internet, with internal
            addresses left out as a best effort, and a denylist keeps
            instance metadata and the Kubernetes API service closed whatever
            the list says; see
            :class:`~ray.experimental.sandbox.config.SandboxConfig`.
        capabilities: Linux capabilities, written exactly (None keeps the
            runtime default; ``[]`` means none). Use
            ``DOCKER_DEFAULT_CAPABILITIES`` for Docker parity.
        resources: Custom logical resource requirements.
        readonly: If True (default), mount container image rootfs in read-only mode
            such that only ``workdir`` is writable. If False, the entire root filesystem
            is writable. Writes are isolated within a per-sandbox copy-on-write overlay
            filesystem, ensuring multiple sandboxes running the same container image do
            not interfere with each other or modify the base image.
        disk: Size cap of the writable rootfs (e.g. "10Gi"); writes past it
            fail with ENOSPC. Zero (default) means no cap. Requires
            ``readonly=False`` or a ``workdir``; see
            :class:`~ray.experimental.sandbox.config.SandboxConfig`.
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
        dns=dns,
        cidr_allowlist=cidr_allowlist,
        capabilities=capabilities,
        readonly=readonly,
        disk=disk,
        **kwargs,
    )


__all__ = [
    "create",
    "ALLOW_ALL_CIDRS",
    "DEFAULT_PUBLIC_ALLOWLIST",
    "DEFAULT_PUBLIC_DNS",
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
