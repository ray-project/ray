from typing import Dict, List, Optional, Union

from ray.actor import ActorHandle
from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    ExecutionResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.checkpoint_utils import (
    CheckpointPath,
    load_checkpoint_manifest,
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
from ray.experimental.sandbox.placement import image_scheduling_strategy
from ray.experimental.sandbox.runtime import SandboxRuntime
from ray.experimental.sandbox.sandbox import Sandbox
from ray.util.annotations import PublicAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


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
    :class:`~ray.experimental.sandbox.runtime.SandboxRuntime` instead. The actor
    prefers the node that ``image`` maps to, so sandboxes of one image share that
    node's image cache; see
    :func:`~ray.experimental.sandbox.image_scheduling_strategy`.

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
    # Separate Ray actor options from Sandbox container configuration
    from ray._common.ray_option_utils import actor_options as _RAY_ACTOR_OPTIONS_SCHEMA

    for key in list(kwargs.keys()):
        if key in _RAY_ACTOR_OPTIONS_SCHEMA:
            actor_opts[key] = kwargs.pop(key)

    # Ray actors default to num_cpu=1 if not set
    if cpu is not None and cpu >= 0:
        actor_opts["num_cpus"] = cpu
    if memory is not None:
        parsed_mem = parse_memory_bytes(memory)
        if parsed_mem is not None and parsed_mem > 0:
            actor_opts["memory"] = parsed_mem
    if resources:
        actor_opts["resources"] = resources
    actor_opts["scheduling_strategy"] = image_scheduling_strategy(
        image,
        num_cpus=actor_opts.get("num_cpus"),
        memory=actor_opts.get("memory"),
        resources=resources,
    )

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


@PublicAPI(stability="alpha")
def restore(
    checkpoint_path: Union[str, CheckpointPath],
    cpu: Optional[float] = None,
    memory: Optional[Union[str, int, float]] = None,
    ttl_seconds: Optional[int] = None,
    timeout_seconds: float = 30.0,
    resources: Optional[Dict[str, float]] = None,
    **kwargs,
) -> ActorHandle:
    """Restore a remote sandbox environment from a checkpoint managed by a Ray actor.

    The restored sandbox preserves the full execution state, network mode,
    mounts, Linux capabilities, and process environment of the checkpointed
    sandbox. Cgroup resource allocations (cpu, memory) and lifecycle limits
    (ttl_seconds, timeout_seconds) can optionally be overridden.

    A checkpoint lives on the node that took it, so the restored actor is
    scheduled there, with the checkpointed sandbox's CPU and memory unless
    overridden.

    Args:
        checkpoint_path: Path to the checkpoint bundle directory, or a
            :class:`~ray.experimental.sandbox.backend.checkpoint_utils.CheckpointPath`
            instance returned by ``Sandbox.checkpoint()`` or
            ``SandboxRuntime.checkpoint()``.
        cpu: Optional CPU allocation override.
        memory: Optional memory allocation override.
        ttl_seconds: Optional time-to-live in seconds.
        timeout_seconds: Timeout in seconds for restore.
        resources: Custom logical resource requirements for the Ray actor.
        **kwargs: Additional options.

    Returns:
        A Sandbox actor handle.
    """
    recorded_cpu = None
    recorded_memory = None
    target_node_id = None

    if isinstance(checkpoint_path, CheckpointPath):
        recorded_cpu = checkpoint_path.cpu
        recorded_memory = checkpoint_path.memory
        target_node_id = checkpoint_path.node_id
    else:
        # A plain path must be readable here to find the node and resources.
        manifest, _ = load_checkpoint_manifest(str(checkpoint_path))
        recorded_config = manifest.get("config", {})
        recorded_cpu = recorded_config.get("cpu")
        recorded_memory = recorded_config.get("memory")
        target_node_id = manifest.get("node_id")

    effective_cpu = cpu if cpu is not None else recorded_cpu
    effective_memory = memory if memory is not None else recorded_memory

    actor_opts = {}
    from ray._common.ray_option_utils import actor_options as _RAY_ACTOR_OPTIONS_SCHEMA

    for key in list(kwargs.keys()):
        if key in _RAY_ACTOR_OPTIONS_SCHEMA:
            actor_opts[key] = kwargs.pop(key)

    if effective_cpu is not None and effective_cpu >= 0:
        actor_opts["num_cpus"] = effective_cpu
    if effective_memory is not None:
        parsed_mem = parse_memory_bytes(effective_memory)
        if parsed_mem is not None and parsed_mem > 0:
            actor_opts["memory"] = parsed_mem
    if resources:
        actor_opts["resources"] = resources

    if target_node_id and "scheduling_strategy" not in actor_opts:
        try:
            actor_opts["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                node_id=target_node_id,
                soft=False,
            )
        except (ValueError, TypeError):
            # A checkpoint taken outside Ray records a hostname, which can't
            # pin the actor; the backend still rejects a restore elsewhere.
            pass

    return Sandbox.options(**actor_opts).remote(
        restore_from=checkpoint_path,
        cpu=cpu,
        memory=memory,
        ttl_seconds=ttl_seconds,
        timeout_seconds=timeout_seconds,
        **kwargs,
    )


__all__ = [
    "create",
    "ALLOW_ALL_CIDRS",
    "DEFAULT_PUBLIC_ALLOWLIST",
    "restore",
    "CheckpointPath",
    "DEFAULT_PUBLIC_DNS",
    "DOCKER_DEFAULT_CAPABILITIES",
    "Sandbox",
    "SandboxRuntime",
    "BaseImageManager",
    "ImageManager",
    "get_default_oci_spec",
    "image_scheduling_strategy",
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
