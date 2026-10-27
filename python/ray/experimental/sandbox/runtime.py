import asyncio
import os
import threading
from typing import Callable, Dict, List, Optional, Union

from ray.experimental.sandbox._internal.cgroup import SandboxCgroupManager
from ray.experimental.sandbox.backend.base import (
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig, parse_memory_bytes
from ray.experimental.sandbox.exceptions import SandboxCreationError
from ray.experimental.sandbox.image_manager import ImageManager
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SandboxRuntime:
    """Low-level interface for managing local sandbox runtime environments."""

    def __init__(self):
        self._image_manager = ImageManager()
        self._backend = GVisorSandboxBackend(image_manager=self._image_manager)
        self._ttl_timers: Dict[str, threading.Timer] = {}

    @property
    def image_manager(self) -> ImageManager:
        """The ImageManager instance used by this runtime."""
        return self._image_manager

    @property
    def backend(self) -> GVisorSandboxBackend:
        """The backend instance used by this runtime."""
        return self._backend

    def pull_image(self, image: str, timeout_seconds: float = 120.0) -> str:
        """Download and extract container image into local cache.

        Args:
            image: Container image name or tar path.
            timeout_seconds: Request timeout.

        Returns:
            Extracted image directory path.
        """
        return self._image_manager.pull_image(image, timeout_seconds=timeout_seconds)

    def check_resources(
        self,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
    ) -> None:
        """Reject unsupported resource limits before downloading an image.

        Args:
            cpu: CPU limit to enforce. Zero means no limit.
            memory: Memory limit to enforce, such as "512Mi". Zero means no limit.
        """
        try:
            config = SandboxConfig(image="resource-preflight", cpu=cpu, memory=memory)
            memory_bytes = parse_memory_bytes(config.memory) or 0
            if config.cpu > 0 or memory_bytes > 0:
                SandboxCgroupManager().check(config.cpu, memory_bytes)
        except Exception as err:
            raise SandboxCreationError(
                f"Cannot enforce sandbox resources: {err}"
            ) from err

    def get_termination_reason(self, instance_id: str) -> Optional[str]:
        """Report an unexpected runtime exit, including a cgroup OOM kill.

        Args:
            instance_id: Sandbox instance ID.

        Returns:
            Why the sandbox stopped unexpectedly, or None if it's running or
            this runtime no longer tracks it.
        """
        return self._backend.get_termination_reason(instance_id)

    def create(
        self,
        image: str,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: str = "none",
        dns: Optional[List[str]] = None,
        cidr_allowlist: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        readonly: bool = True,
        _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = None,
        _ignore_cgroups: bool = False,
        disk: Union[str, int, float] = 0,
        **kwargs,
    ) -> str:
        """Provision the sandbox instance and return unique instance ID.

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
                creation (not idle time), enforced by this runtime with a
                daemon timer. None (default) or <= 0 disables it.
            timeout_seconds: Timeout in seconds for sandbox creation.
            rootless: If True, run gVisor in rootless mode.
            network: Network mode ("none", "public", "host", "sandbox");
                see :class:`~ray.experimental.sandbox.config.SandboxConfig`.
                "public" is the recommended internet-access mode.
            dns: Optional nameserver IPs for the generated /etc/resolv.conf
                (public resolvers by default for "public").
            cidr_allowlist: Optional egress allowlist for network="public":
                only the listed destinations are reachable (plus DNS to
                the ``dns`` resolvers). None means the public internet,
                with internal addresses left out as a best effort, and a
                denylist keeps instance metadata and the Kubernetes API
                service closed whatever the list says; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
                Change it later with :meth:`set_egress_allowlist`.
            capabilities: Linux capabilities, written exactly (None keeps
                the runtime default; ``[]`` means none). Use
                ``DOCKER_DEFAULT_CAPABILITIES`` for Docker parity.
            readonly: If True (default), mount container image rootfs in read-only mode
                such that only ``workdir`` is writable. If False, the entire root filesystem
                is writable. Writes are isolated within a per-sandbox copy-on-write overlay
                filesystem, ensuring multiple sandboxes running the same container image do
                not interfere with each other or modify the base image.
            _oci_spec_transform_fn: PRIVATE — development/testing only. Called with the fully-built OCI
                spec dict before it is written; may mutate in place or return a new dict. Must be
                cloudpickle-serializable. No stability guarantees. Accepts a transform function.
            _ignore_cgroups: PRIVATE — testing only. If True, passes --ignore-cgroups to runsc.
            disk: Size cap of the writable rootfs (e.g. "10Gi"); writes past
                it fail with ENOSPC. Zero means no cap. Requires
                ``readonly=False`` or a ``workdir``; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
            **kwargs: Additional parameters.

        Returns:
            A unique string identifier for the created sandbox.
        """
        cfg = SandboxConfig(
            image=image,
            cpu=cpu,
            memory=memory,
            env=env or {},
            workdir=workdir,
            ttl_seconds=ttl_seconds,
            timeout_seconds=timeout_seconds,
            rootless=rootless,
            network=network,
            dns=dns,
            cidr_allowlist=cidr_allowlist,
            capabilities=capabilities,
            readonly=readonly,
            _oci_spec_transform_fn=_oci_spec_transform_fn,
            _ignore_cgroups=_ignore_cgroups,
            disk=disk,
            **kwargs,
        )
        instance_id = self._backend.create_sandbox(cfg)
        if cfg.ttl_seconds is not None and cfg.ttl_seconds > 0:
            timer = threading.Timer(cfg.ttl_seconds, self._expire, args=(instance_id,))
            timer.daemon = True
            self._ttl_timers[instance_id] = timer
            timer.start()
        return instance_id

    def _expire(self, instance_id: str) -> None:
        """TTL callback: best-effort delete (the sandbox may already be gone)."""
        try:
            self.delete(instance_id)
        except Exception:
            pass

    def exec(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            command: Command to execute, either as a string or a list of arguments.
            timeout: Maximum execution time in seconds.
            cwd: Working directory inside the sandbox for command execution.
            env: Environment variables to set for the command.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).
            user: Optional user to run as: a numeric uid, "uid:gid", or a
                user (optionally ":group") name, resolved against the
                /etc/passwd and /etc/group inside the running sandbox
                (default: the image user).

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        return self._backend.exec_command(
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
            shell=shell,
            user=user,
        )

    async def exec_async(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox asynchronously.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            command: Command to execute, either as a string or a list of arguments.
            timeout: Maximum execution time in seconds.
            cwd: Working directory inside the sandbox for command execution.
            env: Environment variables to set for the command.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).
            user: Optional user to run as: a numeric uid, "uid:gid", or a
                user (optionally ":group") name, resolved against the
                /etc/passwd and /etc/group inside the running sandbox
                (default: the image user).

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        return await asyncio.to_thread(
            self.exec,
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
            shell=shell,
            user=user,
        )

    def upload_file(self, instance_id: str, local_path: str, remote_path: str) -> None:
        """Copy local file into the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            local_path: Path to the source file on the local filesystem.
            remote_path: Destination path inside the sandbox.
        """
        with open(local_path, "rb") as f:
            content = f.read()
        self._backend.write_file(instance_id, remote_path, content)

    def download_file(
        self, instance_id: str, remote_path: str, local_path: str
    ) -> None:
        """Copy file from the sandbox to local.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            remote_path: Path to the source file inside the sandbox.
            local_path: Destination path on the local filesystem.
        """
        content = self._backend.read_file(instance_id, remote_path)
        local_dir = os.path.dirname(os.path.abspath(local_path))
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(content)

    def write_file(
        self,
        instance_id: str,
        path: str,
        content: Union[str, bytes],
        append: bool = False,
    ) -> None:
        """Write string or binary content directly to a file inside the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            path: Destination file path inside the sandbox.
            content: String or binary content to write into the file.
            append: Append to the file instead of truncating it.
        """
        self._backend.write_file(instance_id, path, content, append=append)

    def read_file(self, instance_id: str, path: str) -> bytes:
        """Read binary content from a file inside the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            path: Path to the file inside the sandbox to read.

        Returns:
            File content as bytes.
        """
        return self._backend.read_file(instance_id, path)

    def get_status(self, instance_id: str) -> SandboxStatus:
        """Query operational status of the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.

        Returns:
            SandboxStatus of the sandbox instance.
        """
        return self._backend.get_status(instance_id)

    def set_egress_allowlist(self, instance_id: str, cidrs: List[str]) -> None:
        """Replace the egress allowlist of a network="public" sandbox.

        The new list takes effect for the next packet of every flow; flows
        it no longer permits stall rather than being reset. No list opens
        the denylist; ``ALLOW_ALL_CIDRS`` opens everything else, private
        networks and any Ray node on them included.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            cidrs: The new allowlist, in the forms ``cidr_allowlist`` takes.
        """
        self._backend.set_egress_allowlist(instance_id, cidrs)

    def checkpoint(
        self,
        instance_id: str,
        checkpoint_path: Optional[str] = None,
        leave_running: bool = True,
        remote: bool = False,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> str:
        """Create a checkpoint of the running sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            checkpoint_path: Path where the checkpoint directory will be saved.
                If None, a default directory inside the sandbox storage root is used.
            leave_running: If True, keep the sandbox running after checkpointing.
            remote: If True, mark checkpoint as remote/shared across VM nodes.
            timeout_seconds: Timeout for the checkpoint operation.
            **kwargs: Backend-specific arguments.

        Returns:
            Absolute path to the checkpoint directory.
        """
        res = self._backend.checkpoint_sandbox(
            sandbox_id=instance_id,
            checkpoint_path=checkpoint_path,
            leave_running=leave_running,
            remote=remote,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )
        if not leave_running:
            timer = self._ttl_timers.pop(instance_id, None)
            if timer is not None:
                timer.cancel()

        if isinstance(res, dict) and "checkpoint_path" in res:
            return res["checkpoint_path"]
        return checkpoint_path or ""

    def restore(
        self,
        checkpoint_path: str,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        ttl_seconds: Optional[int] = None,
        remote: Optional[bool] = None,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> str:
        """Restore a sandbox instance from a previously saved checkpoint.

        The restored sandbox preserves the full execution state, network mode,
        mounts, Linux capabilities, and process environment of the checkpointed
        sandbox. Cgroup resource allocations (cpu, memory) and lifecycle limits
        (ttl_seconds, timeout_seconds) can optionally be overridden.

        Args:
            checkpoint_path: Path to the checkpoint directory.
            cpu: CPU allocation override.
            memory: Memory allocation override.
            ttl_seconds: Optional time-to-live for the restored sandbox.
            remote: If True, allow restore on a different VM node than where the
                checkpoint was taken. If None, inherits from checkpoint manifest.
            timeout_seconds: Timeout for the restore operation.
            **kwargs: Backend-specific arguments.

        Returns:
            A unique string identifier for the restored sandbox.
        """
        instance_id = self._backend.restore_sandbox(
            checkpoint_path=checkpoint_path,
            cpu=cpu,
            memory=memory,
            remote=remote,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )
        if ttl_seconds is not None and ttl_seconds > 0:
            timer = threading.Timer(ttl_seconds, self._expire, args=(instance_id,))
            timer.daemon = True
            self._ttl_timers[instance_id] = timer
            timer.start()
        return instance_id

    def pause(self, instance_id: str, timeout_seconds: float = 10.0) -> None:
        """Pause a running sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            timeout_seconds: Timeout for pause operation.
        """
        self._backend.pause_sandbox(instance_id, timeout_seconds=timeout_seconds)

    def resume(self, instance_id: str, timeout_seconds: float = 10.0) -> None:
        """Resume a paused sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            timeout_seconds: Timeout for resume operation.
        """
        self._backend.resume_sandbox(instance_id, timeout_seconds=timeout_seconds)

    def delete(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        timer = self._ttl_timers.pop(instance_id, None)
        if timer is not None:
            timer.cancel()
        self._backend.delete_sandbox(instance_id)

    def terminate(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        self.delete(instance_id)

    async def delete_async(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance asynchronously.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        await asyncio.to_thread(self.delete, instance_id)
