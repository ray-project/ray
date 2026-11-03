from typing import Dict, List, Optional, Union

import ray
from ray.experimental.sandbox.backend.base import (
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import SandboxNotFoundError
from ray.experimental.sandbox.runtime import SandboxRuntime


@ray.remote
class Sandbox:
    """Ray actor proxy for managing scheduling, lifecycle, command execution, and file I/O for an isolated sandbox instance.

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
            runtime default; ``[]`` means none); see
            ``DOCKER_DEFAULT_CAPABILITIES``.
        readonly: If True (default), mount container image rootfs in read-only mode
            such that only ``workdir`` is writable. If False, the entire root filesystem
            is writable. Writes are isolated within a per-sandbox copy-on-write overlay
            filesystem, ensuring multiple sandboxes running the same container image do
            not interfere with each other or modify the base image.
        disk: Size cap of the writable rootfs (e.g. "10Gi"); writes past it
            fail with ENOSPC. Zero (default) means no cap. Requires
            ``readonly=False`` or a ``workdir``; see
            :class:`~ray.experimental.sandbox.config.SandboxConfig`.
        restore_from: Path to checkpoint directory if restoring an existing sandbox.
        **kwargs: Additional parameters passed to runtime.
    """

    def __init__(
        self,
        image: Optional[str] = None,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: Optional[str] = None,
        dns: Optional[List[str]] = None,
        cidr_allowlist: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        readonly: Optional[bool] = None,
        disk: Union[str, int, float] = 0,
        restore_from: Optional[str] = None,
        **kwargs,
    ):
        if "gpu_ids" in kwargs:
            raise TypeError(
                "Sandbox does not accept 'gpu_ids'. A sandbox gets every GPU "
                "Ray assigned this actor, from ray.get_gpu_ids()."
            )

        # Extract CPU and memory from Ray assigned resources if not explicitly provided
        # For restore_from, do not default to actor-assigned resources unless explicitly requested,
        # so that the checkpoint's recorded resource limits are preserved.
        if not restore_from:
            env = env or {}
            if readonly is None:
                readonly = True
            try:
                assigned = ray.get_runtime_context().get_assigned_resources()
                if cpu is None and "CPU" in assigned and assigned["CPU"] > 0:
                    cpu = float(assigned["CPU"])

                if (memory is None) and "memory" in assigned and assigned["memory"] > 0:
                    memory = int(assigned["memory"])
            except Exception:
                pass

        self.runtime = SandboxRuntime()
        if restore_from:
            restore_kwargs = dict(kwargs)
            if env is not None:
                restore_kwargs["env"] = env
            if workdir is not None:
                restore_kwargs["workdir"] = workdir
            if network is not None:
                restore_kwargs["network"] = network
            if dns is not None:
                restore_kwargs["dns"] = dns
            if cidr_allowlist is not None:
                restore_kwargs["cidr_allowlist"] = cidr_allowlist
            if capabilities is not None:
                restore_kwargs["capabilities"] = capabilities
            if readonly is not None:
                restore_kwargs["readonly"] = readonly
            if disk:
                restore_kwargs["disk"] = disk

            self.instance_id = self.runtime.restore(
                checkpoint_path=restore_from,
                cpu=cpu,
                memory=memory,
                ttl_seconds=ttl_seconds,
                timeout_seconds=timeout_seconds,
                **restore_kwargs,
            )
        else:
            if not image:
                raise ValueError("Must provide either 'image' or 'restore_from'")
            if network is None:
                network = "none"
            # A sandbox gets every GPU Ray assigned this actor. Unlike cpu and
            # memory, the actor has no other use for them, so there is nothing
            # to override. An error resolving them propagates, so the sandbox
            # never boots without them.
            gpu_ids = [str(i) for i in ray.get_gpu_ids()] or None
            self.instance_id = self.runtime.create(
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
                gpu_ids=gpu_ids,
                **kwargs,
            )

    def __del__(self):
        try:
            self.delete()
        except Exception:
            pass

    def get_instance_id(self) -> str:
        """Get the unique instance ID for the sandbox.

        Returns:
            The instance ID string.
        """
        return self.instance_id

    def get_config(self) -> SandboxConfig:
        """Get the sandbox configuration used by the runtime.

        Returns:
            SandboxConfig of the sandbox instance.
        """
        meta = self.runtime._backend._sandbox_metadata.get(self.instance_id)
        if not meta:
            raise SandboxNotFoundError(
                f"Sandbox '{self.instance_id}' not found or already deleted."
            )
        return meta["config"]

    def exec(
        self,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command inside the sandbox.

        Args:
            command: Command to execute, either as a string or a list of arguments.
            timeout: Maximum execution time in seconds.
            cwd: Working directory inside the sandbox for command execution.
            env: Environment variables to set for the command.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        return self.runtime.exec(
            self.instance_id, command, timeout=timeout, cwd=cwd, env=env, shell=shell
        )

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Copy local file into the sandbox.

        Args:
            local_path: Path to the source file on the local filesystem.
            remote_path: Destination path inside the sandbox.
        """
        self.runtime.upload_file(self.instance_id, local_path, remote_path)

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Copy file from the sandbox to local.

        Args:
            remote_path: Path to the source file inside the sandbox.
            local_path: Destination path on the local filesystem.
        """
        self.runtime.download_file(self.instance_id, remote_path, local_path)

    def write_file(self, path: str, content: Union[str, bytes]) -> None:
        """Write content directly to a file inside the sandbox.

        Args:
            path: Destination file path inside the sandbox.
            content: String or binary content to write into the file.
        """
        self.runtime.write_file(self.instance_id, path, content)

    def read_file(self, path: str) -> bytes:
        """Read binary content from a file inside the sandbox.

        Args:
            path: Path to the file inside the sandbox to read.

        Returns:
            File content as bytes.
        """
        return self.runtime.read_file(self.instance_id, path)

    def get_status(self) -> SandboxStatus:
        """Query operational status of the sandbox.

        Returns:
            SandboxStatus of the sandbox instance.
        """
        return self.runtime.get_status(self.instance_id)

    def set_egress_allowlist(self, cidrs: List[str]) -> None:
        """Replace the egress allowlist of this network="public" sandbox.

        The update is atomic. Connections the new list no longer allows stall
        from their next packet, and no list opens the denylist; see
        ``SandboxRuntime.set_egress_allowlist``.

        Args:
            cidrs: The new allowlist, in the forms ``cidr_allowlist`` takes.
                ``ALLOW_ALL_CIDRS`` opens every destination outside the
                denylist.
        """
        self.runtime.set_egress_allowlist(self.instance_id, cidrs)

    def checkpoint(
        self,
        checkpoint_path: Optional[str] = None,
        leave_running: bool = True,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> str:
        """Create a checkpoint of the running sandbox.

        Args:
            checkpoint_path: Path where the checkpoint directory will be saved.
            leave_running: If True, keep the sandbox running after checkpointing.
            timeout_seconds: Timeout for the checkpoint operation.
            **kwargs: Additional parameters passed to runtime checkpoint.

        Returns:
            Absolute path to the checkpoint directory.
        """
        return self.runtime.checkpoint(
            self.instance_id,
            checkpoint_path=checkpoint_path,
            leave_running=leave_running,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )

    def pause(self, timeout_seconds: float = 10.0) -> None:
        """Pause the running sandbox instance.

        Args:
            timeout_seconds: Timeout for pause operation.
        """
        self.runtime.pause(self.instance_id, timeout_seconds=timeout_seconds)

    def resume(self, timeout_seconds: float = 10.0) -> None:
        """Resume a paused sandbox instance.

        Args:
            timeout_seconds: Timeout for resume operation.
        """
        self.runtime.resume(self.instance_id, timeout_seconds=timeout_seconds)

    def delete(self) -> None:
        """Clean up and terminate the sandbox instance."""
        self.runtime.delete(self.instance_id)

    def terminate(self) -> None:
        """Clean up and terminate the sandbox instance."""
        self.delete()
