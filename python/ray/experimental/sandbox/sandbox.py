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
        workdir: Default working directory inside the sandbox. By default, the
            working directory is the only writable path in the sandbox (unless
            ``readonly=False`` is set). If not provided, the container's WORKDIR is used.
        mount_workdir: Whether to bind-mount a host scratch directory at
            ``workdir`` (shadows image content there). None (default): only
            when ``readonly=True``.
        ttl_seconds: Optional time-to-live in seconds, wall-clock from
            creation (not idle time). None (default) or <= 0 disables it.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode.
        network: Network mode ("none", "public", "host", "sandbox"); see
            :class:`~ray.experimental.sandbox.config.SandboxConfig`. "public"
            is the recommended internet-access mode.
        dns: Optional nameserver IPs for the generated /etc/resolv.conf
            (public resolvers by default for "public").
        capabilities: Linux capabilities, written exactly (None keeps the
            runtime default; ``[]`` means none); see
            :data:`~ray.experimental.sandbox.config.DOCKER_DEFAULT_CAPABILITIES`.
        readonly: If True (default), mount container image rootfs in read-only mode
            such that only ``workdir`` is writable. If False, the entire root filesystem
            is writable. Writes are isolated within a per-sandbox copy-on-write overlay
            filesystem, ensuring multiple sandboxes running the same container image do
            not interfere with each other or modify the base image.
        **kwargs: Additional parameters passed to runtime.
    """

    def __init__(
        self,
        image: str,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        mount_workdir: Optional[bool] = None,
        ttl_seconds: Optional[int] = None,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: str = "none",
        dns: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        readonly: bool = True,
        **kwargs,
    ):
        env = env or {}

        # Extract CPU and memory from Ray assigned resources if not explicitly provided
        try:
            assigned = ray.get_runtime_context().get_assigned_resources()
            if (cpu is None or cpu <= 0) and "CPU" in assigned and assigned["CPU"] > 0:
                cpu = float(assigned["CPU"])

            if (memory is None) and "memory" in assigned and assigned["memory"] > 0:
                memory = int(assigned["memory"])
        except Exception:
            pass

        self.runtime = SandboxRuntime()
        self.instance_id = self.runtime.create(
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
            dns=dns,
            capabilities=capabilities,
            readonly=readonly,
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

    def delete(self) -> None:
        """Clean up and terminate the sandbox instance."""
        self.runtime.delete(self.instance_id)

    def terminate(self) -> None:
        """Clean up and terminate the sandbox instance."""
        self.delete()
