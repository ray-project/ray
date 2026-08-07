import asyncio
import os
from typing import Dict, List, Optional, Union

from ray.experimental.sandbox.backend.base import (
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig


class SandboxRuntime:
    """Low-level interface for managing local gVisor sandbox runtime environments."""

    def __init__(self):
        self._backend = GVisorSandboxBackend()

    def create(
        self,
        image: str,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        ttl_seconds: Optional[int] = 3600,
        labels: Optional[Dict[str, str]] = None,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: str = "none",
        resources: Optional[Dict[str, float]] = None,
        readonly: bool = True,
        **kwargs,
    ) -> str:
        """Provision the sandbox instance and return unique instance ID.

        Args:
            image: Container image for the sandbox environment.
            cpu: Number of CPU cores allocated to the sandbox.
            memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
            env: Environment variables to inject into the sandbox.
            workdir: Default working directory inside the sandbox. Note that the
                working directory is the only writable path in the sandbox. If not provided,
                the container's WORKDIR is used.
            ttl_seconds: Optional automatic cleanup time-to-live in seconds.
            labels: Optional key-value metadata labels for tracking.
            timeout_seconds: Timeout in seconds for sandbox creation.
            rootless: If True, run gVisor in rootless mode.
            network: Network mode for runsc.
            resources: Custom logical resource requirements.
            readonly: If True, mount container image rootfs in read-only mode (default: True).
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
            labels=labels or {},
            timeout_seconds=timeout_seconds,
            rootless=rootless,
            network=network,
            resources=resources or {},
            readonly=readonly,
            **kwargs,
        )
        return self._backend.create_sandbox(cfg)

    def exec(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox."""
        return self._backend.exec_command(
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
        )

    async def exec_async(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox asynchronously."""
        return await asyncio.to_thread(
            self.exec,
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
        )

    def upload_file(self, instance_id: str, local_path: str, remote_path: str) -> None:
        """Copy local file into the sandbox."""
        with open(local_path, "rb") as f:
            content = f.read()
        self._backend.write_file(instance_id, remote_path, content)

    def download_file(
        self, instance_id: str, remote_path: str, local_path: str
    ) -> None:
        """Copy file from the sandbox to local."""
        content = self._backend.read_file(instance_id, remote_path)
        local_dir = os.path.dirname(os.path.abspath(local_path))
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(content)

    def write_file(
        self, instance_id: str, path: str, content: Union[str, bytes]
    ) -> None:
        """Write string or binary content directly to a file inside the sandbox."""
        self._backend.write_file(instance_id, path, content)

    def read_file(self, instance_id: str, path: str) -> bytes:
        """Read binary content from a file inside the sandbox."""
        return self._backend.read_file(instance_id, path)

    def get_status(self, instance_id: str) -> SandboxStatus:
        """Query operational status of the sandbox."""
        return self._backend.get_status(instance_id)

    def delete(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance."""
        self._backend.delete_sandbox(instance_id)

    def terminate(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance."""
        self.delete(instance_id)

    async def delete_async(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance asynchronously."""
        await asyncio.to_thread(self.delete, instance_id)
