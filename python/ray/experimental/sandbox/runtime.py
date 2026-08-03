import asyncio
import os
from abc import ABC
from typing import Dict, List, Optional, Union

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig


class SandboxRuntime(ABC):
    """Low-level interface for managing local sandbox runtime environments.

    Args:
        backend: Optional BaseSandboxBackend instance. If None, defaults to GVisorSandboxBackend.
    """

    def __init__(self, backend: Optional[BaseSandboxBackend] = None):
        if backend is None:
            backend = GVisorSandboxBackend()
        self._backend = backend

    def create(self, config: Optional[SandboxConfig] = None, **kwargs) -> str:
        """Provision the sandbox instance and return unique instance ID."""
        if config is None:
            config = SandboxConfig(**kwargs)
        elif kwargs:
            for k, v in kwargs.items():
                if hasattr(config, k):
                    setattr(config, k, v)
        return self._backend.create_sandbox(config)

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


class GVisorSandboxRuntime(SandboxRuntime):
    """gVisor sandbox runtime environment implementation."""

    def __init__(self, runsc_path_override: Optional[str] = None):
        super().__init__(
            backend=GVisorSandboxBackend(runsc_path_override=runsc_path_override)
        )
