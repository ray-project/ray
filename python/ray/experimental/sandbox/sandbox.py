from typing import Dict, List, Optional, Union

import ray
from ray.experimental.sandbox.backend.base import (
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.runtime import SandboxRuntime


@ray.remote
class Sandbox:
    """Ray actor interface for managing scheduling and lifecycle of an isolated sandbox.

    Args:
        config: Optional SandboxConfig instance.
        **kwargs: Additional parameters passed to SandboxConfig or runtime.
    """

    def __init__(
        self,
        config: Optional[SandboxConfig] = None,
        **kwargs,
    ):
        runsc_path_override = kwargs.pop("runsc_path_override", None)
        self.runtime = SandboxRuntime(runsc_path_override=runsc_path_override)

        if config is None:
            config = SandboxConfig(**kwargs)
        elif kwargs:
            for k, v in kwargs.items():
                if hasattr(config, k):
                    setattr(config, k, v)

        self.instance_id = self.runtime.create(config)

    def get_instance_id(self) -> str:
        """Get the unique instance ID for the sandbox."""
        return self.instance_id

    def exec(
        self,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command inside the sandbox."""
        return self.runtime.exec(
            self.instance_id, command, timeout=timeout, cwd=cwd, env=env
        )

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Copy local file into the sandbox."""
        self.runtime.upload_file(self.instance_id, local_path, remote_path)

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Copy file from the sandbox to local."""
        self.runtime.download_file(self.instance_id, remote_path, local_path)

    def write_file(self, path: str, content: Union[str, bytes]) -> None:
        """Write content directly to a file inside the sandbox."""
        self.runtime.write_file(self.instance_id, path, content)

    def read_file(self, path: str) -> bytes:
        """Read binary content from a file inside the sandbox."""
        return self.runtime.read_file(self.instance_id, path)

    def get_status(self) -> SandboxStatus:
        """Query operational status of the sandbox."""
        return self.runtime.get_status(self.instance_id)

    def delete(self) -> None:
        """Clean up and terminate the sandbox instance."""
        self.runtime.delete(self.instance_id)

    def terminate(self) -> None:
        """Clean up and terminate the sandbox instance."""
        self.delete()


class SandboxHandle:
    """High-level handle interface for interacting with a Ray sandbox."""

    def __init__(self, actor_handle: ray.actor.ActorHandle):
        self._actor = actor_handle
        self._instance_id: Optional[str] = None

    @property
    def instance_id(self) -> str:
        """Get the unique instance ID for the sandbox."""
        if self._instance_id is None:
            self._instance_id = ray.get(self._actor.get_instance_id.remote())
        return self._instance_id

    @property
    def sandbox_id(self) -> str:
        """Alias for instance_id."""
        return self.instance_id

    def exec(
        self,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command inside the sandbox."""
        return ray.get(
            self._actor.exec.remote(command, timeout=timeout, cwd=cwd, env=env)
        )

    async def exec_async(
        self,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command inside the sandbox asynchronously."""
        return await self._actor.exec.remote(command, timeout=timeout, cwd=cwd, env=env)

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Copy local file into the sandbox."""
        ray.get(self._actor.upload_file.remote(local_path, remote_path))

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Copy file from the sandbox to local."""
        ray.get(self._actor.download_file.remote(remote_path, local_path))

    def write_file(self, path: str, content: Union[str, bytes]) -> None:
        """Write content directly to a file inside the sandbox."""
        ray.get(self._actor.write_file.remote(path, content))

    def read_file(self, path: str) -> bytes:
        """Read binary content from a file inside the sandbox."""
        return ray.get(self._actor.read_file.remote(path))

    def get_status(self) -> SandboxStatus:
        """Query operational status of the sandbox."""
        return ray.get(self._actor.get_status.remote())

    def delete(self) -> None:
        """Clean up and terminate the sandbox instance."""
        ray.get(self._actor.delete.remote())

    def terminate(self) -> None:
        """Clean up and terminate the sandbox instance."""
        self.delete()

    async def delete_async(self) -> None:
        """Clean up and terminate the sandbox instance asynchronously."""
        await self._actor.delete.remote()

    def __enter__(self) -> "SandboxHandle":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.delete()

    def __repr__(self) -> str:
        return f"SandboxHandle(id='{self.instance_id}')"
