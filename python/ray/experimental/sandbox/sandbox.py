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
        image: Container image for the sandbox environment.
        cpu: Number of CPU cores allocated to the sandbox.
        memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
        env: Environment variables to inject into the sandbox.
        work_dir: Default working directory inside the sandbox.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        labels: Optional key-value metadata labels for tracking.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode.
        network: Network mode for runsc.
        resources: Custom logical resource requirements.
        readonly: If True, mount rootfs in read-only mode (default: True).
        **kwargs: Additional parameters passed to runtime.
    """

    def __init__(
        self,
        image: Optional[str] = None,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
        env: Optional[Dict[str, str]] = None,
        work_dir: str = "/workspace",
        ttl_seconds: Optional[int] = 3600,
        labels: Optional[Dict[str, str]] = None,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: str = "none",
        resources: Optional[Dict[str, float]] = None,
        readonly: bool = True,
        **kwargs,
    ):
        env = env or {}
        labels = labels or {}
        resources = resources or {}

        # Translate resources assigned to this Ray actor into runtime config resources
        try:
            assigned = ray.get_runtime_context().get_assigned_resources()
            if "CPU" in assigned and assigned["CPU"] > 0:
                cpu = float(assigned["CPU"])

            if "memory" in assigned and assigned["memory"] > 0:
                memory = int(assigned["memory"])

            custom_resources = {}
            for k, v in assigned.items():
                if (
                    k not in ("CPU", "memory")
                    and not k.startswith("node:")
                    and k != "object_store_memory"
                    and v > 0
                ):
                    custom_resources[k] = float(v)

            if custom_resources:
                resources.update(custom_resources)
        except Exception:
            pass

        self.runtime = SandboxRuntime()
        self.instance_id = self.runtime.create(
            image=image,
            cpu=cpu,
            memory=memory,
            env=env,
            work_dir=work_dir,
            ttl_seconds=ttl_seconds,
            labels=labels,
            timeout_seconds=timeout_seconds,
            rootless=rootless,
            network=network,
            resources=resources,
            readonly=readonly,
            **kwargs,
        )

    def get_instance_id(self) -> str:
        """Get the unique instance ID for the sandbox."""
        return self.instance_id

    def get_config(self) -> SandboxConfig:
        """Get the sandbox configuration used by the runtime."""
        return self.runtime._backend._sandbox_meta[self.instance_id]["config"]

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

    def get_config(self) -> SandboxConfig:
        """Get the configuration of the sandbox instance."""
        return ray.get(self._actor.get_config.remote())

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
