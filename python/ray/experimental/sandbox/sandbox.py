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
        workdir: Default working directory inside the sandbox. Note that the
            working directory is the only writable path in the sandbox. If not provided,
            the container's WORKDIR is used.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode.
        network: Network mode for runsc.
        readonly: If True, mount container image rootfs in read-only mode (default: True).
        **kwargs: Additional parameters passed to runtime.
    """

    def __init__(
        self,
        image: str,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        ttl_seconds: Optional[int] = 3600,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: str = "none",
        readonly: bool = True,
        **kwargs,
    ):
        env = env or {}

        # Extract CPU and memory from Ray assigned resources to use in the runtime config
        try:
            assigned = ray.get_runtime_context().get_assigned_resources()
            if "CPU" in assigned and assigned["CPU"] > 0:
                cpu = float(assigned["CPU"])

            if "memory" in assigned and assigned["memory"] > 0:
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
            ttl_seconds=ttl_seconds,
            timeout_seconds=timeout_seconds,
            rootless=rootless,
            network=network,
            readonly=readonly,
            **kwargs,
        )

    def get_instance_id(self) -> str:
        """Get the unique instance ID for the sandbox."""
        return self.instance_id

    def get_config(self) -> SandboxConfig:
        """Get the sandbox configuration used by the runtime."""
        return self.runtime._backend._sandbox_metadata[self.instance_id]["config"]

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
