import asyncio
from typing import Dict, List, Optional, Union

from ray.sandbox.backend.base import BaseSandboxBackend, ExecResult, SandboxStatus
from ray.sandbox.config import SandboxConfig


class Sandbox:
    """Public handle for managing and interacting with an active Sandbox instance.

    Args:
        sandbox_id: Unique identifier for the sandbox.
        backend: Backend instance managing the underlying container/pod.
        config: Configuration used when launching the sandbox.
    """

    def __init__(
        self, sandbox_id: str, backend: BaseSandboxBackend, config: SandboxConfig
    ):
        self.sandbox_id = sandbox_id
        self._backend = backend
        self.config = config

    def exec(
        self,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command synchronously inside the sandbox environment.

        Args:
            command: Command string or list of argument strings.
            timeout: Optional execution timeout in seconds.
            cwd: Optional working directory override.
            env: Optional additional environment variables.

        Returns:
            An ExecResult instance containing exit code, stdout, stderr, and metadata.
        """
        return self._backend.exec_command(
            self.sandbox_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
        )

    async def exec_async(
        self,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command asynchronously inside the sandbox environment."""
        return await asyncio.to_thread(
            self.exec,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
        )

    def write_file(self, path: str, content: Union[str, bytes]) -> None:
        """Write string or binary content to a file inside the sandbox.

        Args:
            path: Target filepath inside the sandbox.
            content: Text string or bytes content.
        """
        self._backend.write_file(self.sandbox_id, path, content)

    def read_file(self, path: str) -> bytes:
        """Read binary content from a file inside the sandbox.

        Args:
            path: Target filepath inside the sandbox.

        Returns:
            Raw bytes read from the file.
        """
        return self._backend.read_file(self.sandbox_id, path)

    def get_status(self) -> SandboxStatus:
        """Get the current operational status of the sandbox."""
        return self._backend.get_status(self.sandbox_id)

    def delete(self) -> None:
        """Terminate the sandbox and release all associated backend resources."""
        self._backend.delete_sandbox(self.sandbox_id)

    async def delete_async(self) -> None:
        """Terminate the sandbox asynchronously."""
        await asyncio.to_thread(self.delete)

    def __enter__(self) -> "Sandbox":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.delete()

    def __repr__(self) -> str:
        return f"Sandbox(id='{self.sandbox_id}', backend='{self.config.backend}')"
