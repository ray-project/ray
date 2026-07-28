from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Union

from ray.experimental.sandbox.config import SandboxConfig


class SandboxStatus(Enum):
    """Operational status of a Sandbox."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"


@dataclass
class ExecResult:
    """Result of executing a command inside a Sandbox.

    Attributes:
        exit_code: Command process exit code (0 indicates success).
        stdout: Standard output text string.
        stderr: Standard error text string.
        duration_seconds: Command execution time in seconds.
        truncated: True if stdout or stderr was truncated.
    """

    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float
    truncated: bool = False


class BaseSandboxBackend(ABC):
    """Abstract Base Class defining the contract for Sandbox Backends."""

    @abstractmethod
    def create_sandbox(self, config: SandboxConfig) -> str:
        """Provision a new sandbox instance.

        Args:
            config: Sandbox configuration parameters.

        Returns:
            A unique string identifier for the created sandbox.
        """
        pass

    @abstractmethod
    def delete_sandbox(self, sandbox_id: str) -> None:
        """Terminate the sandbox and release all underlying resources immediately.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
        """
        pass

    @abstractmethod
    def exec_command(
        self,
        sandbox_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a command synchronously inside the sandbox.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            command: Command string or list of argument strings.
            timeout: Optional maximum execution time in seconds.
            cwd: Optional working directory override.
            env: Optional additional environment variables.

        Returns:
            An ExecResult instance containing stdout, stderr, and exit code.
        """
        pass

    @abstractmethod
    def write_file(
        self, sandbox_id: str, path: str, content: Union[str, bytes]
    ) -> None:
        """Write content to a file inside the sandbox.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            path: Target file path inside the sandbox environment.
            content: Text string or raw bytes to write.
        """
        pass

    @abstractmethod
    def read_file(self, sandbox_id: str, path: str) -> bytes:
        """Read content from a file inside the sandbox.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            path: Target file path inside the sandbox environment.

        Returns:
            Raw bytes content of the requested file.
        """
        pass

    @abstractmethod
    def get_status(self, sandbox_id: str) -> SandboxStatus:
        """Query the operational status of the sandbox.

        Args:
            sandbox_id: Unique string identifier of the sandbox.

        Returns:
            Current SandboxStatus value.
        """
        pass
