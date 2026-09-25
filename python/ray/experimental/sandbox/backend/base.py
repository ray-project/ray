from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from ray.experimental.sandbox.config import SandboxConfig
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SandboxStatus(Enum):
    """Operational status of a Sandbox."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"


@PublicAPI(stability="alpha")
@dataclass
class ExecResult:
    """Result of executing a command inside a Sandbox.

    Attributes:
        exit_code: Command process exit code (0 indicates success).
        stdout: Standard output text string.
        stderr: Standard error text string.
        duration_seconds: Command execution time in seconds.
    """

    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float

    @property
    def duration_ms(self) -> float:
        """Execution duration in milliseconds."""
        return self.duration_seconds * 1000.0


# Alias for ExecResult
ExecutionResult = ExecResult


class BaseSandboxBackend(ABC):
    """Abstract Base Class defining the contract for Sandbox Backends."""

    def __init__(self, image_manager: Optional[Any] = None):
        if image_manager is None:
            from ray.experimental.sandbox.image_manager import ImageManager

            image_manager = ImageManager()
        self._image_manager = image_manager

    @property
    def image_manager(self):
        return self._image_manager

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
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command synchronously inside the sandbox.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            command: Command string or list of argument strings.
            timeout: Optional maximum execution time in seconds.
            cwd: Optional working directory override.
            env: Optional additional environment variables.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).
            user: Optional user to run as: a numeric uid, "uid:gid", or a
                user (optionally ":group") name, resolved against the
                /etc/passwd and /etc/group inside the running sandbox
                (default: the image user).

        Returns:
            An ExecResult instance containing stdout, stderr, and exit code.
        """
        pass

    @abstractmethod
    def write_file(
        self,
        sandbox_id: str,
        path: str,
        content: Union[str, bytes],
        append: bool = False,
    ) -> None:
        """Write content to a file inside the sandbox.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            path: Target file path inside the sandbox environment.
            content: Text string or raw bytes to write.
            append: Append to the file instead of truncating it.
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

    @abstractmethod
    def checkpoint_sandbox(
        self,
        sandbox_id: str,
        checkpoint_path: Optional[str] = None,
        leave_running: bool = True,
        compression: str = "none",
        exclude_committed_zero_pages: bool = True,
        direct: bool = False,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> Dict[str, Any]:
        """Save the sandbox state to a checkpoint bundle directory.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            checkpoint_path: Directory path where the checkpoint bundle will be saved.
                If None, a default path inside the sandbox directory will be used.
            leave_running: If True, keep the sandbox running after checkpointing.
            compression: Compression level ('none' or 'flate-best-speed').
            exclude_committed_zero_pages: If True, exclude committed zero-filled pages.
            direct: If True, use O_DIRECT for writing checkpoint pages.
            timeout_seconds: Timeout in seconds for the checkpoint operation.
            **kwargs: Backend-specific arguments.

        Returns:
            Dictionary containing checkpoint metadata.
        """
        pass

    @abstractmethod
    def restore_sandbox(
        self,
        checkpoint_path: str,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        background: bool = True,
        direct: bool = False,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> str:
        """Restore a new sandbox instance from a checkpoint bundle.

        Args:
            checkpoint_path: Directory path of the saved checkpoint bundle.
            cpu: Optional CPU allocation override.
            memory: Optional memory allocation override.
            background: If True, restore guest memory asynchronously for sub-10ms start.
            direct: If True, use O_DIRECT for reading checkpoint pages.
            timeout_seconds: Timeout in seconds for restore to reach 'running' state.
            **kwargs: Backend-specific arguments.

        Returns:
            A unique string identifier for the restored sandbox instance.
        """
        pass

    @abstractmethod
    def pause_sandbox(
        self, sandbox_id: str, timeout_seconds: Optional[float] = None
    ) -> None:
        """Pause all processes inside the sandbox without disk serialization.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            timeout_seconds: Optional timeout for the pause operation.
        """
        pass

    @abstractmethod
    def resume_sandbox(
        self, sandbox_id: str, timeout_seconds: Optional[float] = None
    ) -> None:
        """Resume execution of a paused sandbox.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            timeout_seconds: Optional timeout for the resume operation.
        """
        pass
