import os
import shutil
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


def check_local_resources(cpu: float = 0.0, memory: Union[str, int, float] = 0):
    """Check that this node's cgroups can enforce ``cpu`` and ``memory``."""
    from ray.experimental.sandbox._internal.cgroup import SandboxCgroupManager
    from ray.experimental.sandbox.config import parse_memory_bytes

    config = SandboxConfig(image="resource-preflight", cpu=cpu, memory=memory)
    memory_bytes = parse_memory_bytes(config.memory) or 0
    if config.cpu > 0 or memory_bytes > 0:
        SandboxCgroupManager().check(config.cpu, memory_bytes)


def validate_checkpoint_dir(path: str) -> str:
    """Return the real path of a checkpoint directory, or raise.

    A checkpoint directory has a ``manifest.json`` with ``version`` and
    ``sandbox_id``, and a ``state`` directory. ``/``, the home directory and
    the working directory are refused whatever they hold.

    Raises:
        SandboxError: ``path`` isn't a checkpoint directory.
    """
    from ray.experimental.sandbox.backend.checkpoint_utils import (
        load_checkpoint_manifest,
    )
    from ray.experimental.sandbox.exceptions import SandboxError

    real = os.path.realpath(path)
    refused = {
        os.path.realpath(os.sep),
        os.path.realpath(os.path.expanduser("~")),
        os.path.realpath(os.getcwd()),
    }
    if real in refused:
        raise SandboxError(f"Refusing to delete '{path}' as a checkpoint")
    manifest, _ = load_checkpoint_manifest(real)
    if not isinstance(manifest, dict) or not {"version", "sandbox_id"} <= set(manifest):
        raise SandboxError(f"'{path}' is not a checkpoint directory")
    return real


@PublicAPI(stability="alpha")
@dataclass
class ExecResult:
    """Result of executing a command inside a Sandbox.

    Attributes:
        exit_code: Command process exit code (0 indicates success).
        stdout: Standard output text string.
        stderr: Standard error text string.
        duration_seconds: Command execution time in seconds.
        output: With ``combine_output``, raw stdout and stderr bytes in write
            order; ``stdout`` and ``stderr`` are then empty. Otherwise None.
    """

    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float
    output: Optional[bytes] = None

    @property
    def duration_ms(self) -> float:
        """Execution duration in milliseconds."""
        return self.duration_seconds * 1000.0


# Alias for ExecResult
ExecutionResult = ExecResult


class BaseSandboxBackend(ABC):
    """Abstract Base Class defining the contract for Sandbox Backends."""

    #: Whether the backend runs images from a local ``ImageManager`` cache. A
    #: backend that sets it False gets ``image_manager=None`` from
    #: ``SandboxRuntime`` and must provide its own ``image_manager``.
    needs_image_manager: bool = True

    #: The ``backend`` that this backend's checkpoint manifests carry (a
    #: manifest without the field is gVisor's), or None to skip the check.
    #: ``SandboxRuntime.restore`` refuses a checkpoint of another backend.
    checkpoint_format: Optional[str] = None

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
        combine_output: bool = False,
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
            combine_output: Return stdout and stderr together, as raw bytes in
                the order written, in ``ExecResult.output``.

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

    def set_egress_allowlist(self, sandbox_id: str, cidrs: List[str]) -> None:
        """Replace the egress allowlist of a running sandbox.

        Optional: a backend without a per-sandbox destination filter leaves
        this unimplemented.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            cidrs: The new allowlist; see ``SandboxConfig.cidr_allowlist``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support changing a sandbox's "
            "egress allowlist"
        )

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
        remote: bool = False,
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
            remote: If True, checkpoint path is on a remote or shared mount accessible
                across different VM nodes, allowing restoration on any node in the cluster.
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
        background: Optional[bool] = None,
        direct: bool = False,
        timeout_seconds: float = 30.0,
        remote: Optional[bool] = None,
        **kwargs,
    ) -> str:
        """Restore a new sandbox instance from a checkpoint bundle.

        Args:
            checkpoint_path: Directory path of the saved checkpoint bundle.
            cpu: Optional CPU allocation override.
            memory: Optional memory allocation override.
            background: If True, restore guest memory asynchronously for sub-10ms
                start; supported only for uncompressed checkpoints. None (default)
                uses it whenever the checkpoint is uncompressed.
            direct: If True, use O_DIRECT for reading checkpoint pages.
            timeout_seconds: Timeout in seconds for restore to reach 'running' state.
            remote: If True, treat the checkpoint as remote/shared, allowing restoration
                on a different VM node than where the checkpoint was captured. If None,
                defaults to the value recorded in the checkpoint manifest.
            **kwargs: Backend-specific arguments.

        Returns:
            A unique string identifier for the restored sandbox instance.
        """
        pass

    def check_resources(
        self, cpu: float = 0.0, memory: Union[str, int, float] = 0
    ) -> None:
        """Reject resource limits this node can't enforce, before any pull.

        The default checks that the node's cgroups can enforce them. A backend
        that doesn't run sandboxes on this node overrides it.

        Args:
            cpu: CPU limit to enforce. Zero means no limit.
            memory: Memory limit to enforce, such as "512Mi". Zero means none.
        """
        check_local_resources(cpu, memory)

    def delete_checkpoint(self, checkpoint_path: str) -> None:
        """Delete a checkpoint; later restores of it fail.

        The default removes the checkpoint directory, after checking that it
        is one (see :func:`validate_checkpoint_dir`).

        Args:
            checkpoint_path: Path of the checkpoint, as ``checkpoint`` returned.
        """
        path = validate_checkpoint_dir(str(checkpoint_path))
        shutil.rmtree(path)

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
