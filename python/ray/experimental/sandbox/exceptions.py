from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
class SandboxError(Exception):
    """Base exception for all Ray Sandbox errors."""

    pass


@DeveloperAPI(stability="alpha")
class SandboxCreationError(SandboxError):
    """Raised when sandbox creation fails or times out."""

    pass


@DeveloperAPI(stability="alpha")
class SandboxTimeoutError(SandboxError):
    """Raised when a command execution or operation inside a sandbox times out."""

    pass


@DeveloperAPI(stability="alpha")
class SandboxExecError(SandboxError):
    """Raised when a command execution encounters an unexpected system/backend error."""

    pass


@DeveloperAPI(stability="alpha")
class SandboxNotFoundError(SandboxError):
    """Raised when a specified sandbox ID cannot be found."""

    pass
