class SandboxError(Exception):
    """Base exception for all Ray Sandbox errors."""

    pass


class SandboxCreationError(SandboxError):
    """Raised when sandbox creation fails or times out."""

    pass


class SandboxTimeoutError(SandboxError):
    """Raised when a command execution or operation inside a sandbox times out."""

    pass


class SandboxExecError(SandboxError):
    """Raised when a command execution encounters an unexpected system/backend error."""

    pass


class SandboxNotFoundError(SandboxError):
    """Raised when a specified sandbox ID cannot be found."""

    pass
