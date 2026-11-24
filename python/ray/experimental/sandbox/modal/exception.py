"""Exception hierarchy for the Modal-compatible Sandbox API.

Class names mirror ``modal.exception`` so that code written against Modal
ports without touching its ``except`` clauses. Every error also derives from
:class:`ray.experimental.sandbox.exceptions.SandboxError`, so a caller that
only knows about Ray's own hierarchy can still catch everything raised here.
"""

import builtins
import contextlib

import ray.exceptions
from ray.experimental.sandbox import exceptions as _ray_exceptions
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class Error(_ray_exceptions.SandboxError):
    """Base class for every error raised by the Modal-compatible API."""


@PublicAPI(stability="alpha")
class InvalidError(Error):
    """Raised when an API is used in a way that cannot possibly succeed.

    This covers bad argument combinations, relative paths where an absolute
    one is required, and reading from a stream that was not configured as a
    pipe.
    """


@PublicAPI(stability="alpha")
class NotFoundError(Error):
    """Raised when a requested object does not exist."""


@PublicAPI(stability="alpha")
class RemoteError(Error):
    """Raised when something goes wrong on the remote side of an operation.

    Matches ``modal.exception.RemoteError``, whose only role here is to be the
    base :class:`ImageBuildError` inherits from, so that an
    ``except modal.exception.RemoteError`` ported from Modal resolves.
    """


@PublicAPI(stability="alpha")
class ConflictError(InvalidError):
    """Raised when an operation conflicts with the object's current state.

    Modal raises this from ``Sandbox.wait_until_ready()`` when the sandbox is
    already gone, which is why it derives from :class:`InvalidError` there and
    here.
    """


@PublicAPI(stability="alpha")
class AlreadyExistsError(Error):
    """Raised when creating an object that already exists.

    Named in ``Sandbox.create``'s documented failure modes on Modal. Nothing
    raises it here -- there is no name registry to collide in -- but the name
    exists so a ported ``except`` clause resolves.
    """


@PublicAPI(stability="alpha")
class ImageBuildError(RemoteError):
    """Raised when a build step fails or the built image cannot be exported.

    Part of Modal's exception hierarchy. Nothing raises it in this version --
    image building is not implemented -- but it is exported so that a caller's
    ``except modal.ImageBuildError`` keeps compiling against this backend.

    Args:
        message: What went wrong.
        image_id: The image the build was for, which Modal's callers read off
            the exception to fetch build logs.
    """

    def __init__(self, message: str, image_id: str):
        super().__init__(message)
        self.image_id = image_id


@PublicAPI(stability="alpha")
class NotSupportedError(Error, NotImplementedError):
    """Raised for a Modal feature this backend does not implement.

    Inherits from :class:`NotImplementedError` as well as :class:`Error`, so
    both ``except NotImplementedError`` and ``except modal.Error`` catch it.
    Raising the builtin alone would leave these outside the hierarchy this
    module's docstring promises.
    """


@PublicAPI(stability="alpha")
class TimeoutError(Error, builtins.TimeoutError):
    """Raised when a Modal timeout occurs.

    The base of every timeout in this module, matching
    ``modal.exception.TimeoutError``. Raised directly by
    ``Sandbox.wait_until_ready()`` when a readiness probe does not pass in time.

    Shadows the builtin within this module, as upstream's does; reach the
    builtin through :mod:`builtins` here.
    """


@PublicAPI(stability="alpha")
class ExecTimeoutError(TimeoutError):
    """Raised when a single ``Sandbox.exec`` exceeds its own ``timeout``.

    Distinct from :class:`SandboxTimeoutError`, which is the whole sandbox
    running out of time. Nothing raises it here: this backend follows Modal's
    ``ContainerProcess`` behaviour of reporting a timed-out command as exit
    code ``-1`` rather than raising, and the name exists so a ported
    ``except`` clause resolves.
    """


@PublicAPI(stability="alpha")
class SandboxTimeoutError(TimeoutError, _ray_exceptions.SandboxTimeoutError):
    """Raised when a Sandbox exceeds its ``timeout``.

    Inherits from this module's :class:`TimeoutError` -- and so from the builtin
    ``TimeoutError`` too -- and from Ray's own ``SandboxTimeoutError``, so that
    ``except`` clauses written against either library catch it.
    """


@PublicAPI(stability="alpha")
class InteractiveTimeoutError(TimeoutError):
    """Raised when an interactive session cannot be established in time.

    Modal raises it from ``ContainerProcess.attach()``, which this backend does
    not implement, so nothing raises it here.
    """


@PublicAPI(stability="alpha")
class SandboxTerminatedError(Error):
    """Raised when a Sandbox is terminated externally while being waited on."""


@PublicAPI(stability="alpha")
class SnapshotCreationError(Error):
    """Raised when a Sandbox snapshot cannot be created.

    Nothing raises it here -- snapshots need Modal's control plane -- but the
    name exists so a ported ``except`` clause resolves.
    """


@PublicAPI(stability="alpha")
class ExecutionError(Error):
    """Raised when an unexpected condition is hit at runtime."""


@PublicAPI(stability="alpha")
class InternalError(Error):
    """Raised for an error internal to the backend serving a request."""


@PublicAPI(stability="alpha")
class PermissionDeniedError(Error):
    """Raised when the caller is not permitted to perform an operation.

    This is the control-plane counterpart of
    :class:`SandboxFilesystemPermissionError`, which is what a file operation
    inside a Sandbox raises.
    """


@PublicAPI(stability="alpha")
class ServiceError(Error):
    """Raised when communication with the backend fails."""


@PublicAPI(stability="alpha")
class ConnectionError(Error):
    """Raised when a connection to the backend cannot be established.

    Shadows the builtin within this module, as upstream's does; reach the
    builtin through :mod:`builtins` here.
    """


@PublicAPI(stability="alpha")
class ClientClosed(Error):
    """Raised when an operation is attempted through a closed client.

    Raised here, as on Modal, by an operation on a Sandbox handle that has
    been ``detach()``-ed.
    """


# Modal's control-plane errors that a Sandbox program can meet there. Nothing
# raises them here -- there is no control plane to refuse a request -- but the
# names exist so a ported ``except`` clause resolves.


@PublicAPI(stability="alpha")
class AuthError(Error):
    """Raised when authentication with the backend fails."""


@PublicAPI(stability="alpha")
class DataLossError(Error):
    """Raised on unrecoverable loss or corruption of data."""


@PublicAPI(stability="alpha")
class ResourceExhaustedError(Error):
    """Raised when a quota or rate limit is exhausted."""


@PublicAPI(stability="alpha")
class UnimplementedError(Error):
    """Raised when the backend does not implement an operation."""


@PublicAPI(stability="alpha")
class RequestSizeError(Error):
    """Raised when a request is too large."""


@PublicAPI(stability="alpha")
class VersionError(Error):
    """Raised when the client version is not supported."""


@PublicAPI(stability="alpha")
class FilesystemExecutionError(Error):
    """Raised when a container filesystem operation fails for an unknown reason.

    Modal raises this from its older ``FileIO`` surface, which does not exist
    here; ``Sandbox.filesystem`` reports an unclassified failure as
    :class:`SandboxFilesystemError` on both sides.
    """


@PublicAPI(stability="alpha")
class SandboxFilesystemError(Error):
    """Base class for ``Sandbox.filesystem`` errors."""


@PublicAPI(stability="alpha")
class SandboxFilesystemNotFoundError(SandboxFilesystemError):
    """Raised when a path does not exist in the Sandbox."""


@PublicAPI(stability="alpha")
class SandboxFilesystemDirectoryNotEmptyError(SandboxFilesystemError):
    """Raised when removing a non-empty directory without ``recursive=True``."""


@PublicAPI(stability="alpha")
class SandboxFilesystemIsADirectoryError(SandboxFilesystemError):
    """Raised when a path is a directory but a file was expected."""


@PublicAPI(stability="alpha")
class SandboxFilesystemNotADirectoryError(SandboxFilesystemError):
    """Raised when a path component is not a directory."""


@PublicAPI(stability="alpha")
class SandboxFilesystemPermissionError(SandboxFilesystemError):
    """Raised when the Sandbox denies access to a path."""


@PublicAPI(stability="alpha")
class SandboxFilesystemFileTooLargeError(SandboxFilesystemError):
    """Raised when a file exceeds the maximum transferable size."""


@PublicAPI(stability="alpha")
class SandboxFilesystemPathAlreadyExistsError(SandboxFilesystemError):
    """Raised when creating a path that already exists."""


@PublicAPI(stability="alpha")
class AsyncUsageWarning(UserWarning):
    """Warned when a blocking API is called from inside a running event loop.

    The call stalls that loop until it returns; use its ``.aio`` variant
    instead. Set ``MODAL_ASYNC_WARNINGS=0`` to silence it, as on Modal.
    """


@contextlib.contextmanager
def _sandbox_gone_as_not_found():
    """Report a vanished sandbox actor the way Modal reports a gone Sandbox.

    The actor dies with ``terminate()``, or when the node goes away, and every
    handle that still reaches for it -- a process, a stream, a writer -- would
    otherwise surface a raw ``RayActorError`` from outside this hierarchy.
    Modal answers the same situation with ``NotFoundError``.
    """
    try:
        yield
    except ray.exceptions.RayActorError as exc:
        raise NotFoundError(
            "The Sandbox is unavailable. This Sandbox may have already shut down."
        ) from exc


__all__ = [
    "Error",
    "AlreadyExistsError",
    "AsyncUsageWarning",
    "AuthError",
    "ClientClosed",
    "ConflictError",
    "ConnectionError",
    "DataLossError",
    "ExecutionError",
    "FilesystemExecutionError",
    "InternalError",
    "InteractiveTimeoutError",
    "InvalidError",
    "NotFoundError",
    "NotSupportedError",
    "PermissionDeniedError",
    "RemoteError",
    "RequestSizeError",
    "ResourceExhaustedError",
    "ServiceError",
    "SnapshotCreationError",
    "ImageBuildError",
    "ExecTimeoutError",
    "TimeoutError",
    "SandboxTimeoutError",
    "SandboxTerminatedError",
    "SandboxFilesystemError",
    "SandboxFilesystemNotFoundError",
    "SandboxFilesystemDirectoryNotEmptyError",
    "SandboxFilesystemIsADirectoryError",
    "SandboxFilesystemNotADirectoryError",
    "SandboxFilesystemPermissionError",
    "SandboxFilesystemFileTooLargeError",
    "SandboxFilesystemPathAlreadyExistsError",
    "UnimplementedError",
    "VersionError",
]
