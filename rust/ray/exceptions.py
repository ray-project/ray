"""Exception classes for the Rust Ray backend.

Mirrors the exception hierarchy from python/ray/exceptions.py
so that tests can catch the expected exception types.
"""


class RayError(Exception):
    """Base class for Ray exceptions."""
    pass


class RaySystemError(RayError):
    """Raised for system-level errors in Ray."""
    pass


class RayTaskError(RayError):
    """Wraps an exception that occurred during remote task execution.

    The string representation includes the cause type name so that tests
    can check for specific error messages (e.g. "nixlBackendError").
    """

    def __init__(self, function_name="", cause=None, traceback_str=""):
        self.function_name = function_name
        self.cause = cause
        self.traceback_str = traceback_str
        if cause is not None:
            cause_type = type(cause).__name__
            msg = (
                f"ray::{{func_name}}() "
                f"({cause_type}) {cause}"
            ).format(func_name=function_name)
        else:
            msg = f"ray::{function_name}() failed"
        if traceback_str:
            msg += f"\n{traceback_str}"
        super().__init__(msg)

    def as_instanceof_cause(self):
        """Return an exception that is an instance of both RayTaskError and the cause type."""
        if self.cause is None:
            return self

        cause_cls = type(self.cause)

        class _MergedError(RayTaskError, cause_cls):
            def __init__(self, task_error):
                # Don't call cause_cls.__init__ to avoid argument issues
                Exception.__init__(self, str(task_error))
                self.function_name = task_error.function_name
                self.cause = task_error.cause
                self.traceback_str = task_error.traceback_str

        merged = _MergedError(self)
        return merged


class ActorDiedError(RayError):
    """Raised when an actor dies before completing a task."""

    def __init__(self, message="The actor died unexpectedly before finishing this task."):
        super().__init__(message)


class RayDirectTransportError(RaySystemError):
    """Raised when there is an error during a Ray direct transport transfer."""
    pass
