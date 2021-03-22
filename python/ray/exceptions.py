import os
from traceback import format_exception

import colorama

import ray.cloudpickle as pickle
from ray.core.generated.common_pb2 import RayException, Language, PYTHON
import setproctitle


class RayError(Exception):
    """Super class of all ray exception types."""

    def to_bytes(self):
        # Extract exc_info from exception object.
        exc_info = (type(self), self, self.__traceback__)
        formatted_exception_string = "\n".join(format_exception(*exc_info))
        return RayException(
            language=PYTHON,
            serialized_exception=pickle.dumps(self),
            formatted_exception_string=formatted_exception_string
        ).SerializeToString()

    @staticmethod
    def from_bytes(b):
        ray_exception = RayException()
        ray_exception.ParseFromString(b)
        if ray_exception.language == PYTHON:
            return pickle.loads(ray_exception.serialized_exception)
        else:
            return CrossLanguageError(ray_exception)


class CrossLanguageError(RayError):
    """Raised from another language."""

    def __init__(self, ray_exception):
        super().__init__("An exception raised from {}:\n{}".format(
            Language.Name(ray_exception.language),
            ray_exception.formatted_exception_string))


class TaskCancelledError(RayError):
    """Raised when this task is cancelled.

    Attributes:
        task_id (TaskID): The TaskID of the function that was directly
            cancelled.
    """

    def __init__(self, task_id=None):
        self.task_id = task_id

    def __str__(self):
        if self.task_id is None:
            return "This task or its dependency was cancelled by"
        return "Task: " + str(self.task_id) + " was cancelled"


class RayTaskError(RayError):
    """Indicates that a task threw an exception during execution.

    If a task throws an exception during execution, a RayTaskError is stored in
    the object store for each of the task's outputs. When an object is
    retrieved from the object store, the Python method that retrieved it checks
    to see if the object is a RayTaskError and if it is then an exception is
    thrown propagating the error message.
    """

    def __init__(self,
                 function_name,
                 traceback_str,
                 cause,
                 proctitle=None,
                 pid=None,
                 ip=None):
        """Initialize a RayTaskError."""
        import ray

        # BaseException implements a __reduce__ method that returns
        # a tuple with the type and the value of self.args.
        # https://stackoverflow.com/a/49715949/2213289
        self.args = (function_name, traceback_str, cause, proctitle, pid, ip)
        if proctitle:
            self.proctitle = proctitle
        else:
            self.proctitle = setproctitle.getproctitle()
        self.pid = pid or os.getpid()
        self.ip = ip or ray.util.get_node_ip_address()
        self.function_name = function_name
        self.traceback_str = traceback_str
        # TODO(edoakes): should we handle non-serializable exception objects?
        self.cause = cause
        assert traceback_str is not None

    def as_instanceof_cause(self):
        """Returns an exception that is an instance of the cause's class.

        The returned exception will inherit from both RayTaskError and the
        cause class and will contain all of the attributes of the cause
        exception.
        """

        cause_cls = self.cause.__class__
        if issubclass(RayTaskError, cause_cls):
            return self  # already satisfied

        if issubclass(cause_cls, RayError):
            return self  # don't try to wrap ray internal errors

        error_msg = str(self)

        class cls(RayTaskError, cause_cls):
            def __init__(self, cause):
                self.cause = cause
                # BaseException implements a __reduce__ method that returns
                # a tuple with the type and the value of self.args.
                # https://stackoverflow.com/a/49715949/2213289
                self.args = (cause, )

            def __getattr__(self, name):
                return getattr(self.cause, name)

            def __str__(self):
                return error_msg

        name = f"RayTaskError({cause_cls.__name__})"
        cls.__name__ = name
        cls.__qualname__ = name

        return cls(self.cause)

    def __str__(self):
        """Format a RayTaskError as a string."""
        lines = self.traceback_str.strip().split("\n")
        out = []
        in_worker = False
        for line in lines:
            if line.startswith("Traceback "):
                out.append(f"{colorama.Fore.CYAN}"
                           f"{self.proctitle}"
                           f"{colorama.Fore.RESET} "
                           f"(pid={self.pid}, ip={self.ip})")
            elif in_worker:
                in_worker = False
            elif "ray/worker.py" in line or "ray/function_manager.py" in line:
                in_worker = True
            else:
                out.append(line)
        return "\n".join(out)


class WorkerCrashedError(RayError):
    """Indicates that the worker died unexpectedly while executing a task."""

    def __str__(self):
        return ("The worker died unexpectedly while executing this task. "
                "Check python-core-worker-*.log files for more information.")


class RayActorError(RayError):
    """Indicates that the actor died unexpectedly before finishing a task.

    This exception could happen either because the actor process dies while
    executing a task, or because a task is submitted to a dead actor.

    If the actor is dead because of an exception thrown in its creation tasks,
    RayActorError will contains this exception.
    """

    def __init__(self,
                 function_name=None,
                 traceback_str=None,
                 cause=None,
                 proctitle=None,
                 pid=None,
                 ip=None):
        # Traceback handling is similar to RayTaskError, so we create a
        # RayTaskError to reuse its function.
        # But we don't want RayActorError to inherit from RayTaskError, since
        # they have different meanings.
        self.creation_task_error = None
        if function_name and traceback_str and cause:
            self.creation_task_error = RayTaskError(
                function_name, traceback_str, cause, proctitle, pid, ip)

    def has_creation_task_error(self):
        return self.creation_task_error is not None

    def get_creation_task_error(self):
        if self.creation_task_error is not None:
            return self.creation_task_error
        return None

    def __str__(self):
        if self.creation_task_error:
            return ("The actor died because of an error" +
                    " raised in its creation task, " +
                    self.creation_task_error.__str__())
        return ("The actor died unexpectedly before finishing this task.")

    @staticmethod
    def from_task_error(task_error):
        return RayActorError(task_error.function_name,
                             task_error.traceback_str, task_error.cause,
                             task_error.proctitle, task_error.pid,
                             task_error.ip)


class RaySystemError(RayError):
    """Indicates that Ray encountered a system error.

    This exception can be thrown when the raylet is killed.
    """

    def __init__(self, client_exc, traceback_str=None):
        self.client_exc = client_exc
        self.traceback_str = traceback_str

    def __str__(self):
        error_msg = f"System error: {self.client_exc}"
        if self.traceback_str:
            error_msg += f"\ntraceback: {self.traceback_str}"
        return error_msg


class ObjectStoreFullError(RayError):
    """Indicates that the object store is full.

    This is raised if the attempt to store the object fails
    because the object store is full even after multiple retries.
    """

    def __str__(self):
        return super(ObjectStoreFullError, self).__str__() + (
            "\n"
            "The local object store is full of objects that are still in "
            "scope and cannot be evicted. Tip: Use the `ray memory` command "
            "to list active objects in the cluster.")


class ObjectLostError(RayError):
    """Indicates that an object has been lost due to node failure.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __init__(self, object_ref_hex):
        self.object_ref_hex = object_ref_hex

    def __str__(self):
        return (f"Object {self.object_ref_hex} is lost due to node failure.")


class GetTimeoutError(RayError):
    """Indicates that a call to the worker timed out."""
    pass


class PlasmaObjectNotAvailable(RayError):
    """Called when an object was not available within the given timeout."""
    pass


RAY_EXCEPTION_TYPES = [
    PlasmaObjectNotAvailable,
    RayError,
    RayTaskError,
    WorkerCrashedError,
    RayActorError,
    ObjectStoreFullError,
    ObjectLostError,
    GetTimeoutError,
]
