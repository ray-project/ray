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

    Attributes:
        function_name (str): The name of the function that failed and produced
            the RayTaskError.
        traceback_str (str): The traceback from the exception.
    """

    def __init__(self,
                 function_name,
                 traceback_str,
                 cause_cls,
                 proctitle=None,
                 pid=None,
                 ip=None):
        """Initialize a RayTaskError."""
        import ray
        if proctitle:
            self.proctitle = proctitle
        else:
            self.proctitle = setproctitle.getproctitle()
        self.pid = pid or os.getpid()
        self.ip = ip or ray._private.services.get_node_ip_address()
        self.function_name = function_name
        self.traceback_str = traceback_str
        self.cause_cls = cause_cls
        assert traceback_str is not None

    def as_instanceof_cause(self):
        """Returns copy that is an instance of the cause's Python class.

        The returned exception will inherit from both RayTaskError and the
        cause class.
        """

        if issubclass(RayTaskError, self.cause_cls):
            return self  # already satisfied

        if issubclass(self.cause_cls, RayError):
            return self  # don't try to wrap ray internal errors

        class cls(RayTaskError, self.cause_cls):
            def __init__(self, function_name, traceback_str, cause_cls,
                         proctitle, pid, ip):
                RayTaskError.__init__(self, function_name, traceback_str,
                                      cause_cls, proctitle, pid, ip)

        name = f"RayTaskError({self.cause_cls.__name__})"
        cls.__name__ = name
        cls.__qualname__ = name

        return cls(self.function_name, self.traceback_str, self.cause_cls,
                   self.proctitle, self.pid, self.ip)

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
    """

    def __str__(self):
        return ("The actor died unexpectedly before finishing this task. "
                "Check python-core-worker-*.log files for more information.")


class RaySystemError(RayError):
    """Indicates that Ray encountered a system error.

    This exception can be thrown when the raylet is killed.
    """

    def __init__(self, client_exc):
        self.client_exc = client_exc

    def __str__(self):
        return f"System error: {self.client_exc}"


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
        object_ref: ID of the object.
    """

    def __init__(self, object_ref):
        self.object_ref = object_ref

    def __str__(self):
        return (f"Object {self.object_ref.hex()} is lost due to node failure.")


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
