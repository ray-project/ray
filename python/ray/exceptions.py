import os

import colorama

import ray
import setproctitle


class RayError(Exception):
    """Super class of all ray exception types."""
    pass


class RayConnectionError(RayError):
    """Raised when ray is not yet connected but needs to be."""
    pass


class RayCancellationError(RayError):
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
        if proctitle:
            self.proctitle = proctitle
        else:
            self.proctitle = setproctitle.getproctitle()
        self.pid = pid or os.getpid()
        self.ip = ip or ray.services.get_node_ip_address()
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

        name = "RayTaskError({})".format(self.cause_cls.__name__)
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
                out.append("{}{}{} (pid={}, ip={})".format(
                    colorama.Fore.CYAN, self.proctitle, colorama.Fore.RESET,
                    self.pid, self.ip))
            elif in_worker:
                in_worker = False
            elif "ray/worker.py" in line or "ray/function_manager.py" in line:
                in_worker = True
            else:
                out.append(line)
        return "\n".join(out)


class RayWorkerError(RayError):
    """Indicates that the worker died unexpectedly while executing a task."""

    def __str__(self):
        return "The worker died unexpectedly while executing this task."


class RayActorError(RayError):
    """Indicates that the actor died unexpectedly before finishing a task.

    This exception could happen either because the actor process dies while
    executing a task, or because a task is submitted to a dead actor.
    """

    def __str__(self):
        return "The actor died unexpectedly before finishing this task."


class RayletError(RayError):
    """Indicates that the Raylet client has errored.

    This exception can be thrown when the raylet is killed.
    """

    def __init__(self, client_exc):
        self.client_exc = client_exc

    def __str__(self):
        return "The Raylet died with this message: {}".format(self.client_exc)


class ObjectStoreFullError(RayError):
    """Indicates that the object store is full.

    This is raised if the attempt to store the object fails
    because the object store is full even after multiple retries.
    """

    def __str__(self):
        return super(ObjectStoreFullError, self).__str__() + (
            "\n"
            "The local object store is full of objects that are still in scope"
            " and cannot be evicted. Try increasing the object store memory "
            "available with ray.init(object_store_memory=<bytes>). "
            "You can also try setting an option to fallback to LRU eviction "
            "when the object store is full by calling "
            "ray.init(lru_evict=True). See also: "
            "https://docs.ray.io/en/latest/memory-management.html.")


class UnreconstructableError(RayError):
    """Indicates that an object is lost and cannot be reconstructed.

    Note, this exception only happens for actor objects. If actor's current
    state is after object's creating task, the actor cannot re-run the task to
    reconstruct the object.

    Attributes:
        object_id: ID of the object.
    """

    def __init__(self, object_id):
        self.object_id = object_id

    def __str__(self):
        return (
            "Object {} is lost (either LRU evicted or deleted by user) and "
            "cannot be reconstructed. Try increasing the object store "
            "memory available with ray.init(object_store_memory=<bytes>) "
            "or setting object store limits with "
            "ray.remote(object_store_memory=<bytes>). See also: {}".format(
                self.object_id.hex(),
                "https://docs.ray.io/en/latest/memory-management.html"))


class RayTimeoutError(RayError):
    """Indicates that a call to the worker timed out."""
    pass


class PlasmaObjectNotAvailable(RayError):
    """Called when an object was not available within the given timeout."""
    pass


RAY_EXCEPTION_TYPES = [
    PlasmaObjectNotAvailable,
    RayError,
    RayTaskError,
    RayWorkerError,
    RayActorError,
    ObjectStoreFullError,
    UnreconstructableError,
    RayTimeoutError,
]
