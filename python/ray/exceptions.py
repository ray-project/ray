import os

import colorama

try:
    import setproctitle
except ImportError:
    setproctitle = None


class RayError(Exception):
    """Super class of all ray exception types."""
    pass


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

    def __init__(self, function_name, traceback_str):
        """Initialize a RayTaskError."""
        if setproctitle:
            self.proctitle = setproctitle.getproctitle()
        else:
            self.proctitle = "ray_worker"
        self.pid = os.getpid()
        self.host = os.uname()[1]
        self.function_name = function_name
        self.traceback_str = traceback_str
        assert traceback_str is not None

    def __str__(self):
        """Format a RayTaskError as a string."""
        lines = self.traceback_str.split("\n")
        out = []
        in_worker = False
        for line in lines:
            if line.startswith("Traceback "):
                out.append("{}{}{} (pid={}, host={})".format(
                    colorama.Fore.CYAN, self.proctitle, colorama.Fore.RESET,
                    self.pid, self.host))
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
        return ("Object {} is lost (either evicted or explicitly deleted) and "
                + "cannot be reconstructed.").format(self.object_id.hex())


RAY_EXCEPTION_TYPES = [
    RayError,
    RayTaskError,
    RayWorkerError,
    RayActorError,
    UnreconstructableError,
]
