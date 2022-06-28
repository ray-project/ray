import os
from traceback import format_exception
from typing import Optional, Union

import colorama

import ray._private.ray_constants as ray_constants
import ray.cloudpickle as pickle
from ray._raylet import ActorID, TaskID, WorkerID
from ray.core.generated.common_pb2 import (
    PYTHON,
    ActorDiedErrorContext,
    Address,
    Language,
    RayException,
)
from ray.util.annotations import DeveloperAPI, PublicAPI

import setproctitle


@PublicAPI
class RayError(Exception):
    """Super class of all ray exception types."""

    def to_bytes(self):
        # Extract exc_info from exception object.
        exc_info = (type(self), self, self.__traceback__)
        formatted_exception_string = "\n".join(format_exception(*exc_info))
        return RayException(
            language=PYTHON,
            serialized_exception=pickle.dumps(self),
            formatted_exception_string=formatted_exception_string,
        ).SerializeToString()

    @staticmethod
    def from_bytes(b):
        ray_exception = RayException()
        ray_exception.ParseFromString(b)
        return RayError.from_ray_exception(ray_exception)

    @staticmethod
    def from_ray_exception(ray_exception):
        if ray_exception.language == PYTHON:
            try:
                return pickle.loads(ray_exception.serialized_exception)
            except Exception as e:
                msg = "Failed to unpickle serialized exception"
                raise RuntimeError(msg) from e
        else:
            return CrossLanguageError(ray_exception)


@PublicAPI
class CrossLanguageError(RayError):
    """Raised from another language."""

    def __init__(self, ray_exception):
        super().__init__(
            "An exception raised from {}:\n{}".format(
                Language.Name(ray_exception.language),
                ray_exception.formatted_exception_string,
            )
        )


@PublicAPI
class TaskCancelledError(RayError):
    """Raised when this task is cancelled.

    Attributes:
        task_id: The TaskID of the function that was directly
            cancelled.
    """

    def __init__(self, task_id: Optional[TaskID] = None):
        self.task_id = task_id

    def __str__(self):
        if self.task_id is None:
            return "This task or its dependency was cancelled by"
        return "Task: " + str(self.task_id) + " was cancelled"


@PublicAPI
class RayTaskError(RayError):
    """Indicates that a task threw an exception during execution.

    If a task throws an exception during execution, a RayTaskError is stored in
    the object store for each of the task's outputs. When an object is
    retrieved from the object store, the Python method that retrieved it checks
    to see if the object is a RayTaskError and if it is then an exception is
    thrown propagating the error message.
    """

    def __init__(
        self,
        function_name,
        traceback_str,
        cause,
        proctitle=None,
        pid=None,
        ip=None,
        actor_repr=None,
    ):
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
        self.actor_repr = actor_repr
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
                self.args = (cause,)

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
        code_from_internal_file = False

        # Format tracebacks.
        # Python stacktrace consists of
        # Traceback...: Indicate the next line will be a traceback.
        #   File [file_name + line number]
        #     code
        # XError: [message]
        # NOTE: For _raylet.pyx (Cython), the code is not always included.
        for i, line in enumerate(lines):
            # Convert traceback to the readable information.
            if line.startswith("Traceback "):
                traceback_line = (
                    f"{colorama.Fore.CYAN}"
                    f"{self.proctitle}"
                    f"{colorama.Fore.RESET} "
                    f"(pid={self.pid}, ip={self.ip}"
                )
                if self.actor_repr:
                    traceback_line += f", repr={self.actor_repr})"
                else:
                    traceback_line += ")"
                code_from_internal_file = False
                out.append(traceback_line)
            elif line.startswith("  File ") and (
                "ray/worker.py" in line
                or "ray/_private/" in line
                or "ray/util/tracing/" in line
                or "ray/_raylet.pyx" in line
            ):
                # TODO(windows)
                # Process the internal file line.
                # The file line always starts with 2 space and File.
                # https://github.com/python/cpython/blob/0a0a135bae2692d069b18d2d590397fbe0a0d39a/Lib/traceback.py#L421 # noqa
                if "ray._raylet.raise_if_dependency_failed" in line:
                    # It means the current task is failed
                    # due to the dependency failure.
                    # Print out an user-friendly
                    # message to explain that..
                    out.append(
                        "  At least one of the input arguments for "
                        "this task could not be computed:"
                    )
                if i + 1 < len(lines) and lines[i + 1].startswith("    "):
                    # If the next line is indented with 2 space,
                    # that means it contains internal code information.
                    # For example,
                    #   File [file_name] [line]
                    #     [code] # if the next line is indented, it is code.
                    # Note there there are 4 spaces in the code line.
                    code_from_internal_file = True
            elif code_from_internal_file:
                # If the current line is internal file's code,
                # the next line is not code anymore.
                code_from_internal_file = False
            else:
                out.append(line)
        return "\n".join(out)


@PublicAPI
class LocalRayletDiedError(RayError):
    """Indicates that the task's local raylet died."""

    def __str__(self):
        return "The task's local raylet died. Check raylet.out for more information."


@PublicAPI
class WorkerCrashedError(RayError):
    """Indicates that the worker died unexpectedly while executing a task."""

    def __str__(self):
        return (
            "The worker died unexpectedly while executing this task. "
            "Check python-core-worker-*.log files for more information."
        )


@PublicAPI
class RayActorError(RayError):
    """Indicates that the actor died unexpectedly before finishing a task.

    This exception could happen either because the actor process dies while
    executing a task, or because a task is submitted to a dead actor.

    If the actor is dead because of an exception thrown in its creation tasks,
    RayActorError will contain the creation_task_error, which is used to
    reconstruct the exception on the caller side.

    cause: The cause of the actor error. `RayTaskError` type means
        the actor has died because of an exception within `__init__`.
        `ActorDiedErrorContext` means the actor has died because of
        unexepected system error. None means the cause is not known.
        Theoretically, this should not happen,
        but it is there as a safety check.
    """

    def __init__(self, cause: Union[RayTaskError, ActorDiedErrorContext] = None):
        # -- If the actor has failed in the middle of __init__, this is set. --
        self._actor_init_failed = False
        # -- The base actor error message. --
        self.base_error_msg = "The actor died unexpectedly before finishing this task."

        if not cause:
            self.error_msg = self.base_error_msg
        elif isinstance(cause, RayTaskError):
            self._actor_init_failed = True
            self.error_msg = (
                "The actor died because of an error"
                " raised in its creation task, "
                f"{cause.__str__()}"
            )
        else:
            # Inidicating system-level actor failures.
            assert isinstance(cause, ActorDiedErrorContext)
            error_msg_lines = [self.base_error_msg]
            error_msg_lines.append(f"\tclass_name: {cause.class_name}")
            error_msg_lines.append(f"\tactor_id: {ActorID(cause.actor_id).hex()}")
            # Below items are optional fields.
            if cause.pid != 0:
                error_msg_lines.append(f"\tpid: {cause.pid}")
            if cause.name != "":
                error_msg_lines.append(f"\tname: {cause.name}")
            if cause.ray_namespace != "":
                error_msg_lines.append(f"\tnamespace: {cause.ray_namespace}")
            if cause.node_ip_address != "":
                error_msg_lines.append(f"\tip: {cause.node_ip_address}")
            error_msg_lines.append(cause.error_message)
            self.error_msg = "\n".join(error_msg_lines)

    @property
    def actor_init_failed(self) -> bool:
        return self._actor_init_failed

    def __str__(self) -> str:
        return self.error_msg

    @staticmethod
    def from_task_error(task_error: RayTaskError):
        return RayActorError(task_error)


@PublicAPI
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


@PublicAPI
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
            "to list active objects in the cluster."
        )


@PublicAPI
class OutOfDiskError(RayError):
    """Indicates that the local disk is full.

    This is raised if the attempt to store the object fails
    because both the object store and disk are full.
    """

    def __str__(self):
        # TODO(scv119): expose more disk usage information and link to a doc.
        return super(OutOfDiskError, self).__str__() + (
            "\n"
            "The object cannot be created because the local object store"
            " is full and the local disk's utilization is over capacity"
            " (95% by default)."
            "Tip: Use `df` on this node to check disk usage and "
            "`ray memory` to check object store memory usage."
        )


@PublicAPI
class ObjectLostError(RayError):
    """Indicates that the object is lost from distributed memory, due to
    node failure or system error.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __init__(self, object_ref_hex, owner_address, call_site):
        self.object_ref_hex = object_ref_hex
        self.owner_address = owner_address
        self.call_site = call_site.replace(
            ray_constants.CALL_STACK_LINE_DELIMITER, "\n  "
        )

    def _base_str(self):
        msg = f"Failed to retrieve object {self.object_ref_hex}. "
        if self.call_site:
            msg += f"The ObjectRef was created at: {self.call_site}"
        else:
            msg += (
                "To see information about where this ObjectRef was created "
                "in Python, set the environment variable "
                "RAY_record_ref_creation_sites=1 during `ray start` and "
                "`ray.init()`."
            )
        return msg

    def __str__(self):
        return (
            self._base_str()
            + "\n\n"
            + (
                f"All copies of {self.object_ref_hex} have been lost due to node "
                "failure. Check cluster logs (`/tmp/ray/session_latest/logs`) for "
                "more information about the failure."
            )
        )


@PublicAPI
class ObjectFetchTimedOutError(ObjectLostError):
    """Indicates that an object fetch timed out.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __str__(self):
        return (
            self._base_str()
            + "\n\n"
            + (
                f"Fetch for object {self.object_ref_hex} timed out because no "
                "locations were found for the object. This may indicate a "
                "system-level bug."
            )
        )


@DeveloperAPI
class ReferenceCountingAssertionError(ObjectLostError, AssertionError):
    """Indicates that an object has been deleted while there was still a
    reference to it.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __str__(self):
        return (
            self._base_str()
            + "\n\n"
            + (
                "The object has already been deleted by the reference counting "
                "protocol. This should not happen."
            )
        )


@PublicAPI
class OwnerDiedError(ObjectLostError):
    """Indicates that the owner of the object has died while there is still a
    reference to the object.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __str__(self):
        log_loc = "`/tmp/ray/session_latest/logs`"
        if self.owner_address:
            try:
                addr = Address()
                addr.ParseFromString(self.owner_address)
                ip_addr = addr.ip_address
                worker_id = WorkerID(addr.worker_id)
                log_loc = (
                    f"`/tmp/ray/session_latest/logs/*{worker_id.hex()}*`"
                    f" at IP address {ip_addr}"
                )
            except Exception:
                # Catch all to make sure we always at least print the default
                # message.
                pass

        return (
            self._base_str()
            + "\n\n"
            + (
                "The object's owner has exited. This is the Python "
                "worker that first created the ObjectRef via `.remote()` or "
                "`ray.put()`. "
                f"Check cluster logs ({log_loc}) for more "
                "information about the Python worker failure."
            )
        )


@PublicAPI
class ObjectReconstructionFailedError(ObjectLostError):
    """Indicates that the object cannot be reconstructed.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __str__(self):
        return (
            self._base_str()
            + "\n\n"
            + (
                "The object cannot be reconstructed "
                "because it was created by an actor, ray.put() call, or its "
                "ObjectRef was created by a different worker."
            )
        )


@PublicAPI
class ObjectReconstructionFailedMaxAttemptsExceededError(ObjectLostError):
    """Indicates that the object cannot be reconstructed because the maximum
    number of task retries has been exceeded.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __str__(self):
        return (
            self._base_str()
            + "\n\n"
            + (
                "The object cannot be reconstructed "
                "because the maximum number of task retries has been exceeded. "
                "To prevent this error, set "
                "`@ray.remote(max_retries=<num retries>)` (default 3)."
            )
        )


@PublicAPI
class ObjectReconstructionFailedLineageEvictedError(ObjectLostError):
    """Indicates that the object cannot be reconstructed because its lineage
    was evicted due to memory pressure.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __str__(self):
        return (
            self._base_str()
            + "\n\n"
            + (
                "The object cannot be reconstructed because its lineage has been "
                "evicted to reduce memory pressure. "
                "To prevent this error, set the environment variable "
                "RAY_max_lineage_bytes=<bytes> (default 1GB) during `ray start`."
            )
        )


@PublicAPI
class GetTimeoutError(RayError):
    """Indicates that a call to the worker timed out."""

    pass


@PublicAPI
class PlasmaObjectNotAvailable(RayError):
    """Called when an object was not available within the given timeout."""

    pass


@PublicAPI
class AsyncioActorExit(RayError):
    """Raised when an asyncio actor intentionally exits via exit_actor()."""

    pass


@PublicAPI
class RuntimeEnvSetupError(RayError):
    """Raised when a runtime environment fails to be set up.

    params:
        error_message: The error message that explains
            why runtime env setup has failed.
    """

    def __init__(self, error_message: str = None):
        self.error_message = error_message

    def __str__(self):
        msgs = ["Failed to setup runtime environment."]
        if self.error_message:
            msgs.append(self.error_message)
        return "\n".join(msgs)


@PublicAPI
class TaskPlacementGroupRemoved(RayError):
    """Raised when the corresponding placement group was removed."""

    def __str__(self):
        return "The placement group corresponding to this task has been removed."


@PublicAPI
class ActorPlacementGroupRemoved(RayError):
    """Raised when the corresponding placement group was removed."""

    def __str__(self):
        return "The placement group corresponding to this Actor has been removed."


@PublicAPI
class PendingCallsLimitExceeded(RayError):
    """Raised when the pending actor calls exceeds `max_pending_calls` option.

    This exception could happen probably because the caller calls the callee
    too frequently.
    """

    pass


@PublicAPI
class TaskUnschedulableError(RayError):
    """Raised when the task cannot be scheduled.

    One example is that the node specified through
    NodeAffinitySchedulingStrategy is dead.
    """

    def __init__(self, error_message: str):
        self.error_message = error_message

    def __str__(self):
        return f"The task is not schedulable: {self.error_message}"


@PublicAPI
class ActorUnschedulableError(RayError):
    """Raised when the actor cannot be scheduled.

    One example is that the node specified through
    NodeAffinitySchedulingStrategy is dead.
    """

    def __init__(self, error_message: str):
        self.error_message = error_message

    def __str__(self):
        return f"The actor is not schedulable: {self.error_message}"


RAY_EXCEPTION_TYPES = [
    PlasmaObjectNotAvailable,
    RayError,
    RayTaskError,
    WorkerCrashedError,
    RayActorError,
    ObjectStoreFullError,
    ObjectLostError,
    ObjectFetchTimedOutError,
    ReferenceCountingAssertionError,
    ObjectReconstructionFailedError,
    ObjectReconstructionFailedMaxAttemptsExceededError,
    ObjectReconstructionFailedLineageEvictedError,
    OwnerDiedError,
    GetTimeoutError,
    AsyncioActorExit,
    RuntimeEnvSetupError,
    TaskPlacementGroupRemoved,
    ActorPlacementGroupRemoved,
    PendingCallsLimitExceeded,
    LocalRayletDiedError,
    TaskUnschedulableError,
    ActorUnschedulableError,
]
