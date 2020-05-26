from ray.includes.ray_exception cimport CRayException, CErrorType
from ray.core.generated.common_pb2 import ErrorType
import os
from traceback import format_exc
import ray
import ray.cloudpickle as pickle
import setproctitle


cdef class RayException(Exception):
    cdef shared_ptr[CRayException] exception    

    def __init__(self,
                 exc_info=None,
                 int error_type=ErrorType.GENERAL_ERROR,
                 str error_message=None,
                 JobID job_id=None,
                 WorkerID worker_id=None,
                 TaskID task_id=None,
                 ActorID actor_id=None,
                 ObjectID object_id=None,
                 str ip=None,
                 pid=None,
                 str proctitle=None,
                 str file=None,
                 lineno=None,
                 str function=None,
                 str traceback=None,
                 bytes data=None,
                 RayException cause=None):
        cdef:
            CJobID c_job_id = CJobID.Nil()
            CWorkerID c_worker_id = WorkerID(CUniqueID.Nil().Binary()).native()
            CTaskID c_task_id = CTaskID.Nil()
            CActorID c_actor_id = CActorID.Nil()
            CObjectID c_object_id = CObjectID.Nil()

        try:
            ErrorType.Name(error_type)
        except Exception:
            raise Exception("Invalid error type {}, valid error types:\n{}".format(
                error_type, "\n".join(str(e) for e in ErrorType.items())))

        if isinstance(exc_info, BaseException):
            exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
        elif not isinstance(exc_info, tuple):
            exc_info = sys.exc_info()

        ex = exc_info[1]
        if error_message is None:
            error_message = str(ex) if ex else ""
        if data is None:
            data = pickle.dumps(ex) if ex and isinstance(ex, BaseException) else b""
        tb = exc_info[2]
        if file is None:
            file = tb.tb_frame.f_code.co_filename if tb else ""
        if lineno is None:
            lineno = tb.tb_lineno if tb else 0
        if function is None:
            function = tb.tb_frame.f_code.co_name if tb else ""
        if traceback is None:
            traceback = ''.join(format_exc()) if tb else ""
        traceback = self._strip_traceback(traceback)

        worker = ray.worker.global_worker
        try:
            job_id = job_id or worker.current_job_id
            c_job_id = job_id.native()
        except:
            pass
        try:
            worker_id = worker_id or worker.worker_id
            c_worker_id = worker_id.native()
        except:
            pass
        try:
            task_id = task_id or worker.current_task_id
            c_task_id = task_id.native()
        except:
            pass
        try:
            actor_id = actor_id or worker.actor_id
            c_actor_id = actor_id.native()
        except:
            pass
        if object_id is not None:
            c_object_id = object_id.native()
        if ip is None:
            ip = ray.services.get_node_ip_address()
        if pid is None:
            pid = os.getpid()
        if proctitle is None:
            proctitle = setproctitle.getproctitle()
        self.exception.reset(new CRayException(<CErrorType><int>error_type,
                                               error_message,
                                               LANGUAGE_PYTHON,
                                               c_job_id,
                                               c_worker_id,
                                               c_task_id,
                                               c_actor_id,
                                               c_object_id,
                                               ip,
                                               pid,
                                               proctitle,
                                               file,
                                               lineno,
                                               function,
                                               traceback,
                                               data,
                                               shared_ptr[CRayException]() if cause is None else cause.exception))

    def python_exception(self):
        cdef bytes data
        if <int>self.exception.get().Language() == <int>LANGUAGE_PYTHON:
            data = self.exception.get().Data()
            if data:
                return pickle.loads(data)
        return None

    def binary(self):
        return self.exception.get().Serialize()

    @classmethod
    def from_binary(cls, b):
        cdef RayException r = cls()
        r.exception.reset(new CRayException(b))
        return r

    @property
    def error_type(self):
        return <int>self.exception.get().ErrorType()

    @property
    def error_message(self):
        return <str>self.exception.get().ErrorMessage()

    @property
    def language(self):
        return Language(<int>self.exception.get().Language())

    @property
    def job_id(self):
        cdef JobID job_id = JobID.nil()
        job_id.data = self.exception.get().JobId()
        return job_id

    @property
    def worker_id(self):
        cdef WorkerID worker_id = WorkerID.nil()
        worker_id.data = self.exception.get().WorkerId()
        return worker_id

    @property
    def task_id(self):
        cdef TaskID task_id = TaskID.nil()
        task_id.data = self.exception.get().TaskId()
        return task_id

    @property
    def actor_id(self):
        cdef ActorID actor_id = ActorID.nil()
        actor_id.data = self.exception.get().ActorId()
        return actor_id

    @property
    def object_id(self):
        cdef ObjectID object_id = ObjectID.nil()
        object_id.data = self.exception.get().ObjectId()
        return object_id

    @property
    def ip(self):
        return <str>self.exception.get().Ip()

    @property
    def pid(self):
        return self.exception.get().Pid()

    @property
    def proctitle(self):
        return <str>self.exception.get().ProcTitle()

    @property
    def file(self):
        return <str>self.exception.get().File()

    @property
    def lineno(self):
        return self.exception.get().LineNo()

    @property
    def function(self):
        return <str>self.exception.get().Function()

    @property
    def traceback(self):
        return <str>self.exception.get().Traceback()

    @property
    def data(self):
        return self.exception.get().Data()

    @property
    def cause(self):
        cdef shared_ptr[CRayException] cause = self.exception.get().Cause()
        cdef RayException cause_ex = RayException.__new__(RayException)
        if cause.get() == nullptr:
            return None
        else:
            cause_ex.exception = cause
            return cause_ex

    def _strip_traceback(self, traceback):
        """Strip traceback stack, remove unused lines."""
        lines = traceback.strip().split("\n")
        out = []
        in_worker = False
        for line in lines:
            if in_worker:
                in_worker = False
            elif "ray/worker.py" in line or "ray/function_manager.py" in line:
                in_worker = True
            else:
                out.append(line)
        return "\n".join(out)

    def __str__(self):
        return "Caused by:\n\n" + <str>self.exception.get().ToString()


class RayError(RayException):
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
        e = list(sys.exc_info())
        e[1] = "This task or its dependency was cancelled by"
        super(RayCancellationError, self).__init__(e, error_type=ErrorType.TASK_CANCELLED, task_id=task_id)


class RayTaskError(RayError):
    """Indicates that a task threw an exception during execution.

    If a task throws an exception during execution, a RayTaskError is stored in
    the object store for each of the task's outputs. When an object is
    retrieved from the object store, the Python method that retrieved it checks
    to see if the object is a RayTaskError and if it is then an exception is
    thrown propagating the error message.

    Attributes:
        e (BaseException): The exception that failed and produced the RayTaskError.
        cause (RayException): The cause exception.
    """

    def __init__(self, **kwargs):
        """Initialize a RayTaskError."""
        super(RayTaskError, self).__init__(error_type=ErrorType.TASK_EXECUTION_EXCEPTION, **kwargs)

    def as_instanceof_cause(self):
        """Returns copy that is an instance of the cause's Python class.

        The returned exception will inherit from both RayTaskError and the
        cause class.
        """
        cause_cls = RayTaskError
        cause_ex = self.python_exception()
        if cause_ex:
            cause_cls = type(cause_ex)

        if issubclass(RayTaskError, cause_cls):
            return self  # already satisfied

        if issubclass(cause_cls, RayError):
            return self  # don't try to wrap ray internal errors

        class cls(RayTaskError, cause_cls):
            pass

        name = "RayTaskError({})".format(cause_cls.__name__)
        cls.__name__ = name
        cls.__qualname__ = name
        cdef RayException current = self
        cdef RayException r = cls.__new__(cls)
        r.exception = current.exception
        return r


class RayWorkerError(RayError):
    """Indicates that the worker died unexpectedly while executing a task."""

    def __init__(self):
        e = list(sys.exc_info())
        e[1] = "The worker died unexpectedly while executing this task."
        super(RayWorkerError, self).__init__(e, error_type=ErrorType.WORKER_DIED)


class RayActorError(RayError):
    """Indicates that the actor died unexpectedly before finishing a task.

    This exception could happen either because the actor process dies while
    executing a task, or because a task is submitted to a dead actor.
    """

    def __init__(self):
        e = list(sys.exc_info())
        e[1] = "The actor died unexpectedly before finishing this task."
        super(RayActorError, self).__init__(e, error_type=ErrorType.ACTOR_DIED)


class RayletError(RayError):
    """Indicates that the Raylet client has errored.

    This exception can be thrown when the raylet is killed.
    """

    def __init__(self, client_exc):
        e = list(sys.exc_info())
        e[1] = "The Raylet died with this message: {}".format(client_exc)
        super(RayletError, self).__init__(e)


class ObjectStoreFullError(RayError):
    """Indicates that the object store is full.

    This is raised if the attempt to store the object fails
    because the object store is full even after multiple retries.
    """

    def __init__(self):
        e = list(sys.exc_info())
        e[1] = ("The local object store is full of objects that are still in scope"
                " and cannot be evicted. Try increasing the object store memory "
                "available with ray.init(object_store_memory=<bytes>). "
                "You can also try setting an option to fallback to LRU eviction "
                "when the object store is full by calling "
                "ray.init(lru_evict=True). See also: "
                "https://docs.ray.io/en/latest/memory-management.html.")
        super(ObjectStoreFullError, self).__init__(e, error_type=ErrorType.OBJECT_STORE_FULL);


class UnreconstructableError(RayError):
    """Indicates that an object is lost and cannot be reconstructed.

    Note, this exception only happens for actor objects. If actor's current
    state is after object's creating task, the actor cannot re-run the task to
    reconstruct the object.

    Attributes:
        object_id: ID of the object.
    """

    def __init__(self, object_id):
        e = list(sys.exc_info())
        e[1] = ("Object {} is lost (either LRU evicted or deleted by user) and "
                "cannot be reconstructed. Try increasing the object store "
                "memory available with ray.init(object_store_memory=<bytes>) "
                "or setting object store limits with "
                "ray.remote(object_store_memory=<bytes>). See also: {}".format(
                    self.object_id.hex(),
                    "https://docs.ray.io/en/latest/memory-management.html"))
        super(UnreconstructableError, self).__init__(e, object_id=object_id, error_type=ErrorType.OBJECT_UNRECONSTRUCTABLE)


class RayTimeoutError(RayError):
    """Indicates that a call to the worker timed out."""
    pass


class PlasmaObjectNotAvailable(RayError):
    """Called when an object was not available within the given timeout."""
    pass
