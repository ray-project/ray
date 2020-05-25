from ray.includes.ray_exception cimport CRayException, CErrorType
from ray.core.generated.common_pb2 import ErrorType
import os
import traceback
import ray
import ray.cloudpickle as pickle
import setproctitle


cdef class RayException(object):
    cdef shared_ptr[CRayException] exception    

    def __init__(self, exc_info=None, RayException cause=None):
        cdef:
            cdef JobID job_id
            cdef WorkerID worker_id
            cdef TaskID task_id
            cdef ActorID actor_id
            cdef CJobID c_job_id
            cdef CWorkerID c_worker_id
            cdef CTaskID c_task_id
            cdef CActorID c_actor_id

        if isinstance(exc_info, BaseException):
            exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
        elif not isinstance(exc_info, tuple):
            exc_info = sys.exc_info()

        worker = ray.worker.global_worker
        
        value = exc_info[1]
        if value is None:
            error_message = ""
            data = ""
        else:
            error_message = str(value)
            data = pickle.dumps(value)
        tb = exc_info[2]
        if tb is None:
            file = ""
            lineno = 0
            function = ""
            traceback_str = ""
        else:
            file = tb.tb_frame.f_code.co_filename
            lineno = tb.tb_lineno
            function = tb.tb_frame.f_code.co_name
            traceback_str = ''.join(traceback.format_tb(tb))
        ip = ray.services.get_node_ip_address()
        try:
            job_id = worker.current_job_id
            c_job_id = job_id.native()
        except:
            c_job_id = CJobID.Nil()
        try:
            worker_id = worker.worker_id
            c_worker_id = worker_id.native()
        except:
            worker_id = WorkerID.nil()
            c_worker_id = worker_id.native()
        try:
            task_id = worker.current_task_id
            c_task_id = task_id.native()
        except:
            c_task_id = CTaskID.Nil()
        try:
            actor_id = worker.actor_id
            c_actor_id = actor_id.native()
        except:
            c_actor_id = CActorID.Nil()
        self.exception.reset(new CRayException(<CErrorType><int>ErrorType.TASK_EXECUTION_EXCEPTION,
                                               error_message,
                                               LANGUAGE_PYTHON,
                                               c_job_id,
                                               c_worker_id,
                                               c_task_id,
                                               c_actor_id,
                                               CObjectID.Nil(),
                                               ip,
                                               os.getpid(),
                                               setproctitle.getproctitle(),
                                               file,
                                               lineno,
                                               function,
                                               traceback_str,
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
        cdef RayException r = RayException()
        r.exception.reset(new CRayException(b))
        return r

    def error_type(self):
        return <int>self.exception.get().ErrorType()

    def error_message(self):
        return <str>self.exception.get().ErrorMessage()

    def language(self):
        return Language(<int>self.exception.get().Language())

    def job_id(self):
        cdef JobID job_id = JobID.nil()
        job_id.data = self.exception.get().JobId()
        return job_id

    def worker_id(self):
        cdef WorkerID worker_id = WorkerID.nil()
        worker_id.data = self.exception.get().WorkerId()
        return worker_id

    def task_id(self):
        cdef TaskID task_id = TaskID.nil()
        task_id.data = self.exception.get().TaskId()
        return task_id

    def actor_id(self):
        cdef ActorID actor_id = ActorID.nil()
        actor_id.data = self.exception.get().ActorId()
        return actor_id

    def object_id(self):
        cdef ObjectID object_id = ObjectID.nil()
        object_id.data = self.exception.get().ObjectId()
        return object_id

    def ip(self):
        return <str>self.exception.get().Ip()

    def pid(self):
        return self.exception.get().Pid()

    def proc_title(self):
        return <str>self.exception.get().ProcTitle()

    def file(self):
        return <str>self.exception.get().File()

    def lineno(self):
        return self.exception.get().LineNo()

    def function(self):
        return <str>self.exception.get().Function()

    def traceback(self):
        return <str>self.exception.get().Traceback()

    def cause(self):
        cdef shared_ptr[CRayException] cause = self.exception.get().Cause()
        cdef RayException cause_ex = RayException()
        if cause.get() == nullptr:
            return None
        else:
            cause_ex.exception = cause
            return cause_ex

    def __str__(self):
        return <str>self.exception.get().ToString()
