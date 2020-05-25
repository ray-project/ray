from libc.stdint cimport uint64_t, int32_t
from libcpp.string cimport string as c_string
from libcpp.memory cimport shared_ptr, unique_ptr
from ray.includes.common cimport CLanguage, CErrorType
from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CObjectID,
    CTaskID,
    CWorkerID
)


cdef extern from "ray/common/ray_exception.h" nogil:
    cdef cppclass CRayException "ray::RayException":
        CRayException(CErrorType error_type, const c_string &error_message,
            CLanguage language, CJobID job_id, CWorkerID worker_id, CTaskID task_id,
            CActorID actor_id, CObjectID object_id, const c_string &ip, int32_t pid,
            const c_string &proc_title, const c_string &file, uint64_t lineno,
            const c_string &function, const c_string &traceback,
            const c_string &data, shared_ptr[CRayException] cause)
        CRayException(const c_string &serialized_binary)
        CErrorType ErrorType() const
        c_string ErrorMessage() const
        CLanguage Language() const
        CJobID JobId() const
        CWorkerID WorkerId() const
        CTaskID TaskId() const
        CActorID ActorId() const
        CObjectID ObjectId() const
        c_string Ip() const
        int32_t Pid() const
        c_string ProcTitle() const
        c_string File() const
        uint64_t LineNo() const
        c_string Function() const
        c_string Traceback() const
        c_string Data() const 
        shared_ptr[CRayException] Cause() const
        c_string ToString() const
        c_string Serialize() const
