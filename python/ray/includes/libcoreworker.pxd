from libc.stdint cimport int64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector

from ray.includes.unique_ids cimport (
    CJobID,
    CObjectID,
)
from ray.includes.common cimport (
    CBuffer,
    CRayStatus,
    CRayObject,
    CWorkerType,
    CLanguage,
)


cdef extern from "ray/core_worker/object_interface.h" namespace "ray" nogil:
    cdef cppclass CObjectInterface "CoreWorkerObjectInterface":
      CRayStatus Put(const CRayObject &object, CObjectID *object_id);
      CRayStatus Put(const CRayObject &object, const CObjectID &object_id);
      CRayStatus Create(const shared_ptr[CBuffer] metadata, const size_t data_size, const CObjectID &object_id, shared_ptr[CBuffer] &data);
      CRayStatus Seal(const CObjectID &object_id);
      CRayStatus Get(const c_vector[CObjectID] &ids, int64_t timeout_ms,
                 c_vector[shared_ptr[CRayObject]] *results);
      CRayStatus Wait(const c_vector[CObjectID] &object_ids, int num_objects,
                  int64_t timeout_ms, c_vector[c_bool] *results);
      CRayStatus Delete(const c_vector[CObjectID] &object_ids, c_bool local_only,
                    c_bool delete_creating_tasks);


cdef extern from "ray/core_worker/core_worker.h" namespace "ray" nogil:
    cdef cppclass CCoreWorker "ray::CoreWorker":
        CCoreWorker(const CWorkerType worker_type, const CLanguage language,
                    const c_string &store_socket,
                    const c_string &raylet_socket, const CJobID &job_id)
        CWorkerType &GetWorkerType()
        CLanguage &GetLanguage()
        CObjectInterface &Objects()
        #CTaskSubmissionInterface &Tasks()
        #CTaskExecutionInterface &Execution()
