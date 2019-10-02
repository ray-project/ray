from libc.stdint cimport int64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector

from ray.includes.unique_ids cimport (
    CJobID,
    CTaskID,
    CObjectID,
)
from ray.includes.common cimport (
    CActorCreationOptions,
    CActorHandle,
    CBuffer,
    CRayFunction,
    CRayObject,
    CRayStatus,
    CTaskArg,
    CTaskOptions,
    CWorkerType,
    CLanguage,
    CGcsClientOptions,
)
from ray.includes.libraylet cimport CRayletClient


cdef extern from "ray/core_worker/profiling.h" nogil:
    cdef cppclass CProfileEvent "ray::worker::ProfileEvent":
        void SetExtraData(const c_string &extra_data)

cdef extern from "ray/core_worker/task_interface.h" namespace "ray" nogil:
    cdef cppclass CTaskSubmissionInterface "CoreWorkerTaskInterface":
        CRayStatus SubmitTask(
            const CRayFunction &function, const c_vector[CTaskArg] &args,
            const CTaskOptions &options, c_vector[CObjectID] *return_ids)
        CRayStatus CreateActor(
            const CRayFunction &function, const c_vector[CTaskArg] &args,
            const CActorCreationOptions &options,
            unique_ptr[CActorHandle] *handle)
        CRayStatus SubmitActorTask(
            CActorHandle &handle, const CRayFunction &function,
            const c_vector[CTaskArg] &args, const CTaskOptions &options,
            c_vector[CObjectID] *return_ids)

cdef extern from "ray/core_worker/object_interface.h" nogil:
    cdef cppclass CObjectInterface "ray::CoreWorkerObjectInterface":
        CRayStatus SetClientOptions(c_string client_name, int64_t limit)
        CRayStatus Put(const CRayObject &object, CObjectID *object_id)
        CRayStatus Put(const CRayObject &object, const CObjectID &object_id)
        CRayStatus Create(const shared_ptr[CBuffer] &metadata,
                          const size_t data_size, const CObjectID &object_id,
                          shared_ptr[CBuffer] *data)
        CRayStatus Seal(const CObjectID &object_id)
        CRayStatus Get(const c_vector[CObjectID] &ids, int64_t timeout_ms,
                       c_vector[shared_ptr[CRayObject]] *results)
        CRayStatus Contains(const CObjectID &object_id, c_bool *has_object)
        CRayStatus Wait(const c_vector[CObjectID] &object_ids, int num_objects,
                        int64_t timeout_ms, c_vector[c_bool] *results)
        CRayStatus Delete(const c_vector[CObjectID] &object_ids,
                          c_bool local_only, c_bool delete_creating_tasks)
        c_string MemoryUsageString()

cdef extern from "ray/core_worker/core_worker.h" nogil:
    cdef cppclass CCoreWorker "ray::CoreWorker":
        CCoreWorker(const CWorkerType worker_type, const CLanguage language,
                    const c_string &store_socket,
                    const c_string &raylet_socket, const CJobID &job_id,
                    const CGcsClientOptions &gcs_options,
                    const c_string &log_dir, const c_string &node_ip_address,
                    void* execution_callback,
                    c_bool use_memory_store_)
        void Disconnect()
        CWorkerType &GetWorkerType()
        CLanguage &GetLanguage()
        CObjectInterface &Objects()
        CTaskSubmissionInterface &Tasks()
        # CTaskExecutionInterface &Execution()
        unique_ptr[CProfileEvent] CreateProfileEvent(
            const c_string &event_type)

        # TODO(edoakes): remove this once the raylet client is no longer used
        # directly.
        CRayletClient &GetRayletClient()
        # TODO(edoakes): remove these once the Python core worker uses the task
        # interfaces
        void SetCurrentJobId(const CJobID &job_id)
        CTaskID GetCurrentTaskId()
        void SetCurrentTaskId(const CTaskID &task_id)
