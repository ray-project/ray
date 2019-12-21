# cython: profile = False
# distutils: language = c++
# cython: embedsignature = True

from libc.stdint cimport int64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport pair
from libcpp.vector cimport vector as c_vector

from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CTaskID,
    CObjectID,
)
from ray.includes.common cimport (
    CAddress,
    CActorCreationOptions,
    CBuffer,
    CRayFunction,
    CRayObject,
    CRayStatus,
    CTaskArg,
    CTaskOptions,
    CTaskType,
    CWorkerType,
    CLanguage,
    CGcsClientOptions,
)
from ray.includes.task cimport CTaskSpec
from ray.includes.libraylet cimport CRayletClient

ctypedef unordered_map[c_string, c_vector[pair[int64_t, double]]] \
    ResourceMappingType

cdef extern from "ray/core_worker/profiling.h" nogil:
    cdef cppclass CProfiler "ray::worker::Profiler":
        void Start()

    cdef cppclass CProfileEvent "ray::worker::ProfileEvent":
        CProfileEvent(const shared_ptr[CProfiler] profiler,
                      const c_string &event_type)
        void SetExtraData(const c_string &extra_data)

cdef extern from "ray/core_worker/profiling.h" nogil:
    cdef cppclass CProfileEvent "ray::worker::ProfileEvent":
        void SetExtraData(const c_string &extra_data)

cdef extern from "ray/core_worker/transport/direct_actor_transport.h" nogil:
    cdef cppclass CFiberEvent "ray::FiberEvent":
        CFiberEvent()
        void Wait()
        void Notify()

cdef extern from "ray/core_worker/context.h" nogil:
    cdef cppclass CWorkerContext "ray::WorkerContext":
        c_bool CurrentActorIsAsync()

cdef extern from "ray/core_worker/core_worker.h" nogil:
    cdef cppclass CCoreWorker "ray::CoreWorker":
        CCoreWorker(const CWorkerType worker_type, const CLanguage language,
                    const c_string &store_socket,
                    const c_string &raylet_socket, const CJobID &job_id,
                    const CGcsClientOptions &gcs_options,
                    const c_string &log_dir, const c_string &node_ip_address,
                    int node_manager_port,
                    CRayStatus (
                        CTaskType task_type,
                        const CRayFunction &ray_function,
                        const unordered_map[c_string, double] &resources,
                        const c_vector[shared_ptr[CRayObject]] &args,
                        const c_vector[CObjectID] &arg_reference_ids,
                        const c_vector[CObjectID] &return_ids,
                        c_vector[shared_ptr[CRayObject]] *returns) nogil,
                    CRayStatus() nogil,
                    c_bool ref_counting_enabled)
        void Disconnect()
        CWorkerType &GetWorkerType()
        CLanguage &GetLanguage()

        void StartExecutingTasks()

        CRayStatus SubmitTask(
            const CRayFunction &function, const c_vector[CTaskArg] &args,
            const CTaskOptions &options, c_vector[CObjectID] *return_ids,
            int max_retries)
        CRayStatus CreateActor(
            const CRayFunction &function, const c_vector[CTaskArg] &args,
            const CActorCreationOptions &options, CActorID *actor_id)
        CRayStatus SubmitActorTask(
            const CActorID &actor_id, const CRayFunction &function,
            const c_vector[CTaskArg] &args, const CTaskOptions &options,
            c_vector[CObjectID] *return_ids)

        unique_ptr[CProfileEvent] CreateProfileEvent(
            const c_string &event_type)
        CRayStatus AllocateReturnObjects(
            const c_vector[CObjectID] &object_ids,
            const c_vector[size_t] &data_sizes,
            const c_vector[shared_ptr[CBuffer]] &metadatas,
            c_vector[shared_ptr[CRayObject]] *return_objects)

        # TODO(edoakes): remove this once the raylet client is no longer used
        # directly.
        CRayletClient &GetRayletClient()
        CJobID GetCurrentJobId()
        CTaskID GetCurrentTaskId()
        const CActorID &GetActorId()
        CTaskID GetCallerId()
        const ResourceMappingType &GetResourceIDs() const
        CActorID DeserializeAndRegisterActorHandle(const c_string &bytes)
        CRayStatus SerializeActorHandle(const CActorID &actor_id, c_string
                                        *bytes)
        void AddObjectIDReference(const CObjectID &object_id)
        void RemoveObjectIDReference(const CObjectID &object_id)
        void PromoteObjectToPlasma(const CObjectID &object_id)
        void PromoteToPlasmaAndGetOwnershipInfo(const CObjectID &object_id,
                                                CTaskID *owner_id,
                                                CAddress *owner_address)
        void RegisterOwnershipInfoAndResolveFuture(
                const CObjectID &object_id, const CTaskID &owner_id, const
                CAddress &owner_address)

        CRayStatus SetClientOptions(c_string client_name, int64_t limit)
        CRayStatus Put(const CRayObject &object, CObjectID *object_id)
        CRayStatus Put(const CRayObject &object, const CObjectID &object_id)
        CRayStatus Create(const shared_ptr[CBuffer] &metadata,
                          const size_t data_size, CObjectID *object_id,
                          shared_ptr[CBuffer] *data)
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

        CWorkerContext &GetWorkerContext()
        void YieldCurrentFiber(CFiberEvent &coroutine_done)
