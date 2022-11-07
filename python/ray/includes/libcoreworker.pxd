# cython: profile = False
# distutils: language = c++
# cython: embedsignature = True

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.pair cimport pair as c_pair
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport pair
from libcpp.vector cimport vector as c_vector

from ray.includes.unique_ids cimport (
    CActorID,
    CNodeID,
    CJobID,
    CTaskID,
    CObjectID,
    CPlacementGroupID,
    CWorkerID,
)

from ray.includes.common cimport (
    CAddress,
    CObjectReference,
    CActorCreationOptions,
    CBuffer,
    CPlacementGroupCreationOptions,
    CObjectLocation,
    CObjectReference,
    CRayFunction,
    CRayObject,
    CRayStatus,
    CTaskArg,
    CTaskOptions,
    CTaskType,
    CWorkerType,
    CLanguage,
    CGcsClientOptions,
    LocalMemoryBuffer,
    CJobConfig,
    CConcurrencyGroup,
    CSchedulingStrategy,
)
from ray.includes.function_descriptor cimport (
    CFunctionDescriptor,
)

from ray.includes.optional cimport (
    optional
)

ctypedef unordered_map[c_string, c_vector[pair[int64_t, double]]] \
    ResourceMappingType

ctypedef void (*ray_callback_function) \
    (shared_ptr[CRayObject] result_object,
     CObjectID object_id, void* user_data)

ctypedef void (*plasma_callback_function) \
    (CObjectID object_id, int64_t data_size, int64_t metadata_size)

# NOTE: This ctypedef is needed, because Cython doesn't compile
# "pair[shared_ptr[const CActorHandle], CRayStatus]".
# This is a bug of cython: https://github.com/cython/cython/issues/3967.
ctypedef shared_ptr[const CActorHandle] ActorHandleSharedPtr

cdef extern from "ray/core_worker/profiling.h" nogil:
    cdef cppclass CProfiler "ray::core::worker::Profiler":
        void Start()

    cdef cppclass CProfileEvent "ray::core::worker::ProfileEvent":
        CProfileEvent(const shared_ptr[CProfiler] profiler,
                      const c_string &event_type)
        void SetExtraData(const c_string &extra_data)

cdef extern from "ray/core_worker/profiling.h" nogil:
    cdef cppclass CProfileEvent "ray::core::worker::ProfileEvent":
        void SetExtraData(const c_string &extra_data)

cdef extern from "ray/core_worker/fiber.h" nogil:
    cdef cppclass CFiberEvent "ray::core::FiberEvent":
        CFiberEvent()
        void Wait()
        void Notify()

cdef extern from "ray/core_worker/context.h" nogil:
    cdef cppclass CWorkerContext "ray::core::WorkerContext":
        c_bool CurrentActorIsAsync()
        const c_string &GetCurrentSerializedRuntimeEnv()

cdef extern from "ray/core_worker/core_worker.h" nogil:
    cdef cppclass CActorHandle "ray::core::ActorHandle":
        CActorID GetActorID() const
        CJobID CreationJobID() const
        CLanguage ActorLanguage() const
        CFunctionDescriptor ActorCreationTaskFunctionDescriptor() const
        c_string ExtensionData() const
        int MaxPendingCalls() const

    cdef cppclass CCoreWorker "ray::core::CoreWorker":
        void ConnectToRaylet()
        CWorkerType GetWorkerType()
        CLanguage GetLanguage()

        c_vector[CObjectReference] SubmitTask(
            const CRayFunction &function,
            const c_vector[unique_ptr[CTaskArg]] &args,
            const CTaskOptions &options,
            int max_retries,
            c_bool retry_exceptions,
            const CSchedulingStrategy &scheduling_strategy,
            c_string debugger_breakpoint,
            c_string serialized_retry_exception_allowlist)
        CRayStatus CreateActor(
            const CRayFunction &function,
            const c_vector[unique_ptr[CTaskArg]] &args,
            const CActorCreationOptions &options,
            const c_string &extension_data, CActorID *actor_id)
        CRayStatus CreatePlacementGroup(
            const CPlacementGroupCreationOptions &options,
            CPlacementGroupID *placement_group_id)
        CRayStatus RemovePlacementGroup(
            const CPlacementGroupID &placement_group_id)
        CRayStatus WaitPlacementGroupReady(
            const CPlacementGroupID &placement_group_id, int64_t timeout_seconds)
        optional[c_vector[CObjectReference]] SubmitActorTask(
            const CActorID &actor_id, const CRayFunction &function,
            const c_vector[unique_ptr[CTaskArg]] &args,
            const CTaskOptions &options)
        CRayStatus KillActor(
            const CActorID &actor_id, c_bool force_kill,
            c_bool no_restart)
        CRayStatus CancelTask(const CObjectID &object_id, c_bool force_kill,
                              c_bool recursive)

        unique_ptr[CProfileEvent] CreateProfileEvent(
            const c_string &event_type)
        CRayStatus AllocateReturnObject(
            const CObjectID &object_id,
            const size_t &data_size,
            const shared_ptr[CBuffer] &metadata,
            const c_vector[CObjectID] &contained_object_id,
            int64_t *task_output_inlined_bytes,
            shared_ptr[CRayObject] *return_object)
        CRayStatus SealReturnObject(
            const CObjectID& return_id,
            shared_ptr[CRayObject] return_object,
            const CObjectID& generator_id
        )
        c_bool PinExistingReturnObject(
            const CObjectID& return_id,
            shared_ptr[CRayObject] *return_object,
            const CObjectID& generator_id
        )
        CObjectID AllocateDynamicReturnId()

        CJobID GetCurrentJobId()
        CTaskID GetCurrentTaskId()
        CNodeID GetCurrentNodeId()
        c_bool GetCurrentTaskRetryExceptions()
        CPlacementGroupID GetCurrentPlacementGroupId()
        CWorkerID GetWorkerID()
        c_bool ShouldCaptureChildTasksInPlacementGroup()
        const CActorID &GetActorId()
        void SetActorTitle(const c_string &title)
        void SetWebuiDisplay(const c_string &key, const c_string &message)
        CTaskID GetCallerId()
        const ResourceMappingType &GetResourceIDs() const
        void RemoveActorHandleReference(const CActorID &actor_id)
        CActorID DeserializeAndRegisterActorHandle(const c_string &bytes, const
                                                   CObjectID &outer_object_id)
        CRayStatus SerializeActorHandle(const CActorID &actor_id, c_string
                                        *bytes,
                                        CObjectID *c_actor_handle_id)
        ActorHandleSharedPtr GetActorHandle(const CActorID &actor_id) const
        pair[ActorHandleSharedPtr, CRayStatus] GetNamedActorHandle(
            const c_string &name, const c_string &ray_namespace)
        pair[c_vector[c_pair[c_string, c_string]], CRayStatus] ListNamedActors(
            c_bool all_namespaces)
        void AddLocalReference(const CObjectID &object_id)
        void RemoveLocalReference(const CObjectID &object_id)
        void PutObjectIntoPlasma(const CRayObject &object,
                                 const CObjectID &object_id)
        const CAddress &GetRpcAddress() const
        CAddress GetOwnerAddress(const CObjectID &object_id) const
        c_vector[CObjectReference] GetObjectRefs(
                const c_vector[CObjectID] &object_ids) const

        void GetOwnershipInfo(const CObjectID &object_id,
                              CAddress *owner_address,
                              c_string *object_status)
        void RegisterOwnershipInfoAndResolveFuture(
                const CObjectID &object_id,
                const CObjectID &outer_object_id,
                const CAddress &owner_address,
                const c_string &object_status)

        CRayStatus Put(const CRayObject &object,
                       const c_vector[CObjectID] &contained_object_ids,
                       CObjectID *object_id)
        CRayStatus Put(const CRayObject &object,
                       const c_vector[CObjectID] &contained_object_ids,
                       const CObjectID &object_id)
        CRayStatus CreateOwnedAndIncrementLocalRef(
                    const shared_ptr[CBuffer] &metadata,
                    const size_t data_size,
                    const c_vector[CObjectID] &contained_object_ids,
                    CObjectID *object_id, shared_ptr[CBuffer] *data,
                    c_bool created_by_worker,
                    const unique_ptr[CAddress] &owner_address,
                    c_bool inline_small_object)
        CRayStatus CreateExisting(const shared_ptr[CBuffer] &metadata,
                                  const size_t data_size,
                                  const CObjectID &object_id,
                                  const CAddress &owner_address,
                                  shared_ptr[CBuffer] *data,
                                  c_bool created_by_worker)
        CRayStatus SealOwned(const CObjectID &object_id, c_bool pin_object,
                             const unique_ptr[CAddress] &owner_address)
        CRayStatus SealExisting(const CObjectID &object_id, c_bool pin_object,
                                const CObjectID &generator_id,
                                const unique_ptr[CAddress] &owner_address)
        CRayStatus Get(const c_vector[CObjectID] &ids, int64_t timeout_ms,
                       c_vector[shared_ptr[CRayObject]] *results)
        CRayStatus GetIfLocal(
            const c_vector[CObjectID] &ids,
            c_vector[shared_ptr[CRayObject]] *results)
        CRayStatus Contains(const CObjectID &object_id, c_bool *has_object,
                            c_bool *is_in_plasma)
        CRayStatus Wait(const c_vector[CObjectID] &object_ids, int num_objects,
                        int64_t timeout_ms, c_vector[c_bool] *results,
                        c_bool fetch_local)
        CRayStatus Delete(const c_vector[CObjectID] &object_ids,
                          c_bool local_only)
        CRayStatus GetLocationFromOwner(
                const c_vector[CObjectID] &object_ids,
                int64_t timeout_ms,
                c_vector[shared_ptr[CObjectLocation]] *results)
        CRayStatus TriggerGlobalGC()
        c_string MemoryUsageString()

        CWorkerContext &GetWorkerContext()
        void YieldCurrentFiber(CFiberEvent &coroutine_done)

        unordered_map[CObjectID, pair[size_t, size_t]] GetAllReferenceCounts()

        void GetAsync(const CObjectID &object_id,
                      ray_callback_function success_callback,
                      void* python_future)

        CRayStatus PushError(const CJobID &job_id, const c_string &type,
                             const c_string &error_message, double timestamp)
        CRayStatus SetResource(const c_string &resource_name,
                               const double capacity,
                               const CNodeID &client_Id)

        CJobConfig GetJobConfig()

        c_bool IsExiting() const

        int64_t GetNumTasksSubmitted() const

        int64_t GetNumLeasesRequested() const

        unordered_map[c_string, c_vector[int64_t]] GetActorCallStats() const

    cdef cppclass CCoreWorkerOptions "ray::core::CoreWorkerOptions":
        CWorkerType worker_type
        CLanguage language
        c_string store_socket
        c_string raylet_socket
        CJobID job_id
        CGcsClientOptions gcs_options
        c_bool enable_logging
        c_string log_dir
        c_bool install_failure_signal_handler
        c_bool interactive
        c_string node_ip_address
        int node_manager_port
        c_string raylet_ip_address
        c_string driver_name
        c_string stdout_file
        c_string stderr_file
        (CRayStatus(
            const CAddress &caller_address,
            CTaskType task_type,
            const c_string name,
            const CRayFunction &ray_function,
            const unordered_map[c_string, double] &resources,
            const c_vector[shared_ptr[CRayObject]] &args,
            const c_vector[CObjectReference] &arg_refs,
            const c_string debugger_breakpoint,
            const c_string serialized_retry_exception_allowlist,
            c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *returns,
            c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *dynamic_returns,
            shared_ptr[LocalMemoryBuffer]
            &creation_task_exception_pb_bytes,
            c_bool *is_retryable_error,
            c_bool *is_application_error,
            const c_vector[CConcurrencyGroup] &defined_concurrency_groups,
            const c_string name_of_concurrency_group_to_execute,
            c_bool is_reattempt) nogil
         ) task_execution_callback
        (void(const CWorkerID &) nogil) on_worker_shutdown
        (CRayStatus() nogil) check_signals
        (void(c_bool) nogil) gc_collect
        (c_vector[c_string](
            const c_vector[CObjectReference] &) nogil) spill_objects
        (int64_t(
            const c_vector[CObjectReference] &,
            const c_vector[c_string] &) nogil) restore_spilled_objects
        (void(
            const c_vector[c_string]&,
            CWorkerType) nogil) delete_spilled_objects
        (void(
            const c_string&,
            const c_vector[c_string]&) nogil) run_on_util_worker_handler
        (void(const CRayObject&) nogil) unhandled_exception_handler
        (void(c_string *stack_out) nogil) get_lang_stack
        c_bool is_local_mode
        int num_workers
        (c_bool(const CTaskID &) nogil) kill_main
        CCoreWorkerOptions()
        (void() nogil) terminate_asyncio_thread
        c_string serialized_job_config
        int metrics_agent_port
        c_bool connect_on_start
        int runtime_env_hash
        int startup_token
        c_string session_name

    cdef cppclass CCoreWorkerProcess "ray::core::CoreWorkerProcess":
        @staticmethod
        void Initialize(const CCoreWorkerOptions &options)
        # Only call this in CoreWorker.__cinit__,
        # use CoreWorker.core_worker to access C++ CoreWorker.

        @staticmethod
        CCoreWorker &GetCoreWorker()

        @staticmethod
        void Shutdown()

        @staticmethod
        void RunTaskExecutionLoop()
