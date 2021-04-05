from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair

from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CWorkerID,
    CObjectID,
    CTaskID,
    CPlacementGroupID,
)
from ray.includes.function_descriptor cimport (
    CFunctionDescriptor,
)


cdef extern from * namespace "polyfill":
    """
    namespace polyfill {

    template <typename T>
    inline typename std::remove_reference<T>::type&& move(T& t) {
        return std::move(t);
    }

    template <typename T>
    inline typename std::remove_reference<T>::type&& move(T&& t) {
        return std::move(t);
    }

    }  // namespace polyfill
    """
    cdef T move[T](T)


cdef extern from "ray/common/status.h" namespace "ray" nogil:
    cdef cppclass StatusCode:
        pass

    cdef cppclass CRayStatus "ray::Status":
        RayStatus()
        RayStatus(StatusCode code, const c_string &msg)
        RayStatus(const CRayStatus &s)

        @staticmethod
        CRayStatus OK()

        @staticmethod
        CRayStatus OutOfMemory(const c_string &msg)

        @staticmethod
        CRayStatus KeyError(const c_string &msg)

        @staticmethod
        CRayStatus Invalid(const c_string &msg)

        @staticmethod
        CRayStatus IOError(const c_string &msg)

        @staticmethod
        CRayStatus TypeError(const c_string &msg)

        @staticmethod
        CRayStatus UnknownError(const c_string &msg)

        @staticmethod
        CRayStatus NotImplemented(const c_string &msg)

        @staticmethod
        CRayStatus ObjectStoreFull(const c_string &msg)

        @staticmethod
        CRayStatus RedisError(const c_string &msg)

        @staticmethod
        CRayStatus TimedOut(const c_string &msg)

        @staticmethod
        CRayStatus Interrupted(const c_string &msg)

        @staticmethod
        CRayStatus IntentionalSystemExit()

        @staticmethod
        CRayStatus UnexpectedSystemExit()

        @staticmethod
        CRayStatus CreationTaskError()

        @staticmethod
        CRayStatus NotFound()

        c_bool ok()
        c_bool IsOutOfMemory()
        c_bool IsKeyError()
        c_bool IsInvalid()
        c_bool IsIOError()
        c_bool IsTypeError()
        c_bool IsUnknownError()
        c_bool IsNotImplemented()
        c_bool IsObjectStoreFull()
        c_bool IsRedisError()
        c_bool IsTimedOut()
        c_bool IsInterrupted()
        c_bool ShouldExitWorker()
        c_bool IsNotFound()

        c_string ToString()
        c_string CodeAsString()
        StatusCode code()
        c_string message()

    # We can later add more of the common status factory methods as needed
    cdef CRayStatus RayStatus_OK "Status::OK"()
    cdef CRayStatus RayStatus_Invalid "Status::Invalid"()
    cdef CRayStatus RayStatus_NotImplemented "Status::NotImplemented"()


cdef extern from "ray/common/status.h" namespace "ray::StatusCode" nogil:
    cdef StatusCode StatusCode_OK "OK"
    cdef StatusCode StatusCode_OutOfMemory "OutOfMemory"
    cdef StatusCode StatusCode_KeyError "KeyError"
    cdef StatusCode StatusCode_TypeError "TypeError"
    cdef StatusCode StatusCode_Invalid "Invalid"
    cdef StatusCode StatusCode_IOError "IOError"
    cdef StatusCode StatusCode_UnknownError "UnknownError"
    cdef StatusCode StatusCode_NotImplemented "NotImplemented"
    cdef StatusCode StatusCode_RedisError "RedisError"


cdef extern from "ray/common/id.h" namespace "ray" nogil:
    const CTaskID GenerateTaskId(const CJobID &job_id,
                                 const CTaskID &parent_task_id,
                                 int parent_task_counter)


cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef cppclass CLanguage "Language":
        pass
    cdef cppclass CWorkerType "ray::WorkerType":
        pass
    cdef cppclass CTaskType "ray::TaskType":
        pass
    cdef cppclass CPlacementStrategy "ray::PlacementStrategy":
        pass
    cdef cppclass CAddress "ray::rpc::Address":
        CAddress()
        const c_string &SerializeAsString()
        void ParseFromString(const c_string &serialized)


# This is a workaround for C++ enum class since Cython has no corresponding
# representation.
cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CLanguage LANGUAGE_PYTHON "Language::PYTHON"
    cdef CLanguage LANGUAGE_CPP "Language::CPP"
    cdef CLanguage LANGUAGE_JAVA "Language::JAVA"

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CWorkerType WORKER_TYPE_WORKER "ray::WorkerType::WORKER"
    cdef CWorkerType WORKER_TYPE_DRIVER "ray::WorkerType::DRIVER"
    cdef CWorkerType WORKER_TYPE_SPILL_WORKER "ray::WorkerType::SPILL_WORKER"
    cdef CWorkerType WORKER_TYPE_RESTORE_WORKER "ray::WorkerType::RESTORE_WORKER"  # noqa: E501
    cdef CWorkerType WORKER_TYPE_UTIL_WORKER "ray::WorkerType::UTIL_WORKER"  # noqa: E501

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CTaskType TASK_TYPE_NORMAL_TASK "ray::TaskType::NORMAL_TASK"
    cdef CTaskType TASK_TYPE_ACTOR_CREATION_TASK "ray::TaskType::ACTOR_CREATION_TASK"  # noqa: E501
    cdef CTaskType TASK_TYPE_ACTOR_TASK "ray::TaskType::ACTOR_TASK"

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CPlacementStrategy PLACEMENT_STRATEGY_PACK \
        "ray::PlacementStrategy::PACK"
    cdef CPlacementStrategy PLACEMENT_STRATEGY_SPREAD \
        "ray::PlacementStrategy::SPREAD"
    cdef CPlacementStrategy PLACEMENT_STRATEGY_STRICT_PACK \
        "ray::PlacementStrategy::STRICT_PACK"
    cdef CPlacementStrategy PLACEMENT_STRATEGY_STRICT_SPREAD \
        "ray::PlacementStrategy::STRICT_SPREAD"

cdef extern from "ray/common/task/scheduling_resources.h" nogil:
    cdef cppclass ResourceSet "ray::ResourceSet":
        ResourceSet()
        ResourceSet(const unordered_map[c_string, double] &resource_map)
        ResourceSet(const c_vector[c_string] &resource_labels,
                    const c_vector[double] resource_capacity)
        c_bool operator==(const ResourceSet &rhs) const
        c_bool IsEqual(const ResourceSet &other) const
        c_bool IsSubset(const ResourceSet &other) const
        c_bool IsSuperset(const ResourceSet &other) const
        c_bool AddOrUpdateResource(const c_string &resource_name,
                                   double capacity)
        c_bool RemoveResource(const c_string &resource_name)
        void AddResources(const ResourceSet &other)
        c_bool SubtractResourcesStrict(const ResourceSet &other)
        c_bool GetResource(const c_string &resource_name, double *value) const
        double GetNumCpus() const
        c_bool IsEmpty() const
        const unordered_map[c_string, double] &GetResourceMap() const
        const c_string ToString() const

cdef extern from "ray/common/buffer.h" namespace "ray" nogil:
    cdef cppclass CBuffer "ray::Buffer":
        uint8_t *Data() const
        size_t Size() const

    cdef cppclass LocalMemoryBuffer(CBuffer):
        LocalMemoryBuffer(uint8_t *data, size_t size, c_bool copy_data)
        LocalMemoryBuffer(size_t size)

cdef extern from "ray/common/ray_object.h" nogil:
    cdef cppclass CRayObject "ray::RayObject":
        CRayObject(const shared_ptr[CBuffer] &data,
                   const shared_ptr[CBuffer] &metadata,
                   const c_vector[CObjectID] &nested_ids)
        c_bool HasData() const
        c_bool HasMetadata() const
        const size_t DataSize() const
        const shared_ptr[CBuffer] &GetData()
        const shared_ptr[CBuffer] &GetMetadata() const
        c_bool IsInPlasmaError() const

cdef extern from "ray/core_worker/common.h" nogil:
    cdef cppclass CRayFunction "ray::RayFunction":
        CRayFunction()
        CRayFunction(CLanguage language,
                     const CFunctionDescriptor &function_descriptor)
        CLanguage GetLanguage()
        const CFunctionDescriptor GetFunctionDescriptor()

    cdef cppclass CTaskArg "ray::TaskArg":
        pass

    cdef cppclass CTaskArgByReference "ray::TaskArgByReference":
        CTaskArgByReference(const CObjectID &object_id,
                            const CAddress &owner_address)

    cdef cppclass CTaskArgByValue "ray::TaskArgByValue":
        CTaskArgByValue(const shared_ptr[CRayObject] &data)

    cdef cppclass CTaskOptions "ray::TaskOptions":
        CTaskOptions()
        CTaskOptions(c_string name, int num_returns,
                     unordered_map[c_string, double] &resources)
        CTaskOptions(c_string name, int num_returns,
                     unordered_map[c_string, double] &resources,
                     const unordered_map[c_string, c_string]
                     &override_environment_variables)

    cdef cppclass CActorCreationOptions "ray::ActorCreationOptions":
        CActorCreationOptions()
        CActorCreationOptions(
            int64_t max_restarts,
            int64_t max_task_retries,
            int32_t max_concurrency,
            const unordered_map[c_string, double] &resources,
            const unordered_map[c_string, double] &placement_resources,
            const c_vector[c_string] &dynamic_worker_options,
            c_bool is_detached, c_string &name, c_bool is_asyncio,
            c_pair[CPlacementGroupID, int64_t] placement_options,
            c_bool placement_group_capture_child_tasks,
            const unordered_map[c_string, c_string]
            &override_environment_variables)

    cdef cppclass CPlacementGroupCreationOptions \
            "ray::PlacementGroupCreationOptions":
        CPlacementGroupCreationOptions()
        CPlacementGroupCreationOptions(
            const c_string &name,
            CPlacementStrategy strategy,
            const c_vector[unordered_map[c_string, double]] &bundles,
            c_bool is_detached
        )

cdef extern from "ray/gcs/gcs_client.h" nogil:
    cdef cppclass CGcsClientOptions "ray::gcs::GcsClientOptions":
        CGcsClientOptions(const c_string &ip, int port,
                          const c_string &password)

cdef extern from "src/ray/protobuf/gcs.pb.h" nogil:
    cdef cppclass CJobConfig "ray::rpc::JobConfig":
        const c_string &SerializeAsString()
