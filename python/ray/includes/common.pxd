from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t, uint32_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair
from ray.includes.optional cimport (
    optional,
)
from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CClusterID,
    CWorkerID,
    CObjectID,
    CTaskID,
    CPlacementGroupID,
    CNodeID,
)
from ray.includes.function_descriptor cimport (
    CFunctionDescriptor,
)


cdef extern from * namespace "polyfill" nogil:
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
        CRayStatus InvalidArgument(const c_string &msg)

        @staticmethod
        CRayStatus Interrupted(const c_string &msg)

        @staticmethod
        CRayStatus IntentionalSystemExit(const c_string &msg)

        @staticmethod
        CRayStatus UnexpectedSystemExit(const c_string &msg)

        @staticmethod
        CRayStatus CreationTaskError(const c_string &msg)

        @staticmethod
        CRayStatus NotFound()

        @staticmethod
        CRayStatus ObjectRefEndOfStream()

        c_bool ok()
        c_bool IsOutOfMemory()
        c_bool IsKeyError()
        c_bool IsInvalid()
        c_bool IsIOError()
        c_bool IsTypeError()
        c_bool IsUnknownError()
        c_bool IsNotImplemented()
        c_bool IsObjectStoreFull()
        c_bool IsOutOfDisk()
        c_bool IsRedisError()
        c_bool IsTimedOut()
        c_bool IsInvalidArgument()
        c_bool IsInterrupted()
        c_bool ShouldExitWorker()
        c_bool IsObjectNotFound()
        c_bool IsNotFound()
        c_bool IsObjectUnknownOwner()
        c_bool IsRpcError()
        c_bool IsOutOfResource()
        c_bool IsObjectRefEndOfStream()

        c_string ToString()
        c_string CodeAsString()
        StatusCode code()
        c_string message()
        int rpc_code()

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
    cdef cppclass CWorkerType "ray::core::WorkerType":
        pass
    cdef cppclass CWorkerExitType "ray::rpc::WorkerExitType":
        pass
    cdef cppclass CTaskType "ray::TaskType":
        pass
    cdef cppclass CPlacementStrategy "ray::core::PlacementStrategy":
        pass
    cdef cppclass CDefaultSchedulingStrategy "ray::rpc::DefaultSchedulingStrategy":  # noqa: E501
        CDefaultSchedulingStrategy()
    cdef cppclass CSpreadSchedulingStrategy "ray::rpc::SpreadSchedulingStrategy":  # noqa: E501
        CSpreadSchedulingStrategy()
    cdef cppclass CPlacementGroupSchedulingStrategy "ray::rpc::PlacementGroupSchedulingStrategy":  # noqa: E501
        CPlacementGroupSchedulingStrategy()
        void set_placement_group_id(const c_string& placement_group_id)
        void set_placement_group_bundle_index(int64_t placement_group_bundle_index)  # noqa: E501
        void set_placement_group_capture_child_tasks(c_bool placement_group_capture_child_tasks)  # noqa: E501
    cdef cppclass CNodeAffinitySchedulingStrategy "ray::rpc::NodeAffinitySchedulingStrategy":  # noqa: E501
        CNodeAffinitySchedulingStrategy()
        void set_node_id(const c_string& node_id)
        void set_soft(c_bool soft)
        void set_spill_on_unavailable(c_bool spill_on_unavailable)
        void set_fail_on_unavailable(c_bool fail_on_unavailable)
    cdef cppclass CSchedulingStrategy "ray::rpc::SchedulingStrategy":
        CSchedulingStrategy()
        void clear_scheduling_strategy()
        CSpreadSchedulingStrategy* mutable_spread_scheduling_strategy()
        CDefaultSchedulingStrategy* mutable_default_scheduling_strategy()
        CPlacementGroupSchedulingStrategy* mutable_placement_group_scheduling_strategy()  # noqa: E501
        CNodeAffinitySchedulingStrategy* mutable_node_affinity_scheduling_strategy()
        CNodeLabelSchedulingStrategy* mutable_node_label_scheduling_strategy()
    cdef cppclass CAddress "ray::rpc::Address":
        CAddress()
        const c_string &SerializeAsString() const
        void ParseFromString(const c_string &serialized)
        void CopyFrom(const CAddress& address)
        const c_string &worker_id()
    cdef cppclass CObjectReference "ray::rpc::ObjectReference":
        CObjectReference()
        CAddress owner_address() const
        const c_string &object_id() const
        const c_string &call_site() const
    cdef cppclass CNodeLabelSchedulingStrategy "ray::rpc::NodeLabelSchedulingStrategy":  # noqa: E501
        CNodeLabelSchedulingStrategy()
        CLabelMatchExpressions* mutable_hard()
        CLabelMatchExpressions* mutable_soft()
    cdef cppclass CLabelMatchExpressions "ray::rpc::LabelMatchExpressions":  # noqa: E501
        CLabelMatchExpressions()
        CLabelMatchExpression* add_expressions()
    cdef cppclass CLabelMatchExpression "ray::rpc::LabelMatchExpression":  # noqa: E501
        CLabelMatchExpression()
        void set_key(const c_string &key)
        CLabelOperator* mutable_operator_()
    cdef cppclass CLabelIn "ray::rpc::LabelIn":  # noqa: E501
        CLabelIn()
        void add_values(const c_string &value)
    cdef cppclass CLabelNotIn "ray::rpc::LabelNotIn":  # noqa: E501
        CLabelNotIn()
        void add_values(const c_string &value)
    cdef cppclass CLabelExists "ray::rpc::LabelExists":  # noqa: E501
        CLabelExists()
    cdef cppclass CLabelDoesNotExist "ray::rpc::LabelDoesNotExist":  # noqa: E501
        CLabelDoesNotExist()
    cdef cppclass CLabelNotIn "ray::rpc::LabelNotIn":  # noqa: E501
        CLabelNotIn()
        void add_values(const c_string &value)
    cdef cppclass CLabelOperator "ray::rpc::LabelOperator":  # noqa: E501
        CLabelOperator()
        CLabelIn* mutable_label_in()
        CLabelNotIn* mutable_label_not_in()
        CLabelExists* mutable_label_exists()
        CLabelDoesNotExist* mutable_label_does_not_exist()


# This is a workaround for C++ enum class since Cython has no corresponding
# representation.
cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CLanguage LANGUAGE_PYTHON "Language::PYTHON"
    cdef CLanguage LANGUAGE_CPP "Language::CPP"
    cdef CLanguage LANGUAGE_JAVA "Language::JAVA"

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CWorkerType WORKER_TYPE_WORKER "ray::core::WorkerType::WORKER"
    cdef CWorkerType WORKER_TYPE_DRIVER "ray::core::WorkerType::DRIVER"
    cdef CWorkerType WORKER_TYPE_SPILL_WORKER "ray::core::WorkerType::SPILL_WORKER"  # noqa: E501
    cdef CWorkerType WORKER_TYPE_RESTORE_WORKER "ray::core::WorkerType::RESTORE_WORKER"  # noqa: E501
    cdef CWorkerType WORKER_TYPE_UTIL_WORKER "ray::core::WorkerType::UTIL_WORKER"  # noqa: E501
    cdef CWorkerExitType WORKER_EXIT_TYPE_USER_ERROR "ray::rpc::WorkerExitType::USER_ERROR"  # noqa: E501
    cdef CWorkerExitType WORKER_EXIT_TYPE_SYSTEM_ERROR "ray::rpc::WorkerExitType::SYSTEM_ERROR"  # noqa: E501

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CTaskType TASK_TYPE_NORMAL_TASK "ray::TaskType::NORMAL_TASK"
    cdef CTaskType TASK_TYPE_ACTOR_CREATION_TASK "ray::TaskType::ACTOR_CREATION_TASK"  # noqa: E501
    cdef CTaskType TASK_TYPE_ACTOR_TASK "ray::TaskType::ACTOR_TASK"

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CPlacementStrategy PLACEMENT_STRATEGY_PACK \
        "ray::core::PlacementStrategy::PACK"
    cdef CPlacementStrategy PLACEMENT_STRATEGY_SPREAD \
        "ray::core::PlacementStrategy::SPREAD"
    cdef CPlacementStrategy PLACEMENT_STRATEGY_STRICT_PACK \
        "ray::core::PlacementStrategy::STRICT_PACK"
    cdef CPlacementStrategy PLACEMENT_STRATEGY_STRICT_SPREAD \
        "ray::core::PlacementStrategy::STRICT_SPREAD"

cdef extern from "ray/common/buffer.h" namespace "ray" nogil:
    cdef cppclass CBuffer "ray::Buffer":
        uint8_t *Data() const
        size_t Size() const
        c_bool IsPlasmaBuffer() const

    cdef cppclass LocalMemoryBuffer(CBuffer):
        LocalMemoryBuffer(uint8_t *data, size_t size, c_bool copy_data)
        LocalMemoryBuffer(size_t size)

    cdef cppclass SharedMemoryBuffer(CBuffer):
        SharedMemoryBuffer(
            const shared_ptr[CBuffer] &buffer,
            int64_t offset,
            int64_t size)
        c_bool IsPlasmaBuffer() const

cdef extern from "ray/common/ray_object.h" nogil:
    cdef cppclass CRayObject "ray::RayObject":
        CRayObject(const shared_ptr[CBuffer] &data,
                   const shared_ptr[CBuffer] &metadata,
                   const c_vector[CObjectReference] &nested_refs)
        c_bool HasData() const
        c_bool HasMetadata() const
        const size_t DataSize() const
        const shared_ptr[CBuffer] &GetData()
        const shared_ptr[CBuffer] &GetMetadata() const
        c_bool IsInPlasmaError() const

cdef extern from "ray/core_worker/common.h" nogil:
    cdef cppclass CRayFunction "ray::core::RayFunction":
        CRayFunction()
        CRayFunction(CLanguage language,
                     const CFunctionDescriptor &function_descriptor)
        CLanguage GetLanguage()
        const CFunctionDescriptor GetFunctionDescriptor()

    cdef cppclass CTaskArg "ray::TaskArg":
        pass

    cdef cppclass CTaskArgByReference "ray::TaskArgByReference":
        CTaskArgByReference(const CObjectID &object_id,
                            const CAddress &owner_address,
                            const c_string &call_site)

    cdef cppclass CTaskArgByValue "ray::TaskArgByValue":
        CTaskArgByValue(const shared_ptr[CRayObject] &data)

    cdef cppclass CTaskOptions "ray::core::TaskOptions":
        CTaskOptions()
        CTaskOptions(c_string name, int num_returns,
                     unordered_map[c_string, double] &resources,
                     c_string concurrency_group_name)
        CTaskOptions(c_string name, int num_returns,
                     unordered_map[c_string, double] &resources,
                     c_string concurrency_group_name,
                     c_string serialized_runtime_env)

    cdef cppclass CActorCreationOptions "ray::core::ActorCreationOptions":
        CActorCreationOptions()
        CActorCreationOptions(
            int64_t max_restarts,
            int64_t max_task_retries,
            int32_t max_concurrency,
            const unordered_map[c_string, double] &resources,
            const unordered_map[c_string, double] &placement_resources,
            const c_vector[c_string] &dynamic_worker_options,
            optional[c_bool] is_detached, c_string &name, c_string &ray_namespace,
            c_bool is_asyncio,
            const CSchedulingStrategy &scheduling_strategy,
            c_string serialized_runtime_env,
            const c_vector[CConcurrencyGroup] &concurrency_groups,
            c_bool execute_out_of_order,
            int32_t max_pending_calls)

    cdef cppclass CPlacementGroupCreationOptions \
            "ray::core::PlacementGroupCreationOptions":
        CPlacementGroupCreationOptions()
        CPlacementGroupCreationOptions(
            const c_string &name,
            CPlacementStrategy strategy,
            const c_vector[unordered_map[c_string, double]] &bundles,
            c_bool is_detached,
            double max_cpu_fraction_per_node
        )

    cdef cppclass CObjectLocation "ray::core::ObjectLocation":
        const CNodeID &GetPrimaryNodeID() const
        const uint64_t GetObjectSize() const
        const c_vector[CNodeID] &GetNodeIDs() const
        c_bool IsSpilled() const
        const c_string &GetSpilledURL() const
        const CNodeID &GetSpilledNodeID() const

cdef extern from "ray/gcs/gcs_client/gcs_client.h" nogil:
    cdef enum CGrpcStatusCode "grpc::StatusCode":
        UNAVAILABLE "grpc::StatusCode::UNAVAILABLE",
        UNKNOWN "grpc::StatusCode::UNKNOWN",
        DEADLINE_EXCEEDED "grpc::StatusCode::DEADLINE_EXCEEDED",
        RESOURCE_EXHAUSTED "grpc::StatusCode::RESOURCE_EXHAUSTED",
        UNIMPLEMENTED "grpc::StatusCode::UNIMPLEMENTED",

    cdef cppclass CGcsClientOptions "ray::gcs::GcsClientOptions":
        CGcsClientOptions(const c_string &gcs_address)

    cdef cppclass CPythonGcsClient "ray::gcs::PythonGcsClient":
        CPythonGcsClient(const CGcsClientOptions &options)

        CRayStatus Connect(
            const CClusterID &cluster_id,
            int64_t timeout_ms,
            size_t num_retries)
        CRayStatus CheckAlive(
            const c_vector[c_string] &raylet_addresses,
            int64_t timeout_ms,
            c_vector[c_bool] &result)
        CRayStatus InternalKVGet(
            const c_string &ns, const c_string &key,
            int64_t timeout_ms, c_string &value)
        CRayStatus InternalKVMultiGet(
            const c_string &ns, const c_vector[c_string] &keys,
            int64_t timeout_ms, unordered_map[c_string, c_string] &result)
        CRayStatus InternalKVPut(
            const c_string &ns, const c_string &key, const c_string &value,
            c_bool overwrite, int64_t timeout_ms, c_bool &added)
        CRayStatus InternalKVDel(
            const c_string &ns, const c_string &key, c_bool del_by_prefix,
            int64_t timeout_ms, int &deleted_num)
        CRayStatus InternalKVKeys(
            const c_string &ns, const c_string &prefix,
            int64_t timeout_ms, c_vector[c_string] &value)
        CRayStatus InternalKVExists(
            const c_string &ns, const c_string &key,
            int64_t timeout_ms, c_bool &exists)
        CRayStatus PinRuntimeEnvUri(
            const c_string &uri, int expiration_s, int64_t timeout_ms)
        CRayStatus GetAllNodeInfo(
            int64_t timeout_ms, c_vector[CGcsNodeInfo]& result)
        CRayStatus GetAllJobInfo(
            int64_t timeout_ms, c_vector[CJobTableData]& result)
        CRayStatus GetAllResourceUsage(
            int64_t timeout_ms, c_string& serialized_reply)
        CRayStatus RequestClusterResourceConstraint(
            int64_t timeout_ms,
            const c_vector[unordered_map[c_string, double]] &bundles,
            const c_vector[int64_t] &count_array)
        CRayStatus GetClusterStatus(
            int64_t timeout_ms,
            c_string &serialized_reply)
        CClusterID GetClusterId()
        CRayStatus DrainNode(
            const c_string &node_id,
            int32_t reason,
            const c_string &reason_message,
            int64_t timeout_ms,
            c_bool &is_accepted)
        CRayStatus DrainNodes(
            const c_vector[c_string]& node_ids,
            int64_t timeout_ms,
            c_vector[c_string]& drained_node_ids)

cdef extern from "ray/gcs/gcs_client/gcs_client.h" namespace "ray::gcs" nogil:
    unordered_map[c_string, double] PythonGetResourcesTotal(
        const CGcsNodeInfo& node_info)

cdef extern from "ray/gcs/pubsub/gcs_pub_sub.h" nogil:

    cdef cppclass CPythonGcsPublisher "ray::gcs::PythonGcsPublisher":

        CPythonGcsPublisher(const c_string& gcs_address)

        CRayStatus Connect()

        CRayStatus PublishError(
            const c_string &key_id, const CErrorTableData &data, int64_t num_retries)

        CRayStatus PublishLogs(const c_string &key_id, const CLogBatch &data)

        CRayStatus PublishFunctionKey(const CPythonFunction& python_function)

    cdef cppclass CPythonGcsSubscriber "ray::gcs::PythonGcsSubscriber":

        CPythonGcsSubscriber(
            const c_string& gcs_address, int gcs_port, CChannelType channel_type,
            const c_string& subscriber_id, const c_string& worker_id)

        CRayStatus Subscribe()

        int64_t last_batch_size()

        CRayStatus PollError(
            c_string* key_id, int64_t timeout_ms, CErrorTableData* data)

        CRayStatus PollLogs(
            c_string* key_id, int64_t timeout_ms, CLogBatch* data)

        CRayStatus PollActor(
            c_string* key_id, int64_t timeout_ms, CActorTableData* data)

        CRayStatus Close()

cdef extern from "ray/gcs/pubsub/gcs_pub_sub.h" namespace "ray::gcs" nogil:
    c_vector[c_string] PythonGetLogBatchLines(const CLogBatch& log_batch)

cdef extern from "ray/gcs/gcs_client/gcs_client.h" namespace "ray::gcs" nogil:
    unordered_map[c_string, c_string] PythonGetNodeLabels(
        const CGcsNodeInfo& node_info)

    CRayStatus PythonCheckGcsHealth(
        const c_string& gcs_address, const int gcs_port, const int64_t timeout_ms,
        const c_string& ray_version, const c_bool skip_version_check,
        c_bool& is_healthy)

cdef extern from "src/ray/protobuf/gcs.pb.h" nogil:
    cdef enum CChannelType "ray::rpc::ChannelType":
        RAY_ERROR_INFO_CHANNEL "ray::rpc::ChannelType::RAY_ERROR_INFO_CHANNEL",
        RAY_LOG_CHANNEL "ray::rpc::ChannelType::RAY_LOG_CHANNEL",
        RAY_PYTHON_FUNCTION_CHANNEL \
            "ray::rpc::ChannelType::RAY_PYTHON_FUNCTION_CHANNEL",
        GCS_ACTOR_CHANNEL "ray::rpc::ChannelType::GCS_ACTOR_CHANNEL",

    cdef cppclass CJobConfig "ray::rpc::JobConfig":
        c_string ray_namespace() const
        const c_string &SerializeAsString()

    cdef cppclass CGcsNodeInfo "ray::rpc::GcsNodeInfo":
        c_string node_id() const
        c_string node_name() const
        int state() const
        c_string node_manager_address() const
        c_string node_manager_hostname() const
        int node_manager_port() const
        int object_manager_port() const
        c_string object_store_socket_name() const
        c_string raylet_socket_name() const
        int metrics_export_port() const
        int runtime_env_agent_port() const
        void ParseFromString(const c_string &serialized)

    cdef enum CGcsNodeState "ray::rpc::GcsNodeInfo_GcsNodeState":
        ALIVE "ray::rpc::GcsNodeInfo_GcsNodeState_ALIVE",

    cdef cppclass CJobTableData "ray::rpc::JobTableData":
        c_string job_id() const
        c_bool is_dead() const
        CJobConfig config() const
        const c_string &SerializeAsString()

    cdef cppclass CPythonFunction "ray::rpc::PythonFunction":
        void set_key(const c_string &key)
        c_string key() const

    cdef cppclass CErrorTableData "ray::rpc::ErrorTableData":
        c_string job_id() const
        c_string type() const
        c_string error_message() const
        double timestamp() const

        void set_job_id(const c_string &job_id)
        void set_type(const c_string &type)
        void set_error_message(const c_string &error_message)
        void set_timestamp(double timestamp)

    cdef cppclass CLogBatch "ray::rpc::LogBatch":
        c_string ip() const
        c_string pid() const
        c_string job_id() const
        c_bool is_error() const
        c_string actor_name() const
        c_string task_name() const

        void set_ip(const c_string &ip)
        void set_pid(const c_string &pid)
        void set_job_id(const c_string &job_id)
        void set_is_error(c_bool is_error)
        void add_lines(const c_string &line)
        void set_actor_name(const c_string &actor_name)
        void set_task_name(const c_string &task_name)

    cdef cppclass CActorTableData "ray::rpc::ActorTableData":
        CAddress address() const
        void ParseFromString(const c_string &serialized)
        const c_string &SerializeAsString()

cdef extern from "ray/common/task/task_spec.h" nogil:
    cdef cppclass CConcurrencyGroup "ray::ConcurrencyGroup":
        CConcurrencyGroup(
            const c_string &name,
            uint32_t max_concurrency,
            const c_vector[CFunctionDescriptor] &c_fds)
        CConcurrencyGroup()
        c_string GetName() const
        uint32_t GetMaxConcurrency() const
        c_vector[CFunctionDescriptor] GetFunctionDescriptors() const

cdef extern from "ray/common/constants.h" nogil:
    cdef const char[] kWorkerSetupHookKeyName
    cdef int kResourceUnitScaling
    cdef const char[] kImplicitResourcePrefix
    cdef int kStreamingGeneratorReturn
