from libc.stdint cimport uint8_t, uint64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CLanguage,
    ResourceSet,
)
from ray.includes.unique_ids cimport (
    CActorHandleID,
    CActorID,
    CJobID,
    CObjectID,
    CTaskID,
)

cdef extern from "ray/protobuf/common.pb.h" namespace "ray::rpc" nogil:
    cdef cppclass RpcTaskSpec "ray::rpc::TaskSpec":
        void CopyFrom(const RpcTaskSpec &value)

    cdef cppclass RpcTaskExecutionSpec "ray::rpc::TaskExecutionSpec":
        void CopyFrom(const RpcTaskExecutionSpec &value)
        void add_dependencies(const c_string &value)

    cdef cppclass RpcTask "ray::rpc::Task":
        RpcTaskSpec *mutable_task_spec()


cdef extern from "ray/protobuf/gcs.pb.h" namespace "ray::rpc" nogil:
    cdef cppclass TaskTableData "ray::rpc::TaskTableData":
        RpcTask *mutable_task()
        const c_string &SerializeAsString()


cdef extern from "ray/common/task/task_spec.h" namespace "ray" nogil:
    cdef cppclass CTaskSpec "ray::TaskSpecification":
        CTaskSpec(const RpcTaskSpec message)
        CTaskSpec(const c_string &serialized_binary)
        const RpcTaskSpec &GetMessage()
        c_string Serialize() const

        CTaskID TaskId() const
        CJobID JobId() const
        CTaskID ParentTaskId() const
        uint64_t ParentCounter() const
        c_vector[c_string] FunctionDescriptor() const
        c_string FunctionDescriptorString() const
        uint64_t NumArgs() const
        uint64_t NumReturns() const
        c_bool ArgByRef(uint64_t arg_index) const
        int ArgIdCount(uint64_t arg_index) const
        CObjectID ArgId(uint64_t arg_index, uint64_t id_index) const
        CObjectID ReturnId(uint64_t return_index) const
        const uint8_t *ArgVal(uint64_t arg_index) const
        size_t ArgValLength(uint64_t arg_index) const
        double GetRequiredResource(const c_string &resource_name) const
        const ResourceSet GetRequiredResources() const
        const ResourceSet GetRequiredPlacementResources() const
        c_bool IsDriverTask() const
        CLanguage GetLanguage() const
        c_bool IsNormalTask() const
        c_bool IsActorCreationTask() const
        c_bool IsActorTask() const
        CActorID ActorCreationId() const
        CObjectID ActorCreationDummyObjectId() const
        CObjectID PreviousActorTaskDummyObjectId() const
        uint64_t MaxActorReconstructions() const
        CActorID ActorId() const
        CActorHandleID ActorHandleId() const
        uint64_t ActorCounter() const
        CObjectID ActorDummyObject() const
        c_vector[CActorHandleID] NewActorHandles() const


cdef extern from "ray/common/task/task_util.h" namespace "ray" nogil:
    cdef cppclass TaskSpecBuilder "ray::TaskSpecBuilder":
        TaskSpecBuilder &SetCommonTaskSpec(
            const CTaskID &task_id, const CLanguage &language,
            const c_vector[c_string] &function_descriptor, const CJobID &job_id,
            const CTaskID &parent_task_id, uint64_t parent_counter,
            uint64_t num_returns, const unordered_map[c_string, double] &required_resources,
            const unordered_map[c_string, double] &required_placement_resources)

        TaskSpecBuilder &AddByRefArg(const CObjectID &arg_id)

        TaskSpecBuilder &AddByValueArg(const c_string &data)

        TaskSpecBuilder &SetActorCreationTaskSpec(
            const CActorID &actor_id, uint64_t max_reconstructions,
            const c_vector[c_string] &dynamic_worker_options)

        TaskSpecBuilder &SetActorTaskSpec(
            const CActorID &actor_id, const CActorHandleID &actor_handle_id,
            const CObjectID &actor_creation_dummy_object_id,
            const CObjectID &previous_actor_task_dummy_object_id,
            uint64_t actor_counter,
            const c_vector[CActorHandleID] &new_handle_ids);

        RpcTaskSpec GetMessage()


cdef extern from "ray/common/task/task_execution_spec.h" namespace "ray" nogil:
    cdef cppclass CTaskExecutionSpec "ray::TaskExecutionSpecification":
        CTaskExecutionSpec(RpcTaskExecutionSpec message)
        CTaskExecutionSpec(const c_string &serialized_binary)
        const RpcTaskExecutionSpec &GetMessage()
        c_vector[CObjectID] ExecutionDependencies()
        uint64_t NumForwards()

cdef extern from "ray/common/task/task.h" namespace "ray" nogil:
    cdef cppclass CTask "ray::Task":
        CTask(CTaskSpec task_spec, CTaskExecutionSpec task_execution_spec)
