from libc.stdint cimport int64_t, uint8_t, uint64_t
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
    cdef cppclass CTaskArg "ray::rpc::TaskArg":
        void add_object_ids(const c_string &value)
        void set_data(const c_string &value)

    cdef cppclass RpcTaskSpec "ray::rpc::TaskSpec":
        void CopyFrom(const RpcTaskSpec &value)

    cdef cppclass RpcTaskExecutionSpec "ray::rpc::TaskExecutionSpec":
        void CopyFrom(const RpcTaskExecutionSpec &value)
        void add_dependencies(const c_string &value)

    cdef cppclass RpcTask "ray::rpc::Task":
        RpcTaskSpec *mutable_task_spec()
        RpcTaskExecutionSpec *mutable_task_execution_spec()


cdef extern from "ray/protobuf/gcs.pb.h" namespace "ray::rpc" nogil:
    cdef cppclass TaskTableData "ray::rpc::TaskTableData":
        RpcTask *mutable_task()
        const c_string &SerializeAsString()

cdef extern from "ray/raylet/task_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass CTaskSpec "ray::raylet::TaskSpecification":
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
        c_bool ArgByRef(int64_t arg_index) const
        int ArgIdCount(int64_t arg_index) const
        CObjectID ArgId(int64_t arg_index, int64_t id_index) const
        CObjectID ReturnId(int64_t return_index) const
        const uint8_t *ArgVal(int64_t arg_index) const
        size_t ArgValLength(int64_t arg_index) const
        double GetRequiredResource(const c_string &resource_name) const
        const ResourceSet GetRequiredResources() const
        const ResourceSet GetRequiredPlacementResources() const
        c_bool IsDriverTask() const
        CLanguage GetLanguage() const

        c_bool IsActorCreationTask() const
        c_bool IsActorTask() const
        CActorID ActorCreationId() const
        CObjectID ActorCreationDummyObjectId() const
        uint64_t MaxActorReconstructions() const
        CActorID ActorId() const
        CActorHandleID ActorHandleId() const
        uint64_t ActorCounter() const
        CObjectID ActorDummyObject() const
        c_vector[CActorHandleID] NewActorHandles() const

    cdef CTaskSpec *CreateTaskSpec(
        const CJobID &job_id, const CTaskID &parent_task_id, uint64_t parent_counter,
        const CActorID &actor_creation_id, const CObjectID &actor_creation_dummy_object_id,
        uint64_t max_actor_reconstructions, const CActorID &actor_id,
        const CActorHandleID &actor_handle_id, uint64_t actor_counter,
        const c_vector[CActorHandleID] &new_actor_handles,
        const c_vector[shared_ptr[CTaskArg]] &task_arguments, int64_t num_returns,
        const unordered_map[c_string, double] &required_resources,
        const unordered_map[c_string, double] &required_placement_resources,
        const CLanguage &language, const c_vector[c_string] &function_descriptor,
        const c_vector[c_string] &dynamic_worker_options)


cdef extern from "ray/raylet/task_execution_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass CTaskExecutionSpec "ray::raylet::TaskExecutionSpecification":
        CTaskExecutionSpec(RpcTaskExecutionSpec message)
        CTaskExecutionSpec(const c_string &serialized_binary)
        const RpcTaskExecutionSpec &GetMessage()
        c_vector[CObjectID] ExecutionDependencies()
        uint64_t NumForwards()

cdef extern from "ray/raylet/task.h" namespace "ray::raylet" nogil:
    cdef cppclass CTask "ray::raylet::Task":
        CTask(CTaskSpec task_spec, CTaskExecutionSpec task_execution_spec)
