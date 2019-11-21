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
    CActorID,
    CJobID,
    CObjectID,
    CTaskID,
)

cdef extern from "ray/protobuf/common.pb.h" nogil:
    cdef cppclass RpcTaskSpec "ray::rpc::TaskSpec":
        void CopyFrom(const RpcTaskSpec &value)

    cdef cppclass RpcTaskExecutionSpec "ray::rpc::TaskExecutionSpec":
        void CopyFrom(const RpcTaskExecutionSpec &value)
        void add_dependencies(const c_string &value)

    cdef cppclass RpcTask "ray::rpc::Task":
        RpcTaskSpec *mutable_task_spec()


cdef extern from "ray/protobuf/gcs.pb.h" nogil:
    cdef cppclass TaskTableData "ray::rpc::TaskTableData":
        RpcTask *mutable_task()
        const c_string &SerializeAsString()


cdef extern from "ray/common/task/task_spec.h" nogil:
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
        CObjectID ReturnIdForPlasma(uint64_t return_index) const
        const uint8_t *ArgData(uint64_t arg_index) const
        size_t ArgDataSize(uint64_t arg_index) const
        const uint8_t *ArgMetadata(uint64_t arg_index) const
        size_t ArgMetadataSize(uint64_t arg_index) const
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
        uint64_t ActorCounter() const
        CObjectID ActorDummyObject() const


cdef extern from "ray/common/task/task_execution_spec.h" nogil:
    cdef cppclass CTaskExecutionSpec "ray::TaskExecutionSpecification":
        CTaskExecutionSpec(RpcTaskExecutionSpec message)
        CTaskExecutionSpec(const c_string &serialized_binary)
        const RpcTaskExecutionSpec &GetMessage()
        c_vector[CObjectID] ExecutionDependencies()
        uint64_t NumForwards()

cdef extern from "ray/common/task/task.h" nogil:
    cdef cppclass CTask "ray::Task":
        CTask(CTaskSpec task_spec, CTaskExecutionSpec task_execution_spec)
