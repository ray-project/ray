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


cdef extern from "ray/raylet/task_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass CTaskSpecification "ray::raylet::TaskSpecification":
        CTaskSpecification(const c_string &serialized_binary)
        c_string Serialize() const

        CTaskID TaskId() const
        CJobID JobId() const
        CTaskID ParentTaskId() const
        int64_t ParentCounter() const
        c_vector[c_string] FunctionDescriptor() const
        c_string FunctionDescriptorString() const
        int64_t NumArgs() const
        int64_t NumReturns() const
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
        int64_t MaxActorReconstructions() const
        CActorID ActorId() const
        CActorHandleID ActorHandleId() const
        int64_t ActorCounter() const
        CObjectID ActorDummyObject() const
        c_vector[CActorHandleID] NewActorHandles() const

    cdef CTaskSpecification *CreateTaskSpecification(
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

cdef extern from "ray/raylet/task.h" namespace "ray::raylet" nogil:
    cdef c_string SerializeTaskAsString(
        const c_vector[CObjectID] *dependencies,
        const CTaskSpecification *task_spec)
