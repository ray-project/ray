from libc.stdint cimport int64_t, uint8_t
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
    CActorHandleId,
    CActorId,
    CDriverId,
    CObjectId,
    CTaskId,
)


cdef extern from "ray/raylet/task_execution_spec.h" \
        namespace "ray::raylet" nogil:
    cdef cppclass CTaskExecutionSpecification \
            "ray::raylet::TaskExecutionSpecification":
        CTaskExecutionSpecification(const c_vector[CObjectId] &&dependencies)
        CTaskExecutionSpecification(
            const c_vector[CObjectId] &&dependencies, int num_forwards)
        c_vector[CObjectId] ExecutionDependencies() const
        void SetExecutionDependencies(const c_vector[CObjectId] &dependencies)
        int NumForwards() const
        void IncrementNumForwards()
        int64_t LastTimestamp() const
        void SetLastTimestamp(int64_t new_timestamp)


cdef extern from "ray/raylet/task_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass CTaskArgument "ray::raylet::TaskArgument":
        pass

    cdef cppclass CTaskArgumentByReference \
            "ray::raylet::TaskArgumentByReference":
        CTaskArgumentByReference(const c_vector[CObjectId] &references)

    cdef cppclass CTaskArgumentByValue "ray::raylet::TaskArgumentByValue":
        CTaskArgumentByValue(const uint8_t *value, size_t length)

    cdef cppclass CTaskSpecification "ray::raylet::TaskSpecification":
        CTaskSpecification(
            const CDriverId &driver_id, const CTaskId &parent_task_id,
            int64_t parent_counter,
            const c_vector[shared_ptr[CTaskArgument]] &task_arguments,
            int64_t num_returns,
            const unordered_map[c_string, double] &required_resources,
            const CLanguage &language,
            const c_vector[c_string] &function_descriptor)
        CTaskSpecification(
            const CDriverId &driver_id, const CTaskId &parent_task_id,
            int64_t parent_counter, const CActorId &actor_creation_id,
            const CObjectId &actor_creation_dummy_object_id,
            int64_t max_actor_reconstructions, const CActorId &actor_id,
            const CActorHandleId &actor_handle_id, int64_t actor_counter,
            const c_vector[CActorHandleId] &new_actor_handles,
            const c_vector[shared_ptr[CTaskArgument]] &task_arguments,
            int64_t num_returns,
            const unordered_map[c_string, double] &required_resources,
            const unordered_map[c_string, double] &required_placement_res,
            const CLanguage &language,
            const c_vector[c_string] &function_descriptor)
        CTaskSpecification(const c_string &string)
        c_string SerializeAsString() const

        CTaskId GetTaskId() const
        CDriverId GetDriverId() const
        CTaskId ParentTaskId() const
        int64_t ParentCounter() const
        c_vector[c_string] FunctionDescriptor() const
        c_string FunctionDescriptorString() const
        int64_t NumArgs() const
        int64_t NumReturns() const
        c_bool ArgByRef(int64_t arg_index) const
        int ArgIdCount(int64_t arg_index) const
        CObjectId ArgId(int64_t arg_index, int64_t id_index) const
        CObjectId ReturnId(int64_t return_index) const
        const uint8_t *ArgVal(int64_t arg_index) const
        size_t ArgValLength(int64_t arg_index) const
        double GetRequiredResource(const c_string &resource_name) const
        const ResourceSet GetRequiredResources() const
        const ResourceSet GetRequiredPlacementResources() const
        c_bool IsDriverTask() const
        CLanguage GetLanguage() const

        c_bool IsActorCreationTask() const
        c_bool IsActorTask() const
        CActorId ActorCreationId() const
        CObjectId ActorCreationDummyObjectId() const
        int64_t MaxActorReconstructions() const
        CActorId GetActorId() const
        CActorHandleId GetActorHandleId() const
        int64_t ActorCounter() const
        CObjectId ActorDummyObject() const
        c_vector[CActorHandleId] NewActorHandles() const


cdef extern from "ray/raylet/task.h" namespace "ray::raylet" nogil:
    cdef cppclass CTask "ray::raylet::Task":
        CTask(const CTaskExecutionSpecification &execution_spec,
              const CTaskSpecification &task_spec)
        const CTaskExecutionSpecification &GetTaskExecutionSpec() const
        const CTaskSpecification &GetTaskSpecification() const
        void SetExecutionDependencies(const c_vector[CObjectId] &dependencies)
        void IncrementNumForwards()
        const c_vector[CObjectId] &GetDependencies() const
        void CopyTaskExecutionSpec(const CTask &task)

    cdef c_string SerializeTaskAsString(
        const c_vector[CObjectId] *dependencies,
        const CTaskSpecification *task_spec)
