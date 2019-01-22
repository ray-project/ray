from ray.includes.common cimport *

cdef extern from "ray/raylet/format/node_manager_generated.h" namespace "ray::protocol" nogil:
    cdef cppclass TaskExecutionSpecificationT:
        c_vector[c_string] dependencies
        double last_timestamp
        int32_t num_forwards
        TaskExecutionSpecificationT()

    cdef cppclass TaskExecutionSpecification:
        double last_timestamp() const
        c_bool mutate_last_timestamp(double _last_timestamp)
        int32_t num_forwards() const
        c_bool mutate_num_forwards(int32_t _num_forwards)

    cdef cppclass _Task "ray::protocol::Task":
        pass


cdef extern from "ray/raylet/task_execution_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTaskExecutionSpecification "ray::raylet::TaskExecutionSpecification":
        RayletTaskExecutionSpecification(const TaskExecutionSpecificationT &execution_spec)
        RayletTaskExecutionSpecification(const c_vector[CObjectID] &&dependencies)
        RayletTaskExecutionSpecification(const c_vector[CObjectID] &&dependencies, int num_forwards)
        RayletTaskExecutionSpecification(const TaskExecutionSpecification &spec_flatbuffer)
        c_vector[CObjectID] ExecutionDependencies() const
        void SetExecutionDependencies(const c_vector[CObjectID] &dependencies)
        int NumForwards() const
        void IncrementNumForwards()
        int64_t LastTimestamp() const
        void SetLastTimestamp(int64_t new_timestamp)


cdef extern from "ray/raylet/task_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTaskArgument "ray::raylet::TaskArgument":
        pass

    cdef cppclass RayletTaskArgumentByReference "ray::raylet::TaskArgumentByReference":
        RayletTaskArgumentByReference(const c_vector[CObjectID] &references);

    cdef cppclass RayletTaskArgumentByValue "ray::raylet::TaskArgumentByValue":
        RayletTaskArgumentByValue(const uint8_t *value, size_t length);

    cdef cppclass RayletTaskSpecification "ray::raylet::TaskSpecification":
        RayletTaskSpecification(const CDriverID &driver_id, const CTaskID &parent_task_id,
                                int64_t parent_counter,
                                const c_vector[shared_ptr[RayletTaskArgument]] &task_arguments,
                                int64_t num_returns,
                                const unordered_map[c_string, double] &required_resources,
                                const CLanguage &language,
                                const c_vector[c_string] &function_descriptor)
        RayletTaskSpecification(
            const CDriverID &driver_id, const CTaskID &parent_task_id, int64_t parent_counter,
            const CActorID &actor_creation_id, const CObjectID &actor_creation_dummy_object_id,
            int64_t max_actor_reconstructions, const CActorID &actor_id,
            const CActorHandleID &actor_handle_id, int64_t actor_counter,
            const c_vector[CActorHandleID] &new_actor_handles,
            const c_vector[shared_ptr[RayletTaskArgument]] &task_arguments,
            int64_t num_returns,
            const unordered_map[c_string, double] &required_resources,
            const unordered_map[c_string, double] &required_placement_resources,
            const CLanguage &language, const c_vector[c_string] &function_descriptor)
        RayletTaskSpecification(const c_string &string)
        c_string ToFlatbuffer() const

        CTaskID TaskId() const
        CDriverID DriverId() const
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


cdef extern from "ray/raylet/task.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTask "ray::raylet::Task":
        RayletTask(const RayletTaskExecutionSpecification &execution_spec,
                   const RayletTaskSpecification &task_spec)
        const RayletTaskExecutionSpecification &GetTaskExecutionSpec() const
        const RayletTaskSpecification &GetTaskSpecification() const
        void SetExecutionDependencies(const c_vector[CObjectID] &dependencies)
        void IncrementNumForwards()
        const c_vector[CObjectID] &GetDependencies() const
        void CopyTaskExecutionSpec(const RayletTask &task)

    cdef c_string TaskToFlatbuffer(const c_vector[CObjectID] *dependencies,
        const RayletTaskSpecification *task_spec)
