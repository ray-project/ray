from ray.includes.common cimport *
from libc.stdint cimport int32_t, int64_t, uint8_t
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector


cdef extern from "ray/raylet/format/node_manager_generated.h" namespace "ray::protocol" nogil:
    # Note: It is a C++ struct in the header file.
    cdef cppclass TaskExecutionSpecificationT:
        c_vector[c_string] dependencies
        double last_timestamp
        int32_t num_forwards
        TaskExecutionSpecificationT()

    # Note: It is a C++ struct in the header file.
    cdef cppclass TaskExecutionSpecification:
        double last_timestamp() const
        c_bool mutate_last_timestamp(double _last_timestamp)
        int32_t num_forwards() const
        c_bool mutate_num_forwards(int32_t _num_forwards)

    # Note: It is a C++ struct in the header file.
    cdef cppclass _Task "ray::protocol::Task":
        pass


cdef extern from "ray/raylet/task_execution_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTaskExecutionSpecification "ray::raylet::TaskExecutionSpecification":
        RayletTaskExecutionSpecification(const TaskExecutionSpecificationT &execution_spec)
        RayletTaskExecutionSpecification(const c_vector[ObjectID] &&dependencies)
        RayletTaskExecutionSpecification(const c_vector[ObjectID] &&dependencies, int num_forwards)
        RayletTaskExecutionSpecification(const TaskExecutionSpecification &spec_flatbuffer)
        Offset[TaskExecutionSpecification] ToFlatbuffer(FlatBufferBuilder &fbb) const
        c_vector[ObjectID] ExecutionDependencies() const
        void SetExecutionDependencies(const c_vector[ObjectID] &dependencies)
        int NumForwards() const
        void IncrementNumForwards()
        int64_t LastTimestamp() const
        void SetLastTimestamp(int64_t new_timestamp)


cdef extern from "ray/raylet/task_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTaskArgument "TaskArgument":
        Offset[GCSArg] ToFlatbuffer(FlatBufferBuilder &fbb) const

    cdef cppclass RayletTaskSpecification "ray::raylet::TaskSpecification":
        RayletTaskSpecification(const FlatBufferString &string)
        RayletTaskSpecification(const UniqueID &driver_id, const TaskID &parent_task_id,
                                int64_t parent_counter,
                                const c_vector[shared_ptr[RayletTaskArgument]] &task_arguments,
                                int64_t num_returns,
                                const unordered_map[c_string, double] &required_resources,
                                const CLanguage &language,
                                const c_vector[c_string] &function_descriptor)
        RayletTaskSpecification(
            const UniqueID &driver_id, const TaskID &parent_task_id, int64_t parent_counter,
            const ActorID &actor_creation_id, const ObjectID &actor_creation_dummy_object_id,
            int64_t max_actor_reconstructions, const ActorID &actor_id,
            const ActorHandleID &actor_handle_id, int64_t actor_counter,
            const c_vector[ActorHandleID] &new_actor_handles,
            const c_vector[shared_ptr[RayletTaskArgument]] &task_arguments,
            int64_t num_returns,
            const unordered_map[c_string, double] &required_resources,
            const unordered_map[c_string, double] &required_placement_resources,
            const CLanguage &language, const c_vector[c_string] &function_descriptor)
        RayletTaskSpecification(const c_string &string)
        Offset[FlatBufferString] ToFlatbuffer(FlatBufferBuilder &fbb) const

        TaskID TaskId() const
        UniqueID DriverId() const
        TaskID ParentTaskId() const
        int64_t ParentCounter() const
        c_vector[c_string] FunctionDescriptor() const
        c_string FunctionDescriptorString() const
        int64_t NumArgs() const
        int64_t NumReturns() const
        c_bool ArgByRef(int64_t arg_index) const
        int ArgIdCount(int64_t arg_index) const
        ObjectID ArgId(int64_t arg_index, int64_t id_index) const
        ObjectID ReturnId(int64_t return_index) const
        const uint8_t *ArgVal(int64_t arg_index) const
        size_t ArgValLength(int64_t arg_index) const
        double GetRequiredResource(const c_string &resource_name) const
        const ResourceSet GetRequiredResources() const
        const ResourceSet GetRequiredPlacementResources() const
        c_bool IsDriverTask() const
        CLanguage GetLanguage() const

        c_bool IsActorCreationTask() const
        c_bool IsActorTask() const
        ActorID ActorCreationId() const
        ObjectID ActorCreationDummyObjectId() const
        int64_t MaxActorReconstructions() const
        ActorID ActorId() const
        ActorHandleID ActorHandleId() const
        int64_t ActorCounter() const
        ObjectID ActorDummyObject() const
        c_vector[ActorHandleID] NewActorHandles() const


cdef extern from "ray/raylet/task.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTask "ray::raylet::Task":
        RayletTask(const RayletTaskExecutionSpecification &execution_spec,
                   const RayletTaskSpecification &task_spec)
        Offset[_Task] ToFlatbuffer(FlatBufferBuilder &fbb) const
        const RayletTaskExecutionSpecification &GetTaskExecutionSpec() const
        const RayletTaskSpecification &GetTaskSpecification() const
        void SetExecutionDependencies(const c_vector[ObjectID] &dependencies)
        void IncrementNumForwards()
        const c_vector[ObjectID] &GetDependencies() const
        void CopyTaskExecutionSpec(const RayletTask &task)
