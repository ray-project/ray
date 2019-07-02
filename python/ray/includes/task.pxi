from libc.stdint cimport uint8_t
from libcpp.memory cimport (
    make_shared,
    shared_ptr,
    static_pointer_cast,
)
from ray.includes.task cimport (
    CTask,
    CTaskArg,
    CTaskExecutionSpec,
    CTaskSpecification,
    CreateTaskSpecification,
    RpcTask,
    RpcTaskExecutionSpec,
    TaskTableData,
)


cdef class TaskSpec:
    cdef:
        unique_ptr[CTaskSpecification] task_spec

    def __init__(self, JobID job_id, function_descriptor, arguments,
                 int num_returns, TaskID parent_task_id, int parent_counter,
                 ActorID actor_creation_id,
                 ObjectID actor_creation_dummy_object_id,
                 int32_t max_actor_reconstructions, ActorID actor_id,
                 ActorHandleID actor_handle_id, int actor_counter,
                 new_actor_handles, resource_map, placement_resource_map):
        cdef:
            unordered_map[c_string, double] required_resources
            unordered_map[c_string, double] required_placement_resources
            shared_ptr[CTaskArg] c_arg
            c_vector[shared_ptr[CTaskArg]] task_args
            c_vector[CActorHandleID] task_new_actor_handles
            c_vector[c_string] c_function_descriptor
            c_string pickled_str
            c_vector[CObjectID] references

        for item in function_descriptor:
            if not isinstance(item, bytes):
                raise TypeError(
                    "'function_descriptor' takes a list of byte strings.")
            c_function_descriptor.push_back(item)

        # Parse the resource map.
        if resource_map is not None:
            required_resources = resource_map_from_dict(resource_map)
        if placement_resource_map is not None:
            required_placement_resources = (
                resource_map_from_dict(placement_resource_map))

        # Parse the arguments from the list.
        for arg in arguments:
            c_arg = make_shared[CTaskArg]()
            if isinstance(arg, ObjectID):
                c_arg.get().add_object_ids((<ObjectID>arg).native().Binary())
            else:
                pickled_str = pickle.dumps(
                    arg, protocol=pickle.HIGHEST_PROTOCOL)
                c_arg.get().set_data(pickled_str)
            task_args.push_back(c_arg)

        for new_actor_handle in new_actor_handles:
            task_new_actor_handles.push_back(
                (<ActorHandleID?>new_actor_handle).native())

        self.task_spec.reset(CreateTaskSpecification(
            job_id.native(), parent_task_id.native(), parent_counter, actor_creation_id.native(),
            actor_creation_dummy_object_id.native(), max_actor_reconstructions, actor_id.native(),
            actor_handle_id.native(), actor_counter, task_new_actor_handles, task_args, num_returns,
            required_resources, required_placement_resources, LANGUAGE_PYTHON,
            c_function_descriptor, []))

    @staticmethod
    cdef make(unique_ptr[CTaskSpecification]& task_spec):
        cdef TaskSpec self = TaskSpec.__new__(TaskSpec)
        self.task_spec.reset(task_spec.release())
        return self

    @staticmethod
    def from_string(const c_string& task_spec_str):
        """Convert a string to a Ray task specification Python object.

        Args:
            task_spec_str: String representation of the task specification.

        Returns:
            Python task specification object.
        """
        cdef TaskSpec self = TaskSpec.__new__(TaskSpec)
        # TODO(pcm): Use flatbuffers validation here.
        self.task_spec.reset(new CTaskSpecification(task_spec_str))
        return self

    def to_string(self):
        """Convert a Ray task specification Python object to a string.

        Returns:
            String representing the task specification.
        """
        return self.task_spec.get().Serialize()

    def job_id(self):
        """Return the job ID for this task."""
        return JobID(self.task_spec.get().JobId().Binary())

    def task_id(self):
        """Return the task ID for this task."""
        return TaskID(self.task_spec.get().TaskId().Binary())

    def parent_task_id(self):
        """Return the task ID of the parent task."""
        return TaskID(self.task_spec.get().ParentTaskId().Binary())

    def parent_counter(self):
        """Return the parent counter of this task."""
        return self.task_spec.get().ParentCounter()

    def function_descriptor_list(self):
        """Return the function descriptor for this task."""
        cdef c_vector[c_string] function_descriptor = (
            self.task_spec.get().FunctionDescriptor())
        results = []
        for i in range(function_descriptor.size()):
            results.append(function_descriptor[i])
        return results

    def arguments(self):
        """Return the arguments for the task."""
        cdef:
            CTaskSpecification *task_spec = self.task_spec.get()
            int64_t num_args = task_spec.NumArgs()
            int32_t lang = <int32_t>task_spec.GetLanguage()
            int count
        arg_list = []

        if lang == <int32_t>LANGUAGE_PYTHON:
            for i in range(num_args):
                count = task_spec.ArgIdCount(i)
                if count > 0:
                    assert count == 1
                    arg_list.append(
                        ObjectID(task_spec.ArgId(i, 0).Binary()))
                else:
                    serialized_str = (
                        task_spec.ArgVal(i)[:task_spec.ArgValLength(i)])
                    obj = pickle.loads(serialized_str)
                    arg_list.append(obj)
        elif lang == <int32_t>LANGUAGE_JAVA:
            arg_list = num_args * ["<java-argument>"]

        return arg_list

    def returns(self):
        """Return the object IDs for the return values of the task."""
        cdef CTaskSpecification *task_spec = self.task_spec.get()
        return_id_list = []
        for i in range(task_spec.NumReturns()):
            return_id_list.append(ObjectID(task_spec.ReturnId(i).Binary()))
        return return_id_list

    def required_resources(self):
        """Return the resource dictionary of the task."""
        cdef:
            unordered_map[c_string, double] resource_map = (
                self.task_spec.get().GetRequiredResources().GetResourceMap())
            c_string resource_name
            double resource_value
            unordered_map[c_string, double].iterator iterator = (
                resource_map.begin())

        required_resources = {}
        while iterator != resource_map.end():
            resource_name = dereference(iterator).first
            # bytes for Py2, unicode for Py3
            py_resource_name = str(resource_name)
            resource_value = dereference(iterator).second
            required_resources[py_resource_name] = resource_value
            postincrement(iterator)
        return required_resources

    def language(self):
        """Return the language of the task."""
        return Language.from_native(self.task_spec.get().GetLanguage())

    def actor_creation_id(self):
        """Return the actor creation ID for the task."""
        return ActorID(self.task_spec.get().ActorCreationId().Binary())

    def actor_creation_dummy_object_id(self):
        """Return the actor creation dummy object ID for the task."""
        return ObjectID(
            self.task_spec.get().ActorCreationDummyObjectId().Binary())

    def actor_id(self):
        """Return the actor ID for this task."""
        return ActorID(self.task_spec.get().ActorId().Binary())

    def actor_counter(self):
        """Return the actor counter for this task."""
        return self.task_spec.get().ActorCounter()


cdef class TaskExecutionSpec:
    cdef:
        unique_ptr[CTaskExecutionSpec] c_spec

    def __init__(self, execution_dependencies):
        cdef:
            RpcTaskExecutionSpec *message = new RpcTaskExecutionSpec()

        for dependency in execution_dependencies:
            message.add_dependencies(
                (<ObjectID?>dependency).binary())
        self.c_spec.reset(new CTaskExecutionSpec(unique_ptr[RpcTaskExecutionSpec](message)))

    def dependencies(self):
        cdef:
            CObjectID c_id
            c_vector[CObjectID] dependencies = self.c_spec.get().ExecutionDependencies()
        ret = []
        for c_id in dependencies:
            ret.append(ObjectID.c_id.Binary())
        return ret

    def num_forwards(self):
        return self.c_spec.get().NumForwards()


cdef class Task:
    cdef:
        unique_ptr[CTask] c_task

    def __init__(self, TaskSpec task_spec, TaskExecutionSpec task_execution_spec):
        cdef:
            RpcTask *message = new RpcTask()
        message.mutable_task_spec().CopyFrom(task_spec.task_spec.get().GetMessage())
        message.mutable_task_execution_spec().CopyFrom(task_execution_spec.c_spec.get().GetMessage())
        self.c_task.reset(new CTask(unique_ptr[RpcTask](message)))


def generate_gcs_task_table_data(TaskSpec task_spec):
    cdef:
        TaskTableData task_table_data
    task_table_data.mutable_task().mutable_task_spec().CopyFrom(
        task_spec.task_spec.get().GetMessage())
    return task_table_data.SerializeAsString()
