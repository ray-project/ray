from libc.stdint cimport uint8_t
from libcpp.memory cimport (
    make_shared,
    shared_ptr,
    static_pointer_cast,
)
from ray.includes.task cimport (
    CTaskArgument,
    CTaskArgumentByReference,
    CTaskArgumentByValue,
    CTaskSpecification,
    SerializeTaskAsString,
)


cdef class Task:
    cdef:
        unique_ptr[CTaskSpecification] task_spec
        unique_ptr[c_vector[CObjectID]] execution_dependencies

    def __init__(self, DriverID driver_id, function_descriptor, arguments,
                 int num_returns, TaskID parent_task_id, int parent_counter,
                 ActorID actor_creation_id,
                 ObjectID actor_creation_dummy_object_id,
                 int32_t max_actor_reconstructions, ActorID actor_id,
                 ActorHandleID actor_handle_id, int actor_counter,
                 new_actor_handles, execution_arguments, resource_map,
                 placement_resource_map):
        cdef:
            unordered_map[c_string, double] required_resources
            unordered_map[c_string, double] required_placement_resources
            c_vector[shared_ptr[CTaskArgument]] task_args
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
        if required_resources.count(b"CPU") == 0:
            required_resources[b"CPU"] = 1.0
        if placement_resource_map is not None:
            required_placement_resources = (
                resource_map_from_dict(placement_resource_map))

        # Parse the arguments from the list.
        for arg in arguments:
            if isinstance(arg, ObjectID):
                references = c_vector[CObjectID]()
                references.push_back((<ObjectID>arg).native())
                task_args.push_back(
                    static_pointer_cast[CTaskArgument,
                                        CTaskArgumentByReference](
                        make_shared[CTaskArgumentByReference](references)))
            else:
                pickled_str = pickle.dumps(
                    arg, protocol=pickle.HIGHEST_PROTOCOL)
                task_args.push_back(
                    static_pointer_cast[CTaskArgument,
                                        CTaskArgumentByValue](
                        make_shared[CTaskArgumentByValue](
                            <uint8_t *>pickled_str.c_str(),
                            pickled_str.size())))

        for new_actor_handle in new_actor_handles:
            task_new_actor_handles.push_back(
                (<ActorHandleID?>new_actor_handle).native())

        self.task_spec.reset(new CTaskSpecification(
            driver_id.native(), parent_task_id.native(), parent_counter, actor_creation_id.native(),
            actor_creation_dummy_object_id.native(), max_actor_reconstructions, actor_id.native(),
            actor_handle_id.native(), actor_counter, task_new_actor_handles, task_args, num_returns,
            required_resources, required_placement_resources, LANGUAGE_PYTHON,
            c_function_descriptor))

        # Set the task's execution dependencies.
        self.execution_dependencies.reset(new c_vector[CObjectID]())
        if execution_arguments is not None:
            for execution_arg in execution_arguments:
                self.execution_dependencies.get().push_back(
                    (<ObjectID?>execution_arg).native())

    @staticmethod
    cdef make(unique_ptr[CTaskSpecification]& task_spec):
        cdef Task self = Task.__new__(Task)
        self.task_spec.reset(task_spec.release())
        # The created task does not include any execution dependencies.
        self.execution_dependencies.reset(new c_vector[CObjectID]())
        return self

    @staticmethod
    def from_string(const c_string& task_spec_str):
        """Convert a string to a Ray task specification Python object.

        Args:
            task_spec_str: String representation of the task specification.

        Returns:
            Python task specification object.
        """
        cdef Task self = Task.__new__(Task)
        # TODO(pcm): Use flatbuffers validation here.
        self.task_spec.reset(new CTaskSpecification(task_spec_str))
        # The created task does not include any execution dependencies.
        self.execution_dependencies.reset(new c_vector[CObjectID]())
        return self

    def to_string(self):
        """Convert a Ray task specification Python object to a string.

        Returns:
            String representing the task specification.
        """
        return self.task_spec.get().SerializeAsString()

    def _serialized_raylet_task(self):
        return SerializeTaskAsString(
            self.execution_dependencies.get(), self.task_spec.get())

    def driver_id(self):
        """Return the driver ID for this task."""
        return DriverID(self.task_spec.get().DriverId().binary())

    def task_id(self):
        """Return the task ID for this task."""
        return TaskID(self.task_spec.get().TaskId().binary())

    def parent_task_id(self):
        """Return the task ID of the parent task."""
        return TaskID(self.task_spec.get().ParentTaskId().binary())

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
                        ObjectID(task_spec.ArgId(i, 0).binary()))
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
            return_id_list.append(ObjectID(task_spec.ReturnId(i).binary()))
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
        return ActorID(self.task_spec.get().ActorCreationId().binary())

    def actor_creation_dummy_object_id(self):
        """Return the actor creation dummy object ID for the task."""
        return ObjectID(
            self.task_spec.get().ActorCreationDummyObjectId().binary())

    def actor_id(self):
        """Return the actor ID for this task."""
        return ActorID(self.task_spec.get().ActorId().binary())

    def actor_counter(self):
        """Return the actor counter for this task."""
        return self.task_spec.get().ActorCounter()
