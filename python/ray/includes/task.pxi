from ray.includes.task cimport (
    CTask,
    CTaskExecutionSpec,
    CTaskSpec,
    RpcTaskExecutionSpec,
    TaskTableData,
)
from ray.ray_constants import RAW_BUFFER_METADATA
from ray.utils import decode


cdef class TaskSpec:
    """Cython wrapper class of C++ `ray::TaskSpecification`."""
    cdef:
        unique_ptr[CTaskSpec] task_spec

    @staticmethod
    def from_string(const c_string& task_spec_str):
        """Convert a string to a Ray task specification Python object.

        Args:
            task_spec_str: String representation of the task specification.

        Returns:
            Python task specification object.
        """
        cdef TaskSpec self = TaskSpec.__new__(TaskSpec)
        self.task_spec.reset(new CTaskSpec(task_spec_str))
        return self

    def to_string(self):
        """Convert a Ray task specification Python object to a string.

        Returns:
            String representing the task specification.
        """
        return self.task_spec.get().Serialize()

    def is_normal_task(self):
        """Whether this task is a normal task."""
        return self.task_spec.get().IsNormalTask()

    def is_actor_task(self):
        """Whether this task is an actor task."""
        return self.task_spec.get().IsActorTask()

    def is_actor_creation_task(self):
        """Whether this task is an actor creation task."""
        return self.task_spec.get().IsActorCreationTask()

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
            int64_t num_args = self.task_spec.get().NumArgs()
            int32_t lang = <int32_t>self.task_spec.get().GetLanguage()
            int count
        arg_list = []

        if lang == <int32_t>LANGUAGE_PYTHON:
            for i in range(num_args):
                count = self.task_spec.get().ArgIdCount(i)
                if count > 0:
                    assert count == 1
                    arg_list.append(
                        ObjectID(self.task_spec.get().ArgId(i, 0).Binary()))
                else:
                    data = self.task_spec.get().ArgData(i)[
                        :self.task_spec.get().ArgDataSize(i)]
                    metadata = self.task_spec.get().ArgMetadata(i)[
                        :self.task_spec.get().ArgMetadataSize(i)]
                    if metadata == RAW_BUFFER_METADATA:
                        obj = data
                    else:
                        obj = pickle.loads(data)
                    arg_list.append(obj)
        elif lang == <int32_t>LANGUAGE_JAVA:
            arg_list = num_args * ["<java-argument>"]

        return arg_list

    def returns(self):
        """Return the object IDs for the return values of the task."""
        return_id_list = []
        for i in range(self.task_spec.get().NumReturns()):
            return_id_list.append(
                ObjectID(self.task_spec.get().ReturnIdForPlasma(i).Binary()))
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
            py_resource_name = decode(resource_name)
            resource_value = dereference(iterator).second
            required_resources[py_resource_name] = resource_value
            postincrement(iterator)
        return required_resources

    def language(self):
        """Return the language of the task."""
        return Language.from_native(self.task_spec.get().GetLanguage())

    def actor_creation_id(self):
        """Return the actor creation ID for the task."""
        if not self.is_actor_creation_task():
            return ActorID.nil()
        return ActorID(self.task_spec.get().ActorCreationId().Binary())

    def actor_creation_dummy_object_id(self):
        """Return the actor creation dummy object ID for the task."""
        if not self.is_actor_task():
            return ObjectID.nil()
        return ObjectID(
            self.task_spec.get().ActorCreationDummyObjectId().Binary())

    def previous_actor_task_dummy_object_id(self):
        """Return the object ID of the previously executed actor task."""
        if not self.is_actor_task():
            return ObjectID.nil()
        return ObjectID(
            self.task_spec.get().PreviousActorTaskDummyObjectId().Binary())

    def actor_id(self):
        """Return the actor ID for this task."""
        if not self.is_actor_task():
            return ActorID.nil()
        return ActorID(self.task_spec.get().ActorId().Binary())

    def actor_counter(self):
        """Return the actor counter for this task."""
        if not self.is_actor_task():
            return 0
        return self.task_spec.get().ActorCounter()


cdef class TaskExecutionSpec:
    """Cython wrapper class of C++ `ray::TaskExecutionSpecification`."""
    cdef:
        unique_ptr[CTaskExecutionSpec] c_spec

    def __init__(self):
        cdef:
            RpcTaskExecutionSpec message

        self.c_spec.reset(new CTaskExecutionSpec(message))

    @staticmethod
    def from_string(const c_string& string):
        """Convert a string to a Ray `TaskExecutionSpec` Python object.
        """
        cdef TaskExecutionSpec self = TaskExecutionSpec.__new__(
            TaskExecutionSpec)
        self.c_spec.reset(new CTaskExecutionSpec(string))
        return self

    def num_forwards(self):
        return self.c_spec.get().NumForwards()


cdef class Task:
    """Cython wrapper class of C++ `ray::Task`."""
    cdef:
        unique_ptr[CTask] c_task

    def __init__(
            self, TaskSpec task_spec, TaskExecutionSpec task_execution_spec):
        self.c_task.reset(new CTask(task_spec.task_spec.get()[0],
                                    task_execution_spec.c_spec.get()[0]))


def generate_gcs_task_table_data(TaskSpec task_spec):
    """Converts a Python `TaskSpec` object to serialized GCS `TaskTableData`.
    """
    cdef:
        TaskTableData task_table_data
    task_table_data.mutable_task().mutable_task_spec().CopyFrom(
        task_spec.task_spec.get().GetMessage())
    return task_table_data.SerializeAsString()
