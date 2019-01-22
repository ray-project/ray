# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from ray.includes.common cimport *
from ray.includes.libraylet cimport CRayletClient, ResourceMappingType, WaitResultPair
from ray.includes.task cimport RayletTaskSpecification, RayletTaskArgument, RayletTaskArgumentByValue, RayletTaskArgumentByReference
from ray.includes.ray_config cimport RayConfig
from ray.utils import decode, _random_string

from libcpp.utility cimport pair

from cython.operator import dereference, postincrement
cimport cpython

if cpython.PY_MAJOR_VERSION >= 3:
    import pickle
else:
    import cPickle as pickle
import numpy


include "includes/unique_ids.pxi"


cdef int check_status(const CRayStatus& status) nogil except -1:
    if status.ok():
        return 0

    with gil:
        message = status.message().decode()
        raise Exception(message)


cdef c_vector[CObjectID] ObjectIDsToVector(object_ids):
    """A helper function that converts a Python list of object ids to a vector.

    Args:
        object_ids(list): The Python list of object ids.

    Returns:
        The output vector.
    """
    cdef:
        ObjectID object_id
        c_vector[CObjectID] result
    for object_id in object_ids:
        result.push_back(object_id.data)
    return result


cdef VectorToObjectIDs(c_vector[CObjectID] object_ids):
    result = []
    for i in range(object_ids.size()):
        result.append(ObjectID.from_native(object_ids[i]))
    return result


def compute_put_id(TaskID task_id, int64_t put_index):
    if put_index < 1 or put_index > kMaxTaskPuts:
        raise ValueError("The range of 'put_index' should be [1, %d]" % kMaxTaskPuts)
    return ObjectID.from_native(ComputePutId(task_id.data, put_index))


def compute_task_id(ObjectID object_id):
    return TaskID.from_native(ComputeTaskId(object_id.data))


cdef c_bool is_simple_value(value, int *num_elements_contained):
    # Because "RayConfig" have a private destructor,
    # we need to call "RayConfig.instance()" instead of defining a new variable.

    num_elements_contained[0] += 1

    if num_elements_contained[0] >= RayConfig.instance().num_elements_limit():
        return False

    if (cpython.PyInt_Check(value) or cpython.PyLong_Check(value) or value is False or
        value is True or cpython.PyFloat_Check(value) or value is None):
        return True

    if cpython.PyBytes_CheckExact(value):
        num_elements_contained[0] += cpython.PyBytes_Size(value)
        return num_elements_contained[0] < RayConfig.instance().num_elements_limit()

    if cpython.PyUnicode_CheckExact(value):
        num_elements_contained[0] += cpython.PyUnicode_GET_SIZE(value)
        return num_elements_contained[0] < RayConfig.instance().num_elements_limit()

    if cpython.PyList_CheckExact(value) and cpython.PyList_Size(value) < RayConfig.instance().size_limit():
        for item in value:
            if not is_simple_value(item, num_elements_contained):
                return False
        return num_elements_contained[0] < RayConfig.instance().num_elements_limit()

    if cpython.PyDict_CheckExact(value) and cpython.PyDict_Size(value) < RayConfig.instance().size_limit():
        # TODO(suquark): Use "items" in Python2 would be not very efficient.
        for k, v in value.items():
            if not (is_simple_value(k, num_elements_contained) and is_simple_value(v, num_elements_contained)):
                return False
        return num_elements_contained[0] < RayConfig.instance().num_elements_limit()

    if cpython.PyTuple_CheckExact(value) and cpython.PyTuple_Size(value) < RayConfig.instance().size_limit():
        for item in value:
            if not is_simple_value(item, num_elements_contained):
                return False
        return num_elements_contained[0] < RayConfig.instance().num_elements_limit()

    if isinstance(value, numpy.ndarray):
        if value.dtype == "O":
            return False
        num_elements_contained[0] += value.nbytes
        return num_elements_contained[0] < RayConfig.instance().num_elements_limit()

    return False


def check_simple_value(value):
    """This method checks if a Python object is sufficiently simple that it can be
    serialized and passed by value as an argument to a task (without being put in
    the object store). The details of which objects are sufficiently simple are
    defined by this method and are not particularly important. But for
    performance reasons, it is better to place "small" objects in the task itself
    and "large" objects in the object store.
    """

    cdef int num_elements_contained = 0
    return is_simple_value(value, &num_elements_contained)


cdef class Language:
    cdef CLanguage lang
    def __cinit__(self, int32_t lang):
        self.lang = <CLanguage>lang

    @staticmethod
    cdef from_native(const CLanguage& lang):
        return Language(<int32_t>lang)

    def __eq__(self, other):
        return isinstance(other, Language) and (<int32_t>self.lang) == (<int32_t>other.lang)

    def __repr__(self):
        if <int32_t>self.lang == <int32_t>LANGUAGE_PYTHON:
            return "PYTHON"
        elif <int32_t>self.lang == <int32_t>LANGUAGE_CPP:
            return "CPP"
        elif <int32_t>self.lang == <int32_t>LANGUAGE_JAVA:
            return "JAVA"
        else:
            raise Exception("Unexpected error")


# Programming language enum values.
cdef Language LANG_PYTHON = Language.from_native(LANGUAGE_PYTHON)
cdef Language LANG_CPP = Language.from_native(LANGUAGE_CPP)
cdef Language LANG_JAVA = Language.from_native(LANGUAGE_JAVA)


cdef class Config:
    @staticmethod
    def ray_protocol_version():
        return RayConfig.instance().ray_protocol_version()

    @staticmethod
    def handler_warning_timeout_ms():
        return RayConfig.instance().handler_warning_timeout_ms()

    @staticmethod
    def heartbeat_timeout_milliseconds():
        return RayConfig.instance().heartbeat_timeout_milliseconds()

    @staticmethod
    def debug_dump_period_milliseconds():
        return RayConfig.instance().debug_dump_period_milliseconds()

    @staticmethod
    def num_heartbeats_timeout():
        return RayConfig.instance().num_heartbeats_timeout()

    @staticmethod
    def num_heartbeats_warning():
        return RayConfig.instance().num_heartbeats_warning()

    @staticmethod
    def initial_reconstruction_timeout_milliseconds():
        return RayConfig.instance().initial_reconstruction_timeout_milliseconds()

    @staticmethod
    def get_timeout_milliseconds():
        return RayConfig.instance().get_timeout_milliseconds()

    @staticmethod
    def max_lineage_size():
        return RayConfig.instance().max_lineage_size()

    @staticmethod
    def worker_get_request_size():
        return RayConfig.instance().worker_get_request_size()

    @staticmethod
    def worker_fetch_request_size():
        return RayConfig.instance().worker_fetch_request_size()

    @staticmethod
    def actor_max_dummy_objects():
        return RayConfig.instance().actor_max_dummy_objects()

    @staticmethod
    def num_connect_attempts():
        return RayConfig.instance().num_connect_attempts()

    @staticmethod
    def connect_timeout_milliseconds():
        return RayConfig.instance().connect_timeout_milliseconds()

    @staticmethod
    def local_scheduler_fetch_timeout_milliseconds():
        return RayConfig.instance().local_scheduler_fetch_timeout_milliseconds()

    @staticmethod
    def local_scheduler_reconstruction_timeout_milliseconds():
        return RayConfig.instance().local_scheduler_reconstruction_timeout_milliseconds()

    @staticmethod
    def max_num_to_reconstruct():
        return RayConfig.instance().max_num_to_reconstruct()

    @staticmethod
    def local_scheduler_fetch_request_size():
        return RayConfig.instance().local_scheduler_fetch_request_size()

    @staticmethod
    def kill_worker_timeout_milliseconds():
        return RayConfig.instance().kill_worker_timeout_milliseconds()

    @staticmethod
    def max_time_for_handler_milliseconds():
        return RayConfig.instance().max_time_for_handler_milliseconds()

    @staticmethod
    def size_limit():
        return RayConfig.instance().size_limit()

    @staticmethod
    def num_elements_limit():
        return RayConfig.instance().num_elements_limit()

    @staticmethod
    def max_time_for_loop():
        return RayConfig.instance().max_time_for_loop()

    @staticmethod
    def redis_db_connect_retries():
        return RayConfig.instance().redis_db_connect_retries()

    @staticmethod
    def redis_db_connect_wait_milliseconds():
        return RayConfig.instance().redis_db_connect_wait_milliseconds()

    @staticmethod
    def plasma_default_release_delay():
        return RayConfig.instance().plasma_default_release_delay()

    @staticmethod
    def L3_cache_size_bytes():
        return RayConfig.instance().L3_cache_size_bytes()

    @staticmethod
    def max_tasks_to_spillback():
        return RayConfig.instance().max_tasks_to_spillback()

    @staticmethod
    def actor_creation_num_spillbacks_warning():
        return RayConfig.instance().actor_creation_num_spillbacks_warning()

    @staticmethod
    def node_manager_forward_task_retry_timeout_milliseconds():
        return RayConfig.instance().node_manager_forward_task_retry_timeout_milliseconds()

    @staticmethod
    def object_manager_pull_timeout_ms():
        return RayConfig.instance().object_manager_pull_timeout_ms()

    @staticmethod
    def object_manager_push_timeout_ms():
        return RayConfig.instance().object_manager_push_timeout_ms()

    @staticmethod
    def object_manager_repeated_push_delay_ms():
        return RayConfig.instance().object_manager_repeated_push_delay_ms()

    @staticmethod
    def object_manager_default_chunk_size():
        return RayConfig.instance().object_manager_default_chunk_size()

    @staticmethod
    def num_workers_per_process():
        return RayConfig.instance().num_workers_per_process()

    @staticmethod
    def max_task_lease_timeout_ms():
        return RayConfig.instance().max_task_lease_timeout_ms()


cdef unordered_map[c_string, double] resource_map_from_python_dict(resource_map):
    cdef:
        unordered_map[c_string, double] out
        c_string resource_name
    if not isinstance(resource_map, dict):
        raise TypeError("resource_map must be a dictionary")
    for key, value in resource_map.items():
        out[key.encode("ascii")] = float(value)
    return out


cdef class Task:
    cdef:
        unique_ptr[RayletTaskSpecification] task_spec
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
            c_vector[shared_ptr[RayletTaskArgument]] task_args
            c_vector[CActorHandleID] task_new_actor_handles
            c_vector[c_string] c_function_descriptor
            c_string pickled_str
            c_vector[CObjectID] references

        for item in function_descriptor:
            if not isinstance(item, bytes):
                raise TypeError("'function_descriptor' takes a list of byte strings.")
            c_function_descriptor.push_back(item)

        # Parse the resource map.
        if resource_map is not None:
            required_resources = resource_map_from_python_dict(resource_map)
        if required_resources.count(b"CPU") == 0:
            required_resources[b"CPU"] = 1.0
        if placement_resource_map is not None:
            required_placement_resources = resource_map_from_python_dict(placement_resource_map)

        # Parse the arguments from the list.
        for arg in arguments:
            if isinstance(arg, ObjectID):
                references = c_vector[CObjectID]()
                references.push_back((<ObjectID>arg).data)
                task_args.push_back(static_pointer_cast[RayletTaskArgument, RayletTaskArgumentByReference](make_shared[RayletTaskArgumentByReference](references)))
            else:
                pickled_str = pickle.dumps(arg, protocol=pickle.HIGHEST_PROTOCOL)
                task_args.push_back(static_pointer_cast[RayletTaskArgument, RayletTaskArgumentByValue](make_shared[RayletTaskArgumentByValue](<uint8_t *>pickled_str.c_str(), pickled_str.size())))

        for new_actor_handle in new_actor_handles:
            task_new_actor_handles.push_back((<ActorHandleID?>new_actor_handle).data)

        self.task_spec.reset(new RayletTaskSpecification(
            CUniqueID(driver_id.data), parent_task_id.data, parent_counter, actor_creation_id.data,
            actor_creation_dummy_object_id.data, max_actor_reconstructions, CUniqueID(actor_id.data),
            CUniqueID(actor_handle_id.data), actor_counter, task_new_actor_handles, task_args, num_returns,
            required_resources, required_placement_resources, LANGUAGE_PYTHON,
            c_function_descriptor))

        # Set the task's execution dependencies.
        self.execution_dependencies.reset(new c_vector[CObjectID]())
        if execution_arguments is not None:
            for execution_arg in execution_arguments:
                self.execution_dependencies.get().push_back((<ObjectID?>execution_arg).data)

    @staticmethod
    cdef make(unique_ptr[RayletTaskSpecification]& task_spec):
        cdef Task self = Task.__new__(Task)
        self.task_spec.reset(task_spec.release())
        # The created task does not include any execution dependencies.
        self.execution_dependencies.reset(new c_vector[CObjectID]())
        return self

    @classmethod
    def create_driver_task(cls, DriverID task_driver_id):
        cdef int nil_actor_counter = 0
        return cls(
            task_driver_id,
            [],  # function_descriptor
            [],  # arguments.
            0,  # num_returns.
            TaskID(_random_string()),  # parent_task_id.
            0,  # parent_counter.
            ActorID.nil(),  # actor_creation_id.
            ObjectID.nil(),  # actor_creation_dummy_object_id.
            0,  # max_actor_reconstructions.
            ActorID.nil(),  # actor_id.
            ActorHandleID.nil(),  # actor_handle_id.
            nil_actor_counter,  # actor_counter.
            [],  # new_actor_handles.
            [],  # execution_dependencies.
            {"CPU": 0},  # resource_map.
            {},  # placement_resource_map.
        )

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
        self.task_spec.reset(new RayletTaskSpecification(task_spec_str))
        # The created task does not include any execution dependencies.
        self.execution_dependencies.reset(new c_vector[CObjectID]())
        return self

    def to_string(self):
        """Convert a Ray task specification Python object to a string.

        Returns:
            String representing the task specification.
        """
        return self.task_spec.get().ToFlatbuffer()

    def driver_id(self):
        """Return the driver ID for this task."""
        return DriverID.from_native(self.task_spec.get().DriverId())

    def task_id(self):
        """Return the task ID for this task."""
        return TaskID.from_native(self.task_spec.get().TaskId())

    def parent_task_id(self):
        """Return the task ID of the parent task."""
        return TaskID.from_native(self.task_spec.get().ParentTaskId())

    def parent_counter(self):
        """Return the parent counter of this task."""
        return self.task_spec.get().ParentCounter()

    def function_descriptor_list(self):
        """Return the function descriptor for this task."""
        cdef c_vector[c_string] function_descriptor = self.task_spec.get().FunctionDescriptor()
        results = []
        for i in range(function_descriptor.size()):
            results.append(function_descriptor[i])
        return results

    def arguments(self):
        """Return the arguments for the task."""
        cdef:
            RayletTaskSpecification *task_spec = self.task_spec.get()
            int64_t num_args = task_spec.NumArgs()
            int count
        arg_list = []
        for i in range(num_args):
            count = task_spec.ArgIdCount(i)
            if count > 0:
                assert count == 1
                arg_list.append(ObjectID.from_native(task_spec.ArgId(i, 0)))
            else:
                serialized_str = task_spec.ArgVal(i)[:task_spec.ArgValLength(i)]
                obj = pickle.loads(serialized_str)
                arg_list.append(obj)
        return arg_list

    def returns(self):
        """Return the object IDs for the return values of the task."""
        cdef RayletTaskSpecification *task_spec = self.task_spec.get()
        return_id_list = []
        for i in range(task_spec.NumReturns()):
            return_id_list.append(ObjectID.from_native(task_spec.ReturnId(i)))
        return return_id_list

    def required_resources(self):
        """Return the resource vector of the task."""
        cdef:
            unordered_map[c_string, double] resource_map = self.task_spec.get().GetRequiredResources().GetResourceMap()
            c_string resource_name
            double resource_value
            unordered_map[c_string, double].iterator iterator = resource_map.begin()

        required_resources = {}
        while iterator != resource_map.end():
            resource_name = dereference(iterator).first
            # TODO(suquark): What is the type of the resource name (bytes, str, unicode)?
            py_resource_name = str(resource_name)  # bytes for Py2, unicode for Py3
            resource_value = dereference(iterator).second
            required_resources[py_resource_name] = resource_value
            postincrement(iterator)
        return required_resources

    def actor_creation_id(self):
        """Return the actor creation ID for the task."""
        return ActorID.from_native(self.task_spec.get().ActorCreationId())

    def actor_creation_dummy_object_id(self):
        """Return the actor creation dummy object ID for the task."""
        return ObjectID.from_native(self.task_spec.get().ActorCreationDummyObjectId())

    def actor_id(self):
        """Return the actor ID for this task."""
        return ActorID.from_native(self.task_spec.get().ActorId())

    def actor_counter(self):
        """Return the actor counter for this task."""
        return self.task_spec.get().ActorCounter()


cdef class RayletClient:
    cdef unique_ptr[CRayletClient] client
    def __cinit__(self, raylet_socket,
                  ClientID client_id,
                  c_bool is_worker,
                  DriverID driver_id):
        # We have known that we are using Python, so just skip the language parameter.
        # TODO(suquark): Should we allow unicode chars in "raylet_socket"?
        self.client.reset(new CRayletClient(raylet_socket.encode("ascii"), client_id.data,
                          is_worker, driver_id.data, LANGUAGE_PYTHON))

    def disconnect(self):
        check_status(self.client.get().Disconnect())

    def submit_task(self, Task task_spec):
        check_status(self.client.get().SubmitTask(task_spec.execution_dependencies.get()[0], task_spec.task_spec.get()[0]))

    def get_task(self):
        cdef:
            unique_ptr[RayletTaskSpecification] task_spec

        with nogil:
            check_status(self.client.get().GetTask(&task_spec))
        return Task.make(task_spec)

    def task_done(self):
        check_status(self.client.get().TaskDone())

    def fetch_or_reconstruct(self, object_ids,
                             c_bool fetch_only, TaskID current_task_id=TaskID.nil()):
        cdef c_vector[CObjectID] fetch_ids = ObjectIDsToVector(object_ids)
        check_status(self.client.get().FetchOrReconstruct(fetch_ids, fetch_only, current_task_id.data))

    def notify_unblocked(self, TaskID current_task_id):
        check_status(self.client.get().NotifyUnblocked(current_task_id.data))

    def wait(self, object_ids, int num_returns, int64_t timeout_milliseconds,
             c_bool wait_local, TaskID current_task_id):
        cdef:
            WaitResultPair result
            c_vector[CObjectID] wait_ids
        wait_ids = ObjectIDsToVector(object_ids)
        check_status(self.client.get().Wait(wait_ids, num_returns, timeout_milliseconds,
                                            wait_local, current_task_id.data, &result))
        return VectorToObjectIDs(result.first), VectorToObjectIDs(result.second)

    def resource_ids(self):
        cdef:
            ResourceMappingType resource_mapping = self.client.get().GetResourceIDs()
            unordered_map[c_string, c_vector[pair[int64_t, double]]].iterator iterator = resource_mapping.begin()
            c_vector[pair[int64_t, double]] c_value
        resources_dict = {}
        while iterator != resource_mapping.end():
            key = decode(dereference(iterator).first)
            c_value = dereference(iterator).second
            ids_and_fractions = []
            for i in range(c_value.size()):
                ids_and_fractions.append((c_value[i].first, c_value[i].second))
            resources_dict[key] = ids_and_fractions
            postincrement(iterator)
        return resources_dict

    def push_error(self, DriverID job_id, error_type, error_message,
                   double timestamp):
        check_status(self.client.get().PushError(job_id.data,
                                                 error_type.encode("ascii"),
                                                 error_message.encode("ascii"),
                                                 timestamp))

    def push_profile_events(self, component_type, UniqueID component_id,
                            node_ip_address, profile_data):
        cdef:
            GCSProfileTableDataT profile_info
            GCSProfileEventT *profile_event
            c_string event_type

        if not profile_data:
            return  # Short circuit if there are no profile events.

        profile_info.component_type = component_type.encode("ascii")
        profile_info.component_id = component_id.binary()
        profile_info.node_ip_address = node_ip_address.encode("ascii")

        for py_profile_event in profile_data:
            profile_event = new GCSProfileEventT()
            if not isinstance(py_profile_event, dict):
                raise TypeError("Incorrect type for a profile event. Expected dict instead of '%s'" % str(type(py_profile_event)))
            # TODO(rkn): If the dictionary is formatted incorrectly, that could lead
            # to errors. E.g., if any of the strings are empty, that will cause
            # segfaults in the node manager.
            for key_string, event_data in py_profile_event.items():
                if key_string == "event_type":
                    profile_event.event_type = event_data.encode("ascii")
                    if profile_event.event_type.length() == 0:
                        raise ValueError("'event_type' should not be a null string.")
                elif key_string == "start_time":
                    profile_event.start_time = float(event_data)
                elif key_string == "end_time":
                    profile_event.end_time = float(event_data)
                elif key_string == "extra_data":
                    profile_event.extra_data = event_data.encode("ascii")
                    if profile_event.extra_data.length() == 0:
                        raise ValueError("'extra_data' should not be a null string.")
                else:
                    raise ValueError("Unknown profile event key '%s'" % key_string)
            # Note that profile_info.profile_events is a vector of unique pointers, so
            # profile_event will be deallocated when profile_info goes out of scope.
            # "emplace_back" of vector has not been supported by Cython
            profile_info.profile_events.push_back(unique_ptr[GCSProfileEventT](profile_event))

        check_status(self.client.get().PushProfileEvents(profile_info))

    def free_objects(self, object_ids, c_bool local_only):
        cdef c_vector[CObjectID] free_ids = ObjectIDsToVector(object_ids)
        check_status(self.client.get().FreeObjects(free_ids, local_only))

    @property
    def language(self):
        return Language.from_native(self.client.get().GetLanguage())

    @property
    def client_id(self):
        return ClientID.from_native(self.client.get().GetClientID())

    @property
    def driver_id(self):
        return DriverID.from_native(self.client.get().GetDriverID())

    @property
    def is_worker(self):
        return self.client.get().IsWorker()
