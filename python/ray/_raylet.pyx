# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

import numpy

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from cython.operator import dereference, postincrement

from ray.includes.common cimport (
    CLanguage,
    CRayStatus,
    LANGUAGE_CPP,
    LANGUAGE_JAVA,
    LANGUAGE_PYTHON,
)
from ray.includes.libraylet cimport (
    CRayletClient,
    GCSProfileEvent,
    GCSProfileTableData,
    ResourceMappingType,
    WaitResultPair,
)
from ray.includes.unique_ids cimport (
    CActorCheckpointID,
    CObjectID,
    CClientID,
)
from ray.includes.task cimport CTaskSpec
from ray.includes.ray_config cimport RayConfig
from ray.exceptions import RayletError
from ray.utils import decode

cimport cpython

include "includes/unique_ids.pxi"
include "includes/ray_config.pxi"
include "includes/task.pxi"


if cpython.PY_MAJOR_VERSION >= 3:
    import pickle
else:
    import cPickle as pickle


cdef int check_status(const CRayStatus& status) nogil except -1:
    if status.ok():
        return 0

    with gil:
        message = status.message().decode()
        raise RayletError(message)


cdef c_vector[CObjectID] ObjectIDsToVector(object_ids):
    """A helper function that converts a Python list of object IDs to a vector.

    Args:
        object_ids (list): The Python list of object IDs.

    Returns:
        The output vector.
    """
    cdef:
        ObjectID object_id
        c_vector[CObjectID] result
    for object_id in object_ids:
        result.push_back(object_id.native())
    return result


cdef VectorToObjectIDs(c_vector[CObjectID] object_ids):
    result = []
    for i in range(object_ids.size()):
        result.append(ObjectID(object_ids[i].Binary()))
    return result


def compute_put_id(TaskID task_id, int64_t put_index):
    if put_index < 1 or put_index > kMaxTaskPuts:
        raise ValueError("The range of 'put_index' should be [1, %d]"
                         % kMaxTaskPuts)
    return ObjectID(CObjectID.ForPut(task_id.native(), put_index).Binary())


def compute_task_id(ObjectID object_id):
    return TaskID(object_id.native().TaskId().Binary())


cdef c_bool is_simple_value(value, int64_t *num_elements_contained):
    num_elements_contained[0] += 1

    if num_elements_contained[0] >= RayConfig.instance().num_elements_limit():
        return False

    if (cpython.PyInt_Check(value) or cpython.PyLong_Check(value) or
            value is False or value is True or cpython.PyFloat_Check(value) or
            value is None):
        return True

    if cpython.PyBytes_CheckExact(value):
        num_elements_contained[0] += cpython.PyBytes_Size(value)
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if cpython.PyUnicode_CheckExact(value):
        num_elements_contained[0] += cpython.PyUnicode_GET_SIZE(value)
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if (cpython.PyList_CheckExact(value) and
            cpython.PyList_Size(value) < RayConfig.instance().size_limit()):
        for item in value:
            if not is_simple_value(item, num_elements_contained):
                return False
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if (cpython.PyDict_CheckExact(value) and
            cpython.PyDict_Size(value) < RayConfig.instance().size_limit()):
        # TODO(suquark): Using "items" in Python2 is not very efficient.
        for k, v in value.items():
            if not (is_simple_value(k, num_elements_contained) and
                    is_simple_value(v, num_elements_contained)):
                return False
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if (cpython.PyTuple_CheckExact(value) and
            cpython.PyTuple_Size(value) < RayConfig.instance().size_limit()):
        for item in value:
            if not is_simple_value(item, num_elements_contained):
                return False
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if isinstance(value, numpy.ndarray):
        if value.dtype == "O":
            return False
        num_elements_contained[0] += value.nbytes
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    return False


def check_simple_value(value):
    """Check if value is simple enough to be send by value.

    This method checks if a Python object is sufficiently simple that it can
    be serialized and passed by value as an argument to a task (without being
    put in the object store). The details of which objects are sufficiently
    simple are defined by this method and are not particularly important.
    But for performance reasons, it is better to place "small" objects in
    the task itself and "large" objects in the object store.

    Args:
        value: Python object that should be checked.

    Returns:
        True if the value should be send by value, False otherwise.
    """

    cdef int64_t num_elements_contained = 0
    return is_simple_value(value, &num_elements_contained)


cdef class Language:
    cdef CLanguage lang

    def __cinit__(self, int32_t lang):
        self.lang = <CLanguage>lang

    @staticmethod
    cdef from_native(const CLanguage& lang):
        return Language(<int32_t>lang)

    def __eq__(self, other):
        return (isinstance(other, Language) and
                (<int32_t>self.lang) == (<int32_t>other.lang))

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


cdef unordered_map[c_string, double] resource_map_from_dict(resource_map):
    cdef:
        unordered_map[c_string, double] out
        c_string resource_name
    if not isinstance(resource_map, dict):
        raise TypeError("resource_map must be a dictionary")
    for key, value in resource_map.items():
        out[key.encode("ascii")] = float(value)
    return out


cdef class RayletClient:
    cdef unique_ptr[CRayletClient] client

    def __cinit__(self, raylet_socket,
                  ClientID client_id,
                  c_bool is_worker,
                  JobID job_id):
        # We know that we are using Python, so just skip the language
        # parameter.
        # TODO(suquark): Should we allow unicode chars in "raylet_socket"?
        self.client.reset(new CRayletClient(
            raylet_socket.encode("ascii"), client_id.native(), is_worker,
            job_id.native(), LANGUAGE_PYTHON))

    def disconnect(self):
        check_status(self.client.get().Disconnect())

    def submit_task(self, TaskSpec task_spec):
        cdef:
            CObjectID c_id
        check_status(self.client.get().SubmitTask(
            task_spec.task_spec.get()[0]))

    def get_task(self):
        cdef:
            unique_ptr[CTaskSpec] task_spec

        with nogil:
            check_status(self.client.get().GetTask(&task_spec))
        return TaskSpec.make(task_spec)

    def task_done(self):
        check_status(self.client.get().TaskDone())

    def fetch_or_reconstruct(self, object_ids,
                             c_bool fetch_only,
                             TaskID current_task_id=TaskID.nil()):
        cdef c_vector[CObjectID] fetch_ids = ObjectIDsToVector(object_ids)
        check_status(self.client.get().FetchOrReconstruct(
            fetch_ids, fetch_only, current_task_id.native()))

    def notify_unblocked(self, TaskID current_task_id):
        check_status(self.client.get().NotifyUnblocked(current_task_id.native()))

    def wait(self, object_ids, int num_returns, int64_t timeout_milliseconds,
             c_bool wait_local, TaskID current_task_id):
        cdef:
            WaitResultPair result
            c_vector[CObjectID] wait_ids
            CTaskID c_task_id = current_task_id.native()
        wait_ids = ObjectIDsToVector(object_ids)
        with nogil:
            check_status(self.client.get().Wait(wait_ids, num_returns,
                                                timeout_milliseconds,
                                                wait_local,
                                                c_task_id, &result))
        return (VectorToObjectIDs(result.first),
                VectorToObjectIDs(result.second))

    def resource_ids(self):
        cdef:
            ResourceMappingType resource_mapping = (
                self.client.get().GetResourceIDs())
            unordered_map[
                c_string, c_vector[pair[int64_t, double]]
            ].iterator iterator = resource_mapping.begin()
            c_vector[pair[int64_t, double]] c_value
        resources_dict = {}
        while iterator != resource_mapping.end():
            key = decode(dereference(iterator).first)
            c_value = dereference(iterator).second
            ids_and_fractions = []
            for i in range(c_value.size()):
                ids_and_fractions.append(
                    (c_value[i].first, c_value[i].second))
            resources_dict[key] = ids_and_fractions
            postincrement(iterator)
        return resources_dict

    def push_error(self, JobID job_id, error_type, error_message,
                   double timestamp):
        check_status(self.client.get().PushError(job_id.native(),
                                                 error_type.encode("ascii"),
                                                 error_message.encode("ascii"),
                                                 timestamp))

    def push_profile_events(self, component_type, UniqueID component_id,
                            node_ip_address, profile_data):
        cdef:
            GCSProfileTableData profile_info
            GCSProfileEvent *profile_event
            c_string event_type

        if len(profile_data) == 0:
            return  # Short circuit if there are no profile events.

        profile_info.set_component_type(component_type.encode("ascii"))
        profile_info.set_component_id(component_id.binary())
        profile_info.set_node_ip_address(node_ip_address.encode("ascii"))

        for py_profile_event in profile_data:
            profile_event = profile_info.add_profile_events()
            if not isinstance(py_profile_event, dict):
                raise TypeError(
                    "Incorrect type for a profile event. Expected dict "
                    "instead of '%s'" % str(type(py_profile_event)))
            # TODO(rkn): If the dictionary is formatted incorrectly, that
            # could lead to errors. E.g., if any of the strings are empty,
            # that will cause segfaults in the node manager.
            for key_string, event_data in py_profile_event.items():
                if key_string == "event_type":
                    if len(event_data) == 0:
                        raise ValueError(
                            "'event_type' should not be a null string.")
                    profile_event.set_event_type(event_data.encode("ascii"))
                elif key_string == "start_time":
                    profile_event.set_start_time(float(event_data))
                elif key_string == "end_time":
                    profile_event.set_end_time(float(event_data))
                elif key_string == "extra_data":
                    if len(event_data) == 0:
                        raise ValueError(
                            "'extra_data' should not be a null string.")
                    profile_event.set_extra_data(event_data.encode("ascii"))
                else:
                    raise ValueError(
                        "Unknown profile event key '%s'" % key_string)

        check_status(self.client.get().PushProfileEvents(profile_info))

    def free_objects(self, object_ids, c_bool local_only, c_bool delete_creating_tasks):
        cdef c_vector[CObjectID] free_ids = ObjectIDsToVector(object_ids)
        check_status(self.client.get().FreeObjects(free_ids, local_only, delete_creating_tasks))

    def prepare_actor_checkpoint(self, ActorID actor_id):
        cdef CActorCheckpointID checkpoint_id
        cdef CActorID c_actor_id = actor_id.native()
        # PrepareActorCheckpoint will wait for raylet's reply, release
        # the GIL so other Python threads can run.
        with nogil:
            check_status(self.client.get().PrepareActorCheckpoint(
                c_actor_id, checkpoint_id))
        return ActorCheckpointID(checkpoint_id.Binary())

    def notify_actor_resumed_from_checkpoint(self, ActorID actor_id,
                                             ActorCheckpointID checkpoint_id):
        check_status(self.client.get().NotifyActorResumedFromCheckpoint(
            actor_id.native(), checkpoint_id.native()))

    def set_resource(self, basestring resource_name, double capacity, ClientID client_id):
        self.client.get().SetResource(resource_name.encode("ascii"), capacity, CClientID.FromBinary(client_id.binary()))

    @property
    def language(self):
        return Language.from_native(self.client.get().GetLanguage())

    @property
    def client_id(self):
        return ClientID(self.client.get().GetClientID().Binary())

    @property
    def job_id(self):
        return JobID(self.client.get().GetJobID().Binary())

    @property
    def is_worker(self):
        return self.client.get().IsWorker()
