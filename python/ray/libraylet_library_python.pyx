# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from ray.includes.common cimport *
from ray.includes.libraylet_client cimport LibRayletClient, ResourceMappingType, WaitResultPair
from libc.stdint cimport int32_t as c_int32, int64_t as c_int64
from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr
from libcpp.vector cimport vector as c_vector

from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport pair

from cython.operator import dereference, postincrement

include "includes/unique_ids.pxi"


cdef c_vector[ObjectID] ObjectIDsToVector(object_ids):
    cdef:
        PyObjectID object_id
        c_vector[ObjectID] object_id_vector
    for object_id in object_ids:
        object_id_vector.push_back(object_id.data)
    return object_id_vector


cdef VectorToObjectIDs(c_vector[ObjectID] object_id_vector):
    object_ids = []
    for i in range(object_id_vector.size()):
        object_ids.append(PyObjectID.from_native(object_id_vector[i]))
    return object_ids


def compute_put_id(PyTaskID task_id, c_int64 put_index):
    if put_index < 1 or put_index > kMaxTaskPuts:
        raise ValueError("The range of 'put_index' should be [1, %d]" % kMaxTaskPuts)
    cdef CObjectID object_id = ComputePutId(task_id.data, put_index)
    return PyObjectID.from_native(object_id)


# Programming language enum values.
cdef c_int32 LANG_PYTHON = <c_int32>LANGUAGE_PYTHON
cdef c_int32 LANG_CPP = <c_int32>LANGUAGE_CPP
cdef c_int32 LANG_JAVA = <c_int32>LANGUAGE_JAVA

cdef class Language:
    cdef CLanguage lang
    def __cinit__(self, c_int32 lang):
        self.lang = <CLanguage>lang

    @staticmethod
    cdef from_native(const CLanguage& lang):
        return Language(<c_int32>lang)

    def __eq__(self, other):
        return isinstance(other, Language) and (<c_int32>self.lang) == (<c_int32>other.lang)

    def __repr__(self):
        if <c_int32>self.lang == <c_int32>LANGUAGE_PYTHON:
            return "PYTHON"
        elif <c_int32>self.lang == <c_int32>LANGUAGE_CPP:
            return "CPP"
        elif <c_int32>self.lang == <c_int32>LANGUAGE_JAVA:
            return "JAVA"
        else:
            raise Exception("Unexpected error")


cdef class TaskSpecification:
    cdef unique_ptr[RayletTaskSpecification] task_spec
    # cdef from_native(self, unique_ptr[RayletTaskSpecification] task_spec):
    #     self.task_spec = task_spec


cdef class RayletClient:
    cdef unique_ptr[LibRayletClient] client
    def __cinit__(self, const c_string & raylet_socket,
                  PyClientID client_id,
                  c_bool is_worker,
                  PyJobID driver_id):
        # We have known that we are using Python, so just skip the language parameter.
        self.client.reset(new LibRayletClient(raylet_socket, client_id.data,
                          is_worker, driver_id.data, LANGUAGE_PYTHON))

    def disconnect(self):
        cdef:
            CRayStatus status = self.client.get().Disconnect()
        if not status.ok():
            raise ConnectionError("[RayletClient] Failed to disconnect.")

    def submit_task(self, execution_dependencies, TaskSpecification task_spec):
        cdef:
            c_vector[CObjectID] exec_deps
            CRayStatus cstatus
        exec_deps = ObjectIDsToVector(execution_dependencies)
        cstatus = self.client.get().SubmitTask(exec_deps, task_spec.task_spec.get()[0])
        if not cstatus.ok():
            raise Exception(cstatus.ToString())

    def get_task(self):
        return NotImplementedError

    def fetch_or_reconstruct(self, object_ids,
                             c_bool fetch_only, PyTaskID current_task_id):
        cdef:
            c_vector[CObjectID] fetch_ids
            CRayStatus cstatus
        fetch_ids = ObjectIDsToVector(object_ids)
        cstatus = self.client.get().FetchOrReconstruct(fetch_ids, fetch_only, current_task_id.data)
        if not cstatus.ok():
            raise Exception(cstatus.ToString())

    def notify_blocked(self, PyTaskID current_task_id):
        raise NotImplementedError

    def wait(self, object_ids, int num_returns, c_int64 timeout_milliseconds,
             c_bool wait_local, PyTaskID current_task_id):
        cdef:
            WaitResultPair result
            c_vector[CObjectID] wait_ids
            CRayStatus cstatus
        wait_ids = ObjectIDsToVector(object_ids)
        cstatus = self.client.get().Wait(wait_ids, num_returns, timeout_milliseconds,
                                         wait_local, current_task_id.data, &result)
        if not cstatus.ok():
            raise Exception(cstatus.ToString())
        return VectorToObjectIDs(result.first), VectorToObjectIDs(result.second)

    def resource_ids(self):
        cdef:
            ResourceMappingType resource_mapping = self.client.get().GetResourceIDs()
            unordered_map[c_string, c_vector[pair[c_int64, double]]].iterator iterator = resource_mapping.begin()
            c_vector[pair[c_int64, double]] c_value
        resources_dict = {}
        while iterator != resource_mapping.end():
            key = dereference(iterator).first
            c_value = dereference(iterator).second
            ids_and_fractions = []
            for i in range(c_value.size()):
                ids_and_fractions.append((c_value[i].first, c_value[i].second))
            resources_dict[key] = ids_and_fractions
            iterator = postincrement(iterator)
        return resources_dict

    def push_error(self, PyJobID job_id, const c_string& type,
                   const c_string& error_message, double timestamp):
        raise NotImplementedError

    def push_profile_events(self, const c_string& component_type, PyUniqueID component_id,
                            const c_string& node_ip_address, profile_data):

        raise NotImplementedError

    def free_objects(self, object_ids, c_bool local_only):
        cdef:
            c_vector[CObjectID] free_ids
            CRayStatus cstatus
        free_ids = ObjectIDsToVector(object_ids)
        cstatus = self.client.get().FreeObjects(free_ids, local_only)
        if not cstatus.ok():
            raise Exception(cstatus.ToString())

    @property
    def language(self):
        return Language.from_native(self.client.get().GetLanguage())

    @property
    def client_id(self):
        return PyClientID.from_native(self.client.get().GetClientID())

    @property
    def driver_id(self):
        return PyJobID.from_native(self.client.get().GetDriverID())

    @property
    def is_worker(self):
        return self.client.get().IsWorker()
