from ray.includes.common cimport *

from libcpp.string cimport string as c_string
from libc.stdint cimport int64_t, int32_t, uint8_t, uintptr_t
from libcpp.utility cimport pair
from libcpp.vector cimport vector as c_vector
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set as c_unordered_set


ctypedef unordered_map[c_string, c_vector[pair[int64_t, double]]] ResourceMappingType
ctypedef pair[c_vector[ObjectID], c_vector[ObjectID]] WaitResultPair

cdef extern from "ray/raylet/raylet_client.h" nogil:
    cdef cppclass LibRayletClient "RayletClient":
        LibRayletClient(const c_string &raylet_socket,
                        const ClientID &client_id,
                        c_bool is_worker, const JobID &driver_id,
                        const CLanguage &language)
        CRayStatus Disconnect()
        CRayStatus SubmitTask(const c_vector[ObjectID] &execution_dependencies,
                             const RayletTaskSpecification &task_spec)
        CRayStatus GetTask(unique_ptr[RayletTaskSpecification] *task_spec)
        CRayStatus TaskDone()
        CRayStatus FetchOrReconstruct(c_vector[ObjectID] &object_ids,
                                     c_bool fetch_only,
                                     const TaskID &current_task_id)
        CRayStatus NotifyUnblocked(const TaskID &current_task_id)
        CRayStatus Wait(const c_vector[ObjectID] &object_ids, int num_returns,
                       int64_t timeout_milliseconds, c_bool wait_local,
                       const TaskID &current_task_id, WaitResultPair *result)
        CRayStatus PushError(const JobID &job_id, const c_string &type,
                            const c_string &error_message, double timestamp)
        CRayStatus PushProfileEvents(const GCSProfileTableDataT &profile_events)
        CRayStatus FreeObjects(const c_vector[ObjectID] &object_ids,
                              c_bool local_only)
        CLanguage GetLanguage() const
        ClientID GetClientID() const
        JobID GetDriverID() const
        c_bool IsWorker() const
        const ResourceMappingType &GetResourceIDs() const
