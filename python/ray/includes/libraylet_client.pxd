from libcpp.string cimport string as c_string
from libc.stdint cimport int64_t, int32_t, uint8_t, uintptr_t
from libcpp.utility cimport pair
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set as c_unordered_set

from ray.includes.common cimport *

ResourceMappingType = \
    unordered_map[c_string, vector[pair[int64_t, double]]]
WaitResultPair = pair[vector[ObjectID], vector[ObjectID]]

cdef extern from "ray/raylet/raylet_client.h" namespace "" nogil:
    cdef cppclass LibRayletClient "RayletClient":
        LibRayletClient(const c_string &raylet_socket,
                        const UniqueID &client_id,
                        c_bool is_worker, const JobID &driver_id,
                        const Language &language)
        RayStatus Disconnect()
        RayStatus SubmitTask(const vector[ObjectID] &execution_dependencies,
                             const RayletTaskSpecification &task_spec)
        RayStatus GetTask(unique_ptr[RayletTaskSpecification] *task_spec)
        RayStatus TaskDone()
        RayStatus FetchOrReconstruct(vector[ObjectID] &object_ids,
                                     c_bool fetch_only,
                                     const TaskID &current_task_id)
        RayStatus NotifyUnblocked(const TaskID &current_task_id)
        RayStatus Wait(const vector[ObjectID] &object_ids, int num_returns,
                       int64_t timeout_milliseconds, c_bool wait_local,
                       const TaskID &current_task_id, WaitResultPair *result)
        RayStatus PushError(const JobID &job_id, const c_string &type,
                            const c_string &error_message, double timestamp)
        RayStatus PushProfileEvents(const ProfileTableDataT &profile_events)
        RayStatus FreeObjects(const vector[ObjectID] &object_ids,
                              c_bool local_only)
        Language GetLanguage() const
        JobID GetClientID() const
        JobID GetDriverID() const
        c_bool IsWorker() const
        const ResourceMappingType &GetResourceIDs() const
