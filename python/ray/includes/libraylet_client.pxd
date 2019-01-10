from ray.includes.common cimport *

from libcpp.string cimport string as c_string
from libc.stdint cimport int64_t, int32_t, uint8_t, uintptr_t
from libcpp.utility cimport pair
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set as c_unordered_set


ctypedef unordered_map[c_string, vector[pair[int64_t, double]]] ResourceMappingType
ctypedef pair[vector[CObjectID], vector[CObjectID]] WaitResultPair

cdef extern from "ray/raylet/raylet_client.h" namespace "" nogil:
    cdef cppclass LibRayletClient "RayletClient":
        LibRayletClient(const c_string &raylet_socket,
                        const CClientID &client_id,
                        c_bool is_worker, const CJobID &driver_id,
                        const Language &language)
        RayStatus Disconnect()
        RayStatus SubmitTask(const vector[CObjectID] &execution_dependencies,
                             const RayletTaskSpecification &task_spec)
        RayStatus GetTask(unique_ptr[RayletTaskSpecification] *task_spec)
        RayStatus TaskDone()
        RayStatus FetchOrReconstruct(vector[CObjectID] &object_ids,
                                     c_bool fetch_only,
                                     const CTaskID &current_task_id)
        RayStatus NotifyUnblocked(const CTaskID &current_task_id)
        RayStatus Wait(const vector[CObjectID] &object_ids, int num_returns,
                       int64_t timeout_milliseconds, c_bool wait_local,
                       const CTaskID &current_task_id, WaitResultPair *result)
        RayStatus PushError(const CJobID &job_id, const c_string &type,
                            const c_string &error_message, double timestamp)
        RayStatus PushProfileEvents(const ProfileTableDataT &profile_events)
        RayStatus FreeObjects(const vector[CObjectID] &object_ids,
                              c_bool local_only)
        Language GetLanguage() const
        CClientID GetClientID() const
        CJobID GetDriverID() const
        c_bool IsWorker() const
        const ResourceMappingType &GetResourceIDs() const
