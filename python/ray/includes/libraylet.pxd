from ray.includes.common cimport *
from ray.includes.task cimport *

from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map

ctypedef unordered_map[c_string, c_vector[pair[int64_t, double]]] ResourceMappingType
ctypedef pair[c_vector[CObjectID], c_vector[CObjectID]] WaitResultPair

cdef extern from "ray/raylet/raylet_client.h" nogil:
    cdef cppclass CRayletClient "RayletClient":
        CRayletClient(const c_string &raylet_socket,
                      const CClientID &client_id,
                      c_bool is_worker, const CDriverID &driver_id,
                      const CLanguage &language)
        CRayStatus Disconnect()
        CRayStatus SubmitTask(const c_vector[CObjectID] &execution_dependencies,
                             const CTaskSpecification &task_spec)
        CRayStatus GetTask(unique_ptr[CTaskSpecification] *task_spec)
        CRayStatus TaskDone()
        CRayStatus FetchOrReconstruct(c_vector[CObjectID] &object_ids,
                                     c_bool fetch_only,
                                     const CTaskID &current_task_id)
        CRayStatus NotifyUnblocked(const CTaskID &current_task_id)
        CRayStatus Wait(const c_vector[CObjectID] &object_ids, int num_returns,
                       int64_t timeout_milliseconds, c_bool wait_local,
                       const CTaskID &current_task_id, WaitResultPair *result)
        CRayStatus PushError(const CDriverID &job_id, const c_string &type,
                             const c_string &error_message, double timestamp)
        CRayStatus PushProfileEvents(const GCSProfileTableDataT &profile_events)
        CRayStatus FreeObjects(const c_vector[CObjectID] &object_ids,
                              c_bool local_only)
        CLanguage GetLanguage() const
        CClientID GetClientID() const
        CDriverID GetDriverID() const
        c_bool IsWorker() const
        const ResourceMappingType &GetResourceIDs() const
