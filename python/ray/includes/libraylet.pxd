from libc.stdint cimport int64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CLanguage,
    CRayStatus,
)
from ray.includes.unique_ids cimport (
    CActorCheckpointID,
    CActorID,
    CClientID,
    CDriverID,
    CObjectID,
    CTaskID,
)
from ray.includes.task cimport CTaskSpecification


cdef extern from "ray/gcs/format/gcs_generated.h" nogil:
    cdef cppclass GCSProfileEventT "ProfileEventT":
        c_string event_type
        double start_time
        double end_time
        c_string extra_data
        GCSProfileEventT()

    cdef cppclass GCSProfileTableDataT "ProfileTableDataT":
        c_string component_type
        c_string component_id
        c_string node_ip_address
        c_vector[unique_ptr[GCSProfileEventT]] profile_events
        GCSProfileTableDataT()


ctypedef unordered_map[c_string, c_vector[pair[int64_t, double]]] \
    ResourceMappingType
ctypedef pair[c_vector[CObjectID], c_vector[CObjectID]] WaitResultPair


cdef extern from "ray/raylet/raylet_client.h" nogil:
    cdef cppclass CRayletClient "RayletClient":
        CRayletClient(const c_string &raylet_socket,
                      const CClientID &client_id,
                      c_bool is_worker, const CDriverID &driver_id,
                      const CLanguage &language)
        CRayStatus Disconnect()
        CRayStatus SubmitTask(
            const c_vector[CObjectID] &execution_dependencies,
            const CTaskSpecification &task_spec)
        CRayStatus GetTask(unique_ptr[CTaskSpecification] *task_spec)
        CRayStatus TaskDone()
        CRayStatus FetchOrReconstruct(c_vector[CObjectID] &object_ids,
                                      c_bool fetch_only,
                                      const CTaskID &current_task_id)
        CRayStatus NotifyUnblocked(const CTaskID &current_task_id)
        CRayStatus Wait(const c_vector[CObjectID] &object_ids,
                        int num_returns, int64_t timeout_milliseconds,
                        c_bool wait_local, const CTaskID &current_task_id,
                        WaitResultPair *result)
        CRayStatus PushError(const CDriverID &driver_id, const c_string &type,
                             const c_string &error_message, double timestamp)
        CRayStatus PushProfileEvents(
            const GCSProfileTableDataT &profile_events)
        CRayStatus FreeObjects(const c_vector[CObjectID] &object_ids,
                               c_bool local_only)
        CRayStatus PrepareActorCheckpoint(const CActorID &actor_id,
                                          CActorCheckpointID &checkpoint_id)
        CRayStatus NotifyActorResumedFromCheckpoint(
            const CActorID &actor_id, const CActorCheckpointID &checkpoint_id)
        CLanguage GetLanguage() const
        CClientID GetClientID() const
        CDriverID GetDriverID() const
        c_bool IsWorker() const
        const ResourceMappingType &GetResourceIDs() const
