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
    CJobID,
    CWorkerID,
    CObjectID,
    CTaskID,
)
from ray.includes.task cimport CTaskSpec


cdef extern from "ray/protobuf/gcs.pb.h" nogil:
    cdef cppclass GCSProfileEvent "ProfileTableData::ProfileEvent":
        void set_event_type(const c_string &value)
        void set_start_time(double value)
        void set_end_time(double value)
        c_string set_extra_data(const c_string &value)
        GCSProfileEvent()

    cdef cppclass GCSProfileTableData "ProfileTableData":
        void set_component_type(const c_string &value)
        void set_component_id(const c_string &value)
        void set_node_ip_address(const c_string &value)
        GCSProfileEvent *add_profile_events()
        GCSProfileTableData()


ctypedef unordered_map[c_string, c_vector[pair[int64_t, double]]] \
    ResourceMappingType
ctypedef pair[c_vector[CObjectID], c_vector[CObjectID]] WaitResultPair


cdef extern from "ray/raylet/raylet_client.h" nogil:
    cdef cppclass CRayletClient "RayletClient":
        CRayletClient(const c_string &raylet_socket,
                      const CWorkerID &worker_id,
                      c_bool is_worker, const CJobID &job_id,
                      const CLanguage &language)
        CRayStatus Disconnect()
        CRayStatus SubmitTask(const CTaskSpec &task_spec)
        CRayStatus GetTask(unique_ptr[CTaskSpec] *task_spec)
        CRayStatus TaskDone()
        CRayStatus FetchOrReconstruct(c_vector[CObjectID] &object_ids,
                                      c_bool fetch_only,
                                      const CTaskID &current_task_id)
        CRayStatus NotifyUnblocked(const CTaskID &current_task_id)
        CRayStatus Wait(const c_vector[CObjectID] &object_ids,
                        int num_returns, int64_t timeout_milliseconds,
                        c_bool wait_local, const CTaskID &current_task_id,
                        WaitResultPair *result)
        CRayStatus PushError(const CJobID &job_id, const c_string &type,
                             const c_string &error_message, double timestamp)
        CRayStatus PushProfileEvents(
            const GCSProfileTableData &profile_events)
        CRayStatus FreeObjects(const c_vector[CObjectID] &object_ids,
                               c_bool local_only, c_bool delete_creating_tasks)
        CRayStatus PrepareActorCheckpoint(const CActorID &actor_id,
                                          CActorCheckpointID &checkpoint_id)
        CRayStatus NotifyActorResumedFromCheckpoint(
            const CActorID &actor_id, const CActorCheckpointID &checkpoint_id)
        CRayStatus SetResource(const c_string &resource_name,
                               const double capacity,
                               const CClientID &client_Id)
        CLanguage GetLanguage() const
        CWorkerID GetWorkerID() const
        CJobID GetJobID() const
        c_bool IsWorker() const
        const ResourceMappingType &GetResourceIDs() const
