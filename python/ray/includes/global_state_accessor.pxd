from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool
from libcpp.vector cimport vector as c_vector
from libcpp.memory cimport unique_ptr
from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CNodeID,
    CObjectID,
    CWorkerID,
    CPlacementGroupID,
)
from ray.includes.common cimport (
    CRayStatus,
    CGcsClientOptions,
)

cdef extern from "ray/gcs/gcs_client/global_state_accessor.h" nogil:
    cdef cppclass CGlobalStateAccessor "ray::gcs::GlobalStateAccessor":
        CGlobalStateAccessor(const CGcsClientOptions&)
        c_bool Connect()
        void Disconnect()
        c_vector[c_string] GetAllJobInfo()
        CJobID GetNextJobID()
        c_vector[c_string] GetAllNodeInfo()
        c_vector[c_string] GetAllAvailableResources()
        c_vector[c_string] GetAllTaskEvents()
        unique_ptr[c_string] GetObjectInfo(const CObjectID &object_id)
        unique_ptr[c_string] GetAllResourceUsage()
        c_vector[c_string] GetAllActorInfo()
        unique_ptr[c_string] GetActorInfo(const CActorID &actor_id)
        unique_ptr[c_string] GetWorkerInfo(const CWorkerID &worker_id)
        c_vector[c_string] GetAllWorkerInfo()
        c_bool AddWorkerInfo(const c_string &serialized_string)
        unique_ptr[c_string] GetPlacementGroupInfo(
            const CPlacementGroupID &placement_group_id)
        unique_ptr[c_string] GetPlacementGroupByName(
            const c_string &placement_group_name,
            const c_string &ray_namespace,
        )
        c_vector[c_string] GetAllPlacementGroupInfo()
        c_string GetSystemConfig()
        CRayStatus GetNodeToConnectForDriver(
            const c_string &node_ip_address,
            c_string *node_to_connect)
