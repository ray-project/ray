from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool
from libcpp.vector cimport vector as c_vector
from libcpp.memory cimport unique_ptr
from ray.includes.unique_ids cimport (
    CActorID,
    CClientID,
    CObjectID,
    CWorkerID,
)

cdef extern from "ray/gcs/gcs_client/global_state_accessor.h" nogil:
    cdef cppclass CGlobalStateAccessor "ray::gcs::GlobalStateAccessor":
        CGlobalStateAccessor(const c_string &redis_address,
                             const c_string &redis_password,
                             c_bool is_test)
        c_bool Connect()
        void Disconnect()
        c_vector[c_string] GetAllJobInfo()
        c_vector[c_string] GetAllNodeInfo()
        c_vector[c_string] GetAllProfileInfo()
        c_vector[c_string] GetAllObjectInfo()
        unique_ptr[c_string] GetObjectInfo(const CObjectID &object_id)
        c_vector[c_string] GetAllActorInfo()
        unique_ptr[c_string] GetActorInfo(const CActorID &actor_id)
        c_string GetNodeResourceInfo(const CClientID &node_id)
        unique_ptr[c_string] GetWorkerInfo(const CWorkerID &worker_id)
        c_vector[c_string] GetAllWorkerInfo()
        c_bool AddWorkerInfo(const c_string &serialized_string)
