from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair

from ray.includes.common cimport (
    CRayStatus,
    CGcsClientOptions,
)

cdef extern from "ray/gcs/accessor.h" nogil:
    cdef cppclass CKVAccessor "ray::gcs::KVAccessor":
        CRayStatus Put(const c_string &key, const c_string &value)
        CRayStatus Get(const c_string &key, c_string &value)
        CRayStatus Del(const c_string &key)
        CRayStatus Exists(const c_string &key, c_bool &exist)

cdef extern from "ray/gcs/gcs_client.h" nogil:
    cdef cppclass CGcsClient "ray::gcs::GcsClient":
        CKVAccessor &KV()
        CRayStatus Connect(c_instrumented_io_context& io_service)

cdef extern from "ray/gcs/gcs_client/service_based_gcs_client.h" nogil:
    cdef cppclass CServiceBasedGcsClient "ray::gcs::ServiceBasedGcsClient"(CGcsClient):
        CServiceBasedGcsClient(const CGcsClientOptions& options)

cdef extern from "ray/common/asio/instrumented_io_context.h" nogil:
    cdef cppclass c_instrumented_io_context "instrumented_io_context":
        c_instrumented_io_context()
