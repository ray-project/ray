from libcpp.vector cimport vector as c_vector
from libcpp.string cimport string as c_string
from libc.stdint cimport int32_t as c_int32_t
from libcpp.memory cimport unique_ptr, make_unique
from ray.includes.common cimport CRayletClient, CRayStatus, CAddress

cdef class RayletClient:
    cdef:
        unique_ptr[CRayletClient] inner

    def __cinit__(self, ip_address: str, port: int):
        cdef:
            c_string c_ip_address
            c_int32_t c_port
        c_ip_address = ip_address.encode('utf-8')
        c_port = <int32_t>port
        self.inner = make_unique[CRayletClient](c_ip_address, c_port)

    cdef list get_worker_pids(self, timeout_ms: int):
        cdef:
            c_vector[c_int32_t] pids
            CRayStatus status
        assert self.inner.get() is not NULL
        status = self.inner.get().GetWorkerPIDs(pids, timeout_ms)
        if not status.ok():
            raise RuntimeError("Failed to get worker PIDs from raylet: " + status.message())
        return [pid for pid in pids]
