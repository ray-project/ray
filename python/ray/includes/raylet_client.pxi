from libcpp.vector cimport vector as c_vector
from libcpp.string cimport string as c_string
from libc.stdint cimport int32_t as c_int32_t
from libcpp.memory cimport unique_ptr, make_unique, shared_ptr
from ray.includes.common cimport (
    CRayletClientWithIoContext,
    CRayStatus,
    CAddress,
    ConnectOnSingletonIoContext
)
from cython.operator import dereference

cdef class RayletClient:
    cdef:
        unique_ptr[CRayletClientWithIoContext] inner

    def __cinit__(self, ip_address: str, port: int):
        cdef:
            c_string c_ip_address
            c_int32_t c_port
        c_ip_address = ip_address.encode('utf-8')
        c_port = <int32_t>port
        self.inner = make_unique[CRayletClientWithIoContext](c_ip_address, c_port)

    def get_worker_pids(self, timeout_ms: int = 1000) -> list[int]:
        """Get the PIDs of all workers registered with the raylet."""
        cdef:
            shared_ptr[c_vector[c_int32_t]] pids
            CRayStatus status
        pids = make_shared[c_vector[c_int32_t]]()
        assert self.inner.get() is not NULL
        status = self.inner.get().GetWorkerPIDs(pids, timeout_ms)
        if status.IsTimedOut():
            raise TimeoutError(status.message())
        elif not status.ok():
            raise RuntimeError(
                "Failed to get worker PIDs from raylet: " + status.message()
            )
        assert pids.get() is not NULL
        return [pid for pid in pids.get()[0]]
