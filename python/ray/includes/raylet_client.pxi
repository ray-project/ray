from asyncio import Future
import concurrent.futures
from libcpp.vector cimport vector as c_vector
from libcpp.string cimport string as c_string
from libc.stdint cimport int32_t
from libcpp.utility cimport move
from libcpp.memory cimport unique_ptr, make_unique, shared_ptr
from ray.includes.common cimport (
    CRayletClientWithIoContext,
    CRayStatus,
    CAddress,
    OptionalItemPyCallback,
)
from ray.includes.optional cimport optional


cdef convert_optional_vector_int32(
        CRayStatus status, optional[c_vector[int32_t]] vec) with gil:
    try:
        check_status_timeout_as_rpc_error(status)
        assert vec.has_value()
        return move(vec.value()), None
    except Exception as e:
        return None, e


cdef class RayletClient:
    cdef:
        unique_ptr[CRayletClientWithIoContext] inner

    def __cinit__(self, ip_address: str, port: int):
        cdef:
            c_string c_ip_address
            int32_t c_port
        c_ip_address = ip_address.encode('utf-8')
        c_port = <int32_t>port
        self.inner = make_unique[CRayletClientWithIoContext](c_ip_address, c_port)

    def async_get_worker_pids(self, timeout_ms: int = 1000) -> Future[list[int]]:
        """Get the PIDs of all workers registered with the raylet."""
        cdef:
            fut = incremented_fut()
            int32_t timeout = <int32_t>timeout_ms
        assert self.inner.get() is not NULL
        with nogil:
            self.inner.get().GetWorkerPIDs(
                OptionalItemPyCallback[c_vector[int32_t]](
                    &convert_optional_vector_int32,
                    assign_and_decrement_fut,
                    fut),
                timeout)
        return asyncio.wrap_future(fut)
