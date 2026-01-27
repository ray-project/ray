# cython: language_level = 3

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

from libcpp.map cimport map as c_map
from libcpp.pair cimport pair as c_pair
from libcpp cimport bool as c_bool
from cython.operator cimport dereference, postincrement

cdef class RayletRPCError(Exception):
    def __init__(self, code: int, message: str):
        self.code = int(code)
        self.message = message
        super().__init__(f"Raylet RPC error {self.code}: {self.message}")


cdef convert_optional_vector_int32(
        CRayStatus status, optional[c_vector[int32_t]] vec) with gil:
    try:
        check_status_timeout_as_rpc_error(status)
        assert vec.has_value()
        return move(vec.value()), None
    except Exception as e:
        return None, e


cdef convert_optional_map_string_double(
        CRayStatus status, optional[c_map[c_string, double]] totals) with gil:
    cdef c_map[c_string, double] values
    cdef c_map[c_string, double].iterator it
    try:
        check_status_timeout_as_rpc_error(status)
        assert totals.has_value()
        py_total = {}
        values = totals.value()
        it = values.begin()
        while it != values.end():
            py_total[(<bytes>dereference(it).first).decode()] = dereference(it).second
            postincrement(it)
        return py_total, None
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

    def async_get_agent_pids(self, timeout_ms: int = 1000) -> Future[list[int]]:
        """Get the PIDs of dashboard and runtime_env agent."""
        cdef:
            fut = incremented_fut()
            int32_t timeout = <int32_t>timeout_ms
        assert self.inner.get() is not NULL
        with nogil:
            self.inner.get().GetAgentPIDs(
                OptionalItemPyCallback[c_vector[int32_t]](
                    &convert_optional_vector_int32,
                    assign_and_decrement_fut,
                    fut),
                timeout)
        return asyncio.wrap_future(fut)

    def async_resize_local_resource_instances(
            self, resources: dict, timeout_ms: int = 1000) -> Future[dict]:
        """Resize local resource instances on the raylet."""
        cdef:
            fut = incremented_fut()
            int32_t timeout = <int32_t>timeout_ms
            c_map[c_string, double] c_resources
        # Convert Python dict[str, float] to std::map<string,double>
        for k, v in resources.items():
            c_resources[k.encode()] = float(v)

        assert self.inner.get() is not NULL
        with nogil:
            self.inner.get().AsyncResizeLocalResourceInstances(
                c_resources,
                OptionalItemPyCallback[c_map[c_string, double]](
                    &convert_optional_map_string_double,
                    assign_and_decrement_fut,
                    fut),
                timeout)
        return asyncio.wrap_future(fut)
