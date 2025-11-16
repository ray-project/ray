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


cdef extern from "ray/raylet_rpc_client/raylet_pxi_client.h" namespace "ray::rpc" nogil:
    cdef cppclass CResizeResult "ray::rpc::RayletPXIClient::ResizeResult":
        int status_code
        c_string message
        c_map[c_string, double] total_resources

    cdef cppclass CRayletPXIClient "ray::rpc::RayletPXIClient":
        CRayletPXIClient(const c_string &host, int port)
        CResizeResult ResizeLocalResourceInstances(
            const c_map[c_string, double] &resources)


cdef class RayletPXIClient:
    cdef:
        shared_ptr[CRayletPXIClient] inner

    @staticmethod
    def create(host: str, port: int) -> "RayletPXIClient":
        cdef c_string c_host = host.encode()
        cdef shared_ptr[CRayletPXIClient] p = shared_ptr[CRayletPXIClient](
            new CRayletPXIClient(c_host, port)
        )
        cdef RayletPXIClient obj = RayletPXIClient()
        obj.inner = p
        return obj

    def resize_local_resource_instances(self, resources: dict) -> dict:
        cdef c_map[c_string, double] c_resources
        # Convert Python dict[str, float] to std::map<string,double>
        for k, v in resources.items():
            c_resources[k.encode()] = float(v)

        cdef CRayletPXIClient.CResizeResult res
        with nogil:
            res = self.inner.get().ResizeLocalResourceInstances(c_resources)

        # On error, raise Python exception with code + message
        if res.status_code != 0:
            raise RayletRPCError(int(res.status_code), (<bytes>res.message).decode())

        py_total = {}
        cdef c_map[c_string, double] totals = res.total_resources
        cdef c_map[c_string, double].iterator it = totals.begin()
        while it != totals.end():
            py_total[(<bytes>dereference(it).first).decode()] = dereference(it).second
            postincrement(it)

        return py_total

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
