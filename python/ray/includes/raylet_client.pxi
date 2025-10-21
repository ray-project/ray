# cython: language_level = 3

"""
Binding of C++ ray::rpc::RayletPXIClient.
"""

from libcpp.string cimport string as c_string
from libcpp.map cimport map as c_map
from libcpp.memory cimport shared_ptr
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
