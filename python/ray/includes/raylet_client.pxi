# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from libcpp.string cimport string as c_string
from libcpp.memory cimport shared_ptr
from libcpp cimport bool as c_bool

from ray.includes.unique_ids cimport CNodeID
from cython.operator import dereference
from ray.includes.common cimport CRayStatus

import asyncio
import threading
from asyncio import Future
from ray.core.generated import node_manager_pb2

logger = logging.getLogger(__name__)

cdef extern from "ray/common/asio/instrumented_io_context.h":
    cdef cppclass instrumented_io_context:
        instrumented_io_context()

cdef extern from "ray/raylet_client/raylet_client.h" namespace "ray::raylet":
    cdef cppclass CyIoContextRunner:
        CyIoContextRunner()
        instrumented_io_context &GetMainService()
        void StopAndJoin()

cdef extern from "ray/rpc/client_call.h" namespace "ray::rpc":
    cdef cppclass ClientCallManager:
        ClientCallManager(instrumented_io_context& io_service, c_bool record_stats)

cdef extern from "ray/rpc/node_manager/raylet_client_pool.h" namespace "ray::rpc":
    cdef cppclass Address
    cdef cppclass RayletClientPool:
        pass
    Address RayletClientPool_GenerateRayletAddress "ray::rpc::RayletClientPool::GenerateRayletAddress"(
        const CNodeID &node_id, const c_string &ip, int port)

cdef extern from "src/ray/protobuf/node_manager.pb.h" namespace "ray::rpc":
    cdef cppclass ResizeLocalResourceInstancesReply:
        const c_string &SerializeAsString() const
    cdef cppclass ResizeLocalResourceInstancesRequest:
        ResizeLocalResourceInstancesRequest()
        MapStringDouble* mutable_resources()

cdef extern from "google/protobuf/map.h" namespace "google::protobuf":
    cdef cppclass MapStringDouble "google::protobuf::Map<std::string, double>":
        double& operator[](const c_string &)

cdef extern from "ray/raylet_client/raylet_client.h" namespace "ray::raylet":
    cdef cppclass RayletClient:
        RayletClient(const Address &address,
                     ClientCallManager &client_call_manager,
                     void (*)())

    ctypedef void (*ResizeLocalResourceInstancesCcb)(
        void* ctx, const CRayStatus &status, const ResizeLocalResourceInstancesReply &reply)
    void RayletClient_ResizeLocalResourceInstancesC(
        RayletClient* client,
        const ResizeLocalResourceInstancesRequest &request,
        ResizeLocalResourceInstancesCcb cb,
        void* ctx) nogil

cdef void _resize_local_resource_instances_ccb(
    void* ctx,
    const CRayStatus &status,
    const ResizeLocalResourceInstancesReply &reply
) noexcept nogil:
    with gil:
        if status.ok():
            py = node_manager_pb2.ResizeLocalResourceInstancesReply()
            py.ParseFromString(bytes(reply.SerializeAsString()))
            assign_and_decrement_fut((py, None), <object>ctx)
        else:
            err = RuntimeError(status.ToString().decode())
            assign_and_decrement_fut((None, err), <object>ctx)

cdef class InnerRayletClient:
    cdef:
        CyIoContextRunner *_runner
        ClientCallManager *_mgr
        shared_ptr[RayletClient] _client
        object _io_thread

    def __cinit__(self, str host, int port):
        self._runner = new CyIoContextRunner()
        self._mgr = new ClientCallManager(self._runner.GetMainService(), False)
        self._client = shared_ptr[RayletClient](
            new RayletClient(
                RayletClientPool_GenerateRayletAddress(CNodeID.Nil(), host.encode(), port),
                dereference(self._mgr),
                NULL,
            )
        )
        # No Python thread needed; runner owns thread and work guard in C++.

    def __dealloc__(self):
        self._client.reset()
        if self._runner is not NULL:
            self._runner.StopAndJoin()
            del self._runner
            self._runner = NULL
        if self._mgr is not NULL:
            del self._mgr
            self._mgr = NULL

    def async_resize_local_resource_instances(self, resources: dict[str, float]) -> Future[node_manager_pb2.ResizeLocalResourceInstancesReply]:
        cdef ResizeLocalResourceInstancesRequest req
        cdef MapStringDouble* m = req.mutable_resources()
        for k, v in resources.items():
            dereference(m)[k.encode()] = float(v)

        cdef object fut = incremented_fut()

        with nogil:
            RayletClient_ResizeLocalResourceInstancesC(
                self._client.get(), req, _resize_local_resource_instances_ccb, <void*>fut
            )
        return asyncio.wrap_future(fut)

def create_raylet_client(host: str, port: int):
    """Create a reusable Raylet client for host and port."""
    return InnerRayletClient(host, port)


