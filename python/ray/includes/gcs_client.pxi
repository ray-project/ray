# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

"""
Binding of C++ ray::gcs::GcsClient.
"""

# This file is a .pxi which is included in _raylet.pyx. This means any already-imported
# symbols can directly be used here. This is not ideal, but we can't easily split this
# out to a separate translation unit because we need to access the singleton thread.
#
# We need to best-effort import everything we need.
#
# Implementation Notes:
#
# Async API
#
# One challenge is that the C++ async API is callback-based, and the callbacks are
# invoked in the C++ threads. In `make_future_and_callback` we create a future and a
# callback, and the callback will fulfill the future in the event loop thread. The
# future is returned to Python to await, and the callback is passed to the C++ async
# API. Once C++ async API invokes the callback, the future is fulfilled in the Python
# event loop thread.
#
# Marshalling
#
# The C++ API returns ints, strings, `ray::Status` and C++ protobuf types. We need to
# convert them to Python types. In `python_callbacks.h` we define a series of converters
# with Cpython APIs:
#
# - bools, ints and strings are converted using `PyBool_FromLong` and alike.
# - `ray::Status` is marshalled to a 3-tuple and unmarshall it back to `CRayStatus` via
#     `to_c_ray_status`.
# - C++ protobuf types are serialized them to bytes, passed to Python and deserialized
# in the Python `postprocess` functions. Later if we need performance for specific
# methods we can add a custom Converter in `python_callbacks.h`.

from asyncio import Future
from typing import List
from ray.includes.common cimport (
    CGcsClient,
    CGetAllResourceUsageReply,
    ConnectOnSingletonIoContext,
    CStatusCode,
    CStatusCode_OK,
    PyDefaultCallback,
    PyMultiItemCallback,
    BoolConverter,
)
from ray.core.generated import gcs_pb2
from cython.operator import dereference, postincrement

cdef class NewGcsClient:
    cdef:
        shared_ptr[CGcsClient] inner

    # Creates and connects a standalone GcsClient.
    # cluster_id is in hex, if any.
    # TODO(ryw): we can also reuse the CoreWorker's GcsClient to save resources.
    @staticmethod
    def standalone(gcs_address: str,
                   cluster_id: Optional[str],
                   timeout_ms: int) -> "NewGcsClient":
        cdef GcsClientOptions gcs_options = None
        if cluster_id:
            gcs_options = GcsClientOptions.create(
                gcs_address, cluster_id, allow_cluster_id_nil=False,
                fetch_cluster_id_if_nil=False)
        else:
            gcs_options = GcsClientOptions.create(
                gcs_address, None, allow_cluster_id_nil=True,
                fetch_cluster_id_if_nil=True)
        cdef CGcsClientOptions* native = gcs_options.native()
        cdef shared_ptr[CGcsClient] inner = make_shared[CGcsClient](
            dereference(native))
        cdef int64_t c_timeout_ms = timeout_ms

        with nogil:
            check_status_timeout_as_rpc_error(
                ConnectOnSingletonIoContext(dereference(inner), c_timeout_ms))

        gcs_client = NewGcsClient()
        gcs_client.inner = inner
        return gcs_client

    @property
    def address(self) -> str:
        cdef c_pair[c_string, int] pair = self.inner.get().GetGcsServerAddress()
        host = pair.first.decode("utf-8")
        port = pair.second
        return f"{host}:{port}"

    @property
    def cluster_id(self) -> ray.ClusterID:
        cdef CClusterID cluster_id = self.inner.get().GetClusterId()
        return ray.ClusterID.from_binary(cluster_id.Binary())

    #############################################################
    # Internal KV sync methods
    #############################################################
    def internal_kv_get(
        self, c_string key, namespace=None, timeout=None
    ) -> Optional[bytes]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_string value
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Get(ns, key, timeout_ms, value)
        if status.IsNotFound():
            return None
        else:
            check_status_timeout_as_rpc_error(status)
            return value

    def internal_kv_multi_get(
        self, keys: List[bytes], namespace=None, timeout=None
    ) -> Dict[bytes, bytes]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_keys = [key for key in keys]
            unordered_map[c_string, c_string] values
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().MultiGet(ns, c_keys, timeout_ms, values)
            )

        result = {}
        it = values.begin()
        while it != values.end():
            key = dereference(it).first
            value = dereference(it).second
            result[key] = value
            postincrement(it)
        return result

    def internal_kv_put(self, c_string key, c_string value, c_bool overwrite=False,
                        namespace=None, timeout=None) -> int:
        """
        Returns 1 if the key is newly added, 0 if the key is overwritten.
        """
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_bool added = False
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get()
                .InternalKV()
                .Put(ns, key, value, overwrite, timeout_ms, added)
            )
        return 1 if added else 0

    def internal_kv_del(self, c_string key, c_bool del_by_prefix,
                        namespace=None, timeout=None) -> int:
        """
        Returns number of keys deleted.
        """
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            int num_deleted = 0
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get()
                .InternalKV()
                .Del(ns, key, del_by_prefix, timeout_ms, num_deleted)
            )
        return num_deleted

    def internal_kv_keys(
        self, c_string prefix, namespace=None, timeout=None
    ) -> List[bytes]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] keys
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().Keys(ns, prefix, timeout_ms, keys)
            )

        result = [key for key in keys]
        return result

    def internal_kv_exists(self, c_string key, namespace=None, timeout=None) -> bool:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_bool exists = False
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().Exists(ns, key, timeout_ms, exists)
            )
        return exists

    #############################################################
    # Internal KV async methods
    #############################################################

    def async_internal_kv_get(
        self, c_string key, namespace=None, timeout=None
    ) -> Future[Optional[bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1

        def postprocess(tup: Tuple[StatusParts, Any]):
            status_parts, val = tup
            cdef CRayStatus c_ray_status = to_c_ray_status(status_parts)
            if c_ray_status.IsNotFound():
                return None
            check_status_timeout_as_rpc_error(c_ray_status)
            return val
        fut, cb = make_future_and_callback(postprocess=postprocess)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVGet(
                    ns, key, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_multi_get(
        self, keys: List[bytes], namespace=None, timeout=None
    ) -> Future[Dict[bytes, bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_keys = [key for key in keys]
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVMultiGet(
                    ns, c_keys, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_put(
        self, c_string key, c_string value, c_bool overwrite=False, namespace=None,
        timeout=None
    ) -> Future[int]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVPut(
                    ns, key, value, overwrite, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_del(self, c_string key, c_bool del_by_prefix,
                              namespace=None, timeout=None) -> Future[int]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVDel(
                    ns, key, del_by_prefix, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_keys(self, c_string prefix, namespace=None, timeout=None
                               ) -> Future[List[bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVKeys(
                    ns, prefix, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_exists(self, c_string key, namespace=None, timeout=None
                                 ) -> Future[bool]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVExists(
                    ns, key, timeout_ms, cy_callback))
        return fut

    #############################################################
    # NodeInfo methods
    #############################################################
    def check_alive(
        self, node_ips: List[bytes], timeout: Optional[float] = None
    ) -> List[bool]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_node_ips = [ip for ip in node_ips]
            c_vector[c_bool] results
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Nodes().CheckAlive(c_node_ips, timeout_ms, results)
            )
        return [result for result in results]

    def async_check_alive(
        self, node_ips: List[bytes], timeout: Optional[float] = None
    ) -> Future[List[bool]]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_node_ips = [ip for ip in node_ips]
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyMultiItemCallback[BoolConverter] cy_callback = \
            PyMultiItemCallback[BoolConverter](cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Nodes().AsyncCheckAlive(
                    c_node_ips, timeout_ms, cy_callback))
        return fut

    def drain_nodes(
        self, node_ids: List[bytes], timeout: Optional[float] = None
    ) -> List[bytes]:
        """returns a list of node_ids that are successfully drained."""
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[CNodeID] c_node_ids
            c_vector[c_string] results
        for node_id in node_ids:
            c_node_ids.push_back(CNodeID.FromBinary(node_id))
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Nodes().DrainNodes(c_node_ids, timeout_ms, results)
            )
        return [result for result in results]

    def get_all_node_info(
        self, timeout: Optional[float] = None
    ) -> Dict[NodeID, gcs_pb2.GcsNodeInfo]:
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        cdef c_vector[CGcsNodeInfo] reply
        cdef c_vector[c_string] serialized_reply
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Nodes().GetAllNoCache(timeout_ms, reply)
            )
            for node in reply:
                serialized_reply.push_back(node.SerializeAsString())
        ret = {}
        for serialized in serialized_reply:
            proto = gcs_pb2.GcsNodeInfo()
            proto.ParseFromString(serialized)
            ret[NodeID.from_binary(proto.node_id)] = proto
        return ret

    #############################################################
    # NodeResources methods
    #############################################################
    def get_all_resource_usage(
        self, timeout: Optional[float] = None
    ) -> GetAllResourceUsageReply:
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        cdef CGetAllResourceUsageReply c_reply
        cdef c_string serialized_reply
        with nogil:
            check_status_timeout_as_rpc_error(
                    self.inner.get()
                    .NodeResources()
                    .GetAllResourceUsage(timeout_ms, c_reply)
                )
            serialized_reply = c_reply.SerializeAsString()
        ret = GetAllResourceUsageReply()
        ret.ParseFromString(serialized_reply)
        return ret

    #############################################################
    # Job methods
    #############################################################
    def get_all_job_info(
        self, timeout: Optional[float] = None
    ) -> Dict[JobID, gcs_pb2.JobTableData]:
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        cdef c_vector[CJobTableData] reply
        cdef c_vector[c_string] serialized_reply
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Jobs().GetAll(reply, timeout_ms)
            )
            for i in range(reply.size()):
                serialized_reply.push_back(reply[i].SerializeAsString())
        ret = {}
        for serialized in serialized_reply:
            proto = gcs_pb2.JobTableData()
            proto.ParseFromString(serialized)
            ret[JobID.from_binary(proto.job_id)] = proto
        return ret

    def async_get_all_job_info(
        self, timeout: Optional[float] = None
    ) -> Future[Dict[str, gcs_pb2.JobTableData]]:
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1

        def postprocess(binary):
            list_of_bytes: List[bytes] = check_status_or_return(binary)
            job_table_data = {}
            for b in list_of_bytes:
                proto = gcs_pb2.JobTableData()
                proto.ParseFromString(b)
                job_table_data[proto.job_id] = proto
            return job_table_data
        fut, cb = make_future_and_callback(postprocess=postprocess)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Jobs().AsyncGetAll(cy_callback, timeout_ms))
        return fut

    #############################################################
    # Runtime Env methods
    #############################################################
    def pin_runtime_env_uri(self, str uri, int expiration_s, timeout=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_string c_uri = uri.encode()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get()
                .RuntimeEnvs()
                .PinRuntimeEnvUri(c_uri, expiration_s, timeout_ms)
            )

    #############################################################
    # Autoscaler methods
    #############################################################
    def request_cluster_resource_constraint(
            self,
            bundles: c_vector[unordered_map[c_string, double]],
            count_array: c_vector[int64_t],
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get()
                .Autoscaler()
                .RequestClusterResourceConstraint(timeout_ms, bundles, count_array)
            )

    def get_cluster_resource_state(
            self,
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
            c_string serialized_reply
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get()
                .Autoscaler()
                .GetClusterResourceState(timeout_ms, serialized_reply)
            )

        return serialized_reply

    def get_cluster_status(
            self,
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
            c_string serialized_reply
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get()
                .Autoscaler()
                .GetClusterStatus(timeout_ms, serialized_reply)
            )

        return serialized_reply

    def report_autoscaling_state(
        self,
        serialzied_state: c_string,
        timeout_s=None
    ):
        """Report autoscaling state to GCS"""
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get()
                .Autoscaler()
                .ReportAutoscalingState(timeout_ms, serialzied_state)
            )

    def drain_node(
            self,
            node_id: c_string,
            reason: int32_t,
            reason_message: c_string,
            deadline_timestamp_ms: int64_t):
        """Send the DrainNode request to GCS.

        This is only for testing.
        """
        cdef:
            int64_t timeout_ms = -1
            c_bool is_accepted = False
            c_string rejection_reason_message
        with nogil:
            check_status_timeout_as_rpc_error(self.inner.get().Autoscaler().DrainNode(
                node_id, reason, reason_message,
                deadline_timestamp_ms, timeout_ms, is_accepted,
                rejection_reason_message))

        return (is_accepted, rejection_reason_message.decode())


# Util functions for async handling

# Ideally we want to pass CRayStatus around. However it's not easy to wrap a
# `ray::Status` to a `PythonObject*` so we marshall it to a 3-tuple like this. It can be
# unmarshalled to CRayStatus with `to_c_ray_status`.
StatusParts = Tuple[int, str, int]

cdef CRayStatus to_c_ray_status(tup: StatusParts):
    cdef:
        uint8_t code = <uint8_t>tup[0]
        CStatusCode status_code = <CStatusCode>(code)
        c_string msg = tup[1]
        int rpc_code = tup[2]
        CRayStatus s
    if status_code == CStatusCode_OK:
        return CRayStatus.OK()
    s = CRayStatus(status_code, msg, rpc_code)
    return s


def check_status_parts(parts: StatusParts):
    check_status_timeout_as_rpc_error(to_c_ray_status(parts))


def check_status_or_return(tup: Tuple[StatusParts, Any]):
    status_parts, val = tup
    check_status_parts(status_parts)
    return val


cdef make_future_and_callback(postprocess=None):
    """
    Prepares a series of async call and returns (future, callback).
    In runtime it's in this order:
    - Async API invoked.
        - if it returns non-OK, the async call raises.
    - Async API invokes `callback`, in the C++ thread
    - `callback` sends the result to the event loop thread and fulfill `fut`.
    - `run_postprocess` awaits `fut`, invokes `postprocess` and fulfill `fut2`.
    - `fut2` is what we return to the user.

    Params:
        `postprocess` is a sync function that returns transformed value, may raise.
    """
    loop = asyncio.get_event_loop()
    fut = loop.create_future()

    def callback(result, exc):
        # May run in in C++ thread
        if fut.cancelled():
            return
        if exc is not None:
            loop.call_soon_threadsafe(fut.set_exception, exc)
        else:
            loop.call_soon_threadsafe(fut.set_result, result)

    async def run_postprocess(fut, postprocess):
        result = await fut
        if postprocess is None:
            return result
        else:
            return postprocess(result)

    return run_postprocess(fut, postprocess), callback
