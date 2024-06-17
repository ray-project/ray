# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

"""
An alternative GcsClient API that is a direct binding from the C++ GcsClient. In
`ray._raylet.GcsClient`, we have a binding that is purpose built for Python via the
C++ `PythonGcsClient`. This introduces a duplicate code path that has slightly different
semantics. Also it's sync-only and the async is only supported via ThreadPoolExecutor.

This is a refactor to remove the Python-specific bindings and directly bind to the C++
GcsClient. It it natively async and has the same semantics as the C++ GcsClient.

## Progress
- [x] InternalKV sync methods
- [x] InternalKV async methods
- [x] InternalKV timeout_ms argument (https://github.com/ray-project/ray/pull/45444)
- [x] InternalKV multi-get (https://github.com/ray-project/ray/pull/45444)
- [x] InternalKV().Del() return num_deleted (https://github.com/ray-project/ray/pull/45451)
- [x] GcsClient ctor standalone from Ray CoreWorker
(here is when we can merge this PR)
- [x] Nodes().CheckAlive (https://github.com/ray-project/ray/pull/45451)
- [ ] RuntimeEnvGcsService::PinRuntimeEnvURI
- [ ] GetAllNodeInfo
- [ ] GetAllResourceUsage
- [ ] Autoscaler APIs

## Roadmap

1. Implement InternalKV sync and async methods to feature parity with PythonGcsClient.
2. Redirect GcsAioClient callsites to MyGcsClient. (with a good rename)
3. Redirect GcsClient callsites to MyGcsClient. This involves some refactoring on retry
    logic since we don't have `retry` in the new API.
4. Implement the rest of the GcsClient methods, redirect callsites.
5. Remove PythonGcsClient.

## Issues

There are many nuanced API differences between the PythonGcsClient and the C++ GcsClient.
In this PR I randomly "fixed" some of the issues, but we may really wanna keep the interface
consistent with older versions so we must revisit:

- timeout: PythonGcsClient returns RpcError, C++ GcsClient returns TimedOut -> GetTimeoutError
- get not found: PythonGcsClient returns KeyError, C++ GcsClient returns NotFound
- C++ GcsClient Del() had arg del_by_prefix but did not really support it (bug)
- maybe more
- bad test infra in stop_gcs_server

## Notes

### Async API

One challenge is that the C++ async API is callback-based, and the callbacks are invoked in
the C++ threads. We need to send the result back to the event loop thread to fulfill the
future. This is done by `make_future_and_callback`.

### Marshalling

The C++ API returns ints, strings, `ray::Status` and C++ protobuf types. On Python side:

- bools, ints and strings are easy to convert.
- `ray::Status`, we marshall it to a 3-tuple and unmarshall it back to `CRayStatus` via
    `to_c_ray_status`.
- For C++ protobuf types, we by default serialize them to bytes and deserialize them
    back in the postprocess function. It's possible to omit the protobuf serialization
    by providing a custom Converter in PyCallback.

"""

# This file is a .pxi which is included in _raylet.pyx. This means any already-imported
# symbols can directly be used here. This is not ideal, but we can't easily split this
# out to a separate translation unit because we need to access the singleton
# CCoreWorkerProcess.
#
# We need to best-effort import everything we need.

from asyncio import Future
from typing import List
from ray.includes.common cimport (
    CGcsClient,
    CGetAllResourceUsageReply,
    ConnectToGcsStandalone,
    PyDefaultCallback,
    PyMultiItemCallback,
    BoolConverter,
)
from ray.core.generated import gcs_pb2
from cython.operator import dereference, postincrement

cdef class MyGcsClient:
    cdef:
        shared_ptr[CGcsClient] inner

    @staticmethod
    def from_core_worker() -> "MyGcsClient":
        worker = ray._private.worker.global_worker
        worker.check_connected()

        cdef shared_ptr[CGcsClient] inner = CCoreWorkerProcess.GetCoreWorker().GetGcsClient()
        my = MyGcsClient()
        my.inner = inner
        return my


    # Creates and connects a standalone GcsClient.
    # cluster_id is in hex, if any.
    @staticmethod
    def standalone(gcs_address: str, cluster_id: str = None) -> "MyGcsClient":
        cdef GcsClientOptions gcs_options = GcsClientOptions.from_gcs_address(gcs_address)
        cdef CClusterID c_cluster_id = CClusterID.Nil() if cluster_id is None else CClusterID.FromHex(cluster_id)
        cdef shared_ptr[CGcsClient] inner = ConnectToGcsStandalone(dereference(gcs_options.native()), c_cluster_id)
        my = MyGcsClient()
        my.inner = inner
        return my

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
    def internal_kv_get(self, c_string key, namespace=None, timeout=None) -> Optional[bytes]:
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
            check_status(status)
            return value

    def internal_kv_multi_get(self, keys: List[bytes], namespace=None, timeout=None) -> Dict[bytes,bytes]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_keys = [key for key in keys]
            unordered_map[c_string, c_string] values
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().MultiGet(ns, c_keys, timeout_ms, values)

        check_status(status)

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
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Put(ns, key, value, overwrite, timeout_ms, added)
        check_status(status)
        return 1 if added else 0

    def internal_kv_del(self, c_string key, c_bool del_by_prefix,
                        namespace=None, timeout=None) -> int:
        """
        Returns number of keys deleted.
        """
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            CRayStatus status
            int num_deleted = 0
        with nogil:
            status = self.inner.get().InternalKV().Del(ns, key, del_by_prefix, timeout_ms, num_deleted)
        check_status(status)
        return num_deleted

    def internal_kv_keys(self, c_string prefix, namespace=None, timeout=None) -> List[bytes]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] keys
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Keys(ns, prefix, timeout_ms, keys)
        check_status(status)

        result = [key for key in keys]
        return result

    def internal_kv_exists(self, c_string key, namespace=None, timeout=None) -> bool:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_bool exists = False
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Exists(ns, key, timeout_ms, exists)
        check_status(status)
        return exists

    #############################################################
    # Internal KV async methods
    #############################################################

    def async_internal_kv_get(self, c_string key, namespace=None, timeout=None) -> Future[Optional[bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        def postprocess(tup: Tuple[StatusParts, Any]):
            status_parts, val = tup
            cdef CRayStatus c_ray_status = to_c_ray_status(status_parts)
            if c_ray_status.IsNotFound():
                return None
            check_status(c_ray_status)
            return val
        fut, cb = make_future_and_callback(postprocess=postprocess)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVGet(ns, key, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_multi_get(self, keys: List[bytes], namespace=None, timeout=None) -> Future[Dict[bytes,bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_keys = [key for key in keys]
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVMultiGet(ns, c_keys, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_put(self, c_string key, c_string value, c_bool overwrite=False,
                        namespace=None, timeout=None) -> Future[int]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVPut(ns, key, value, overwrite, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_del(self, c_string key, c_bool del_by_prefix,
                        namespace=None, timeout=None) -> Future[int]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVDel(ns, key, del_by_prefix, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_keys(self, c_string prefix, namespace=None, timeout=None) -> Future[List[bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVKeys(ns, prefix, timeout_ms, cy_callback))
        return fut

    def async_internal_kv_exists(self, c_string key, namespace=None, timeout=None) -> Future[bool]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyDefaultCallback cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVExists(ns, key, timeout_ms, cy_callback))
        return fut

    #############################################################
    # NodeInfo methods
    #############################################################

    def check_alive(self, node_ips: List[bytes], timeout: Optional[float] = None) -> List[bool]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_node_ips = [ip for ip in node_ips]
            c_vector[c_bool] results
            CRayStatus status
        with nogil:
            status = self.inner.get().Nodes().CheckAlive(c_node_ips, timeout_ms, results)
        check_status(status)
        return [result for result in results]

    def async_check_alive(
        self, node_ips: List[bytes], timeout: Optional[float] = None
    ) -> Future[List[bool]]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_node_ips = [ip for ip in node_ips]
        fut, cb = make_future_and_callback(postprocess=check_status_or_return)
        cdef PyMultiItemCallback[BoolConverter] cy_callback = PyMultiItemCallback[BoolConverter](cb)
        with nogil:
            check_status(self.inner.get().Nodes().AsyncCheckAlive(c_node_ips, timeout_ms, cy_callback))
        return fut

    def drain_nodes(self, node_ids: List[bytes], timeout: Optional[float] = None) -> List[bytes]:
        """returns a list of node_ids that are successfully drained."""
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[CNodeID] c_node_ids
            c_vector[c_string] results
        for node_id in node_ids:
            c_node_ids.push_back(CNodeID.FromBinary(node_id))
        with nogil:
            check_status(self.inner.get().Nodes().DrainNodes(c_node_ids, timeout_ms, results))
        return [result for result in results]

    #############################################################
    # NodeResources methods
    #############################################################
    def get_all_resource_usage(self, timeout: Optional[float] = None) -> gcs_pb2.GetAllResourceUsageReply:
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        cdef CGetAllResourceUsageReply c_reply
        cdef c_string serialized_reply
        with nogil:
            check_status(self.inner.get().NodeResources().GetAllResourceUsage(timeout_ms, c_reply))
            serialized_reply = c_reply.SerializeAsString()
        ret = gcs_pb2.GetAllResourceUsageReply()
        ret.ParseFromString(serialized_reply)
        return ret

    #############################################################
    # Job methods
    #############################################################

    def async_get_next_job_id(self) -> Future[JobID]:
        cdef PyDefaultCallback cy_callback
        fut, cb = make_future_and_callback(postprocess=lambda binary: JobID(binary))
        cy_callback = PyDefaultCallback(cb)
        with nogil:
            check_status(self.inner.get().Jobs().AsyncGetNextJobID(cy_callback))
        return fut

    def async_get_all_job_info(self, timeout: Optional[float] = None) -> Future[Dict[str, gcs_pb2.JobTableData]]:
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
            check_status(self.inner.get().Jobs().AsyncGetAll(timeout_ms, cy_callback))
        return fut

    def get_all_job_info(self, timeout: Optional[float] = None) -> Dict[str, gcs_pb2.JobTableData]:
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        cdef c_vector[CJobTableData] reply
        cdef c_vector[c_string] serialized_reply
        with nogil:
            check_status(self.inner.get().Jobs().GetAll(timeout_ms, reply))
            for i in range(reply.size()):
                serialized_reply.push_back(reply[i].SerializeAsString())
        ret = {}
        for serialized in serialized_reply:
            proto = gcs_pb2.JobTableData()
            proto.ParseFromString(serialized)
            ret[proto.job_id] = proto
        return ret

    #############################################################
    # Autoscaler sync methods
    #############################################################


    def request_cluster_resource_constraint(
            self,
            bundles: c_vector[unordered_map[c_string, double]],
            count_array: c_vector[int64_t],
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
        with nogil:
            check_status(self.inner.get().Autoscaler().RequestClusterResourceConstraint(
                timeout_ms, bundles, count_array))


    def get_cluster_resource_state(
            self,
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
            c_string serialized_reply
        with nogil:
            check_status(self.inner.get().Autoscaler().GetClusterResourceState(timeout_ms,
                         serialized_reply))

        return serialized_reply


    def get_cluster_status(
            self,
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
            c_string serialized_reply
        with nogil:
            check_status(self.inner.get().Autoscaler().GetClusterStatus(timeout_ms,
                         serialized_reply))

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
            check_status(self.inner.get().Autoscaler().ReportAutoscalingState(
                timeout_ms, serialzied_state))


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
            check_status(self.inner.get().Autoscaler().DrainNode(
                node_id, reason, reason_message,
                deadline_timestamp_ms, timeout_ms, is_accepted,
                rejection_reason_message))

        return (is_accepted, rejection_reason_message.decode())

# Ideally we want to pass CRayStatus around. However it's not easy to wrap a
# `ray::Status` to a `PythonObject*` so we marshall it to a 3-tuple like this. It can be
# unmarshalled to CRayStatus with `to_c_ray_status`.
StatusParts = Tuple[int, str, int]

cdef CRayStatus to_c_ray_status(tup: StatusParts):
    cdef:
        uint8_t code = <uint8_t>tup[0]
        StatusCode status_code = <StatusCode>(code)
        c_string msg = tup[1]
        int rpc_code = tup[2]
        CRayStatus s
    if status_code == StatusCode_OK:
        return CRayStatus.OK()
    s = CRayStatus(status_code, msg, rpc_code)
    return s


def check_status_parts(parts: StatusParts):
    check_status(to_c_ray_status(parts))


def check_status_or_return(tup: Tuple[StatusParts, Any]):
    status_parts, val = tup
    check_status_parts(status_parts)
    return val

cdef make_future_and_callback(postprocess = None):
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
