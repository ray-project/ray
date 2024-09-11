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
# For how async API are implemented, see src/ray/gcs/gcs_client/python_callbacks.h
from asyncio import Future
from typing import List
from libcpp.utility cimport move
import concurrent.futures
from ray.includes.common cimport (
    CGcsClient,
    CGetAllResourceUsageReply,
    ConnectOnSingletonIoContext,
    CStatusCode,
    CStatusCode_OK,
    MultiItemPyCallback,
    OptionalItemPyCallback,
    StatusPyCallback,
)
from ray.includes.optional cimport optional, make_optional
from ray.core.generated import gcs_pb2
from cython.operator import dereference, postincrement
cimport cpython


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
            optional[c_string] opt_value = c_string()
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Get(
                ns, key, timeout_ms, opt_value.value())
        return raise_or_return(
            convert_optional_str_none_for_not_found(status, move(opt_value)))

    def internal_kv_multi_get(
        self, keys: List[bytes], namespace=None, timeout=None
    ) -> Dict[bytes, bytes]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_keys = [key for key in keys]
            optional[unordered_map[c_string, c_string]] opt_values = \
                unordered_map[c_string, c_string]()
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().MultiGet(
                ns, c_keys, timeout_ms, opt_values.value())
        return raise_or_return(convert_optional_multi_get(status, move(opt_values)))

    def internal_kv_put(self, c_string key, c_string value, c_bool overwrite=False,
                        namespace=None, timeout=None) -> int:
        """
        Returns 1 if the key is newly added, 0 if the key is overwritten.
        """
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            optional[c_bool] opt_added = 0
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Put(
                ns, key, value, overwrite, timeout_ms, opt_added.value())
        added = raise_or_return(convert_optional_bool(status, move(opt_added)))
        return 1 if added else 0

    def internal_kv_del(self, c_string key, c_bool del_by_prefix,
                        namespace=None, timeout=None) -> int:
        """
        Returns number of keys deleted.
        """
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            optional[int] opt_num_deleted = 0
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Del(
                ns, key, del_by_prefix, timeout_ms, opt_num_deleted.value())
        return raise_or_return(convert_optional_int(status, move(opt_num_deleted)))

    def internal_kv_keys(
        self, c_string prefix, namespace=None, timeout=None
    ) -> List[bytes]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            optional[c_vector[c_string]] opt_keys = c_vector[c_string]()
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Keys(
                ns, prefix, timeout_ms, opt_keys.value())
        return raise_or_return(convert_optional_vector_str(status, move(opt_keys)))

    def internal_kv_exists(self, c_string key, namespace=None, timeout=None) -> bool:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            optional[c_bool] opt_exists = 0
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Exists(
                ns, key, timeout_ms, opt_exists.value())
        return raise_or_return(convert_optional_bool(status, move(opt_exists)))

    #############################################################
    # Internal KV async methods
    #############################################################

    def async_internal_kv_get(
        self, c_string key, namespace=None, timeout=None
    ) -> Future[Optional[bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVGet(
                    ns, key, timeout_ms,
                    OptionalItemPyCallback[c_string](
                        &convert_optional_str_none_for_not_found,
                        assign_and_decrement_fut,
                        fut)))
        return asyncio.wrap_future(fut)

    def async_internal_kv_multi_get(
        self, keys: List[bytes], namespace=None, timeout=None
    ) -> Future[Dict[bytes, bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_keys = [key for key in keys]
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVMultiGet(
                    ns, c_keys, timeout_ms,
                    OptionalItemPyCallback[unordered_map[c_string, c_string]](
                        &convert_optional_multi_get,
                        assign_and_decrement_fut,
                        fut)))
        return asyncio.wrap_future(fut)

    def async_internal_kv_put(
        self, c_string key, c_string value, c_bool overwrite=False, namespace=None,
        timeout=None
    ) -> Future[int]:
        # TODO(ryw): the sync `internal_kv_put` returns bool while this async version
        # returns int. We should make them consistent.
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVPut(
                    ns, key, value, overwrite, timeout_ms,
                    OptionalItemPyCallback[int](
                        &convert_optional_int,
                        assign_and_decrement_fut,
                        fut)))
        return asyncio.wrap_future(fut)

    def async_internal_kv_del(self, c_string key, c_bool del_by_prefix,
                              namespace=None, timeout=None) -> Future[int]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVDel(
                    ns, key, del_by_prefix, timeout_ms,
                    OptionalItemPyCallback[int](
                        &convert_optional_int,
                        assign_and_decrement_fut,
                        fut)))
        return asyncio.wrap_future(fut)

    def async_internal_kv_keys(self, c_string prefix, namespace=None, timeout=None
                               ) -> Future[List[bytes]]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVKeys(
                    ns, prefix, timeout_ms,
                    OptionalItemPyCallback[c_vector[c_string]](
                        &convert_optional_vector_str,
                        assign_and_decrement_fut,
                        fut)))
        return asyncio.wrap_future(fut)

    def async_internal_kv_exists(self, c_string key, namespace=None, timeout=None
                                 ) -> Future[bool]:
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().InternalKV().AsyncInternalKVExists(
                    ns, key, timeout_ms,
                    OptionalItemPyCallback[c_bool](
                        &convert_optional_bool,
                        assign_and_decrement_fut,
                        fut)))
        return asyncio.wrap_future(fut)

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
            CRayStatus status
        with nogil:
            status = self.inner.get().Nodes().CheckAlive(
                c_node_ips, timeout_ms, results)
        return raise_or_return(convert_multi_bool(status, move(results)))

    def async_check_alive(
        self, node_ips: List[bytes], timeout: Optional[float] = None
    ) -> Future[List[bool]]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_node_ips = [ip for ip in node_ips]
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Nodes().AsyncCheckAlive(
                    c_node_ips, timeout_ms,
                    MultiItemPyCallback[c_bool](
                        &convert_multi_bool,
                        assign_and_decrement_fut,
                        fut)))
        return asyncio.wrap_future(fut)

    def drain_nodes(
        self, node_ids: List[bytes], timeout: Optional[float] = None
    ) -> List[bytes]:
        """returns a list of node_ids that are successfully drained."""
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[CNodeID] c_node_ids
            c_vector[c_string] results
            CRayStatus status
        for node_id in node_ids:
            c_node_ids.push_back(CNodeID.FromBinary(node_id))
        with nogil:
            status = self.inner.get().Nodes().DrainNodes(
                c_node_ids, timeout_ms, results)
        return raise_or_return(convert_multi_str(status, move(results)))

    def get_all_node_info(
        self, timeout: Optional[float] = None
    ) -> Dict[NodeID, gcs_pb2.GcsNodeInfo]:
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        cdef c_vector[CGcsNodeInfo] reply
        cdef CRayStatus status
        with nogil:
            status = self.inner.get().Nodes().GetAllNoCache(timeout_ms, reply)
        return raise_or_return(convert_get_all_node_info(status, move(reply)))

    def async_get_all_node_info(
        self, timeout: Optional[float] = None
    ) -> Future[Dict[NodeID, gcs_pb2.GcsNodeInfo]]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            fut = incremented_fut()
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Nodes().AsyncGetAll(
                    MultiItemPyCallback[CGcsNodeInfo](
                        convert_get_all_node_info,
                        assign_and_decrement_fut,
                        fut),
                    timeout_ms))
        return asyncio.wrap_future(fut)

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
    # Actor methods
    #############################################################

    def async_get_all_actor_info(
        self,
        actor_id: Optional[ActorID] = None,
        job_id: Optional[JobID] = None,
        actor_state_name: Optional[str] = None,
        timeout: Optional[float] = None
    ) -> Future[Dict[ActorID, gcs_pb2.ActorTableData]]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            optional[CActorID] c_actor_id
            optional[CJobID] c_job_id
            optional[c_string] c_actor_state_name
            fut = incremented_fut()
        if actor_id is not None:
            c_actor_id = (<ActorID>actor_id).native()
        if job_id is not None:
            c_job_id = (<JobID>job_id).native()
        if actor_state_name is not None:
            c_actor_state_name = <c_string>actor_state_name.encode()

        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Actors().AsyncGetAllByFilter(
                    c_actor_id, c_job_id, c_actor_state_name,
                    MultiItemPyCallback[CActorTableData](
                        &convert_get_all_actor_info,
                        assign_and_decrement_fut,
                        fut),
                    timeout_ms))
        return asyncio.wrap_future(fut)

    def async_kill_actor(
        self, actor_id: ActorID, c_bool force_kill, c_bool no_restart,
        timeout: Optional[float] = None
    ) -> ConcurrentFuture[None]:
        """
        On success: returns None.
        On failure: raises an exception.
        """
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            fut = incremented_fut()
            CActorID c_actor_id = actor_id.native()

        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Actors().AsyncKillActor(
                    c_actor_id,
                    force_kill,
                    no_restart,
                    StatusPyCallback(convert_status, assign_and_decrement_fut,  fut),
                    timeout_ms
                )
            )
        return asyncio.wrap_future(fut)
    #############################################################
    # Job methods
    #############################################################

    def get_all_job_info(
        self, *, job_or_submission_id: Optional[str] = None,
        skip_submission_job_info_field: bool = False,
        skip_is_running_tasks_field: bool = False,
        timeout: Optional[float] = None
    ) -> Dict[JobID, gcs_pb2.JobTableData]:
        cdef c_string c_job_or_submission_id
        cdef optional[c_string] c_optional_job_or_submission_id = nullopt
        cdef int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        cdef c_bool c_skip_submission_job_info_field = skip_submission_job_info_field
        cdef c_bool c_skip_is_running_tasks_field = skip_is_running_tasks_field
        cdef CRayStatus status
        cdef c_vector[CJobTableData] reply
        if job_or_submission_id:
            c_job_or_submission_id = job_or_submission_id
            c_optional_job_or_submission_id = \
                make_optional[c_string](c_job_or_submission_id)
        with nogil:
            status = self.inner.get().Jobs().GetAll(
                c_optional_job_or_submission_id, c_skip_submission_job_info_field,
                c_skip_is_running_tasks_field, reply, timeout_ms)
        return raise_or_return((convert_get_all_job_info(status, move(reply))))

    def async_get_all_job_info(
        self, *, job_or_submission_id: Optional[str] = None,
        skip_submission_job_info_field: bool = False,
        skip_is_running_tasks_field: bool = False,
        timeout: Optional[float] = None
    ) -> Future[Dict[JobID, gcs_pb2.JobTableData]]:
        cdef:
            c_string c_job_or_submission_id
            optional[c_string] c_optional_job_or_submission_id = nullopt
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_bool c_skip_submission_job_info_field = skip_submission_job_info_field
            c_bool c_skip_is_running_tasks_field = skip_is_running_tasks_field
            fut = incremented_fut()
        if job_or_submission_id:
            c_job_or_submission_id = job_or_submission_id
            c_optional_job_or_submission_id = \
                make_optional[c_string](c_job_or_submission_id)
        with nogil:
            check_status_timeout_as_rpc_error(
                self.inner.get().Jobs().AsyncGetAll(
                    c_optional_job_or_submission_id,
                    c_skip_submission_job_info_field,
                    c_skip_is_running_tasks_field,
                    MultiItemPyCallback[CJobTableData](
                        &convert_get_all_job_info,
                        assign_and_decrement_fut,
                        fut),
                    timeout_ms))
        return asyncio.wrap_future(fut)

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

cdef incremented_fut():
    fut = concurrent.futures.Future()
    cpython.Py_INCREF(fut)
    return fut

cdef void assign_and_decrement_fut(result, fut) with gil:
    assert isinstance(fut, concurrent.futures.Future)

    assert not fut.done()
    try:
        ret, exc = result
        if exc:
            fut.set_exception(exc)
        else:
            fut.set_result(ret)
    finally:
        # We INCREFed it in `incremented_fut` to keep it alive during the async wait,
        # and we DECREF it here to balance it.
        cpython.Py_DECREF(fut)

cdef raise_or_return(tup):
    ret, exc = tup
    if exc:
        raise exc
    return ret

#############################################################
# Converter functions: C++ types -> Python types, use by both Sync and Async APIs.
# They have to be defined here as pure functions because a function pointer is passed
# to C++ for Async APIs.
#
# Each function accepts what the C++ callback passes, typically a Status and a value.
# Returns `Tuple[object, Optional[Exception]]` (we are all gophers now lol).
# Must not raise exceptions, or it crashes the process.
#############################################################

cdef convert_get_all_node_info(
        CRayStatus status, c_vector[CGcsNodeInfo]&& c_data) with gil:
    # -> Dict[NodeID, gcs_pb2.GcsNodeInfo]
    # No GIL block for C++ looping && serialization.
    # GIL block for Pyhton deserialization and dict building.
    # Not doing per-object GIL lock because it's expensive.
    cdef c_vector[c_string] serialized_reply
    try:
        check_status_timeout_as_rpc_error(status)
        with nogil:
            serialized_reply.reserve(c_data.size())
            for c_proto in c_data:
                serialized_reply.push_back(c_proto.SerializeAsString())
        node_table_data = {}
        for b in serialized_reply:
            proto = gcs_pb2.GcsNodeInfo()
            proto.ParseFromString(b)
            node_table_data[NodeID.from_binary(proto.node_id)] = proto
        return node_table_data, None
    except Exception as e:
        return None, e

cdef convert_get_all_job_info(
        CRayStatus status, c_vector[CJobTableData]&& c_data) with gil:
    # -> Dict[JobID, gcs_pb2.JobTableData]
    # No GIL block for C++ looping && serialization.
    # GIL block for Pyhton deserialization and dict building.
    # Not doing per-object GIL lock because it's expensive.
    cdef c_vector[c_string] serialized_reply
    try:
        check_status_timeout_as_rpc_error(status)
        with nogil:
            serialized_reply.reserve(c_data.size())
            for c_proto in c_data:
                serialized_reply.push_back(c_proto.SerializeAsString())
        job_table_data = {}
        for b in serialized_reply:
            proto = gcs_pb2.JobTableData()
            proto.ParseFromString(b)
            job_table_data[JobID.from_binary(proto.job_id)] = proto
        return job_table_data, None
    except Exception as e:
        return None, e

cdef convert_get_all_actor_info(
        CRayStatus status, c_vector[CActorTableData]&& c_data) with gil:
    # -> Dict[ActorID, gcs_pb2.ActorTableData]
    cdef c_vector[c_string] serialized_reply
    try:
        check_status_timeout_as_rpc_error(status)
        with nogil:
            serialized_reply.reserve(c_data.size())
            for c_proto in c_data:
                serialized_reply.push_back(c_proto.SerializeAsString())
        actor_table_data = {}
        for b in serialized_reply:
            proto = gcs_pb2.ActorTableData()
            proto.ParseFromString(b)
            actor_table_data[ActorID.from_binary(proto.actor_id)] = proto
        return actor_table_data, None
    except Exception as e:
        return None, e

cdef convert_status(CRayStatus status) with gil:
    # -> None
    try:
        check_status_timeout_as_rpc_error(status)
        return None, None
    except Exception as e:
        return None, e
cdef convert_optional_str_none_for_not_found(
        CRayStatus status, optional[c_string]&& c_str) with gil:
    # If status is NotFound, return None.
    # If status is OK, return the value.
    # Else, raise exception.
    # -> Optional[bytes]
    try:
        if status.IsNotFound():
            return None, None
        check_status_timeout_as_rpc_error(status)
        assert c_str.has_value()
        return move(c_str.value()), None
    except Exception as e:
        return None, e

cdef convert_optional_multi_get(
        CRayStatus status,
        optional[unordered_map[c_string, c_string]]&& c_map) with gil:
    # -> Dict[str, str]
    cdef unordered_map[c_string, c_string].iterator it
    try:
        check_status_timeout_as_rpc_error(status)
        assert c_map.has_value()

        result = {}
        it = c_map.value().begin()
        while it != c_map.value().end():
            result[dereference(it).first] = move(dereference(it).second)
            postincrement(it)
        return result, None
    except Exception as e:
        return None, e

cdef convert_optional_int(CRayStatus status, optional[int]&& c_int) with gil:
    # -> int
    try:
        check_status_timeout_as_rpc_error(status)
        assert c_int.has_value()
        return c_int.value(), None
    except Exception as e:
        return None, e

cdef convert_optional_vector_str(
        CRayStatus status, optional[c_vector[c_string]]&& c_vec) with gil:
    # -> List[bytes]
    try:
        check_status_timeout_as_rpc_error(status)
        return convert_multi_str(status, move(c_vec.value()))
    except Exception as e:
        return None, e

cdef convert_optional_bool(CRayStatus status, optional[c_bool]&& b) with gil:
    # -> bool
    try:
        check_status_timeout_as_rpc_error(status)
        assert b.has_value()
        return b.value(), None
    except Exception as e:
        return None, e

cdef convert_multi_bool(CRayStatus status, c_vector[c_bool]&& c_data) with gil:
    # -> List[bool]
    try:
        check_status_timeout_as_rpc_error(status)
        return [b for b in c_data], None
    except Exception as e:
        return None, e

cdef convert_multi_str(CRayStatus status, c_vector[c_string]&& vec) with gil:
    # -> List[bytes]
    cdef c_vector[c_string].iterator it
    try:
        check_status_timeout_as_rpc_error(status)

        it = vec.begin()
        result = []
        while it != vec.end():
            result.append(move(dereference(it)))
            postincrement(it)
        return result, None
    except Exception as e:
        return None, e
