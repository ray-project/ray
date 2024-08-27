"""This file includes the Worker class which sits on the client side.
It implements the Ray API functions that are forwarded through grpc calls
to the server.
"""
import base64
import json
import logging
import os
import tempfile
import threading
import time
import uuid
import warnings
from collections import defaultdict
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

import grpc

import ray._private.tls_utils
import ray.cloudpickle as cloudpickle
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray._private.ray_constants import DEFAULT_CLIENT_RECONNECT_GRACE_PERIOD
from ray._private.runtime_env.py_modules import upload_py_modules_if_needed
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed

# Use cloudpickle's version of pickle for UnpicklingError
from ray.cloudpickle.compat import pickle
from ray.exceptions import GetTimeoutError
from ray.job_config import JobConfig
from ray.util.client.client_pickler import dumps_from_client, loads_from_server
from ray.util.client.common import (
    GRPC_OPTIONS,
    GRPC_UNRECOVERABLE_ERRORS,
    INT32_MAX,
    OBJECT_TRANSFER_WARNING_SIZE,
    ClientActorClass,
    ClientActorHandle,
    ClientActorRef,
    ClientObjectRef,
    ClientRemoteFunc,
    ClientStub,
)
from ray.util.client.dataclient import DataClient
from ray.util.client.logsclient import LogstreamClient
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.actor import ActorClass
    from ray.remote_function import RemoteFunction

logger = logging.getLogger(__name__)

INITIAL_TIMEOUT_SEC = 5
MAX_TIMEOUT_SEC = 30

# The max amount of time an operation can run blocking in the server. This
# allows for Ctrl-C of the client to work without explicitly cancelling server
# operations.
MAX_BLOCKING_OPERATION_TIME_S: float = 2.0

# If the total size (bytes) of all outbound messages to schedule tasks since
# the connection began exceeds this value, a warning should be raised
MESSAGE_SIZE_THRESHOLD = 10 * 2**20  # 10 MB

# Links to the Ray Design Pattern doc to use in the task overhead warning
# message
DESIGN_PATTERN_FINE_GRAIN_TASKS_LINK = "https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit#heading=h.f7ins22n6nyl"  # noqa E501

DESIGN_PATTERN_LARGE_OBJECTS_LINK = "https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit#heading=h.1afmymq455wu"  # noqa E501


def backoff(timeout: int) -> int:
    timeout = timeout + 5
    if timeout > MAX_TIMEOUT_SEC:
        timeout = MAX_TIMEOUT_SEC
    return timeout


class Worker:
    def __init__(
        self,
        conn_str: str = "",
        secure: bool = False,
        metadata: List[Tuple[str, str]] = None,
        connection_retries: int = 3,
        _credentials: Optional[grpc.ChannelCredentials] = None,
    ):
        """Initializes the worker side grpc client.

        Args:
            conn_str: The host:port connection string for the ray server.
            secure: whether to use SSL secure channel or not.
            metadata: additional metadata passed in the grpc request headers.
            connection_retries: Number of times to attempt to reconnect to the
              ray server if it doesn't respond immediately. Setting to 0 tries
              at least once.  For infinite retries, catch the ConnectionError
              exception.
            _credentials: gprc channel credentials. Default ones will be used
              if None.
        """
        self._client_id = make_client_id()
        self.metadata = [("client_id", self._client_id)] + (
            metadata if metadata else []
        )
        self.channel = None
        self.server = None
        self._conn_state = grpc.ChannelConnectivity.IDLE
        self._converted: Dict[str, ClientStub] = {}
        self._secure = secure or os.environ.get("RAY_USE_TLS", "0").lower() in (
            "1",
            "true",
        )
        self._conn_str = conn_str
        self._connection_retries = connection_retries

        if _credentials is not None:
            self._credentials = _credentials
            self._secure = True
        else:
            self._credentials = None

        self._reconnect_grace_period = DEFAULT_CLIENT_RECONNECT_GRACE_PERIOD
        if "RAY_CLIENT_RECONNECT_GRACE_PERIOD" in os.environ:
            # Use value in environment variable if available
            self._reconnect_grace_period = int(
                os.environ["RAY_CLIENT_RECONNECT_GRACE_PERIOD"]
            )
        # Disable retries if grace period is set to 0
        self._reconnect_enabled = self._reconnect_grace_period != 0

        # Set to True when the connection cannot be recovered and reconnect
        # attempts should be stopped
        self._in_shutdown = False
        # Set to True after initial connection succeeds
        self._has_connected = False

        self._connect_channel()
        self._has_connected = True

        # Has Ray been initialized on the server?
        self._serverside_ray_initialized = False

        # Initialize the streams to finish protocol negotiation.
        self.data_client = DataClient(self, self._client_id, self.metadata)
        self.reference_count: Dict[bytes, int] = defaultdict(int)

        self.log_client = LogstreamClient(self, self.metadata)
        self.log_client.set_logstream_level(logging.INFO)

        self.closed = False

        # Track this value to raise a warning if a lot of data are transferred.
        self.total_outbound_message_size_bytes = 0

        # Used to create unique IDs for RPCs to the RayletServicer
        self._req_id_lock = threading.Lock()
        self._req_id = 0

    def _connect_channel(self, reconnecting=False) -> None:
        """
        Attempts to connect to the server specified by conn_str. If
        reconnecting after an RPC error, cleans up the old channel and
        continues to attempt to connect until the grace period is over.
        """
        if self.channel is not None:
            self.channel.unsubscribe(self._on_channel_state_change)
            self.channel.close()

        if self._secure:
            if self._credentials is not None:
                credentials = self._credentials
            elif os.environ.get("RAY_USE_TLS", "0").lower() in ("1", "true"):
                (
                    server_cert_chain,
                    private_key,
                    ca_cert,
                ) = ray._private.tls_utils.load_certs_from_env()
                credentials = grpc.ssl_channel_credentials(
                    certificate_chain=server_cert_chain,
                    private_key=private_key,
                    root_certificates=ca_cert,
                )
            else:
                credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(
                self._conn_str, credentials, options=GRPC_OPTIONS
            )
        else:
            self.channel = grpc.insecure_channel(self._conn_str, options=GRPC_OPTIONS)

        self.channel.subscribe(self._on_channel_state_change)

        # Retry the connection until the channel responds to something
        # looking like a gRPC connection, though it may be a proxy.
        start_time = time.time()
        conn_attempts = 0
        timeout = INITIAL_TIMEOUT_SEC
        service_ready = False
        while conn_attempts < max(self._connection_retries, 1) or reconnecting:
            conn_attempts += 1
            if self._in_shutdown:
                # User manually closed the worker before connection finished
                break
            elapsed_time = time.time() - start_time
            if reconnecting and elapsed_time > self._reconnect_grace_period:
                self._in_shutdown = True
                raise ConnectionError(
                    "Failed to reconnect within the reconnection grace period "
                    f"({self._reconnect_grace_period}s)"
                )
            try:
                # Let gRPC wait for us to see if the channel becomes ready.
                # If it throws, we couldn't connect.
                grpc.channel_ready_future(self.channel).result(timeout=timeout)
                # The HTTP2 channel is ready. Wrap the channel with the
                # RayletDriverStub, allowing for unary requests.
                self.server = ray_client_pb2_grpc.RayletDriverStub(self.channel)
                service_ready = bool(self.ping_server())
                if service_ready:
                    break
                # Ray is not ready yet, wait a timeout
                time.sleep(timeout)
            except grpc.FutureTimeoutError:
                logger.debug(f"Couldn't connect channel in {timeout} seconds, retrying")
                # Note that channel_ready_future constitutes its own timeout,
                # which is why we do not sleep here.
            except grpc.RpcError as e:
                logger.debug(
                    "Ray client server unavailable, " f"retrying in {timeout}s..."
                )
                logger.debug(f"Received when checking init: {e.details()}")
                # Ray is not ready yet, wait a timeout.
                time.sleep(timeout)
            # Fallthrough, backoff, and retry at the top of the loop
            logger.debug(
                "Waiting for Ray to become ready on the server, "
                f"retry in {timeout}s..."
            )
            if not reconnecting:
                # Don't increase backoff when trying to reconnect --
                # we already know the server exists, attempt to reconnect
                # as soon as we can
                timeout = backoff(timeout)

        # If we made it through the loop without service_ready
        # it means we've used up our retries and
        # should error back to the user.
        if not service_ready:
            self._in_shutdown = True
            if log_once("ray_client_security_groups"):
                warnings.warn(
                    "Ray Client connection timed out. Ensure that "
                    "the Ray Client port on the head node is reachable "
                    "from your local machine. See https://docs.ray.io/en"
                    "/latest/cluster/ray-client.html#step-2-check-ports for "
                    "more information."
                )
            raise ConnectionError("ray client connection timeout")

    def _can_reconnect(self, e: grpc.RpcError) -> bool:
        """
        Returns True if the RPC error can be recovered from and a retry is
        appropriate, false otherwise.
        """
        if not self._reconnect_enabled:
            return False
        if self._in_shutdown:
            # Channel is being shutdown, don't try to reconnect
            return False
        if e.code() in GRPC_UNRECOVERABLE_ERRORS:
            # Unrecoverable error -- These errors are specifically raised
            # by the server's application logic
            return False
        if e.code() == grpc.StatusCode.INTERNAL:
            details = e.details()
            if details == "Exception serializing request!":
                # The client failed tried to send a bad request (for example,
                # passing "None" instead of a valid grpc message). Don't
                # try to reconnect/retry.
                return False
        # All other errors can be treated as recoverable
        return True

    def _call_stub(self, stub_name: str, *args, **kwargs) -> Any:
        """
        Calls the stub specified by stub_name (Schedule, WaitObject, etc...).
        If a recoverable error occurrs while calling the stub, attempts to
        retry the RPC.
        """
        while not self._in_shutdown:
            try:
                return getattr(self.server, stub_name)(*args, **kwargs)
            except grpc.RpcError as e:
                if self._can_reconnect(e):
                    time.sleep(0.5)
                    continue
                raise
            except ValueError:
                # Trying to use the stub on a cancelled channel will raise
                # ValueError. This should only happen when the data client
                # is attempting to reset the connection -- sleep and try
                # again.
                time.sleep(0.5)
                continue
        raise ConnectionError("Client is shutting down.")

    def _get_object_iterator(
        self, req: ray_client_pb2.GetRequest, *args, **kwargs
    ) -> Any:
        """
        Calls the stub for GetObject on the underlying server stub. If a
        recoverable error occurs while streaming the response, attempts
        to retry the get starting from the first chunk that hasn't been
        received.
        """
        last_seen_chunk = -1
        while not self._in_shutdown:
            # If we disconnect partway through, restart the get request
            # at the first chunk we haven't seen
            req.start_chunk_id = last_seen_chunk + 1
            try:
                for chunk in self.server.GetObject(req, *args, **kwargs):
                    if chunk.chunk_id <= last_seen_chunk:
                        # Ignore repeat chunks
                        logger.debug(
                            f"Received a repeated chunk {chunk.chunk_id} "
                            f"from request {req.req_id}."
                        )
                        continue
                    if last_seen_chunk + 1 != chunk.chunk_id:
                        raise RuntimeError(
                            f"Received chunk {chunk.chunk_id} when we expected "
                            f"{self.last_seen_chunk + 1}"
                        )
                    last_seen_chunk = chunk.chunk_id
                    yield chunk
                    if last_seen_chunk == chunk.total_chunks - 1:
                        # We've yielded the last chunk, exit early
                        return
                return
            except grpc.RpcError as e:
                if self._can_reconnect(e):
                    time.sleep(0.5)
                    continue
                raise
            except ValueError:
                # Trying to use the stub on a cancelled channel will raise
                # ValueError. This should only happen when the data client
                # is attempting to reset the connection -- sleep and try
                # again.
                time.sleep(0.5)
                continue
        raise ConnectionError("Client is shutting down.")

    def _add_ids_to_metadata(self, metadata: Any):
        """
        Adds a unique req_id and the current thread's identifier to the
        metadata. These values are useful for preventing mutating operations
        from being replayed on the server side in the event that the client
        must retry a requsest.
        Args:
            metadata - the gRPC metadata to append the IDs to
        """
        if not self._reconnect_enabled:
            # IDs not needed if the reconnects are disabled
            return metadata
        thread_id = str(threading.get_ident())
        with self._req_id_lock:
            self._req_id += 1
            if self._req_id > INT32_MAX:
                self._req_id = 1
            req_id = str(self._req_id)
        return metadata + [("thread_id", thread_id), ("req_id", req_id)]

    def _on_channel_state_change(self, conn_state: grpc.ChannelConnectivity):
        logger.debug(f"client gRPC channel state change: {conn_state}")
        self._conn_state = conn_state

    def connection_info(self):
        try:
            data = self.data_client.ConnectionInfo()
        except grpc.RpcError as e:
            raise decode_exception(e)
        return {
            "num_clients": data.num_clients,
            "python_version": data.python_version,
            "ray_version": data.ray_version,
            "ray_commit": data.ray_commit,
        }

    def register_callback(
        self,
        ref: ClientObjectRef,
        callback: Callable[[ray_client_pb2.DataResponse], None],
    ) -> None:
        req = ray_client_pb2.GetRequest(ids=[ref.id], asynchronous=True)
        self.data_client.RegisterGetCallback(req, callback)

    def get(self, vals, *, timeout: Optional[float] = None) -> Any:
        if isinstance(vals, list):
            if not vals:
                return []
            to_get = vals
        elif isinstance(vals, ClientObjectRef):
            to_get = [vals]
        else:
            raise Exception(
                "Can't get something that's not a "
                "list of IDs or just an ID: %s" % type(vals)
            )

        if timeout is None:
            deadline = None
        else:
            deadline = time.monotonic() + timeout

        max_blocking_operation_time = MAX_BLOCKING_OPERATION_TIME_S
        if "RAY_CLIENT_MAX_BLOCKING_OPERATION_TIME_S" in os.environ:
            max_blocking_operation_time = float(
                os.environ["RAY_CLIENT_MAX_BLOCKING_OPERATION_TIME_S"]
            )
        while True:
            if deadline:
                op_timeout = min(
                    max_blocking_operation_time,
                    max(deadline - time.monotonic(), 0.001),
                )
            else:
                op_timeout = max_blocking_operation_time
            try:
                res = self._get(to_get, op_timeout)
                break
            except GetTimeoutError:
                if deadline and time.monotonic() > deadline:
                    raise
                logger.debug("Internal retry for get {}".format(to_get))
        if len(to_get) != len(res):
            raise Exception(
                "Mismatched number of items in request ({}) and response ({})".format(
                    len(to_get), len(res)
                )
            )
        if isinstance(vals, ClientObjectRef):
            res = res[0]
        return res

    def _get(self, ref: List[ClientObjectRef], timeout: float):
        req = ray_client_pb2.GetRequest(ids=[r.id for r in ref], timeout=timeout)
        data = bytearray()
        try:
            resp = self._get_object_iterator(req, metadata=self.metadata)
            for chunk in resp:
                if not chunk.valid:
                    try:
                        err = cloudpickle.loads(chunk.error)
                    except (pickle.UnpicklingError, TypeError):
                        logger.exception("Failed to deserialize {}".format(chunk.error))
                        raise
                    raise err
                if chunk.total_size > OBJECT_TRANSFER_WARNING_SIZE and log_once(
                    "client_object_transfer_size_warning"
                ):
                    size_gb = chunk.total_size / 2**30
                    warnings.warn(
                        "Ray Client is attempting to retrieve a "
                        f"{size_gb:.2f} GiB object over the network, which may "
                        "be slow. Consider serializing the object to a file "
                        "and using S3 or rsync instead.",
                        UserWarning,
                        stacklevel=5,
                    )
                data.extend(chunk.data)
        except grpc.RpcError as e:
            raise decode_exception(e)
        return loads_from_server(data)

    def put(
        self,
        val,
        *,
        client_ref_id: bytes = None,
        _owner: Optional[ClientActorHandle] = None,
    ):
        if isinstance(val, ClientObjectRef):
            raise TypeError(
                "Calling 'put' on an ObjectRef is not allowed "
                "(similarly, returning an ObjectRef from a remote "
                "function is not allowed). If you really want to "
                "do this, you can wrap the ObjectRef in a list and "
                "call 'put' on it (or return it)."
            )
        data = dumps_from_client(val, self._client_id)
        return self._put_pickled(data, client_ref_id, _owner)

    def _put_pickled(
        self, data, client_ref_id: bytes, owner: Optional[ClientActorHandle] = None
    ):
        req = ray_client_pb2.PutRequest(data=data)
        if client_ref_id is not None:
            req.client_ref_id = client_ref_id
        if owner is not None:
            req.owner_id = owner.actor_ref.id

        resp = self.data_client.PutObject(req)
        if not resp.valid:
            try:
                raise cloudpickle.loads(resp.error)
            except (pickle.UnpicklingError, TypeError):
                logger.exception("Failed to deserialize {}".format(resp.error))
                raise
        return ClientObjectRef(resp.id)

    # TODO(ekl) respect MAX_BLOCKING_OPERATION_TIME_S for wait too
    def wait(
        self,
        object_refs: List[ClientObjectRef],
        *,
        num_returns: int = 1,
        timeout: float = None,
        fetch_local: bool = True,
    ) -> Tuple[List[ClientObjectRef], List[ClientObjectRef]]:
        if not isinstance(object_refs, list):
            raise TypeError(
                "wait() expected a list of ClientObjectRef, " f"got {type(object_refs)}"
            )
        for ref in object_refs:
            if not isinstance(ref, ClientObjectRef):
                raise TypeError(
                    "wait() expected a list of ClientObjectRef, "
                    f"got list containing {type(ref)}"
                )
        data = {
            "object_ids": [object_ref.id for object_ref in object_refs],
            "num_returns": num_returns,
            "timeout": timeout if (timeout is not None) else -1,
            "client_id": self._client_id,
        }
        req = ray_client_pb2.WaitRequest(**data)
        resp = self._call_stub("WaitObject", req, metadata=self.metadata)
        if not resp.valid:
            # TODO(ameer): improve error/exceptions messages.
            raise Exception("Client Wait request failed. Reference invalid?")
        client_ready_object_ids = [
            ClientObjectRef(ref) for ref in resp.ready_object_ids
        ]
        client_remaining_object_ids = [
            ClientObjectRef(ref) for ref in resp.remaining_object_ids
        ]

        return (client_ready_object_ids, client_remaining_object_ids)

    def call_remote(self, instance, *args, **kwargs) -> List[Future]:
        task = instance._prepare_client_task()
        # data is serialized tuple of (args, kwargs)
        task.data = dumps_from_client((args, kwargs), self._client_id)
        num_returns = instance._num_returns()
        if num_returns == "dynamic":
            num_returns = -1
        if num_returns == "streaming":
            raise RuntimeError(
                'Streaming actor methods (num_returns="streaming") '
                "are not currently supported when using Ray Client."
            )

        return self._call_schedule_for_task(task, num_returns)

    def _call_schedule_for_task(
        self, task: ray_client_pb2.ClientTask, num_returns: Optional[int]
    ) -> List[Future]:
        logger.debug(f"Scheduling task {task.name} {task.type} {task.payload_id}")
        task.client_id = self._client_id
        if num_returns is None:
            num_returns = 1

        num_return_refs = num_returns
        if num_return_refs == -1:
            num_return_refs = 1
        id_futures = [Future() for _ in range(num_return_refs)]

        def populate_ids(resp: Union[ray_client_pb2.DataResponse, Exception]) -> None:
            if isinstance(resp, Exception):
                if isinstance(resp, grpc.RpcError):
                    resp = decode_exception(resp)
                for future in id_futures:
                    future.set_exception(resp)
                return

            ticket = resp.task_ticket
            if not ticket.valid:
                try:
                    ex = cloudpickle.loads(ticket.error)
                except (pickle.UnpicklingError, TypeError) as e_new:
                    ex = e_new
                for future in id_futures:
                    future.set_exception(ex)
                return

            if len(ticket.return_ids) != num_return_refs:
                exc = ValueError(
                    f"Expected {num_return_refs} returns but received "
                    f"{len(ticket.return_ids)}"
                )
                for future, raw_id in zip(id_futures, ticket.return_ids):
                    future.set_exception(exc)
                return

            for future, raw_id in zip(id_futures, ticket.return_ids):
                future.set_result(raw_id)

        self.data_client.Schedule(task, populate_ids)

        self.total_outbound_message_size_bytes += task.ByteSize()
        if (
            self.total_outbound_message_size_bytes > MESSAGE_SIZE_THRESHOLD
            and log_once("client_communication_overhead_warning")
        ):
            warnings.warn(
                "More than 10MB of messages have been created to schedule "
                "tasks on the server. This can be slow on Ray Client due to "
                "communication overhead over the network. If you're running "
                "many fine-grained tasks, consider running them inside a "
                'single remote function. See the section on "Too '
                'fine-grained tasks" in the Ray Design Patterns document for '
                f"more details: {DESIGN_PATTERN_FINE_GRAIN_TASKS_LINK}. If "
                "your functions frequently use large objects, consider "
                "storing the objects remotely with ray.put. An example of "
                'this is shown in the "Closure capture of large / '
                'unserializable object" section of the Ray Design Patterns '
                "document, available here: "
                f"{DESIGN_PATTERN_LARGE_OBJECTS_LINK}",
                UserWarning,
            )
        return id_futures

    def call_release(self, id: bytes) -> None:
        if self.closed:
            return
        self.reference_count[id] -= 1
        if self.reference_count[id] == 0:
            self._release_server(id)
            del self.reference_count[id]

    def _release_server(self, id: bytes) -> None:
        if self.data_client is not None:
            logger.debug(f"Releasing {id.hex()}")
            self.data_client.ReleaseObject(ray_client_pb2.ReleaseRequest(ids=[id]))

    def call_retain(self, id: bytes) -> None:
        logger.debug(f"Retaining {id.hex()}")
        self.reference_count[id] += 1

    def close(self):
        self._in_shutdown = True
        self.closed = True
        self.data_client.close()
        self.log_client.close()
        self.server = None
        if self.channel:
            self.channel.close()
            self.channel = None

    def get_actor(
        self, name: str, namespace: Optional[str] = None
    ) -> ClientActorHandle:
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.NAMED_ACTOR
        task.name = name
        task.namespace = namespace or ""
        # Populate task.data with empty args and kwargs
        task.data = dumps_from_client(([], {}), self._client_id)
        futures = self._call_schedule_for_task(task, 1)
        assert len(futures) == 1
        handle = ClientActorHandle(ClientActorRef(futures[0], weak_ref=True))
        # `actor_ref.is_nil()` waits until the underlying ID is resolved.
        # This is needed because `get_actor` is often used to check the
        # existence of an actor.
        if handle.actor_ref.is_nil():
            raise ValueError(f"ActorID for {name} is empty")
        return handle

    def terminate_actor(self, actor: ClientActorHandle, no_restart: bool) -> None:
        if not isinstance(actor, ClientActorHandle):
            raise ValueError(
                "ray.kill() only supported for actors. Got: {}.".format(type(actor))
            )
        term_actor = ray_client_pb2.TerminateRequest.ActorTerminate()
        term_actor.id = actor.actor_ref.id
        term_actor.no_restart = no_restart
        term = ray_client_pb2.TerminateRequest(actor=term_actor)
        term.client_id = self._client_id
        try:
            self.data_client.Terminate(term)
        except grpc.RpcError as e:
            raise decode_exception(e)

    def terminate_task(
        self, obj: ClientObjectRef, force: bool, recursive: bool
    ) -> None:
        if not isinstance(obj, ClientObjectRef):
            raise TypeError(
                "ray.cancel() only supported for non-actor object refs. "
                f"Got: {type(obj)}."
            )
        term_object = ray_client_pb2.TerminateRequest.TaskObjectTerminate()
        term_object.id = obj.id
        term_object.force = force
        term_object.recursive = recursive
        term = ray_client_pb2.TerminateRequest(task_object=term_object)
        term.client_id = self._client_id
        try:
            self.data_client.Terminate(term)
        except grpc.RpcError as e:
            raise decode_exception(e)

    def get_cluster_info(
        self,
        req_type: ray_client_pb2.ClusterInfoType.TypeEnum,
        timeout: Optional[float] = None,
    ):
        req = ray_client_pb2.ClusterInfoRequest()
        req.type = req_type
        resp = self.server.ClusterInfo(req, timeout=timeout, metadata=self.metadata)
        if resp.WhichOneof("response_type") == "resource_table":
            # translate from a proto map to a python dict
            output_dict = {k: v for k, v in resp.resource_table.table.items()}
            return output_dict
        elif resp.WhichOneof("response_type") == "runtime_context":
            return resp.runtime_context
        return json.loads(resp.json)

    def internal_kv_get(self, key: bytes, namespace: Optional[bytes]) -> bytes:
        req = ray_client_pb2.KVGetRequest(key=key, namespace=namespace)
        try:
            resp = self._call_stub("KVGet", req, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e)
        if resp.HasField("value"):
            return resp.value
        # Value is None when the key does not exist in the KV.
        return None

    def internal_kv_exists(self, key: bytes, namespace: Optional[bytes]) -> bool:
        req = ray_client_pb2.KVExistsRequest(key=key, namespace=namespace)
        try:
            resp = self._call_stub("KVExists", req, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e)
        return resp.exists

    def internal_kv_put(
        self, key: bytes, value: bytes, overwrite: bool, namespace: Optional[bytes]
    ) -> bool:
        req = ray_client_pb2.KVPutRequest(
            key=key, value=value, overwrite=overwrite, namespace=namespace
        )
        metadata = self._add_ids_to_metadata(self.metadata)
        try:
            resp = self._call_stub("KVPut", req, metadata=metadata)
        except grpc.RpcError as e:
            raise decode_exception(e)
        return resp.already_exists

    def internal_kv_del(
        self, key: bytes, del_by_prefix: bool, namespace: Optional[bytes]
    ) -> int:
        req = ray_client_pb2.KVDelRequest(
            key=key, del_by_prefix=del_by_prefix, namespace=namespace
        )
        metadata = self._add_ids_to_metadata(self.metadata)
        try:
            resp = self._call_stub("KVDel", req, metadata=metadata)
        except grpc.RpcError as e:
            raise decode_exception(e)
        return resp.deleted_num

    def internal_kv_list(
        self, prefix: bytes, namespace: Optional[bytes]
    ) -> List[bytes]:
        try:
            req = ray_client_pb2.KVListRequest(prefix=prefix, namespace=namespace)
            return self._call_stub("KVList", req, metadata=self.metadata).keys
        except grpc.RpcError as e:
            raise decode_exception(e)

    def pin_runtime_env_uri(self, uri: str, expiration_s: int) -> None:
        req = ray_client_pb2.ClientPinRuntimeEnvURIRequest(
            uri=uri, expiration_s=expiration_s
        )
        self._call_stub("PinRuntimeEnvURI", req, metadata=self.metadata)

    def list_named_actors(self, all_namespaces: bool) -> List[Dict[str, str]]:
        req = ray_client_pb2.ClientListNamedActorsRequest(all_namespaces=all_namespaces)
        return json.loads(self.data_client.ListNamedActors(req).actors_json)

    def is_initialized(self) -> bool:
        if not self.is_connected() or self.server is None:
            return False
        if not self._serverside_ray_initialized:
            # We only check that Ray is initialized on the server once to
            # avoid making an RPC every time this function is called. This is
            # safe to do because Ray only 'un-initializes' on the server when
            # the Client connection is torn down.
            self._serverside_ray_initialized = self.get_cluster_info(
                ray_client_pb2.ClusterInfoType.IS_INITIALIZED
            )

        return self._serverside_ray_initialized

    def ping_server(self, timeout=None) -> bool:
        """Simple health check.

        Piggybacks the IS_INITIALIZED call to check if the server provides
        an actual response.
        """
        if self.server is not None:
            logger.debug("Pinging server.")
            result = self.get_cluster_info(
                ray_client_pb2.ClusterInfoType.PING, timeout=timeout
            )
            return result is not None
        return False

    def is_connected(self) -> bool:
        return not self._in_shutdown and self._has_connected

    def _server_init(
        self, job_config: JobConfig, ray_init_kwargs: Optional[Dict[str, Any]] = None
    ):
        """Initialize the server"""
        if ray_init_kwargs is None:
            ray_init_kwargs = {}
        try:
            if job_config is None:
                serialized_job_config = None
            else:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    runtime_env = job_config.runtime_env or {}
                    runtime_env = upload_py_modules_if_needed(
                        runtime_env, tmp_dir, logger=logger
                    )
                    runtime_env = upload_working_dir_if_needed(
                        runtime_env, tmp_dir, logger=logger
                    )
                    # Remove excludes, it isn't relevant after the upload step.
                    runtime_env.pop("excludes", None)
                    job_config.set_runtime_env(runtime_env, validate=True)

                serialized_job_config = pickle.dumps(job_config)

            response = self.data_client.Init(
                ray_client_pb2.InitRequest(
                    job_config=serialized_job_config,
                    ray_init_kwargs=json.dumps(ray_init_kwargs),
                    reconnect_grace_period=self._reconnect_grace_period,
                )
            )
            if not response.ok:
                raise ConnectionAbortedError(
                    f"Initialization failure from server:\n{response.msg}"
                )

        except grpc.RpcError as e:
            raise decode_exception(e)

    def _convert_actor(self, actor: "ActorClass") -> str:
        """Register a ClientActorClass for the ActorClass and return a UUID"""
        key = uuid.uuid4().hex
        cls = actor.__ray_metadata__.modified_class
        self._converted[key] = ClientActorClass(cls, options=actor._default_options)
        return key

    def _convert_function(self, func: "RemoteFunction") -> str:
        """Register a ClientRemoteFunc for the ActorClass and return a UUID"""
        key = uuid.uuid4().hex
        self._converted[key] = ClientRemoteFunc(
            func._function, options=func._default_options
        )
        return key

    def _get_converted(self, key: str) -> "ClientStub":
        """Given a UUID, return the converted object"""
        return self._converted[key]

    def _converted_key_exists(self, key: str) -> bool:
        """Check if a key UUID is present in the store of converted objects."""
        return key in self._converted

    def _dumps_from_client(self, val) -> bytes:
        return dumps_from_client(val, self._client_id)


def make_client_id() -> str:
    id = uuid.uuid4()
    return id.hex


def decode_exception(e: grpc.RpcError) -> Exception:
    if e.code() != grpc.StatusCode.ABORTED:
        # The ABORTED status code is used by the server when an application
        # error is serialized into the the exception details. If the code
        # isn't ABORTED, then return the original error since there's no
        # serialized error to decode.
        # See server.py::return_exception_in_context for details
        return ConnectionError(f"GRPC connection failed: {e}")
    data = base64.standard_b64decode(e.details())
    return loads_from_server(data)
