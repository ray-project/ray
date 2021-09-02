"""This file includes the Worker class which sits on the client side.
It implements the Ray API functions that are forwarded through grpc calls
to the server.
"""
import base64
import json
import logging
import time
import threading
import uuid
import warnings
from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from typing import Optional
from typing import TYPE_CHECKING

import grpc

from ray.job_config import JobConfig
import ray.cloudpickle as cloudpickle
# Use cloudpickle's version of pickle for UnpicklingError
from ray.cloudpickle.compat import pickle
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.exceptions import GetTimeoutError
from ray.util.client.client_pickler import convert_to_arg
from ray.util.client.client_pickler import dumps_from_client
from ray.util.client.client_pickler import loads_from_server
from ray.util.client.common import ClientStub
from ray.util.client.common import ClientActorHandle
from ray.util.client.common import ClientActorClass
from ray.util.client.common import ClientRemoteFunc
from ray.util.client.common import ClientActorRef
from ray.util.client.common import ClientObjectRef
from ray.util.client.common import DEFAULT_CLIENT_RECONNECT_GRACE_PERIOD
from ray.util.client.common import GRPC_OPTIONS
from ray.util.client.common import INT32_MAX
from ray.util.client.dataclient import DataClient
from ray.util.client.logsclient import LogstreamClient
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.actor import ActorClass
    from ray.remote_function import RemoteFunction

INITIAL_TIMEOUT_SEC = 5
MAX_TIMEOUT_SEC = 30

logger = logging.getLogger(__name__)

# The max amount of time an operation can run blocking in the server. This
# allows for Ctrl-C of the client to work without explicitly cancelling server
# operations.
MAX_BLOCKING_OPERATION_TIME_S = 2

# If the total size (bytes) of all outbound messages to schedule tasks since
# the connection began exceeds this value, a warning should be raised
MESSAGE_SIZE_THRESHOLD = 10 * 2**20  # 10 MB

# If the number of tasks scheduled on the client side since the connection
# began exceeds this value, a warning should be raised
TASK_WARNING_THRESHOLD = 1000

# Links to the Ray Design Pattern doc to use in the task overhead warning
# message
DESIGN_PATTERN_FINE_GRAIN_TASKS_LINK = \
    "https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit#heading=h.f7ins22n6nyl" # noqa E501

DESIGN_PATTERN_LARGE_OBJECTS_LINK = \
    "https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit#heading=h.1afmymq455wu" # noqa E501


def backoff(timeout: int) -> int:
    timeout = timeout + 5
    if timeout > MAX_TIMEOUT_SEC:
        timeout = MAX_TIMEOUT_SEC
    return timeout


class Worker:
    def __init__(self,
                 conn_str: str = "",
                 secure: bool = False,
                 metadata: List[Tuple[str, str]] = None,
                 connection_retries: int = 3,
                 reconnect_grace_period=None):
        """Initializes the worker side grpc client.

        Args:
            conn_str: The host:port connection string for the ray server.
            secure: whether to use SSL secure channel or not.
            metadata: additional metadata passed in the grpc request headers.
            connection_retries: Number of times to attempt to reconnect to the
              ray server if it doesn't respond immediately. Setting to 0 tries
              at least once.  For infinite retries, catch the ConnectionError
              exception.
        """
        self._client_id = make_client_id()
        self.metadata = [("client_id", self._client_id)] + (metadata if
                                                            metadata else [])
        self.channel_lock = threading.Lock()
        self.channel = None
        self.server = None
        self._conn_state = grpc.ChannelConnectivity.SHUTDOWN
        self._converted: Dict[str, ClientStub] = {}
        self._session_ended = False
        self._secure = secure
        self._conn_str = conn_str
        self._connection_retries = connection_retries
        if reconnect_grace_period is None:
            self._reconnect_grace_period = \
                DEFAULT_CLIENT_RECONNECT_GRACE_PERIOD
        self._reconnect_grace_period = reconnect_grace_period

        # Set to True when close() is called
        self._in_shutdown = False

        self._connect_channel()

        # Initialize the streams to finish protocol negotiation.
        self.data_client = DataClient(self, self._client_id, self.metadata)
        self.reference_count: Dict[bytes, int] = defaultdict(int)

        self.log_client = LogstreamClient(self, self.metadata)
        self.log_client.set_logstream_level(logging.INFO)

        self.closed = False

        # Track these values to raise a warning if many tasks are being
        # scheduled
        self.total_num_tasks_scheduled = 0
        self.total_outbound_message_size_bytes = 0

        # Used to create unique IDs for RPCs to the RayletServicer
        self._req_id_lock = threading.Lock()
        self._req_id = 0

    def _can_reconnect(self, e: grpc.RpcError) -> bool:
        if self._in_shutdown:
            # Channel is being shutdown, don't try to reconnect
            return False
        if e.code() in (grpc.StatusCode.RESOURCE_EXHAUSTED,
                        grpc.StatusCode.INVALID_ARGUMENT,
                        grpc.StatusCode.NOT_FOUND,
                        grpc.StatusCode.FAILED_PRECONDITION):
            # Unrecoverable error -- These errors are specifically raised
            # by the server's application logic
            return False
        # All other errors can be treated as recoverable
        return True

    def _connect_channel(self, reconnecting=False):
        if self.channel:
            self.channel.unsubscribe(self._on_channel_state_change)
            self.channel.close()

        if self._secure:
            credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(
                self._conn_str, credentials, options=GRPC_OPTIONS)
        else:
            self.channel = grpc.insecure_channel(
                self._conn_str, options=GRPC_OPTIONS)

        self.channel.subscribe(self._on_channel_state_change)

        # Retry the connection until the channel responds to something
        # looking like a gRPC connection, though it may be a proxy.
        start_time = time.time()
        conn_attempts = 0
        timeout = INITIAL_TIMEOUT_SEC
        service_ready = False
        while conn_attempts < max(self._connection_retries, 1) or reconnecting:
            conn_attempts += 1
            elapsed_time = time.time() - start_time
            if reconnecting and elapsed_time > self._reconnect_grace_period:
                raise ConnectionError(
                    "Failed to reconnect within the reconnection grace period "
                    f"({self._reconnect_grace_period}s)")
            try:
                # Let gRPC wait for us to see if the channel becomes ready.
                # If it throws, we couldn't connect.
                grpc.channel_ready_future(self.channel).result(timeout=timeout)
                # The HTTP2 channel is ready. Wrap the channel with the
                # RayletDriverStub, allowing for unary requests.
                self.server = ray_client_pb2_grpc.RayletDriverStub(
                    self.channel)
                service_ready = bool(self.ping_server())
                if service_ready:
                    break
                # Ray is not ready yet, wait a timeout
                time.sleep(timeout)
            except grpc.FutureTimeoutError:
                logger.info(
                    f"Couldn't connect channel in {timeout} seconds, retrying")
                # Note that channel_ready_future constitutes its own timeout,
                # which is why we do not sleep here.
            except grpc.RpcError as e:
                logger.info("Ray client server unavailable, "
                            f"retrying in {timeout}s...")
                logger.debug(f"Received when checking init: {e.details()}")
                # Ray is not ready yet, wait a timeout.
                time.sleep(timeout)
            # Fallthrough, backoff, and retry at the top of the loop
            logger.info("Waiting for Ray to become ready on the server, "
                        f"retry in {timeout}s...")
            if not reconnecting:
                # Don't increase backoff when trying to reconnect --
                # server should already be ready
                timeout = backoff(timeout)

        # If we made it through the loop without service_ready
        # it means we've used up our retries and
        # should error back to the user.
        if not service_ready:
            if log_once("ray_client_security_groups"):
                warnings.warn(
                    "Ray Client connection timed out. Ensure that "
                    "the Ray Client port on the head node is reachable "
                    "from your local machine. See https://docs.ray.io/en"
                    "/latest/cluster/ray-client.html#step-2-check-ports for "
                    "more information.")
            raise ConnectionError("ray client connection timeout")

    def _call_stub(self, stub_name: str, *args, **kwargs) -> Any:
        """
        Calls the stub specified by stub_name, and retries on grpc internal
        errors
        """
        while not self._in_shutdown:
            try:
                return getattr(self.server, stub_name)(*args, **kwargs)
            except grpc.RpcError as e:
                if self._can_reconnect(e):
                    time.sleep(.5)
                    continue
                raise
            except ValueError:
                # Attempted to use stub while channel is being recreated
                time.sleep(.5)
                continue
        raise ConnectionError("Client is shutting down.")

    def _add_ids_to_request(self, request: Any):
        """
        Adds a unique req_id and the current thread's identifier to the
        request. These values are useful for preventing mutating operations
        from being replayed on the server side in the event that the client
        must retry a requsest.
        Args:
            request - A gRPC message to add the thread and request IDs to
        """
        request.thread_id = threading.get_ident()
        with self._req_id_lock:
            self._req_id += 1
            if self._req_id > INT32_MAX:
                self._req_id = 0
            request.req_id = self._req_id

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
            "protocol_version": data.protocol_version,
        }

    def register_callback(
            self, ref: ClientObjectRef,
            callback: Callable[[ray_client_pb2.DataResponse], None]) -> None:
        req = ray_client_pb2.GetRequest(id=ref.id, asynchronous=True)
        self.data_client.RegisterGetCallback(req, callback)

    def get(self, vals, *, timeout: Optional[float] = None) -> Any:
        to_get = []
        single = False
        if isinstance(vals, list):
            to_get = vals
        elif isinstance(vals, ClientObjectRef):
            to_get = [vals]
            single = True
        else:
            raise Exception("Can't get something that's not a "
                            "list of IDs or just an ID: %s" % type(vals))
        if timeout is None:
            timeout = 0
            deadline = None
        else:
            deadline = time.monotonic() + timeout
        out = []
        for obj_ref in to_get:
            res = None
            # Implement non-blocking get with a short-polling loop. This allows
            # cancellation of gets via Ctrl-C, since we never block for long.
            while True:
                try:
                    if deadline:
                        op_timeout = min(
                            MAX_BLOCKING_OPERATION_TIME_S,
                            max(deadline - time.monotonic(), 0.001))
                    else:
                        op_timeout = MAX_BLOCKING_OPERATION_TIME_S
                    res = self._get(obj_ref, op_timeout)
                    break
                except GetTimeoutError:
                    if deadline and time.monotonic() > deadline:
                        raise
                    logger.debug("Internal retry for get {}".format(obj_ref))
            out.append(res)
        if single:
            out = out[0]
        return out

    def _get(self, ref: ClientObjectRef, timeout: float):
        req = ray_client_pb2.GetRequest(id=ref.id, timeout=timeout)
        try:
            data = self.data_client.GetObject(req)
        except grpc.RpcError as e:
            raise decode_exception(e)
        if not data.valid:
            try:
                err = cloudpickle.loads(data.error)
            except (pickle.UnpicklingError, TypeError):
                logger.exception("Failed to deserialize {}".format(data.error))
                raise
            raise err
        return loads_from_server(data.data)

    def put(self, vals, *, client_ref_id: bytes = None):
        to_put = []
        single = False
        if isinstance(vals, list):
            to_put = vals
        else:
            single = True
            to_put.append(vals)

        out = [self._put(x, client_ref_id=client_ref_id) for x in to_put]
        if single:
            out = out[0]
        return out

    def _put(self, val, *, client_ref_id: bytes = None):
        if isinstance(val, ClientObjectRef):
            raise TypeError(
                "Calling 'put' on an ObjectRef is not allowed "
                "(similarly, returning an ObjectRef from a remote "
                "function is not allowed). If you really want to "
                "do this, you can wrap the ObjectRef in a list and "
                "call 'put' on it (or return it).")
        data = dumps_from_client(val, self._client_id)
        req = ray_client_pb2.PutRequest(data=data)
        if client_ref_id is not None:
            req.client_ref_id = client_ref_id
        resp = self.data_client.PutObject(req)
        if not resp.valid:
            try:
                raise cloudpickle.loads(resp.error)
            except (pickle.UnpicklingError, TypeError):
                logger.exception("Failed to deserialize {}".format(resp.error))
                raise
        return ClientObjectRef(resp.id)

    # TODO(ekl) respect MAX_BLOCKING_OPERATION_TIME_S for wait too
    def wait(self,
             object_refs: List[ClientObjectRef],
             *,
             num_returns: int = 1,
             timeout: float = None,
             fetch_local: bool = True
             ) -> Tuple[List[ClientObjectRef], List[ClientObjectRef]]:
        if not isinstance(object_refs, list):
            raise TypeError("wait() expected a list of ClientObjectRef, "
                            f"got {type(object_refs)}")
        for ref in object_refs:
            if not isinstance(ref, ClientObjectRef):
                raise TypeError("wait() expected a list of ClientObjectRef, "
                                f"got list containing {type(ref)}")
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

    def call_remote(self, instance, *args, **kwargs) -> List[bytes]:
        task = instance._prepare_client_task()
        for arg in args:
            pb_arg = convert_to_arg(arg, self._client_id)
            task.args.append(pb_arg)
        for k, v in kwargs.items():
            task.kwargs[k].CopyFrom(convert_to_arg(v, self._client_id))
        return self._call_schedule_for_task(task)

    def _call_schedule_for_task(
            self, task: ray_client_pb2.ClientTask) -> List[bytes]:
        logger.debug("Scheduling %s" % task)
        task.client_id = self._client_id
        self._add_ids_to_request(task)
        try:
            ticket = self._call_stub("Schedule", task, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e)
        if not ticket.valid:
            try:
                raise cloudpickle.loads(ticket.error)
            except (pickle.UnpicklingError, TypeError):
                logger.exception("Failed to deserialize {}".format(
                    ticket.error))
                raise
        self.total_num_tasks_scheduled += 1
        self.total_outbound_message_size_bytes += task.ByteSize()
        if self.total_num_tasks_scheduled > TASK_WARNING_THRESHOLD and \
                log_once("client_communication_overhead_warning"):
            warnings.warn(
                f"More than {TASK_WARNING_THRESHOLD} remote tasks have been "
                "scheduled. This can be slow on Ray Client due to "
                "communication overhead over the network. If you're running "
                "many fine-grained tasks, consider running them in a single "
                "remote function. See the section on \"Too fine-grained "
                "tasks\" in the Ray Design Patterns document for more "
                f"details: {DESIGN_PATTERN_FINE_GRAIN_TASKS_LINK}",
                UserWarning)
        if self.total_outbound_message_size_bytes > MESSAGE_SIZE_THRESHOLD \
                and log_once("client_communication_overhead_warning"):
            warnings.warn(
                "More than 10MB of messages have been created to schedule "
                "tasks on the server. This can be slow on Ray Client due to "
                "communication overhead over the network. If you're running "
                "many fine-grained tasks, consider running them inside a "
                "single remote function. See the section on \"Too "
                "fine-grained tasks\" in the Ray Design Patterns document for "
                f"more details: {DESIGN_PATTERN_FINE_GRAIN_TASKS_LINK}. If "
                "your functions frequently use large objects, consider "
                "storing the objects remotely with ray.put. An example of "
                "this is shown in the \"Closure capture of large / "
                "unserializable object\" section of the Ray Design Patterns "
                "document, available here: "
                f"{DESIGN_PATTERN_LARGE_OBJECTS_LINK}", UserWarning)
        return ticket.return_ids

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
            self.data_client.ReleaseObject(
                ray_client_pb2.ReleaseRequest(ids=[id]))

    def call_retain(self, id: bytes) -> None:
        logger.debug(f"Retaining {id.hex()}")
        self.reference_count[id] += 1

    def close(self):
        self._in_shutdown = True
        self.data_client.close()
        self.log_client.close()
        if self.channel:
            self.channel.close()
            self.channel = None
        self.server = None
        self.closed = True

    def get_actor(self, name: str,
                  namespace: Optional[str] = None) -> ClientActorHandle:
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.NAMED_ACTOR
        task.name = name
        task.namespace = namespace or ""
        ids = self._call_schedule_for_task(task)
        assert len(ids) == 1
        return ClientActorHandle(ClientActorRef(ids[0]))

    def terminate_actor(self, actor: ClientActorHandle,
                        no_restart: bool) -> None:
        if not isinstance(actor, ClientActorHandle):
            raise ValueError("ray.kill() only supported for actors. "
                             "Got: {}.".format(type(actor)))
        term_actor = ray_client_pb2.TerminateRequest.ActorTerminate()
        term_actor.id = actor.actor_ref.id
        term_actor.no_restart = no_restart
        try:
            term = ray_client_pb2.TerminateRequest(actor=term_actor)
            term.client_id = self._client_id
            self._add_ids_to_request(term)
            self._call_stub("Terminate", term, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e)

    def terminate_task(self, obj: ClientObjectRef, force: bool,
                       recursive: bool) -> None:
        if not isinstance(obj, ClientObjectRef):
            raise TypeError(
                "ray.cancel() only supported for non-actor object refs. "
                f"Got: {type(obj)}.")
        term_object = ray_client_pb2.TerminateRequest.TaskObjectTerminate()
        term_object.id = obj.id
        term_object.force = force
        term_object.recursive = recursive
        try:
            term = ray_client_pb2.TerminateRequest(task_object=term_object)
            term.client_id = self._client_id
            self._add_ids_to_request(term)
            self._call_stub("Terminate", term, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e)

    def get_cluster_info(self,
                         type: ray_client_pb2.ClusterInfoType.TypeEnum,
                         timeout=None):
        req = ray_client_pb2.ClusterInfoRequest()
        req.type = type
        resp = self.server.ClusterInfo(
            req, timeout=timeout, metadata=self.metadata)
        if resp.WhichOneof("response_type") == "resource_table":
            # translate from a proto map to a python dict
            output_dict = {k: v for k, v in resp.resource_table.table.items()}
            return output_dict
        elif resp.WhichOneof("response_type") == "runtime_context":
            return resp.runtime_context
        return json.loads(resp.json)

    def internal_kv_get(self, key: bytes) -> bytes:
        req = ray_client_pb2.KVGetRequest(key=key)
        resp = self._call_stub("KVGet", req, metadata=self.metadata)
        return resp.value

    def internal_kv_exists(self, key: bytes) -> bytes:
        # TODO: This doesn't look right?
        req = ray_client_pb2.KVGetRequest(key=key)
        resp = self._call_stub("KVGet", req, metadata=self.metadata)
        return resp.value

    def internal_kv_put(self, key: bytes, value: bytes,
                        overwrite: bool) -> bool:
        req = ray_client_pb2.KVPutRequest(
            key=key, value=value, overwrite=overwrite)
        self._add_ids_to_request(req)
        resp = self._call_stub("KVPut", req, metadata=self.metadata)
        return resp.already_exists

    def internal_kv_del(self, key: bytes) -> None:
        req = ray_client_pb2.KVDelRequest(key=key)
        self._add_ids_to_request(req)
        self._call_stub("KVDel", req, metadata=self.metadata)

    def internal_kv_list(self, prefix: bytes) -> bytes:
        req = ray_client_pb2.KVListRequest(prefix=prefix)
        return self._call_stub("KVList", req, metadata=self.metadata).keys

    def list_named_actors(self, all_namespaces: bool) -> List[Dict[str, str]]:
        req = ray_client_pb2.ClientListNamedActorsRequest(
            all_namespaces=all_namespaces)
        return json.loads(
            self._call_stub("ListNamedActors", req,
                            metadata=self.metadata).actors_json)

    def is_initialized(self) -> bool:
        if self.server is not None:
            return self.get_cluster_info(
                ray_client_pb2.ClusterInfoType.IS_INITIALIZED)
        return False

    def ping_server(self, timeout=None) -> bool:
        """Simple health check.

        Piggybacks the IS_INITIALIZED call to check if the server provides
        an actual response.
        """
        if self.server is not None:
            logger.debug("Pinging server.")
            result = self.get_cluster_info(
                ray_client_pb2.ClusterInfoType.PING, timeout=timeout)
            return result is not None
        return False

    def is_connected(self) -> bool:
        return not self._session_ended

    def _call_init(self, init_request: ray_client_pb2.InitRequest) -> None:
        init_resp = self.data_client.Init(init_request)
        if not init_resp.ok:
            raise ConnectionAbortedError(
                f"Init Failure From Server:\n{init_resp.msg}")
        return

    def _server_init(self,
                     job_config: JobConfig,
                     ray_init_kwargs: Optional[Dict[str, Any]] = None):
        """Initialize the server"""
        if ray_init_kwargs is None:
            ray_init_kwargs = {}
        try:
            if job_config is None:
                init_req = ray_client_pb2.InitRequest(
                    ray_init_kwargs=json.dumps(ray_init_kwargs),
                    reconnect_grace_period=self._reconnect_grace_period)
                self._call_init(init_req)
                return

            import ray._private.runtime_env as runtime_env
            import tempfile
            with tempfile.TemporaryDirectory() as tmp_dir:
                (old_dir, runtime_env.PKG_DIR) = (runtime_env.PKG_DIR, tmp_dir)
                # Generate the uri for runtime env
                runtime_env.rewrite_runtime_env_uris(job_config)
                init_req = ray_client_pb2.InitRequest(
                    job_config=pickle.dumps(job_config),
                    ray_init_kwargs=json.dumps(ray_init_kwargs),
                    reconnect_grace_period=self._reconnect_grace_period)
                self._call_init(init_req)
                runtime_env.upload_runtime_env_package_if_needed(job_config)
                runtime_env.PKG_DIR = old_dir
                prep_req = ray_client_pb2.PrepRuntimeEnvRequest()
                self.data_client.PrepRuntimeEnv(prep_req)
        except grpc.RpcError as e:
            raise decode_exception(e)

    def _convert_actor(self, actor: "ActorClass") -> str:
        """Register a ClientActorClass for the ActorClass and return a UUID"""
        key = uuid.uuid4().hex
        md = actor.__ray_metadata__
        cls = md.modified_class
        self._converted[key] = ClientActorClass(
            cls,
            options={
                "max_restarts": md.max_restarts,
                "max_task_retries": md.max_task_retries,
                "num_cpus": md.num_cpus,
                "num_gpus": md.num_gpus,
                "memory": md.memory,
                "object_store_memory": md.object_store_memory,
                "resources": md.resources,
                "accelerator_type": md.accelerator_type,
            })
        return key

    def _convert_function(self, func: "RemoteFunction") -> str:
        """Register a ClientRemoteFunc for the ActorClass and return a UUID"""
        key = uuid.uuid4().hex
        f = func._function
        self._converted[key] = ClientRemoteFunc(
            f,
            options={
                "num_cpus": func._num_cpus,
                "num_gpus": func._num_gpus,
                "max_calls": func._max_calls,
                "max_retries": func._max_retries,
                "resources": func._resources,
                "accelerator_type": func._accelerator_type,
                "num_returns": func._num_returns,
                "memory": func._memory
            })
        return key

    def _get_converted(self, key: str) -> "ClientStub":
        """Given a UUID, return the converted object"""
        return self._converted[key]

    def _converted_key_exists(self, key: str) -> bool:
        """Check if a key UUID is present in the store of converted objects."""
        return key in self._converted


def make_client_id() -> str:
    id = uuid.uuid4()
    return id.hex


def decode_exception(e: grpc.RpcError) -> Exception:
    if e.code() != grpc.StatusCode.INVALID_ARGUMENT:
        # This error was generated by the GRPC library itself, not by the
        # server
        raise
    data = base64.standard_b64decode(e.details())
    return loads_from_server(data)
