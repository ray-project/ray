import atexit
import json
import logging
import socket
import sys
import time
import traceback
import urllib
from concurrent import futures
from dataclasses import dataclass
from itertools import chain
from threading import Event, Lock, RLock, Thread
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import grpc

import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import ray.core.generated.runtime_env_agent_pb2 as runtime_env_agent_pb2
from ray._common.network_utils import (
    build_address,
    is_ipv6,
    is_localhost,
)
from ray._private.authentication.http_token_authentication import (
    format_authentication_http_error,
    get_auth_headers_if_auth_enabled,
)
from ray._private.client_mode_hook import disable_client_hook
from ray._private.grpc_utils import init_grpc_channel
from ray._private.parameter import RayParams
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.services import (
    ProcessInfo,
    get_node_with_retry,
    start_ray_client_server,
)
from ray._private.tls_utils import add_port_to_grpc_server
from ray._private.utils import detect_fate_sharing_support
from ray._raylet import GcsClient
from ray.cloudpickle.compat import pickle
from ray.exceptions import AuthenticationError
from ray.job_config import JobConfig
from ray.util.client.common import (
    CLIENT_SERVER_MAX_THREADS,
    GRPC_OPTIONS,
    ClientServerHandle,
    _get_client_id_from_context,
    _propagate_error_in_context,
)
from ray.util.client.server.dataservicer import _get_reconnecting_from_context

# Import psutil after ray so the packaged version is used.
import psutil

logger = logging.getLogger(__name__)

CHECK_PROCESS_INTERVAL_S = 30

MIN_SPECIFIC_SERVER_PORT = 23000
MAX_SPECIFIC_SERVER_PORT = 24000

CHECK_CHANNEL_TIMEOUT_S = 30

LOGSTREAM_RETRIES = 5
LOGSTREAM_RETRY_INTERVAL_SEC = 2


@dataclass
class SpecificServer:
    port: int
    process_handle_future: futures.Future
    channel: "grpc._channel.Channel"

    def is_ready(self) -> bool:
        """Check if the server is ready or not (doesn't block)."""
        return self.process_handle_future.done()

    def wait_ready(self, timeout: Optional[float] = None) -> None:
        """
        Wait for the server to actually start up.
        """
        res = self.process_handle_future.result(timeout=timeout)
        if res is None:
            # This is only set to none when server creation specifically fails.
            raise RuntimeError("Server startup failed.")

    def poll(self) -> Optional[int]:
        """Check if the process has exited."""
        try:
            proc = self.process_handle_future.result(timeout=0.1)
            if proc is not None:
                return proc.process.poll()
        except futures.TimeoutError:
            return

    def kill(self) -> None:
        """Try to send a KILL signal to the process."""
        try:
            proc = self.process_handle_future.result(timeout=0.1)
            if proc is not None:
                proc.process.kill()
        except futures.TimeoutError:
            # Server has not been started yet.
            pass

    def set_result(self, proc: Optional[ProcessInfo]) -> None:
        """Set the result of the internal future if it is currently unset."""
        if not self.is_ready():
            self.process_handle_future.set_result(proc)


def _match_running_client_server(command: List[str]) -> bool:
    """
    Detects if the main process in the given command is the RayClient Server.
    This works by ensuring that the command is of the form:
        <py_executable> -m ray.util.client.server <args>
    """
    flattened = " ".join(command)
    return "-m ray.util.client.server" in flattened


class ProxyManager:
    def __init__(
        self,
        address: Optional[str],
        runtime_env_agent_address: str,
        *,
        session_dir: Optional[str] = None,
        redis_username: Optional[str] = None,
        redis_password: Optional[str] = None,
        node_id: Optional[str] = None,
    ):
        self.servers: Dict[str, SpecificServer] = dict()
        self.server_lock = RLock()
        self._address = address
        self._redis_username = redis_username
        self._redis_password = redis_password
        self._free_ports: List[int] = list(
            range(MIN_SPECIFIC_SERVER_PORT, MAX_SPECIFIC_SERVER_PORT)
        )

        if runtime_env_agent_address:
            parsed = urlparse(runtime_env_agent_address)
            # runtime env agent self-assigns a free port, fetch it from GCS
            if parsed.port is None or parsed.port == 0:
                if node_id is None:
                    raise ValueError(
                        "node_id is required when runtime_env_agent_address "
                        "has no port specified"
                    )
                node_info = get_node_with_retry(address, node_id)
                runtime_env_agent_address = urlunparse(
                    parsed._replace(
                        netloc=f"{parsed.hostname}:{node_info['runtime_env_agent_port']}"
                    )
                )

        self._runtime_env_agent_address = runtime_env_agent_address

        self._check_thread = Thread(target=self._check_processes, daemon=True)
        self._check_thread.start()

        self.fate_share = bool(detect_fate_sharing_support())
        self._node: Optional[ray._private.node.Node] = None
        atexit.register(self._cleanup)

    def _get_unused_port(self, family: int = socket.AF_INET) -> int:
        """
        Search for a port in _free_ports that is unused.
        """
        with self.server_lock:
            num_ports = len(self._free_ports)
            for _ in range(num_ports):
                port = self._free_ports.pop(0)
                s = socket.socket(family, socket.SOCK_STREAM)
                try:
                    s.bind(("", port))
                except OSError:
                    self._free_ports.append(port)
                    continue
                finally:
                    s.close()
                return port
        raise RuntimeError("Unable to succeed in selecting a random port.")

    @property
    def address(self) -> str:
        """
        Returns the provided Ray bootstrap address, or creates a new cluster.
        """
        if self._address:
            return self._address
        # Start a new, locally scoped cluster.
        connection_tuple = ray.init()
        self._address = connection_tuple["address"]
        self._session_dir = connection_tuple["session_dir"]
        return self._address

    @property
    def node(self) -> ray._private.node.Node:
        """Gets a 'ray.Node' object for this node (the head node).
        If it does not already exist, one is created using the bootstrap
        address.
        """
        if self._node:
            return self._node
        ray_params = RayParams(gcs_address=self.address)

        self._node = ray._private.node.Node(
            ray_params,
            head=False,
            shutdown_at_exit=False,
            spawn_reaper=False,
            connect_only=True,
        )

        return self._node

    def create_specific_server(self, client_id: str) -> SpecificServer:
        """
        Create, but not start a SpecificServer for a given client. This
        method must be called once per client.
        """
        with self.server_lock:
            assert (
                self.servers.get(client_id) is None
            ), f"Server already created for Client: {client_id}"

            host = "127.0.0.1"
            port = self._get_unused_port(
                socket.AF_INET6 if is_ipv6(host) else socket.AF_INET
            )

            server = SpecificServer(
                port=port,
                process_handle_future=futures.Future(),
                channel=init_grpc_channel(
                    build_address(host, port), options=GRPC_OPTIONS
                ),
            )
            self.servers[client_id] = server
            return server

    def _create_runtime_env(
        self,
        serialized_runtime_env: str,
        runtime_env_config: str,
        specific_server: SpecificServer,
    ):
        """Increase the runtime_env reference by sending an RPC to the agent.

        Includes retry logic to handle the case when the agent is
        temporarily unreachable (e.g., hasn't been started up yet).
        """
        logger.info(
            f"Increasing runtime env reference for "
            f"ray_client_server_{specific_server.port}."
            f"Serialized runtime env is {serialized_runtime_env}."
        )

        assert (
            len(self._runtime_env_agent_address) > 0
        ), "runtime_env_agent_address not set"

        create_env_request = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest(
            serialized_runtime_env=serialized_runtime_env,
            runtime_env_config=runtime_env_config,
            job_id=f"ray_client_server_{specific_server.port}".encode("utf-8"),
            source_process="client_server",
        )

        retries = 0
        max_retries = 5
        wait_time_s = 0.5
        last_exception = None
        while retries <= max_retries:
            try:
                url = urllib.parse.urljoin(
                    self._runtime_env_agent_address, "/get_or_create_runtime_env"
                )
                data = create_env_request.SerializeToString()
                headers = {"Content-Type": "application/octet-stream"}
                headers.update(**get_auth_headers_if_auth_enabled(headers))
                req = urllib.request.Request(
                    url, data=data, method="POST", headers=headers
                )
                response = urllib.request.urlopen(req, timeout=None)
                response_data = response.read()
                r = runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply()
                r.ParseFromString(response_data)

                if r.status == runtime_env_agent_pb2.AgentRpcStatus.AGENT_RPC_STATUS_OK:
                    return r.serialized_runtime_env_context
                elif (
                    r.status
                    == runtime_env_agent_pb2.AgentRpcStatus.AGENT_RPC_STATUS_FAILED
                ):
                    raise RuntimeError(
                        "Failed to create runtime_env for Ray client "
                        f"server, it is caused by:\n{r.error_message}"
                    )
                else:
                    assert False, f"Unknown status: {r.status}."
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode("utf-8", "ignore")
                except Exception:
                    body = e.reason if hasattr(e, "reason") else str(e)

                formatted_error = format_authentication_http_error(e.code, body or "")
                if formatted_error:
                    raise AuthenticationError(formatted_error) from e

                # Treat non-auth HTTP errors like URLError (retry with backoff)
                last_exception = e
                logger.warning(
                    f"GetOrCreateRuntimeEnv request failed with HTTP {e.code}: {body or e}. "
                    f"Retrying after {wait_time_s}s. "
                    f"{max_retries-retries} retries remaining."
                )

            except urllib.error.URLError as e:
                last_exception = e
                logger.warning(
                    f"GetOrCreateRuntimeEnv request failed: {e}. "
                    f"Retrying after {wait_time_s}s. "
                    f"{max_retries-retries} retries remaining."
                )

            # Exponential backoff.
            time.sleep(wait_time_s)
            retries += 1
            wait_time_s *= 2

        raise TimeoutError(
            f"GetOrCreateRuntimeEnv request failed after {max_retries} attempts."
            f" Last exception: {last_exception}"
        )

    def start_specific_server(self, client_id: str, job_config: JobConfig) -> bool:
        """
        Start up a RayClient Server for an incoming client to
        communicate with. Returns whether creation was successful.
        """
        specific_server = self._get_server_for_client(client_id)
        assert specific_server, f"Server has not been created for: {client_id}"

        output, error = self.node.get_log_file_handles(
            f"ray_client_server_{specific_server.port}", unique=True
        )

        serialized_runtime_env = job_config._get_serialized_runtime_env()
        runtime_env_config = job_config._get_proto_runtime_env_config()
        if not serialized_runtime_env or serialized_runtime_env == "{}":
            # TODO(edoakes): can we just remove this case and always send it
            # to the agent?
            serialized_runtime_env_context = RuntimeEnvContext().serialize()
        else:
            serialized_runtime_env_context = self._create_runtime_env(
                serialized_runtime_env=serialized_runtime_env,
                runtime_env_config=runtime_env_config,
                specific_server=specific_server,
            )

        proc = start_ray_client_server(
            self.address,
            "127.0.0.1",
            specific_server.port,
            stdout_file=output,
            stderr_file=error,
            fate_share=self.fate_share,
            server_type="specific-server",
            serialized_runtime_env_context=serialized_runtime_env_context,
            redis_username=self._redis_username,
            redis_password=self._redis_password,
        )

        # Wait for the process being run transitions from the shim process
        # to the actual RayClient Server.
        pid = proc.process.pid
        if sys.platform != "win32":
            psutil_proc = psutil.Process(pid)
        else:
            psutil_proc = None
        # Don't use `psutil` on Win32
        while psutil_proc is not None:
            if proc.process.poll() is not None:
                logger.error(f"SpecificServer startup failed for client: {client_id}")
                break
            cmd = psutil_proc.cmdline()
            if _match_running_client_server(cmd):
                break
            logger.debug("Waiting for Process to reach the actual client server.")
            time.sleep(0.5)
        specific_server.set_result(proc)
        logger.info(
            f"SpecificServer started on port: {specific_server.port} "
            f"with PID: {pid} for client: {client_id}"
        )
        return proc.process.poll() is None

    def _get_server_for_client(self, client_id: str) -> Optional[SpecificServer]:
        with self.server_lock:
            client = self.servers.get(client_id)
            if client is None:
                logger.error(f"Unable to find channel for client: {client_id}")
            return client

    def has_channel(self, client_id: str) -> bool:
        server = self._get_server_for_client(client_id)
        if server is None:
            return False

        return server.is_ready()

    def get_channel(
        self,
        client_id: str,
        *,
        timeout_s: float = CHECK_CHANNEL_TIMEOUT_S,
    ) -> Optional["grpc._channel.Channel"]:
        """
        Find the gRPC Channel for the given client_id. This will block until
        the server process has started.
        """
        server = self._get_server_for_client(client_id)
        if server is None:
            return None
        # Wait for the SpecificServer to become ready.
        server.wait_ready()
        try:
            grpc.channel_ready_future(server.channel).result(timeout=timeout_s)
            return server.channel
        except grpc.FutureTimeoutError:
            logger.exception(f"Timeout waiting for channel for {client_id}")
            exit_code = server.poll()
            # If the process has already exited, eagerly remove it so reconnect can recreate
            # the server without waiting for _check_processes (30s interval).
            if exit_code is not None:
                with self.server_lock:
                    cur = self.servers.get(client_id)
                    if cur is server:
                        del self.servers[client_id]
                        self._free_ports.append(server.port)
            return None

    def _check_processes(self):
        """
        Keeps the internal servers dictionary up-to-date with running servers.
        """
        while True:
            with self.server_lock:
                for client_id, specific_server in list(self.servers.items()):
                    if specific_server.poll() is not None:
                        logger.info(
                            f"Specific server {client_id} is no longer running"
                            f", freeing its port {specific_server.port}"
                        )
                        del self.servers[client_id]
                        # Port is available to use again.
                        self._free_ports.append(specific_server.port)

            time.sleep(CHECK_PROCESS_INTERVAL_S)

    def _cleanup(self) -> None:
        """
        Forcibly kill all spawned RayClient Servers. This ensures cleanup
        for platforms where fate sharing is not supported.
        """
        for server in self.servers.values():
            server.kill()


class RayletServicerProxy(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self, ray_connect_handler: Callable, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self.ray_connect_handler = ray_connect_handler

    def _call_inner_function(
        self, request, context, method: str
    ) -> Optional[ray_client_pb2_grpc.RayletDriverStub]:
        client_id = _get_client_id_from_context(context)
        # During reconnect, the specific-server process/channel may be in the middle of being
        # recreated by the DataServicerProxy. In that case we should fail fast with a
        # *recoverable* error so the client retries instead of surfacing an unrecoverable
        # NOT_FOUND to the user.
        chan = self.proxy_manager.get_channel(client_id, timeout_s=5)
        if not chan:
            logger.error(f"Channel for Client: {client_id} not found!")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(
                "Ray Client backend is temporarily unavailable (likely reconnecting). "
                "Please retry."
            )
            return None

        stub = ray_client_pb2_grpc.RayletDriverStub(chan)
        try:
            metadata = [("client_id", client_id)]
            if context:
                metadata = context.invocation_metadata()
            return getattr(stub, method)(request, metadata=metadata)
        except Exception as e:
            # Error while proxying -- propagate the error's context to user
            logger.exception(f"Proxying call to {method} failed!")
            _propagate_error_in_context(e, context)

    def _has_channel_for_request(self, context):
        client_id = _get_client_id_from_context(context)
        return self.proxy_manager.has_channel(client_id)

    def Init(self, request, context=None) -> ray_client_pb2.InitResponse:
        return self._call_inner_function(request, context, "Init")

    def KVPut(self, request, context=None) -> ray_client_pb2.KVPutResponse:
        """Proxies internal_kv.put.

        This is used by the working_dir code to upload to the GCS before
        ray.init is called. In that case (if we don't have a server yet)
        we directly make the internal KV call from the proxier.

        Otherwise, we proxy the call to the downstream server as usual.
        """
        if self._has_channel_for_request(context):
            return self._call_inner_function(request, context, "KVPut")

        with disable_client_hook():
            already_exists = ray.experimental.internal_kv._internal_kv_put(
                request.key, request.value, overwrite=request.overwrite
            )
        return ray_client_pb2.KVPutResponse(already_exists=already_exists)

    def KVGet(self, request, context=None) -> ray_client_pb2.KVGetResponse:
        """Proxies internal_kv.get.

        This is used by the working_dir code to upload to the GCS before
        ray.init is called. In that case (if we don't have a server yet)
        we directly make the internal KV call from the proxier.

        Otherwise, we proxy the call to the downstream server as usual.
        """
        if self._has_channel_for_request(context):
            return self._call_inner_function(request, context, "KVGet")

        with disable_client_hook():
            value = ray.experimental.internal_kv._internal_kv_get(request.key)
        return ray_client_pb2.KVGetResponse(value=value)

    def KVDel(self, request, context=None) -> ray_client_pb2.KVDelResponse:
        """Proxies internal_kv.delete.

        This is used by the working_dir code to upload to the GCS before
        ray.init is called. In that case (if we don't have a server yet)
        we directly make the internal KV call from the proxier.

        Otherwise, we proxy the call to the downstream server as usual.
        """
        if self._has_channel_for_request(context):
            return self._call_inner_function(request, context, "KVDel")

        with disable_client_hook():
            ray.experimental.internal_kv._internal_kv_del(request.key)
        return ray_client_pb2.KVDelResponse()

    def KVList(self, request, context=None) -> ray_client_pb2.KVListResponse:
        """Proxies internal_kv.list.

        This is used by the working_dir code to upload to the GCS before
        ray.init is called. In that case (if we don't have a server yet)
        we directly make the internal KV call from the proxier.

        Otherwise, we proxy the call to the downstream server as usual.
        """
        if self._has_channel_for_request(context):
            return self._call_inner_function(request, context, "KVList")

        with disable_client_hook():
            keys = ray.experimental.internal_kv._internal_kv_list(request.prefix)
        return ray_client_pb2.KVListResponse(keys=keys)

    def KVExists(self, request, context=None) -> ray_client_pb2.KVExistsResponse:
        """Proxies internal_kv.exists.

        This is used by the working_dir code to upload to the GCS before
        ray.init is called. In that case (if we don't have a server yet)
        we directly make the internal KV call from the proxier.

        Otherwise, we proxy the call to the downstream server as usual.
        """
        if self._has_channel_for_request(context):
            return self._call_inner_function(request, context, "KVExists")

        with disable_client_hook():
            exists = ray.experimental.internal_kv._internal_kv_exists(request.key)
        return ray_client_pb2.KVExistsResponse(exists=exists)

    def PinRuntimeEnvURI(
        self, request, context=None
    ) -> ray_client_pb2.ClientPinRuntimeEnvURIResponse:
        """Proxies internal_kv.pin_runtime_env_uri.

        This is used by the working_dir code to upload to the GCS before
        ray.init is called. In that case (if we don't have a server yet)
        we directly make the internal KV call from the proxier.

        Otherwise, we proxy the call to the downstream server as usual.
        """
        if self._has_channel_for_request(context):
            return self._call_inner_function(request, context, "PinRuntimeEnvURI")

        with disable_client_hook():
            ray.experimental.internal_kv._pin_runtime_env_uri(
                request.uri, expiration_s=request.expiration_s
            )
        return ray_client_pb2.ClientPinRuntimeEnvURIResponse()

    def ListNamedActors(
        self, request, context=None
    ) -> ray_client_pb2.ClientListNamedActorsResponse:
        return self._call_inner_function(request, context, "ListNamedActors")

    def ClusterInfo(self, request, context=None) -> ray_client_pb2.ClusterInfoResponse:

        # NOTE: We need to respond to the PING request here to allow the client
        # to continue with connecting.
        if request.type == ray_client_pb2.ClusterInfoType.PING:
            resp = ray_client_pb2.ClusterInfoResponse(json=json.dumps({}))
            return resp
        return self._call_inner_function(request, context, "ClusterInfo")

    def Terminate(self, req, context=None):
        return self._call_inner_function(req, context, "Terminate")

    def GetObject(self, request, context=None):
        try:
            yield from self._call_inner_function(request, context, "GetObject")
        except Exception as e:
            # Error while iterating over response from GetObject stream
            logger.exception("Proxying call to GetObject failed!")
            _propagate_error_in_context(e, context)

    def PutObject(
        self, request: ray_client_pb2.PutRequest, context=None
    ) -> ray_client_pb2.PutResponse:
        return self._call_inner_function(request, context, "PutObject")

    def WaitObject(self, request, context=None) -> ray_client_pb2.WaitResponse:
        return self._call_inner_function(request, context, "WaitObject")

    def Schedule(self, task, context=None) -> ray_client_pb2.ClientTaskTicket:
        return self._call_inner_function(task, context, "Schedule")


def ray_client_server_env_prep(job_config: JobConfig) -> JobConfig:
    return job_config


def prepare_runtime_init_req(
    init_request: ray_client_pb2.DataRequest,
) -> Tuple[ray_client_pb2.DataRequest, JobConfig]:
    """
    Extract JobConfig and possibly mutate InitRequest before it is passed to
    the specific RayClient Server.
    """
    init_type = init_request.WhichOneof("type")
    assert init_type == "init", (
        "Received initial message of type " f"{init_type}, not 'init'."
    )
    req = init_request.init
    job_config = JobConfig()
    if req.job_config:
        job_config = pickle.loads(req.job_config)
    new_job_config = ray_client_server_env_prep(job_config)
    modified_init_req = ray_client_pb2.InitRequest(
        job_config=pickle.dumps(new_job_config),
        ray_init_kwargs=init_request.init.ray_init_kwargs,
        reconnect_grace_period=init_request.init.reconnect_grace_period,
    )

    init_request.init.CopyFrom(modified_init_req)
    return (init_request, new_job_config)


class RequestIteratorProxy:
    def __init__(self, request_iterator):
        self.request_iterator = request_iterator

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.request_iterator)
        except grpc.RpcError as e:
            # To stop proxying already CANCLLED request stream gracefully,
            # we only translate the exact grpc.RpcError to StopIteration,
            # not its subsclasses. ex: grpc._Rendezvous
            # https://github.com/grpc/grpc/blob/v1.43.0/src/python/grpcio/grpc/_server.py#L353-L354
            # This fixes the https://github.com/ray-project/ray/issues/23865
            if type(e) is not grpc.RpcError:
                raise e  # re-raise other grpc exceptions
            logger.exception(
                "Stop iterating cancelled request stream with the following exception:"
            )
            raise StopIteration


class DataServicerProxy(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, proxy_manager: ProxyManager):
        self.num_clients = 0
        # dictionary mapping client_id's to the last time they connected
        self.clients_last_seen: Dict[str, float] = {}
        self.reconnect_grace_periods: Dict[str, float] = {}
        # dictionary mapping client_id's to their job_config for reconnection
        self.job_configs: Dict[str, JobConfig] = {}
        # dictionary mapping client_id's to the serialized job_config bytes (already pickled)
        # used to synthesize an InitRequest without re-pickling Python objects.
        self.job_config_bytes: Dict[str, bytes] = {}
        # dictionary mapping client_id's to their original ray_init_kwargs (JSON string)
        # used to synthesize an InitRequest if the specific-server must be recreated.
        self.ray_init_kwargs: Dict[str, str] = {}
        # Per-client flag indicating the current backend is fresh and must receive an InitRequest
        # before it can accept reconnecting=True requests.
        self.backend_needs_init: Dict[str, bool] = {}
        # Schedule delayed cleanup without blocking the Datapath RPC.
        # For each client_id, a monotonically increasing token invalidates any
        # previously scheduled cleanup task.
        self._cleanup_tokens: Dict[str, int] = {}
        self.clients_lock = Lock()
        self.proxy_manager = proxy_manager
        self.stopped = Event()

    def _invalidate_cleanup_locked(self, client_id: str) -> int:
        """Invalidate any previously scheduled cleanup for this client."""
        token = self._cleanup_tokens.get(client_id, 0) + 1
        self._cleanup_tokens[client_id] = token
        return token

    def _schedule_delayed_cleanup(
        self, client_id: str, start_time: float, delay_s: float
    ) -> None:
        """
        Schedule cleanup after delay_s without blocking the current Datapath RPC.

        IMPORTANT: If we block inside the Datapath handler (e.g. time.sleep / Event.wait),
        the client will not observe the RpcError and therefore will not begin reconnecting
        until after that wait finishes.
        """
        with self.clients_lock:
            token = self._invalidate_cleanup_locked(client_id)

        def _run_cleanup():
            # Allow server shutdown to interrupt the wait.
            self.stopped.wait(timeout=delay_s)
            with self.clients_lock:
                if self._cleanup_tokens.get(client_id) != token:
                    return

                if client_id not in self.clients_last_seen:
                    self.reconnect_grace_periods.pop(client_id, None)
                    self.job_configs.pop(client_id, None)
                    self.job_config_bytes.pop(client_id, None)
                    self.ray_init_kwargs.pop(client_id, None)
                    self.backend_needs_init.pop(client_id, None)
                    return

                current_last_seen = self.clients_last_seen[client_id]
                if current_last_seen > start_time:
                    return

                if self.num_clients > 0:
                    self.num_clients -= 1
                del self.clients_last_seen[client_id]
                self.reconnect_grace_periods.pop(client_id, None)
                self.job_configs.pop(client_id, None)
                self.job_config_bytes.pop(client_id, None)
                self.ray_init_kwargs.pop(client_id, None)
                self.backend_needs_init.pop(client_id, None)

        Thread(target=_run_cleanup, daemon=True).start()

    def modify_connection_info_resp(
        self, init_resp: ray_client_pb2.DataResponse
    ) -> ray_client_pb2.DataResponse:
        """
        Modify the `num_clients` returned the ConnectionInfoResponse because
        individual SpecificServers only have **one** client.
        """
        init_type = init_resp.WhichOneof("type")
        if init_type != "connection_info":
            return init_resp
        modified_resp = ray_client_pb2.DataResponse()
        modified_resp.CopyFrom(init_resp)
        with self.clients_lock:
            modified_resp.connection_info.num_clients = self.num_clients
        return modified_resp

    def Datapath(self, request_iterator, context):
        request_iterator = RequestIteratorProxy(request_iterator)
        cleanup_requested = False
        start_time = time.time()
        client_id = _get_client_id_from_context(context)
        if client_id == "":
            return
        reconnecting = _get_reconnecting_from_context(context)
        downstream_reconnecting = reconnecting

        if reconnecting:
            recreated_backend = False
            ray_init_kwargs = None
            with self.clients_lock:
                # A reconnect attempt means the session is active again; cancel any pending cleanup.
                self._invalidate_cleanup_locked(client_id)
                # Get saved job_config first - it might still exist even if clients_last_seen was cleaned
                job_config = self.job_configs.get(client_id)
                ray_init_kwargs = self.ray_init_kwargs.get(client_id)
                
                if client_id not in self.clients_last_seen:
                    # Client took too long to reconnect, session has already been cleaned up
                    # But check if we still have job_config - if so, allow recovery (race condition)
                    if job_config is not None:
                        # Restore clients_last_seen entry for this reconnection
                        self.clients_last_seen[client_id] = start_time
                    else:
                        # No job_config either, truly cannot recover
                        logger.error(
                            f"Client {client_id} not found in clients_last_seen "
                            f"and no job_config available. Session has been cleaned up."
                        )
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(
                            "Attempted to reconnect a session that has already "
                            "been cleaned up"
                        )
                        return
                else:
                    self.clients_last_seen[client_id] = start_time

            server = self.proxy_manager._get_server_for_client(client_id)
            if server is None:
                # Server was removed by _check_processes, need to recreate it
                if job_config is None:
                    # No saved job_config, cannot recover
                    logger.error(
                        f"Cannot reconnect {client_id}: server was removed "
                        "and job config is not available."
                    )
                    # Don't return here - let it go through exception handling
                    # so cleanup can happen properly in finally block
                    raise RuntimeError(
                        "Cannot reconnect: server was removed and job config "
                        "is not available. Please reconnect as a new client."
                    )
                
                # Recreate server for reconnection
                server = self.proxy_manager.create_specific_server(client_id)
                recreated_backend = True
                downstream_reconnecting = False
                if not self.proxy_manager.start_specific_server(
                    client_id, job_config
                ):
                    logger.error(
                        f"Failed to restart server for {client_id} during reconnection"
                    )
                    # Don't return here - let it go through exception handling
                    raise RuntimeError(
                        "Failed to restart server during reconnection. "
                        "Please try reconnecting again."
                    )
                with self.clients_lock:
                    self.backend_needs_init[client_id] = True
            
            # Use a shorter readiness timeout during reconnect to avoid 30s stalls.
            channel = self.proxy_manager.get_channel(client_id, timeout_s=5)
            if channel is None:
                # If the server process is dead or the channel isn't becoming ready,
                # don't sit in a 30s timeout loop. Eagerly recreate the server.
                exit_code = server.poll() if server is not None else None
                if job_config is None:
                    raise RuntimeError(
                        "Failed to get channel during reconnection and no job_config is available. "
                        "Please reconnect as a new client."
                    )
                # Force recreate: drop the existing server entry (if any) and start a fresh one.
                with self.proxy_manager.server_lock:
                    existing = self.proxy_manager.servers.get(client_id)
                    if existing is not None:
                        del self.proxy_manager.servers[client_id]
                        self.proxy_manager._free_ports.append(existing.port)
                server = self.proxy_manager.create_specific_server(client_id)
                recreated_backend = True
                downstream_reconnecting = False
                if not self.proxy_manager.start_specific_server(client_id, job_config):
                    raise RuntimeError(
                        "Failed to restart server during reconnection (channel not ready). "
                        "Please try reconnecting again."
                    )
                with self.clients_lock:
                    self.backend_needs_init[client_id] = True
                # Use a shorter readiness timeout during reconnect to avoid 30s stalls.
                channel = self.proxy_manager.get_channel(client_id, timeout_s=5)
                if channel is None:
                    # If we just recreated the server, it might not be ready yet
                    if server is None or not server.is_ready():
                        logger.error(
                            f"Failed to get channel for {client_id} during reconnection: "
                            f"server={'None' if server is None else 'not ready'}"
                        )
                        raise RuntimeError(
                            "Failed to get channel during reconnection. "
                            "The specific server may not be ready yet. "
                            "Please try reconnecting again."
                        )
                    # Server exists but channel is None - this shouldn't happen
                    logger.error(
                        f"Failed to get channel for {client_id}: "
                        "server exists but channel is None"
                    )
                    raise RuntimeError(
                        "Failed to get channel during reconnection. "
                        "Please try reconnecting again."
                    )
            # If the backend is fresh (recreated, or we haven't observed an init ack yet),
            # it has no in-memory session state. Do NOT forward reconnecting=True downstream
            # (it would be rejected with NOT_FOUND). Prepend a synthetic InitRequest and
            # keep forcing reconnecting=False until we observe an init response.
            with self.clients_lock:
                needs_init = recreated_backend or self.backend_needs_init.get(
                    client_id, False
                )
                jc_bytes = self.job_config_bytes.get(client_id)

            if needs_init:
                init_kwargs = ray_init_kwargs or "{}"
                grace = int(self.reconnect_grace_periods.get(client_id, 0) or 0)
                if jc_bytes is None:
                    # Fallback: try to re-pickle the JobConfig object if bytes are missing.
                    if job_config is None:
                        raise RuntimeError(
                            "Internal error: backend needs init but job_config is missing."
                        )
                    jc_bytes = pickle.dumps(job_config)
                # IMPORTANT:
                # - The client never "mints" req_id=0 (it's reserved for unawaited /
                #   opportunistic requests).
                # - The specific-server's OrderedResponseCache assumes the streaming req_id
                #   sequence is positive int32 and monotonic with rollover handling.
                # Therefore, to avoid interfering with the client's req_id space *and* to
                # avoid OrderedResponseCache validation, we send the synthetic init with
                # req_id=0 and ensure `init` is not cached on the specific-server.
                synthetic_init = ray_client_pb2.DataRequest(
                    req_id=0,
                    init=ray_client_pb2.InitRequest(
                        job_config=jc_bytes,
                        ray_init_kwargs=init_kwargs,
                        reconnect_grace_period=grace,
                    ),
                )
                new_iter = chain([synthetic_init], request_iterator)
                downstream_reconnecting = False
            else:
                new_iter = request_iterator
                downstream_reconnecting = True
        else:
            # Create Placeholder *before* reading the first request.
            server = self.proxy_manager.create_specific_server(client_id)
            with self.clients_lock:
                # New stream means the session is active; cancel any pending cleanup.
                self._invalidate_cleanup_locked(client_id)
                self.clients_last_seen[client_id] = start_time
                self.num_clients += 1

        try:
            if not reconnecting:
                logger.info(f"New data connection from client {client_id}: ")
                init_req = next(request_iterator)
                with self.clients_lock:
                    self.reconnect_grace_periods[
                        client_id
                    ] = init_req.init.reconnect_grace_period
                    self.ray_init_kwargs[client_id] = init_req.init.ray_init_kwargs
                try:
                    modified_init_req, job_config = prepare_runtime_init_req(init_req)
                    # Save job_config for potential reconnection
                    with self.clients_lock:
                        self.job_configs[client_id] = job_config
                        # Persist the already-serialized bytes to avoid re-pickling on reconnect.
                        self.job_config_bytes[client_id] = modified_init_req.init.job_config
                        self.backend_needs_init[client_id] = True
                    if not self.proxy_manager.start_specific_server(
                        client_id, job_config
                    ):
                        logger.error(
                            f"Server startup failed for client: {client_id}, "
                            f"using JobConfig: {job_config}!"
                        )
                        raise RuntimeError(
                            "Starting Ray client server failed. See "
                            f"ray_client_server_{server.port}.err for "
                            "detailed logs."
                        )
                    channel = self.proxy_manager.get_channel(client_id)
                    if channel is None:
                        logger.error(f"Channel not found for {client_id}")
                        raise RuntimeError(
                            "Proxy failed to Connect to backend! Check "
                            "`ray_client_server.err` and "
                            f"`ray_client_server_{server.port}.err` on the "
                            "head node of the cluster for the relevant logs. "
                            "By default these are located at "
                            "/tmp/ray/session_latest/logs."
                        )
                except Exception:
                    init_resp = ray_client_pb2.DataResponse(
                        init=ray_client_pb2.InitResponse(
                            ok=False, msg=traceback.format_exc()
                        )
                    )
                    init_resp.req_id = init_req.req_id
                    yield init_resp
                    return None

                new_iter = chain([modified_init_req], request_iterator)

            stub = ray_client_pb2_grpc.RayletDataStreamerStub(channel)
            metadata = [
                ("client_id", client_id),
                ("reconnecting", str(downstream_reconnecting)),
            ]
            resp_stream = stub.Datapath(new_iter, metadata=metadata)
            for resp in resp_stream:
                resp_type = resp.WhichOneof("type")
                if resp_type == "init":
                    with self.clients_lock:
                        # Once we observe an init response from the backend, it is safe to
                        # treat subsequent reconnect attempts as reconnecting=True downstream.
                        if self.backend_needs_init.get(client_id):
                            self.backend_needs_init[client_id] = False
                if resp_type == "connection_cleanup":
                    # Specific server is skipping cleanup, proxier should too
                    cleanup_requested = True
                yield self.modify_connection_info_resp(resp)
        except Exception as e:
            logger.exception("Proxying Datapath failed!")
            # Propogate error through context
            recoverable = _propagate_error_in_context(e, context)
            if not recoverable:
                # Client shouldn't attempt to recover, clean up connection
                cleanup_requested = True
        finally:
            cleanup_delay = self.reconnect_grace_periods.get(client_id)
            if not cleanup_requested and cleanup_delay is not None:
                # DO NOT block here. Blocking the Datapath handler delays the client observing
                # the RpcError, which in turn delays reconnection until after the grace period.
                self._schedule_delayed_cleanup(client_id, start_time, cleanup_delay)
            else:
                with self.clients_lock:
                    # Cancel any scheduled cleanup and clean immediately.
                    self._invalidate_cleanup_locked(client_id)
                    if client_id in self.clients_last_seen:
                        if self.num_clients > 0:
                            self.num_clients -= 1
                        del self.clients_last_seen[client_id]
                    self.reconnect_grace_periods.pop(client_id, None)
                    self.job_configs.pop(client_id, None)
                    self.job_config_bytes.pop(client_id, None)
                    self.ray_init_kwargs.pop(client_id, None)
                    self.backend_needs_init.pop(client_id, None)

            if server is not None:
                server.set_result(None)


class LogstreamServicerProxy(ray_client_pb2_grpc.RayletLogStreamerServicer):
    def __init__(self, proxy_manager: ProxyManager):
        super().__init__()
        self.proxy_manager = proxy_manager

    def Logstream(self, request_iterator, context):
        request_iterator = RequestIteratorProxy(request_iterator)
        client_id = _get_client_id_from_context(context)
        if client_id == "":
            return
        logger.debug(f"New logstream connection from client {client_id}: ")

        channel = None
        # We need to retry a few times because the LogClient *may* connect
        # Before the DataClient has finished connecting.
        for i in range(LOGSTREAM_RETRIES):
            channel = self.proxy_manager.get_channel(client_id)

            if channel is not None:
                break
            logger.warning(f"Retrying Logstream connection. {i+1} attempts failed.")
            time.sleep(LOGSTREAM_RETRY_INTERVAL_SEC)

        if channel is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                "Logstream proxy failed to connect. Channel for client "
                f"{client_id} not found."
            )
            return None

        stub = ray_client_pb2_grpc.RayletLogStreamerStub(channel)

        resp_stream = stub.Logstream(
            request_iterator, metadata=[("client_id", client_id)]
        )
        try:
            for resp in resp_stream:
                yield resp
        except Exception:
            logger.exception("Proxying Logstream failed!")


def serve_proxier(
    host: str,
    port: int,
    gcs_address: Optional[str],
    *,
    redis_username: Optional[str] = None,
    redis_password: Optional[str] = None,
    session_dir: Optional[str] = None,
    runtime_env_agent_address: Optional[str] = None,
    node_id: Optional[str] = None,
):
    # Initialize internal KV to be used to upload and download working_dir
    # before calling ray.init within the RayletServicers.
    # NOTE(edoakes): redis_address and redis_password should only be None in
    # tests.
    if gcs_address is not None:
        gcs_cli = GcsClient(address=gcs_address)
        ray.experimental.internal_kv._initialize_internal_kv(gcs_cli)

    from ray._private.grpc_utils import create_grpc_server_with_interceptors

    server = create_grpc_server_with_interceptors(
        max_workers=CLIENT_SERVER_MAX_THREADS,
        thread_name_prefix="ray_client_proxier",
        options=GRPC_OPTIONS,
        asynchronous=False,
    )
    proxy_manager = ProxyManager(
        gcs_address,
        session_dir=session_dir,
        redis_username=redis_username,
        redis_password=redis_password,
        runtime_env_agent_address=runtime_env_agent_address,
        node_id=node_id,
    )
    task_servicer = RayletServicerProxy(None, proxy_manager)
    data_servicer = DataServicerProxy(proxy_manager)
    logs_servicer = LogstreamServicerProxy(proxy_manager)
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(task_servicer, server)
    ray_client_pb2_grpc.add_RayletDataStreamerServicer_to_server(data_servicer, server)
    ray_client_pb2_grpc.add_RayletLogStreamerServicer_to_server(logs_servicer, server)
    if not is_localhost(host):
        add_port_to_grpc_server(server, f"127.0.0.1:{port}")
    add_port_to_grpc_server(server, f"{host}:{port}")
    server.start()
    return ClientServerHandle(
        task_servicer=task_servicer,
        data_servicer=data_servicer,
        logs_servicer=logs_servicer,
        grpc_server=server,
    )
