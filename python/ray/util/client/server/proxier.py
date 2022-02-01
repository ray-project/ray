import atexit
from concurrent import futures
from dataclasses import dataclass
import grpc
import logging
from itertools import chain
import json
import socket
import sys
from threading import Event, Lock, Thread, RLock
import time
import traceback
from typing import Callable, Dict, List, Optional, Tuple

import ray
from ray.cloudpickle.compat import pickle
from ray.job_config import JobConfig
import ray.core.generated.agent_manager_pb2 as agent_manager_pb2
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import ray.core.generated.runtime_env_agent_pb2 as runtime_env_agent_pb2
import ray.core.generated.runtime_env_agent_pb2_grpc as runtime_env_agent_pb2_grpc  # noqa: E501
from ray.util.client.common import (
    _get_client_id_from_context,
    ClientServerHandle,
    CLIENT_SERVER_MAX_THREADS,
    GRPC_OPTIONS,
    _propagate_error_in_context,
)
from ray.util.client.server.dataservicer import _get_reconnecting_from_context
from ray._private.client_mode_hook import disable_client_hook
from ray._private.parameter import RayParams
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.services import ProcessInfo, start_ray_client_server
from ray._private.tls_utils import add_port_to_grpc_server
from ray._private.gcs_utils import GcsClient, use_gcs_for_bootstrap
from ray._private.utils import detect_fate_sharing_support

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
    This works by ensuring that the the first three arguments are similar to:
        <python> -m ray.util.client.server
    """
    flattened = " ".join(command)
    rejoined = flattened.split()
    if len(rejoined) < 3:
        return False
    return rejoined[1:3] == ["-m", "ray.util.client.server"]


class ProxyManager:
    def __init__(
        self,
        address: Optional[str],
        *,
        session_dir: Optional[str] = None,
        redis_password: Optional[str] = None,
        runtime_env_agent_port: int = 0,
    ):
        self.servers: Dict[str, SpecificServer] = dict()
        self.server_lock = RLock()
        self._address = address
        self._redis_password = redis_password
        self._free_ports: List[int] = list(
            range(MIN_SPECIFIC_SERVER_PORT, MAX_SPECIFIC_SERVER_PORT)
        )

        self._runtime_env_channel = ray._private.utils.init_grpc_channel(
            f"127.0.0.1:{runtime_env_agent_port}"
        )
        self._runtime_env_stub = (
            runtime_env_agent_pb2_grpc.RuntimeEnvServiceStub(  # noqa: E501
                self._runtime_env_channel
            )
        )

        self._check_thread = Thread(target=self._check_processes, daemon=True)
        self._check_thread.start()

        self.fate_share = bool(detect_fate_sharing_support())
        self._node: Optional[ray.node.Node] = None
        atexit.register(self._cleanup)

    def _get_unused_port(self) -> int:
        """
        Search for a port in _free_ports that is unused.
        """
        with self.server_lock:
            num_ports = len(self._free_ports)
            for _ in range(num_ports):
                port = self._free_ports.pop(0)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
    def node(self) -> ray.node.Node:
        """Gets a 'ray.Node' object for this node (the head node).
        If it does not already exist, one is created using the bootstrap
        address.
        """
        if self._node:
            return self._node
        if use_gcs_for_bootstrap():
            ray_params = RayParams(gcs_address=self.address)
        else:
            ray_params = RayParams(redis_address=self.address)
            if self._redis_password:
                ray_params.redis_password = self._redis_password

        self._node = ray.node.Node(
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
            port = self._get_unused_port()
            server = SpecificServer(
                port=port,
                process_handle_future=futures.Future(),
                channel=ray._private.utils.init_grpc_channel(
                    f"127.0.0.1:{port}", options=GRPC_OPTIONS
                ),
            )
            self.servers[client_id] = server
            return server

    def _create_runtime_env(
        self, serialized_runtime_env: str, specific_server: SpecificServer
    ):
        """Creates the runtime_env by sending an RPC to the agent.

        Includes retry logic to handle the case when the agent is
        temporarily unreachable (e.g., hasn't been started up yet).
        """
        create_env_request = runtime_env_agent_pb2.CreateRuntimeEnvRequest(
            serialized_runtime_env=serialized_runtime_env,
            job_id=f"ray_client_server_{specific_server.port}".encode("utf-8"),
        )

        retries = 0
        max_retries = 5
        wait_time_s = 0.5
        while retries <= max_retries:
            try:
                r = self._runtime_env_stub.CreateRuntimeEnv(create_env_request)
                if r.status == agent_manager_pb2.AgentRpcStatus.AGENT_RPC_STATUS_OK:
                    return r.serialized_runtime_env_context
                elif (
                    r.status == agent_manager_pb2.AgentRpcStatus.AGENT_RPC_STATUS_FAILED
                ):
                    raise RuntimeError(
                        "Failed to create runtime_env for Ray client "
                        f"server: {r.error_message}"
                    )
                else:
                    assert False, f"Unknown status: {r.status}."
            except grpc.RpcError as e:
                # Whitelist of errors we consider transient.
                # NOTE(edoakes): we can get UNIMPLEMENTED while the server
                # starts up because the agent runs multiple gRPC services
                # on the same port.
                if e.code() not in [
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.UNIMPLEMENTED,
                ]:
                    raise e

                logger.warning(
                    f"CreateRuntimeEnv request failed: {e}. "
                    f"Retrying after {wait_time_s}s. "
                    f"{max_retries-retries} retries remaining."
                )

            # Exponential backoff.
            time.sleep(wait_time_s)
            retries += 1
            wait_time_s *= 2

        raise TimeoutError(
            f"CreateRuntimeEnv request failed after {max_retries} attempts."
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

        serialized_runtime_env = job_config.get_serialized_runtime_env()
        if not serialized_runtime_env or serialized_runtime_env == "{}":
            # TODO(edoakes): can we just remove this case and always send it
            # to the agent?
            serialized_runtime_env_context = RuntimeEnvContext().serialize()
        else:
            serialized_runtime_env_context = self._create_runtime_env(
                serialized_runtime_env=serialized_runtime_env,
                specific_server=specific_server,
            )

        proc = start_ray_client_server(
            self.address,
            self.node.node_ip_address,
            specific_server.port,
            stdout_file=output,
            stderr_file=error,
            fate_share=self.fate_share,
            server_type="specific-server",
            serialized_runtime_env_context=serialized_runtime_env_context,
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
            grpc.channel_ready_future(server.channel).result(
                timeout=CHECK_CHANNEL_TIMEOUT_S
            )
            return server.channel
        except grpc.FutureTimeoutError:
            logger.exception(f"Timeout waiting for channel for {client_id}")
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
        chan = self.proxy_manager.get_channel(client_id)
        if not chan:
            logger.error(f"Channel for Client: {client_id} not found!")
            context.set_code(grpc.StatusCode.NOT_FOUND)
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
            return self._call_inner_function(request, context, "KVGet")

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
        return self._call_inner_function(request, context, "GetObject")

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


class DataServicerProxy(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, proxy_manager: ProxyManager):
        self.num_clients = 0
        # dictionary mapping client_id's to the last time they connected
        self.clients_last_seen: Dict[str, float] = {}
        self.reconnect_grace_periods: Dict[str, float] = {}
        self.clients_lock = Lock()
        self.proxy_manager = proxy_manager
        self.stopped = Event()

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
        cleanup_requested = False
        start_time = time.time()
        client_id = _get_client_id_from_context(context)
        if client_id == "":
            return
        reconnecting = _get_reconnecting_from_context(context)

        if reconnecting:
            with self.clients_lock:
                if client_id not in self.clients_last_seen:
                    # Client took too long to reconnect, session has already
                    # been cleaned up
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(
                        "Attempted to reconnect a session that has already "
                        "been cleaned up"
                    )
                    return
                self.clients_last_seen[client_id] = start_time
            server = self.proxy_manager._get_server_for_client(client_id)
            channel = self.proxy_manager.get_channel(client_id)
            # iterator doesn't need modification on reconnect
            new_iter = request_iterator
        else:
            # Create Placeholder *before* reading the first request.
            server = self.proxy_manager.create_specific_server(client_id)
            with self.clients_lock:
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
                try:
                    modified_init_req, job_config = prepare_runtime_init_req(init_req)
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
            metadata = [("client_id", client_id), ("reconnecting", str(reconnecting))]
            resp_stream = stub.Datapath(new_iter, metadata=metadata)
            for resp in resp_stream:
                resp_type = resp.WhichOneof("type")
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
                # Delay cleanup, since client may attempt a reconnect
                # Wait on stopped event in case the server closes and we
                # can clean up earlier
                self.stopped.wait(timeout=cleanup_delay)
            with self.clients_lock:
                if client_id not in self.clients_last_seen:
                    logger.info(f"{client_id} not found. Skipping clean up.")
                    # Connection has already been cleaned up
                    return
                last_seen = self.clients_last_seen[client_id]
                logger.info(
                    f"{client_id} last started stream at {last_seen}. Current "
                    f"stream started at {start_time}."
                )
                if last_seen > start_time:
                    logger.info("Client reconnected. Skipping cleanup.")
                    # Client has reconnected, don't clean up
                    return
                logger.debug(f"Client detached: {client_id}")
                self.num_clients -= 1
                del self.clients_last_seen[client_id]
                if client_id in self.reconnect_grace_periods:
                    del self.reconnect_grace_periods[client_id]
                server.set_result(None)


class LogstreamServicerProxy(ray_client_pb2_grpc.RayletLogStreamerServicer):
    def __init__(self, proxy_manager: ProxyManager):
        super().__init__()
        self.proxy_manager = proxy_manager

    def Logstream(self, request_iterator, context):
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
    connection_str: str,
    address: Optional[str],
    *,
    redis_password: Optional[str] = None,
    session_dir: Optional[str] = None,
    runtime_env_agent_port: int = 0,
):
    # Initialize internal KV to be used to upload and download working_dir
    # before calling ray.init within the RayletServicers.
    # NOTE(edoakes): redis_address and redis_password should only be None in
    # tests.
    if use_gcs_for_bootstrap():
        if address is not None:
            gcs_cli = GcsClient(address=address)
            ray.experimental.internal_kv._initialize_internal_kv(gcs_cli)
    else:
        if address is not None and redis_password is not None:
            gcs_cli = GcsClient.connect_to_gcs_by_redis_address(address, redis_password)
            ray.experimental.internal_kv._initialize_internal_kv(gcs_cli)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
        options=GRPC_OPTIONS,
    )
    proxy_manager = ProxyManager(
        address,
        session_dir=session_dir,
        redis_password=redis_password,
        runtime_env_agent_port=runtime_env_agent_port,
    )
    task_servicer = RayletServicerProxy(None, proxy_manager)
    data_servicer = DataServicerProxy(proxy_manager)
    logs_servicer = LogstreamServicerProxy(proxy_manager)
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(task_servicer, server)
    ray_client_pb2_grpc.add_RayletDataStreamerServicer_to_server(data_servicer, server)
    ray_client_pb2_grpc.add_RayletLogStreamerServicer_to_server(logs_servicer, server)
    add_port_to_grpc_server(server, connection_str)
    server.start()
    return ClientServerHandle(
        task_servicer=task_servicer,
        data_servicer=data_servicer,
        logs_servicer=logs_servicer,
        grpc_server=server,
    )
