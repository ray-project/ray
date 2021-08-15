import atexit
from concurrent import futures
from dataclasses import dataclass
import grpc
import logging
from itertools import chain
import json
import socket
import sys
from threading import Lock, Thread, RLock
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

import ray
from ray.cloudpickle.compat import pickle
from ray.job_config import JobConfig
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client.common import (ClientServerHandle,
                                    CLIENT_SERVER_MAX_THREADS, GRPC_OPTIONS)
from ray._private.parameter import RayParams
from ray._private.services import ProcessInfo, start_ray_client_server
from ray._private.utils import detect_fate_sharing_support

# Import psutil after ray so the packaged version is used.
import psutil

logger = logging.getLogger(__name__)

CHECK_PROCESS_INTERVAL_S = 30

MIN_SPECIFIC_SERVER_PORT = 23000
MAX_SPECIFIC_SERVER_PORT = 24000

CHECK_CHANNEL_TIMEOUT_S = 10

LOGSTREAM_RETRIES = 5
LOGSTREAM_RETRY_INTERVAL_SEC = 2


def _get_client_id_from_context(context: Any) -> str:
    """
    Get `client_id` from gRPC metadata. If the `client_id` is not present,
    this function logs an error and sets the status_code.
    """
    metadata = {k: v for k, v in context.invocation_metadata()}
    client_id = metadata.get("client_id") or ""
    if client_id == "":
        logger.error("Client connecting with no client_id")
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    return client_id


@dataclass
class SpecificServer:
    port: int
    process_handle_future: futures.Future
    channel: "grpc._channel.Channel"

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
        if not self.process_handle_future.done():
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


class ProxyManager():
    def __init__(self,
                 redis_address: Optional[str],
                 *,
                 session_dir: Optional[str] = None,
                 redis_password: Optional[str] = None):
        self.servers: Dict[str, SpecificServer] = dict()
        self.server_lock = RLock()
        self._redis_address = redis_address
        self._redis_password = redis_password
        self._free_ports: List[int] = list(
            range(MIN_SPECIFIC_SERVER_PORT, MAX_SPECIFIC_SERVER_PORT))

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
    def redis_address(self) -> str:
        """
        Returns the provided Ray Redis address, or creates a new cluster.
        """
        if self._redis_address:
            return self._redis_address
        # Start a new, locally scoped cluster.
        connection_tuple = ray.init()
        self._redis_address = connection_tuple["redis_address"]
        self._session_dir = connection_tuple["session_dir"]
        return self._redis_address

    @property
    def node(self) -> ray.node.Node:
        """Gets a 'ray.Node' object for this node (the head node).
        If it does not already exist, one is created using the redis_address.
        """
        if self._node:
            return self._node

        ray_params = RayParams(redis_address=self.redis_address)
        if self._redis_password:
            ray_params.redis_password = self._redis_password

        self._node = ray.node.Node(
            ray_params,
            head=False,
            shutdown_at_exit=False,
            spawn_reaper=False,
            connect_only=True)

        return self._node

    def create_specific_server(self, client_id: str) -> SpecificServer:
        """
        Create, but not start a SpecificServer for a given client. This
        method must be called once per client.
        """
        with self.server_lock:
            assert self.servers.get(client_id) is None, (
                f"Server already created for Client: {client_id}")
            port = self._get_unused_port()
            server = SpecificServer(
                port=port,
                process_handle_future=futures.Future(),
                channel=grpc.insecure_channel(
                    f"localhost:{port}", options=GRPC_OPTIONS))
            self.servers[client_id] = server
            return server

    def start_specific_server(self, client_id: str,
                              job_config: JobConfig) -> bool:
        """
        Start up a RayClient Server for an incoming client to
        communicate with. Returns whether creation was successful.
        """
        specific_server = self._get_server_for_client(client_id)
        assert specific_server, f"Server has not been created for: {client_id}"

        serialized_runtime_env = job_config.get_serialized_runtime_env()

        output, error = self.node.get_log_file_handles(
            f"ray_client_server_{specific_server.port}", unique=True)

        proc = start_ray_client_server(
            self.redis_address,
            specific_server.port,
            stdout_file=output,
            stderr_file=error,
            fate_share=self.fate_share,
            server_type="specific-server",
            serialized_runtime_env=serialized_runtime_env,
            session_dir=self.node.get_session_dir_path(),
            redis_password=self._redis_password)

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
                logger.error(
                    f"SpecificServer startup failed for client: {client_id}")
                break
            cmd = psutil_proc.cmdline()
            if _match_running_client_server(cmd):
                break
            logger.debug(
                "Waiting for Process to reach the actual client server.")
            time.sleep(0.5)
        specific_server.set_result(proc)
        logger.info(f"SpecificServer started on port: {specific_server.port} "
                    f"with PID: {pid} for client: {client_id}")
        return proc.process.poll() is None

    def _get_server_for_client(self,
                               client_id: str) -> Optional[SpecificServer]:
        with self.server_lock:
            client = self.servers.get(client_id)
            if client is None:
                logger.error(f"Unable to find channel for client: {client_id}")
            return client

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
            grpc.channel_ready_future(
                server.channel).result(timeout=CHECK_CHANNEL_TIMEOUT_S)
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
    def __init__(self, ray_connect_handler: Callable,
                 proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self.ray_connect_handler = ray_connect_handler

    def _call_inner_function(
            self, request, context,
            method: str) -> Optional[ray_client_pb2_grpc.RayletDriverStub]:
        client_id = _get_client_id_from_context(context)
        chan = self.proxy_manager.get_channel(client_id)
        if not chan:
            logger.error(f"Channel for Client: {client_id} not found!")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return None

        stub = ray_client_pb2_grpc.RayletDriverStub(chan)
        try:
            return getattr(stub, method)(
                request, metadata=[("client_id", client_id)])
        except Exception:
            logger.exception(f"Proxying call to {method} failed!")

    def Init(self, request, context=None) -> ray_client_pb2.InitResponse:
        return self._call_inner_function(request, context, "Init")

    def PrepRuntimeEnv(self, request,
                       context=None) -> ray_client_pb2.PrepRuntimeEnvResponse:
        return self._call_inner_function(request, context, "PrepRuntimeEnv")

    def KVPut(self, request, context=None) -> ray_client_pb2.KVPutResponse:
        return self._call_inner_function(request, context, "KVPut")

    def KVGet(self, request, context=None) -> ray_client_pb2.KVGetResponse:
        return self._call_inner_function(request, context, "KVGet")

    def KVDel(self, request, context=None) -> ray_client_pb2.KVDelResponse:
        return self._call_inner_function(request, context, "KVGet")

    def KVList(self, request, context=None) -> ray_client_pb2.KVListResponse:
        return self._call_inner_function(request, context, "KVList")

    def KVExists(self, request,
                 context=None) -> ray_client_pb2.KVExistsResponse:
        return self._call_inner_function(request, context, "KVExists")

    def ListNamedActors(self, request, context=None
                        ) -> ray_client_pb2.ClientListNamedActorsResponse:
        return self._call_inner_function(request, context, "ListNamedActors")

    def ClusterInfo(self, request,
                    context=None) -> ray_client_pb2.ClusterInfoResponse:

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

    def PutObject(self, request: ray_client_pb2.PutRequest,
                  context=None) -> ray_client_pb2.PutResponse:
        return self._call_inner_function(request, context, "PutObject")

    def WaitObject(self, request, context=None) -> ray_client_pb2.WaitResponse:
        return self._call_inner_function(request, context, "WaitObject")

    def Schedule(self, task, context=None) -> ray_client_pb2.ClientTaskTicket:
        return self._call_inner_function(task, context, "Schedule")


def ray_client_server_env_prep(job_config: JobConfig) -> JobConfig:
    return job_config


def prepare_runtime_init_req(init_request: ray_client_pb2.DataRequest
                             ) -> Tuple[ray_client_pb2.DataRequest, JobConfig]:
    """
    Extract JobConfig and possibly mutate InitRequest before it is passed to
    the specific RayClient Server.
    """
    init_type = init_request.WhichOneof("type")
    assert init_type == "init", ("Received initial message of type "
                                 f"{init_type}, not 'init'.")
    req = init_request.init
    job_config = JobConfig()
    if req.job_config:
        job_config = pickle.loads(req.job_config)
    new_job_config = ray_client_server_env_prep(job_config)
    modified_init_req = ray_client_pb2.InitRequest(
        job_config=pickle.dumps(new_job_config))

    init_request.init.CopyFrom(modified_init_req)
    return (init_request, new_job_config)


class DataServicerProxy(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, proxy_manager: ProxyManager):
        self.num_clients = 0
        self.clients_lock = Lock()
        self.proxy_manager = proxy_manager

    def modify_connection_info_resp(self,
                                    init_resp: ray_client_pb2.DataResponse
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
        client_id = _get_client_id_from_context(context)
        if client_id == "":
            return

        # Create Placeholder *before* reading the first request.
        server = self.proxy_manager.create_specific_server(client_id)
        try:
            with self.clients_lock:
                self.num_clients += 1

            logger.info(f"New data connection from client {client_id}: ")
            init_req = next(request_iterator)
            try:
                modified_init_req, job_config = prepare_runtime_init_req(
                    init_req)
                if not self.proxy_manager.start_specific_server(
                        client_id, job_config):
                    logger.error(
                        f"Server startup failed for client: {client_id}, "
                        f"using JobConfig: {job_config}!")
                    raise RuntimeError(
                        "Starting up Server Failed! Check "
                        "`ray_client_server_[port].err` on the cluster.")
                channel = self.proxy_manager.get_channel(client_id)
                if channel is None:
                    logger.error(f"Channel not found for {client_id}")
                    raise RuntimeError(
                        "Proxy failed to Connect to backend! Check "
                        "`ray_client_server.err` on the cluster.")
                stub = ray_client_pb2_grpc.RayletDataStreamerStub(channel)
            except Exception:
                init_resp = ray_client_pb2.DataResponse(
                    init=ray_client_pb2.InitResponse(
                        ok=False, msg=traceback.format_exc()))
                init_resp.req_id = init_req.req_id
                yield init_resp
                return None

            new_iter = chain([modified_init_req], request_iterator)
            resp_stream = stub.Datapath(
                new_iter, metadata=[("client_id", client_id)])
            for resp in resp_stream:
                yield self.modify_connection_info_resp(resp)
        except Exception:
            logger.exception("Proxying Datapath failed!")
        finally:
            server.set_result(None)
            with self.clients_lock:
                logger.debug(f"Client detached: {client_id}")
                self.num_clients -= 1


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
            logger.warning(
                f"Retrying Logstream connection. {i+1} attempts failed.")
            time.sleep(LOGSTREAM_RETRY_INTERVAL_SEC)

        if channel is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return None

        stub = ray_client_pb2_grpc.RayletLogStreamerStub(channel)

        resp_stream = stub.Logstream(
            request_iterator, metadata=[("client_id", client_id)])
        try:
            for resp in resp_stream:
                yield resp
        except Exception:
            logger.exception("Proxying Logstream failed!")


def serve_proxier(connection_str: str,
                  redis_address: str,
                  *,
                  redis_password: Optional[str] = None,
                  session_dir: Optional[str] = None):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
        options=GRPC_OPTIONS)
    proxy_manager = ProxyManager(
        redis_address, session_dir=session_dir, redis_password=redis_password)
    task_servicer = RayletServicerProxy(None, proxy_manager)
    data_servicer = DataServicerProxy(proxy_manager)
    logs_servicer = LogstreamServicerProxy(proxy_manager)
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    ray_client_pb2_grpc.add_RayletDataStreamerServicer_to_server(
        data_servicer, server)
    ray_client_pb2_grpc.add_RayletLogStreamerServicer_to_server(
        logs_servicer, server)
    server.add_insecure_port(connection_str)
    server.start()
    return ClientServerHandle(
        task_servicer=task_servicer,
        data_servicer=data_servicer,
        logs_servicer=logs_servicer,
        grpc_server=server,
    )
