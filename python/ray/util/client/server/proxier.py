from concurrent import futures
from dataclasses import dataclass
import grpc
import logging
import json
from queue import Queue
import socket
from threading import Thread, Lock
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from ray.job_config import JobConfig
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client.common import (ClientServerHandle,
                                    CLIENT_SERVER_MAX_THREADS, GRPC_OPTIONS)
from ray._private.services import ProcessInfo, start_ray_client_server
from ray._private.utils import detect_fate_sharing_support

logger = logging.getLogger(__name__)

CHECK_PROCESS_INTERVAL_S = 30

MIN_SPECIFIC_SERVER_PORT = 23000
MAX_SPECIFIC_SERVER_PORT = 24000

CHECK_CHANNEL_TIMEOUT_S = 5


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
    process_handle: ProcessInfo
    channel: "grpc._channel.Channel"


class ProxyManager():
    def __init__(self, redis_address):
        self.servers: Dict[str, SpecificServer] = dict()
        self.server_lock = Lock()
        self.redis_address = redis_address
        self._free_ports: List[int] = list(
            range(MIN_SPECIFIC_SERVER_PORT, MAX_SPECIFIC_SERVER_PORT))

        self._check_thread = Thread(target=self._check_processes, daemon=True)
        self._check_thread.start()

        self.fate_share = bool(detect_fate_sharing_support())

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

    def start_specific_server(self, client_id: str,
                              job_config: JobConfig) -> None:
        """
        Start up a RayClient Server for an incoming client to
        communicate with.
        """
        port = self._get_unused_port()
        serialized_runtime_env = job_config.get_serialized_runtime_env()
        proc = start_ray_client_server(
            self.redis_address,
            port,
            fate_share=self.fate_share,
            server_type="specific-server",
            serialized_runtime_env=serialized_runtime_env)

        specific_server = SpecificServer(
            port=port,
            process_handle=proc,
            channel=grpc.insecure_channel(
                f"localhost:{port}", options=GRPC_OPTIONS))
        with self.server_lock:
            self.servers[client_id] = specific_server

    def get_channel(self, client_id: str) -> Optional["grpc._channel.Channel"]:
        """
        Find the gRPC Channel for the given client_id
        """
        client = None
        with self.server_lock:
            client = self.servers.get(client_id)
            if client is None:
                logger.error(f"Unable to find channel for client: {client_id}")
                return None
        try:
            grpc.channel_ready_future(
                client.channel).result(timeout=CHECK_CHANNEL_TIMEOUT_S)
            return client.channel
        except grpc.FutureTimeoutError:
            return None

    def _check_processes(self):
        """
        Keeps the internal servers dictionary up-to-date with running servers.
        """
        while True:
            with self.server_lock:
                for client_id, specific_server in list(self.servers.items()):
                    poll_result = specific_server.process_handle.process.poll()
                    if poll_result is not None:
                        del self.servers[client_id]
                        # Port is available to use again.
                        self._free_ports.append(specific_server.port)

            time.sleep(CHECK_PROCESS_INTERVAL_S)


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
        return getattr(stub, method)(
            request, metadata=[("client_id", client_id)])

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


def forward_streaming_requests(grpc_input_generator: Iterator[Any],
                               output_queue: "Queue") -> None:
    """
    Forwards streaming requests from the grpc_input_generator into the
    output_queue.
    """
    try:
        for req in grpc_input_generator:
            output_queue.put(req)
    except grpc.RpcError as e:
        logger.debug("closing dataservicer reader thread "
                     f"grpc error reading request_iterator: {e}")
    finally:
        # Set the sentinel value for the output_queue
        output_queue.put(None)


def ray_client_server_env_prep(job_config: JobConfig) -> JobConfig:
    return job_config


def prepare_runtime_init_req(req: ray_client_pb2.InitRequest
                             ) -> Tuple[ray_client_pb2.InitRequest, JobConfig]:
    """
    Extract JobConfig and possibly mutate InitRequest before it is passed to
    the specific RayClient Server.
    """
    import pickle
    job_config = JobConfig()
    if req.job_config:
        job_config = pickle.loads(req.job_config)
    new_job_config = ray_client_server_env_prep(job_config)
    return (
        ray_client_pb2.InitRequest(job_config=pickle.dumps(new_job_config)),
        new_job_config)


class DataServicerProxy(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager

    def Datapath(self, request_iterator, context):
        client_id = _get_client_id_from_context(context)
        if client_id == "":
            return
        logger.debug(f"New data connection from client {client_id}: ")

        init_req = next(request_iterator)
        init_type = init_req.WhichOneof("type")
        assert init_type == "init", ("Received initial message of type "
                                     f"{init_type}, not 'init'.")

        modified_init_req, job_config = prepare_runtime_init_req(init_req.init)
        init_req.init.CopyFrom(modified_init_req)
        queue = Queue()
        queue.put(init_req)

        self.proxy_manager.start_specific_server(client_id, job_config)

        channel = self.proxy_manager.get_channel(client_id)
        if channel is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return None
        stub = ray_client_pb2_grpc.RayletDataStreamerStub(channel)
        thread = Thread(
            target=forward_streaming_requests,
            args=(request_iterator, queue),
            daemon=True)
        thread.start()

        resp_stream = stub.Datapath(
            iter(queue.get, None), metadata=[("client_id", client_id)])
        for resp in resp_stream:
            yield resp


class LogstreamServicerProxy(ray_client_pb2_grpc.RayletLogStreamerServicer):
    def __init__(self, proxy_manager: ProxyManager):
        super().__init__()
        self.proxy_manager = proxy_manager

    def Logstream(self, request_iterator, context):
        client_id = _get_client_id_from_context(context)
        if client_id == "":
            return
        logger.debug(f"New data connection from client {client_id}: ")

        channel = None
        for i in range(10):
            # TODO(ilr) Ensure LogClient starts after startup has happened.
            # This will remove the need for retries here.
            channel = self.proxy_manager.get_channel(client_id)

            if channel is not None:
                break
            logger.warning(
                f"Retrying Logstream connection. {i+1} attempts failed.")
            time.sleep(5)

        if channel is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return None

        stub = ray_client_pb2_grpc.RayletLogStreamerStub(channel)
        queue = Queue()
        thread = Thread(
            target=forward_streaming_requests,
            args=(request_iterator, queue),
            daemon=True)
        thread.start()

        resp_stream = stub.Logstream(
            iter(queue.get, None), metadata=[("client_id", client_id)])
        for resp in resp_stream:
            yield resp


def serve_proxier(connection_str: str, redis_address: str):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
        options=GRPC_OPTIONS)
    proxy_manager = ProxyManager(redis_address)
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
