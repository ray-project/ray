from typing import List
import asyncio

from ray._private.utils import init_grpc_channel
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc

from ray.dashboard.datacenter import DataSource
import ray.dashboard.modules.log.log_consts as log_consts


class LogsGrpcClient:
    def __init__(self):

        self._agent_stubs = {}
        self._ip_to_node_id = {}
        DataSource.agents.signal.append(self._update_stubs)

    async def wait_until_initialized(self):
        """
        Wait until connected to at least one node's log agent.
        """
        POLL_SLEEP_TIME = 0.5
        POLL_RETRIES = 10
        for _ in range(POLL_RETRIES):
            if self._agent_stubs != {}:
                return
            await asyncio.sleep(POLL_SLEEP_TIME)
        raise ValueError("Could not connect to agents via gRPC after "
                         f"{POLL_SLEEP_TIME * POLL_RETRIES} seconds.")

    def unregister_raylet_client(self, node_id: str):
        self._raylet_stubs.pop(node_id)

    def register_agent_client(self, node_id, address: str, port: int):
        options = (("grpc.enable_http_proxy", 0),)
        channel = init_grpc_channel(
            f"{address}:{port}", options=options, asynchronous=True
        )
        self._agent_stubs[node_id] = reporter_pb2_grpc.LogServiceStub(channel)

    def unregister_agent_client(self, node_id: str):
        self._agent_stubs.pop(node_id)

    async def _update_stubs(self, change):
        if change.old:
            node_id, _ = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self.unregister_agent_client(node_id)
            self._ip_to_node_id.pop(ip)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]
            self.register_agent_client(node_id, ip, ports[1])
            self._ip_to_node_id[ip] = node_id

    def get_all_registered_nodes(self) -> List[str]:
        return self._agent_stubs.keys()

    async def list_logs(self, node_id: str, timeout: int = None):
        stub = self._agent_stubs.get(node_id)
        if not stub:
            raise ValueError(f"Agent for node id: {node_id} doesn't exist.")
        return await stub.ListLogs(
            reporter_pb2.ListLogsRequest(), timeout=log_consts.GRPC_TIMEOUT
        )

    async def stream_log(
        self,
        node_id: str,
        log_file_name: str,
        keep_alive: bool,
        lines: int,
        interval: float
    ):
        stub = self._agent_stubs.get(node_id)
        if not stub:
            raise ValueError(f"Agent for node id: {node_id} doesn't exist.")
        stream = stub.StreamLog(
            reporter_pb2.StreamLogRequest(
                keep_alive=keep_alive,
                log_file_name=log_file_name,
                lines=lines,
                interval=interval,
            )
        )
        await self._validate_stream(stream)
        return stream

    @staticmethod
    async def _validate_stream(stream):
        metadata = await stream.initial_metadata()
        if metadata.get(log_consts.LOG_GRPC_ERROR) == log_consts.FILE_NOT_FOUND:
            raise ValueError('File "{log_file_name}" not found on node {node_id}')
