from typing import List, Tuple, cast
from dataclasses import dataclass, fields, is_dataclass
import asyncio
import aiohttp.web

import ray.dashboard.modules.log.log_consts as log_consts
from ray import ray_constants
from ray._private.utils import init_grpc_channel
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.dashboard.datacenter import DataSource


@dataclass(init=True)
class NodeIdentifiers:
    node_id: str
    node_ip: str


@dataclass(init=True)
class FileIdentifiers:
    log_file_name: str
    actor_id: str
    pid: str


@dataclass(init=True)
class LogIdentifiers:
    file: FileIdentifiers
    node: NodeIdentifiers


@dataclass(init=True)
class LogStreamOptions:
    media_type: str
    lines: int
    interval: float


def to_schema(req: aiohttp.web.Request, Dataclass):
    kwargs = {}
    for field in fields(Dataclass):
        if is_dataclass(field.type):
            # recursively resolve the dataclass
            kwargs[field.name] = to_schema(req, field.type)
        else:
            value = req.query.get(field.name)
            if value is None:
                value = req.match_info.get(field.name)
            # This is brittle as the class needs to be
            # instantiable from the str
            kwargs[field.name] = field.type(value) if value else None
    return Dataclass(**kwargs)


class GrpcLogsClient:
    def __init__(self):
        self._stubs = {}
        self._ip_to_node_id = {}
        DataSource.agents.signal.append(self._update_stubs)

    async def list_logs(self, node_id_query: str, filters: List[str]):
        """
        Helper function to list the logs by querying each agent
        on each cluster via gRPC.
        """
        response = {}
        tasks = []
        for node_id, grpc_stub in self._stubs.items():
            if node_id_query is None or node_id_query == node_id:

                async def coro():
                    reply = await grpc_stub.ListLogs(
                        reporter_pb2.ListLogsRequest(), timeout=log_consts.GRPC_TIMEOUT
                    )
                    response[node_id] = self._list_logs_single_node(
                        reply.log_files, filters
                    )

                tasks.append(coro())
        await asyncio.gather(*tasks)
        return response

    async def wait_until_initialized(self):
        """
        Wait until connected to at least one node's log agent.
        """
        POLL_SLEEP_TIME = 0.5
        POLL_RETRIES = 10
        for _ in range(POLL_RETRIES):
            if self._stubs != {}:
                return None
            await asyncio.sleep(POLL_SLEEP_TIME)
        raise ValueError("Could not connect to agents via gRPC after "
                         f"{POLL_SLEEP_TIME * POLL_RETRIES} seconds.")

    async def _update_stubs(self, change):
        if change.old:
            node_id, _ = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(node_id)
            self._ip_to_node_id.pop(ip)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]

            options = (("grpc.enable_http_proxy", 0),)
            channel = init_grpc_channel(
                f"{ip}:{ports[1]}", options=options, asynchronous=True
            )
            stub = reporter_pb2_grpc.LogServiceStub(channel)
            self._stubs[node_id] = stub
            self._ip_to_node_id[ip] = node_id

    def resolve_node_id(self, args: NodeIdentifiers):
        node_id = args.node_id
        if node_id is None:
            ip = args.node_ip
            if ip is not None:
                if ip not in self._ip_to_node_id:
                    raise ValueError(f"node_ip: {ip} not found")
                node_id = self._ip_to_node_id[ip]
        elif node_id not in self._stubs:
            raise ValueError(f"node_id: {node_id} not found")
        return node_id

    async def _resolve_file_and_node(
        self,
        args: LogIdentifiers
    ):
        node_id = self.resolve_node_id(args.node)
        log_file_name = args.file.log_file_name

        # If `log_file_name` is not provided, check if we can get the
        # corresponding `log_file_name` if an `actor_id` is provided.
        if log_file_name is None:
            if args.file.actor_id is not None:
                actor_data = DataSource.actors.get(args.file.actor_id)
                if actor_data is None:
                    raise ValueError("Actor ID {actor_id} not found.")
                worker_id = actor_data["address"].get("workerId")
                if worker_id is None:
                    raise ValueError("Worker ID for Actor ID {actor_id} not found.")

                index = await self.list_logs(node_id, [worker_id])
                for node in index:
                    for file in index[node]["worker_outs"]:
                        if file.split(".")[0].split("-")[1] == worker_id:
                            log_file_name = file
                            if node_id is None:
                                node_id = node
                            break

        # If `log_file_name` cannot be resolved, check if we can get the
        # corresponding `log_file_name` if a `pid` is provided.
        if log_file_name is None:
            pid = args.file.pid
            if pid is not None:
                if node_id is None:
                    raise ValueError("Node identifiers (node_ip, node_id) not provided "
                                     f"with pid: {pid}. Available: {self._ip_to_node_id}")
                index = await self.list_logs(node_id, [pid])
                for file in index[node_id]["worker_outs"]:
                    if file.split(".")[0].split("-")[3] == pid:
                        log_file_name = file
                        break
                if log_file_name is None:
                    raise ValueError(
                        f"Worker with pid {pid} not found on node {node_id}")

        # node_id and log_file_name need to be known by this point
        if node_id is None or node_id not in self._stubs:
            raise ValueError(f"node_id {node_id} not found")
        if log_file_name is None:
            raise ValueError("Could not resolve file identifiers to a file name.")

        return log_file_name, node_id

    @staticmethod
    async def _validate_grpc_log_stream(stream):
        metadata = await stream.initial_metadata()
        if metadata.get(log_consts.LOG_GRPC_ERROR) == log_consts.FILE_NOT_FOUND:
            raise ValueError('File "{log_file_name}" not found on node {node_id}')

    async def create_and_validate_log_stream(
        self,
        identifiers: LogIdentifiers,
        stream_options: LogStreamOptions,
    ):
        log_file_name, node_id = await self._resolve_file_and_node(identifiers)

        if stream_options.media_type == "stream":
            keep_alive = True
        elif stream_options.media_type == "file":
            keep_alive = False
        else:
            raise ValueError("Invalid media type: {media_type}")

        stream = self._stubs[node_id].StreamLog(
            reporter_pb2.StreamLogRequest(
                keep_alive=keep_alive,
                log_file_name=log_file_name,
                lines=stream_options.lines,
                interval=stream_options.interval,
            )
        )

        await self._validate_grpc_log_stream(stream)

        return stream

    @staticmethod
    def _list_logs_single_node(log_files: List[str], filters: List[str]):
        """
        Returns a JSON file mapping a category of log component to a list of filenames,
        on the given node.
        """
        filters = [] if filters == [""] else filters

        def contains_all_filters(log_file_name):
            return all(f in log_file_name for f in filters)

        filtered = list(filter(contains_all_filters, log_files))
        logs = {}
        logs["worker_errors"] = list(
            filter(lambda s: "worker" in s and s.endswith(".err"), filtered)
        )
        logs["worker_outs"] = list(
            filter(lambda s: "worker" in s and s.endswith(".out"), filtered)
        )
        for lang in ray_constants.LANGUAGE_WORKER_TYPES:
            logs[f"{lang}_core_worker_logs"] = list(
                filter(
                    lambda s: f"{lang}-core-worker" in s and s.endswith(".log"),
                    filtered,
                )
            )
            logs[f"{lang}_driver_logs"] = list(
                filter(
                    lambda s: f"{lang}-core-driver" in s and s.endswith(".log"),
                    filtered,
                )
            )
        logs["dashboard"] = list(filter(lambda s: "dashboard" in s, filtered))
        logs["raylet"] = list(filter(lambda s: "raylet" in s, filtered))
        logs["gcs_server"] = list(filter(lambda s: "gcs" in s, filtered))
        logs["ray_client"] = list(filter(lambda s: "ray_client" in s, filtered))
        logs["autoscaler"] = list(
            filter(lambda s: "monitor" in s and "log_monitor" not in s, filtered)
        )
        logs["runtime_env"] = list(filter(lambda s: "runtime_env" in s, filtered))
        logs["folders"] = list(filter(lambda s: "." not in s, filtered))
        logs["misc"] = list(
            filter(lambda s: all([s not in logs[k] for k in logs]), filtered)
        )
        return logs
