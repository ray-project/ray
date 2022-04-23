import logging

import aiohttp.web
import ray.dashboard.modules.log.log_utils as log_utils
import ray.dashboard.modules.log.log_consts as log_consts
import ray.dashboard.utils as dashboard_utils
from ray._private.utils import init_grpc_channel
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.datacenter import DataSource, GlobalSignals
from ray import ray_constants
from typing import List, Tuple, cast
from dataclasses import dataclass, fields, is_dataclass
import asyncio

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class LogHead(dashboard_utils.DashboardHeadModule):
    LOG_URL_TEMPLATE = "http://{ip}:{port}/logs"

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # We disable auto_decompress when forward / proxy log url.
        self._proxy_session = aiohttp.ClientSession(auto_decompress=False)
        log_utils.register_mimetypes()
        routes.static("/logs", self._dashboard_head.log_dir, show_index=True)
        GlobalSignals.node_info_fetched.append(self.insert_log_url_to_node_info)
        GlobalSignals.node_summary_fetched.append(self.insert_log_url_to_node_info)

    async def insert_log_url_to_node_info(self, node_info):
        node_id = node_info.get("raylet", {}).get("nodeId")
        if node_id is None:
            return
        agent_port = DataSource.agents.get(node_id)
        if agent_port is None:
            return
        agent_http_port, _ = agent_port
        log_url = self.LOG_URL_TEMPLATE.format(
            ip=node_info.get("ip"), port=agent_http_port
        )
        node_info["logUrl"] = log_url

    @routes.get("/log_index")
    async def get_log_index(self, req) -> aiohttp.web.Response:
        url_list = []
        agent_ips = []
        for node_id, ports in DataSource.agents.items():
            ip = DataSource.node_id_to_ip[node_id]
            agent_ips.append(ip)
            url_list.append(self.LOG_URL_TEMPLATE.format(ip=ip, port=str(ports[0])))
        if self._dashboard_head.ip not in agent_ips:
            url_list.append(
                self.LOG_URL_TEMPLATE.format(
                    ip=self._dashboard_head.ip, port=self._dashboard_head.http_port
                )
            )
        return aiohttp.web.Response(
            text=self._directory_as_html(url_list), content_type="text/html"
        )

    @routes.get("/log_proxy")
    async def get_log_from_proxy(self, req) -> aiohttp.web.StreamResponse:
        url = req.query.get("url")
        if not url:
            raise Exception("url is None.")
        body = await req.read()
        async with self._proxy_session.request(
            req.method, url, data=body, headers=req.headers
        ) as r:
            sr = aiohttp.web.StreamResponse(
                status=r.status, reason=r.reason, headers=req.headers
            )
            sr.content_length = r.content_length
            sr.content_type = r.content_type
            sr.charset = r.charset

            writer = await sr.prepare(req)
            async for data in r.content.iter_any():
                await writer.write(data)

            return sr

    @staticmethod
    def _directory_as_html(url_list) -> str:
        # returns directory's index as html

        index_of = "Index of logs"
        h1 = f"<h1>{index_of}</h1>"

        index_list = []
        for url in sorted(url_list):
            index_list.append(f'<li><a href="{url}">{url}</a></li>')
        index_list = "\n".join(index_list)
        ul = f"<ul>\n{index_list}\n</ul>"
        body = f"<body>\n{h1}\n{ul}\n</body>"

        head_str = f"<head>\n<title>{index_of}</title>\n</head>"
        html = f"<html>\n{head_str}\n{body}\n</html>"

        return html

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False


class LogHeadV1(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.grpc_logs_client = GrpcLogsClient()

    @routes.get("/api/experimental/logs/list")
    async def handle_list_logs(self, req: aiohttp.web.Request):
        """
        Returns a JSON file containing, for each node in the cluster,
        a dict mapping a category of log component to a list of filenames.
        """
        try:
            filters = req.query.get("filters", "").split(",")
            await self.grpc_logs_client._wait_until_initialized()
            node_id = self.grpc_logs_client._resolve_node_id(
                to_schema(req, NodeResolverArgs))
            response = await self.grpc_logs_client._list_logs(node_id, filters)
            return aiohttp.web.json_response(response)

        except Exception as e:
            logger.exception(e)
            return aiohttp.web.HTTPInternalServerError(reason=e)

    @routes.get("/api/experimental/logs/{media_type}")
    async def handle_get_log(self, req: aiohttp.web.Request):
        """
        If `media_type = stream`, creates HTTP stream which is either kept alive while
        the HTTP connection is not closed. Else, if `media_type = file`, the stream
        ends once all the lines in the file requested are transmitted.
        """
        response = None
        try:
            await self.grpc_logs_client._wait_until_initialized()

            stream = await self.grpc_logs_client._create_and_validate_log_stream(
                resolver_args=to_schema(req, NodeResolverArgs),
                stream_options=to_schema(req, LogStreamOptions)
            )

            response = aiohttp.web.StreamResponse()
            response.content_type = "text/plain"
            await response.prepare(req)

            # try-except here in order to properly handle ongoing HTTP stream
            try:
                async for log_response in stream:
                    await response.write(log_response.data)
                await response.write_eof()
                return response
            except Exception as e:
                logger.exception(str(e))
                await response.write(
                    b"Closing HTTP stream due to internal server error:\n"
                )
                await response.write(str(e).encode())
                await response.write_eof()
                return response
        except Exception as e:
            logger.exception(e)
            return aiohttp.web.HTTPInternalServerError(reason=e)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False


def to_schema(req, Dataclass):
    kwargs = {}
    for field in fields(Dataclass):
        print(field.type)
        if is_dataclass(field.type):
            # recursively resolve the dataclass
            kwargs[field.name] = to_schema(req, field.type)
        else:
            value = req.get(field.name)
            kwargs[field.name] = cast(field.type, value) if value else None
    print(kwargs)
    return Dataclass(**kwargs)


@dataclass(init=True)
class NodeResolverArgs:
    node_id: str
    node_ip: str


@dataclass(init=True)
class FileResolverArgs:
    log_file_name: str
    actor_id: str
    pid: str


@dataclass(init=True)
class ResolverArgs:
    file: FileResolverArgs
    node: NodeResolverArgs


@dataclass(init=True)
class LogStreamOptions:
    media_type: str
    lines: int
    interval: float

class GrpcLogsClient:
    def __init__(self):
        self._stubs = {}
        self._ip_to_node_id = {}
        DataSource.agents.signal.append(self._update_stubs)

    async def _list_logs(self, node_id_query: str, filters: List[str]):
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

    async def _wait_until_initialized(self):
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

    def _resolve_node_id(self, args: NodeResolverArgs):
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
        args: ResolverArgs
    ):
        node_id = self._resolve_node_id(args.node)
        log_file_name = args.file.log_file_name

        # If `log_file_name` is not provided, check if we can get the
        # corresponding `log_file_name` if an `actor_id` is provided.
        if log_file_name is None or node_id is None:
            if args.file.actor_id is not None:
                actor_data = DataSource.actors.get(args.file.actor_id)
                if actor_data is None:
                    raise ValueError("Actor ID {actor_id} not found.")
                worker_id = actor_data["address"].get("workerId")
                if worker_id is None:
                    raise ValueError("Worker ID for Actor ID {actor_id} not found.")

                index = await self._list_logs(node_id)
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
                index = await self._list_logs(node_id, [pid])
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

    async def _create_and_validate_log_stream(
        self,
        resolver_args: ResolverArgs,
        stream_options: LogStreamOptions,
    ):
        log_file_name, node_id = await self._resolve_file_and_node(resolver_args)

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
