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
    LOG_URL_TEMPLATE = "http://{ip}:{port}/logs"

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        DataSource.agents.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, _ = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(node_id)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]

            options = (("grpc.enable_http_proxy", 0),)
            channel = init_grpc_channel(
                f"{ip}:{ports[1]}", options=options, asynchronous=True
            )
            stub = reporter_pb2_grpc.LogServiceStub(channel)
            self._stubs[node_id] = stub

    @staticmethod
    async def get_logs_json_index(grpc_stub, filters):
        reply = await grpc_stub.LogIndex(reporter_pb2.LogIndexRequest())
        filters = [] if filters == [""] else filters
        links = list(filter(lambda s: all(f in s for f in filters), reply.log_files))
        logs = {}
        logs["worker_errors"] = list(
            filter(lambda s: "worker" in s and s.endswith(".err"), links)
        )
        logs["worker_outs"] = list(
            filter(lambda s: "worker" in s and s.endswith(".out"), links)
        )
        for lang in ray_constants.LANGUAGE_WORKER_TYPES:
            logs[f"{lang}_core_worker_logs"] = list(
                filter(
                    lambda s: f"{lang}-core-worker" in s and s.endswith(".log"),
                    links,
                )
            )
            logs[f"{lang}_driver_logs"] = list(
                filter(
                    lambda s: f"{lang}-core-driver" in s and s.endswith(".log"),
                    links,
                )
            )
        logs["dashboard"] = list(filter(lambda s: "dashboard" in s, links))
        logs["raylet_logs"] = list(filter(lambda s: "raylet" in s, links))
        logs["gcs_logs"] = list(filter(lambda s: "gcs" in s, links))
        logs["ray_client"] = list(filter(lambda s: "ray_client" in s, links))
        logs["autoscaler_monitor"] = list(
            filter(lambda s: "monitor" in s and "log_monitor" not in s, links)
        )
        logs["folders"] = list(filter(lambda s: "." not in s, links))
        logs["misc"] = list(
            filter(lambda s: all([s not in logs[k] for k in logs]), links)
        )
        return logs

    @routes.get("/v1/api/logs/index")
    async def handle_log_index(self, req):
        node_id = req.query.get("node_id", None)
        filters = req.query.get("filters", "").split(",")
        response = {}
        while self._stubs == {}:
            await asyncio.sleep(0.5)
        tasks = []
        # TODO: check this is in fact running in parallel
        for node_id, grpc_stub in self._stubs.items():

            async def coro():
                response[node_id] = await self.get_logs_json_index(grpc_stub, filters)

            tasks.append(coro())
        await asyncio.gather(*tasks)
        return aiohttp.web.json_response(response)

    @routes.get("/v1/api/logs/{media_type}/{node_id}/{log_file_name}")
    async def handle_log(self, req):
        node_id = req.match_info.get("node_id", None)
        log_file_name = req.match_info.get("log_file_name", None)
        media_type = req.match_info.get("media_type", None)

        lines = req.query.get("lines", None)
        lines = int(lines) if lines else None

        interval = req.match_info.get("interval", None)
        interval = float(interval) if interval else None

        while self._stubs == {}:
            await asyncio.sleep(0.5)
        if node_id not in self._stubs:
            return aiohttp.web.HTTPNotFound(reason=f"Node ID {node_id} not found")

        if media_type == "stream":
            keep_alive = True
        elif media_type == "file":
            keep_alive = False
        else:
            return aiohttp.web.HTTPNotFound(reason=f"Invalid media type: {media_type}")
        stream = self._stubs[node_id].StreamLog(
            reporter_pb2.StreamLogRequest(
                keep_alive=keep_alive,
                log_file_name=log_file_name,
                lines=lines,
                interval=interval,
            )
        )

        metadata = await stream.initial_metadata()
        if metadata[log_consts.LOG_STREAM_STATUS] == log_consts.FILE_NOT_FOUND:
            return aiohttp.web.HTTPNotFound(
                reason=f'File "{log_file_name}" not found on node {node_id}'
            )

        response = aiohttp.web.StreamResponse()
        response.content_type = "text/plain"
        await response.prepare(req)

        async for log_response in stream:
            await response.write(log_response.data)
        await response.write_eof()
        return response

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
