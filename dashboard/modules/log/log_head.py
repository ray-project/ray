import logging

import aiohttp.web
import ray.dashboard.modules.log.log_utils as log_utils
import ray.dashboard.utils as dashboard_utils
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
        # We disable auto_decompress when forward / proxy log url.
        self._proxy_session = aiohttp.ClientSession(auto_decompress=False)
        log_utils.register_mimetypes()
        self._stubs = {}
        DataSource.agents.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, _ = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(ip)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]

            # options = (("grpc.enable_http_proxy", 0),)
            # channel = ray._private.utils.init_grpc_channel(
            #     f"{ip}:{ports[1]}", options=options, asynchronous=True
            # )
            # stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            # Add HTTP address
            self._stubs[ip] = {"node_id": node_id, "address": f"http://{ip}:{ports[0]}"}

    @staticmethod
    async def get_logs_json_index(node_info, filters):
        import requests

        addr = node_info["address"]
        log_html = requests.get(f"{addr}/logs").text

        def get_link(s):
            s = s[len('<li><a href="/logs/') :]
            path = s[: s.find('"')]
            return path

        filters = [] if filters == [''] else filters

        filtered = list(
            filter(
                lambda s: '<li><a href="/logs/' in s and all(f in s for f in filters),
                log_html.splitlines(),
            )
        )
        links = list(map(get_link, filtered))
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
    @dashboard_optional_utils.aiohttp_cache
    async def handle_log_index(self, req):
        node_id = req.query.get("node_id", None)
        filters = req.query.get("filters", "").split(",")
        response = {}
        while self._stubs == {}:
            await asyncio.sleep(0.5)
        for node_info in self._stubs.values():
            if node_id is None or (node_id and node_id == node_info["node_id"]):
                response[node_info["node_id"]] = await self.get_logs_json_index(
                    node_info, filters
                )
        return aiohttp.web.json_response(response)

    @routes.get("/v1/api/logs/file/{node_id}/{log_file_name}")
    async def handle_get_log(self, req):
        node_id = req.match_info.get("node_id", None)
        log_file_name = req.match_info.get("log_file_name", None)
        while self._stubs == {}:
            await asyncio.sleep(0.5)
        matches = list(
            filter(lambda info: node_id == info["node_id"], self._stubs.values())
        )
        if len(matches) != 1:
            raise aiohttp.web.HTTPNotFound()
        addr = matches[0]["address"]
        import requests

        return aiohttp.web.Response(
            text=requests.get(
                f"{addr}/v1/api/logs/agent/file/{log_file_name}?{req.query_string}"
            ).text
        )

    # This creates a websocket session which first sends ?lines=x lines from
    # end of log file
    #
    # Subsequently, log file is polled periodically by agent and new bytes are streamed.
    @routes.get("/v1/api/logs/stream/{node_id}/{log_file_name}")
    async def handle_stream_log(self, req):
        node_id = req.match_info.get("node_id", None)
        log_file_name = req.match_info.get("log_file_name", None)
        while self._stubs == {}:
            await asyncio.sleep(0.5)

        matches = list(
            filter(lambda info: node_id == info["node_id"], self._stubs.values())
        )
        if len(matches) != 1:
            raise aiohttp.web.HTTPNotFound()
        addr = matches[0]["address"]

        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(req)

        async with aiohttp.ClientSession(auto_decompress=False) as session:
            ws_client = await session.ws_connect(
                f"{addr}/v1/api/logs/agent/stream/{log_file_name}?{req.query_string}"
            )

            while True:
                msg = await ws_client.receive()
                if msg.type != aiohttp.WSMsgType.BINARY:
                    await ws.send_str("Data could not be read from agent")
                    break
                asyncio.sleep(0.5)
                await ws.send_bytes(msg.data)
        return ws

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
        return False
