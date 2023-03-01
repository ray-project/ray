import logging

import aiohttp.web
import ray.dashboard.modules.log.log_utils as log_utils
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.datacenter import DataSource, GlobalSignals

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class LogHead(dashboard_utils.DashboardHeadModule):
    LOG_URL_TEMPLATE = "http://{ip}:{port}/logs"
    LOG_INDEX_TEXT_TEMPLATE = "Node ID: {node_id} (IP: {node_ip})"

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
        agent_ips = set()
        index_text_log_url = []
        for node_id, ports in DataSource.agents.items():
            ip = DataSource.node_id_to_ip[node_id]
            agent_ips.add(ip)
            # Use node_id(ip) as log url's text
            index_text = self.LOG_INDEX_TEXT_TEMPLATE.format(
                node_id=node_id, node_ip=ip
            )
            log_url = self.LOG_URL_TEMPLATE.format(ip=ip, port=str(ports[0]))
            index_text_log_url.append((index_text, log_url))
        if self._dashboard_head.ip not in agent_ips:
            index_text = self.LOG_INDEX_TEXT_TEMPLATE.format(
                node_id="head", node_ip=self._dashboard_head.ip
            )
            log_url = self.LOG_URL_TEMPLATE.format(
                ip=self._dashboard_head.ip, port=self._dashboard_head.http_port
            )
            index_text_log_url.append((index_text, log_url))
        return aiohttp.web.Response(
            text=self._directory_as_html(index_text_log_url), content_type="text/html"
        )

    @routes.get("/log_proxy")
    async def get_log_from_proxy(self, req) -> aiohttp.web.StreamResponse:
        url = req.query.get("url")
        if not url:
            raise Exception("url is None.")
        body = await req.read()
        # Special logic to handle hashtags only. The only character that
        # is not getting properly encoded by aiohttp's static file server
        encoded_url = url.replace("#", "%23")
        async with self._proxy_session.request(
            req.method, encoded_url, data=body, headers=req.headers
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
    def _directory_as_html(index_text_log_url) -> str:
        # returns directory's index as html

        index_of = "Index of logs"
        h1 = f"<h1>{index_of}</h1>"

        index_list = []
        for text, log_url in index_text_log_url:
            index_list.append(f'<li><a href="{log_url}">{text}</a></li>')
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
