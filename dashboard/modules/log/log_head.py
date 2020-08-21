import logging

import mimetypes

import aiohttp.web
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.datacenter import DataSource, GlobalSignals

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class LogHead(dashboard_utils.DashboardHeadModule):
    LOG_URL_TEMPLATE = "http://{ip}:{port}/logs"

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        mimetypes.add_type("text/plain", ".err")
        mimetypes.add_type("text/plain", ".out")
        routes.static("/logs", self._dashboard_head.log_dir, show_index=True)
        GlobalSignals.node_info_fetched.append(
            self.insert_log_url_to_node_info)

    async def insert_log_url_to_node_info(self, node_info):
        ip = node_info.get("ip")
        if ip is None:
            return
        agent_port = DataSource.agents.get(ip)
        if agent_port is None:
            return
        agent_http_port, _ = agent_port
        log_url = self.LOG_URL_TEMPLATE.format(ip=ip, port=agent_http_port)
        node_info["logUrl"] = log_url

    @routes.get("/log_index")
    async def get_log_index(self, req) -> aiohttp.web.Response:
        url_list = []
        for ip, ports in DataSource.agents.items():
            url_list.append(
                self.LOG_URL_TEMPLATE.format(ip=ip, port=str(ports[0])))
        if self._dashboard_head.ip not in DataSource.agents:
            url_list.append(
                self.LOG_URL_TEMPLATE.format(
                    ip=self._dashboard_head.ip,
                    port=self._dashboard_head.http_port))
        return aiohttp.web.Response(
            text=self._directory_as_html(url_list), content_type="text/html")

    @staticmethod
    def _directory_as_html(url_list) -> str:
        # returns directory's index as html

        index_of = "Index of logs"
        h1 = "<h1>{}</h1>".format(index_of)

        index_list = []
        for url in sorted(url_list):
            index_list.append('<li><a href="{url}">{name}</a></li>'.format(
                url=url, name=url))
        ul = "<ul>\n{}\n</ul>".format("\n".join(index_list))
        body = "<body>\n{}\n{}\n</body>".format(h1, ul)

        head_str = "<head>\n<title>{}</title>\n</head>".format(index_of)
        html = "<html>\n{}\n{}\n</html>".format(head_str, body)

        return html

    async def run(self, server):
        pass
