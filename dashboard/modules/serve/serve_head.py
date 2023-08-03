import logging
from typing import Optional

import aiohttp
from aiohttp.web import Request, Response

from ray.dashboard.datacenter import DataOrganizer
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable


class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._http_session = aiohttp.ClientSession()

    @routes.get("/api/serve_head/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def get_serve_instance_details(self, req: Request) -> Response:
        """
        This proxies to the serve_agent's /api/serve/applciations call
        """
        head_agent_address = await self._get_head_agent()
        if not head_agent_address:
            return Response(
                status=503,
                text=(
                    "Failed to find the serve agent. "
                    "Check the dashboard_agent logs to see if the agent "
                    "failed to launch."
                ),
            )

        try:
            async with self._http_session.get(
                f"{head_agent_address}/api/serve/applications/"
            ) as resp:
                if resp.status == 200:
                    result_text = await resp.text()
                    return Response(
                        text=result_text,
                        content_type="application/json",
                    )
                else:
                    status = resp.status
                    error_text = await resp.text()
                    raise Response(
                        status=500,
                        text=f"Request failed with status code {status}: {error_text}.",
                    )
        except Exception as e:
            return Response(
                status=503,
                text=(
                    "Failed to hit serve agent. "
                    "Check the dashboard_agent logs to see "
                    f"if the agent failed to launch. {e}"
                ),
            )

    async def _get_head_agent(self) -> Optional[str]:
        """
        Grabs the head node dashboard_agent's address.
        """
        # the number of agents which has an available HTTP port.
        raw_agent_infos = await DataOrganizer.get_all_agent_infos()
        agent_infos = {
            key: value
            for key, value in raw_agent_infos.items()
            if value.get("httpPort", -1) > 0
        }
        if not agent_infos:
            return None

        # TODO(aguo): Get the head agent by node_id instead of node_ip once
        # head_node_id is easily available in dashboard_head.py
        head_node_ip = self._dashboard_head.ip
        logger.info(f"head_node_ip: {head_node_ip}")
        for agent_info in agent_infos.values():
            logger.info(f"agent_info: {agent_info}")
            if agent_info["ipAddress"] == head_node_ip:
                node_ip = agent_info["ipAddress"]
                http_port = agent_info["httpPort"]
                return f"http://{node_ip}:{http_port}"

        return None

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
