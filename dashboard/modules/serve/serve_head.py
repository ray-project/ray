import logging
from enum import Enum
from typing import Optional

import aiohttp
from aiohttp.web import Request, Response

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.datacenter import DataOrganizer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable


class RestMethod(str, Enum):
    GET = "GET"
    PUT = "PUT"
    DELETE = "DELETE"


class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._http_session = aiohttp.ClientSession()
        self.rest_method_executors = {
            RestMethod.GET: self._http_session.get,
            RestMethod.PUT: self._http_session.put,
            RestMethod.DELETE: self._http_session.delete,
        }

    async def proxy_request(self, req: Request, route: str, method: RestMethod):
        """Forwards the req request to the Serve agent on the head node.

        Args:
            req: request to forward.
            route: route to send the request to.
            rest_method: REST method to use when sending the request.
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
            req_data = await req.read()
            headers = {"content-type": req.content_type}
            rest_method_executor = self.rest_method_executors[method]
            url = f"{head_agent_address}{route}"
            async with rest_method_executor(
                url=url, data=req_data, headers=headers
            ) as resp:
                resp_text = await resp.text()
                return Response(
                    text=resp_text,
                    status=resp.status,
                    content_type=resp.content_type,
                )
        except Exception:
            import traceback

            return Response(
                status=503,
                text=(
                    f"Failed to hit serve agent at address {url} on "
                    "the head node. Check the dashboard_agent logs to see "
                    "if the agent failed to launch. "
                    f"See traceback:\n{traceback.format_exc()}"
                ),
            )

    @routes.get("/api/serve_head/version")
    async def get_version(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/ray/version",
            method=RestMethod.GET,
        )

    @routes.get("/api/serve_head/deployments/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def get_all_deployments(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/serve/deployments/",
            method=RestMethod.GET,
        )

    @routes.get("/api/serve_head/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def get_serve_instance_details(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/serve/applications/",
            method=RestMethod.GET,
        )

    @routes.get("/api/serve_head/deployments/status")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def get_all_deployment_statuses(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/serve/deployments/status",
            method=RestMethod.GET,
        )

    @routes.delete("/api/serve_head/deployments/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_application(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/serve/deployments/",
            method=RestMethod.DELETE,
        )

    @routes.delete("/api/serve_head/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_applications(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/serve/applications/",
            method=RestMethod.DELETE,
        )

    @routes.put("/api/serve_head/deployments/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def put_all_deployments(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/serve/deployments/",
            method=RestMethod.PUT,
        )

    @routes.put("/api/serve_head/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def put_all_applications(self, req: Request) -> Response:
        return await self.proxy_request(
            req=req,
            route="/api/serve/applications/",
            method=RestMethod.PUT,
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
