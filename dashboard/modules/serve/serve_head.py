import aiohttp.web
from aiohttp.web import Request, Response
import json
import logging
from typing import Optional

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils

from ray import serve

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    def _get_deployment_by_name(self, name: str) -> Optional[serve.api.Deployment]:
        try:
            return serve.get_deployment(name)
        except KeyError:
            return None

    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployments(self, req: Request) -> Response:
        dict_response = {
            name: str(deployment)
            for name, deployment in serve.list_deployments().items()
        }

        return Response(text=json.dumps(dict_response), content_type="application/json")

    @routes.get("/api/serve/deployments/{name}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_single_deployment(self, req: Request) -> Response:
        name = req.match_info["name"]
        deployment = self._get_deployment_by_name(name)
        if deployment is None:
            return Response(
                text=f"Deployment {name} does not exist.",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(
            text=json.dumps(str(deployment)), content_type="application/json"
        )

    @routes.delete("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def delete_all_deployments(self, req: Request) -> Response:
        serve.shutdown()

    @routes.delete("/api/serve/deployments/{name}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def delete_single_deployment(self, req: Request) -> Response:
        name = req.match_info["name"]
        deployment = self._get_deployment_by_name(name)
        if deployment is None:
            return Response(
                text=f"Deployment {name} does not exist.",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        deployment.delete()
        return Response()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
