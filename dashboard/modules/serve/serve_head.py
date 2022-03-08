from aiohttp.web import Request, Response
import logging

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils

from ray import serve
from ray.serve.api import deploy_group, get_deployment_statuses
from ray.dashboard.modules.serve.schema import (
    ServeApplicationSchema,
    serve_application_to_schema,
    schema_to_serve_application,
    serve_application_status_to_schema,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployments(self, req: Request) -> Response:
        deployments = list(serve.list_deployments().values())
        serve_application_schema = serve_application_to_schema(deployments=deployments)
        return Response(
            text=serve_application_schema.json(),
            content_type="application/json",
        )

    @routes.get("/api/serve/deployments/status")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployment_statuses(self, req: Request) -> Response:
        serve_application_status_schema = serve_application_status_to_schema(
            get_deployment_statuses()
        )
        return Response(
            text=serve_application_status_schema.json(),
            content_type="application/json",
        )

    @routes.delete("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def delete_serve_application(self, req: Request) -> Response:
        serve.shutdown()
        return Response()

    @routes.put("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def put_all_deployments(self, req: Request) -> Response:
        serve_application_text = await req.text()
        serve_application_schema = ServeApplicationSchema.parse_raw(
            serve_application_text, content_type="application/json"
        )
        deployments = schema_to_serve_application(serve_application_schema)

        deploy_group(deployments, _blocking=False)

        new_names = set()
        for deployment in serve_application_schema.deployments:
            new_names.add(deployment.name)

        all_deployments = serve.list_deployments()
        all_names = set(all_deployments.keys())
        names_to_delete = all_names.difference(new_names)
        for name in names_to_delete:
            all_deployments[name].delete()

        return Response()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
