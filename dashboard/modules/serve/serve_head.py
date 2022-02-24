from aiohttp.web import Request, Response
import json
import logging

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils

from ray import serve
from ray.serve.api import deploy_group, get_deployment_statuses
from ray.dashboard.modules.serve.schema import (
    ServeInstanceSchema,
    serve_instance_to_schema,
    schema_to_serve_instance,
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
        statuses = get_deployment_statuses()
        serve_instance_schema = serve_instance_to_schema(
            deployments=deployments, statuses=statuses
        )
        return Response(
            text=json.dumps(serve_instance_schema.json()),
            content_type="application/json",
        )

    @routes.delete("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def delete_serve_instance(self, req: Request) -> Response:
        serve.shutdown()
        return Response()

    @routes.put("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def put_all_deployments(self, req: Request) -> Response:
        serve_instance_json = await req.json()
        serve_instance_schema = ServeInstanceSchema.parse_raw(
            json.dumps(serve_instance_json)
        )
        deployments = schema_to_serve_instance(serve_instance_schema)

        deploy_group(deployments, _blocking=False)

        new_names = set()
        for deployment in serve_instance_schema.deployments:
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
