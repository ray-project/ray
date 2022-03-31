from aiohttp.web import Request, Response
import json
import logging

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployments(self, req: Request) -> Response:
        from ray.serve.api import Application, list_deployments

        app = Application(list(list_deployments().values()))
        return Response(
            text=json.dumps(app.to_dict()),
            content_type="application/json",
        )

    @routes.get("/api/serve/deployments/status")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployment_statuses(self, req: Request) -> Response:
        from ray.serve.api import (
            serve_application_status_to_schema,
            get_deployment_statuses,
        )

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
        from ray import serve

        serve.shutdown()
        return Response()

    @routes.put("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def put_all_deployments(self, req: Request) -> Response:
        from ray import serve
        from ray.serve.api import (
            Application,
            internal_get_global_client,
        )

        app = Application.from_dict(await req.json())
        serve.run(app, _blocking=False)

        new_names = set()
        for deployment in app.deployments.values():
            new_names.add(deployment.name)

        all_deployments = serve.list_deployments()
        all_names = set(all_deployments.keys())
        names_to_delete = all_names.difference(new_names)
        internal_get_global_client().delete_deployments(names_to_delete)

        return Response()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
