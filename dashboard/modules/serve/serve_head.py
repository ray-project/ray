from aiohttp.web import Request, Response
import json
import logging

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


# NOTE (shrekris-anyscale): This class uses delayed imports for all
# Ray Serve-related modules. That way, users can use the Ray dashboard for
# non-Serve purposes without downloading Serve dependencies.
class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployments(self, req: Request) -> Response:
        from ray.serve.api import list_deployments
        from ray.serve.application import Application

        app = Application(list(list_deployments().values()))
        return Response(
            text=json.dumps(app.to_dict()),
            content_type="application/json",
        )

    @routes.get("/api/serve/deployments/status")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployment_statuses(self, req: Request) -> Response:
        from ray.serve.context import get_global_client
        from ray.serve.schema import serve_status_to_schema

        client = get_global_client(_override_controller_namespace="serve")

        serve_status_schema = serve_status_to_schema(client.get_serve_status())
        return Response(
            text=serve_status_schema.json(),
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
        from ray.serve.application import Application

        app = Application.from_dict(await req.json())
        serve.run(app, _blocking=False)

        return Response()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
