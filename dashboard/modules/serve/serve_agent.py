import json
import logging

from aiohttp.web import Request, Response

import dataclasses
import ray
import aiohttp.web
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.version import (
    CURRENT_VERSION,
    VersionResponse,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


# NOTE (shrekris-anyscale): This class uses delayed imports for all
# Ray Serve-related modules. That way, users can use the Ray dashboard agent for
# non-Serve purposes without downloading Serve dependencies.
class ServeAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

    # TODO: It's better to use `/api/version`.
    # It requires a refactor of ClassMethodRouteTable to differentiate the server.
    @routes.get("/api/ray/version")
    async def get_version(self, req: Request) -> Response:
        # NOTE(edoakes): CURRENT_VERSION should be bumped and checked on the
        # client when we have backwards-incompatible changes.
        resp = VersionResponse(
            version=CURRENT_VERSION,
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
        )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployments(self, req: Request) -> Response:
        from ray.serve.context import get_global_client

        client = get_global_client()

        return Response(
            text=json.dumps(client.get_app_config()),
            content_type="application/json",
        )

    @routes.get("/api/serve/deployments/status")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployment_statuses(self, req: Request) -> Response:
        from ray.serve.context import get_global_client
        from ray.serve.schema import serve_status_to_schema

        client = get_global_client()

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
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def put_all_deployments(self, req: Request) -> Response:
        from ray import serve
        from ray.serve.schema import ServeApplicationSchema

        config = ServeApplicationSchema.parse_obj(await req.json())

        client = serve.start(
            detached=True,
            http_options={
                "host": config.host,
                "port": config.port,
                "location": "EveryNode",
            },
        )

        if client.http_config.host != config.host:
            return Response(
                status=400,
                reason=(
                    "Serve is already running on this Ray cluster. Its "
                    f'HTTP host is set to "{client.http_config.host}". '
                    f'However, the requested host is "{config.host}". '
                    f"The requested host must match the running Serve "
                    "application's host. To change the Serve application "
                    "host, shut down Serve on this Ray cluster using the "
                    "`serve delete` CLI command or by sending a DELETE "
                    "request to this Ray cluster's "
                    '"/api/serve/deployments/" endpoint. CAUTION: shutting '
                    "down Serve will also shut down all Serve deployments."
                ),
            )

        if client.http_config.port != config.port:
            return Response(
                status=400,
                reason=(
                    "Serve is already running on this Ray cluster. Its "
                    f'HTTP port is set to "{client.http_config.port}". '
                    f'However, the requested port is "{config.port}". '
                    f"The requested port must match the running Serve "
                    "application's port. To change the Serve application "
                    "port, shut down Serve on this Ray cluster using the "
                    "`serve delete` CLI command or by sending a DELETE "
                    "request to this Ray cluster's "
                    '"/api/serve/deployments/" endpoint. CAUTION: shutting '
                    "down Serve will also shut down all Serve deployments."
                ),
            )

        client.deploy_app(config)

        return Response()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
