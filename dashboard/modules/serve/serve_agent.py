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
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_all_deployments(self, req: Request) -> Response:
        from ray.serve.schema import ServeApplicationSchema

        client = self.get_serve_client()

        if client is None:
            config = ServeApplicationSchema.get_empty_schema_dict()
        else:
            config = client.get_app_config()

        return Response(
            text=json.dumps(config),
            content_type="application/json",
        )

    @routes.get("/api/serve/deployments/status")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_all_deployment_statuses(self, req: Request) -> Response:
        from ray.serve.schema import serve_status_to_schema, ServeStatusSchema

        client = self.get_serve_client()

        if client is None:
            status_json = ServeStatusSchema.get_empty_schema_dict()
            status_json_str = json.dumps(status_json)
        else:
            status = client.get_serve_status()
            status_json_str = serve_status_to_schema(status).json()

        return Response(
            text=status_json_str,
            content_type="application/json",
        )

    @routes.delete("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_application(self, req: Request) -> Response:
        from ray import serve

        if self.get_serve_client() is not None:
            serve.shutdown()

        return Response()

    @routes.put("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def put_all_deployments(self, req: Request) -> Response:
        from ray.serve.schema import ServeApplicationSchema
        from ray.serve._private.api import serve_start

        config = ServeApplicationSchema.parse_obj(await req.json())

        client = serve_start(
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
                text=(
                    "Serve is already running on this Ray cluster. Its "
                    f'HTTP host is set to "{client.http_config.host}". '
                    f'However, the requested host is "{config.host}". '
                    f"The requested host must match the running Serve "
                    "application's host. To change the Serve application "
                    "host, shut down Serve on this Ray cluster using the "
                    "`serve shutdown` CLI command or by sending a DELETE "
                    "request to this Ray cluster's "
                    '"/api/serve/deployments/" endpoint. CAUTION: shutting '
                    "down Serve will also shut down all Serve deployments."
                ),
            )

        if client.http_config.port != config.port:
            return Response(
                status=400,
                text=(
                    "Serve is already running on this Ray cluster. Its "
                    f'HTTP port is set to "{client.http_config.port}". '
                    f'However, the requested port is "{config.port}". '
                    f"The requested port must match the running Serve "
                    "application's port. To change the Serve application "
                    "port, shut down Serve on this Ray cluster using the "
                    "`serve shutdown` CLI command or by sending a DELETE "
                    "request to this Ray cluster's "
                    '"/api/serve/deployments/" endpoint. CAUTION: shutting '
                    "down Serve will also shut down all Serve deployments."
                ),
            )

        client.deploy_app(config)

        return Response()

    def get_serve_client(self):
        """Gets the ServeControllerClient to the this cluster's Serve app.

        return: If Serve is running on this Ray cluster, returns a client to
            the Serve controller. If Serve is not running, returns None.
        """

        from ray.serve.context import get_global_client
        from ray.serve.exceptions import RayServeException

        try:
            return get_global_client(_health_check_controller=True)
        except RayServeException:
            logger.debug("There's no Serve app running on this Ray cluster.")
            return None

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
