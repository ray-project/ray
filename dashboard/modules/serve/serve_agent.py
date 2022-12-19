import json
import logging

from aiohttp.web import Request, Response
import dataclasses
import ray
import asyncio
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
        self._controller = None
        self._controller_lock = asyncio.Lock()

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

    # TODO(sihanwang) Changed to application instead of deployments
    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_all_deployments(self, req: Request) -> Response:
        from ray.serve.schema import ServeApplicationSchema

        controller = await self.get_serve_controller()

        if controller is None:
            config = ServeApplicationSchema.get_empty_schema_dict()
        else:
            try:
                config = await controller.get_app_config.remote()
            except ray.exceptions.RayTaskError as e:
                # Task failure sometimes are due to GCS
                # failure. When GCS failed, we expect a longer time
                # to recover.
                return Response(
                    status=503,
                    text=(
                        "Fail to get the response from the controller. "
                        f"Potentially the GCS is down: {e}"
                    ),
                )

        return Response(
            text=json.dumps(config),
            content_type="application/json",
        )

    @routes.delete("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_application(self, req: Request) -> Response:
        from ray import serve

        if await self.get_serve_controller() is not None:
            serve.shutdown()

        return Response()

    @routes.delete("/api/serve/applications/{app_name}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_application(self, req: Request) -> Response:
        from ray import serve

        app_name = req.match_info["app_name"]

        controller = await self.get_serve_controller()
        controller.delete_apps.remote(app_name)

        return Response()

    @routes.put("/api/serve/applications/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def put_all_applications(self, req: Request) -> Response:
        from ray.serve.schema import ServeDeploySchema
        from ray.serve._private.api import serve_start

        config = ServeDeploySchema.parse_obj(await req.json())

        client = serve_start(
            detached=True,
            http_options={
                "host": config.host,
                "port": config.port,
                "location": "EveryNode",
            },
        )

        for app_schema in config.applications:
            client.deploy_app(app_schema)

        return Response()

    @routes.get("/api/serve/applications/status/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_all_applications_statuses(self, req: Request) -> Response:

        from ray.serve.schema import serve_status_to_schema, ServeStatusSchema

        controller = await self.get_serve_controller()

        status_json_list = []

        if controller is None:
            status_json = ServeStatusSchema.get_empty_schema_dict()
            status_json_str = json.dumps(status_json)
        else:
            from ray.serve._private.common import StatusOverview
            from ray.serve.generated.serve_pb2 import (
                StatusOverview as StatusOverviewProto,
            )

            serve_statuses = await controller.get_serve_status.remote()

            for serve_status in serve_statuses:
                proto = StatusOverviewProto.FromString(serve_status)
                status = StatusOverview.from_proto(proto)
                # status_json_str = serve_status_to_schema(status).json()
                status_json_list.append(
                    json.loads(serve_status_to_schema(status).json())
                )

        return Response(
            text=json.dumps(status_json_list),
            content_type="application/json",
        )

    @routes.put("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def put_all_deployments(self, req: Request) -> Response:
        from ray.serve.schema import ServeApplicationSchema
        from ray.serve.schema import ServeDeploySchema
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

    async def get_serve_controller(self):
        """Gets the ServeController to the this cluster's Serve app.

        return: If Serve is running on this Ray cluster, returns a client to
            the Serve controller. If Serve is not running, returns None.
        """
        async with self._controller_lock:
            if self._controller is not None:
                try:
                    await self._controller.check_alive.remote()
                    return self._controller
                except ray.exceptions.RayActorError:
                    logger.info("Controller is dead")
                self._controller = None

            # Try to connect to serve even when we detect the actor is dead
            # because the user might have started a new
            # serve cluter.
            from ray.serve._private.constants import (
                SERVE_CONTROLLER_NAME,
                SERVE_NAMESPACE,
            )

            try:
                # get_actor is a sync call but it'll timeout after
                # ray.dashboard.consts.GCS_RPC_TIMEOUT_SECONDS
                self._controller = ray.get_actor(
                    SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
                )
            except Exception as e:
                logger.debug(
                    "There is no "
                    "instance running on this Ray cluster. Please "
                    "call `serve.start(detached=True) to start "
                    f"one: {e}"
                )

            return self._controller

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
