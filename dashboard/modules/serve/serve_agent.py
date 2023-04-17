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
from ray.exceptions import RayTaskError

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

        # serve_start_async is not thread-safe call. This lock
        # will make sure there is only one call that starts the serve instance.
        # If the lock is already acquired by another async task, the async task
        # will asynchronously wait for the lock.
        self._controller_start_lock = asyncio.Lock()

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

        controller = await self.get_serve_controller()

        if controller is None:
            config = ServeApplicationSchema.get_empty_schema_dict()
        else:
            try:
                config = await controller.get_app_config.remote()
                if config is None:
                    config = ServeApplicationSchema.get_empty_schema_dict()
            except ray.exceptions.RayTaskError as e:
                # Task failure sometimes are due to GCS
                # failure. When GCS failed, we expect a longer time
                # to recover.
                return Response(
                    status=503,
                    text=(
                        "Failed to get a response from the controller.  "
                        f"The GCS may be down, please retry later: {e}"
                    ),
                )

        return Response(
            text=json.dumps(config),
            content_type="application/json",
        )

    @routes.get("/api/serve/applications/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_serve_instance_details(self, req: Request) -> Response:
        from ray.serve.schema import ServeInstanceDetails

        controller = await self.get_serve_controller()

        if controller is None:
            # If no serve instance is running, return a dict that represents that.
            details = ServeInstanceDetails.get_empty_schema_dict()
        else:
            try:
                details = await controller.get_serve_instance_details.remote()
            except ray.exceptions.RayTaskError as e:
                # Task failure sometimes are due to GCS
                # failure. When GCS failed, we expect a longer time
                # to recover.
                return Response(
                    status=503,
                    text=(
                        "Failed to get a response from the controller. "
                        f"The GCS may be down, please retry later: {e}"
                    ),
                )

        return Response(
            text=json.dumps(details),
            content_type="application/json",
        )

    @routes.get("/api/serve/deployments/status")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_all_deployment_statuses(self, req: Request) -> Response:
        from ray.serve.schema import serve_status_to_schema, ServeStatusSchema

        controller = await self.get_serve_controller()

        if controller is None:
            status_json = ServeStatusSchema.get_empty_schema_dict()
            status_json_str = json.dumps(status_json)
        else:
            from ray.serve._private.common import StatusOverview
            from ray.serve.generated.serve_pb2 import (
                StatusOverview as StatusOverviewProto,
            )

            serve_status = await controller.get_serve_status.remote()
            proto = StatusOverviewProto.FromString(serve_status)
            status = StatusOverview.from_proto(proto)
            status_json_str = serve_status_to_schema(status).json()

        return Response(
            text=status_json_str,
            content_type="application/json",
        )

    @routes.delete("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_application(self, req: Request) -> Response:
        from ray import serve

        if await self.get_serve_controller() is not None:
            serve.shutdown()

        return Response()

    @routes.delete("/api/serve/applications/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_applications(self, req: Request) -> Response:
        from ray import serve

        if await self.get_serve_controller() is not None:
            serve.shutdown()

        return Response()

    @routes.put("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def put_all_deployments(self, req: Request) -> Response:
        from ray.serve._private.api import serve_start_async
        from ray.serve.schema import ServeApplicationSchema
        from pydantic import ValidationError
        from ray.serve._private.constants import MULTI_APP_MIGRATION_MESSAGE
        from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

        try:
            config = ServeApplicationSchema.parse_obj(await req.json())
        except ValidationError as e:
            return Response(
                status=400,
                text=repr(e),
            )

        if "name" in config.dict(exclude_unset=True):
            error_msg = (
                "Specifying the name of an application is only allowed for apps that "
                "are listed as part of a multi-app config file. "
            ) + MULTI_APP_MIGRATION_MESSAGE
            logger.warning(error_msg)
            return Response(
                status=400,
                text=error_msg,
            )

        async with self._controller_start_lock:
            client = await serve_start_async(
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

        try:
            client.deploy_apps(config)
            record_extra_usage_tag(TagKey.SERVE_REST_API_VERSION, "v1")
        except RayTaskError as e:
            return Response(
                status=400,
                text=str(e),
            )
        else:
            return Response()

    @routes.put("/api/serve/applications/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def put_all_applications(self, req: Request) -> Response:
        from ray.serve._private.api import serve_start_async
        from ray.serve.schema import ServeDeploySchema
        from pydantic import ValidationError
        from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

        try:
            config = ServeDeploySchema.parse_obj(await req.json())
        except ValidationError as e:
            return Response(
                status=400,
                text=repr(e),
            )

        async with self._controller_start_lock:
            client = await serve_start_async(
                detached=True,
                http_options={
                    "host": config.http_options.host,
                    "port": config.http_options.port,
                    "root_path": config.http_options.root_path,
                    "location": config.proxy_location,
                },
            )

        # Check HTTP Host
        host_conflict = self.check_http_options(
            "host", client.http_config.host, config.http_options.host
        )
        if host_conflict is not None:
            return host_conflict

        # Check HTTP Port
        port_conflict = self.check_http_options(
            "port", client.http_config.port, config.http_options.port
        )
        if port_conflict is not None:
            return port_conflict

        # Check HTTP root path
        root_path_conflict = self.check_http_options(
            "root path", client.http_config.root_path, config.http_options.root_path
        )
        if root_path_conflict is not None:
            return root_path_conflict

        # Check HTTP location
        location_conflict = self.check_http_options(
            "location", client.http_config.location, config.proxy_location
        )
        if location_conflict is not None:
            return location_conflict

        try:
            client.deploy_apps(config)
            record_extra_usage_tag(TagKey.SERVE_REST_API_VERSION, "v2")
        except RayTaskError as e:
            return Response(
                status=400,
                text=str(e),
            )
        else:
            return Response()

    def check_http_options(self, option: str, old: str, new: str):
        http_mismatch_message = (
            "Serve is already running on this Ray cluster. Its HTTP {option} is set to "
            '"{old}". However, the requested {option} is "{new}". The requested '
            "{option} must match the running Serve instance's HTTP {option}. To change "
            "the Serve HTTP {option}, shut down Serve on this Ray cluster using "
            "the `serve shutdown` CLI command or by sending a DELETE request to the "
            '"/api/serve/applications/" endpoint. CAUTION: shutting down Serve will '
            "also shut down all Serve applications."
        )

        if not old == new:
            return Response(
                status=400,
                text=http_mismatch_message.format(option=option, old=old, new=new),
            )

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
