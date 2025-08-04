import asyncio
import dataclasses
import json
import logging
from functools import wraps

import aiohttp
from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray._common.pydantic_compat import ValidationError
from ray.dashboard.modules.version import CURRENT_VERSION, VersionResponse
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.exceptions import RayTaskError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def validate_endpoint():
    def decorator(func):
        @wraps(func)
        async def check(self, *args, **kwargs):
            try:
                from ray import serve  # noqa: F401
            except ImportError:
                return Response(
                    status=501,
                    text=(
                        "Serve dependencies are not installed. Please run `pip "
                        'install "ray[serve]"`.'
                    ),
                )
            return await func(self, *args, **kwargs)

        return check

    return decorator


# NOTE (shrekris-anyscale): This class uses delayed imports for all
# Ray Serve-related modules. That way, users can use the Ray dashboard agent for
# non-Serve purposes without downloading Serve dependencies.
class ServeHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._controller = None
        self._controller_lock = asyncio.Lock()

        # serve_start_async is not thread-safe call. This lock
        # will make sure there is only one call that starts the serve instance.
        # If the lock is already acquired by another async task, the async task
        # will asynchronously wait for the lock.
        self._controller_start_lock = asyncio.Lock()

        # To init gcs_client in internal_kv for record_extra_usage_tag.
        assert self.gcs_client is not None
        assert ray.experimental.internal_kv._internal_kv_initialized()

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
            session_name=self.session_name,
        )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.get("/api/serve/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @validate_endpoint()
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

    @routes.delete("/api/serve/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_applications(self, req: Request) -> Response:
        from ray import serve

        if await self.get_serve_controller() is not None:
            serve.shutdown()

        return Response()

    @routes.put("/api/serve/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @validate_endpoint()
    async def put_all_applications(self, req: Request) -> Response:
        from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
        from ray.serve._private.api import serve_start_async
        from ray.serve.config import ProxyLocation
        from ray.serve.schema import ServeDeploySchema

        try:
            config: ServeDeploySchema = ServeDeploySchema.parse_obj(await req.json())
        except ValidationError as e:
            return Response(
                status=400,
                text=repr(e),
            )

        config_http_options = config.http_options.dict()
        location = ProxyLocation._to_deployment_mode(config.proxy_location)
        full_http_options = dict({"location": location}, **config_http_options)
        grpc_options = config.grpc_options.dict()

        async with self._controller_start_lock:
            client = await serve_start_async(
                http_options=full_http_options,
                grpc_options=grpc_options,
                global_logging_config=config.logging_config,
            )

        # Serve ignores HTTP options if it was already running when
        # serve_start_async() is called. Therefore we validate that no
        # existing HTTP options are updated and print warning in case they are
        self.validate_http_options(client, full_http_options)

        try:
            if config.logging_config:
                client.update_global_logging_config(config.logging_config)
            client.deploy_apps(config)
            record_extra_usage_tag(TagKey.SERVE_REST_API_VERSION, "v2")
        except RayTaskError as e:
            return Response(
                status=400,
                text=str(e),
            )
        else:
            return Response()

    def validate_http_options(self, client, http_options):
        divergent_http_options = []

        for option, new_value in http_options.items():
            prev_value = getattr(client.http_config, option)
            if prev_value != new_value:
                divergent_http_options.append(option)

        if divergent_http_options:
            logger.warning(
                "Serve is already running on this Ray cluster and "
                "it's not possible to update its HTTP options without "
                "restarting it. Following options are attempted to be "
                f"updated: {divergent_http_options}."
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
