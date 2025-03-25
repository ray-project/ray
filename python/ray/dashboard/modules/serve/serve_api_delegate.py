import asyncio
import logging
from typing import Dict, TYPE_CHECKING

import ray
from ray.dashboard.modules.version import CURRENT_VERSION, VersionResponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if TYPE_CHECKING:
    from ray.serve.schema import ServeDeploySchema


class ServeAPIDelegate:
    """
    This class is composed by both the head and agent modules and
    contains the shared implementation for the Serve REST API.
    This class uses delayed imports for all Ray Serve-related modules.
    That way, users can use the Ray dashboard agent for non-Serve purposes
    without downloading Serve dependencies.
    """

    def __init__(self):
        self._controller = None
        self._controller_lock = asyncio.Lock()

        # serve_start_async is not thread-safe call. This lock
        # will make sure there is only one call that starts the serve instance.
        # If the lock is already acquired by another async task, the async task
        # will asynchronously wait for the lock.
        self._controller_start_lock = asyncio.Lock()

    def get_version(self, session_name: str) -> VersionResponse:
        # NOTE(edoakes): CURRENT_VERSION should be bumped and checked on the
        # client when we have backwards-incompatible changes.
        return VersionResponse(
            version=CURRENT_VERSION,
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
            session_name=session_name,
        )

    async def get_serve_instance_details(self) -> Dict:
        from ray.serve.schema import ServeInstanceDetails

        controller = await self._get_serve_controller()

        if controller is None:
            # If no serve instance is running, return a dict that represents that.
            return ServeInstanceDetails.get_empty_schema_dict()
        return await controller.get_serve_instance_details.remote()

    async def delete_serve_applications(self):
        from ray import serve

        if await self._get_serve_controller() is not None:
            serve.shutdown()

    async def put_all_applications(self, config: "ServeDeploySchema"):
        from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
        from ray.serve._private.api import serve_start_async
        from ray.serve.config import ProxyLocation

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
        self._validate_http_options(client, full_http_options)

        if config.logging_config:
            client.update_global_logging_config(config.logging_config)
        client.deploy_apps(config)
        record_extra_usage_tag(TagKey.SERVE_REST_API_VERSION, "v2")

    def _validate_http_options(self, client, http_options):
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

    async def _get_serve_controller(self):
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
