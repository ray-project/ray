import asyncio
import logging
from typing import Optional

import aiohttp
from aiohttp.web import Request, Response
import ray

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable


class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self,
                 dashboard_head,
                 http_session: Optional[aiohttp.ClientSession] = None):
        super().__init__(dashboard_head)
        self._controller = None
        self._controller_lock = asyncio.Lock()
        self._http_session = http_session

    @routes.get("/api/serve/applications_head/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
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
                    text=("Fail to get the response from the controller. "
                          f"Potentially the GCS is down: {e}"),
                )

        return Response(
            text=ServeInstanceDetails(**details).json(exclude_unset=True),
            content_type="application/json",
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
                    SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
            except Exception as e:
                logger.debug("There is no "
                             "instance running on this Ray cluster. Please "
                             "call `serve.start(detached=True) to start "
                             f"one: {e}")

            return self._controller

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
