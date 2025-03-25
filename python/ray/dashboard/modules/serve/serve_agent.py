import dataclasses
import json
import logging

import aiohttp
from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.pydantic_compat import ValidationError
from ray.exceptions import RayTaskError
from ray.dashboard.modules.serve.serve_api_delegate import ServeAPIDelegate
from ray.dashboard.modules.serve.utils import validate_endpoint


logger = logging.getLogger(__name__)
routes = optional_utils.DashboardAgentRouteTable


class ServeAgent(dashboard_utils.DashboardAgentModule):
    def __init__(
        self,
        dashboard_agent: dashboard_utils.DashboardAgentModule,
    ):
        super().__init__(dashboard_agent)
        self._api_delegate = ServeAPIDelegate()

    # TODO: It's better to use `/api/version`.
    @routes.get("/api/ray/version")
    async def get_version(self, req: Request) -> Response:
        version_res = self._api_delegate.get_version(self.session_name)
        return Response(
            text=json.dumps(dataclasses.asdict(version_res)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.get("/api/serve/applications/")
    @optional_utils.init_ray_and_catch_exceptions()
    @validate_endpoint(log_deprecation_warning=True)
    async def get_serve_instance_details(self, req: Request) -> Response:
        try:
            details = await self._api_delegate.get_serve_instance_details()
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
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_applications(self, req: Request) -> Response:
        await self._api_delegate.delete_serve_applications()
        return Response()

    @routes.put("/api/serve/applications/")
    @optional_utils.init_ray_and_catch_exceptions()
    @validate_endpoint(log_deprecation_warning=True)
    async def put_all_applications(self, req: Request) -> Response:
        from ray.serve.schema import ServeDeploySchema

        try:
            config: ServeDeploySchema = ServeDeploySchema.parse_obj(await req.json())
        except ValidationError as e:
            return Response(
                status=400,
                text=repr(e),
            )

        try:
            await self._api_delegate.put_all_applications(config)
        except RayTaskError as e:
            return Response(
                status=400,
                text=str(e),
            )
        return Response()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
