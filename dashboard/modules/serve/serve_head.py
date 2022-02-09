import aiohttp.web
from aiohttp.web import Request, Response
import dataclasses
from functools import wraps
import logging
from typing import Any, Callable
import json
import traceback
from dataclasses import dataclass

import ray
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
from ray._private.gcs_utils import use_gcs_for_bootstrap
from ray._private.runtime_env.packaging import package_exists, upload_package_to_gcs

from ray import serve

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable

class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    async def _parse_and_validate_request(
        self, req: Request, request_type: dataclass
    ) -> Any:
        """Parse request and cast to request type. If parsing failed, return a
        Response object with status 400 and stacktrace instead.
        """
        try:
            return validate_request_type(await req.json(), request_type)
        except Exception as e:
            logger.info(f"Got invalid request type: {e}")
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_deployments(self, req: Request) -> Response:
        # TODO: this should be moved to @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True).
        dict_response = {
            name: str(deployment)
            for name, deployment in serve.list_deployments().items()
        }

        return Response(text=json.dumps(dict_response), content_type="application/json")

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
