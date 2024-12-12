from aiohttp.web import HTTPServiceUnavailable, Request, Response

import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.healthz.utils import HealthChecker

routes = optional_utils.DashboardHeadRouteTable


class HealthzHead(dashboard_utils.DashboardHeadModule):
    """Health check in the head.

    This module adds health check related endpoint to the head to check
    GCS's heath.
    """

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._health_checker = HealthChecker(dashboard_head.gcs_aio_client)

    @routes.get("/api/gcs_healthz")
    async def health_check(self, req: Request) -> Response:
        alive = False
        try:
            alive = await self._health_checker.check_gcs_liveness()
            if alive is True:
                return Response(
                    text="success",
                    content_type="application/text",
                )
        except Exception as e:
            return HTTPServiceUnavailable(reason=f"Health check failed: {e}")

        return HTTPServiceUnavailable(reason="Health check failed")

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return True
