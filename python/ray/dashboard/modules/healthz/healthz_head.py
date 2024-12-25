# from aiohttp.web import HTTPServiceUnavailable, Request, Response
from aiohttp.web import HTTPServiceUnavailable, Response

import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_utils import GcsAioClient
from ray.dashboard.modules.healthz.utils import HealthChecker

routes = optional_utils.DashboardHeadActorRouteTable


class HealthzHead(dashboard_utils.DashboardHeadActorModule):
    """Health check in the head.

    This module adds health check related endpoint to the head to check
    GCS's heath.
    """

    def __init__(self, gcs_address):
        super().__init__(gcs_address=gcs_address)
        HealthzHead._gcs_aio_client = GcsAioClient(gcs_address)
        HealthzHead._health_checker = HealthChecker(self._gcs_aio_client)

    @routes.get("/api/gcs_healthz")
    async def health_check(self, req: bytes) -> Response:
        alive = False
        try:
            alive = await HealthzHead._health_checker.check_gcs_liveness()
            if alive is True:
                return Response(
                    text="success",
                    content_type="application/text",
                )
        except Exception as e:
            # TODO: aiohttp HTTPException is not serializable (pickle fails)
            return Response(
                text=f"Health check failed: {e}",
                status=HTTPServiceUnavailable.status_code,
                content_type="application/text",
            )
        return Response(
            text="Health check failed",
            status=HTTPServiceUnavailable.status_code,
            content_type="application/text",
        )

    async def run(self):
        print(f"HealthzHead running! {self._gcs_address}, {self._health_checker}")

    @staticmethod
    def is_minimal_module():
        return True
