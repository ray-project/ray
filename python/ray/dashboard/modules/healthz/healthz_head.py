from aiohttp.web import HTTPServiceUnavailable, Request, Response

from ray.dashboard.modules.healthz.utils import HealthChecker
from ray.dashboard.subprocesses.routes import SubprocessRouteTable
from ray.dashboard.subprocesses.module import SubprocessModule

import logging

logger = logging.getLogger(__name__)


class HealthzHead(SubprocessModule):
    """Health check in the head.

    This module adds health check related endpoint to the head to check
    GCS's heath.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._health_checker = HealthChecker(self.gcs_aio_client)

    @SubprocessRouteTable.get("/api/gcs_healthz")
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

    async def init(self):
        pass
