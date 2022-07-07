import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
from ray.dashboard.modules.healthz.utils import HealthChecker
from aiohttp.web import Request, Response, HTTPServiceUnavailable
import grpc

routes = optional_utils.ClassMethodRouteTable


class HealthzHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._health_checker = HealthChecker(dashboard_head.gcs_aio_client)

    @routes.get("/api/gcs_healthz/")
    async def health_check(self, req: Request) -> Response:
        try:
            alive = await self._health_checker.check_gcs_liveness()
            if alive is True:
                return Response(
                    text="success",
                    content_type="application/text",
                )
        except grpc.RpcError as e:
            if e.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.UNKNOWN):
                return HTTPServiceUnavailable(reason=e.message())

        return HTTPServiceUnavailable(reason="Unknown error")
