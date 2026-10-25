import asyncio
import logging

from aiohttp.web import Request, Response

import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
import ray.exceptions
from ray._raylet import NodeID
from ray.dashboard.modules.reporter.utils import HealthChecker

routes = optional_utils.DashboardAgentRouteTable

logger = logging.getLogger(__name__)


class HealthzAgent(dashboard_utils.DashboardAgentModule):
    """Health check in the agent.

    This module adds health check related endpoint to the agent to check
    local components' health.
    """

    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        node_id = (
            NodeID.from_hex(dashboard_agent.node_id)
            if dashboard_agent.node_id
            else None
        )
        self._health_checker = HealthChecker(
            dashboard_agent.gcs_client,
            node_id,
        )

    @routes.get("/api/local_raylet_healthz")
    async def health_check(self, req: Request) -> Response:
        try:
            await self.raylet_health()
        except Exception as e:
            return Response(status=503, text=str(e), content_type="application/text")

        return Response(
            text="success",
            content_type="application/text",
        )

    async def raylet_health(self) -> str:
        try:
            alive = await self._health_checker.check_local_raylet_liveness()
            if alive is False:
                raise Exception("Local Raylet failed")
        except ray.exceptions.RpcError as e:
            # We only consider the error other than GCS unreachable as raylet failure
            # to avoid false positive.
            # In case of GCS failed, Raylet will crash eventually if GCS is not back
            # within a given time and the check will fail since agent can't live
            # without a local raylet.
            if e.rpc_code not in (
                ray._raylet.GRPC_STATUS_CODE_UNAVAILABLE,
                ray._raylet.GRPC_STATUS_CODE_UNKNOWN,
                ray._raylet.GRPC_STATUS_CODE_DEADLINE_EXCEEDED,
            ):
                raise Exception(f"Health check failed due to: {e}")
        return "success"

    async def local_gcs_health(self) -> str:
        # If GCS is not local, don't check its health.
        if not self._dashboard_agent.is_head:
            return "success (no local gcs)"
        gcs_alive = await self._health_checker.check_gcs_liveness()
        if not gcs_alive:
            raise Exception("GCS health check failed.")
        return "success"

    @routes.get("/api/healthz")
    async def unified_health(self, req: Request) -> Response:
        [raylet_check, gcs_check] = await asyncio.gather(
            self.raylet_health(),
            self.local_gcs_health(),
            return_exceptions=True,
        )
        checks = {"raylet": raylet_check, "gcs": gcs_check}

        # Log failures.
        status = 200
        for name, result in checks.items():
            if isinstance(result, Exception):
                status = 503
                logger.warning(f"health check {name} failed: {result}")

        return Response(
            status=status,
            text="\n".join([f"{name}: {result}" for name, result in checks.items()]),
            content_type="application/text",
        )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
