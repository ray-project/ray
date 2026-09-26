import asyncio
import logging
import os

from aiohttp.web import Request, Response

import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
import ray.exceptions
from ray._raylet import NodeID
from ray.dashboard.modules.reporter.utils import HealthChecker
from ray.dashboard.optional_deps import aiohttp

routes = optional_utils.DashboardAgentRouteTable

logger = logging.getLogger(__name__)

_RAY_SERVICE_HEALTHZ_PATH = "/api/ray_service_healthz"
_SERVE_HEALTH_CHECK_PATH = "/-/healthz"
_SERVE_HEALTH_CHECK_TIMEOUT_S = 1.0
_SERVE_HEALTH_CHECK_PORT_ENV = "RAY_DASHBOARD_SERVE_HEALTH_CHECK_PORT"
_SERVE_HEALTH_CHECK_HOST_ENV = "RAY_DASHBOARD_SERVE_HEALTH_CHECK_HOST"


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

    def _is_serve_controller_running(self) -> bool:
        try:
            from ray.serve._private.constants import SERVE_NAMESPACE
            from ray.serve._private.controller import (
                CONFIG_CHECKPOINT_KEY,
                LOGGING_CONFIG_CHECKPOINT_KEY,
            )
            from ray.serve._private.storage.kv_store import RayInternalKVStore
        except ImportError:
            return False

        kv_store = RayInternalKVStore(
            namespace=f"ray-serve-{SERVE_NAMESPACE}",
            gcs_client=self._dashboard_agent.gcs_client,
        )
        return (
            kv_store.get(LOGGING_CONFIG_CHECKPOINT_KEY) is not None
            or kv_store.get(CONFIG_CHECKPOINT_KEY) is not None
        )

    async def local_serve_health(self) -> str:
        serve_running = await asyncio.to_thread(self._is_serve_controller_running)
        if not serve_running:
            return "success (serve not running)"

        serve_proxy_host = os.environ.get(_SERVE_HEALTH_CHECK_HOST_ENV, "127.0.0.1")
        serve_proxy_port = os.environ.get(_SERVE_HEALTH_CHECK_PORT_ENV, "8000")
        serve_health_url = (
            f"http://{serve_proxy_host}:{serve_proxy_port}{_SERVE_HEALTH_CHECK_PATH}"
        )

        try:
            async with self._dashboard_agent.http_session.get(
                serve_health_url, timeout=_SERVE_HEALTH_CHECK_TIMEOUT_S
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    raise Exception(
                        "Serve proxy health check failed with status code "
                        f"{response.status}: {body}"
                    )
        except aiohttp.ClientError as e:
            raise Exception(f"Serve proxy health check failed: {e}") from e
        except asyncio.TimeoutError as e:
            raise Exception("Serve proxy health check timed out") from e

        return "success"

    @routes.get(_RAY_SERVICE_HEALTHZ_PATH)
    async def ray_service_health(self, req: Request) -> Response:
        [raylet_check, serve_check] = await asyncio.gather(
            self.raylet_health(),
            self.local_serve_health(),
            return_exceptions=True,
        )
        checks = {"raylet": raylet_check, "serve": serve_check}

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
