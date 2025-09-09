import logging
import asyncio
from typing import Set
import aiohttp.web
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray._common.utils import get_or_create_event_loop
from ray.dashboard.utils import async_loop_forever
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.util.insight import (
    create_http_insight_client,
)

logger = logging.getLogger(__name__)


class InsightHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._insight_client = None
        self._insight_server_address = None
        self._loop = get_or_create_event_loop()
        self._background_tasks: Set[asyncio.Task] = set()

    async def is_insight_server_alive(self):
        if self._insight_server_address is None:
            return False
        insight_client = create_http_insight_client(self._insight_server_address)

        resp = await insight_client.async_ping()
        if not resp["result"]:
            return False
        return True

    @async_loop_forever(10)
    async def _refresh_client(self):
        insight_server_address = await self.gcs_client.async_internal_kv_get(
            "insight_monitor_address",
            namespace="flowinsight",
            timeout=5,
        )
        if insight_server_address is None:
            return
        self._insight_server_address = insight_server_address.decode()

        if self._insight_client is None:
            self._insight_client = create_http_insight_client(
                self._insight_server_address
            )

    @routes.get("/insight/{path:.*}")
    async def proxy_get_request(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Proxy GET requests to the insight monitor service."""
        return await self._proxy_request(req, "GET")

    @routes.post("/insight/{path:.*}")
    async def proxy_post_request(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """Proxy POST requests to the insight monitor service."""
        return await self._proxy_request(req, "POST")

    async def _proxy_request(
        self, req: aiohttp.web.Request, method: str
    ) -> aiohttp.web.Response:
        """Helper method to proxy requests to the insight monitor service."""
        try:
            path = req.match_info["path"]
            query_string = req.query_string

            if self._insight_server_address is None:
                insight_server_address = await self.gcs_client.async_internal_kv_get(
                    "insight_monitor_address",
                    namespace="flowinsight",
                    timeout=5,
                )
                if insight_server_address is None:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="InsightMonitor address not found in KV store",
                    )

                self._insight_server_address = insight_server_address.decode()

            if not await self.is_insight_server_alive():
                self._insight_server_address = (
                    await self.gcs_client.async_internal_kv_get(
                        "insight_monitor_address",
                        namespace="flowinsight",
                        timeout=5,
                    )
                )
                if self._insight_server_address is None:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="InsightMonitor address not found in KV store",
                    )
                self._insight_server_address = self._insight_server_address.decode()
                self._insight_client = None

            # Get insight monitor address
            target_url = f"http://{self._insight_server_address}/{path}"
            if query_string:
                target_url += f"?{query_string}"

            # Forward the request
            async with aiohttp.ClientSession() as session:
                if method == "GET":
                    async with session.get(
                        target_url, headers=dict(req.headers)
                    ) as resp:
                        body = await resp.read()
                        return aiohttp.web.Response(
                            body=body,
                            status=resp.status,
                            headers=resp.headers,
                        )
                elif method == "POST":
                    data = await req.read()
                    async with session.post(
                        target_url, data=data, headers=dict(req.headers)
                    ) as resp:
                        body = await resp.read()
                        return aiohttp.web.Response(
                            body=body,
                            status=resp.status,
                            headers=resp.headers,
                        )
                else:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message=f"Unsupported method: {method}",
                    )
        except Exception as e:
            logger.error(f"Error proxying request to insight monitor: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False,
                message=f"Error proxying request to insight monitor: {str(e)}",
            )

    async def run(self):
        await super().run()
        coros = [
            self._refresh_client()
        ]
        for coro in coros:
            task = self._loop.create_task(coro)
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
