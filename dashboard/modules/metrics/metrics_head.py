import aiohttp
import logging
import os
from pydantic import BaseModel
from urllib.parse import quote

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"

routes = dashboard_optional_utils.ClassMethodRouteTable


class TaskProgress(BaseModel):
    num_finished: int = 0
    num_running: int = 0
    nun_scheduled: int = 0
    num_waiting_for_dependencies: int = 0
    num_waiting_for_execution: int = 0


prometheus_metric_map = {
    "FINISHED": "num_finished",
    "RUNNING": "num_running",
    "SCHEDULED": "nun_scheduled",
    "WAITING_FOR_DEPENDENCIES": "num_waiting_for_dependencies",
    "WAITING_FOR_EXECUTION": "num_waiting_for_execution",
}


class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.http_session = aiohttp.ClientSession()
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )

    @routes.get("/api/progress")
    async def get_progress(self, req):
        query = "sum(ray_tasks) by (State)"
        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}"
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                if (
                    prom_data["status"] == "success"
                    and prom_data["data"]["resultType"] == "vector"
                ):
                    metrics = prom_data["data"]["result"]
                    kwargs = {
                        prometheus_metric_map[metric["metric"]["State"]]: metric[
                            "value"
                        ][1]
                        for metric in metrics
                        if metric["metric"]["State"] in prometheus_metric_map
                    }
                    progress = TaskProgress(**kwargs)
                    return dashboard_optional_utils.rest_response(
                        success=True, message="success", detail=progress.dict()
                    )

            else:
                message = await resp.text()
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message=f"Error fetching data from prometheus. "
                    "status: {resp.status}, message: {message}",
                )

    @staticmethod
    def is_minimal_module():
        return False

    async def run(self, server):
        pass
