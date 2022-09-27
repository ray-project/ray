from typing import Optional
import aiohttp
import logging
import os
from pydantic import BaseModel
import shutil
from urllib.parse import quote

from ray.dashboard.modules.metrics.grafana_datasource_template import (
    GRAFANA_DATASOURCE_TEMPLATE,
)
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"

routes = dashboard_optional_utils.ClassMethodRouteTable


METRICS_OUTPUT_ROOT_ENV_VAR = "RAY_METRICS_OUTPUT_ROOT"
METRICS_INPUT_ROOT = os.path.join(os.path.dirname(__file__), "export")

GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")

PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    METRICS_INPUT_ROOT, "prometheus", "prometheus.yml"
)


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
    def __init__(
        self, dashboard_head, http_session: Optional[aiohttp.ClientSession] = None
    ):
        super().__init__(dashboard_head)
        self.http_session = http_session or aiohttp.ClientSession()
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        default_metrics_root = os.path.join(self._dashboard_head.session_dir, "metrics")
        self.metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )

    @routes.get("/api/progress")
    async def get_progress(self, req):
        """
        Fetches the progress of tasks by job id. If job_id is not provided,
        then we will fetch the progress across all jobs.
        """
        job_id = req.query.get("job_id")
        job_id_query = f'{{JobId="{job_id}"}}' if job_id else ""
        query = f"sum(ray_tasks{job_id_query}) by (State)"
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
                    message="Error fetching data from prometheus. "
                    f"status: {resp.status}, message: {message}",
                )

    @staticmethod
    def is_minimal_module():
        return False

    def _create_default_grafana_configs(self):
        """
        Creates the grafana configurations that are by default provided by Ray.
        """
        grafana_config_output_path = os.path.join(self.metrics_root, "grafana")

        # Copy default grafana configurations
        if os.path.exists(grafana_config_output_path):
            shutil.rmtree(grafana_config_output_path)
        os.makedirs(os.path.dirname(grafana_config_output_path), exist_ok=True)
        shutil.copytree(GRAFANA_CONFIG_INPUT_PATH, grafana_config_output_path)
        # Overwrite grafana's prometheus datasource based on env var
        prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        data_sources_path = os.path.join(
            grafana_config_output_path, "provisioning", "datasources"
        )
        os.makedirs(
            data_sources_path,
            exist_ok=True,
        )
        with open(
            os.path.join(
                data_sources_path,
                "default.yml",
            ),
            "w",
        ) as f:
            f.write(GRAFANA_DATASOURCE_TEMPLATE.format(prometheus_host=prometheus_host))

    def _create_default_prometheus_configs(self):
        """
        Creates the prometheus configurations that are by default provided by Ray.
        """
        prometheus_config_output_path = os.path.join(
            self.metrics_root, "prometheus", "prometheus.yml"
        )

        # Copy default prometheus configurations
        if os.path.exists(prometheus_config_output_path):
            os.remove(prometheus_config_output_path)
        os.makedirs(os.path.dirname(prometheus_config_output_path), exist_ok=True)
        shutil.copy(PROMETHEUS_CONFIG_INPUT_PATH, prometheus_config_output_path)

    async def run(self, server):
        self._create_default_grafana_configs()
        self._create_default_prometheus_configs()

        logger.info(
            f"Generated prometheus and grafana configurations in: {self.metrics_root}"
        )
