import aiohttp
import logging
import os
from pydantic import BaseModel
import shutil
from urllib.parse import quote

# from ray.dashboard.modules.metrics.grafana_datasource_template import (
#     GRAFANA_DATASOURCE_TEMPLATE, )
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"

METRICS_PATH = "/tmp/ray/metrics"
GRAFANA_CONFIG_OUTPUT_PATH = f"{METRICS_PATH}/grafana"
GRAFANA_CONFIG_INPUT_PATH = os.path.join(os.path.dirname(__file__), "grafana")

PROMETHEUS_CONFIG_OUTPUT_PATH = f"{METRICS_PATH}/prometheus/prometheus.yml"
PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    os.path.dirname(__file__), "prometheus", "prometheus.yml")

USER_CUSTOM_METRIC_CONFIG_PATH = f"{METRICS_PATH}/custom"
USER_CUSTOM_GRAFANA_CONFIG_PATH = f"{USER_CUSTOM_METRIC_CONFIG_PATH}/grafana-dashboards"

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
        self.prometheus_host = os.environ.get(PROMETHEUS_HOST_ENV_VAR,
                                              DEFAULT_PROMETHEUS_HOST)

    @routes.get("/api/progress")
    async def get_progress(self, req):
        query = "sum(ray_tasks) by (State)"
        async with self.http_session.get(
                f"{self.prometheus_host}/api/v1/query?query={quote(query)}"
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                if prom_data["status"] == "success" and prom_data["data"]["resultType"] == "vector":
                    metrics = prom_data["data"]["result"]
                    kwargs = {
                        prometheus_metric_map[metric["metric"]["State"]]:
                        metric["value"][1]
                        for metric in metrics
                        if metric["metric"]["State"] in prometheus_metric_map
                    }
                    progress = TaskProgress(**kwargs)
                    return dashboard_optional_utils.rest_response(
                        success=True,
                        message="success",
                        detail=progress.dict())

            else:
                message = await resp.text()
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message=
                    f"Error fetching data from prometheus. status: {resp.status}, message: {message}"
                )

    @staticmethod
    def is_minimal_module():
        return False

    def _create_default_grafana_configs(self):
        """
        Creates the grafana configurations that are by default provided by Ray.
        This will completely replace the `/tmp/ray/metrics/grafana` folder.

        Users can put custom configurations in `/tmp/ray/metrics/custom`. That folder
        will be picked up by grafana but not be modified by ray.
        """
        # Copy default grafana configurations
        if os.path.exists(GRAFANA_CONFIG_OUTPUT_PATH):
            shutil.rmtree(GRAFANA_CONFIG_OUTPUT_PATH)
        os.makedirs(os.path.dirname(GRAFANA_CONFIG_OUTPUT_PATH), exist_ok=True)
        shutil.copytree(GRAFANA_CONFIG_INPUT_PATH, GRAFANA_CONFIG_OUTPUT_PATH)
        # Overwrite grafana's prometheus datasource based on env var

        data_sources_path = os.path.join(GRAFANA_CONFIG_OUTPUT_PATH,
                                         "provisioning", "datasources")
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
            f.write(
                GRAFANA_DATASOURCE_TEMPLATE.format(
                    prometheus_host=self.prometheus_host))

    def _create_default_prometheus_configs(self):
        """
        Creates the prometheus configurations that are by default provided by Ray.
        This will completely replace the `/tmp/ray/metrics/prometheus` folder.
        """

        # Copy default prometheus configurations
        if os.path.exists(PROMETHEUS_CONFIG_OUTPUT_PATH):
            os.remove(PROMETHEUS_CONFIG_OUTPUT_PATH)
        os.makedirs(
            os.path.dirname(PROMETHEUS_CONFIG_OUTPUT_PATH), exist_ok=True)
        shutil.copy(PROMETHEUS_CONFIG_INPUT_PATH,
                    PROMETHEUS_CONFIG_OUTPUT_PATH)

    def _create_custom_config_folders(self):
        """
        Creates folder for custom metric configurations if it doesn't exist.

        Currently, only custom grafana dashboards are supported at
        `/tmp/ray/metrics/custom/grafana-dashboards`
        """

        # grafana
        os.makedirs(USER_CUSTOM_GRAFANA_CONFIG_PATH, exist_ok=True)

    async def run(self, server):
        # self._create_default_grafana_configs()
        # self._create_default_prometheus_configs()
        # self._create_custom_config_folders()

        logger.info(
            f"Generated prometheus and grafana configurations in: {METRICS_PATH}"
        )
