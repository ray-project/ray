import logging
import os
import shutil

import aiohttp
import aiohttp.web

from ray.dashboard.modules.metrics.grafana_datasource_template import GRAFANA_DATASOURCE_TEMPLATE
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable

GRAFANA_HOST_ENV_VAR = "RAY_GRAFANA_HOST"
DEFAULT_GRAFANA_HOST = "http://localhost:3000"
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"
GRAFANA_HEALTHCHECK_PATH = f"api/health"
METRICS_PATH = "/tmp/ray/metrics"
GRAFANA_CONFIG_OUTPUT_PATH = f"{METRICS_PATH}/grafana"
GRAFANA_CONFIG_INPUT_PATH = os.path.join(os.path.dirname(__file__), "grafana")
PROMETHEUS_CONFIG_OUTPUT_PATH = f"{METRICS_PATH}/prometheus/prometheus.yml"
PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    os.path.dirname(__file__), "prometheus", "prometheus.yml")


class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._session = aiohttp.ClientSession()

    @routes.get("/api/grafana_health")
    async def grafana_health(self, req) -> aiohttp.web.Response:
        """
        Endpoint that checks if grafana is running
        """
        grafana_host = os.environ.get(GRAFANA_HOST_ENV_VAR, DEFAULT_GRAFANA_HOST)
        path = f"{grafana_host}/{GRAFANA_HEALTHCHECK_PATH}"
        try:
            async with self._session.get(path) as resp:
                if resp.status == 200:
                    json = await resp.json()
                    if json["database"] == "ok":
                        return dashboard_optional_utils.rest_response(
                            success=True,
                            message="Grafana running",
                            grafana_host=grafana_host)
        except Exception as e:
            logger.warning(
                "Error fetching grafana endpoint. Is grafana running?",
                exc_info=e)
            pass

        return dashboard_optional_utils.rest_response(
            success=False, message="Grafana healtcheck failed")

    @staticmethod
    def is_minimal_module():
        return True

    async def run(self, server):
        # Copy default grafana configurations
        if os.path.exists(GRAFANA_CONFIG_OUTPUT_PATH):
            shutil.rmtree(GRAFANA_CONFIG_OUTPUT_PATH)
        os.makedirs(os.path.dirname(GRAFANA_CONFIG_OUTPUT_PATH), exist_ok=True)
        shutil.copytree(GRAFANA_CONFIG_INPUT_PATH, GRAFANA_CONFIG_OUTPUT_PATH)
        # Overwrite grafana's prometheus datasource based on env var
        prometheus_host = os.environ.get(PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST)
        os.makedirs(os.path.join(GRAFANA_CONFIG_OUTPUT_PATH, "provisioning", "datasources"), exist_ok=True)
        with open(os.path.join(GRAFANA_CONFIG_OUTPUT_PATH, "provisioning", "datasources", "default.yaml"), "w") as f:
            f.write(GRAFANA_DATASOURCE_TEMPLATE.format(prometheus_host=prometheus_host))

        # Copy default prometheus configurations
        if os.path.exists(PROMETHEUS_CONFIG_OUTPUT_PATH):
            os.remove(PROMETHEUS_CONFIG_OUTPUT_PATH)
        os.makedirs(
            os.path.dirname(PROMETHEUS_CONFIG_OUTPUT_PATH), exist_ok=True)
        shutil.copy(PROMETHEUS_CONFIG_INPUT_PATH,
                    PROMETHEUS_CONFIG_OUTPUT_PATH)
