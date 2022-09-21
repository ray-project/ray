import logging
import os
import shutil

import aiohttp
import aiohttp.web

from ray.dashboard.modules.metrics.grafana_datasource_template import (
    GRAFANA_DATASOURCE_TEMPLATE,
)
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable

METRICS_OUTPUT_ROOT_ENV_VAR = "RAY_METRICS_OUTPUT_ROOT"
METRICS_INPUT_ROOT = os.path.join(os.path.dirname(__file__), "export")

DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    METRICS_INPUT_ROOT, "prometheus", "prometheus.yml"
)

DEFAULT_GRAFANA_HOST = "http://localhost:3000"
GRAFANA_HOST_ENV_VAR = "RAY_GRAFANA_HOST"
GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")
GRAFANA_HEALTHCHECK_PATH = f"api/health"



class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        default_metrics_root = os.path.join(self._dashboard_head.session_dir, "metrics")
        self.metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )
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
                            grafana_host=grafana_host,
                        )
        except Exception as e:
            logger.warning(
                "Error fetching grafana endpoint. Is grafana running?", exc_info=e
            )
            pass

        return dashboard_optional_utils.rest_response(
            success=False, message="Grafana healtcheck failed"
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
