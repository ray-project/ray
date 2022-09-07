import logging
import os
import shutil

from ray.dashboard.modules.metrics.grafana_datasource_template import (
    GRAFANA_DATASOURCE_TEMPLATE,
)
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
    os.path.dirname(__file__), "prometheus", "prometheus.yml"
)


class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @staticmethod
    def is_minimal_module():
        return False

    async def run(self, server):
        # Copy default grafana configurations
        if os.path.exists(GRAFANA_CONFIG_OUTPUT_PATH):
            shutil.rmtree(GRAFANA_CONFIG_OUTPUT_PATH)
        os.makedirs(os.path.dirname(GRAFANA_CONFIG_OUTPUT_PATH), exist_ok=True)
        shutil.copytree(GRAFANA_CONFIG_INPUT_PATH, GRAFANA_CONFIG_OUTPUT_PATH)
        # Overwrite grafana's prometheus datasource based on env var
        prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        data_sources_path = os.path.join(GRAFANA_CONFIG_OUTPUT_PATH, "provisioning", "datasources")
        os.makedirs(
            data_sources_path,
            exist_ok=True,
        )
        with open(
            os.path.join(
                data_sources_path,
                "default.yaml",
            ),
            "w",
        ) as f:
            f.write(GRAFANA_DATASOURCE_TEMPLATE.format(prometheus_host=prometheus_host))

        # Copy default prometheus configurations
        if os.path.exists(PROMETHEUS_CONFIG_OUTPUT_PATH):
            os.remove(PROMETHEUS_CONFIG_OUTPUT_PATH)
        os.makedirs(os.path.dirname(PROMETHEUS_CONFIG_OUTPUT_PATH), exist_ok=True)
        shutil.copy(PROMETHEUS_CONFIG_INPUT_PATH, PROMETHEUS_CONFIG_OUTPUT_PATH)

        logger.info(f"Generated prometheus and grafana configurations in: {METRICS_PATH}")
