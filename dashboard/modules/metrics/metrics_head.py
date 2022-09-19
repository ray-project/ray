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

METRICS_OUTPUT_ROOT_ENV_VAR = "RAY_METRICS_OUTPUT_ROOT"
METRICS_INPUT_ROOT = os.path.join(os.path.dirname(__file__), "export")

GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")

PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    METRICS_INPUT_ROOT, "prometheus", "prometheus.yml"
)


class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        default_metrics_root = os.path.join(self._dashboard_head.session_dir, "metrics")
        self.metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )

    @staticmethod
    def is_minimal_module():
        return False

    def _create_default_grafana_configs(self):
        """
        Creates the grafana configurations that are by default provided by Ray.
        This will completely replace the `/tmp/ray/metrics/grafana` folder.
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
        This will completely replace the `/tmp/ray/metrics/prometheus` folder.
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
