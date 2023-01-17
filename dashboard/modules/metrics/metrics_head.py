import asyncio
import aiohttp
import logging
import os
import shutil

from typing import Optional

import psutil

from urllib.parse import quote
from ray.dashboard.modules.metrics.grafana_dashboard_factory import (
    generate_grafana_dashboard,
)
from ray.dashboard.modules.metrics.grafana_datasource_template import (
    GRAFANA_DATASOURCE_TEMPLATE,
)
from ray.dashboard.modules.metrics.grafana_dashboard_provisioning_template import (
    DASHBOARD_PROVISIONING_TEMPLATE,
)
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.consts import AVAILABLE_COMPONENT_NAMES_FOR_METRICS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable

routes = dashboard_optional_utils.ClassMethodRouteTable

METRICS_OUTPUT_ROOT_ENV_VAR = "RAY_METRICS_OUTPUT_ROOT"
METRICS_INPUT_ROOT = os.path.join(os.path.dirname(__file__), "export")
METRICS_RECORD_INTERVAL_S = 5

DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    METRICS_INPUT_ROOT, "prometheus", "prometheus.yml"
)
PROMETHEUS_HEALTHCHECK_PATH = "-/healthy"

DEFAULT_GRAFANA_HOST = "http://localhost:3000"
GRAFANA_HOST_ENV_VAR = "RAY_GRAFANA_HOST"
GRAFANA_HOST_DISABLED_VALUE = "DISABLED"
GRAFANA_IFRAME_HOST_ENV_VAR = "RAY_GRAFANA_IFRAME_HOST"
GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR = "RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR"
GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")
GRAFANA_HEALTHCHECK_PATH = "api/health"

PROMETHEUS_METRIC_MAP = {
    "FINISHED": "num_finished",
    "PENDING_ARGS_AVAIL": "num_pending_args_avail",
    "SUBMITTED_TO_WORKER": "num_submitted_to_worker",
    "RUNNING": "num_running",
    "RUNNING_IN_RAY_GET": "num_running",
    "RUNNING_IN_RAY_WAIT": "num_running",
    "PENDING_NODE_ASSIGNMENT": "num_pending_node_assignment",
    "PENDING_ARGS_FETCH": "num_pending_node_assignment",
    "PENDING_OBJ_STORE_MEM_AVAIL": "num_pending_node_assignment",
    "FAILED": "num_failed",
}


class PrometheusQueryError(Exception):
    def __init__(self, status, message):
        self.message = (
            "Error fetching data from prometheus. "
            f"status: {status}, message: {message}"
        )
        super().__init__(self.message)


class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(
        self, dashboard_head, http_session: Optional[aiohttp.ClientSession] = None
    ):
        super().__init__(dashboard_head)
        self.http_session = http_session or aiohttp.ClientSession()
        self.grafana_host = os.environ.get(GRAFANA_HOST_ENV_VAR, DEFAULT_GRAFANA_HOST)
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        default_metrics_root = os.path.join(self._dashboard_head.session_dir, "metrics")
        self._metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )
        grafana_config_output_path = os.path.join(self._metrics_root, "grafana")
        self._grafana_dashboard_output_dir = os.environ.get(
            GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR,
            os.path.join(grafana_config_output_path, "dashboards"),
        )

        self._session = aiohttp.ClientSession()
        self._ip = dashboard_head.ip
        self._pid = os.getpid()
        self._component = "dashboard"
        self._session_name = dashboard_head.session_name
        assert self._component in AVAILABLE_COMPONENT_NAMES_FOR_METRICS

    @routes.get("/api/grafana_health")
    async def grafana_health(self, req) -> aiohttp.web.Response:
        """
        Endpoint that checks if grafana is running
        """
        # If disabled, we don't want to show the metrics tab at all.
        if self.grafana_host == GRAFANA_HOST_DISABLED_VALUE:
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Grafana disabled",
                grafana_host=GRAFANA_HOST_DISABLED_VALUE,
            )

        grafana_iframe_host = os.environ.get(
            GRAFANA_IFRAME_HOST_ENV_VAR, self.grafana_host
        )
        path = f"{self.grafana_host}/{GRAFANA_HEALTHCHECK_PATH}"
        try:
            async with self._session.get(path) as resp:
                if resp.status != 200:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="Grafana healtcheck failed",
                        status=resp.status,
                    )
                json = await resp.json()
                # Check if the required grafana services are running.
                if json["database"] != "ok":
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="Grafana healtcheck failed. Database not ok.",
                        status=resp.status,
                        json=json,
                    )

                return dashboard_optional_utils.rest_response(
                    success=True,
                    message="Grafana running",
                    grafana_host=grafana_iframe_host,
                    session_name=self._session_name,
                )

        except Exception as e:
            logger.debug(
                "Error fetching grafana endpoint. Is grafana running?", exc_info=e
            )

            return dashboard_optional_utils.rest_response(
                success=False, message="Grafana healtcheck failed", exception=str(e)
            )

    @routes.get("/api/prometheus_health")
    async def prometheus_health(self, req) -> bool:
        try:
            path = f"{self.prometheus_host}/{PROMETHEUS_HEALTHCHECK_PATH}"

            async with self._session.get(path) as resp:
                if resp.status != 200:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="prometheus healthcheck failed.",
                        status=resp.status,
                    )

                text = await resp.text()
                # Basic sanity check of prometheus health check schema
                if "Prometheus" not in text:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="prometheus healthcheck failed.",
                        status=resp.status,
                        text=text,
                    )

                return dashboard_optional_utils.rest_response(
                    success=True,
                    message="prometheus running",
                )
        except Exception as e:
            logger.debug(
                "Error fetching prometheus endpoint. Is prometheus running?", exc_info=e
            )
            return dashboard_optional_utils.rest_response(
                success=False, message="prometheus healthcheck failed.", reason=str(e)
            )

    @staticmethod
    def is_minimal_module():
        return False

    def _create_default_grafana_configs(self):
        """
        Creates the grafana configurations that are by default provided by Ray.
        """
        grafana_config_output_path = os.path.join(self._metrics_root, "grafana")

        # Copy default grafana configurations
        if os.path.exists(grafana_config_output_path):
            shutil.rmtree(grafana_config_output_path)
        os.makedirs(os.path.dirname(grafana_config_output_path), exist_ok=True)
        shutil.copytree(GRAFANA_CONFIG_INPUT_PATH, grafana_config_output_path)

        # Overwrite grafana's dashboard provisioning directory based on env var
        dashboard_provisioning_path = os.path.join(
            grafana_config_output_path, "provisioning", "dashboards"
        )
        os.makedirs(
            dashboard_provisioning_path,
            exist_ok=True,
        )
        with open(
            os.path.join(
                dashboard_provisioning_path,
                "default.yml",
            ),
            "w",
        ) as f:
            f.write(
                DASHBOARD_PROVISIONING_TEMPLATE.format(
                    dashboard_output_folder=self._grafana_dashboard_output_dir
                )
            )

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
        os.makedirs(
            self._grafana_dashboard_output_dir,
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
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "default_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            f.write(generate_grafana_dashboard())

    def _create_default_prometheus_configs(self):
        """
        Creates the prometheus configurations that are by default provided by Ray.
        """
        prometheus_config_output_path = os.path.join(
            self._metrics_root, "prometheus", "prometheus.yml"
        )

        # Copy default prometheus configurations
        if os.path.exists(prometheus_config_output_path):
            os.remove(prometheus_config_output_path)
        os.makedirs(os.path.dirname(prometheus_config_output_path), exist_ok=True)
        shutil.copy(PROMETHEUS_CONFIG_INPUT_PATH, prometheus_config_output_path)

    @dashboard_utils.async_loop_forever(METRICS_RECORD_INTERVAL_S)
    async def record_dashboard_metrics(self):
        dashboard_proc = psutil.Process()
        self._dashboard_head.metrics.metrics_dashboard_cpu.labels(
            ip=self._ip,
            pid=self._pid,
            Component=self._component,
            SessionName=self._session_name,
        ).set(float(dashboard_proc.cpu_percent()) * 100)
        self._dashboard_head.metrics.metrics_dashboard_mem.labels(
            ip=self._ip,
            pid=self._pid,
            Component=self._component,
            SessionName=self._session_name,
        ).set(float(dashboard_proc.memory_full_info().uss) / 1.0e6)

    async def run(self, server):
        self._create_default_grafana_configs()
        self._create_default_prometheus_configs()
        await asyncio.gather(self.record_dashboard_metrics())

        logger.info(
            f"Generated prometheus and grafana configurations in: {self._metrics_root}"
        )

    async def _query_prometheus(self, query):
        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}"
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                return prom_data

            message = await resp.text()
            raise PrometheusQueryError(resp.status, message)
