import asyncio
import json
import logging
import os
import shutil
from typing import Optional
from urllib.parse import quote

import aiohttp

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.async_utils import enable_monitor_loop_lag
from ray._private.ray_constants import (
    PROMETHEUS_SERVICE_DISCOVERY_FILE,
    SESSION_LATEST,
    env_integer,
)
from ray._private.utils import get_or_create_event_loop
from ray.dashboard.consts import AVAILABLE_COMPONENT_NAMES_FOR_METRICS
from ray.dashboard.modules.metrics.grafana_dashboard_factory import (
    generate_data_grafana_dashboard,
    generate_default_grafana_dashboard,
    generate_serve_deployment_grafana_dashboard,
    generate_serve_grafana_dashboard,
)
from ray.dashboard.modules.metrics.templates import (
    DASHBOARD_PROVISIONING_TEMPLATE,
    GRAFANA_DATASOURCE_TEMPLATE,
    GRAFANA_INI_TEMPLATE,
    PROMETHEUS_YML_TEMPLATE,
)

import psutil

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.DashboardHeadRouteTable

routes = dashboard_optional_utils.DashboardHeadRouteTable

METRICS_OUTPUT_ROOT_ENV_VAR = "RAY_METRICS_OUTPUT_ROOT"
METRICS_RECORD_INTERVAL_S = env_integer("METRICS_RECORD_INTERVAL_S", 5)

DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
DEFAULT_PROMETHEUS_HEADERS = "{}"
PROMETHEUS_HEADERS_ENV_VAR = "RAY_PROMETHEUS_HEADERS"
DEFAULT_PROMETHEUS_NAME = "Prometheus"
PROMETHEUS_NAME_ENV_VAR = "RAY_PROMETHEUS_NAME"
PROMETHEUS_HEALTHCHECK_PATH = "-/healthy"

DEFAULT_GRAFANA_HOST = "http://localhost:3000"
GRAFANA_HOST_ENV_VAR = "RAY_GRAFANA_HOST"
GRAFANA_HOST_DISABLED_VALUE = "DISABLED"
GRAFANA_IFRAME_HOST_ENV_VAR = "RAY_GRAFANA_IFRAME_HOST"
GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR = "RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR"
GRAFANA_HEALTHCHECK_PATH = "api/health"


# parse_prom_headers will make sure the input is in one of the following formats:
# 1. {"H1": "V1", "H2": "V2"}
# 2. [["H1", "V1"], ["H2", "V2"], ["H2", "V3"]]
def parse_prom_headers(prometheus_headers):
    parsed = json.loads(prometheus_headers)
    if isinstance(parsed, dict):
        if all(isinstance(k, str) and isinstance(v, str) for k, v in parsed.items()):
            return parsed
    if isinstance(parsed, list):
        if all(len(e) == 2 and all(isinstance(v, str) for v in e) for e in parsed):
            return parsed
    raise ValueError(
        f"{PROMETHEUS_HEADERS_ENV_VAR} should be a JSON string in one of the formats:\n"
        + "1) An object with string keys and string values.\n"
        + "2) an array of string arrays with 2 string elements each.\n"
        + 'For example, {"H1": "V1", "H2": "V2"} and\n'
        + '[["H1", "V1"], ["H2", "V2"], ["H2", "V3"]] are valid.'
    )


class PrometheusQueryError(Exception):
    def __init__(self, status, message):
        self.message = (
            "Error fetching data from prometheus. "
            f"status: {status}, message: {message}"
        )
        super().__init__(self.message)


class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self.grafana_host = os.environ.get(GRAFANA_HOST_ENV_VAR, DEFAULT_GRAFANA_HOST)
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        default_metrics_root = os.path.join(self.session_dir, "metrics")
        self.prometheus_headers = parse_prom_headers(
            os.environ.get(
                PROMETHEUS_HEADERS_ENV_VAR,
                DEFAULT_PROMETHEUS_HEADERS,
            )
        )
        session_latest_metrics_root = os.path.join(
            self.temp_dir, SESSION_LATEST, "metrics"
        )
        self._metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )
        self._metrics_root_session_latest = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, session_latest_metrics_root
        )
        self._grafana_config_output_path = os.path.join(self._metrics_root, "grafana")
        self._grafana_session_latest_config_output_path = os.path.join(
            self._metrics_root_session_latest, "grafana"
        )
        self._grafana_dashboard_output_dir = os.environ.get(
            GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR,
            os.path.join(self._grafana_config_output_path, "dashboards"),
        )

        self._prometheus_name = os.environ.get(
            PROMETHEUS_NAME_ENV_VAR, DEFAULT_PROMETHEUS_NAME
        )

        # To be set later when dashboards gets generated
        self._dashboard_uids = {}

        self._pid = os.getpid()
        self._component = "dashboard"
        assert self._component in AVAILABLE_COMPONENT_NAMES_FOR_METRICS
        self._dashboard_proc = psutil.Process()

        self._event_loop_lag_s_max: Optional[float] = None

    @routes.get("/api/grafana_health")
    async def grafana_health(self, req) -> aiohttp.web.Response:
        """
        Endpoint that checks if Grafana is running
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
            async with self.http_session.get(path) as resp:
                if resp.status != 200:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="Grafana healtcheck failed",
                        status=resp.status,
                    )
                json = await resp.json()
                # Check if the required Grafana services are running.
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
                    session_name=self.session_name,
                    dashboard_uids=self._dashboard_uids,
                    dashboard_datasource=self._prometheus_name,
                )

        except Exception as e:
            logger.debug(
                "Error fetching grafana endpoint. Is grafana running?", exc_info=e
            )

            return dashboard_optional_utils.rest_response(
                success=False, message="Grafana healtcheck failed", exception=str(e)
            )

    @routes.get("/api/prometheus_health")
    async def prometheus_health(self, req):
        try:
            path = f"{self.prometheus_host}/{PROMETHEUS_HEALTHCHECK_PATH}"

            async with self.http_session.get(
                path, headers=self.prometheus_headers
            ) as resp:
                if resp.status != 200:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="prometheus healthcheck failed.",
                        status=resp.status,
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
        Creates the Grafana configurations that are by default provided by Ray.
        """
        # Create Grafana configuration folder
        if os.path.exists(self._grafana_config_output_path):
            shutil.rmtree(self._grafana_config_output_path)
        os.makedirs(self._grafana_config_output_path, exist_ok=True)

        # Overwrite Grafana's configuration file
        grafana_provisioning_folder = os.path.join(
            self._grafana_config_output_path, "provisioning"
        )
        grafana_prov_folder_with_latest_session = os.path.join(
            self._grafana_session_latest_config_output_path, "provisioning"
        )
        with open(
            os.path.join(
                self._grafana_config_output_path,
                "grafana.ini",
            ),
            "w",
        ) as f:
            f.write(
                GRAFANA_INI_TEMPLATE.format(
                    grafana_provisioning_folder=grafana_prov_folder_with_latest_session
                )
            )

        # Overwrite Grafana's dashboard provisioning directory based on env var
        dashboard_provisioning_path = os.path.join(
            grafana_provisioning_folder, "dashboards"
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

        # Overwrite Grafana's Prometheus datasource based on env var
        prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        prometheus_headers = parse_prom_headers(
            os.environ.get(PROMETHEUS_HEADERS_ENV_VAR, DEFAULT_PROMETHEUS_HEADERS)
        )
        # parse_prom_headers will make sure the prometheus_headers is either format of:
        # 1. {"H1": "V1", "H2": "V2"} or
        # 2. [["H1", "V1"], ["H2", "V2"], ["H2", "V3"]]
        prometheus_header_pairs = []
        if isinstance(prometheus_headers, list):
            prometheus_header_pairs = prometheus_headers
        elif isinstance(prometheus_headers, dict):
            prometheus_header_pairs = list(prometheus_headers.items())

        data_sources_path = os.path.join(grafana_provisioning_folder, "datasources")
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
            f.write(
                GRAFANA_DATASOURCE_TEMPLATE(
                    prometheus_host=prometheus_host,
                    prometheus_name=self._prometheus_name,
                    jsonData={
                        f"httpHeaderName{i+1}": header
                        for i, (header, _) in enumerate(prometheus_header_pairs)
                    },
                    secureJsonData={
                        f"httpHeaderValue{i+1}": value
                        for i, (_, value) in enumerate(prometheus_header_pairs)
                    },
                )
            )
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "default_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            (
                content,
                self._dashboard_uids["default"],
            ) = generate_default_grafana_dashboard()
            f.write(content)
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "serve_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            content, self._dashboard_uids["serve"] = generate_serve_grafana_dashboard()
            f.write(content)
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "serve_deployment_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            (
                content,
                self._dashboard_uids["serve_deployment"],
            ) = generate_serve_deployment_grafana_dashboard()
            f.write(content)
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "data_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            (
                content,
                self._dashboard_uids["data"],
            ) = generate_data_grafana_dashboard()
            f.write(content)

    def _create_default_prometheus_configs(self):
        """
        Creates the Prometheus configurations that are by default provided by Ray.
        """
        prometheus_config_output_path = os.path.join(
            self._metrics_root, "prometheus", "prometheus.yml"
        )

        # Generate the default Prometheus configurations
        if os.path.exists(prometheus_config_output_path):
            os.remove(prometheus_config_output_path)
        os.makedirs(os.path.dirname(prometheus_config_output_path), exist_ok=True)

        # This code generates the Prometheus config based on the custom temporary root
        # path set by the user at Ray cluster start up (via --temp-dir). In contrast,
        # start_prometheus in install_and_start_prometheus.py uses a hardcoded
        # Prometheus config at PROMETHEUS_CONFIG_INPUT_PATH that always uses "/tmp/ray".
        # Other than the root path, the config file generated here is identical to that
        # hardcoded config file.
        prom_discovery_file_path = os.path.join(
            self.temp_dir, PROMETHEUS_SERVICE_DISCOVERY_FILE
        )
        with open(prometheus_config_output_path, "w") as f:
            f.write(
                PROMETHEUS_YML_TEMPLATE.format(
                    prom_metrics_service_discovery_file_path=prom_discovery_file_path
                )
            )

    @dashboard_utils.async_loop_forever(METRICS_RECORD_INTERVAL_S)
    async def record_dashboard_metrics(self):
        labels = {
            "ip": self.ip,
            "pid": self._pid,
            "Version": ray.__version__,
            "Component": self._component,
            "SessionName": self.session_name,
        }
        self.metrics.metrics_dashboard_cpu.labels(**labels).set(
            float(self._dashboard_proc.cpu_percent())
        )
        self.metrics.metrics_dashboard_mem_uss.labels(**labels).set(
            float(self._dashboard_proc.memory_full_info().uss) / 1.0e6
        )
        self.metrics.metrics_dashboard_mem_rss.labels(**labels).set(
            float(self._dashboard_proc.memory_full_info().rss) / 1.0e6
        )

        loop = get_or_create_event_loop()

        self.metrics.metrics_event_loop_tasks.labels(**labels).set(
            len(asyncio.all_tasks(loop))
        )

        # Report the max lag since the last export, if any.
        if self._event_loop_lag_s_max is not None:
            self.metrics.metrics_event_loop_lag.labels(**labels).set(
                float(self._event_loop_lag_s_max)
            )
            self._event_loop_lag_s_max = None

    async def run(self, server):
        self._create_default_grafana_configs()
        self._create_default_prometheus_configs()

        def on_new_lag(lag_s):
            # Record the lag. It's exported in `record_dashboard_metrics`
            self._event_loop_lag_s_max = max(self._event_loop_lag_s_max or 0, lag_s)

        enable_monitor_loop_lag(on_new_lag)

        logger.info(
            f"Generated prometheus and grafana configurations in: {self._metrics_root}"
        )

        await asyncio.gather(self.record_dashboard_metrics())

    async def _query_prometheus(self, query):
        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}",
            headers=self.prometheus_headers,
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                return prom_data

            message = await resp.text()
            raise PrometheusQueryError(resp.status, message)
