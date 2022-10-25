from typing import Any, Dict, Optional
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

routes = dashboard_optional_utils.ClassMethodRouteTable

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
GRAFANA_HOST_DISABLED_VALUE = "DISABLED"
GRAFANA_IFRAME_HOST_ENV_VAR = "RAY_GRAFANA_IFRAME_HOST"
GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR = "RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR"
GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")
GRAFANA_HEALTHCHECK_PATH = "api/health"


class TaskProgress(BaseModel):
    num_finished: int = 0
    num_pending_args_avail: int = 0
    num_submitted_to_worker: int = 0
    num_running: int = 0
    num_pending_node_assignment: int = 0
    num_failed: int = 0
    num_unknown: int = 0


prometheus_metric_map = {
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
        self._metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )
        self._grafana_dashboard_output_dir = os.environ.get(
            GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR
        )
        self._session = aiohttp.ClientSession()

    @routes.get("/api/grafana_health")
    async def grafana_health(self, req) -> aiohttp.web.Response:
        """
        Endpoint that checks if grafana is running
        """
        grafana_host = os.environ.get(GRAFANA_HOST_ENV_VAR, DEFAULT_GRAFANA_HOST)

        # If disabled, we don't want to show the metrics tab at all.
        if grafana_host == GRAFANA_HOST_DISABLED_VALUE:
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Grafana disabled",
                grafana_host=GRAFANA_HOST_DISABLED_VALUE,
            )

        grafana_iframe_host = os.environ.get(GRAFANA_IFRAME_HOST_ENV_VAR, grafana_host)
        path = f"{grafana_host}/{GRAFANA_HEALTHCHECK_PATH}"
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
                )

        except Exception as e:
            logger.warning(
                "Error fetching grafana endpoint. Is grafana running?", exc_info=e
            )

            return dashboard_optional_utils.rest_response(
                success=False, message="Grafana healtcheck failed", exception=str(e)
            )

    @routes.get("/api/progress")
    async def get_progress(self, req):
        """
        Fetches the progress of tasks by job id. If job_id is not provided,
        then we will fetch the progress across all jobs.
        """
        job_id = req.query.get("job_id")

        job_id_filter = f'JobId="{job_id}"' if job_id else None
        filter_for_terminal_states = ['State=~"FINISHED|FAILED"']
        filter_for_non_terminal_states = ['State!~"FINISHED|FAILED"']
        if job_id_filter:
            filter_for_terminal_states.append(job_id_filter)
            filter_for_non_terminal_states.append(job_id_filter)

        filter_for_terminal_states_str = ",".join(filter_for_terminal_states)
        filter_for_non_terminal_states_str = ",".join(filter_for_non_terminal_states)

        # Ray does not currently permanently track worker task metrics.
        # The metric is cleared after a worker exits. We need to work around
        # these restrictions when we query metrics.

        # For terminal states (Finished, Failed), we know that the count can
        # never decrease. We therefore use the get the latest count of tasks
        # by fetching the max value over the past 14 days.
        query_for_terminal_states = (
            "sum(max_over_time("
            f"ray_tasks{{{filter_for_terminal_states_str}}}[14d])) by (State)"
        )

        # For non-terminal states, we assume that if a worker has at least
        # one task in one of these states, the worker has not exited. Therefore,
        # we fetch the current count.
        query_for_non_terminal_states = (
            "clamp_min(sum("
            f"ray_tasks{{{filter_for_non_terminal_states_str}}}) by (State), 0)"
        )
        query = f"{query_for_terminal_states} or {query_for_non_terminal_states}"

        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}"
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                progress = _format_prometheus_output(prom_data)
                if progress:
                    return dashboard_optional_utils.rest_response(
                        success=True, message="success", detail=progress.dict()
                    )

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
        grafana_config_output_path = os.path.join(self._metrics_root, "grafana")

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

        # Output the dashboards in a special directory
        if self._grafana_dashboard_output_dir:
            grafana_dashboards_dir = os.path.join(
                GRAFANA_CONFIG_INPUT_PATH, "dashboards"
            )
            # Copy all dashboard jsons from directory
            for root, _, files in os.walk(grafana_dashboards_dir):
                for file in files:
                    shutil.copy2(
                        os.path.join(root, file),
                        os.path.join(self._grafana_dashboard_output_dir, file),
                    )

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

    async def run(self, server):
        self._create_default_grafana_configs()
        self._create_default_prometheus_configs()

        logger.info(
            f"Generated prometheus and grafana configurations in: {self._metrics_root}"
        )


def _format_prometheus_output(prom_data: Dict[str, Any]) -> Optional[TaskProgress]:
    if prom_data["status"] == "success" and prom_data["data"]["resultType"] == "vector":
        metrics = prom_data["data"]["result"]
        kwargs = {}
        for metric in metrics:
            metric_name = metric["metric"]["State"]
            kwarg_name = (
                prometheus_metric_map[metric_name]
                if metric_name in prometheus_metric_map
                else "num_unknown"
            )
            # metric["value"] is a tuple where first item is a timestamp
            # and second item is the value.
            metric_value = int(metric["value"][1])
            kwargs[kwarg_name] = kwargs.get(kwarg_name, 0) + metric_value

        return TaskProgress(**kwargs)

    return None
