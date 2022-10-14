import asyncio
import time
from typing import Any, Dict, Optional
import aiohttp
import logging
import os
from pydantic import BaseModel
import shutil
from urllib.parse import quote
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

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
PROMETHEUS_HEALTHCHECK_PATH = "-/healthy"

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
}


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
        self._grafana_dashboard_output_dir = os.environ.get(
            GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR
        )
        self._session = aiohttp.ClientSession()

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
        job_id_query = f'{{JobId="{job_id}"}}' if job_id else ""
        query = f"sum(ray_tasks{job_id_query}) by (State)"
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

    async def _check_grafana_running(self) -> bool:
        try:
            if self.grafana_host == GRAFANA_HOST_DISABLED_VALUE:
                return False

            path = f"{self.grafana_host}/{GRAFANA_HEALTHCHECK_PATH}"
            async with self._session.get(path) as resp:
                if resp.status != 200:
                    return False

                json = await resp.json()
                # Basic sanity check of grafana health check schema
                if "version" not in json:
                    return False

                return True
        except Exception:
            return False

    async def _check_prometheus_health(self) -> bool:
        try:
            path = f"{self.prometheus_host}/{PROMETHEUS_HEALTHCHECK_PATH}"

            async with self._session.get(path) as resp:
                if resp.status != 200:
                    return False

                text = await resp.text()
                # Basic sanity check of prometheus health check schema
                if "Prometheus" not in text:
                    return False

                return True
        except Exception:
            return False

    async def _push_telemetry_data_loop(self):
        time_between_pushes = 600  # 10 minutes
        last_push = (
            -time_between_pushes
        )  # First loop should always send telemetry data.
        while True:
            now = time.monotonic()
            try:
                if now - last_push >= time_between_pushes:
                    # Push data
                    logger.info("Recording dashboard metrics telemetry data...")
                    record_extra_usage_tag(
                        TagKey.DASHBOARD_METRICS_GRAFANA_ENABLED,
                        str(await self._check_grafana_running()),
                    )
                    record_extra_usage_tag(
                        TagKey.DASHBOARD_METRICS_PROMETHEUS_ENABLED,
                        str(await self._check_prometheus_health()),
                    )
                    last_push = now
            except Exception as exc:
                logger.warning("Error pushing telemetry data" + str(exc))

            await asyncio.sleep(max(0, time_between_pushes - (now - last_push)))

    async def run(self, server):
        self._create_default_grafana_configs()
        self._create_default_prometheus_configs()
        asyncio.create_task(self._push_telemetry_data_loop())

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
