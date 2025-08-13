import logging
import os
import subprocess
import time

import ray
from ray._private import ray_constants
from ray._common.ray_constants import (
    LOGGING_ROTATE_BYTES,
    LOGGING_ROTATE_BACKUP_COUNT,
)
from ray._private.ray_logging import setup_component_logger
from ray._private.services import get_node_ip_address
from ray._common.network_utils import build_address
from ray._common.utils import try_to_create_directory
from ray._raylet import GcsClient
from ray.autoscaler._private.kuberay.autoscaling_config import AutoscalingConfigProducer
from ray.autoscaler._private.monitor import Monitor
from ray.autoscaler.v2.instance_manager.config import KubeRayConfigReader
from ray.autoscaler.v2.utils import is_autoscaler_v2

logger = logging.getLogger(__name__)

BACKOFF_S = 5


def _get_log_dir() -> str:
    return os.path.join(
        ray._common.utils.get_ray_temp_dir(),
        ray._private.ray_constants.SESSION_LATEST,
        "logs",
    )


def run_kuberay_autoscaler(cluster_name: str, cluster_namespace: str):
    """Wait until the Ray head container is ready. Then start the autoscaler."""
    head_ip = get_node_ip_address()
    ray_address = build_address(head_ip, 6379)
    while True:
        try:
            # Autoscaler Ray version might not exactly match GCS version, so skip the
            # version check when checking GCS status.
            subprocess.check_call(
                [
                    "ray",
                    "health-check",
                    "--address",
                    ray_address,
                    "--skip-version-check",
                ]
            )
            logger.info("The Ray head is ready. Starting the autoscaler.")
            break
        except subprocess.CalledProcessError:
            logger.warning(
                f"The Ray head is not ready. Will check again in {BACKOFF_S} seconds."
            )
            time.sleep(BACKOFF_S)

    # The Ray head container sets up the log directory. Thus, we set up logging
    # only after the Ray head is ready.
    _setup_logging()

    # autoscaling_config_producer reads the RayCluster CR from K8s and uses the CR
    # to output an autoscaling config.
    autoscaling_config_producer = AutoscalingConfigProducer(
        cluster_name, cluster_namespace
    )

    gcs_client = GcsClient(ray_address)
    if is_autoscaler_v2(fetch_from_server=True, gcs_client=gcs_client):
        from ray.autoscaler.v2.monitor import AutoscalerMonitor as MonitorV2

        MonitorV2(
            address=gcs_client.address,
            config_reader=KubeRayConfigReader(autoscaling_config_producer),
            log_dir=_get_log_dir(),
            monitor_ip=head_ip,
        ).run()
    else:
        Monitor(
            address=gcs_client.address,
            # The `autoscaling_config` arg can be a dict or a `Callable: () -> dict`.
            # In this case, it's a callable.
            autoscaling_config=autoscaling_config_producer,
            monitor_ip=head_ip,
            # Let the autoscaler process exit after it hits 5 exceptions.
            # (See ray.autoscaler._private.constants.AUTOSCALER_MAX_NUM_FAILURES.)
            # Kubernetes will then restart the autoscaler container.
            retry_on_failure=False,
        ).run()


def _setup_logging() -> None:
    """Log to autoscaler log file
    (typically, /tmp/ray/session_latest/logs/monitor.*)

    Also log to pod stdout (logs viewable with `kubectl logs <head-pod> -c autoscaler`).
    """
    log_dir = _get_log_dir()
    # The director should already exist, but try (safely) to create it just in case.
    try_to_create_directory(log_dir)

    # Write logs at info level to monitor.log.
    setup_component_logger(
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=log_dir,
        filename=ray_constants.MONITOR_LOG_FILE_NAME,  # monitor.log
        max_bytes=LOGGING_ROTATE_BYTES,
        backup_count=LOGGING_ROTATE_BACKUP_COUNT,
    )

    # For the autoscaler, the root logger _also_ needs to write to stderr, not just
    # ray_constants.MONITOR_LOG_FILE_NAME.
    level = logging.getLevelName(ray_constants.LOGGER_LEVEL.upper())
    stderr_handler = logging._StderrHandler()
    stderr_handler.setFormatter(logging.Formatter(ray_constants.LOGGER_FORMAT))
    stderr_handler.setLevel(level)
    logging.root.setLevel(level)
    logging.root.addHandler(stderr_handler)

    # The stdout handler was set up in the Ray CLI entry point.
    # See ray.scripts.scripts::cli().
