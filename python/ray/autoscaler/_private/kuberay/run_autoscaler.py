import logging
import os
import subprocess
import sys
import time

import ray
from ray._common.network_utils import build_address
from ray._common.ray_constants import (
    LOGGING_ROTATE_BACKUP_COUNT,
    LOGGING_ROTATE_BYTES,
)
from ray._common.utils import env_integer, try_to_create_directory
from ray._private import ray_constants
from ray._private.ray_logging import setup_component_logger
from ray._private.services import get_node_ip_address
from ray._private.utils import get_all_node_info_until_retrieved
from ray._raylet import GcsClient
from ray.autoscaler._private.kuberay.autoscaling_config import AutoscalingConfigProducer
from ray.autoscaler._private.monitor import Monitor
from ray.autoscaler.v2.instance_manager.config import KubeRayConfigReader
from ray.autoscaler.v2.utils import is_autoscaler_v2
from ray.core.generated.gcs_service_pb2 import GetAllNodeInfoRequest

logger = logging.getLogger(__name__)

BACKOFF_S = 5


class _MaxLevelFilter(logging.Filter):
    """Filter that passes only records below a given level.

    Args:
        level: Records at or above this level are dropped.
    """

    def __init__(self, level: int):
        super().__init__()
        self._level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self._level


def _get_log_dir(gcs_client: GcsClient) -> str:
    head_node_selector = GetAllNodeInfoRequest.NodeSelector()
    head_node_selector.is_head_node = True

    # We need to wait until head node's raylet is registered in GCS.
    node_infos = get_all_node_info_until_retrieved(
        gcs_client,
        node_selectors=[head_node_selector],
    )

    node_info = next(iter(node_infos))
    # The head node reports where it actually writes logs, which is not
    # necessarily under its temp dir when it was started with `--logs-dir`.
    # It is unset when the head node runs a Ray version that predates the
    # field, so fall back to the historical location.
    logs_dir = getattr(node_info, "logs_dir", "")
    if logs_dir:
        return logs_dir

    temp_dir = getattr(node_info, "temp_dir", None)
    if temp_dir is None:
        raise Exception(
            "Node temp_dir was not found in NodeInfo. did the head node's raylet start successfully?"
        )
    return os.path.join(temp_dir, ray._private.ray_constants.SESSION_LATEST, "logs")


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

    gcs_client = GcsClient(ray_address)
    log_dir = _get_log_dir(gcs_client)

    # The Ray head container sets up the log directory. Thus, we set up logging
    # only after the Ray head is ready.
    _setup_logging(log_dir)

    # autoscaling_config_producer reads the RayCluster CR from K8s and uses the CR
    # to output an autoscaling config.
    autoscaling_config_producer = AutoscalingConfigProducer(
        cluster_name, cluster_namespace
    )

    if is_autoscaler_v2(fetch_from_server=True, gcs_client=gcs_client):
        from ray.autoscaler.v2.monitor import AutoscalerMonitor as MonitorV2

        MonitorV2(
            address=gcs_client.address,
            config_reader=KubeRayConfigReader(autoscaling_config_producer),
            log_dir=log_dir,
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


def _setup_logging(log_dir: str) -> None:
    """Log to autoscaler log file
    (typically, /tmp/ray/session_latest/logs/monitor.*)

    Also log to pod stdout (logs viewable with `kubectl logs <head-pod> -c autoscaler`).

    Args:
        log_dir: The path to the log directory.
    """
    # The director should already exist, but try (safely) to create it just in case.
    try_to_create_directory(log_dir)

    # Write logs at info level to monitor.log.
    max_bytes = env_integer("RAY_ROTATION_MAX_BYTES", LOGGING_ROTATE_BYTES)
    backup_count = env_integer("RAY_ROTATION_BACKUP_COUNT", LOGGING_ROTATE_BACKUP_COUNT)
    setup_component_logger(
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=log_dir,
        filename=ray_constants.MONITOR_LOG_FILE_NAME,  # monitor.log
        max_bytes=max_bytes,
        backup_count=backup_count,
    )

    # For the autoscaler, the root logger _also_ needs to write to the container's
    # stdout/stderr, not just ray_constants.MONITOR_LOG_FILE_NAME.
    #
    # Route by severity: sub-WARNING records go to stdout and WARNING and above go
    # to stderr. Container runtimes tag each log line with a severity derived from
    # the stream it arrived on, so emitting INFO on stderr makes every autoscaler
    # log line show up as an error in log collectors.
    level = logging.getLevelName(ray_constants.LOGGER_LEVEL.upper())
    formatter = logging.Formatter(ray_constants.LOGGER_FORMAT)

    # The KubeRay entry point does not redirect stdout after logging setup.
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(level)
    stdout_handler.addFilter(_MaxLevelFilter(logging.WARNING))

    stderr_handler = logging._StderrHandler()
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(level)
    # Floor stderr at WARNING, but keep a stricter LOGGER_LEVEL (e.g. "error"). The level
    # is read back rather than reusing `level` because getLevelName() returns a string
    # like "Level FOO" for names it does not recognize; setLevel is what rejects those,
    # raising ValueError as this function has always done for an invalid LOGGER_LEVEL.
    stderr_handler.setLevel(max(stderr_handler.level, logging.WARNING))

    logging.root.setLevel(level)
    logging.root.addHandler(stdout_handler)
    logging.root.addHandler(stderr_handler)
