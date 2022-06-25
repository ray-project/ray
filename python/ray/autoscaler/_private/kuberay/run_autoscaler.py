import logging
import os
import subprocess
import time

import ray
from ray._private import ray_constants
from ray._private.ray_logging import setup_component_logger
from ray._private.services import get_node_ip_address
from ray.autoscaler._private.kuberay.autoscaling_config import AutoscalingConfigProducer
from ray.autoscaler._private.monitor import Monitor

logger = logging.getLogger(__name__)

BACKOFF_S = 5


def run_kuberay_autoscaler(cluster_name: str, cluster_namespace: str):
    """Wait until the Ray head container is ready. Then start the autoscaler."""
    _setup_logging()
    head_ip = get_node_ip_address()
    ray_address = f"{head_ip}:6379"
    while True:
        try:
            subprocess.check_call(["ray", "health-check", "--address", ray_address])
            logger.info("The Ray head is ready. Starting the autoscaler.")
            break
        except subprocess.CalledProcessError:
            logger.warning("The Ray head is not yet ready.")
            logger.warning(f"Will check again in {BACKOFF_S} seconds.")
            time.sleep(BACKOFF_S)

    # autoscaling_config_producer reads the RayCluster CR from K8s and uses the CR
    # to output an autoscaling config.
    autoscaling_config_producer = AutoscalingConfigProducer(
        cluster_name, cluster_namespace
    )

    Monitor(
        address=ray_address,
        # The `autoscaling_config` arg can be a dict or a `Callable: () -> dict`.
        # In this case, it's a callable.
        autoscaling_config=autoscaling_config_producer,
        monitor_ip=head_ip,
    ).run()


def _setup_logging() -> None:
    """Log to autoscaler log file
    (typically, /tmp/ray/session_latest/logs/monitor.*)

    Also log to pod stdout (logs viewable with `kubectl logs <head-pod> -c autoscaler`).
    """
    # Write logs at info level to monitor.log.
    setup_component_logger(
        logging_level=ray_constants.LOGGER_LEVEL,  # info
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=os.path.join(
            ray._private.utils.get_ray_temp_dir(),
            ray._private.node.SESSION_LATEST,
            "logs",
        ),
        filename=ray_constants.MONITOR_LOG_FILE_NAME,  # monitor.log
        max_bytes=ray_constants.LOGGING_ROTATE_BYTES,
        backup_count=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        logger_name="ray",  # Root of the logging hierachy for Ray code.
    )
    # Logs will also be written to the container's stdout.
    # The stdout handler was set up in the cli entry point.
    # See ray.scripts.scripts::cli().
