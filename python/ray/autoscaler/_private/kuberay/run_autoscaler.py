import argparse
import logging
import os

import ray
from ray import ray_constants
from ray._private.ray_logging import setup_component_logger
from ray._private.services import get_node_ip_address
from ray.autoscaler._private.monitor import Monitor

from autoscaling_config import AutoscalingConfigProducer


def setup_logging() -> None:
    """Log to autoscaler log file
    (typically, /tmp/ray/session_latest/logs/monitor.*)

    Also log to pod stdout (logs viewable with `kubectl logs <head-pod> -c autoscaler`).
    """
    # Write logs at info level to monitor.log.
    setup_component_logger(
        logging_level=ray_constants.LOGGER_LEVEL,  # info
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=os.path.join(
            ray._private.utils.get_ray_temp_dir(), ray.node.SESSION_LATEST, "logs"
        ),
        filename=ray_constants.MONITOR_LOG_FILE_NAME,  # monitor.log
        max_bytes=ray_constants.LOGGING_ROTATE_BYTES,
        backup_count=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
    )

    # Also log to stdout for debugging with `kubectl logs`.
    root_logger = logging.getLogger("")
    root_logger.setLevel(logging.INFO)

    root_handler = logging.StreamHandler()
    root_handler.setLevel(logging.INFO)
    root_handler.setFormatter(logging.Formatter(ray_constants.LOGGER_FORMAT))

    root_logger.addHandler(root_handler)


if __name__ == "__main__":
    setup_logging()

    parser = argparse.ArgumentParser(description="Kuberay Autoscaler")
    parser.add_argument(
        "--cluster-name",
        required=True,
        type=str,
        help="The name of the Ray Cluster.\n"
        "Should coincide with the `metadata.name` of the RayCluster CR.",
    )
    parser.add_argument(
        "--cluster-namespace",
        required=True,
        type=str,
        help="The Kubernetes namespace the Ray Cluster lives in.\n"
        "Should coincide with the `metadata.namespace` of the RayCluster CR.",
    )
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="The password to use for Redis",
    )
    args = parser.parse_args()

    head_ip = get_node_ip_address()

    autoscaling_config_producer = AutoscalingConfigProducer(
        args.cluster_name, args.cluster_namespace
    )

    Monitor(
        address=f"{head_ip}:6379",
        redis_password=args.redis_password,
        # The `autoscaling_config` arg can be a dict or a `Callable: () -> dict`.
        # In this case, it's a callable.
        autoscaling_config=autoscaling_config_producer,
        monitor_ip=head_ip,
    ).run()
