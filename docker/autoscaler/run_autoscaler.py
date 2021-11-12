import logging

from ray import ray_constants
from ray._private.ray_logging import setup_component_logger
from ray._private.services import get_node_ip_address
from ray.autoscaler._private.monitor import Monitor
import yaml


AUTOSCALING_CONFIG_PATH = "/autoscaler/ray_bootstrap_config.yaml"
AUTOSCALING_LOG_DIR = "/tmp/ray/session_latest/logs/"


def setup_logging() -> None:
    """Setup logging to
    - log at INFO level to standard autoscaler log location. (logs viewable in UI)
    - log at DEBUG level to pod stout. (logs viewable with `kubectl logs <head-pod> -c autoscaler`)
    """
    # Write logs at info level to monitor.log.
    setup_component_logger(
        logging_level=ray_constants.LOGGER_LEVEL,  # info
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=AUTOSCALING_LOG_DIR,
        filename=ray_constants.MONITOR_LOG_FILE_NAME,  # monitor.log
        max_bytes=ray_constants.LOGGING_ROTATE_BYTES,
        backup_count=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
    )

    # Also log at DEBUG level to stdout for debugging with `kubectl logs`.
    root_logger = logging.getLogger("")
    root_logger.setLevel(logging.DEBUG)

    root_handler = logging.StreamHandler()
    root_handler.setLevel(logging.DEBUG)
    root_handler.setFormatter(logging.Formatter(ray_constants.LOGGER_FORMAT))

    root_logger.addHandler(root_handler)


if __name__ == "__main__":
    setup_logging()
    cluster_name = yaml.safe_load(open(AUTOSCALING_CONFIG_PATH).read())["cluster_name"]
    head_ip = get_node_ip_address()
    Monitor(
        redis_address=f"{head_ip}:6379",
        redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
        autoscaling_config=AUTOSCALING_CONFIG_PATH,
        monitor_ip=head_ip,
    ).run()
