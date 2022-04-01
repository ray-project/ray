import logging
import os
from typing import Optional

import ray
from ray.serve.constants import DEBUG_LOG_ENV_VAR, SERVE_LOGGER_NAME

COMPONENT_LOG_FMT = "%(levelname)s %(asctime)s {component} {component_id} %(filename)s:%(lineno)d - %(message)s"  # noqa:E501


def access_log_msg(*, method: str, route: str, status: str, latency_ms: float):
    """Returns a formatted message for an HTTP or ServeHandle access log."""
    return f"{method.upper()} {route} {status.upper()} {latency_ms:.1f}ms"


def configure_component_logger(
    *,
    component: str,
    component_id: str,
    log_level: Optional[int] = logging.INFO,
    log_to_stream: bool = True,
    log_file_name: Optional[str] = None,
):
    """Returns a logger to be used by a Serve component.

    The logger will log using a standard format to make components identifiable
    using the provided name and unique ID for this instance (e.g., replica ID).

    This logger will *not* propagate its log messages to the parent logger(s).
    """
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    logger.propagate = False
    logger.setLevel(log_level)
    if os.environ.get(DEBUG_LOG_ENV_VAR, "0") != "0":
        logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        COMPONENT_LOG_FMT.format(component=component, component_id=component_id)
    )
    if log_to_stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if log_file_name is not None:
        logs_dir = os.path.join(ray.worker._global_node.get_logs_dir_path(), "serve")
        os.makedirs(logs_dir, exist_ok=True)
        file_handler = logging.FileHandler(os.path.join(logs_dir, log_file_name))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


class LoggingContext:
    """
    Context manager to manage logging behaviors within a particular block, such as:
    1) Overriding logging level

    Source (python3 official documentation)
    https://docs.python.org/3/howto/logging-cookbook.html#using-a-context-manager-for-selective-logging # noqa: E501
    """

    def __init__(self, logger, level=None):
        self.logger = logger
        self.level = level

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
