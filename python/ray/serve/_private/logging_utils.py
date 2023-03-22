import functools
import logging
import os
from typing import Any, Callable, Dict, Optional

import ray
from ray.serve._private.constants import DEBUG_LOG_ENV_VAR, SERVE_LOGGER_NAME

COMPONENT_LOG_FMT = "%(levelname)s %(asctime)s {component_name} {component_id} %(filename)s:%(lineno)d - %(message)s"  # noqa:E501
LOG_FILE_FMT = "{component_name}_{component_id}.log"


def access_log_msg(*, method: str, route: str, status: str, latency_ms: float):
    """Returns a formatted message for an HTTP or ServeHandle access log."""
    return f"{method.upper()} {route} {status.upper()} {latency_ms:.1f}ms"


def to_file_only_filter(record: logging.LogRecord):
    """Filters log records based on the `to_file_only` parameter of the `extra` dictionary."""
    if not hasattr(record, "to_file_only") or record.to_file_only is None:
        return True

    return not record.to_file_only

def add_to_file_only_kwarg_to_logger_method(logger_method: Callable):
    """Wraps the provided logger method to take a `to_file_only` kwarg.

    The kwarg is converted into a field in the `extra` dictionary and passed to the underlying
    logger method.
    """
    @functools.wraps(logger_method)
    def wrapped(*args, to_file_only: Optional[bool] = False, extra: Optional[Dict[Any, Any]], **kwargs):
        if to_file_only is not None:
            extra = extra if extra is not None else {}
            extra["to_file_only"] = to_file_only

        return logger_method(*args, extra=extra, **kwargs)

    return wrapped

def configure_component_logger(
    *,
    component_name: str,
    component_id: str,
    component_type: Optional[str] = None,
    log_level: int = logging.INFO,
    log_to_stream: bool = True,
    log_to_file: bool = True,
    max_bytes: Optional[int] = None,
    backup_count: Optional[int] = None,
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
        COMPONENT_LOG_FMT.format(
            component_name=component_name, component_id=component_id
        )
    )
    if log_to_stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(to_file_only_filter)
        logger.addHandler(stream_handler)


    if log_to_file:
        logs_dir = os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(), "serve"
        )
        os.makedirs(logs_dir, exist_ok=True)
        if max_bytes is None:
            max_bytes = ray._private.worker._global_node.max_bytes
        if backup_count is None:
            backup_count = ray._private.worker._global_node.backup_count
        if component_type is not None:
            component_name = f"{component_type}_{component_name}"
        log_file_name = LOG_FILE_FMT.format(
            component_name=component_name, component_id=component_id
        )
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(logs_dir, log_file_name),
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Wrap logger methods to take the `to_file_only` kwarg.
    # If this parameter is passed and set to `True`, the given log message
    # will be logged only to the file handler and not the stream handler.
    logger.debug = add_to_file_only_kwarg_to_logger_method(logger.debug)
    logger.info = add_to_file_only_kwarg_to_logger_method(logger.info)
    logger.warning = add_to_file_only_kwarg_to_logger_method(logger.warning)
    logger.error = add_to_file_only_kwarg_to_logger_method(logger.error)
    logger.critical = add_to_file_only_kwarg_to_logger_method(logger.critical)
    logger.log = add_to_file_only_kwarg_to_logger_method(logger.log)
    logger.exception = add_to_file_only_kwarg_to_logger_method(logger.exception)


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
