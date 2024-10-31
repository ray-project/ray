import logging
import logging.config
import os
from typing import Optional

import yaml

import ray
from ray.train.constants import LOG_CONFIG_PATH_ENV, LOG_ENCODING_ENV

# JSON Encoding format for Ray Train structured logging
DEFAULT_JSON_LOG_ENCODING_FORMAT = "JSON"

# Default logging configuration for Ray Train
DEFAULT_LOG_CONFIG_JSON_STRING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "ray": {
            "format": "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"  # noqa: E501
        },
        "ray_json": {"class": "ray._private.ray_logging.formatters.JSONFormatter"},
    },
    "filters": {
        "console_filter": {"()": "ray.train._internal.logging.HiddenRecordFilter"},
        "core_context_filter": {
            "()": "ray._private.ray_logging.filters.CoreContextFilter"
        },
    },
    "handlers": {
        "file_text": {
            "class": "ray.train._internal.logging.SessionFileHandler",
            "formatter": "ray",
            "filename": "ray-train.log",
        },
        "file_json": {
            "class": "ray.train._internal.logging.SessionFileHandler",
            "formatter": "ray_json",
            "filename": "ray-train.log",
            "filters": ["core_context_filter"],
        },
        "console": {
            "class": "ray._private.log.PlainRayHandler",
            "formatter": "ray",
            "level": "INFO",
            "filters": ["console_filter"],
        },
    },
    "loggers": {
        "ray.train": {
            "level": "DEBUG",
            "handlers": ["file_text", "console"],
            "propagate": False,
        },
    },
}


class HiddenRecordFilter(logging.Filter):
    """Filters out log records with the "hide" attribute set to True.

    This filter allows you to override default logging behavior. For example, if errors
    are printed by default, and you don't want to print a specific error, you can set
    the "hide" attribute to avoid printing the message.

    .. testcode::

        import logging
        logger = logging.getLogger("ray.train.spam")

        # This warning won't be printed to the console.
        logger.warning("ham", extra={"hide": True})
    """

    def filter(self, record):
        return not getattr(record, "hide", False)


class SessionFileHandler(logging.Handler):
    """A handler that writes to a log file in the Ray session directory.

    The Ray session directory isn't available until Ray is initialized, so this handler
    lazily creates the file handler when you emit a log record.

    Args:
        filename: The name of the log file. The file is created in the 'logs' directory
            of the Ray session directory.
    """

    def __init__(self, filename: str):
        super().__init__()
        self._filename = filename
        self._handler = None
        self._formatter = None
        self._path = None

    def emit(self, record):
        if self._handler is None:
            self._try_create_handler()
        if self._handler is not None:
            self._handler.emit(record)

    def setFormatter(self, fmt: logging.Formatter) -> None:
        if self._handler is not None:
            self._handler.setFormatter(fmt)
        self._formatter = fmt

    def _try_create_handler(self):
        assert self._handler is None

        log_directory = get_log_directory()
        if log_directory is None:
            return

        os.makedirs(log_directory, exist_ok=True)

        self._path = os.path.join(log_directory, self._filename)
        self._handler = logging.FileHandler(self._path)
        if self._formatter is not None:
            self._handler.setFormatter(self._formatter)


def configure_logging() -> None:
    """Configure the Python logger named 'ray.train'.

    This function loads the configration YAML specified by "RAY_TRAIN_LOGGING_CONFIG"
    environment variable. If the variable isn't set, this function loads the default
    "logging.yaml" file that is adjacent to this module.

    If "RAY_TRAIN_LOG_ENCODING" is specified as "JSON" we will enable JSON reading mode
    if using the default logging config.
    """

    def _load_logging_config(config_path: str):
        with open(config_path) as file:
            config = yaml.safe_load(file)
        return config

    # Dynamically configure the logger based on the environment variables.
    ray_train_log_encoding = os.environ.get(LOG_ENCODING_ENV)
    ray_train_log_config_path = os.environ.get(LOG_CONFIG_PATH_ENV)

    if ray_train_log_config_path is not None:
        config = _load_logging_config(ray_train_log_config_path)
    else:
        config = DEFAULT_LOG_CONFIG_JSON_STRING
        if ray_train_log_encoding == DEFAULT_JSON_LOG_ENCODING_FORMAT:
            for logger in config["loggers"].values():
                logger["handlers"].remove("file_text")
                logger["handlers"].append("file_json")

    logging.config.dictConfig(config)

    # After configuring logger, warn if RAY_TRAIN_LOGGING_CONFIG_PATH is used with
    # RAY_TRAIN_LOG_ENCODING, because they are not both supported together.
    if ray_train_log_config_path is not None and ray_train_log_encoding is not None:
        logger = logging.getLogger("ray.train")
        logger.warning(
            f"Using {LOG_ENCODING_ENV} is not supported with "
            f"{LOG_CONFIG_PATH_ENV}."
            "If you are already specifying a custom config yaml file, "
            "please configure your desired encoding in the yaml file as well."
        )


def get_log_directory() -> Optional[str]:
    """Return the directory where Ray Train writes log files.

    If Ray isn't initialized, this function returns ``None``.
    """
    global_node = ray._private.worker._global_node
    if global_node is None:
        return None

    session_dir = global_node.get_session_dir_path()
    return os.path.join(session_dir, "logs", "ray-train")
