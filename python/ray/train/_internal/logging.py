import logging
import logging.config
import os
from enum import Enum
from typing import Optional

import yaml

import ray
from ray.train._internal.session import _get_session

# JSON Encoding format for Ray Train structured logging
DEFAULT_JSON_LOG_ENCODING_FORMAT = "JSON"
# Env. variable to specify the encoding of the file logs when using the default config.
LOG_ENCODING_ENV = "RAY_TRAIN_LOG_ENCODING"
# Env. variable to specify the logging config path use defaults if not set
LOG_CONFIG_PATH_ENV = "RAY_TRAIN_LOG_CONFIG_PATH"


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
        "train_context_filter": {
            "()": "ray.train._internal.logging.TrainContextFilter"
        },
    },
    "handlers": {
        "console_json": {
            "class": "logging.StreamHandler",
            "formatter": "ray_json",
            "filters": ["core_context_filter", "train_context_filter"],
        },
        "console_text": {
            "class": "ray._private.log.PlainRayHandler",
            "formatter": "ray",
            "level": "INFO",
            "filters": ["train_context_filter", "console_filter"],
        },
        "file": {
            "class": "ray.train._internal.logging.SessionFileHandler",
            "formatter": "ray_json",
            "filename": "ray-train.log",
            "filters": ["core_context_filter", "train_context_filter"],
        },
    },
    "loggers": {
        "ray.train": {
            "level": "DEBUG",
            "handlers": ["file", "console_text"],
            "propagate": False,
        },
    },
}


class TrainLogKey(str, Enum):
    RUN_ID = "run_id"
    WORLD_SIZE = "world_size"
    WORLD_RANK = "world_rank"
    LOCAL_WORLD_SIZE = "local_world_size"
    LOCAL_RANK = "local_rank"
    NODE_RANK = "node_rank"


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


class TrainContextFilter(logging.Filter):
    """Add rank and size information to the log record if the log is from a train worker.

    This filter is a subclass of CoreContextFilter, which adds the job_id, worker_id,
    and node_id to the log record. This filter adds the rank and size information of
    the train context to the log record.
    """

    # TODO (hpguo): This implementation is subject to change in Train V2.
    def _is_worker_process(self) -> bool:
        # If this process does not have a train session, it is a driver process,
        # not a worker process.
        if not _get_session():
            return False
        # If this process has a train session, but its world size field is None,
        # it means the process is the train controller process created from Tune.
        # It is not a worker process, either.
        return _get_session().world_size is not None

    def filter(self, record):
        # If this process does not have a train session, it is a driver process,
        # not a worker process. We don't need to add any extra information to the log.
        if not _get_session():
            return True
        # Add the run_id info to the log for all ray train processes. Including a
        # controller process created from Tune and the worker processes.
        setattr(record, TrainLogKey.RUN_ID, _get_session().run_id)
        # If the process is a train controller process created from Tune, we don't
        # need to add the rank and size information to the log record.
        if not self._is_worker_process():
            return True
        # Otherwise, we need to check if the corresponding field of the train session
        # is None or not. If it is not None, we add the field to the log record.
        # If it is None, it means the process is the train driver created from Tune.
        setattr(record, TrainLogKey.WORLD_SIZE, _get_session().world_size)
        setattr(record, TrainLogKey.WORLD_RANK, _get_session().world_rank)
        setattr(record, TrainLogKey.LOCAL_WORLD_SIZE, _get_session().local_rank)
        setattr(record, TrainLogKey.LOCAL_RANK, _get_session().local_world_size)
        setattr(record, TrainLogKey.NODE_RANK, _get_session().node_rank)
        return True


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
    DEFAULT_LOG_CONFIG_JSON_STRING that is in this module.

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
        if (
            ray_train_log_encoding is not None
            and ray_train_log_encoding.upper() == DEFAULT_JSON_LOG_ENCODING_FORMAT
        ):
            for logger in config["loggers"].values():
                logger["handlers"].remove("console_text")
                logger["handlers"].append("console_json")

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
