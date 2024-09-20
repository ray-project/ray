import logging
import logging.config
import os
from typing import Optional

import yaml

import ray

DEFAULT_CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "logging.yaml")
)

# Env. variable to specify the encoding of the file logs when using the default config.
RAY_TRAIN_LOG_ENCODING = os.environ.get("RAY_TRAIN_LOG_ENCODING", "").upper()

# Env. variable to specify the logging config path use defaults if not set
RAY_TRAIN_LOGGING_CONFIG_PATH = os.environ.get("RAY_TRAIN_LOGGING_CONFIG_PATH")


class HiddenRecordFilter:
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

    if RAY_TRAIN_LOGGING_CONFIG_PATH is not None:
        config = _load_logging_config(RAY_TRAIN_LOGGING_CONFIG_PATH)
    else:
        config = _load_logging_config(DEFAULT_CONFIG_PATH)
        if RAY_TRAIN_LOG_ENCODING == "JSON":
            for logger in config["loggers"].values():
                logger["handlers"].remove("file")
                logger["handlers"].append("file_json")

    logging.config.dictConfig(config)

    # After configuring logger, warn if RAY_TRAIN_LOGGING_CONFIG_PATH is used with
    # RAY_TRAIN_LOG_ENCODING, because they are not both supported together.
    if RAY_TRAIN_LOGGING_CONFIG_PATH is not None and RAY_TRAIN_LOG_ENCODING is not None:
        logger = logging.getLogger("ray.train")
        logger.warning(
            "Using `RAY_TRAIN_LOG_ENCODING` is not supported with "
            + "`RAY_TRAIN_LOGGING_CONFIG_PATH`"
        )


def reset_logging() -> None:
    """Reset the logger named 'ray.train' to its initial state.

    Used for testing.
    """
    logger = logging.getLogger("ray.train")
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)


def get_log_directory() -> Optional[str]:
    """Return the directory where Ray Train writes log files.

    If Ray isn't initialized, this function returns ``None``.
    """
    global_node = ray._private.worker._global_node
    if global_node is None:
        return None

    session_dir = global_node.get_session_dir_path()
    return os.path.join(session_dir, "logs", "ray-train")
