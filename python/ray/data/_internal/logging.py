import logging
import logging.config
import os
from typing import Any, Dict, List, Optional, Tuple

import yaml

import ray

DEFAULT_TEXT_FORMATTER = (
    "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"  # noqa: E501
)
DEFAULT_JSON_FORMATTER = ray._common.formatters.JSONFormatter
DEFAULT_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "ray": {"format": DEFAULT_TEXT_FORMATTER},
        "ray_json": {
            "class": f"{DEFAULT_JSON_FORMATTER.__module__}.{DEFAULT_JSON_FORMATTER.__name__}"
        },
    },
    "filters": {
        "console_filter": {"()": "ray.data._internal.logging.HiddenRecordFilter"},
        "core_context_filter": {"()": "ray._common.filters.CoreContextFilter"},
    },
    "handlers": {
        "file": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray",
            "filename": "ray-data.log",
        },
        "file_json": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray_json",
            "filename": "ray-data.log",
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
        "ray.data": {
            "level": "DEBUG",
            "handlers": ["file", "console"],
            "propagate": False,
        },
        "ray.air.util.tensor_extensions": {
            "level": "DEBUG",
            "handlers": ["file", "console"],
            "propagate": False,
        },
    },
}

# Dictionary of substitutions to be performed when using JSON mode. Handlers with names
# corresponding to keys will be replaced by those corresponding to values.
RAY_DATA_LOG_HANDLER_JSON_SUBSTITUTIONS = {"file": "file_json"}

# Env. variable to specify the encoding of the file logs when using the default config.
RAY_DATA_LOG_ENCODING_ENV_VAR_NAME = "RAY_DATA_LOG_ENCODING"

# Env. variable to specify the logging config path use defaults if not set
RAY_DATA_LOGGING_CONFIG_ENV_VAR_NAME = "RAY_DATA_LOGGING_CONFIG"

_DATASET_LOGGER_HANDLER = {}
_ACTIVE_DATASET = None

# To facilitate debugging, Ray Data writes debug logs to a file. However, if Ray Data
# logs every scheduler loop, logging might impact performance. So, we add a "TRACE"
# level where logs aren't written by default.
#
# Use the following code to log a message at the "TRACE" level:
# ```
# logger.log(logging.getLevelName("TRACE"), "Your message here.")
# ````
logging.addLevelName(logging.DEBUG - 1, "TRACE")


class HiddenRecordFilter:
    """Filters out log records with the "hide" attribute set to True.

    This filter allows you to override default logging behavior. For example, if errors
    are printed by default, and you don't want to print a specific error, you can set
    the "hide" attribute to avoid printing the message.

    .. testcode::

        import logging
        logger = logging.getLogger("ray.data.spam")

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


def _get_logging_config() -> Optional[dict]:
    import copy

    def _load_logging_config(config_path: str):
        with open(config_path) as file:
            config = yaml.safe_load(file)
        return config

    # Dynamically load env vars
    config_path = os.environ.get(RAY_DATA_LOGGING_CONFIG_ENV_VAR_NAME)
    log_encoding = os.environ.get(RAY_DATA_LOG_ENCODING_ENV_VAR_NAME)

    if config_path is not None:
        config = _load_logging_config(config_path)
    else:
        # Make a deep copy to avoid mutating the DEFAULT_CONFIG
        config = copy.deepcopy(DEFAULT_CONFIG)
        if log_encoding is not None and log_encoding.upper() == "JSON":
            for logger in config["loggers"].values():
                for (
                    old_handler_name,
                    new_handler_name,
                ) in RAY_DATA_LOG_HANDLER_JSON_SUBSTITUTIONS.items():
                    logger["handlers"].remove(old_handler_name)
                    logger["handlers"].append(new_handler_name)

    return config


def _get_logger_names() -> List[str]:
    logger_config = _get_logging_config().get("loggers", {})

    return list(logger_config.keys())


def _get_loggers_to_configure(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return subset of loggers from config that need configuration.

    Loggers that already have handlers attached are skipped to preserve existing setup.
    """
    loggers_to_configure: Dict[str, Dict[str, Any]] = {}
    for logger_name, logger_config in config.get("loggers", {}).items():
        logger = logging.getLogger(logger_name)
        if not logger.handlers:
            loggers_to_configure[logger_name] = logger_config
    return loggers_to_configure


def _snapshot_handler_attrs(handler: logging.Handler) -> Dict[str, Any]:
    """Capture a safe, minimal set of handler attributes to restore later."""
    attrs: Dict[str, Any] = {
        "level": handler.level,
        "formatter": getattr(handler, "formatter", None),
        "filters": list(getattr(handler, "filters", []) or []),
    }
    if hasattr(handler, "target"):
        attrs["target"] = getattr(handler, "target", None)
    return attrs


def _apply_handler_attrs(handler: logging.Handler, attrs: Dict[str, Any]) -> None:
    """Restore the captured handler attributes (best-effort, safe only)."""
    try:
        if "level" in attrs:
            handler.setLevel(attrs["level"])
    except Exception:
        pass

    try:
        fmt = attrs.get("formatter")
        if fmt is not None:
            handler.setFormatter(fmt)
    except Exception:
        pass

    try:
        current_filters = list(getattr(handler, "filters", []) or [])
        for f in attrs.get("filters", []) or []:
            if f not in current_filters:
                handler.addFilter(f)
    except Exception:
        pass

    if "target" in attrs:
        try:
            if hasattr(handler, "setTarget"):
                handler.setTarget(attrs["target"])
            elif hasattr(handler, "target"):
                handler.target = attrs["target"]
        except Exception:
            pass


def _preserve_and_detach_child_handlers(
    prefixes: List[str],
) -> Dict[str, List[Tuple[logging.Handler, Dict[str, Any]]]]:
    """Collect and detach handlers from child loggers of given prefixes.

    We temporarily detach to prevent dictConfig from closing or mutating them.
    Returns a mapping: logger_name -> list of (handler, preserved_attrs).
    """
    preserved: Dict[str, List[Tuple[logging.Handler, Dict[str, Any]]]] = {}
    for logger_name, existing in logging.root.manager.loggerDict.items():
        if not isinstance(existing, logging.Logger):
            continue

        if any(logger_name.startswith(prefix) for prefix in prefixes):
            child_logger = logging.getLogger(logger_name)
            if not child_logger.handlers:
                continue

            handlers_info: List[Tuple[logging.Handler, Dict[str, Any]]] = []
            for handler in list(child_logger.handlers):
                handlers_info.append((handler, _snapshot_handler_attrs(handler)))
                child_logger.removeHandler(handler)

            preserved[logger_name] = handlers_info

    return preserved


def _restore_child_handlers(
    preserved: Dict[str, List[Tuple[logging.Handler, Dict[str, Any]]]]
) -> None:
    """Reattach preserved child handlers and restore minimal attributes if needed."""
    for logger_name, handlers_info in preserved.items():
        child_logger = logging.getLogger(logger_name)
        for handler, attrs in handlers_info:
            child_logger.addHandler(handler)
            _apply_handler_attrs(handler, attrs)


_logging_configured = False


def configure_logging() -> None:
    """Configure the Python logger named 'ray.data'.

    This function loads the configration YAML specified by "RAY_DATA_LOGGING_CONFIG"
    environment variable. If the variable isn't set, this function loads the default
    "logging.yaml" file that is adjacent to this module.

    If "RAY_DATA_LOG_ENCODING" is specified as "JSON" we will enable JSON logging mode
    if using the default logging config.

    Note: This function is idempotent after initial configuration when handlers have been
    added to ray.data loggers, as reconfiguration would close and invalidate those handlers.
    """
    global _logging_configured

    # If already configured, don't reconfigure
    if _logging_configured:
        return

    # Dynamically load env vars and get config
    config_path = os.environ.get(RAY_DATA_LOGGING_CONFIG_ENV_VAR_NAME)
    log_encoding = os.environ.get(RAY_DATA_LOG_ENCODING_ENV_VAR_NAME)
    config = _get_logging_config()

    # Determine which Ray Data loggers actually need configuration (preserve existing handlers)
    loggers_to_configure = _get_loggers_to_configure(config)

    # If no loggers need configuration, mark configured and return
    if not loggers_to_configure:
        _logging_configured = True
        return

    # Preserve and detach handlers from child loggers (e.g., "ray.data.*") to avoid closure by dictConfig
    prefixes_to_configure = list(loggers_to_configure.keys())
    preserved_child_handlers = _preserve_and_detach_child_handlers(
        prefixes_to_configure
    )

    # Configure only the necessary loggers
    config["loggers"] = loggers_to_configure
    logging.config.dictConfig(config)
    _logging_configured = True

    # Restore preserved child logger handlers and any relevant attributes
    _restore_child_handlers(preserved_child_handlers)

    # After configuring logger, warn if RAY_DATA_LOGGING_CONFIG is used with
    # RAY_DATA_LOG_ENCODING, because they are not both supported together.
    if config_path is not None and log_encoding is not None:
        logger = logging.getLogger(__name__)
        logger.warning(
            "Using `RAY_DATA_LOG_ENCODING` is not supported with "
            + "`RAY_DATA_LOGGING_CONFIG`"
        )


def reset_logging() -> None:
    """Reset the logger named 'ray.data' to its initial state.

    Used for testing.
    """
    global _DATASET_LOGGER_HANDLER
    global _ACTIVE_DATASET
    global _logging_configured

    def _clear_logger_handlers(handler_names: List[str]):
        for name in handler_names:
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.setLevel(logging.NOTSET)

    # Also clear all loggers managed by Ray Data logging config
    _clear_logger_handlers(_get_logger_names())

    _DATASET_LOGGER_HANDLER = {}
    _ACTIVE_DATASET = None
    _logging_configured = False


def get_log_directory() -> Optional[str]:
    """Return the directory where Ray Data writes log files.

    If Ray isn't initialized, this function returns ``None``.
    """
    global_node = ray._private.worker._global_node
    if global_node is None:
        return None

    session_dir = global_node.get_session_dir_path()
    return os.path.join(session_dir, "logs", "ray-data")


def _get_default_formatter() -> logging.Formatter:
    log_encoding = os.environ.get(RAY_DATA_LOG_ENCODING_ENV_VAR_NAME)
    if log_encoding is not None and log_encoding.upper() == "JSON":
        return DEFAULT_JSON_FORMATTER()

    return logging.Formatter(DEFAULT_TEXT_FORMATTER)


def _create_dataset_log_handler(dataset_id: str) -> SessionFileHandler:
    """Create a log handler for a dataset with the given ID.

    Args:
        dataset_id: The ID of the dataset.

    Returns:
        A log handler for the dataset.
    """
    handler = SessionFileHandler(filename=f"ray-data-{dataset_id}.log")
    handler.setFormatter(_get_default_formatter())

    return handler


def update_dataset_logger_for_worker(dataset_id: Optional[str]) -> None:
    """Create a log handler for a dataset with the given ID. Switch the dataset logger
    for the worker to this dataset logger. Note that only the driver keeps track of the
    active dataset. The worker will just use the handler that the driver tells it to use.

    Args:
        dataset_id: The ID of the dataset.
    """
    if not dataset_id:
        return
    configure_logging()
    log_handler = _create_dataset_log_handler(dataset_id)
    loggers = [logging.getLogger(name) for name in _get_logger_names()]
    for logger in loggers:
        logger.addHandler(log_handler)


def register_dataset_logger(dataset_id: str) -> Optional[int]:
    """Create a log handler for a dataset with the given ID. Activate the handler if
    this is the only active dataset. Otherwise, print a warning to that handler and
    keep it inactive until it becomes the only active dataset.

    Args:
        dataset_id: The ID of the dataset.
    """
    global _DATASET_LOGGER_HANDLER
    global _ACTIVE_DATASET
    loggers = [logging.getLogger(name) for name in _get_logger_names()]
    log_handler = _create_dataset_log_handler(dataset_id)

    # The per-dataset log will always have the full context about its registration,
    # regardless of whether it is active or inactive.
    local_logger = logging.getLogger(__name__)
    local_logger.addHandler(log_handler)
    local_logger.info("Registered dataset logger for dataset %s", dataset_id)

    _DATASET_LOGGER_HANDLER[dataset_id] = log_handler
    if not _ACTIVE_DATASET:
        _ACTIVE_DATASET = dataset_id
        for logger in loggers:
            logger.addHandler(log_handler)
    else:
        local_logger.info(
            f"{dataset_id} registers for logging while another dataset "
            f"{_ACTIVE_DATASET} is also logging. For performance reasons, we will not "
            f"log to the dataset {dataset_id} until it is the only active dataset."
        )
    local_logger.removeHandler(log_handler)

    return _ACTIVE_DATASET


def unregister_dataset_logger(dataset_id: str) -> Optional[int]:
    """Remove the logger for a dataset with the given ID.

    Args:
        dataset_id: The ID of the dataset.
    """
    global _DATASET_LOGGER_HANDLER
    global _ACTIVE_DATASET
    loggers = [logging.getLogger(name) for name in _get_logger_names()]

    log_handler = _DATASET_LOGGER_HANDLER.pop(dataset_id, None)

    if _ACTIVE_DATASET == dataset_id:
        _ACTIVE_DATASET = None
        if _DATASET_LOGGER_HANDLER:
            # If there are still active dataset loggers, activate the first one.
            register_dataset_logger(next(iter(_DATASET_LOGGER_HANDLER.keys())))

    if log_handler:
        for logger in loggers:
            logger.removeHandler(log_handler)
        log_handler.close()

    return _ACTIVE_DATASET
