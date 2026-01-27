import logging
import logging.config
import os
import threading
from typing import List, Optional

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
        "console": {
            "()": "ray.data._internal.logging.ConsoleFormatter",
            "format": DEFAULT_TEXT_FORMATTER,
        },
    },
    "filters": {
        "console_filter": {"()": "ray.data._internal.logging.HiddenRecordFilter"},
        "dataset_context_filter": {
            "()": "ray.data._internal.logging.DatasetContextFilter"
        },
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
            "formatter": "console",
            "level": "INFO",
            "filters": ["console_filter", "dataset_context_filter"],
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
_ACTIVE_DATASETS = set()

# Thread-local storage for dataset IDs
_thread_local_dataset_id = threading.local()

# To facilitate debugging, Ray Data writes debug logs to a file. However, if Ray Data
# logs every scheduler loop, logging might impact performance. So, we add a "TRACE"
# level where logs aren't written by default.
#
# Use the following code to log a message at the "TRACE" level:
# ```
# logger.log(logging.getLevelName("TRACE"), "Your message here.")
# ````
logging.addLevelName(logging.DEBUG - 1, "TRACE")


def set_thread_dataset_id(dataset_id: str) -> None:
    """Set dataset ID for the current thread.

    This allows the logging system to identify which dataset is logging
    in multi-threaded environments where multiple datasets may run concurrently.

    Args:
        dataset_id: The ID of the dataset.
    """
    _thread_local_dataset_id.value = dataset_id


def get_thread_dataset_id() -> Optional[str]:
    """Get dataset ID for the current thread.

    Returns:
        The dataset ID for the current thread, or None if not set.
    """
    return getattr(_thread_local_dataset_id, "value", None)


def clear_thread_dataset_id() -> None:
    """Clear the dataset ID for the current thread.

    This should be called when a dataset finishes execution to clean up
    thread-local state.
    """
    if hasattr(_thread_local_dataset_id, "value"):
        delattr(_thread_local_dataset_id, "value")


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


class DatasetContextFilter:
    """Mark log records with dataset context for conditional formatting.

    When multiple datasets are running concurrently, this filter adds metadata
    to the log record that can be used by formatters to add dataset ID prefixes
    to console output only, without affecting file logs.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Add dataset ID metadata if multiple datasets are active.

        This filter adds a 'dataset_prefix' attribute to the log record that
        can be used by formatters. It does not modify record.msg directly to
        avoid affecting other handlers that share the same LogRecord object.

        Args:
            record: The log record to filter.

        Returns:
            Always returns True to allow the record to be logged.
        """
        try:
            # Get dataset ID from thread-local storage instead of global singleton
            dataset_id = get_thread_dataset_id()

            # Only add prefix metadata when multiple datasets are running concurrently
            global _ACTIVE_DATASETS
            if len(_ACTIVE_DATASETS) > 1 and dataset_id:
                short_id = dataset_id[:8] if len(dataset_id) > 8 else dataset_id
                record.dataset_prefix = f"[{short_id}] "
            else:
                record.dataset_prefix = ""
        except Exception:
            record.dataset_prefix = ""
        return True


class ConsoleFormatter(logging.Formatter):
    """Formatter for console output that includes dataset prefix.

    This formatter adds the dataset_prefix attribute (set by DatasetContextFilter)
    to console messages without affecting the original log record, ensuring file
    logs remain unmodified.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with dataset prefix for console output.

        Args:
            record: The log record to format.

        Returns:
            Formatted log string with dataset prefix if present.
        """
        # Get the prefix added by DatasetContextFilter
        prefix = getattr(record, "dataset_prefix", "")

        if prefix:
            # Save original message
            original_msg = record.msg
            original_args = record.args

            # Temporarily modify for formatting
            # getMessage() combines msg and args
            msg_with_prefix = f"{prefix}{record.getMessage()}"
            record.msg = msg_with_prefix
            record.args = None

            # Format with the modified message
            result = super().format(record)

            # Restore original values to avoid affecting other handlers
            record.msg = original_msg
            record.args = original_args

            return result
        else:
            # No prefix, use standard formatting
            return super().format(record)


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
        config = DEFAULT_CONFIG
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


def configure_logging() -> None:
    """Configure the Python logger named 'ray.data'.

    This function loads the configuration YAML specified by "RAY_DATA_LOGGING_CONFIG"
    environment variable. If the variable isn't set, this function loads the default
    "logging.yaml" file that is adjacent to this module.

    If "RAY_DATA_LOG_ENCODING" is specified as "JSON" we will enable JSON logging mode
    if using the default logging config.
    """
    config = _get_logging_config()

    # Create formatters, filters, and handlers from config
    formatters = _create_formatters(config)
    filters = _create_filters(config)
    handlers = _create_handlers(config, formatters, filters)

    # Configure each logger defined in the config
    _configure_loggers(config, handlers)

    # Warn if both env vars are set (incompatible)
    _warn_if_incompatible_env_vars()


def _import_class(class_path: str):
    """Dynamically import a class from a fully qualified path."""
    import importlib

    if "." not in class_path:
        raise ValueError(f"Invalid class path: {class_path}")

    module_name, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _create_formatters(config: dict) -> dict:
    """Create formatter instances from config."""
    formatters = {}

    for name, fmt_config in config.get("formatters", {}).items():
        # Handle "()" factory syntax (same as filters and handlers)
        if "()" in fmt_config:
            formatter_class = _import_class(fmt_config["()"])
            # Pass format string if provided
            format_str = fmt_config.get("format")
            formatters[name] = (
                formatter_class(format_str) if format_str else formatter_class()
            )
        elif "class" in fmt_config:
            formatter_class = _import_class(fmt_config["class"])
            formatters[name] = formatter_class()
        elif "format" in fmt_config:
            formatters[name] = logging.Formatter(fmt_config["format"])

    return formatters


def _create_filters(config: dict) -> dict:
    """Create filter instances from config."""
    filters = {}

    for name, filter_config in config.get("filters", {}).items():
        # https://docs.python.org/3/library/logging.config.html#dictionary-schema-details
        if "()" in filter_config:
            filter_class = _import_class(filter_config["()"])
            filters[name] = filter_class()

    return filters


def _create_handlers(config: dict, formatters: dict, filters: dict) -> dict:
    """Create and configure handler instances from config."""
    handlers = {}

    # Keys that are not passed to handler constructor
    HANDLER_CONFIG_KEYS = {"class", "level", "formatter", "filters"}

    for name, handler_config in config.get("handlers", {}).items():
        # Instantiate handler with all keys except config-only keys
        handler_class = _import_class(handler_config["class"])
        handler_kwargs = {
            k: v for k, v in handler_config.items() if k not in HANDLER_CONFIG_KEYS
        }
        handler = handler_class(**handler_kwargs)
        handler.name = name

        # Configure handler
        if "level" in handler_config:
            handler.setLevel(handler_config["level"])

        if "formatter" in handler_config:
            formatter = formatters.get(handler_config["formatter"])
            if formatter:
                handler.setFormatter(formatter)

        for filter_name in handler_config.get("filters", []):
            filter_obj = filters.get(filter_name)
            if filter_obj:
                handler.addFilter(filter_obj)

        handlers[name] = handler

    return handlers


def _configure_loggers(config: dict, handlers: dict) -> None:
    """Configure logger instances from config."""
    for logger_name, logger_config in config.get("loggers", {}).items():
        logger = logging.getLogger(logger_name)
        logger.setLevel(logger_config.get("level", logging.NOTSET))

        # Clear existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Add configured handlers
        for handler_name in logger_config.get("handlers", []):
            handler = handlers.get(handler_name)
            if handler:
                logger.addHandler(handler)

        logger.propagate = logger_config.get("propagate", True)


def _warn_if_incompatible_env_vars() -> None:
    """Warn if both RAY_DATA_LOGGING_CONFIG and RAY_DATA_LOG_ENCODING are set."""
    config_path = os.environ.get(RAY_DATA_LOGGING_CONFIG_ENV_VAR_NAME)
    log_encoding = os.environ.get(RAY_DATA_LOG_ENCODING_ENV_VAR_NAME)

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
    global _ACTIVE_DATASETS
    logger = logging.getLogger("ray.data")
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)

    _DATASET_LOGGER_HANDLER = {}
    _ACTIVE_DATASETS = set()


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
        SessionFileHandler: A log handler for the dataset.
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


def register_dataset_logger(dataset_id: str) -> Optional[str]:
    """Create a log handler for a dataset with the given ID and activate it.

    This function allows multiple datasets to log concurrently, each to its own
    log file. All datasets are activated immediately upon registration.

    Args:
        dataset_id: The ID of the dataset.

    Returns:
        str: The dataset_id if successfully registered, None otherwise.
    """
    global _DATASET_LOGGER_HANDLER
    global _ACTIVE_DATASETS

    loggers = [logging.getLogger(name) for name in _get_logger_names()]
    log_handler = _create_dataset_log_handler(dataset_id)

    # Log registration info to the dataset's own log file
    local_logger = logging.getLogger(__name__)
    local_logger.addHandler(log_handler)
    local_logger.info("Registered dataset logger for dataset %s", dataset_id)
    local_logger.removeHandler(log_handler)

    # Save handler and mark as active
    _DATASET_LOGGER_HANDLER[dataset_id] = log_handler
    _ACTIVE_DATASETS.add(dataset_id)

    # Add this handler to all ray.data loggers
    for logger in loggers:
        logger.addHandler(log_handler)

    # If multiple datasets are running concurrently, log this information
    if len(_ACTIVE_DATASETS) > 1:
        local_logger.addHandler(log_handler)
        local_logger.info(
            f"Dataset {dataset_id} is logging concurrently with "
            f"{len(_ACTIVE_DATASETS) - 1} other dataset(s). "
            f"Logs for this dataset are in ray-data-{dataset_id}.log"
        )
        local_logger.removeHandler(log_handler)

    return dataset_id


def unregister_dataset_logger(dataset_id: str) -> None:
    """Remove the logger for a dataset with the given ID.

    Args:
        dataset_id: The ID of the dataset.
    """
    global _DATASET_LOGGER_HANDLER
    global _ACTIVE_DATASETS

    loggers = [logging.getLogger(name) for name in _get_logger_names()]
    log_handler = _DATASET_LOGGER_HANDLER.pop(dataset_id, None)

    # Remove from active set
    if dataset_id in _ACTIVE_DATASETS:
        _ACTIVE_DATASETS.discard(dataset_id)

    if log_handler:
        # Remove handler from all loggers
        for logger in loggers:
            logger.removeHandler(log_handler)
        log_handler.close()
