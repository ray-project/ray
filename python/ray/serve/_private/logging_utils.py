import builtins
import logging
import os
import sys
import traceback
from typing import Any, Optional

import ray
from ray._common.ray_constants import LOGGING_ROTATE_BACKUP_COUNT, LOGGING_ROTATE_BYTES
from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter, TextFormatter
from ray.serve._private.common import ServeComponentType
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_JSON_LOGGING,
    RAY_SERVE_ENABLE_MEMORY_PROFILING,
    RAY_SERVE_LOG_TO_STDERR,
    SERVE_LOG_APPLICATION,
    SERVE_LOG_COMPONENT,
    SERVE_LOG_COMPONENT_ID,
    SERVE_LOG_DEPLOYMENT,
    SERVE_LOG_LEVEL_NAME,
    SERVE_LOG_MESSAGE,
    SERVE_LOG_RECORD_FORMAT,
    SERVE_LOG_REPLICA,
    SERVE_LOG_REQUEST_ID,
    SERVE_LOG_ROUTE,
    SERVE_LOG_TIME,
    SERVE_LOG_UNWANTED_ATTRS,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.utils import get_component_file_name
from ray.serve.schema import EncodingType, LoggingConfig

buildin_print = builtins.print


def should_skip_context_filter(record: logging.LogRecord) -> bool:
    """Check if the log record should skip the context filter."""
    return getattr(record, "skip_context_filter", False)


class ServeCoreContextFilter(CoreContextFilter):
    def filter(self, record: logging.LogRecord) -> bool:
        if should_skip_context_filter(record):
            return True
        return super().filter(record)


class ServeComponentFilter(logging.Filter):
    """Serve component filter.

    The filter will add the component name, id, and type to the log record.
    """

    def __init__(
        self,
        component_name: str,
        component_id: str,
        component_type: Optional[ServeComponentType] = None,
    ):
        self.component_name = component_name
        self.component_id = component_id
        self.component_type = component_type

    def filter(self, record: logging.LogRecord) -> bool:
        """Add component attributes to the log record.

        Note: the filter doesn't do any filtering, it only adds the component
        attributes.
        """
        if should_skip_context_filter(record):
            return True
        if self.component_type and self.component_type == ServeComponentType.REPLICA:
            setattr(record, SERVE_LOG_DEPLOYMENT, self.component_name)
            setattr(record, SERVE_LOG_REPLICA, self.component_id)
            setattr(record, SERVE_LOG_COMPONENT, self.component_type)
        else:
            setattr(record, SERVE_LOG_COMPONENT, self.component_name)
            setattr(record, SERVE_LOG_COMPONENT_ID, self.component_id)

        return True


class ServeContextFilter(logging.Filter):
    """Serve context filter.

    The filter will add the route, request id, app name to the log record.

    Note: the filter doesn't do any filtering, it only adds the serve request context
    attributes.
    """

    def filter(self, record):
        if should_skip_context_filter(record):
            return True
        request_context = ray.serve.context._get_serve_request_context()
        if request_context.route:
            setattr(record, SERVE_LOG_ROUTE, request_context.route)
        if request_context.request_id:
            setattr(record, SERVE_LOG_REQUEST_ID, request_context.request_id)
        if request_context.app_name:
            setattr(record, SERVE_LOG_APPLICATION, request_context.app_name)
        return True


class ServeLogAttributeRemovalFilter(logging.Filter):
    """Serve log attribute removal filter.

    The filter will remove unwanted attributes on the log record so they won't be
    included in the structured logs.

    Note: the filter doesn't do any filtering, it only removes unwanted attributes.
    """

    def filter(self, record):
        for key in SERVE_LOG_UNWANTED_ATTRS:
            if hasattr(record, key):
                delattr(record, key)

        return True


class ServeFormatter(TextFormatter):
    """Serve Logging Formatter

    The formatter will generate the log format on the fly based on the field of record.
    Optimized to pre-compute format strings and formatters for better performance.
    """

    COMPONENT_LOG_FMT = f"%({SERVE_LOG_LEVEL_NAME})s %({SERVE_LOG_TIME})s {{{SERVE_LOG_COMPONENT}}} {{{SERVE_LOG_COMPONENT_ID}}} "  # noqa:E501

    def __init__(
        self,
        component_name: str,
        component_id: str,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        style: str = "%",
        validate: bool = True,
    ):
        super().__init__(fmt, datefmt, style, validate)
        self.component_log_fmt = ServeFormatter.COMPONENT_LOG_FMT.format(
            component_name=component_name, component_id=component_id
        )

        # Pre-compute format strings and formatters for performance
        self._precompute_formatters()

    def set_additional_log_standard_attrs(self, *args, **kwargs):
        super().set_additional_log_standard_attrs(*args, **kwargs)
        self._precompute_formatters()

    def _precompute_formatters(self):
        self.base_formatter = self._create_formatter([])
        self.request_formatter = self._create_formatter(
            [SERVE_LOG_RECORD_FORMAT[SERVE_LOG_REQUEST_ID]]
        )

    def _create_formatter(self, initial_attrs: list) -> logging.Formatter:
        attrs = initial_attrs.copy()
        attrs.extend([f"%({k})s" for k in self.additional_log_standard_attrs])
        attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_MESSAGE])

        format_string = self.component_log_fmt + " ".join(attrs)
        return logging.Formatter(format_string)

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record into the format string.

        Args:
            record: The log record to be formatted.
            Returns:
                The formatted log record in string format.
        """
        # Use pre-computed formatters for better performance
        if SERVE_LOG_REQUEST_ID in record.__dict__:
            return self.request_formatter.format(record)
        else:
            return self.base_formatter.format(record)


def access_log_msg(*, method: str, route: str, status: str, latency_ms: float):
    """Returns a formatted message for an HTTP or ServeHandle access log."""
    return f"{method} {route} {status} {latency_ms:.1f}ms"


def log_to_stderr_filter(record: logging.LogRecord) -> bool:
    """Filters log records based on a parameter in the `extra` dictionary."""
    if not hasattr(record, "log_to_stderr") or record.log_to_stderr is None:
        return True

    return record.log_to_stderr


def log_access_log_filter(record: logging.LogRecord) -> bool:
    """Filters ray serve access log based on 'serve_access_log' key in `extra` dict."""
    if not hasattr(record, "serve_access_log") or record.serve_access_log is None:
        return True

    return not record.serve_access_log


def get_component_logger_file_path() -> Optional[str]:
    """Returns the relative file path for the Serve logger, if it exists.

    If a logger was configured through configure_component_logger() for the Serve
    component that's calling this function, this returns the location of the log file
    relative to the ray logs directory.
    """
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    for handler in logger.handlers:
        if isinstance(handler, logging.handlers.MemoryHandler):
            absolute_path = handler.target.baseFilename
            ray_logs_dir = ray._private.worker._global_node.get_logs_dir_path()
            if absolute_path.startswith(ray_logs_dir):
                return absolute_path[len(ray_logs_dir) :]


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.

    This comes from https://stackoverflow.com/a/36296215 directly.
    """

    def __init__(self, logger: logging.Logger, log_level: int, original_object: Any):
        self._logger = logger
        self._log_level = log_level
        self._original_object = original_object
        self._linebuf = ""

    def __getattr__(self, attr: str) -> Any:
        # getting attributes from the original object
        return getattr(self._original_object, attr)

    @staticmethod
    def get_stacklevel() -> int:
        """Rewind stack to get the stacklevel for the user code.

        Going from the back of the traceback and traverse until it's no longer in
        logging_utils.py or site-packages.
        """
        reverse_traces = traceback.extract_stack()[::-1]
        for index, trace in enumerate(reverse_traces):
            if (
                "logging_utils.py" not in trace.filename
                and "site-packages" not in trace.filename
            ):
                return index
        return 1

    def write(self, buf: str):
        temp_linebuf = self._linebuf + buf
        self._linebuf = ""
        for line in temp_linebuf.splitlines(True):
            # From the io.TextIOWrapper docs:
            #   On output, if newline is None, any '\n' characters written
            #   are translated to the system default line separator.
            # By default sys.stdout.write() expects '\n' newlines and then
            # translates them so this is still cross-platform.
            if line[-1] == "\n":
                self._logger.log(
                    self._log_level,
                    line.rstrip(),
                    stacklevel=self.get_stacklevel(),
                )
            else:
                self._linebuf += line

    def flush(self):
        if self._linebuf != "":
            self._logger.log(
                self._log_level,
                self._linebuf.rstrip(),
                stacklevel=self.get_stacklevel(),
            )
        self._linebuf = ""

    def isatty(self) -> bool:
        return True


def redirected_print(*objects, sep=" ", end="\n", file=None, flush=False):
    """Implement python's print function to redirect logs to Serve's logger.

    If the file is set to anything other than stdout, stderr, or None, call the
    builtin print. Else, construct the message and redirect to Serve's logger.

    See https://docs.python.org/3/library/functions.html#print
    """
    if file not in [sys.stdout, sys.stderr, None]:
        return buildin_print(objects, sep=sep, end=end, file=file, flush=flush)

    serve_logger = logging.getLogger(SERVE_LOGGER_NAME)
    message = sep.join(map(str, objects)) + end
    # We monkey patched print function, so this is always at stack level 2.
    serve_logger.log(logging.INFO, message, stacklevel=2)


def configure_component_logger(
    *,
    component_name: str,
    component_id: str,
    logging_config: LoggingConfig,
    component_type: Optional[ServeComponentType] = None,
    max_bytes: Optional[int] = None,
    backup_count: Optional[int] = None,
    stream_handler_only: bool = False,
    buffer_size: int = 1,
):
    """Configure a logger to be used by a Serve component.

    The logger will log using a standard format to make components identifiable
    using the provided name and unique ID for this instance (e.g., replica ID).

    This logger will *not* propagate its log messages to the parent logger(s).
    """
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    logger.propagate = False
    logger.setLevel(logging_config.log_level)
    logger.handlers.clear()

    serve_formatter = ServeFormatter(component_name, component_id)
    json_formatter = JSONFormatter()
    if logging_config.additional_log_standard_attrs:
        json_formatter.set_additional_log_standard_attrs(
            logging_config.additional_log_standard_attrs
        )
        serve_formatter.set_additional_log_standard_attrs(
            logging_config.additional_log_standard_attrs
        )

    # Only add stream handler if RAY_SERVE_LOG_TO_STDERR is True or if
    # `stream_handler_only` is set to True.
    if RAY_SERVE_LOG_TO_STDERR or stream_handler_only:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(serve_formatter)
        stream_handler.addFilter(log_to_stderr_filter)
        stream_handler.addFilter(ServeContextFilter())
        logger.addHandler(stream_handler)

    # Skip setting up file handler and stdout/stderr redirect if `stream_handler_only`
    # is set to True. Logger such as default serve logger can be configured outside the
    # context of a Serve component, we don't want those logs to redirect into serve's
    # logger and log files.
    if stream_handler_only:
        return

    if logging_config.logs_dir:
        logs_dir = logging_config.logs_dir
    else:
        logs_dir = get_serve_logs_dir()
    os.makedirs(logs_dir, exist_ok=True)

    if max_bytes is None:
        max_bytes = ray._private.worker._global_node.max_bytes
    if backup_count is None:
        backup_count = ray._private.worker._global_node.backup_count

    log_file_name = get_component_file_name(
        component_name=component_name,
        component_id=component_id,
        component_type=component_type,
        suffix=".log",
    )

    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(logs_dir, log_file_name),
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    if RAY_SERVE_ENABLE_JSON_LOGGING:
        logger.warning(
            "'RAY_SERVE_ENABLE_JSON_LOGGING' is deprecated, please use "
            "'LoggingConfig' to enable json format."
        )
    if RAY_SERVE_ENABLE_JSON_LOGGING or logging_config.encoding == EncodingType.JSON:
        file_handler.addFilter(ServeCoreContextFilter())
        file_handler.addFilter(ServeContextFilter())
        file_handler.addFilter(
            ServeComponentFilter(component_name, component_id, component_type)
        )
        file_handler.setFormatter(json_formatter)
    else:
        file_handler.setFormatter(serve_formatter)

    if logging_config.enable_access_log is False:
        file_handler.addFilter(log_access_log_filter)
    else:
        file_handler.addFilter(ServeContextFilter())

    # Remove unwanted attributes from the log record.
    file_handler.addFilter(ServeLogAttributeRemovalFilter())

    # Redirect print, stdout, and stderr to Serve logger, only when it's on the replica.
    if not RAY_SERVE_LOG_TO_STDERR and component_type == ServeComponentType.REPLICA:
        builtins.print = redirected_print
        sys.stdout = StreamToLogger(logger, logging.INFO, sys.stdout)
        sys.stderr = StreamToLogger(logger, logging.INFO, sys.stderr)

    # Create a memory handler that buffers log records and flushes to file handler
    # Buffer capacity: buffer_size records
    # Flush triggers: buffer full, ERROR messages, or explicit flush
    memory_handler = logging.handlers.MemoryHandler(
        capacity=buffer_size,
        target=file_handler,
        flushLevel=logging.ERROR,  # Auto-flush on ERROR/CRITICAL
    )

    # Add the memory handler instead of the file handler directly
    logger.addHandler(memory_handler)


def configure_default_serve_logger():
    """Helper function to configure the default Serve logger that's used outside of
    individual Serve components."""
    configure_component_logger(
        component_name="serve",
        component_id=str(os.getpid()),
        logging_config=LoggingConfig(),
        max_bytes=LOGGING_ROTATE_BYTES,
        backup_count=LOGGING_ROTATE_BACKUP_COUNT,
        stream_handler_only=True,
    )


def configure_component_memory_profiler(
    component_name: str,
    component_id: str,
    component_type: Optional[ServeComponentType] = None,
):
    """Configures the memory logger for this component.

    Does nothing if RAY_SERVE_ENABLE_MEMORY_PROFILING is disabled.
    """

    if RAY_SERVE_ENABLE_MEMORY_PROFILING:
        logger = logging.getLogger(SERVE_LOGGER_NAME)

        try:
            import memray

            logs_dir = get_serve_logs_dir()
            memray_file_name = get_component_file_name(
                component_name=component_name,
                component_id=component_id,
                component_type=component_type,
                suffix="_memray_0.bin",
            )
            memray_file_path = os.path.join(logs_dir, memray_file_name)

            # If the actor restarted, memray requires a new file to start
            # tracking memory.
            restart_counter = 1
            while os.path.exists(memray_file_path):
                memray_file_name = get_component_file_name(
                    component_name=component_name,
                    component_id=component_id,
                    component_type=component_type,
                    suffix=f"_memray_{restart_counter}.bin",
                )
                memray_file_path = os.path.join(logs_dir, memray_file_name)
                restart_counter += 1

            # Memray usually tracks the memory usage of only a block of code
            # within a context manager. We explicitly call __enter__ here
            # instead of using a context manager to track memory usage across
            # all of the caller's code instead.
            memray.Tracker(memray_file_path, native_traces=True).__enter__()

            logger.info(
                "RAY_SERVE_ENABLE_MEMORY_PROFILING is enabled. Started a "
                "memray tracker on this actor. Tracker file located at "
                f'"{memray_file_path}"'
            )

        except ImportError:
            logger.warning(
                "RAY_SERVE_ENABLE_MEMORY_PROFILING is enabled, but memray "
                "is not installed. No memory profiling is happening. "
                "`pip install memray` to enable memory profiling."
            )


def get_serve_logs_dir() -> str:
    """Get the directory that stores Serve log files.

    If `ray._private.worker._global_node` is None (running outside the context of Ray),
    then the current working directory with subdirectory of serve is used as the logs
    directory. Otherwise, the logs directory is determined by the global node's logs
    directory path.
    """
    if ray._private.worker._global_node is None:
        return os.path.join(os.getcwd(), "serve")

    return os.path.join(ray._private.worker._global_node.get_logs_dir_path(), "serve")


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
