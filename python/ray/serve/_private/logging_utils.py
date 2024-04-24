import builtins
import copy
import json
import logging
import os
import sys
import traceback
from typing import Any, Optional, Tuple

import ray
from ray.serve._private.common import ServeComponentType
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_CPU_PROFILING,
    RAY_SERVE_ENABLE_JSON_LOGGING,
    RAY_SERVE_ENABLE_MEMORY_PROFILING,
    RAY_SERVE_LOG_TO_STDERR,
    SERVE_LOG_ACTOR_ID,
    SERVE_LOG_APPLICATION,
    SERVE_LOG_COMPONENT,
    SERVE_LOG_COMPONENT_ID,
    SERVE_LOG_DEPLOYMENT,
    SERVE_LOG_EXTRA_FIELDS,
    SERVE_LOG_LEVEL_NAME,
    SERVE_LOG_MESSAGE,
    SERVE_LOG_RECORD_FORMAT,
    SERVE_LOG_REPLICA,
    SERVE_LOG_REQUEST_ID,
    SERVE_LOG_ROUTE,
    SERVE_LOG_TIME,
    SERVE_LOG_WORKER_ID,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.utils import get_component_file_name
from ray.serve.schema import EncodingType, LoggingConfig

try:
    import cProfile
except ImportError:
    pass


buildin_print = builtins.print


class ServeJSONFormatter(logging.Formatter):
    """Serve Logging Json Formatter

    The formatter will generate the json log format on the fly
    based on the field of record.
    """

    ADD_IF_EXIST_FIELDS = [
        SERVE_LOG_REQUEST_ID,
        SERVE_LOG_ROUTE,
        SERVE_LOG_APPLICATION,
    ]

    def __init__(
        self,
        component_name: str,
        component_id: str,
        component_type: Optional[ServeComponentType] = None,
    ):
        self.component_log_fmt = {
            SERVE_LOG_LEVEL_NAME: SERVE_LOG_RECORD_FORMAT[SERVE_LOG_LEVEL_NAME],
            SERVE_LOG_TIME: SERVE_LOG_RECORD_FORMAT[SERVE_LOG_TIME],
        }
        try:
            runtime_context = ray.get_runtime_context()
            actor_id = runtime_context.get_actor_id()
            if actor_id:
                self.component_log_fmt[SERVE_LOG_ACTOR_ID] = actor_id
            worker_id = runtime_context.get_worker_id()
            if worker_id:
                self.component_log_fmt[SERVE_LOG_WORKER_ID] = worker_id
        except Exception:
            # If get_runtime_context() fails for any reason, do nothing (no adding
            # actor_id and/or worker_id to the fmt)
            pass

        if component_type and component_type == ServeComponentType.REPLICA:
            self.component_log_fmt[SERVE_LOG_DEPLOYMENT] = component_name
            self.component_log_fmt[SERVE_LOG_REPLICA] = component_id
            self.component_log_fmt[SERVE_LOG_COMPONENT] = component_type
        else:
            self.component_log_fmt[SERVE_LOG_COMPONENT] = component_name
            self.component_log_fmt[SERVE_LOG_COMPONENT_ID] = component_id
        self.message_formatter = logging.Formatter(
            SERVE_LOG_RECORD_FORMAT[SERVE_LOG_MESSAGE]
        )
        self.asctime_formatter = logging.Formatter("%(asctime)s")

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record into json format.

        Args:
            record: The log record to be formatted.

            Returns:
                The formatted log record in json format.
        """
        record_format = copy.deepcopy(self.component_log_fmt)
        record_attributes = copy.deepcopy(record.__dict__)
        record_format[SERVE_LOG_LEVEL_NAME] = record.levelname
        record_format[SERVE_LOG_TIME] = self.asctime_formatter.format(record)

        for field in ServeJSONFormatter.ADD_IF_EXIST_FIELDS:
            if field in record_attributes:
                record_format[field] = record_attributes[field]

        record_format[SERVE_LOG_MESSAGE] = self.message_formatter.format(record)

        if SERVE_LOG_EXTRA_FIELDS in record_attributes:
            if not isinstance(record_attributes[SERVE_LOG_EXTRA_FIELDS], dict):
                raise ValueError(
                    f"Expected a dictionary passing into {SERVE_LOG_EXTRA_FIELDS}, "
                    f"but got {type(record_attributes[SERVE_LOG_EXTRA_FIELDS])}"
                )
            for k, v in record_attributes[SERVE_LOG_EXTRA_FIELDS].items():
                if k in record_format:
                    raise KeyError(f"Found duplicated key in the log record: {k}")
                record_format[k] = v

        return json.dumps(record_format)


class ServeFormatter(logging.Formatter):
    """Serve Logging Formatter

    The formatter will generate the log format on the fly based on the field of record.
    """

    COMPONENT_LOG_FMT = f"%({SERVE_LOG_LEVEL_NAME})s %({SERVE_LOG_TIME})s {{{SERVE_LOG_COMPONENT}}} {{{SERVE_LOG_COMPONENT_ID}}} "  # noqa:E501

    def __init__(
        self,
        component_name: str,
        component_id: str,
    ):
        self.component_log_fmt = ServeFormatter.COMPONENT_LOG_FMT.format(
            component_name=component_name, component_id=component_id
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record into the format string.

        Args:
            record: The log record to be formatted.

            Returns:
                The formatted log record in string format.
        """
        record_format = self.component_log_fmt
        record_formats_attrs = []
        if SERVE_LOG_REQUEST_ID in record.__dict__:
            record_formats_attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_REQUEST_ID])
        if SERVE_LOG_ROUTE in record.__dict__:
            record_formats_attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_ROUTE])
        record_formats_attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_MESSAGE])
        record_format += " ".join(record_formats_attrs)

        # create a formatter using the format string
        formatter = logging.Formatter(record_format)

        # format the log record using the formatter
        return formatter.format(record)


def access_log_msg(*, method: str, status: str, latency_ms: float):
    """Returns a formatted message for an HTTP or ServeHandle access log."""
    return f"{method.upper()} {status.upper()} {latency_ms:.1f}ms"


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
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            absolute_path = handler.baseFilename
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

    factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        request_context = ray.serve.context._serve_request_context.get()
        record = factory(*args, **kwargs)
        if request_context.route:
            setattr(record, SERVE_LOG_ROUTE, request_context.route)
        if request_context.request_id:
            setattr(record, SERVE_LOG_REQUEST_ID, request_context.request_id)
        if request_context.app_name:
            setattr(record, SERVE_LOG_APPLICATION, request_context.app_name)
        return record

    logging.setLogRecordFactory(record_factory)

    # Only add stream handler if RAY_SERVE_LOG_TO_STDERR is True.
    if RAY_SERVE_LOG_TO_STDERR:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(ServeFormatter(component_name, component_id))
        stream_handler.addFilter(log_to_stderr_filter)
        logger.addHandler(stream_handler)

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
        file_handler.setFormatter(
            ServeJSONFormatter(component_name, component_id, component_type)
        )
    else:
        file_handler.setFormatter(ServeFormatter(component_name, component_id))

    if logging_config.enable_access_log is False:
        file_handler.addFilter(log_access_log_filter)

    # Redirect print, stdout, and stderr to Serve logger.
    if not RAY_SERVE_LOG_TO_STDERR:
        builtins.print = redirected_print
        sys.stdout = StreamToLogger(logger, logging.INFO, sys.stdout)
        sys.stderr = StreamToLogger(logger, logging.INFO, sys.stderr)

    logger.addHandler(file_handler)


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


def configure_component_cpu_profiler(
    component_name: str,
    component_id: str,
    component_type: Optional[ServeComponentType] = None,
) -> Tuple[Optional[cProfile.Profile], Optional[str]]:
    """Configures the CPU profiler for this component.

    Does nothing if RAY_SERVE_ENABLE_CPU_PROFILING is disabled.

    Returns:
        2-tuple containing profiler object and log file name for profile stats.
    """

    if RAY_SERVE_ENABLE_CPU_PROFILING:
        logger = logging.getLogger(SERVE_LOGGER_NAME)

        try:
            import cProfile
        except ImportError:
            logger.warning(
                "RAY_SERVE_ENABLE_CPU_PROFILING is enabled, but cProfile "
                "is not installed. No CPU profiling is happening."
            )
            return None, None
        try:
            # Need marshal to dump data. Check if marshal is installed before
            # starting the profiler.
            import marshal  # noqa: F401
        except ImportError:
            logger.warning(
                "RAY_SERVE_ENABLE_CPU_PROFILING is enabled, but marshal "
                "is not installed. No CPU profiling is happening."
            )
            return None, None

        logs_dir = get_serve_logs_dir()
        cpu_profiler_file_name = get_component_file_name(
            component_name=component_name,
            component_id=component_id,
            component_type=component_type,
            suffix="_cprofile.prof",
        )
        cpu_profiler_file_path = os.path.join(logs_dir, cpu_profiler_file_name)

        profile = cProfile.Profile()
        profile.enable()
        logger.info(
            "RAY_SERVE_ENABLE_CPU_PROFILING is enabled. Started cProfile "
            "on this actor."
        )
        return profile, cpu_profiler_file_path
    else:
        return None, None


def get_serve_logs_dir() -> str:
    """Get the directory that stores Serve log files."""

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
