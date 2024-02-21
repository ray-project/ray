import logging
import os
from typing import Callable

import ray
from ray._private.ray_constants import LOGGER_FORMAT, LOGGER_LEVEL
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class UserCodeException(Exception):
    """Represents an Exception originating from user code, e.g.
    user-specified UDF used in a Ray Data transformation.

    By default, the stack trace for these exceptions is omitted from
    stdout, but will still be emitted to the Ray Data specific log file.
    To emit all stack frames to stdout, set
    `DataContext.log_internal_stack_trace_to_stdout` to True."""

    pass


@DeveloperAPI
class SystemException(Exception):
    """Represents an Exception originating from Ray Data internal code
    or Ray Core private code paths, as opposed to user code. When
    Exceptions of this form are raised, it likely indicates a bug
    in Ray Data or Ray Core."""


def omit_traceback_stdout(fn: Callable) -> Callable:
    """Decorator which runs the function, and if there is an exception raised,
    drops the stack trace before re-raising the exception. The original exception,
    including the full unmodified stack trace, is always written to the Ray Data
    log file at `data_exception_logger._log_path`.

    This is useful for stripping long stack traces of internal Ray Data code,
    which can otherwise obfuscate user code errors."""

    def handle_trace(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            # Only log the full internal stack trace to stdout when configured.
            # The full stack trace will always be emitted to the Ray Data log file.
            log_to_stdout = DataContext.get_current().log_internal_stack_trace_to_stdout
            # data_exception_logger.get_logger(log_to_stdout=log_to_stdout).exception(e)

            is_user_code_exception = isinstance(e, ray.exceptions.RayTaskError)
            if is_user_code_exception:
                # Exception has occurred in user code.
                if not log_to_stdout and ray.util.log_once(
                    "ray_data_exception_internal_hidden"
                ):
                    data_exception_logger.get_logger().error(
                        "Exception occurred in user code, with the abbreviated stack "
                        "trace below. By default, the Ray Data internal stack trace "
                        "is omitted from stdout, and only written to the Ray Data log "
                        f"file at {data_exception_logger._log_path}. To output the "
                        "full stack trace to stdout, set "
                        "`DataContext.log_internal_stack_trace_to_stdout` to True`."
                    )
            else:
                # Exception has occurred in internal Ray Data / Ray Core code.
                data_exception_logger.get_logger().error(
                    "Exception occurred in Ray Data or Ray Core internal code. "
                    "If you continue to see this error, please open an issue on "
                    "the Ray project GitHub page with the full stack trace below: "
                    "https://github.com/ray-project/ray/issues/new/choose"
                )

            data_exception_logger.get_logger(log_to_stdout=log_to_stdout).exception(
                "Full stack trace:"
            )

            if is_user_code_exception:
                raise e.with_traceback(None) from UserCodeException()
            else:
                raise e.with_traceback(None) from SystemException()

    return handle_trace


class DatasetLogger:
    """Logger for Ray Datasets which writes logs to a separate log file
    at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`. Can optionally turn off
    logging to stdout to reduce clutter (but always logs to the aformentioned
    Datasets-specific log file).

    After initialization, always use the `get_logger()` method to correctly
    set whether to log to stdout. Example usage:
    ```
    logger = DatasetLogger(__name__)
    logger.get_logger().info("This logs to file and stdout")
    logger.get_logger(log_to_stdout=False).info("This logs to file only)
    logger.get_logger().warning("Can call the usual Logger methods")
    ```
    """

    DEFAULT_DATASET_LOG_PATH = "logs/ray-data.log"

    def __init__(self, log_name: str):
        """Initialize DatasetLogger for a given `log_name`.

        Args:
            log_name: Name of logger (usually passed into `logging.getLogger(...)`)
        """
        # Logger used to logging to log file (in addition to the root logger,
        # which logs to stdout as normal). For logging calls made with the
        # parameter `log_to_stdout = False`, `_logger.propagate` will be set
        # to `False` in order to prevent the root logger from writing the log
        # to stdout.
        self.log_name = log_name
        # Lazily initialized in self._initialize_logger()
        self._logger = None
        self._log_path = None

    def _initialize_logger(self) -> logging.Logger:
        """Internal method to initialize the logger and the extra file handler
        for writing to the Dataset log file. Not intended (nor necessary)
        to call explicitly. Assumes that `ray.init()` has already been called prior
        to calling this method; otherwise raises a `ValueError`."""

        # We initialize a logger using the given base `log_name`, which
        # logs to stdout. Logging with this logger to stdout is enabled by the
        # `log_to_stdout` parameter in `self.get_logger()`.
        stdout_logger = logging.getLogger(self.log_name)
        stdout_logger.setLevel(LOGGER_LEVEL.upper())

        # The second logger that we initialize is designated as the main logger,
        # which has the above `stdout_logger` as an ancestor.
        # This is so that even if the file handler is not initialized below,
        # the logger will still propagate up to `stdout_logger` for the option
        # of logging to stdout.
        logger = logging.getLogger(f"{self.log_name}.logfile")
        # We need to set the log level again when explicitly
        # initializing a new logger (otherwise can have undesirable level).
        logger.setLevel(LOGGER_LEVEL.upper())

        # If ray.init() is called and the global node session directory path
        # is valid, we can create the additional handler to write to the
        # Dataset log file. If this is not the case (e.g. when used in Ray
        # Client), then we skip initializing the FileHandler.
        global_node = ray._private.worker._global_node
        if global_node is not None:
            # Add a FileHandler to write to the specific Ray Datasets log file
            # at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`, using the standard
            # default logger format used by the root logger
            session_dir = global_node.get_session_dir_path()
            datasets_log_path = os.path.join(
                session_dir,
                DatasetLogger.DEFAULT_DATASET_LOG_PATH,
            )
            self._log_path = datasets_log_path

            file_log_formatter = logging.Formatter(fmt=LOGGER_FORMAT)
            file_log_handler = logging.FileHandler(datasets_log_path)
            file_log_handler.setLevel(LOGGER_LEVEL.upper())
            file_log_handler.setFormatter(file_log_formatter)
            logger.addHandler(file_log_handler)
        return logger

    def get_logger(self, log_to_stdout: bool = True) -> logging.Logger:
        """
        Returns the underlying Logger, with the `propagate` attribute set
        to the same value as `log_to_stdout`. For example, when
        `log_to_stdout = False`, we do not want the `DatasetLogger` to
        propagate up to the base Logger which writes to stdout.

        This is a workaround needed due to the DatasetLogger wrapper object
        not having access to the log caller's scope in Python <3.8.
        In the future, with Python 3.8 support, we can use the `stacklevel` arg,
        which allows the logger to fetch the correct calling file/line and
        also removes the need for this getter method:
        `logger.info(msg="Hello world", stacklevel=2)`
        """
        if self._logger is None:
            self._logger = self._initialize_logger()
        self._logger.propagate = log_to_stdout
        return self._logger


# Logger used by Ray Data to log Exceptions while skip internal stack frames.
data_exception_logger = DatasetLogger(__name__)
