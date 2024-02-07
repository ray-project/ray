import logging
import os

import ray
from ray._private.ray_constants import LOGGER_FORMAT, LOGGER_LEVEL
from ray.data.context import DataContext


class UserCodeException(Exception):
    """Represents an Exception originating from user code, e.g.
    user-specified UDF used in a Ray Data transformation."""

    pass


class RayDataInternalException(Exception):
    """Represents an Exception originating from Ray Data internal code
    or Ray Core private code paths, as opposed to user code. When
    Exceptions of this form are raised, it likely indicate a bug
    in Ray Data or Ray Core.

    By default, the stack trace for these exceptions is omitted from
    stdout, but will still be emitted to the Ray Data specific log file.
    To emit all stack frames to stdout, set `DataContext.internal_stack_trace_stdout`
    to True.
    """

    pass


def skip_internal_stack_frames(ex: Exception) -> Exception:
    """
    For the given Exception, skip stack frames which belong to
    Ray Data internal or Ray Core private code paths. By default,
    these skipped frames be omitted from stdout output, but will
    still be emitted to the Ray Data specific log file, under
    `logs/ray-data.log`. To emit all stack frames to stdout, set
    `DataContext.internal_stack_trace_stdout` to True.
    """
    RAY_DATA_INTERNAL_STACKTRACE_PREFIX = "ray/data/_internal"
    RAY_CORE_PRIVATE_STACKTRACE_PREFIX = "ray/_private"

    if ex is None:
        return ex
    orig_tb = ex.__traceback__

    tb = ex.__traceback__
    if tb is not None:
        # Always log the full traceback, including internal code paths,
        # to the Ray Data log file.
        try:
            raise ex
        except Exception:
            data_exception_logger.get_logger(log_to_stdout=False).exception(
                "Internal stack trace:"
            )

    last_traceback_was_skipped = False
    while tb is not None:
        call_path = tb.tb_frame.f_code.co_filename
        if (
            RAY_DATA_INTERNAL_STACKTRACE_PREFIX in call_path
            or RAY_CORE_PRIVATE_STACKTRACE_PREFIX in call_path
        ):
            # Current frame is from Ray Data internal or Ray Core private code,
            # so skip it in the output Exception.
            ex.__traceback__ = tb.tb_next
            last_traceback_was_skipped = True
        else:
            last_traceback_was_skipped = False
        tb = tb.tb_next
    if last_traceback_was_skipped:
        # If the bottom-most stack frame was skipped, and there was no previous
        # user code error encountered, this indicates that the
        # error originated from Ray Data internal or Ray Core private code.
        if not isinstance(ex, UserCodeException):
            data_exception_logger.get_logger().error(
                "Exception occured in Ray Data or Ray Core internal code. "
                "If you continue to see this error, please open an issue on "
                "the Ray project GitHub page with the full stack trace below: "
                "https://github.com/ray-project/ray/issues/new/choose"
            )
            return RayDataInternalException(*ex.args).with_traceback(orig_tb)
        return ex

    ex_with_full_tb = UserCodeException(*ex.args).with_traceback(orig_tb)
    if DataContext.get_current().internal_stack_trace_stdout:
        data_exception_logger.get_logger().error(
            "Exception occured in user code. Full stack trace "
            "including internal Ray Data code:"
        )
        return ex_with_full_tb

    # By default, raise the stripped Exception, and log the full stack trace
    # to the Ray Data log file.
    data_exception_logger.get_logger().error(
        "Exception occured in user code. By default, the Ray Data internal stack "
        "trace is omitted from stdout, and only written to the Ray Data log file at "
        f"{data_exception_logger._log_path}. To output the full stack trace to stdout, "
        "set `DataContext.internal_stack_trace_stdout = True`."
    )
    return UserCodeException(*ex.args).with_traceback(ex.__traceback__)


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
