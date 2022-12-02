import logging
import os

import ray
from ray._private.ray_constants import LOGGER_FORMAT, LOGGER_LEVEL


class DatasetLogger:
    """Logger for Ray Datasets which, in addition to logging to stdout,
    also writes to a separate log file at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`.
    """

    DEFAULT_DATASET_LOG_PATH = "logs/ray-data.log"
    # Since this class is a wrapper around the base Logger, when we call
    # `DatasetLogger(...).info(...)`, the file/line number calling the logger will by
    # default link to functions in this file, `dataset_logger.py`. By setting the
    # `stacklevel` arg to 2, this allows the logger to fetch the actual caller of
    # `DatasetLogger(...).info(...)`.
    ROOT_LOGGER_STACK_LEVEL = 2

    def __init__(self, log_name: str):
        """Initialize DatasetLogger for a given `log_name`.

        Args:
            log_name: Name of logger (usually passed into `logging.getLogger(...)`)
        """
        # Logger used to logging to log file (in addition to the root logger,
        # which logs to stdout as normal). We set `logger.propagate` to False
        # to ensure the file logger only logs to the file, and not stdout, by default.
        # For logging calls made with the parameter `log_to_stdout = False`,
        # `logger.propagate` will be set to `False` in order to prevent the
        # root logger from writing the log to stdout.
        self.logger = logging.getLogger(f"{log_name}.logfile")
        # We need to set the log level again when explicitly
        # initializing a new logger (otherwise can have undesirable level).
        self.logger.setLevel(LOGGER_LEVEL.upper())

        # Add log handler which writes to a separate Datasets log file
        # at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`
        global_node = ray._private.worker._global_node
        if global_node is not None:
            # With current implementation, we can only get session_dir
            # after ray.init() is called. A less hacky way could potentially fix this
            session_dir = global_node.get_session_dir_path()
            self.datasets_log_path = os.path.join(
                session_dir,
                DatasetLogger.DEFAULT_DATASET_LOG_PATH,
            )
            # Add a FileHandler to write to the specific Ray Datasets log file,
            # using the standard default logger format used by the root logger
            file_log_handler = logging.FileHandler(self.datasets_log_path)
            file_log_formatter = logging.Formatter(fmt=LOGGER_FORMAT)
            file_log_handler.setFormatter(file_log_formatter)
            self.logger.addHandler(file_log_handler)

    def debug(self, msg: str, log_to_stdout: bool = True, *args, **kwargs):
        """Calls the standard `Logger.debug` method and emits the resulting
        row to the Datasets log file.

        Args:
            msg: Message to log; logs 'msg % args'
            log_to_stdout: If True, also emit logs to stdout in addition
            to writing to the log file.
        """
        self.logger.propagate = log_to_stdout
        self.logger.debug(
            msg, stacklevel=DatasetLogger.ROOT_LOGGER_STACK_LEVEL, *args, **kwargs
        )

    def info(self, msg: str, log_to_stdout: bool = True, *args, **kwargs):
        """Calls the standard `Logger.info` method and emits the resulting
        row to the Datasets log file.

        Args:
            msg: Message to log; logs 'msg % args'
            log_to_stdout: If True, also emit logs to stdout in addition
            to writing to the log file.
        """
        self.logger.propagate = log_to_stdout
        self.logger.info(
            msg,
            stacklevel=DatasetLogger.ROOT_LOGGER_STACK_LEVEL,
            *args,
            **kwargs,
        )

    def warning(self, msg: str, log_to_stdout: bool = True, *args, **kwargs):
        """Calls the standard `Logger.warning` method and emits the resulting
        row to the Datasets log file.

        Args:
            msg: Message to log; logs 'msg % args'
            log_to_stdout: If True, also emit logs to stdout in addition
            to writing to the log file.
        """
        self.logger.propagate = log_to_stdout
        self.logger.warning(
            msg,
            stacklevel=DatasetLogger.ROOT_LOGGER_STACK_LEVEL,
            *args,
            **kwargs,
        )

    def error(self, msg: str, log_to_stdout: bool = True, *args, **kwargs):
        """Calls the standard `Logger.error` method and emits the resulting
        row to the Datasets log file.

        Args:
            msg: Message to log; logs 'msg % args'
            log_to_stdout: If True, also emit logs to stdout in addition
            to writing to the log file.
        """
        self.logger.propagate = log_to_stdout
        self.logger.error(
            msg,
            stacklevel=DatasetLogger.ROOT_LOGGER_STACK_LEVEL,
            *args,
            **kwargs,
        )

    def exception(self, msg: str, log_to_stdout: bool = True, *args, **kwargs):
        """Calls the standard `Logger.exception` method and emits the resulting
        row to the Datasets log file.

        Args:
            msg: Message to log; logs 'msg % args'
            log_to_stdout: If True, also emit logs to stdout in addition
            to writing to the log file.
        """
        self.logger.propagate = log_to_stdout
        self.logger.exception(
            msg,
            stacklevel=DatasetLogger.ROOT_LOGGER_STACK_LEVEL,
            *args,
            **kwargs,
        )

    def critical(self, msg: str, log_to_stdout: bool = True, *args, **kwargs):
        """Calls the standard `Logger.critical` method and emits the resulting
        row to the Datasets log file.

        Args:
            msg: Message to log; logs 'msg % args'
            log_to_stdout: If True, also emit logs to stdout in addition
            to writing to the log file.
        """
        self.logger.propagate = log_to_stdout
        self.logger.critical(
            msg,
            stacklevel=DatasetLogger.ROOT_LOGGER_STACK_LEVEL,
            *args,
            **kwargs,
        )
