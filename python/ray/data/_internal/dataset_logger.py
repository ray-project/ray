import logging
import os

import ray
from ray._private.ray_constants import LOGGER_FORMAT


class DatasetLogger:
    """Logger for Ray Datasets which, in addition to logging to stdout,
    also writes to a separate log file at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`.
    """

    DEFAULT_DATASET_LOG_PATH = "logs/ray-data.log"

    def __init__(self, log_name: str):
        """Initialize DatasetLogger for a given `log_name`.

        Args:
            log_name: Name of logger (usually passed into `logging.getLogger(...)`)
        """
        # Primary logger used to logging to log file
        self.logger = logging.getLogger(log_name)
        # Secondary logger used for logging to stdout
        self.logger_stdout = logging.getLogger(f"{log_name}.stdout")
        # Clear existing handlers in primary logger to disable logging to stdout by default
        while len(self.logger.handlers) > 0:
            self.logger.removeHandler(self.logger.handlers[0])

        # Explicitly need to set the logging level again;
        # otherwise the default level is `logging.NOTSET`,
        # which suppresses logs from being emitted to the file
        self.logger.setLevel(logging.INFO)
        self.logger_stdout.setLevel(logging.INFO)

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
            # Add a FileHandler to write to the specific Ray Datasets log file
            file_log_handler = logging.FileHandler(self.datasets_log_path)
            # For file logs, use the standard default logger format
            file_log_formatter = logging.Formatter(fmt=LOGGER_FORMAT)
            file_log_handler.setFormatter(file_log_formatter)
            self.logger.addHandler(file_log_handler)

        def debug(msg: str, log_to_stdout: bool = False, *args, **kwargs):
            """Calls the standard `Logger.debug` method and emits the resulting
            row to the Datasets log file.

            Args:
                msg: Message to log; logs 'msg % args'
                log_to_stdout: If True, also emit logs to stdout in addition
                to writing to the log file.
            """
            self.logger.debug(msg, *args, **kwargs)
            if log_to_stdout:
                self.logger_stdout.debug(msg, *args, **kwargs)

        def info(msg: str, log_to_stdout: bool = False, *args, **kwargs):
            """Calls the standard `Logger.info` method and emits the resulting
            row to the Datasets log file.

            Args:
                msg: Message to log; logs 'msg % args'
                log_to_stdout: If True, also emit logs to stdout in addition
                to writing to the log file.
            """
            self.logger.info(msg, *args, **kwargs)
            if log_to_stdout:
                self.logger_stdout.info(msg, *args, **kwargs)

        def warning(msg: str, log_to_stdout: bool = False, *args, **kwargs):
            """Calls the standard `Logger.info` method and emits the resulting
            row to the Datasets log file.

            Args:
                msg: Message to log; logs 'msg % args'
                log_to_stdout: If True, also emit logs to stdout in addition
                to writing to the log file.
            """
            self.logger.warning(msg, *args, **kwargs)
            if log_to_stdout:
                self.logger_stdout.warning(msg, *args, **kwargs)

        def error(msg: str, log_to_stdout: bool = False, *args, **kwargs):
            """Calls the standard `Logger.error` method and emits the resulting
            row to the Datasets log file.

            Args:
                msg: Message to log; logs 'msg % args'
                log_to_stdout: If True, also emit logs to stdout in addition
                to writing to the log file.
            """
            self.logger.error(msg, *args, **kwargs)
            if log_to_stdout:
                self.logger_stdout.error(msg, *args, **kwargs)

        def exception(msg: str, log_to_stdout: bool = False, *args, **kwargs):
            """Calls the standard `Logger.exception` method and emits the resulting
            row to the Datasets log file.

            Args:
                msg: Message to log; logs 'msg % args'
                log_to_stdout: If True, also emit logs to stdout in addition
                to writing to the log file.
            """
            self.logger.exception(msg, *args, **kwargs)
            if log_to_stdout:
                self.logger_stdout.exception(msg, *args, **kwargs)

        def critical(msg: str, log_to_stdout: bool = False, *args, **kwargs):
            """Calls the standard `Logger.info` method and emits the resulting
            row to the Datasets log file.

            Args:
                msg: Message to log; logs 'msg % args'
                log_to_stdout: If True, also emit logs to stdout in addition
                to writing to the log file.
            """
            self.logger.critical(msg, *args, **kwargs)
            if log_to_stdout:
                self.logger_stdout.critical(msg, *args, **kwargs)
