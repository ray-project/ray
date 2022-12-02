import logging
import os

import ray
from ray._private.ray_constants import LOGGER_FORMAT, LOGGER_LEVEL


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

    def get_logger(self, log_to_stdout: bool = True):
        """
        Returns the underlying Logger, with the `propagate` attribute set
        to the same value as `log_to_stdout`. For example, when
        `log_to_stdout = False`, we do not want the `DatasetLogger` to
        propagate up to the base Logger which writes to stdout.

        This is a workaround needed due to the DatasetLogger wrapper object
        not having access to the log caller's scope in Python <3.8.
        In the future, with Python 3.8 support, we can use the `stacklevel` arg,
        which allows the logger to fetch the correct calling file/line:
        `logger.info(msg="Hello world", stacklevel=2)`
        """
        self.logger.propagate = log_to_stdout
        return self.logger
