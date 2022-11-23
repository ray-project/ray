import logging
import os

import ray
from ray._private.ray_constants import LOGGER_FORMAT


class DatasetLogger:
    """Logger for Ray Datasets which, in addition to logging to stdout,
    also writes to a separate log file at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`.
    """

    DEFAULT_DATASET_LOG_PATH = "logs/ray-data.log"

    def __init__(self, log_name):
        """Initialize DatasetLogger for a given `log_name`.

        Args:
            log_name: Name of logger (usually passed into `logging.getLogger(...)`)
        """
        self.logger = logging.getLogger(log_name)
        # Explicitly need to set the logging level again;
        # otherwise the default level is `logging.NOTSET`,
        # which suppresses logs from being emitted to the file
        self.logger.setLevel(logging.INFO)

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
            dataset_log_handler = logging.FileHandler(self.datasets_log_path)
            # For file logs, use the same default logger format as stdout
            dataset_log_formatter = logging.Formatter(
                fmt=LOGGER_FORMAT,
            )
            dataset_log_handler.setFormatter(dataset_log_formatter)
            self.logger.addHandler(dataset_log_handler)
