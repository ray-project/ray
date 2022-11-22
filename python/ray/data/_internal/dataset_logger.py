import logging
import os
from typing import Optional

import ray
from ray._private.ray_constants import LOGGER_FORMAT
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DatasetLogger:
    """Logger for Ray Datasets which, in addition to logging to stdout,
    also writes to a separate log file at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`.
    """

    DEFAULT_DATASET_LOG_PATH = "logs/datasets.log"

    def __init__(self, log_name, dataset_log_path: Optional[str] = None):
        """Initialize DatasetLogger for a given `log_name`.

        Args:
            log_name: Name of logger (usually passed into `logging.getLogger(...)`)
            dataset_log_path: If specified, write logs to this path
            instead of `DatasetLogger.DEFAULT_DATASET_LOG_PATH`
        """
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(logging.INFO)

        # If ray.init() was previously called, add log handler which writes to
        # a separate Datasets log file at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`
        if ray.is_initialized():  
            # With current implementation, we can only get session_dir after ray.init() is called
            session_dir = ray._private.worker._global_node.get_session_dir_path()
            self.datasets_log_path = os.path.join(
                session_dir,
                dataset_log_path or DatasetLogger.DEFAULT_DATASET_LOG_PATH,
            )
            dataset_log_handler = logging.FileHandler(self.datasets_log_path)
            dataset_log_formatter = logging.Formatter(
                fmt=LOGGER_FORMAT,
            )
            dataset_log_handler.setFormatter(dataset_log_formatter)
            self.logger.addHandler(dataset_log_handler)
