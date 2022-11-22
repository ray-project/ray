import logging
import os
from typing import Optional

import ray
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DatasetLogger:
    """ Logger for Ray Datasets which, in addition to logging to stdout,
        also writes to a separate log file at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`.
    """
    DEFAULT_DATASET_LOG_PATH = 'logs/datasets.log'

    def __init__(self, log_name, dataset_log_path: Optional[str] = None):
        """ Initialize DatasetLogger for a given `log_name`.

        Args:
            log_name: Name of logger (usually passed into `logging.getLogger(...)`)
            dataset_log_path: If specified, write logs to this path 
            instead of `DatasetLogger.DEFAULT_DATASET_LOG_PATH`
        """
        self.logger = logging.getLogger(log_name)

        # Add log handler to write to a separate Datasets log file
        # at `DatasetLogger.DEFAULT_DATASET_LOG_PATH`
        session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
        self.datasets_log_path = os.path.join(
            session_dir, 
            dataset_log_path or DatasetLogger.DEFAULT_DATASET_LOG_PATH,
        )
        self.logger.addHandler(logging.FileHandler(self.datasets_log_path))

        
        

    
