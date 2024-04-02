import logging
import os
from typing import Optional

import ray
from ray._private.ray_constants import LOGGER_FORMAT

_default_log_handler: logging.Handler = None

DEFAULT_DATASET_LOG_FILENAME = "ray-data.log"


class LogHandler(logging.Handler):
    def __init__(self, filename: str):
        super().__init__()
        self._filename = filename
        self._initialized = False
        self._handler = None
        self._formatter = None
        self._path = None

    def emit(self, record):
        if not self._initialized:
            self._initialize_handler()
        if self._handler is not None:
            self._handler.emit(record)

    def setFormatter(self, fmt: logging.Formatter) -> None:
        self._formatter = fmt

    def _initialize_handler(self):
        global_node = ray._private.worker._global_node
        if global_node is None:
            self._handler = None
        else:
            session_dir = global_node.get_session_dir_path()
            self._path = os.path.join(session_dir, "logs", self._filename)
            self._handler = logging.FileHandler(self._path)
            if self._formatter is not None:
                self._handler.setFormatter(self._formatter)
        self._initialized = True

    @property
    def path(self) -> Optional[str]:
        return self._path


def configure_logging():
    global _default_log_handler
    assert _default_log_handler is None, "Logging already configured."

    logger = logging.getLogger("ray.data")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(fmt=LOGGER_FORMAT)
    log_handler = LogHandler(DEFAULT_DATASET_LOG_FILENAME)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    _default_log_handler = log_handler


def get_log_path() -> Optional[str]:
    return _default_log_handler.path
