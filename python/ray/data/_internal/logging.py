import logging
import logging.config
import os
from typing import Optional

import yaml

import ray
from ray._private.ray_constants import LOGGER_FORMAT

CONFIG_FILENAME = "logging.yaml"


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
        if self._handler is not None:
            self._handler.setFormatter(fmt)
        self._formatter = fmt

    def _initialize_handler(self):
        log_dir = get_log_directory()
        if log_dir is not None:
            self._path = os.path.join(log_dir, self._filename)
            self._handler = logging.FileHandler(self._path)
            if self._formatter is not None:
                self._handler.setFormatter(self._formatter)
        self._initialized = True


def configure_logging():
    with open(
        os.abspath(os.path.join(os.path.dirname(__file__), CONFIG_FILENAME))
    ) as file:
        config = yaml.safe_load(file)
    logging.config.dictConfig(config)


def get_log_directory() -> Optional[str]:
    global_node = ray._private.worker._global_node
    if global_node is None:
        return None

    session_dir = global_node.get_session_dir_path()
    return os.path.join(session_dir, "logs")
