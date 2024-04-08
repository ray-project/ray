import logging
import os
from typing import Optional

import ray
from ray._private.ray_constants import LOGGER_FORMAT

_default_file_handler: logging.Handler = None

DEFAULT_DATASET_LOG_FILENAME = "ray-data.log"

# To facilitate debugging, Ray Data writes debug logs to a file. However, if Ray Data
# logs every scheduler loop, logging might impact performance. So, we add a "TRACE"
# level where logs aren't written by default.
#
# Use the following code to log a message at the "TRACE" level:
# ```
# logger.log(logging.getLevelName("TRACE"), "Your message here.")
# ````
logging.addLevelName(logging.DEBUG - 1, "TRACE")


class SessionFileHandler(logging.Handler):
    """A handler that writes to a log file in the Ray session directory.

    The Ray session directory isn't available until Ray is initialized, so this handler
    lazily creates the file handler when you emit a log record.

    Args:
        filename: The name of the log file. The file is created in the 'logs' directory
            of the Ray session directory.
    """

    def __init__(self, filename: str):
        super().__init__()
        self._filename = filename
        self._handler = None
        self._formatter = None
        self._path = None

    def emit(self, record):
        if self._handler is None:
            self._try_create_handler()
        if self._handler is not None:
            self._handler.emit(record)

    def setFormatter(self, fmt: logging.Formatter) -> None:
        if self._handler is not None:
            self._handler.setFormatter(fmt)
        self._formatter = fmt

    def _try_create_handler(self):
        assert self._handler is None

        global_node = ray._private.worker._global_node
        if global_node is None:
            return

        session_dir = global_node.get_session_dir_path()
        self._path = os.path.join(session_dir, "logs", self._filename)
        self._handler = logging.FileHandler(self._path)
        if self._formatter is not None:
            self._handler.setFormatter(self._formatter)

    @property
    def path(self) -> Optional[str]:
        """Path to the log file or ``None`` if the file hasn't been created yet."""
        return self._path


def configure_logging() -> None:
    """Configure the Python logger named 'ray.data'.

    This function configures the logger to write Ray Data logs to a file in the Ray
    session directory.

    Only call this function once. It'll fail if you call it twice.
    """
    # Save the file handler in a global variable so that we can get the log path later.
    global _default_file_handler
    assert _default_file_handler is None, "Logging already configured."

    logger = logging.getLogger("ray.data")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt=LOGGER_FORMAT)
    file_handler = SessionFileHandler(DEFAULT_DATASET_LOG_FILENAME)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    _default_file_handler = file_handler


def is_logging_configured() -> bool:
    return _default_file_handler is not None


def reset_logging() -> None:
    """Reset the logger named 'ray.data' to its initial state.

    Used for testing.
    """
    global _default_file_handler
    _default_file_handler = None

    logger = logging.getLogger("ray.data")
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)


def get_log_path() -> Optional[str]:
    return _default_file_handler.path
