import logging
from typing import Optional

from ray._private.ray_logging.filters import CoreContextFilter


def _setup_logger(logger_name: str):
    """Setup logger given the logger name.

    This function is idempotent and won't set up the same logger multiple times. It will
    also skip the setup if Data logger is already setup and has handlers.

    Args:
        logger_name: logger name used to get the logger.
    """
    logger = logging.getLogger(logger_name)
    llm_logger = logging.getLogger("ray.data")

    # Skip setup if the logger already has handlers setup or if the parent (Data
    # logger) has handlers.
    if logger.handlers or llm_logger.handlers:
        return

    # Set up stream handler, which logs to console as plaintext.
    stream_handler = logging.StreamHandler()
    stream_handler.addFilter(CoreContextFilter())
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def get_logger(name: Optional[str] = None):
    """Get a structured logger inherited from the Ray Data logger.

    Loggers by default are logging to stdout, and are expected to be scraped by an
    external process.
    """
    logger_name = f"ray.data.{name}"
    _setup_logger(logger_name)
    return logging.getLogger(logger_name)
