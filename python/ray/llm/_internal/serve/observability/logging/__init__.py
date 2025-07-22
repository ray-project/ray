import logging
from typing import Optional

from ray._private.ray_logging.filters import CoreContextFilter
from ray.serve._private.logging_utils import ServeContextFilter


def _setup_logger(logger_name: str):
    """Setup logger given the logger name.

    This function is idempotent and won't set up the same logger multiple times. It will
    Also skip the setup if Serve logger is already setup and has handlers.
    """
    logger = logging.getLogger(logger_name)
    serve_logger = logging.getLogger("ray.serve")

    # Skip setup if the logger already has handlers setup or if the parent (Serve
    # logger) has handlers.
    if logger.handlers or serve_logger.handlers:
        return

    # Set up stream handler, which logs to console as plaintext.
    stream_handler = logging.StreamHandler()
    stream_handler.addFilter(CoreContextFilter())
    stream_handler.addFilter(ServeContextFilter())
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def get_logger(name: Optional[str] = None):
    """Get a structured logger inherited from the Ray Serve logger.

    Loggers by default are logging to stdout, and are expected to be scraped by an
    external process.
    """
    logger_name = f"ray.serve.{name}"
    _setup_logger(logger_name)
    return logging.getLogger(logger_name)
