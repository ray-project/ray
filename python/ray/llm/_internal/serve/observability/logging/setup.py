import logging

from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter
from ray.serve._private.logging_utils import ServeContextFilter


def _configure_stdlib_logging():
    """Configures stdlib root logger to make sure stdlib loggers (created as
    `logging.getLogger(...)`) are using Ray's `JSONFormatter` with Core and Serve
     context filters.
    """

    handler = logging.StreamHandler()
    handler.addFilter(CoreContextFilter())
    handler.addFilter(ServeContextFilter())
    handler.setFormatter(JSONFormatter())

    root_logger = logging.getLogger()
    # NOTE: It's crucial we reset all the handlers of the root logger,
    #       to make sure that logs aren't emitted twice
    root_logger.handlers = []
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def setup_logging():
    _configure_stdlib_logging()
