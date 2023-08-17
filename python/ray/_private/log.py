import logging
import re
from logging.config import dictConfig
import threading
from typing import Union


def _print_loggers():
    """Print a formatted list of loggers and their handlers for debugging."""
    loggers = {logging.root.name: logging.root}
    loggers.update(dict(sorted(logging.root.manager.loggerDict.items())))
    for name, logger in loggers.items():
        if isinstance(logger, logging.Logger):
            print(f"  {name}: disabled={logger.disabled}, propagate={logger.propagate}")
            for handler in logger.handlers:
                print(f"    {handler}")


def clear_logger(logger: Union[str, logging.Logger]):
    """Reset a logger, clearing its handlers and enabling propagation.

    Args:
        logger: Logger to be cleared
    """
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    logger.propagate = True
    logger.handlers.clear()


class ContextFilter(logging.Filter):
    """A filter that adds ray context info to log records.

    This filter adds a package name to append to the message as well as information
    about what worker emitted the message, if applicable.
    """

    logger_regex = re.compile(r"ray(\.(?P<subpackage>\w+))?(\..*)?")
    package_message_names = {
        "air": "AIR",
        "data": "Data",
        "rllib": "RLlib",
        "serve": "Serve",
        "train": "Train",
        "tune": "Tune",
        "workflow": "Workflow",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filter(self, record: logging.LogRecord) -> bool:
        """Add context information to the log record.

        This filter adds a package name from where the message was generated as
        well as the worker IP address, if applicable.

        Args:
            record: Record to be filtered

        Returns:
            True if the record is to be logged, False otherwise. (This filter only
            adds context, so records are always logged.)
        """
        match = self.logger_regex.search(record.name)
        if match and match["subpackage"] in self.package_message_names:
            record.package = f"[Ray {self.package_message_names[match['subpackage']]}]"
        else:
            record.package = ""

        return True


class PlainRayHandler(logging.StreamHandler):
    """A plain log handler.

    This handler writes to whatever sys.stderr points to at emit-time,
    not at instantiation time. See docs for logging._StderrHandler.
    """

    def __init__(self):
        super().__init__()
        self.plain_handler = logging._StderrHandler()
        self.plain_handler.level = self.level
        self.plain_handler.formatter = logging.Formatter(fmt="%(message)s")

    def emit(self, record: logging.LogRecord):
        """Emit the log message.

        If this is a worker, bypass fancy logging and just emit the log record.
        If this is the driver, emit the message using the appropriate console handler.

        Args:
            record: Log record to be emitted
        """
        import ray

        if (
            hasattr(ray, "_private")
            and hasattr(ray._private, "worker")
            and ray._private.worker.global_worker.mode
            == ray._private.worker.WORKER_MODE
        ):
            self.plain_handler.emit(record)
        else:
            logging._StderrHandler.emit(self, record)


logger_initialized = False
logging_config_lock = threading.Lock()


def generate_logging_config():
    """Generate the default Ray logging configuration."""
    with logging_config_lock:
        global logger_initialized
        if logger_initialized:
            return
        logger_initialized = True

        formatters = {
            "plain": {
                "format": (
                    "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
                ),
            },
        }
        filters = {"context_filter": {"()": ContextFilter}}
        handlers = {
            "default": {
                "()": PlainRayHandler,
                "formatter": "plain",
                "filters": ["context_filter"],
            }
        }

        loggers = {
            # Default ray logger; any log message that gets propagated here will be
            # logged to the console. Disable propagation, as many users will use
            # basicConfig to set up a default handler. If so, logs will be
            # printed twice unless we prevent propagation here.
            "ray": {
                "level": "INFO",
                "handlers": ["default"],
                "propagate": False,
            },
            # Special handling for ray.rllib: only warning-level messages passed through
            # See https://github.com/ray-project/ray/pull/31858 for related PR
            "ray.rllib": {
                "level": "WARN",
            },
        }

        dictConfig(
            {
                "version": 1,
                "formatters": formatters,
                "filters": filters,
                "handlers": handlers,
                "loggers": loggers,
                "disable_existing_loggers": False,
            }
        )
