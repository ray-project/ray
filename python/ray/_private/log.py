import logging
import threading
import time
from typing import Union

INTERNAL_TIMESTAMP_LOG_KEY = "_ray_timestamp_ns"


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


def _setup_log_record_factory():
    """Setup log record factory to add _ray_timestamp_ns to LogRecord."""
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        # Python logging module starts to use `time.time_ns()` to generate `created`
        # from Python 3.13 to avoid the precision loss caused by the float type.
        # Here, we generate the `created` for the LogRecord to support older Python
        # versions.
        ct = time.time_ns()
        record.created = ct / 1e9

        record.__dict__[INTERNAL_TIMESTAMP_LOG_KEY] = ct

        return record

    logging.setLogRecordFactory(record_factory)


def generate_logging_config():
    """Generate the default Ray logging configuration."""
    with logging_config_lock:
        global logger_initialized
        if logger_initialized:
            return
        logger_initialized = True

        plain_formatter = logging.Formatter(
            "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
        )

        default_handler = PlainRayHandler()
        default_handler.setFormatter(plain_formatter)

        ray_logger = logging.getLogger("ray")
        ray_logger.setLevel(logging.INFO)
        ray_logger.addHandler(default_handler)
        ray_logger.propagate = False

        # Special handling for ray.rllib: only warning-level messages passed through
        # See https://github.com/ray-project/ray/pull/31858 for related PR
        rllib_logger = logging.getLogger("ray.rllib")
        rllib_logger.setLevel(logging.WARN)

        # Set up the LogRecord factory.
        _setup_log_record_factory()
