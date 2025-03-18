import logging
import threading
import time

from ray._private.ray_logging.handlers import PlainRayHandler


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
