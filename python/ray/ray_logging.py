import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import ray
from ray.utils import binary_to_hex

_default_handler = None


def setup_logger(logging_level, logging_format):
    """Setup default logging for ray."""
    logger = logging.getLogger("ray")
    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    logger.setLevel(logging_level)
    global _default_handler
    if _default_handler is None:
        _default_handler = logging.StreamHandler()
        logger.addHandler(_default_handler)
    _default_handler.setFormatter(logging.Formatter(logging_format))
    # Setting this will avoid the message
    # is propagated to the parent logger.
    logger.propagate = False


def setup_component_logger(*, logging_level, logging_format, log_dir, filename,
                           max_bytes, backup_count):
    """Configure the root logger that is used for Ray's python components.

    For example, it should be used for monitor, dashboard, and log monitor.
    The only exception is workers. They use the different logging config.

    Args:
        logging_level(str | int): Logging level in string or logging enum.
        logging_format(str): Logging format string.
        log_dir(str): Log directory path.
        filename(str): Name of the file to write logs.
        max_bytes(int): Same argument as RotatingFileHandler's maxBytes.
        backup_count(int): Same argument as RotatingFileHandler's backupCount.
    """
    # Get the root logger.
    logger = logging.getLogger("")
    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    assert filename, "filename argument should not be None."
    assert log_dir, "log_dir should not be None."
    handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, filename),
        maxBytes=max_bytes,
        backupCount=backup_count)
    logger.setLevel(logging_level)
    handler.setFormatter(logging.Formatter(logging_format))
    logger.addHandler(handler)


"""
All components underneath here is used specifically for the default_worker.py.
"""


class StandardStreamInterceptor:
    """Used to intercept stdout and stderr.

    Intercepted messages are handled by the given logger.

    NOTE: The logger passed to this method should always have
          logging.INFO severity level.

    Example:
        >>> from contextlib import redirect_stdout
        >>> logger = logging.getLogger("ray_logger")
        >>> hook = StandardStreamHook(logger)
        >>> with redirect_stdout(hook):
        >>>     print("a") # stdout will be delegated to logger.

    Args:
        logger: Python logger that will receive messages streamed to
                the standard out/err and delegate writes.
    """

    def __init__(self, logger):
        self.logger = logger
        assert len(self.logger.handlers) == 1, (
            "Only one handler is allowed for the interceptor logger.")

    def write(self, message):
        """Redirect the original message to the logger."""
        self.logger.info(message)
        return len(message)

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()

    def isatty(self):
        # Return the standard out isatty. This is used by colorful.
        # Note: Both stdout and stderr should return True.
        return os.isatty(1)

    def close(self):
        handler = self.logger.handlers[0]
        handler.close()

    def fileno(self):
        handler = self.logger.handlers[0]
        return handler.stream.fileno()


class StandardFdRedirectionRotatingFileHandler(RotatingFileHandler):
    """RotatingFileHandler that redirects stdout and stderr to the log file.

    It is specifically used to default_worker.py.

    The only difference from this handler vs original RotatingFileHandler is
    that it actually duplicates the OS level fd using os.dup2.
    """

    def __init__(self,
                 filename,
                 mode="a",
                 maxBytes=0,
                 backupCount=0,
                 encoding=None,
                 delay=False):
        super().__init__(
            filename,
            mode=mode,
            maxBytes=maxBytes,
            backupCount=backupCount,
            encoding=encoding,
            delay=delay)
        self.switch_os_fd()

    def doRollover(self):
        super().doRollover()
        self.switch_os_fd()

    def switch_os_fd(self):
        # Old fd will automatically closed by dup2 when necessary.
        print(sys.stdout.fileno())
        print(sys.stderr.fileno())
        os.dup2(self.stream.fileno(), sys.stdout.fileno())
        os.dup2(self.stream.fileno(), sys.stderr.fileno())


def setup_and_get_worker_interceptor_logger(args, max_bytes=0, backup_count=0):
    """Setup a logger to be used to intercept worker log messages.

    NOTE: This method is only meant to be used within default_worker.py.

    Ray worker logs should be treated in a special way because
    there's a need to intercept stdout and stderr to support various
    ray features. For example, ray will prepend 0 or 1 in the beggining
    of each log message to decide if logs should be streamed to driveres.

    This logger will also setup the RotatingFileHandler for
    ray workers processes.

    If max_bytes and backup_count is not set, files will grow indefinitely.

    Args:
        args: args received from default_worker.py.
        max_bytes(int): maxBytes argument of RotatingFileHandler.
        backup_count(int): backupCount argument of RotatingFileHandler.
    """
    logger = logging.getLogger(f"ray_default_worker")
    if len(logger.handlers) == 1:
        return logger
    logger.setLevel(logging.INFO)
    # TODO(sang): This is how the job id is propagated to workers now.
    # But eventually, it will be clearer to just pass the job id.
    job_id = os.environ.get("RAY_JOB_ID")
    if args.worker_type == "WORKER":
        assert job_id is not None, (
            "RAY_JOB_ID should be set as an env "
            "variable within default_worker.py. If you see this error, "
            "please report it to Ray's Github issue.")
        worker_name = "worker"
    else:
        job_id = ray.JobID.nil()
        worker_name = "io_worker"

    # Make sure these values are set already.
    assert ray.worker._global_node is not None
    assert ray.worker.global_worker is not None
    filename = (f"{ray.worker._global_node.get_session_dir_path()}/logs/"
                f"{worker_name}-"
                f"{binary_to_hex(ray.worker.global_worker.worker_id)}-"
                f"{job_id}-{os.getpid()}.log")
    handler = StandardFdRedirectionRotatingFileHandler(
        filename, maxBytes=max_bytes, backupCount=backup_count)
    logger.addHandler(handler)
    # TODO(sang): Add 0 or 1 to decide whether
    # or not logs are streamed to drivers.
    handler.setFormatter(logging.Formatter("%(message)s"))
    # Avoid messages are propagated to parent loggers.
    logger.propagate = False
    # Remove the terminator. It is important because we don't want this
    # logger to add a newline at the end of string.
    handler.terminator = ""
    return logger
