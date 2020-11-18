import logging
import os

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
        intercept_stdout(bool): True if the class intercepts stdout. False
                         if stderr is intercepted.
    """

    def __init__(self, logger, intercept_stdout=True):
        self.logger = logger
        self.intercept_stdout = intercept_stdout

    def write(self, message):
        """Redirect the original message to the logger."""
        self.logger.info(message)

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()

    def isatty(self):
        # Return the standard out isatty. This is used by colorful.
        fd = 1 if self.intercept_stdout else 2
        return os.isatty(fd)
