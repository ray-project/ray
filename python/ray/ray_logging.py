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


def setup_component_logger(logging_level, logging_format):
    """Logger that is used for Ray's python components.
    
    For example, it should be used for monitor, dashboard, and log monitor.
    The only exception is workers. They use the different logging config.
    """
    try:
        if args.logging_filename:
            logging_handlers = [
                logging.handlers.RotatingFileHandler(
                    os.path.join(args.log_dir, args.logging_filename),
                    maxBytes=args.logging_rotate_bytes,
                    backupCount=args.logging_rotate_backup_count)
            ]
        else:
            logging_handlers = None
        logging.basicConfig(
            level=args.logging_level,
            format=args.logging_format,
            handlers=logging_handlers)


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
