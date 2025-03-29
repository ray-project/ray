import logging
import inspect


def log_with_context(msg: str, level: str = "INFO", logger=None, *args, **kwargs):
    """Logs a message with class and function context.

    Args:
        msg: The message to log.
        level: Log level (INFO, DEBUG, etc.). Defaults to INFO.
        logger: Logger instance. Defaults to module-level logger.
        *args: Extra positional arguments for logging.
        **kwargs: Extra keyword arguments for logging.
    """
    if logger is None:
        logger = logging.getLogger(__name__)  # Use module-level logger

    # Get caller info
    frame = inspect.currentframe().f_back
    class_name = getattr(frame.f_locals.get("self"), "__class__", None)
    func_name = frame.f_code.co_name

    # Build the prefix "[ClassName/FunctionName]"
    prefix = f"[{class_name.__name__}/{func_name}]" if class_name else f"[{func_name}]"

    # Log with the appropriate level
    level = getattr(logging, level.upper(), logging.INFO)
    logger.log(level, f"{prefix} {msg}", *args, **kwargs)
